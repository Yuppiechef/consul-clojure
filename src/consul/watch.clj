(ns consul.watch
  (:require
    [consul.core :as consul]
    [clojure.core.async :as async]))

(def watch-fns
  {:key       consul/kv-get
   :keyprefix consul/kv-recurse
   :service   consul/service-health})

(defn poll! [conn [kw x] params]
  (let [f    (get watch-fns kw)]
    (f conn x params)))

(defn poll
  "Calls consul for the given spec passing params.  Returns the exception if one occurs."
  [conn spec params]
  (try (poll! conn spec params)
       (catch Exception err
         err)))

(defn long-poll
  "Polls consul using spec and publishes results onto ch.  Applies no throttling of requests towards consul except via ch."
  [conn spec ch {:as options}]
  (assert (get watch-fns (first spec) (str "unimplemented watch type " spec)))
  (async/go-loop [resp (async/<! (async/thread (poll conn spec options)))]
    (when (async/>! ch resp)
      (recur (async/<! (async/thread (if (consul/ex-info? resp)
                                       (poll conn spec options) ;; don't rely on the old index if we experience a consul failure
                                       (poll conn spec (merge options {:index (:modify-index resp) :wait "45s"})))))))))

(defn update-state
  "When called with new-config, creates a new state map.  When called with old-state and new-config, old-state is updated with new-config."
  ([new-config] {:config new-config :failures 0})
  ([old-state new-config]
   (if (consul/ex-info? new-config)
     (-> old-state
         (update-in [:failures] (fnil inc 0))
         (assoc :error new-config))
     (assoc old-state :config new-config :failures 0))))

(defn exp-wait [n max-ms]
  {:pre [(number? n) (>= n 0) (number? max-ms) (> max-ms 0)]}
  (min (* (Math/pow 2 n) 100) max-ms))

(defn setup-watch
  "Creates a watching channel and notifies changes to a change-chan channel

  :query-params   - the query params passed into the underlying service call
  :max-retry-wait - max interval before retying consul when a failure occurs.  Defaults to 5s.

  The watch will terminate when the change-chan output channel is closed or when resulting
  function is called"
  [conn [watch-key path :as spec] change-chan {:keys [max-retry-wait query-params log] :as options}]
  (let [ch (async/chan)
        log (or log (fn [& _]))]
    (long-poll conn spec ch query-params)
    (async/go
      (loop [old-state nil]
        (log "Start watching " spec)
        (when-let [new-config (async/<! ch)]
          (if-not (consul/ex-info? new-config) (log "Message: " new-config))
          (cond
            (consul/ex-info? new-config)
            (do
              (async/<! (async/timeout (exp-wait (or (:failures old-state) 0) (get options :max-retry-wait 5000))))
              (recur (update-state old-state new-config)))
            (not= (:mapped (:config old-state)) (:mapped new-config))
            (do
              (log "State changed for " spec " : " new-config)
              (when (async/>! change-chan new-config)
                (recur (update-state old-state new-config))))
            :else
            (recur (update-state old-state new-config)))))
      (log "Finished watching " spec))
    #(async/close! ch)))

(defn ttl-check-update
  [conn check-id ^long ms ch]
  "Periodically updates check-id according to freq. Exits if ch closes."
  {:pre [(string? check-id) ms]}
  (async/go-loop []
    (async/<! (async/thread (consul/agent-pass-check conn check-id)))
    (when (async/alt! ch
            ([v] v)
            (async/timeout ms)
            ([_] :continue))
      (recur))))

(defn check-id [service-definition]
  (str "service:" (:id service-definition)))

(defn register-service
  "Register a service and keeps sending a health check to Consul every update-ms"
  [conn service-definition update-ms]
  (let [control-ch (async/chan)]
    (consul/agent-register-service conn service-definition)
    (ttl-check-update conn (check-id service-definition) update-ms control-ch)
    (fn []
      (async/close! control-ch)
      (consul/agent-deregister-service conn (:id service-definition)))))

(defn setup-leader-watch [name]
  (let [w (async/chan (async/sliding-buffer 1))]
    (async/go-loop [m (async/<! w)]
      (when m
        (println name ": " m)
        (recur (async/<! w))))
    w))


;; TODO: The session needs to manage its own ttl renewal and shutdown lifecycle.

(def local-sessions (atom {}))

(defn leader-watch
  "Setup a leader election watch on a given key, sends a vector with [path true/false] when the election changes.
   If you close the leader-ch, it will stop trying to renew leadership.
   To kill leader immediately use `(kill-leader leader-ch)"
  [conn session-opts k leader-ch {:keys [max-retry-wait query-params log] :as options}]
  (let [ch (async/chan)
        log (or log println)
        log (partial log)
        session-info (fn [session]
                       (when session
                         (try (:body (consul/session-info conn session))
                              (catch Exception _ (log "Exception info session")))))
        ensure-session (fn [session]
                         (let [info (session-info session)]
                           (if (seq info)
                             (do (log "Session info:" info) session)
                             (try
                               (log "Create session " k)
                               (consul/session-create conn session-opts)
                               (catch Exception e
                                 (log "Exception creating session: " (.getMessage e))
                                 (.printStackTrace e))))))
        aquire-session? (fn [session]
                          (try
                            (when session
                              (log "Acquiring on " session)
                              (let [a (consul/kv-put conn k "1" {:acquire session})]
                                (log "Acquire: " a)
                                a))
                            (catch Exception e
                              (log "Exception acquiring session: " (.getMessage e))
                              (.printStackTrace e)
                              false)))]
    (log "Start leader election on" k)
    (consul.watch/long-poll conn [:key k] ch query-params)
    (async/go
      (loop [state {}]
        (let [result (async/<! ch)
              old-leader? (boolean (:leader state))]
          (log "---------- Loop with " state)

          (if (consul/ex-info? result)
            (let [failures (:failures state 0)
                  timeout (consul.watch/exp-wait failures (get options :max-retry-wait (or max-retry-wait 5000)))]
              (when (and old-leader? (zero? failures) (async/>! leader-ch [k false]))
                (log "Error, release leader for" k " - " (.getMessage ^Throwable result)))
              (log "Retrying in" timeout "ms")
              (async/<! (async/timeout timeout))
              (recur (consul.watch/update-state state result)))

            (let [leader (:session result)
                  session (ensure-session (:session state))
                  _ (log "Sessionid:" session)
                  _ (log "Leaderid: " leader)
                  _ (swap! local-sessions assoc leader-ch {:conn conn :k k :session session})
                  state (assoc state :session session)
                  new-leader? (cond leader (= leader session)
                                    (not old-leader?) (aquire-session? session)
                                    old-leader? (when (async/>! leader-ch [k false])
                                                  (aquire-session? session)))
                  _ (log "Leader? " new-leader? result)
                  continue? (or (= leader session)
                                (async/>! leader-ch [k new-leader?]))]
              (if continue?
                (recur (assoc state :leader new-leader?))
                (do (log "Finished and releasing:" k)
                    (consul/kv-put conn k nil {:release session})
                    (consul/session-destroy conn session)
                    (async/>! leader-ch [k false]))))))))))

(defn kill-leader!
  "Given a channel representing a leader election node, give it a swift death."
  [ch]
  (async/close! ch)
  (when-let [{:keys [conn k session]} (get @local-sessions ch)]
    (consul/kv-put conn k nil {:release session})
    (swap! local-sessions dissoc ch)))

(defn kill-leader-pool!
  "Attempt to evict all leaders and clear lock. Returns (assumed) success.
   This will fail if there are any non-zombies, as they will reclaim the lock.
   This works by attempting to evict sessions, and seeing if the session reconnects, so will spam stop/starts
   for any non-zombies.
   May return false positives if active leader has high latency.
   Failure is not always negative.. it may mean a real leader has taken over from a zombie."
  ([conn leader-key] (kill-leader-pool! conn leader-key 3))
  ([conn leader-key n] (kill-leader-pool! conn leader-key n 300))
  ([conn leader-key n next-leader-pause]
   (not
     (async/<!!
       (async/go-loop [tries-left n]
         (if (zero? tries-left)
           true
           (when-let [id (:session (consul.core/kv-get conn leader-key))]
             (consul/kv-put :local leader-key nil {:release id})
             (async/<! (async/timeout next-leader-pause))
             (recur (dec tries-left)))))))))

(comment
  (def leader-key "/common/stuff/leader")

  (def log-agent (agent nil))

  (defn serial-log [& msgs]
    (send-off log-agent #(apply println %2) msgs))

  (defn close-component [var]
    (when-let [old-ch (:ch @var)]
      (println "closing..")
      (kill-leader! old-ch))
    (when-let [status (:status @var)]
      (swap! status assoc :started false)))

  (defn init-component [var id]
    (close-component var)
    (let [ch (async/chan)
          status (or (:status @var) (atom {}))]
      (reset! status {:started false :id id :bumps 0})
      (leader-watch :local {:ttl "30m"} leader-key ch {:log (partial serial-log (str "[" id "]"))})
      (async/go-loop []
        (let [result (async/<! ch)]
          (serial-log "Component:" id result)
          (swap! status assoc :started (boolean (second result)))
          (swap! status update :bumps inc)
          (when result (recur))))
      (alter-var-root var (constantly {:id id :ch ch :status status}))))

  ;; BUGS:
  ;; 1. when creating new session, seems to always fail (even if no leader). will work after timeout though

  (declare comp-1)
  (declare comp-2)
  (declare comp-3)

  (init-component #'comp-1 1)
  (init-component #'comp-2 2)
  (init-component #'comp-3 3)

  (close-component #'comp-1)
  (close-component #'comp-2)
  (close-component #'comp-3)

  (doseq [v [comp-1 comp-2 comp-3]]
    (prn @(:status v)))

  (run! kill-leader! (keys @watch/local-sessions))

  (kill-leader-pool! :local leader-key)

  (def session-id (:session (consul.core/kv-get :local leader-key)))
  (consul/kv-put :local leader-key (str (Math/random)) (when session-id {:release session-id}))
  (when session-id (consul/session-destroy :local session-id))
  (when session-id (:body (consul/session-info :local session-id))))
