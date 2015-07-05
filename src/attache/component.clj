(ns attache.component
  (:require [attache.core :as attache]
            [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [com.stuartsierra.dependency :as deps]
            [taoensso.timbre :as timbre :refer (log trace debug info warn error fatal spy)]))


(defn log-changes [key atom old-state new-state]
  (when (not= old-state new-state)
    (info "detected changes from consul")
    (info "was:")
    (spy :info old-state)
    (info "now:")
    (spy :info new-state)))

(defn watch-consul
  "Watches consul for changes associated with spec and publishes them onto ch."
  [conn spec ch]
  (async/go-loop [resp (async/<! (async/thread (try (attache/kv-get conn (:key spec))
                                                    (catch Exception err
                                                      err))))]
    (let [v (async/>! ch resp)]
      (if (true? v)
        (recur (async/<! (async/thread
                           (try (if (attache/ex-info? resp)
                                  (attache/kv-get conn (:key spec)) ;; ditch the index for faster clearing of errors
                                  (attache/kv-get conn (:key spec) :index (:modify-index (meta resp)) :wait "45s"))
                                (catch Exception err
                                  err)))))
        (do (info "exiting consul watch")
            (spy :debug spec))))))

(defn update-state
  "When called with new-config, creates a new state map.  When called with old-state and new-config, old-state is updated with new-config."
  ([new-config] {:config new-config :failures 0})
  ([old-state new-config]
   (if (attache/ex-info? new-config)
     (-> old-state
         (update-in [:failures] inc)
         (assoc :error new-config))
     (-> old-state
         (assoc :config new-config :failures 0)
         (dissoc :error)))))

(defn exp-wait [n max-ms]
  {:pre [(number? n) (>= n 0) (number? max-ms) (> max-ms 0)]}
  (min (* (Math/pow 2 n) 100) max-ms))

(defrecord WatchComponent [conn spec ch state options]
  component/Lifecycle
  (start [{:keys [ch] :as watch}]
    (info "starting consul watch component")
    (spy :info spec)
    (let [_          (add-watch state :logger log-changes)
          watcher-ch (watch-consul conn spec ch)
          updater-ch (async/go-loop []
                       (when-let [new-config (async/<! ch)]
                         (swap! state update-state new-config)
                         (when (attache/ex-info? new-config)
                           (warn "failure when polling consul")
                           (spy :warn new-config)
                           (async/<! (async/timeout (exp-wait (or (:failures @state) 0) (get options :max-retry-wait 5000)))))
                         (recur)))
          resp       (async/<!! ch)]
      (reset! state (update-state resp))
      (assoc watch :watcher-ch watcher-ch :updater-ch updater-ch)))
  (stop [{:keys [ch] :as watch}]
    (info "stopping consul watch component")
    (spy :info spec)
    (async/close! ch)
    watch))

(defn watch-component
  "Creates a component that watches for changes to spec and caches results in state.

  :query-params   - the query params passed into the underlying service call
  :max-retry-wait - max interval before retying consul when a failure occurs.  Defaults to 5s."
  [conn spec & {:as options}]
  (map->WatchComponent {:conn conn :spec spec :ch (async/chan) :state (atom nil) :options options}))


(defn service-registration
  "Creates a component that registers the application service with consul."
  [conn name & {:keys [port]}]
  )
