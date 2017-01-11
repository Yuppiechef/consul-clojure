(defproject yuppiechef/consul-clojure "0.5.6"
  :description "A Consul client for Clojure applications."
  :url "http://github.com/yuppiechef/consul-clojure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14" :scope "provided"]
                 [org.clojure/core.async "0.2.395" :scope "provided"]
                 [camel-snake-kebab "0.4.0"]
                 [cheshire "5.6.3"]
                 [clj-http-lite "0.3.0"]]
  :profiles {:1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}})
