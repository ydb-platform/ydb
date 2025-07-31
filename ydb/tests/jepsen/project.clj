(defproject jepsen.ydb "0.1.0-SNAPSHOT"
  :description "A Jepsen test for YDB"
  :url "https://ydb.tech"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "2.5.0"]
                 [jepsen "0.3.5"]
                 [tech.ydb/ydb-sdk-scheme "2.1.11"]
                 [tech.ydb/ydb-sdk-table "2.1.11"]]
  :profiles {:uberjar {:aot :all}}
  :main jepsen.ydb
  :jvm-opts ["-Djava.awt.headless=true"
             ;"-Djava.net.preferIPv6Addresses=true"
             "-server"]
  :repl-options {:init-ns jepsen.ydb})
