(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.os :as os]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.tests :as tests]
            [jepsen.control.util :as cu]
            [jepsen.ydb.cli.clean :refer [clean-valid-cmd]]
            [jepsen.ydb.append :as append]
            [jepsen.ydb.append-with-deletes :as append-with-deletes]))

(defn ydb-workload
  "Returns the workload"
  [opts]
  (case (:workload-name opts)
    "append" (append/workload opts)
    "append-with-deletes" (append-with-deletes/workload opts)))

(defn ydb-unhandled-exceptions
  "Returns a checker for unhandled exceptions"
  [opts]
  (let [wrapped (checker/unhandled-exceptions)]
    (if (:allow-exceptions opts)
      wrapped
      (reify checker/Checker
        (check [_this test history opts]
          (let [result (checker/check wrapped test history opts)]
            (if (and (:valid? result) (seq (:exceptions result)))
              (merge result {:valid? false})
              result)))))))

(defn ydb-test
  "Tests YDB"
  [opts]
  (let [workload (ydb-workload opts)
        db       db/noop
        os       os/noop
        packages (nc/nemesis-packages
                  {:db        db
                   :nodes     (:nodes opts)
                   :faults    (:nemesis opts)
                   :partition {:targets [:one]}
                   :pause     {:targets [:one]}
                   :kill      {:targets [:one :all]}
                   :interval  (:nemesis-interval opts)})
        ; The default nemesis-package will compose all available packages, even
        ; those that are not enabled. This includes bitflip, which downloads
        ; and installs an archive from github.com, which doesn't work in
        ; restricted networks. Filter only those that have a generator.
        needed-packages (filter
                         (fn [x] (not (= (:generator x) nil)))
                         packages)
        nemesis (nc/compose-packages needed-packages)]

    (merge tests/noop-test
           opts
           {:name "ydb"
            :db db
            :os os
            :client (:client workload)
            :nemesis (:nemesis nemesis)
            :checker (checker/compose
                      {:perf (checker/perf
                              {:nemeses (:perf nemesis)})
                       :clock (checker/clock-plot)
                       :stats (checker/stats)
                       :exceptions (ydb-unhandled-exceptions opts)
                       :workload (:checker workload)})
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn valid-probabily?
  [value]
  (and (>= value 0.0) (<= value 1.0)))

(defn valid-read-replicas?
  [value]
  (>= value 0))

(def cli-opts
  "Command line options"
  [[nil "--db-name DBNAME" "YDB database name."
    :default "/local"]

   [nil "--db-port NUM" "YDB database port."
    :default 2135
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--db-table NAME" "YDB table name to use for testing."
    :default "jepsen_test"]

   [nil "--workload-name NAME" "YDB workload name to test."
    :default "append"]

   [nil "--allow-exceptions" "Relaxes the test to allow unhandled exceptions."
    :default false]

   [nil "--partition-size-mb NUM" "YDB table partition size in MBs"
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--initial-partition-count NUM" "YDB table initial number of partitions"
    :default 30
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--initial-partition-keys NUM" "YDB table initial number of keys per partition"
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--with-read-replicas NUM" "Additional per-az read replicas for the YDB table"
    :default 0
    :parse-fn parse-long
    :validate [valid-read-replicas? "Must a 0 or greater"]]

   [nil "--with-changefeed" "Additional updates changefeed for the YDB table"
    :default false]

   [nil "--batch-single-ops" "Execute single ops using a batch query"
    :default false]

   [nil "--batch-ops-probability NUM" "Probability of batching compatibile operation with the previous one"
    :default 0.0
    :parse-fn parse-double
    :validate [valid-probabily? "Must be between 0.0 and 1.0 inclusive"]]

   [nil "--batch-commit-probability NUM" "Probability of batching commit with the last operation"
    :default 1.0
    :parse-fn parse-double
    :validate [valid-probabily? "Must be between 0.0 and 1.0 inclusive"]]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--ballast-size NUM" "Number of ballast bytes added to values"
    :default 1000
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  16
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--store-type TYPE" "Store type, can be 'row' or 'column'"
    :default "row"]])

(defn -main
  "Handles command line arguments."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn ydb-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd)
                   (clean-valid-cmd))
            args))
