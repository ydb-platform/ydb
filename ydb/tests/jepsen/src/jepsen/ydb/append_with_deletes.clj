(ns jepsen.ydb.append-with-deletes
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.util ArrayList)
           (com.google.protobuf ByteString)
           (tech.ydb.core StatusCode)
           (tech.ydb.table.query Params)
           (tech.ydb.table.values PrimitiveValue)
           (tech.ydb.table.values StructValue)
           (tech.ydb.table.values ListValue)
           (tech.ydb.table.values Value)))

(def ^:dynamic *ballast* (ByteString/copyFromUtf8 ""))

(defn new-ballast
  "Creates a new ballast value with size bytes"
  [size]
  (ByteString/copyFromUtf8 (.repeat "x" size)))

(defmacro with-ballast
  "Runs body with *ballast* bound to the specified ballast value"
  [ballast & body]
  `(binding [*ballast* ~ballast]
     (do ~@body)))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defn drop-initial-tables-post-24-1
  [test table-client]
  (info "dropping initial tables")
  (conn/with-session [session table-client]
    (let [query (format "DROP TABLE IF EXISTS `%1$s`;" (:db-table test))]
      (conn/execute-scheme! session query))))

(defn is-issue-db-path-not-found?
  [issue]
  (let [message (.getMessage issue)]
    (or (.contains message "Path does not exist")
        (some is-issue-db-path-not-found? (.getIssues issue)))))

(defn is-status-db-path-not-found?
  [status]
  (if (= StatusCode/SCHEME_ERROR (.getCode status))
    (some is-issue-db-path-not-found? (.getIssues status))
    nil))

(defn db-table-exists
  [test session table-name]
  (let [table-path (str (:db-name test) "/" table-name)
        result (-> session
                   (.describeTable table-path)
                   .join)
        status (.getStatus result)]
    (when-not (is-status-db-path-not-found? status)
      (.expectSuccess status))))

(defn db-drop-table-if-exists
  [test session table-name]
  (let [table-path (str (:db-name test) "/" table-name)
        status (-> session
                   (.dropTable table-path)
                   .join)]
    (when-not (is-status-db-path-not-found? status)
      (.expectSuccess status))))

(defn drop-initial-tables
  [test table-client]
  (info "dropping initial tables")
  (conn/with-session [session table-client]
    (db-drop-table-if-exists test session (:db-table test))))

(defn generate-partition-at-keys
  "Generates an optional PARTITION_AT_KEYS fragment with a list of partitioning keys"
  [test]
  (let [keys (:initial-partition-keys test)
        count (:initial-partition-count test)]
    (if (> count 1)
      (format "PARTITION_AT_KEYS = (%s),"
              (->> (iterate inc 1) ; 1, 2, 3, ...
                   (map #(+ (* % keys) 1)) ; 11, 21, 31, ...
                   (take (dec count))
                   (str/join ", ")))
      "")))

(defn generate-read-replicas-settings
  "Generates an optional READ_REPLICAS_SETTINGS fragment"
  [test]
  (let [count (:with-read-replicas test)]
    (if (> count 0)
      (format "READ_REPLICAS_SETTINGS = \"PER_AZ:%s\"," count)
      "")))

(defn create-initial-tables
  [test table-client]
  (info "creating initial tables")
  (conn/with-session [session table-client]
    (let [query (format "CREATE TABLE `%1$s` (
                             key Int64 NOT NULL,
                             version Int64 NOT NULL,
                             index Int64 NOT NULL,
                             value Int64,
                             ballast string,
                             PRIMARY KEY (key, version, index))
                         WITH (%3$s
                               %4$s
                               STORE = %5$s,
                               AUTO_PARTITIONING_BY_SIZE = ENABLED,
                               AUTO_PARTITIONING_BY_LOAD = ENABLED,
                               AUTO_PARTITIONING_PARTITION_SIZE_MB = %2$d);"
                        (:db-table test)
                        (:partition-size-mb test)
                        (generate-partition-at-keys test)
                        (generate-read-replicas-settings test)
                        (:store-type test))]
      (conn/execute-scheme! session query)
      (when (:with-changefeed test)
        (let [query (format "ALTER TABLE `%1$s`
                             ADD CHANGEFEED `updates_feed`
                             WITH (FORMAT = 'JSON',
                                   MODE = 'UPDATES',
                                   TOPIC_MIN_ACTIVE_PARTITIONS = 1,
                                   VIRTUAL_TIMESTAMPS = TRUE,
                                   RESOLVED_TIMESTAMPS = Interval('PT1S'))"
                            (:db-table test))]
          (conn/execute-scheme! session query))))))

(defn list-read-query
  [test]
  (format "DECLARE $key AS Int64;
           SELECT index, value FROM `%1$s`
           WHERE key = $key
           ORDER BY index"
          (:db-table test)))

(defn parse-list-read-result
  "Parses a single ResultSet of a read into a vec of values"
  [rs k]
  (let [result (ArrayList.)]
    (while (. rs next)
      (let [expectedIndex (.size result)
            index (-> rs (.getColumn 0) .getInt64)
            value (-> rs (.getColumn 1) .getInt64)]
        (assert (<= expectedIndex index)
                (format "List %s indexes are not distinct or not ordered correctly (index# %s expected# %s)"
                        k index expectedIndex))
        ; In the unlikely case some indexes are missing fill those with nils
        (when (< expectedIndex index)
          (dotimes [_ (- index expectedIndex)]
            (. result add nil)))
        (. result add value)))
    (when (.isTruncated rs)
      (. result add :truncated))
    (vec result)))

(defn execute-list-read
  "Executes a list read for the given key k.
   Works only when the list has at most 1k values."
  [test tx k]
  (let [query (list-read-query test)
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        query-result (conn/execute! tx query params)]
    (parse-list-read-result (. query-result getResultSet 0) k)))

(defn list-append-query
  [test]
  (format "DECLARE $key AS Int64;
           DECLARE $value AS Int64;
           DECLARE $ballast AS Bytes;
           $prev_state = (SELECT version, index, value FROM `%1$s` WHERE key = $key);
           $prev_version = (SELECT COALESCE(MAX(version), 0) FROM $prev_state);
           $next_version = $prev_version + 1;
           $next_index = (SELECT COALESCE(MAX(index) + 1, 0) FROM $prev_state);
           DELETE FROM `%1$s` WHERE key = $key AND version = $prev_version;
           UPSERT INTO `%1$s`
               SELECT $key AS key, $next_version AS version, index, value, $ballast AS ballast FROM $prev_state
               UNION ALL
               SELECT $key AS key, $next_version AS version, $next_index AS index, $value AS value, $ballast AS ballast;
           -- redundant read forces uncommitted changes to be flushed
           SELECT COUNT(*) FROM `%1$s` WHERE key = $key;"
          (:db-table test)))

(defn execute-list-append
  [test tx k v]
  (let [query (list-append-query test)
        params (Params/of "$key" (PrimitiveValue/newInt64 k)
                          "$value" (PrimitiveValue/newInt64 v)
                          "$ballast" (PrimitiveValue/newBytes *ballast*))]
    (conn/execute! tx query params)))

(defn batch-commit-last
  "Wraps the last micro op into [:commit nil mop] based on configured probability."
  ([test]
   (let [probability (:batch-commit-probability test 1.0)]
     (fn [rf]
       (let [last (volatile! ::none)]
         (fn
           ([] (rf))
           ([result]
            (let [final @last
                  _ (vreset! last ::none)
                  result (if (identical? final ::none)
                           result
                           ; Push a wrapped final value
                           (let [final (if (< (rand) probability)
                                         [:commit nil final]
                                         final)]
                             (unreduced (rf result final))))]
              (rf result)))
           ([result op]
            (let [prev @last
                  _ (vreset! last ::none)]
              (if (identical? prev ::none)
                (do
                  (vreset! last op)
                  result)
                (let [result (rf result prev)]
                  (when-not (reduced? result)
                    (vreset! last op))
                  result)))))))))
  ([test coll]
   (sequence (batch-commit-last test) coll)))

(defn apply-mop!
  [test tx [f k v :as mop]]
  (case f
    :r [[f k (execute-list-read test tx k)]]
    :append [(do
               (execute-list-append test tx k v)
               mop)]
    :commit (do
              (conn/auto-commit! tx)
              (apply-mop! test tx v))))

(defrecord Client [transport table-client ballast setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          table-client (conn/open-table-client transport)]
      (assoc this :transport transport :table-client table-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-tables test table-client)
     (create-initial-tables test table-client)))

  (invoke! [_ test op]
    ;; (info "processing op:" op)
    (with-ballast ballast
      (debug-info/with-debug-info
        (conn/with-errors op
          (conn/with-session [session table-client]
            (conn/with-transaction [tx session]
              (let [txn (:value op)
                    ; modified transaction we are going to execute
                    txn' (->> txn
                              (batch-commit-last test)
                              (into []))
                    op' (if (not= txn txn') (assoc op :modified-txn txn') op)
                    ; execute modified transaction and gather results
                    txn'' (->> txn'
                               (mapcat (partial apply-mop! test tx))
                               (into []))
                    op'' (assoc op' :type :ok, :value txn'')]
                op'')))))))

  (teardown! [this test])

  (close! [this test]
    (.close table-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil (new-ballast (:ballast-size opts)) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [:ydb-serializable])))
      (assoc :client (new-client opts))))
