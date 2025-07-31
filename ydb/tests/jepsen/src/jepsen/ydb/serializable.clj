(ns jepsen.ydb.serializable
  (:require [elle.core :as elle]
            [elle.list-append :as a]
            [jepsen.history :as h]
            [jepsen.checker :as checker]
            [bifurcan-clj [core :as b]]
            [elle.graph :as g]
            [elle.rels :as rels :refer [ww wr rw process realtime]]
            [dom-top.core :refer [loopr]])
  (:import (io.lacuna.bifurcan ISet
                               Set
                               IMap
                               Map)
           (jepsen.history Op)
           (elle.core RealtimeExplainer)))

(defn mop-key
  [[f k v :as mop]]
  (case f
    :r k
    :append k))

(defn op-keys
  [op]
  (apply hash-set (map mop-key (:value op))))

(defn link-per-key-oks
  [^IMap alloks g op op']
  (reduce
   ; Iterate for each affected key k in op
   ; When a given key has oks set we link them to completion op'
   (fn [g k]
     (let [oks (.get alloks k nil)]
       (if (nil? oks)
         g
         (g/link-all-to g oks op' realtime))))
   g (op-keys op)))

(defn ^ISet oks-without-implied
  [^ISet oks g op]
  (if (nil? oks)
    ; Create a new empty set
    (.linear (Set.))
    ; Find and remove implied operations
    (let [implied (g/in g op)]
      (if implied
        (.difference oks implied)
        oks))))

(defn add-per-key-oks
  [^IMap alloks g op]
  (reduce
   ; Iterate for each affected key k in op
   ; Adds op to oks buffer for each key, removing implied ops
   (fn [alloks k]
     (let [oks (.get alloks k nil)
           oks (oks-without-implied oks g op)
           oks (.add oks op)]
       (.put alloks k oks)))
   alloks (op-keys op)))

(defn ydb-realtime-graph
  "A modification of realtime-graph from elle, where only per-key realtime
   edges are added. This corresponds to per-key linearizability."
  [history]
  (loopr [^IMap alloks (.linear (Map.)) ; Our buffer of completed ops for each key
          g (b/linear (g/op-digraph))] ; Our order graph
         [op history :via :reduce]
         (case (:type op)
           ; A new operation begins! Link every completed op to this one's
           ; completion. Note that we generate edges here regardless of whether
           ; this op will fail or crash--we might not actually NEED edges to
           ; failures, but I don't think they'll hurt. We *do* need edges to
           ; crashed ops, because they may complete later on.
           :invoke ; NB: we might get a partial history without completions
           (if-let [op' (h/completion history op)]
             (recur alloks (link-per-key-oks alloks g op op'))
             (recur alloks g))

           ; An operation has completed. Add it to the oks buffer, and remove
           ; oks that this ok implies must have completed.
           :ok
           (let [alloks (add-per-key-oks alloks g op)]
             (recur alloks g))
           ; An operation that failed doesn't affect anything--we don't generate
           ; dependencies on failed transactions because they didn't happen. I
           ; mean we COULD, but it doesn't seem useful.
           :fail (recur alloks g)
           ; Crashed operations, likewise: nothing can follow a crashed op, so
           ; we don't need to add them to the ok set.
           :info (recur alloks g))
         ; All done!
         {:graph     (b/forked g)
          :explainer (RealtimeExplainer. history)}))

(defmacro with-ydb-realtime-graph
  [& body]
  `(with-redefs [elle/realtime-graph ydb-realtime-graph]
     (do ~@body)))

(defn replace-ydb-serializable-model
  "Searches for :ydb-serializable consistency model and replaces it with :strict-serializable when found.
   Returns (opts replaced) tuple"
  [opts]
  (let [models (:consistency-models opts)
        models-set (apply hash-set models)]
    (if (contains? models-set :ydb-serializable)
      (do
        (when (contains? models-set :strict-serializable)
          (throw (RuntimeException. ":ydb-serializable cannot be mixed with :strict-serializable")))
        (let [models (mapv (fn [model]
                             (if (= model :ydb-serializable)
                               :strict-serializable
                               model)) models)]
          (list (assoc opts :consistency-models models) true)))
      (list opts false))))

(def append-check-orig a/check)

(defn append-check
  "Modified append/check with support for :ydb-serializable consistency model."
  ([history]
   (append-check-orig history))
  ([opts history]
   (let [[opts replaced] (replace-ydb-serializable-model opts)]
     (if replaced
       (with-ydb-realtime-graph
         (append-check-orig opts history))
       (append-check-orig opts history)))))

(defmacro with-ydb-serializable
  [& body]
  `(with-redefs [a/check append-check]
     (do ~@body)))

(defn wrap-checker
  "Wraps an existing jepsen checker with support for :ydb-serializable consistency model."
  [wrapped]
  (reify checker/Checker
    (check [this test history checker-opts]
      (with-ydb-serializable
        (checker/check wrapped test history checker-opts)))))

(defn wrap-test
  "Wraps a jepsen test with support for :ydb-serializable consistency model."
  [test]
  (assoc test :checker (wrap-checker (:checker test))))
