(ns jepsen.ydb.debug-info
  (:require [clojure.data.json :as json]))

(def ^:dynamic *debug-info* nil)

(defmacro with-debug-info
  "Runs macro body, allowing accumulation of debug-info.
   Associates :debug-info to the result when debug-info is not empty."
  [& body]
  `(let [slot# (volatile! (transient []))
         r# (binding [*debug-info* slot#]
              (do ~@body))
         v# (persistent! @slot#)]
     (if (> (count v#) 0)
       (assoc r# :debug-info v#)
       r#)))

(defn try-parse-debug-info
  "Tries to parse debug info from a json string."
  [debug-info]
  (try
    (json/read-str debug-info
                   :key-fn #(keyword (.replace % \_ \-)))
    (catch Exception e debug-info)))

(defn add-debug-info
  "Adds debug info when running inside with-debug-info."
  [debug-info]
  (cond
    (= debug-info nil) nil
    (= debug-info ()) nil
    (= debug-info []) nil
    :else
    (let [slot *debug-info*]
      (when (not (= slot nil))
        (let [d @slot]
          (vreset! slot (conj! d debug-info)))))))
