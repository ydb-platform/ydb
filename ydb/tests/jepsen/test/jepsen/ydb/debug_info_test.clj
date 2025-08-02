(ns jepsen.ydb.debug-info-test
  (:require [clojure.test :refer :all]
            [jepsen.ydb.debug-info :as debug-info]))

(deftest try-parse-debug-info
  (testing "Parsing single debug info"
    (is (=
         (debug-info/try-parse-debug-info "{\"tablet\":123,\"op\":\"read\",\"read_id\":456}")
         {:tablet 123, :op "read", :read-id 456}))))

(deftest add-debug-info-outside-macro
  (testing "Trying to add debug info outside the macro"
    (debug-info/add-debug-info {:op "read", :read-id 123})))

(deftest with-debug-info
  (testing "Using macro without debug info"
    (is (= (debug-info/with-debug-info
             (debug-info/add-debug-info nil)
             (debug-info/add-debug-info [])
             {:f :txn, :type :ok})
           {:f :txn, :type :ok})))
  (testing "Using macro with real debug info"
    (is (= (debug-info/with-debug-info
             (debug-info/add-debug-info {:op "read", :read-id 123})
             (debug-info/add-debug-info {:op "read", :read-id 456})
             {:f :txn, :type :ok})
           {:f :txn, :type :ok, :debug-info [{:op "read", :read-id 123}
                                             {:op "read", :read-id 456}]}))))
