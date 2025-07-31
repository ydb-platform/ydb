(ns jepsen.ydb.append-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.append :as append]))

(defn- make-batch
  [test ops]
  (append/batch-operations test ops))

(deftest batch-compatible-operations
  (testing "Empty list of operations"
    (is (= [] (make-batch {} []))))
  (testing "A single read operation"
    (let [ops [[:r 1 nil]]
          expected [[:batch nil ops]]]
      (is (= expected (make-batch {} ops)))))
  (testing "A single append operation"
    (let [ops [[:append 1 42]]
          expected [[:batch nil ops]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Multiple operations"
    (let [ops [[:append 1 42] [:r 1 nil] [:append 2 51]]
          expected [[:batch nil ops]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Multiple operations, not combined"
    (let [ops [[:append 1 42] [:r 1 nil] [:append 2 51]]
          expected [[:batch nil [[:append 1 42]]]
                    [:batch nil [[:r 1 nil]]]
                    [:batch nil [[:append 2 51]]]]]
      (is (= expected (make-batch {:batch-ops-probability 0.0} ops)))))
  (testing "Multiple operations, not combined and original ops"
    (let [ops [[:append 1 42] [:r 1 nil] [:append 2 51]]
          expected ops]
      (is (= expected (make-batch {:batch-ops-probability 0.0 :batch-single-ops false} ops))))))

(defn- with-commit-last
  [test ops & xforms]
  (into [] (apply comp (append/batch-commit-last test) xforms) ops))

(deftest batch-commit-last
  (testing "Empty list of operations"
    (is (= [] (with-commit-last {} []))))
  (testing "A single operation"
    (let [ops [[:r 1 nil]]
          expected [[:commit nil [:r 1 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Two operations"
    (let [ops [[:r 1 nil] [:r 2 nil]]
          expected [[:r 1 nil] [:commit nil [:r 2 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Multiple operations"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:commit nil [:r 4 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Multiple operations, take 3"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil] [:r 3 nil]]]
      (is (= expected (with-commit-last {} ops (take 3))))))
  (testing "Multiple operations, take 2"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil]]]
      (is (= expected (with-commit-last {} ops (take 2))))))
  (testing "Multiple operations, no :commit due to low probability"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected ops]
      (is (= expected (with-commit-last {:batch-commit-probability 0.0} ops))))))

(deftest mops-to-multi-ops
  (testing "Multiple same-key appends with read in-between"
    (let [ops [[:append 1 10]
               [:append 1 20]
               [:r 1 nil]
               [:r 2 nil]
               [:append 1 30]
               [:append 2 40]]
          expected [[:append [[1 0 10] [1 1 20]]]
                    [:r 1]
                    [:r 2]
                    [:append [[1 0 30] [2 0 40]]]]
          [multi-ops rs-map] (append/mops->multi-ops ops)]
      (is (= expected multi-ops))
      (is (= [nil nil 0 1 nil nil] rs-map)))))
