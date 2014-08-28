(ns cascalog-avro-parquet.core-test
  (:use [midje sweet cascalog])
  (:require [clojure.test :refer :all]
            [cascalog-avro-parquet.core :refer :all]
            [abracad (avro :as avro)])
  (:import  java.util.HashMap))

(defn keys-in
  "Returns an array containing the different keys and nested keys in a map"
  [m]
  (cond
   (or (map? m) (instance? HashMap m)) (->> m
                 (mapcat (fn [[k v]]
                           (let [sub (keys-in v)
                                 nested (map #(into [k] %) (filter (comp not empty?) sub))]
                             (cond
                              (seq? nested) nested
                              :else [[k]]))))
                 (into []))
   :else []))

(defn select-in
  "Returns the nested values referenced by ks in coll"
  [coll ks]
  (->> ks
       (reduce (fn [result k]
                 (assoc-in result k (get-in coll k)))
               {})))

(defn intersect-in
  "Returns a map of the common properties between two input maps s1 and s2"
  [s1 s2]
  (cond
   (nil? s1) nil
   (nil? s2) nil
   (= s1 s2) s1
   :else
   (let [ks (clojure.set/intersection (set (keys-in s1)) (set (keys-in s2)))
         ks (filter (fn [k] (= (get-in s1 k) (get-in s2 k))) ks)
         ret (select-in s1 ks)]
     ret)))


;;testing column selection
  (fact "test-projection-schema"
       (writer "parquet" schema records)
       (reader "parquet" selection)
   =>
       (produces
        [[{:z {:c 1}}]
         [{:z {:c 3}}]])
      (delete-recursively "parquet"))

  (fact "test-full-schema"
       (writer "parquet" schema records)
       (reader "parquet" schema)
   =>
       (produces
        [[{:z {:c 1, :d 2}, :b "123", :a "foo"}]
         [{:z {:c 3, :d 4}, :b "345", :a "bar"}]])
      (delete-recursively "parquet"))


;; testing avro data types (string int float double long bytes null enum array fixed map union FactSchema)
(defn default-datum
  "Function that generate a default datum for the default-schema"
  [value] {:field value})

(defn default-schema
  "Function that generate a default schema of type mtype"
  [mtype] (avro/parse-schema {:name "Node" :type :record :fields [ {:name "field" :type mtype}]}))

(def default-access
  "Default accessor to field containing the value to test"
  #(->> % (first) :field))

(defn bytes-checker
  "Checker function for data of type bytes"
  [access array]
    #((wrap-checker has)
     every?
     (fn [x] (java.util.Arrays/equals (access x) array))))

(defn map-checker
  "Checker funtion for maps"
  [access mymap]
    #((wrap-checker has)
     every?
     (fn [x]
       (let [a (access x)]
         (= (intersect-in a mymap) a)))))

(defn test-template
  "Generic function to generate a test"
  [folder datum schema checker-function]
    (let [generator (fn [] [datum])]
      (writer folder schema generator)
      (fact
        (reader folder schema) => (checker-function)
        (delete-recursively folder))))


(defn primitive-test-template [mtype value]
  "Function that generate a test on a primitive data type"
  (let [folder (name mtype)
        datum (default-datum value)
        schema (default-schema mtype)
        checker-function #(produces [[datum]])]
    (test-template folder datum schema checker-function)))

(fact "test-string"
      (primitive-test-template :string "foobar"))

  (fact "test-int"
      (primitive-test-template :int 1))

  (fact "test-float"
        (primitive-test-template :float (float 0.5)))

  (fact "test-double"
      (primitive-test-template :double (double 0.5)))

  (fact "test-long"
      (primitive-test-template :long (long 0.5)))

  (fact "test-bytes"
      (let [folder "bytes"
            value (byte-array 10)
            datum (default-datum value)
            schema (default-schema :bytes)
            checker-function (bytes-checker default-access value)]
      (test-template folder datum schema checker-function)))

  (fact "test-null"
     (let [folder "null"
           datum {:a nil :b "foobar"}
           schema (avro/parse-schema {:name "Node" :type :record :fields [{:name "a" :type :null}
                                                                           {:name "b" :type :string}]})
           checker-function #(produces [[{:b "foobar"}]])]
    (test-template folder datum schema checker-function)))

  (fact "test-enum"
        (let [folder "enum"
          datum {:field "SPADES"}
          schema (default-schema {:name "field" :type :enum :symbols ["SPADES" "HEARTS" "DIAMONDS" "CLUBS"]})
          checker-function #(produces [[datum]])]
      (test-template folder datum schema checker-function)))

  (fact "test-array"
        (let [folder "array"
              values [1 2 3 4 5]
              datum {:field values}
              schema (default-schema {:name "field" :type :array :items :int})
              checker-function #(produces [[{:field values}]])]
          (test-template folder datum schema checker-function)))

  (fact "test-fixed"
        (let [folder "fixed"
              value (byte-array 3)
              datum {:field value}
              schema (default-schema {:name "field" :type :fixed :size 3})
              checker-function (bytes-checker default-access value)]
          (test-template folder datum schema checker-function)))

  (fact "test-map"
          (let [folder "map"
            datum {:field {"a" 1 "b" 2 "c" 3}}
            schema (default-schema {:name "field" :type :map :values :int})
            checker-function (map-checker default-access {:a 1 :b 2 :c 3})]
        (test-template folder datum schema checker-function)))

  (fact "test-union"
      (let [folder "union"
            datum {:field 1}
            schema (default-schema [:int :string])
            checker-function #(produces [[datum]])]
        (test-template folder datum schema checker-function)))
