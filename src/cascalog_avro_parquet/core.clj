(ns cascalog-avro-parquet.core
  (:gen-class)
  (:use cascalog.api)
  (:require [abracad (avro :as avro)])
  (:import [cascading.avro HMAvroParquetScheme]))

(def complex-schema (avro/parse-schema
                     {:name "Complex"
                      :type :record
                      :fields [

                               {:name "enum"
                                :type {
                                       :name "Suit"
                                       :type :enum
                                       :symbols ["SPADES" "HEARTS" "DIAMONDS" "CLUBS"]}}
                               {:name "array"
                                :type {
                                       :name "attributes"
                                       :type :array
                                       :items :string}}
                               {:name "map"
                                :type {
                                       :name "values"
                                       :type :map
                                       :values :int
                                       }}
                               {:name "fixed"
                                :type {
                                       :name "md5"
                                       :type :fixed
                                       :size 16
                                       }}
                               {:name "union"
                                :type [:int :string]}]}))

(def complex-record
  [{:enum :SPADES :array ["blue" "red" "green"] :map {:a 1 :b 2 :c 3} :fixed (byte-array 16) :union "string"}])


(def simpsons-schema (avro/parse-schema
             {:name "Simpson-Characters"
              :type :record
              :fields [{:name "firstname" :type :string}
                       {:name "lastname" :type :string}
                       {:name "age" :type :int}]}))

(def simpsons-firstname-projection-schema (avro/parse-schema
                {:name "People"
                 :type :record
                 :fields [{:name "firstname" :type :string}]}))

(def simpsons-records [{:firstname "Bart" :lastname "Simpson" :age 10}
             {:firstname "Homer" :lastname "Simpson" :age 45}
             {:firstname "Marge" :lastname "Simpson" :age 45}
             {:firstname "Lisa" :lastname "Simpson" :age 8}
             {:firstname "Maggie" :lastname "Simpson" :age 1}])

(defn delete-recursively [fname]
  (try
    (let [func (fn [func f]
                 (when (.isDirectory f)
                   (doseq [f2 (.listFiles f)]
                     (func func f2)))
                 (clojure.java.io/delete-file f))]
      (func func (clojure.java.io/file fname)))
    (catch Exception e#)))

(defn io-parquet
  [dir schema]
  (let [ps (HMAvroParquetScheme. schema)]
    (lfs-tap ps dir :sinkmode :replace)))

(defn writer "Function that writes records to parquet"
  [output schema generator]
  (delete-recursively output)
  (?<- (io-parquet output schema)
       [?datum]
       (generator :> ?datum)))

(defn reader "Function that reads data from parquet"
  [src schema]
  (<- [?datum]
      ((io-parquet src schema) :> ?datum)))

(defn -main "This will write the parquet data and then read it to stdout"
  []
  (writer "parquet" simpsons-schema simpsons-records)
  (?- (stdout) (reader "parquet" simpsons-schema))
  (delete-recursively "parquet"))




