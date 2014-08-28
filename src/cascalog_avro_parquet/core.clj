(ns cascalog-avro-parquet.core
  (:gen-class)
  (:use cascalog.api)
  (:require [abracad (avro :as avro)])
  (:import [cascading.avro HMAvroParquetScheme]
           cascading.tap.hadoop.Hfs))

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

(defn complex-record []
  [{:enum :SPADES :array ["blue" "red" "green"] :map {:a 1 :b 2 :c 3} :fixed (byte-array 16) :union "string"}])


(def schema (avro/parse-schema
             {:name "Node2"
              :type :record
              :fields [{:name "c"
                        :type :int}
                       {:name "d"
                        :type :int}]}
             {:name "Node"
              :type :record
              :fields [
                       {:name "a"
                        :type :string}
                       {:name "b"
                        :type :string}
                       {:name "z"
                        :type "Node2"}]}))

(def selection (avro/parse-schema
                {:name "Node"
                 :type :record
                 :fields [{:name "z"
                           :type {
                                  :name "Node2"
                                  :type :record
                                  :fields [{:name "c" :type :int }]}}]}))

(defn records []
  [{:a "foo"
    :b "123"
    :z {:c 1 :d 2}}
   {:a "bar"
    :b "345"
    :z {:c 3 :d 4}}])

(defn delete-recursively [fname]
  (try
    (let [func (fn [func f]
                 (when (.isDirectory f)
                   (doseq [f2 (.listFiles f)]
                     (func func f2)))
                 (clojure.java.io/delete-file f))]
      (func func (clojure.java.io/file fname)))
    (catch Exception e#)))

(defn sink-parquet-clj "Create a cascading sink for parquet that takes PersistentArrayMap for input"
  [output schema]
  (let [ps (HMAvroParquetScheme. schema)]
    (Hfs. ps output)))

(defn tap-parquet-clj "Creates a cascading parquet tap that outputs PersistentArrayMap"
  [input selection]
  (let [ps (HMAvroParquetScheme. selection)]
    (Hfs. ps input)))

(defn writer "Function that writes records to parquet"
  [output schema generator]
  (delete-recursively output)
  (?<- (sink-parquet-clj output schema)
       [?record]
       ((generator) :> ?record)))

(defn reader "Function that reads data from parquet"
  [input selection]
  (<- [?output]
      ((tap-parquet-clj input selection) :> ?output)
     ))

(defn -main "This will write the parquet data and then read it to stdout"
  []
  (writer "parquet" schema records)
  (?- (stdout) (reader "parquet" schema))
  (delete-recursively "parquet"))




