(defproject cascalog-avro-parquet "0.1.0-SNAPSHOT"
  :description "Cascading tap/sink for parquet that uses clojure data structures"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.damballa/abracad "0.4.9"]
                 [org.apache.avro/avro "1.7.6"]
                 [cascalog "2.1.0"]
                 [com.twitter/parquet-avro "1.4.3"]
                 [com.twitter/parquet-cascading "1.4.3"]
                 [cascading/cascading-core "2.5.4"]
                 [org.apache.avro/avro-mapred "1.7.6"]
                 [org.apache.avro/avro-compiler "1.7.6"]
                 [org.apache.hadoop/hadoop-core "1.2.1"]
                 [jackknife "0.1.7"]
                 [criterium "0.4.3"]
                 [org.apache.avro/trevni-avro "1.7.6"]
                 [midje "1.6.3"]]
  :dev-dependencies [[cascalog/midje-cascalog "2.1.0"]
                     ;[com.cemerick/pomegranate "0.3.0"]
                     ]
  :repositories [["java.net" "http://download.java.net/maven/2"]
                 ["sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                              :snapshots false
                              :checksum :fail
                              :update :always
                              :releases {:checksum :fail :update :always}}]
                 ["conjars" "http://conjars.org/repo"]
                 ["twttr" "http://maven.twttr.com"]]
  :main ^:skip-aot cascalog-avro-parquet.core
  :target-path "target/%s"
  :java-source-paths ["src/java"]
  :profiles {:uberjar {:aot :all}})
