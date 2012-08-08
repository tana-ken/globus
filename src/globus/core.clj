(ns globus.core
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clojure.pprint :as pp]
   [clojure.java.jdbc :as jdbc]
   [incanter.core :as icore]
   [incanter.charts :as icharts]
   [incanter.io :as iio]
   [incanter.datasets :as idatasets]
   [incanter.pdf :as ipdf]
   ))

(def org-dir "/Users/kenta/clojure/globus/data/org/")
(def out-dir "/Users/kenta/clojure/globus/data/out/")

(def yqset (for [y (range 5 7) q (range 1 5)] {:y y :q q}))
(defn str-num
  [i]
  (if (> 10 i)
    (str 0 i)
    i))

(def items (for [i (range 0 27)] (str "i" (str-num i))))
(def regions '("AP", "EA", "LA", "NA"))
(def ks (for [i items r regions] {:item i :rg r}))


;; need to change for year
(defn extract-numbers
  ""
  [in-file]
  (with-open [in (io/reader in-file)]
    (doall
     (take 27
       (for [line (csv/read-csv in) :when (= 17 (count line))]
         (take 8 (drop 4 line)))))))

(defn pre-transpose
  ""
  [lists]
  (into-array (for [list lists] (into-array String list))))

(defn transpose
  ""
  [in-array]
  (let [out-array (make-array String 8 27)]
    (doseq [i (range 0 27) j (range 0 8)]
      (aset #^String out-array j i (aget #^String in-array i j)))
  out-array))

(defn create-header
  ""
  [r n]
  (for [y (range 5 7) q (range 1 5)] `(~q ~y ~n ~r)))

(defn prepare
  ""
  [filename]
  (for [line (transpose
              (pre-transpose
               (extract-numbers filename)))] (seq line)))

(defn write-csv
  ""
  [lists]
  (with-open [out (io/writer (str out-dir "test.txt") :append true)]
    (csv/write-csv out lists)))

(defn main-a
  ""
  [filename r n]
  (write-csv (map into (prepare filename) (create-header r n))))

(defn control-a
  ""
  [in-dir]
  (doseq [f (.listFiles (io/file in-dir))]
    (let [[_ r n] (re-matches #"CIR_CompanyAnalysis_(..)_Y\d_([A-H])_Company.csv" (.getName f))] (main-a (.getPath f) r n))))

(def h2 {:subprotocol "h2"
         :subname "/Users/kenta/h2/test"
         :user "sa"
         :password ""
         :classname "org.h2.Driver"
          })

(defn db-select
  ""
  [item y q]
  (jdbc/with-connection h2
    (jdbc/with-query-results rows [(str
     " SELECT
           rg, cp, " item
     " FROM
           CSVREAD('/Users/kenta/clojure/globus/data/out/test.txt')
       WHERE y =" y "and q = " q
      " ORDER BY rg, cp;")] (doall rows))))


(defn control-b
  ""
  [yqs items]
  (for [yq yqs]
    (cons (str (+ (yq :y) (* 0.25 (- (yq :q) 1))))(flatten (for [item items] (map #((keyword item) %) (db-select item (:y yq) (:q yq))))))))

(def mid (control-b yqset items))

(defn out-2
  ""
  [out-data]
  (with-open [out (io/writer (str out-dir "sum.csv") :append true)]
    (csv/write-csv out out-data)))

(defn split-file
  ""
  [in-file]
  (with-open [in (io/reader in-file)]
    (doall
      (for [line (csv/read-csv in)]
        (take 8 (drop 1 line))))))

;(def control-c
;  ""
;  (icore/col-names 
;    (icore/conj-cols
;     (icore/to-dataset (range 5 7 0.25))
;     (icore/to-dataset (split-file (str out-dir "sum.csv"))))
;  '("Period", "A", "B", "C", "D", "E", "F", "G", "H")))

(defn get-data
  ""
  [item rg]
  (jdbc/with-connection h2
    (jdbc/with-query-results rows [(str
     " SELECT 
           y, q, cp, " item " as item"
     " FROM
           CSVREAD('/Users/kenta/clojure/globus/data/out/test.txt')
       WHERE rg = '" rg "'
       ORDER BY cp, y, q;")] (doall rows))))

(defn calc-time
  [{y :y q :q}]
  (+ (read-string y) (* 0.25 (- (read-string q) 1))))


(defn translate-data
  ""
  [lines]
  ;; (for
  (for [line lines] [(line :cp) (calc-time line) (line :item)]))


(defn out-3
  ""
  [item rg out-data]
  (with-open [out (io/writer (str out-dir  item rg ".csv") :append false)]
    (csv/write-csv out out-data)))

(defn control-d
  ""
  []
  (doseq [{item :item rg :rg} ks]
    (out-3 item rg (translate-data (get-data item rg)))))

(defn load-dataset-with-col-names
  [item rg]
  (icore/col-names
   (iio/read-dataset (str out-dir item rg ".csv"):header false)
   '("C" "X" "Y")))

(defn control-e
  ""
  []
  (doseq [{item :item rg :rg} ks]
    (ipdf/save-pdf
     (icharts/scatter-plot
      :X
      :Y
      :data(load-dataset-with-col-names item rg)
      :group-by :C
      :legend false
      :title (str item "_" rg)
      :x-label nil
      :y-label nil)
     (str out-dir item "_" rg ".pdf")
     :width 400
     :height 200)))
