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
(def prcs-dir "/Users/kenta/clojure/globus/data/prcs/")
(def out-dir "/Users/kenta/clojure/globus/data/out/")

; when year 8 finished, it will be changed
(def yqs (for [y (range 5 7) q (range 1 5)] {:y y :q q}))

(defn zero-fill
  "if it is 1 digit, number is made to 2 digit by 0"
  [i]
  (if (> 10 i)
    (str 0 i)
    i))

(def regions '("AP", "EA", "LA", "NA"))

(defn read-csv
  "read csv data"
  [in-file]
  (with-open [in (io/reader in-file)]
    (doall (csv/read-csv in))))

(defn write-csv
  "write csv data"
  [out-file lists]
  (with-open [out (io/writer out-file :append true)]
    (csv/write-csv out lists)))

(defn into-2d-str-array
  "make a 2 dimention Stirng array"
  [lists]
  (into-array (for [list lists] (into-array String list))))

(defn transpose-2d-str-array
  "return transposed 2 dimention String array"
  [in-array]
  (let [x (alength in-array)
        y (alength (aget #^String in-array 0))
        out-array (make-array String y x)]
    (doseq [i (range 0 x) j (range 0 y)]
      (aset #^String out-array j i (aget #^String in-array i j)))
  out-array))

(def h2 {:subprotocol "h2"
         :subname "/Users/kenta/h2/globus"
         :user "sa"
         :password ""
         :classname "org.h2.Driver"
          })

(defn select-from-h2
  ""
  [sql]
  (jdbc/with-connection h2
    (jdbc/with-query-results
      rows
      [sql]
      (doall rows))))

(defn get-value
  ""
  [line t d]
  (take t (drop d line)))

;;; for cir report
(def cir-file "cir.csv")
(def cir-items (for [i (range 0 27)] (str "i" (zero-fill i))))
(def cir-keys (for [i cir-items r regions] {:item i :rg r}))

;; to merge cir files
(defn extract-cir-numbers
  "extract numbers from original cir report"
  [lists]
  (take 27
        (for [line lists :when (= 17 (count line))]
         ; need to change for year
          (get-value 8 4))))

(defn create-cir-header
  "return list that supplement the lack of information in cir file"
  [rg cp]
  (for [y (range 5 7) q (range 1 5)] `(~q ~y ~rg ~cp)))

(defn get-cir-in-data
  "get cir data in proper format"
  [file-path]
  (for [line (-> (read-csv file-path)
                 (extract-cir-numbers ,)
                 (into-2d-str-array ,)
                 (transpose-2d-str-array ,))]
    (seq line)))

(defn cir-in-process
  "write lines of data in each cir file"
  [file-path rg cp]
  (write-csv
   (str prcs-dir cir-file)
   (map into (get-cir-in-data file-path) (create-cir-header rg cp))))

(defn generate-merged-cir-data
  ""
  []
  (doseq [f (.listFiles (io/file org-dir))]
    (let [[_ rg cp]
          (re-matches
           #"CIR_CompanyAnalysis_(..)_Y\d_([A-H])_Company.csv"
           (.getName f))]
      (cir-in-process (.getPath f) rg cp))))

;; to draw charts of cir items
(defn create-cir-query
  [item rg]
  "create query string for cir"
  (str
   " SELECT 
         y, q, cp, " item " as item"
   " FROM
         CSVREAD('" out-dir cir-file "')
     WHERE rg = '" rg "'
     ORDER BY cp, y, q;"))

(defn select-cir-data
  "select cir data from cir table"
  [item rg]
  (select-from-h2 (create-cir-query item rg)))

(defn calc-time
  "calculate time from year and quater"
  [{y :y q :q}]
  (+ (read-string y) (* 0.25 (- (read-string q) 1))))

(defn get-cir-out-data
  "get cir our data"
  [lines]
  (for [line lines] [(line :cp) (calc-time line) (line :item)]))

(defn cir-out-process
  ""
  [item rg]
  (write-csv
   (str prcs-dir item rg ".csv")
   (-> (select-cir-data item rg)
       (get-cir-out-data ,))))

(defn generate-splited-cir-data
  ""
  []
  (doseq [{item :item rg :rg} cir-keys]
    (cir-out-process item rg)))

(defn load-dataset-with-col-names
  [item rg]
  (icore/col-names
   (iio/read-dataset (str prcs-dir item rg ".csv"):header false)
   '("C" "X" "Y")))

(defn generate-cir-charts
  ""
  []
  (doseq [{item :item rg :rg} cir-keys]
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

;;; gsr
(def gsr-file "gsr.csv")
(def gsr-items (for [i (range 0 35)] (str "g" (zero-fill i))))

(defn extract-gsr-numbers
  ""
  [lines]
  (for [line2 (for [line1 lines :when (< 5 (count line1))] line1)
        :when (not (= "" (nth line2 5)))]
    (get-value line2 8 4)))

(def testfile (str org-dir  "GSR_Y6.csv"))

(defn get-gsr-in-data
  ""
  [file-path]
  (for [line (-> (read-csv file-path)
             (extract-gsr-numbers ,)
             (into-2d-str-array ,)
             (transpose-2d-str-array ,))]
    (seq line)))

(defn gsr-in-process
  ""
  [file-path year]
  (write-csv
   (str prcs-dir gsr-file)
   (for [line (get-gsr-in-data file-path)] (conj line year))))
;;
(def p (idatasets/get-dataset :airline-passengers))
(def sb (icharts/stacked-bar-chart :year :passengers :data p :group-by :month :legend true))