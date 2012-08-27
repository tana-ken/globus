(ns globus.core
  (:require
;   [clj-pdf.core :as pdf]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
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
(def yqs (for [y (range 5 8) q (range 1 5)] {:y y :q q}))

(defn zero-fill
  "if it is 1 digit, number is made to 2 digit by 0"
  [i]
  (if (> 10 i)
    (str 0 i)
    i))

(def regions '("AP", "EA", "LA", "NA"))
(def cps '("A" "B" "C" "D" "E" "F" "G" "H"))


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
  (into-array (for [list lists] (into-array String (map str list)))))

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
(def cir-names '(
  "Common Retail Dealers Chains"
  "Common Retail Dealers Online"
  "Common Retail Dealers Shops"
  "Common Tech Support Budget"
  "Common Advertising Budget"
  "Common Warranty Period Entry-L"
  "Common Warranty Period Multi-F"
  "Entry-L Average Price"
  "Entry-L Promotions Number"
  "Entry-L Promotions Length"
  "Entry-L Promotions Discount"
  "Entry-L Special Utility Features"
  "Entry-L Number of Models"
  "Entry-L P/Q Rating"
  "Entry-L Demand"
  "Entry-L Market Share"
  "Entry-L Stockout (yes/no)"
  "Multi-F Average Price"
  "Multi-F Promotions Number"
  "Multi-F Promotions Length"
  "Multi-F Promotions Discount"
  "Multi-F Special Utility Features"
  "Multi-F Number of Models"
  "Multi-F P/Q Rating"
  "Multi-F Demand"
  "Multi-F Market Share"
  "Multi-F Stockout (yes/no)"))

(def cir-hash (apply hash-map (interleave cir-items cir-names)))

(def cir-keys (for [i cir-items r regions] {:item i :rg r}))

;; to merge cir files
(defn extract-cir-numbers
  "extract numbers from original cir report"
  [lists]
  (take 27
        (for [line lists :when (= 17 (count line))]
         ; need to change for year
          (get-value line 12 4))))

(defn create-cir-header
  "return list that supplement the lack of information in cir file"
  [rg cp]
  (for [y (range 5 8) q (range 1 5)] `(~q ~y ~rg ~cp)))

(defn get-cir-in-data
  "get cir data in proper format"
  ;need file filter
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
  ;; match mechanism did not work well
  []
  (doseq [f (.listFiles (io/file (str org-dir "/cir")))]
    (let [[_ rg cp]
          (re-matches
           #"CIR_CompanyAnalysis_(..)_Y\d_([A-H])_Company.csv"
           (.getName f))]
      (cir-in-process (.getPath f) rg cp))))

;; add ave
(def asql (str
           " SELECT 
                  'AVG' as cp, rg, y, q" (apply str (for [i cir-items] (str ", AVG(cast(" i " as double)) as " i)))
           " FROM
                 CSVREAD('" prcs-dir cir-file "')
             GROUP BY rg, y, q
             ORDER BY rg, y, q;"))

(defn add-avg
  []
  (write-csv
   (str prcs-dir cir-file)
   (for [line (select-from-h2 asql)]
     [(line :cp)
      (line :rg)
      (line :y)
      (line :q)
      (line :i00) (line :i01) (line :i02)
      (line :i03) (line :i04) (line :i05)
      (line :i06) (line :i07) (line :i08)
      (line :i09) (line :i10) (line :i11)
      (line :i12) (line :i13) (line :i14)
      (line :i15) (line :i16) (line :i17)
      (line :i18) (line :i19) (line :i20)
      (line :i21) (line :i22) (line :i23)
      (line :i24) (line :i25) (line :i26)])))

;; to draw charts of cir items
(defn create-cir-query
  [item rg]
  "create query string for cir"
  (str
   " SELECT 
         y, q, cp, " item " as item"
   " FROM
         CSVREAD('" prcs-dir cir-file "')
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
   (str prcs-dir "cir/" item rg ".csv")
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
   (iio/read-dataset (str prcs-dir "cir/" item rg ".csv"):header false)
   '("C" "X" "Y")))

(defn generate-cir-charts
  ""
  []
  (doseq [{item :item rg :rg} cir-keys]
    (icore/save
     (icharts/scatter-plot
      :X
      :Y
      :data(load-dataset-with-col-names item rg)
      :group-by :C
      :legend false
      :title (str (cir-hash item) "_" rg)
      :x-label nil
      :y-label nil)
     (str out-dir "cir/" item "_" rg ".png")
     :width 600
     :height 200)))

;;
(def aasql
  (str
   " SELECT
         rg, y, q,"
   (apply str (interpose "," (for [cp cps] (str " CASE WHEN cp='" cp "' THEN i00 as " cp))))
   " FROM
         CSVREAD('" prcs-dir cir-file "')
     ORDER BY rg, y, q;"))

;; from joe
(defn create-sum-sql
  ""
  [y]
  (str
   " SELECT
         rg, cp, sum(cast(i15 as double)) as el, sum(cast(i25 as double)) as mf"
   " FROM
         CSVREAD('" prcs-dir cir-file "')"
   " WHERE
         y='" y "' AND NOT cp='AVG'"
   " GROUP BY
         rg, cp"
   " ORDER BY
         rg, cp"))

(defn convert-sum-data
  ""
  [lines]
  (for [line lines] [(line :rg) (line :cp) (line :el) (line :mf)]))

(defn add-sum-data
  ""
  [y]
  (write-csv 
   (str prcs-dir "sum.csv")
   (convert-sum-data (select-from-h2 (create-sum-sql y)))))

;;; cor
(def short-name {:a "actual" :p "projection"})
(def pg-rg {"1" "NA" "2" "EA" "3" "AP" "4" "LA"}) 
(def geo-prd-cost-el "1540x60+780+770")
(def geo-prd-cost-mf "1540x60+780+2015")
(def geo-lbr-cost "1540x60+780+2760")
(def geo-lbr-prod "1540x60+780+2810")
(def geo-inv-el "1540x135+780+960")
(def geo-inv-mf "1540x135+780+2310")

(defn make-cor-file-name-str
  ""
  [y k-ap]
  (str "COR_year" y "_" (short-name k-ap)))

(defn make-pdf2png-args
  ""
  [file-name]
  (list
   "-density" "300x300"
   "-units" "PixelsPerInch"
   (str org-dir "cor/" file-name ".pdf")
   (str prcs-dir "cor/" file-name ".png")))

(defn pdf2png
  ""
  [y k-ap]
  (apply shell/sh "convert" (make-pdf2png-args (make-cor-file-name-str y k-ap))))

(defn make-crop-args
  ""
  [file-name geo in-ext out-ext]
  (list
   "-crop"
   geo
   (str prcs-dir "cor/" file-name in-ext)
   (str prcs-dir "cor/crop/" file-name out-ext)))

(defn crop
  [args]
  (apply shell/sh "convert" args))

(defn crop-prd
  ""
  [y k-ap]
  (do
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-prd-cost-el "-0.png" "_prd_el.png"))
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-prd-cost-mf "-0.png" "_prd_mf.png"))))

(defn crop-lbr
  ""
  [y k-ap]
  (do
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-lbr-cost "-0.png" "_lbr_cost.png"))
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-lbr-prod "-0.png" "_lbr_prod.png"))))

(defn crop-inv
  ""
  [y k-ap]
  (doseq [[pg rg] pg-rg]
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-inv-el (str "-" pg ".png") (str "_" rg "_el.png")))
    (crop (make-crop-args (make-cor-file-name-str y k-ap) geo-inv-mf (str "-" pg ".png") (str "_" rg "_mf.png")))))

(defn extract-text
  ""
  [file-name]
  (shell/sh
   "tesseract"
   (str prcs-dir "cor/crop/" file-name)
   (str prcs-dir "cor/extract/" file-name)
   "-psm"
   "6"))

(defn doseq-et
  ""
  []
  (doseq [f (.listFiles (io/file (str prcs-dir "cor/crop")))]
    (extract-text (.getName f))))

(defn rep-zero
  ""
  [s]
  (clojure.string/replace s "O" "0"))

(defn read-a-line
  ""
  [y k-ap ext]
  (rep-zero (slurp (str prcs-dir "cor/extract/" (make-cor-file-name-str y k-ap) (str ext ".png.txt")))))

(defn perse-8-token
  ""
  [y k-ap ext]
   (take 8 (re-seq #"[0-9,.]+" (read-a-line y k-ap ext))))

(defn remove-comma
  ""
  [s]
  (apply str (re-seq #"[0-9.]+" s)))

(defn ins-prd
  "it should be called in transaction"
  [y k-ap em]
  (let [[a b c d e f g h] (vec (map bigdec (map remove-comma (perse-8-token y k-ap (str "_prd_" em)))))]
    (jdbc/insert-values
     :prd
     [:y :q :em :ap :prd_n :prd_c :prd_c_pu]
     [y 1 em (name k-ap) (with-precision 2 :rounding HALF_DOWN (/ a b)) a b]
     [y 2 em (name k-ap) (with-precision 2 :rounding HALF_DOWN (/ c d)) c d]
     [y 3 em (name k-ap) (with-precision 2 :rounding HALF_DOWN (/ e f)) e f]
     [y 4 em (name k-ap) (with-precision 2 :rounding HALF_DOWN (/ g h)) g h])))

(defn prd-select
  ""
  [y]
  (select-from-h2 (str
   "SELECT
        y, q, em, ap, prd_n, prd_c, prd_c_pu
    FROM
        prd
    WHERE y=" y "
    ORDER BY
        y, q, em, ap DESC")))

(defn main-prd
  ""
  [y]
  (write-csv (str prcs-dir "/cor/csv/prd.csv")
             (transpose-2d-str-array (into-2d-str-array
              (for [line (prd-select y)] (map str [(line :y) (line :q) (line :em) (line :ap) (.intValue (line :prd_n)) (.intValue (line :prd_c)) (.doubleValue (line :prd_c_pu))]))))))

;"select y, q, sum(casewhen(ap='p',prd_n,0)) as p_prd_n, sum(casewhen(ap='a',prd_n,0)) as a_prd_n from prd group by q order by y, q")

(defn get-lbr-cost
  ""
  [y k-ap]
  (map remove-comma
       (map first
            (partition 2 (perse-8-token y k-ap "_lbr_cost")))))

(defn get-lbr-prod
  ""
  [y k-ap]
  (map remove-comma
       (re-seq #"[0-9,]+" (read-a-line y k-ap "_lbr_prod"))))

(defn ins-lbr
  "it should be called in transaction"
  [y k-ap]
  (let [[a b c d] (vec (get-lbr-cost y k-ap)) [e f g h] (vec (get-lbr-prod y k-ap))]
    (jdbc/insert-values
     :lbr
     [:y :q :ap :lbr_c :prdtv]
     [y 1 (name k-ap) a e]
     [y 2 (name k-ap) b f]
     [y 3 (name k-ap) c g]
     [y 4 (name k-ap) d h])))

(defn lbr-select
  ""
  [y]
  (select-from-h2 (str
   "SELECT
        y, q, ap, lbr_c, prdtv
    FROM
        lbr
    WHERE y=" y "
    ORDER BY
        y, q, ap DESC")))

(defn main-lbr
  ""
  [y]
  (write-csv (str prcs-dir "/cor/csv/lbr.csv")
             (transpose-2d-str-array (into-2d-str-array
              (for [line (lbr-select y)] (map str [(line :y) (line :q) (line :ap) (line :lbr_c) (line :prdtv)]))))))

(defn get-inv-inf
  ""
  [y k-ap rg em]
  (do (println (str y k-ap rg em))
  (let [lines (reverse (for [line (re-seq #".+\n" (read-a-line y k-ap (str "_" rg "_" em)))] (re-seq #"[0-9.%]+" line)))]
    (vec (map vec (reverse (for [each (cons
     (map first (partition 2 (first lines)))
     (rest lines))] (for [j (take 4 each)] (Integer/parseInt j))))))))
)
(defn ins-inv
  ""
  [y k-ap rg em]
  (let [[[a b c d] [e f g h] [i j k l]] (get-inv-inf y k-ap rg em)]
    (jdbc/insert-values
     :inv
     [:rg :y :q :em :ap :bg_inv :cur_ship :demand :ed_inv]
     [rg y 1 em (name k-ap) a e i (- (+ a e) i)]
     [rg y 2 em (name k-ap) b f j (- (+ b f) j)]
     [rg y 3 em (name k-ap) c g k (- (+ c g) k)]
     [rg y 4 em (name k-ap) d h l (- (+ d h) l)])))

(defn ds-ins-inv
  ""
  [y]
  (doseq [rg '("NA" "EA" "AP" "LA") em '("el" "mf") k-ap '(:p :a)]
    (ins-inv y k-ap rg em)))

(defn inv-select
  ""
  [y]
  (select-from-h2 (str
   "SELECT
        rg, y, q, em, ap, bg_inv, cur_ship, demand, ed_inv
    FROM
        inv
    WHERE y=" y "
    ORDER BY
        q, y, rg, em, ap DESC")))

(defn main-inv
  ""
  [y]
  (write-csv (str prcs-dir "/cor/csv/inv.csv")
             (transpose-2d-str-array (into-2d-str-array
                                      (for [line (inv-select y)] (map str [(line :rg) (line :y) (line :q) (line :em) (line :ap)
                                                                         (line :bg_inv) (line :cur_ship) (line :demand) (line :ed_inv)]))))))

                                        ; (select-from-h2 "select sum(casewhen(rg='NA', bg_inv, 0)) as NA, sum(casewhen(rg='EA', bg_inv, 0)) as EA, sum(casewhen(rg='AP', bg_inv, 0)) as AP, sum(casewhen(rg='LA', bg_inv, 0)) as LA from inv where y=7 and q=1 and ap='a' and em='el';")
                                        ;(pp/pprint (select-from-h2 "select em, ap, sum(casewhen(rg='NA', bg_inv, 0)) as NA, sum(casewhen(rg='EA', bg_inv, 0)) as EA, sum(casewhen(rg='AP', bg_inv, 0)) as APa, sum(casewhen(rg='LA', bg_inv, 0)) as LA from inv where y=7 and q=1 group by em, ap order by em, ap;"))


(defn make-str-aaa
  ""
  [y, q]
  (str "select "
  (apply str
  (for [rg '("NA" "EA" "AP" "LA") em '("el" "mf")]
    (str "sum(casewhen(rg='" rg "'and em='" em "', bg_inv, 0)) as bi_" rg em ", "
         "sum(casewhen(rg='" rg "'and em='" em "', cur_ship, 0)) as cs_" rg em ", "    
         "sum(casewhen(rg='" rg "'and em='" em "', demand, 0)) as dm_" rg em ", "
         "sum(casewhen(rg='" rg "'and em='" em "', ed_inv, 0)) as ed_" rg em ", ")))
  " ap from inv where y=" y " and q=" q " group by ap order by ap DESC;"))

(defn aaa-conv
  [lines]
  (for [line lines] [(line :ap)
     (line :bi_nael) (line :cs_nael) (line :dm_nael) (line :ed_nael)
     (line :bi_namf) (line :cs_namf) (line :dm_namf) (line :ed_namf)
     (line :bi_eael) (line :cs_eael) (line :dm_eael) (line :ed_eael)
     (line :bi_eamf) (line :cs_eamf) (line :dm_eamf) (line :ed_eamf)
     (line :bi_apel) (line :cs_apel) (line :dm_apel) (line :ed_apel)
     (line :bi_apmf) (line :cs_apmf) (line :dm_apmf) (line :ed_apmf)
     (line :bi_lael) (line :cs_lael) (line :dm_lael) (line :ed_lael)
     (line :bi_lamf) (line :cs_lamf) (line :dm_lamf) (line :ed_lamf)]))
    
(defn create-tbl-prd
  ""
  []
  (jdbc/create-table
   :prd
   [:y "INT"]
   [:q "INT"]
   [:em "VARCHAR(12)"]
   [:ap "VARCHAR(12)"]
   [:prd_n "DECIMAL(12, 2)"]
   [:prd_c "DECIMAL(12, 2)"]
   [:prd_c_pu "DECIMAL(12, 2)"]))

(defn create-tbl-lbr
  ""
  []
  (jdbc/create-table
   :lbr
   [:y "INT"]
   [:q "INT"]
   [:ap "VARCHAR(12)"]
   [:lbr_c "VARCHAR(12)"]
   [:prdtv "VARCHAR(12)"]))

(defn create-tbl-inv
  ""
  []
  (jdbc/create-table
   :inv
   [:rg "VARCHAR(12)"]
   [:y "INT"]
   [:q "INT"]
   [:em "VARCHAR(12)"]
   [:ap "VARCHAR(12)"]
   [:bg_inv "INT"]
   [:cur_ship "INT"]
   [:demand "INT"]
   [:ed_inv "INT"]))

(defn drop-tbl
  ""
  [s-tbl]
  (try
    (jdbc/drop-table s-tbl)
    (catch Exception _)))

(defn main-post-cor
  [y]
  (doseq [q (range 1 5)]
    (write-csv (str prcs-dir "cor/csv/" y ".csv")
    (-> (select-from-h2 (make-str-aaa y q))
        (aaa-conv ,)
        (into-2d-str-array ,)
        (transpose-2d-str-array ,)))))


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
;(def p (idatasets/get-dataset :airline-passengers))
;(def sb (icharts/stacked-bar-chart :year :passengers :data p :group-by :month :legend true))