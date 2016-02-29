#extract measure Id as Key 
measures = sc.textFile("hdfs:///user/w205/hospital_compare/measures/")
meas_tr = measures.map(lambda r: r.split(",")).map(lambda r: (r[1].replace('"', ''), r))
meas_tr.saveAsTextFile("hdfs:///user/w205/hospital_compare/meas_tr")

