#extract hospital id as Key
#extract the hospital id as Key
hosp = sc.textFile("hdfs:///user/w205/hospital_compare/hospitals/")
hosp_tr = hosp.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), r))
hosp_tr.saveAsTextFile("hdfs:///user/w205/hospital_compare/hosp_tr")

