#extract the unique information by providerid as Key
# 0: provider id - field 0
# 1: Condition -field 8
# 2: MeasureId - field 9
# 3: Score - field 11 
# 4: sample - field 12 
# 5: footnote - field 13
ef = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care/")
ef_prov_tr = ef.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), r[8], r[9].replace('"', ''),  r[11].replace('"', ''), r[12].replace('"', ''), r[13]))
ef_prov_tr.saveAsTextFile("hdfs:///user/w205/hospital_compare/ef_prov_tr")

