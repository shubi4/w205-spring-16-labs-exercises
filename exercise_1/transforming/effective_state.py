#extract the information by state as key and measureid as key
# state - field 0 
# condition - field 1
# measureId - field 3 
# score - field 4 

ef_state = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care_state/")
ef_state_prov = ef_state.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), r[1], r[3].replace('"', ''),  r[4].replace('"', '')))
ef_state_prov_tr.saveAsTextFile("hdfs:///user/w205/hospital_compare/ef_state_prov_tr")


