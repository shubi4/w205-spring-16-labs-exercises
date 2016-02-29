# to get the best state measures, sum up the scores over all measures for each state, then sort
# in descending order

#read from i-memory RDD
res_state = ef_state_prov.map(lambda r: (r[0], r[3])).reduceByKey(lambda x,y: try_parse_int(x) + try_parse_int(y)).sortBy(lambda x : x[1], False)
res_state.take(10)
