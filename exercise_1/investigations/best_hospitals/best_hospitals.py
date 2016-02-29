def try_parse_int(s, val=0):
  try:
    return int(s)
  except ValueError:
    return val

#find the best hospitals by overall score. Add up all the scores for different measures to get the hospitals' overall score

ef_prov_by_score =  ef_prov_tr.map(lambda r: (r[0], r[3])).reduceByKey(lambda x,y: try_parse_int(x) + try_parse_int(y))
ef_prov_by_score_sorted = ef_prov_by_score.join(hosp_tr).sortBy(lambda x : x[1][0], False)
best_hosp_by_score = ef_prov_by_score_sorted.take(10)
for i in best_hosp_by_score:
  print(i[1])

#find the best hospitals by individual measure score  

#group by measure id
r = ef_prov_tr.map(lambda r: (r[0], r[2], r[3])).groupBy(lambda x: x[1])
#sort within each measure by the score and get the top 10
s = r.map(lambda x: sorted(x[1],key=lambda r : try_parse_int(r[2]), reverse=True)[:10]) 
res = s.collect()
for i in range(len(res)):
  print("-----Measure-----")
  print(res[i])

