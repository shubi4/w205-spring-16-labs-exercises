# extract the unique information by providerId as Key.

#converts all columns in the record that have the form "N out of M" to "N"
def cleanup(rec):
        out = []
        for i in range(len(rec)):
            out.append(rec[i].split(" ")[0])
        return tuple(out)

#extract hospital id, survey scores
# scores have to be cleaned up to convert them to numbers
surveys = sc.textFile("hdfs:///user/w205/hospital_compare/surveys")
surveys_tr =  surveys.map(lambda r : r.split(",")).map(lambda r :  tuple(r[i] for i in [0]) + tuple(r[i] for i in range(7,33))).map(lambda r: cleanup(r))
surveys_tr.saveAsTextFile("hdfs:///user/w205/hospital_compare/surveys_tr")

