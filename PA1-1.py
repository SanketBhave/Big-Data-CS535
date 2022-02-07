from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
years_published = ["1992", "1993","1994", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002"]
years1 = ["1992", "1993","1994", "1995", "1996", "1997", "1998", "1999", "2000"]
years2 = ["2001", "2002"]
dates = sc.textFile('/published-dates.txt')
citations = sc.textFile('/citations.txt')
#f = open("output.csv", "w")
#f.write("year", "nodes", "edges")
cols = ["nodes", "edges"]

#nodes_string = []
#data = []
#edges_count = []
nodes = dates.filter(lambda x: '#' not in x).map(lambda x: (x.split("\t")[1].split("-")[0], 1)).reduceByKey(lambda a
,b: a+b)
print(nodes.collect())

nodes = citations.filter(lambda x: ("#" not in x)).map(lambda x: (x.split("\t")[0], x.split("\t")[1]))
year_nodes1 = dates.filter(lambda x: ("#" not in x)).filter(lambda line: line.split("\t")[1].split("-")[0] in years1).map(lambda line: (line.split("\t")[0], line.split("\t")[1].split("-")[0]))
year_nodes2 = dates.filter(lambda x: ("#" not in x)).filter(lambda line: line.split("\t")[1].split("-")[0] in years2).map(lambda line: (line.split("\t")[0][1:], line.split("\t")[1].split("-")[0]))
year_nodes = year_nodes1.union(year_nodes2)
rdd = nodes.join(year_nodes)
    

for y in range(len(years_published)):
    print((years_published[y], len(rdd.filter(lambda x: x[1][1] in years_published[:y]).collect())))
