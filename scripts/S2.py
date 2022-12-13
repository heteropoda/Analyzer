
import json
from pyspark import SparkConf, SparkContext
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


conf = SparkConf().setAppName('task1')
conf = conf.setMaster('local[*]')
sc = SparkContext(conf=conf)

stoplist = sc.broadcast(stopwords.words('english'))
def fun1(x):
    if x in stoplist.value:
        return False
    if len(x) <= 1:
        return False
    return True
    
rdd1 = sc.textFile('file:///home/linux/work/Analyzer/data/out.json')
rdd2 = rdd1.map(lambda x:json.loads(x)['content'])
rdd3 = rdd2.flatMap(lambda x:word_tokenize(x))
rdd4 = rdd3.filter(fun1)
rdd5 = rdd4.map(lambda x:(x,1))
rdd6 = rdd5.reduceByKey(lambda a,b:a+b)
rdd7 = rdd6.filter(lambda x:x[1] > 10)
rdd8 = rdd7.sortBy(lambda x:x[1],ascending=False)
print(rdd8.take(10))
