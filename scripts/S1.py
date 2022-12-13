
import json
from pyspark import SparkConf, SparkContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer


conf = SparkConf().setAppName('task1')
conf = conf.setMaster('local[*]')
sc = SparkContext(conf=conf)

sid = sc.broadcast(SentimentIntensityAnalyzer())
def fun1(x):
    l = sid.value.polarity_scores(x)
    y = 'neu'
    if l['neg'] > l[y]: y = 'neg'
    if l['pos'] > l[y]: y = 'pos'
    return (y, 1)

rdd1 = sc.textFile('file:///home/linux/work/Analyzer/data/out.json')
rdd2 = rdd1.map(lambda x:json.loads(x)['content'])
rdd3 = rdd2.map(fun1)
rdd4 = rdd3.countByKey()
print(rdd4)
