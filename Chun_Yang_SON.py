import sys
import itertools
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('HW2')
sc = SparkContext(conf=conf)

if sys.argv[1]=="1":
    users = sc.textFile(sys.argv[3])
    user=users.map(lambda x: x.split("::")).filter(lambda x: x[1]=="M").map(lambda y:(int(y[0]),y[1]))
    ratings = sc.textFile(sys.argv[2])
    rating=ratings.map(lambda x: x.split("::")).map(lambda y: (int(y[0]),(int(y[1]))))
    join=rating.join(user).map(lambda (k,(v1,v2)): (k,[v1])).reduceByKey(lambda a,b: list(set(a+b)))
    baskets=join.map(lambda (a,b):b)
    t=int(sys.argv[4])
    st=t/baskets.getNumPartitions()
if sys.argv[1]=="2":
    users = sc.textFile(sys.argv[3])
    user=users.map(lambda x: x.split("::")).filter(lambda x: x[1]=="F").map(lambda y:(int(y[0]),y[1]))
    ratings = sc.textFile(sys.argv[2])
    rating=ratings.map(lambda x: x.split("::")).map(lambda y: (int(y[0]),(int(y[1]))))
    join=rating.join(user).map(lambda (k,(v1,v2)): (v1,[k])).reduceByKey(lambda a,b: list(set(a+b)))
    baskets=join.map(lambda (a,b):b)
    t=int(sys.argv[4])
    st=t/baskets.getNumPartitions()

    
def candidates(baskets,size,poss_cads):
    if size==1:
        count={}
        for basket in baskets:
            for item in basket:
                 count[item]=count.get(item,0)+1
        freqcad = sorted([x for x in count.keys() if count[x]>=st])    
    else:
        count={}
        for basket in baskets:
            for item in poss_cads:
                if set(item).issubset(set(basket)):
                    count[item]=count.get(item,0)+1
        freqcad = sorted([x for x in count.keys() if count[x]>=st])
        
    return freqcad


def frequency(baskets,size,cand):
    if size==1:
        count={}
        for basket in baskets:
            for item in cand:
                if item in basket:
                    count[item]=count.get(item,0)+1
    else:
        count={}
        for basket in baskets:
            for item in cand:
                if set(item).issubset(set(basket)):
                    count[item]=count.get(item,0)+1
    return count.items()
    
    
size=1
l=1
poss_lists=[]
output=[]


while l!=0:
    freqpartition=baskets.mapPartitions(lambda x:candidates(x,size,poss_lists)).distinct().collect()
    freq=baskets.mapPartitions(lambda x:frequency(x,size,freqpartition))
    if size==1:
        truefrequent=freq.map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1]>=t).map(lambda x:x[0]).collect()
        poss_lists= list(itertools.combinations(truefrequent, (size+1)))
        output.append(sorted(truefrequent))
   
    else:   
        truefrequents=freq.reduceByKey(lambda x,y: x+y).filter(lambda x: x[1]>=t).map(lambda x:x[0]).collect()
        truefrequent=[tuple(sorted(x)) for x in truefrequents]
        output.append(sorted(truefrequent))
        
        s=set(itertools.chain(*truefrequent))
        allsize=[tuple(sorted(x)) for x in [i for i in itertools.combinations(s, size+1)]] ##size=2, allsize=3
    
        c=[]
        for size_1 in allsize:
            anysets=[tuple(sorted(x)) for x in [i for i in itertools.combinations(size_1, size)]]
            if set(anysets).issubset (set(truefrequent)):
                c.append(size_1) 
        poss_lists=c
        
    l=len(poss_lists)
    size=size+1  

    
filename="Chun_Yang_SON.case" + sys.argv[1] + "_" + sys.argv[4]+".txt"
output_file = open(filename, "w")
for i in range(len(output)):
    for j in range(len(output[i])):
        if i==0:
            if j != len(output[i]) - 1:
                output_file.write("("+str(output[i][j])+"),")
            else:
                output_file.write("("+str(output[i][j])+")")
        else :
            if j != len(output[i]) - 1:
                output_file.write(str(output[i][j])+",")
            else:
                output_file.write(str(output[i][j]))
    output_file.write("\n")