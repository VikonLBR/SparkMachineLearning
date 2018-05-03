from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, HiveContext
from pyspark.mllib.regression import LabeledPoint
import pickle
import pandas as pd



sc = SparkContext('local')
hive_context = HiveContext(sc)
#session = SparkSession(sc)

#get the data
data = hive_context.sql("select * from userdb.titanic").toPandas()[1:]
#data preprocessing
#numberic attributes
Nattr = ['passengerid', 'pclass', 'age', 'sibsp', 'parch', 'fare', 'survived']
for attr in Nattr:
	data[attr] = pd.to_numeric(data[attr])
data['age'].fillna(int(data.age.mean()), inplace=True)
data['cabin'] = data['cabin'].map(lambda x : x[0] if len(x)>0 else "missing")
data['cabin'].fillna("missing", inplace=True)
data['embarked'].fillna("missing", inplace=True)
for attr in ['cabin', 'sex', 'embarked']:
	dummy = pd.get_dummies(data[attr], prefix=attr)
	data = pd.concat([data,dummy], axis=1)
	data.pop(attr)
'''
[  u'passengerid',      u'survived',        u'pclass',
                 u'age',         u'sibsp',         u'parch',
                u'fare',       u'cabin_A',       u'cabin_B',       u'cabin_C',
             u'cabin_D',       u'cabin_E',       u'cabin_F',       u'cabin_G',
             u'cabin_missing',    u'sex_female',      u'sex_male',
	     u'embarked_C',    u'embarked_Q',    u'embarked_S']
'''
data.pop('name')
data.pop('ticket')
data.pop('cabin_T')
data.pop('embarked_')
print(data.columns)




sqlcontext = SQLContext(sc)
data = sqlcontext.createDataFrame(data)
(trainingData, testingData) = data.randomSplit([0.8, 0.2])

trainingD = trainingData.rdd.map(lambda x: LabeledPoint(x['survived'],
[x['passengerid'], x['pclass'], x['age'], x['sibsp'], x['parch'],
x['fare'], x['cabin_A'], x['cabin_B'], x['cabin_C'], x['cabin_D'],
x['cabin_E'], x['cabin_F'], x['cabin_G'], x['cabin_missing'],
x['sex_female'], x['sex_male'], x['embarked_C'],  x['embarked_Q'],
x['embarked_S']]))

testingD = testingData.rdd.map(lambda data: [data['passengerid'], data['pclass'], data['age'], data['sibsp'], data['parch'], data['fare'], data['cabin_A'], data['cabin_B'], data['cabin_C'], data['cabin_D'], data['cabin_E'], data['cabin_F'], data['cabin_G'], data['cabin_missing'], data['sex_female'], data['sex_male'], data['embarked_C'], data['embarked_Q'], data['embarked_S']])





#modeling
model = RandomForest.trainClassifier(trainingD,2,{},1000,seed=42)

#model = RandomForestModel.load(sc, "myFirstWorkModel")	
prediction = model.predict(testingD).collect()

trueLabels = testingData.rdd.map(lambda x:x['survived']).collect()

#print model.toDebugString()

total = 0
for i in range(len(prediction)):
	if (prediction[i]==trueLabels[i]):
		total += 1
print ' '
print ' '

print("the accuracy is", float(total)/len(prediction))

print ' '
print ' '



#save the model
model.save(sc, "Model")
#model = RandomForestModel.load(sc, "myFirstWorkModel")	



























