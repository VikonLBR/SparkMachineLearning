from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, HiveContext
from pyspark.mllib.regression import LabeledPoint
import pickle
import pandas as pd

sc = SparkContext('local')
model = RandomForestModel.load(sc, "myFirstWorkModel")

data = pd.read_csv('/home/hadoop/hadooptest/test.csv')

Nattr = ['PassengerId', 'Pclass', 'Age', 'SibSp', 'Parch', 'Fare']
data['Age'].fillna(int(data.Age.mean()), inplace=True)
data['Cabin'] = data['Cabin'].fillna('missing').map(lambda x: x[0]).map(lambda x: x if x!='m' else 'missing')

data['Embarked'].fillna("missing", inplace=True)
for attr in ['Cabin', 'Sex', 'Embarked']:
	dummy = pd.get_dummies(data[attr], prefix=attr)
	data = pd.concat([data,dummy], axis=1)
	data.pop(attr)
'''
[  u'passengerid',      u'survived',        u'pclass',
                 u'age',         u'sibsp',         u'parch',
                u'fare',       u'cabin_A',       u'cabin_B',       u'cabin_C',
             u'cabin_D',       u'cabin_E',       u'cabin_F',       u'cabin_G',
             u'cabin_T', u'cabin_missing',    u'sex_female',      u'sex_male',
           u'embarked_',    u'embarked_C',    u'embarked_Q',    u'embarked_S']
'''
data.pop('Name')
data.pop('Ticket')
model = RandomForestModel.load(sc, "Model")
sqlcontext = SQLContext(sc)
data = sqlcontext.createDataFrame(data)
testingD = data.rdd.map(lambda data: [data['PassengerId'], data['Pclass'], data['Age'], data['SibSp'], data['Parch'], data['Fare'], data['Cabin_A'], data['Cabin_B'], data['Cabin_C'], data['Cabin_D'], data['Cabin_E'], data['Cabin_F'], data['Cabin_G'], data['Cabin_missing'], data['Sex_female'], data['Sex_male'], data['Embarked_C'], data['Embarked_Q'], data['Embarked_S']])



prediction = model.predict(testingD)
print ' '
print ' '
print ' '
print prediction.collect()
print ' '
print ' '
print ' '

with open('result.txt', 'w') as file:
	file.write(prediction.collect())





