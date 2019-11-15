from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

takeN = 3
def getTFIDF(tf):
    if(tf.isEmpty()):
        return tf
    return IDF().fit(tf).transform(tf)

def clean(rdd):
    if(len(rdd) < 2):
        print(rdd)
        
    return [rdd[0].encode('ascii', 'ignore'), rdd[1].encode('ascii', 'ignore').translate(None, '.\'\",:;[]()!?')]

sc = SparkContext("local[2]", "HW3Proj")

#training portion

# Load documents (one per line).
train = sc.textFile("train.txt")

train = train.map(lambda line: line.split("||"))

train = train.map(lambda word: clean(word))

label_train = train.map(lambda word: word[0])
#print("Train: first labels")
#for x in label_train.take(takeN):
    #print(x)

words_train = train.map(lambda word: word[1])
#print("Train: first unstopped words")
#for x in words_train.take(takeN):
    #print(x)

print(words_train.collect()[0])
print(label_train.collect()[0])

#NaiveBayes
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
text_clf = Pipeline([('vect', CountVectorizer(stop_words='english')), ('tfidf', TfidfTransformer()), ('clf', MultinomialNB())])
text_clf = text_clf.fit(words_train.collect(), label_train.collect())

#SVM
from sklearn.linear_model import SGDClassifier
text_clf_svm = Pipeline([('vect', CountVectorizer(stop_words='english')), ('tfidf', TfidfTransformer()), ('clf-svm', SGDClassifier())])
text_clf_svm = text_clf_svm.fit(words_train.collect(), label_train.collect())

'''
#Naive Bayes testing portion

# Load documents (one per line).
test = sc.textFile("test.txt")

test = test.map(lambda line: line.split("||"))

test = test.map(lambda word: clean(word))

label_test = test.map(lambda word: word[0])
#print("Test: first labels")
#for x in label_test.take(takeN):
#    print(x)

words_test = test.map(lambda word: word[1])
#print("Test: first unstopped words")
#for x in words_test.take(takeN):
#    print(x)

import numpy as np
predicted = text_clf.predict(words_test.collect())
print("Accuracy:" + str(np.mean(predicted == label_test.collect())))
'''


# stream/test portion
brokers, topic = sys.argv[1:]

# Create a local StreamingContext with two working thread and batch interval of 1 second
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to Kafka
lines = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

lines = lines.map(lambda line: line[1].split("||"))
lines = lines.map(lambda word: [word[0].encode('ascii', 'ignore'), word[1].encode('ascii', 'ignore').translate(None, '.\'\",:;[]()!?')] )

label_lines = lines.map(lambda word: word[0])
#print("Train: first labels")
#for x in label_train.take(takeN):
    #print(x)

words_lines = lines.map(lambda word: word[1])
#print("Train: first unstopped words")
#for x in words_train.take(takeN):
    #print(x)

#label = lines.map(lambda word: word[0])

#words = lines.flatMap(lambda word: word[1])

def getPrediction(model, x):
	y = model.value.predict([x])
	return y[0]

#NB
myClfModel = sc.broadcast(text_clf)

predict = words_lines.map(lambda word_line: (word_line, getPrediction(myClfModel, word_line)))
#predict.pprint()
article = lines.map(lambda word: (word[1], word[0]))
#article.pprint()

final = article.join(predict).window(30,10)
#final.pprint()

final_true_pred = final.map(lambda final: (final[1][0], final[1][1]) )
final_true_pred.pprint()

def outputResult(time, rdd):
	from sklearn.metrics import accuracy_score
	from sklearn.metrics import precision_score
	from sklearn.metrics import recall_score
	from operator import itemgetter
	y_true = map(itemgetter(0), rdd.collect())
	y_pred = map(itemgetter(1), rdd.collect())
	acc = accuracy_score(y_true, y_pred)
	prec = precision_score(y_true, y_pred, average = 'weighted')
	recall = recall_score(y_true, y_pred, average = 'weighted')
	print("@Time: " + str(time) + "\n" + "Naive Bayes Accuracy: " + str(acc) + "\n" + "Naive Bayes Avg Precision: " + str(prec) + "\n" + 			"Naive Bayes Recall: " + str(recall))

final_true_pred.foreachRDD(outputResult)

#SVM
myClfModel_SVM = sc.broadcast(text_clf_svm)

predict = words_lines.map(lambda word_line: (word_line, getPrediction(myClfModel_SVM, word_line)))
#predict.pprint()
article = lines.map(lambda word: (word[1], word[0]))
#article.pprint()

final = article.join(predict).window(30,10)
#final.pprint()

final_true_pred = final.map(lambda final: (final[1][0], final[1][1]) )
final_true_pred.pprint()

def outputResult(time, rdd):
	from sklearn.metrics import accuracy_score
	from sklearn.metrics import precision_score
	from sklearn.metrics import recall_score
	from operator import itemgetter
	y_true = map(itemgetter(0), rdd.collect())
	y_pred = map(itemgetter(1), rdd.collect())
	acc = accuracy_score(y_true, y_pred)
	prec = precision_score(y_true, y_pred, average = 'weighted')
	recall = recall_score(y_true, y_pred, average = 'weighted')
	print("@Time: " + str(time) + "\n" + "SVM Accuracy: " + str(acc) + "\n" + "SVM Avg Precision: " + str(prec) + "\n" + 			"SVM Recall: " + str(recall))

final_true_pred.foreachRDD(outputResult)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

