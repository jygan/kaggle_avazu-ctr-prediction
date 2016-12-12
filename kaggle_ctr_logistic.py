#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Logistic Regression With LBFGS Example.
"""
from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="CTRLogisticRegression")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        values = [float(x) for x in line.split(',')]
        features = values[0:1]
        features.extend(values[2:])
        return LabeledPoint(values[1],features)

    data = sc.textFile("/Users/jiayangan/project/machine_learning/Kaggle/CTR/train_data")
    testData = sc.textFile("/Users/jiayangan/project/machine_learning/Kaggle/CTR/test_data")

    parsedTrainData = data.map(parsePoint)
    parsedTestData = testData.map(parsePoint)

    # Build the model
    model = LogisticRegressionWithLBFGS.train(parsedTrainData)

    # Evaluating the model on training data
    labelsAndPreds = parsedTestData.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedTestData.count())
    print("Training Error = " + str(trainErr))

    # Save and load model
    model.save(sc, "target/tmp/CTR")
    # $example off$
