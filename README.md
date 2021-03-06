JPMML-Piglet
============

A Java UDF for evaluating PMML models on the Apache Pig platform (http://pig.apache.org/).

# Prerequisites #

* Apache Pig 0.14.0 or newer.

# Installation #

Enter the project root directory and build using [Apache Maven] (http://maven.apache.org/):
```
mvn clean install
```

The build produces an uber-JAR file `target/jpmml-piglet-distributable-1.0-SNAPSHOT.jar`.

# Usage #

Add the uber-JAR file to Apache Pig classpath:
```
REGISTER target/jpmml-piglet-distributable-1.0-SNAPSHOT.jar;
```

The following example scores the `src/etc/Iris.csv` CSV file with the `src/etc/RandomForestIris.pmml` PMML file.

Importing data from the CSV file:
```
iris_data = LOAD 'src/etc/Iris.csv' USING PigStorage(',')
	AS (Sepal_Length:double, Sepal_Width:double, Petal_Length:double, Petal_Width:double);

DESCRIBE iris_data;
```

Defining a Java UDF for the PMML file:
```
DEFINE iris_rf org.jpmml.piglet.PMMLFunc('src/etc/RandomForestIris.pmml');
```

Scoring data using the Java UDF:
```
iris_rf_prediction = FOREACH iris_data GENERATE iris_rf(*);

DESCRIBE iris_rf_prediction;

DUMP iris_rf_prediction;
```

# License #

JPMML-Piglet is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html) and a commercial license.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)