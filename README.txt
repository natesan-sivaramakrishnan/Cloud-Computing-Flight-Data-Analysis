1. Code files included:
	a. As per the objective there are 3 JAVA files for each objective.
		1. Probability.java 2. FlightCancellationReason.java 3. TaxiTimeForFlight.java
	b. Sample dataset(2001.csv) is included in ZIP file.

2. Instruction for running code:
	a. To convert the java files to jar file follow below steps:
		1. export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar (for setting Hadoop Claspath)
		2. bin/hadoop com.sun.tools.javac.Main filename.java (will change as per the Java file name)
			e.g. bin/hadoop com.sun.tools.javac.Main Probability.java
		3. jar cf jarname.jar filename*.class
			e.g. jar cf probability.jar Probability*.class
	b. hadoop jar jarname.jar filename /input /output (Change output folder for each run)
			e.g. hadoop jar probability.jar Probability /input /output

3. Checking the output:
	a. bin/hadoop fs -cat /output/part-r-0000 (Change output as per the output folder name given in above step)

NOTE: We were getting container launch error on t2.micro so we are part of whole input file. The submited code runs properly on data.
