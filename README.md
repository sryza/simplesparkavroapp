Spark with Avro and Parquet
==================

Enclosed is a simple Spark app demonstrating how to read and write data in the Parquet and Avro
formats.

Avro refers to both a binary format and an in-memory Java object representation.  Parquet refers
to only a binary format, and it supports pluggable in-memory representations.  One of the options
for Parquet's in-memory representations is Avro, and that's the one we use here and recommend in
general.

Avro's actually has two in-memory representations. "Specific" records rely on generated code.
"Generic" records essentially represent objects as key-value pairs.  We use the specific
representation here because it is more efficient and easier to program against once the
code-generation has been set up.

To compile and package:

    mvn package
    
This will both generate Java classes from the Avro schema as well as build the project. The Avro
schema is a simple "User" object defined in src/main/resources/user.avsc.

To test writing an Avro file:

    spark-submit --class com.cloudera.sparkavro.SparkSpecificAvroWriter \
      target/sparkavroapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
      users.avro
    
To test reading an Avro file:

    spark-submit --class com.cloudera.sparkavro.SparkSpecificAvroReader \
      target/sparkavroapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
      users.avro
    
To test writing a Parquet file:

    spark-submit --class com.cloudera.sparkavro.SparkSpecificParquetWriter \
      target/sparkavroapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
      users.parquet
  
To test reading a Parquet file:

    spark-submit --class com.cloudera.sparkavro.SparkSpecificParquetReader \
      target/sparkavroapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
      users.parquet

Note that all the examples register the specific Avro class with Kryo.  This allows instances of it
to be serialized more efficiently when being passed around within Spark.  The examples don't
actually end up passing the objects around within Spark, but register them anyway because it's good
practice.


