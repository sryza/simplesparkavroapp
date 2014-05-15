/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sparkavro

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.avro.Schema.Parser
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.mapreduce.{AvroKeyInputFormat, AvroJob}
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.hadoop.io.AvroSerialization
import org.apache.avro.specific.SpecificData

object SparkSpecificAvroReader {
  class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[User])
    }
  }

  def main(args: Array[String]) {
    val inPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark Avro")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    val sc = new SparkContext(sparkConf)
    sc.addJar(SparkContext.jarOfObject(this).get)

    val schema = new Parser().parse(this.getClass.getClassLoader
      .getResourceAsStream("user.avsc"))

    val conf = new Job()
    FileInputFormat.setInputPaths(conf, inPath)
    AvroJob.setInputKeySchema(conf, schema) // TODO: needed?
    val records = sc.newAPIHadoopRDD(conf.getConfiguration,
      classOf[AvroKeyInputFormat[User]],
      classOf[AvroKey[User]],
      classOf[NullWritable])
    /*
    val localRecords = records.collect()
    //    println("localRecords(0): " + localRecords(0))

    println("more: " + localRecords(0).getClass)
    println("more: " + localRecords(0).getClass.getCanonicalName)
    println("more: " + localRecords(0)._1.getClass)
    println("more: " + localRecords(0)._1.getClass.getCanonicalName)
    println("more: " + localRecords(0)._1.datum.getClass)
    println("more: " + localRecords(0)._1.datum.getClass.getCanonicalName)
    println("more: " + localRecords(0)._1.datum.get("name"))
    println("more: " + localRecords(0)._1.datum.get("favorite_color"))
    println("more: " + localRecords(0)._1.datum.get("favorite_number"))
    println("more: " + localRecords(0)._2.getClass)
    println("more: " + localRecords(0)._2.getClass.getCanonicalName)
    println("more: " + localRecords(0)._1)
    println("more: " + localRecords(0)._2)
*/
    println("records: " + records.map(x => x._1.datum).collect())
    println("num records: " + records.count())
  }
}
