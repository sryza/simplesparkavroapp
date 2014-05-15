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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.avro.mapreduce.{AvroKeyOutputFormat, AvroJob}
import org.apache.avro.Schema.Parser

import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.mapred.AvroKey

object SparkAvroWriter {
  def main(args: Array[String]) {
    val outPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark Avro")
    val sc = new SparkContext(sparkConf)

    val schema = new Parser().parse(this.getClass.getClassLoader
      .getResourceAsStream("user.avsc"))

    val user1 = new GenericData.Record(schema)
    user1.put("name", "Alyssa")
    user1.put("favorite_number", 256)

    val user2 = new GenericData.Record(schema)
    user2.put("name", "Ben")
    user2.put("favorite_number", 7)
    user2.put("favorite_color", "red")

    val records = sc.parallelize(Array(user1, user2))
    val withValues = records.map((x) => (new AvroKey(x), NullWritable.get))

    val conf = new Job()
    FileOutputFormat.setOutputPath(conf, new Path(outPath))
    AvroJob.setOutputKeySchema(conf, schema)
    conf.setOutputFormatClass(classOf[AvroKeyOutputFormat[Any]])
    withValues.saveAsNewAPIHadoopDataset(conf.getConfiguration)
  }
}
