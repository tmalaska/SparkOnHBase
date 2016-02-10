package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{ SparkContext, TaskContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SerializableWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.InterruptibleIterator
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkHadoopMapReduceUtilExtended
import org.apache.spark.Logging
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import java.text.SimpleDateFormat
import java.util.Date
import java.util.ArrayList
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper

class HBaseScanRDD (@transient sc: SparkContext,
                    @transient tableName: String,
                    @transient scan: Scan,
                    val configBroadcast: Broadcast[SerializableWritable[Configuration]],
                    val credentialsConf: Broadcast[SerializableWritable[Credentials]])
  extends RDD[(ImmutableBytesWritable, Result)](sc, Nil)
  with SparkHadoopMapReduceUtilExtended
  with Logging {

  @transient var appliedCredentials = false

  ///
  @transient val jobTransient = new Job(configBroadcast.value.value, "ExampleRead");
  TableMapReduceUtil.initTableMapperJob(
    tableName, // input HBase table name
    scan, // Scan instance to control CF and attribute selection
    classOf[IdentityTableMapper], // mapper
    null, // mapper output key
    null, // mapper output value
    jobTransient);

  @transient val jobConfigurationTrans = jobTransient.getConfiguration()
  jobConfigurationTrans.set(TableInputFormat.INPUT_TABLE, tableName)
  val jobConfigBroadcast = sc.broadcast(new SerializableWritable(jobConfigurationTrans))
  ////

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[Partition] = {

    addCreds

    val tableInputFormat = new TableInputFormat
    tableInputFormat.setConf(jobConfigBroadcast.value.value)

    val jobContext = newJobContext(jobConfigBroadcast.value.value, jobId)
    val rawSplits = tableInputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }

    result
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(ImmutableBytesWritable, Result)] = {

    addCreds
    applyCreds

    val iter = new Iterator[(ImmutableBytesWritable, Result)] {

      addCreds
      applyCreds

      val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = jobConfigBroadcast.value.value

      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      val format = new TableInputFormat
      format.setConf(conf)

      val reader = format.createRecordReader(
        split.serializableHadoopSplit.value, hadoopAttemptContext)
      reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        !finished
      }

      override def next(): (ImmutableBytesWritable, Result) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false

        val writableKey = new ImmutableBytesWritable(reader.getCurrentKey.copyBytes())

        (writableKey, reader.getCurrentValue)
      }

      private def close() {
        try {
          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  def addCreds {
    val creds = SparkHadoopUtil.get.getCurrentUserCredentials()

    val ugi = UserGroupInformation.getCurrentUser()
    ugi.addCredentials(creds)
    // specify that this is a proxy user
    ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)
  }

  def applyCreds[T]{
    val credentials = SparkHadoopUtil.get.getCurrentUserCredentials()

    if (!appliedCredentials && credentials != null) {
      appliedCredentials = true

      @transient val ugi = UserGroupInformation.getCurrentUser
      ugi.addCredentials(credentials)
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

      ugi.addCredentials(credentialsConf.value.value)
    }
  }

  private[spark] class NewHadoopPartition(
                                           rddId: Int,
                                           val index: Int,
                                           @transient rawSplit: InputSplit with Writable)
    extends Partition {

    val serializableHadoopSplit = new SerializableWritable(rawSplit)

    override def hashCode(): Int = 41 * (41 + rddId) + index
  }
}
