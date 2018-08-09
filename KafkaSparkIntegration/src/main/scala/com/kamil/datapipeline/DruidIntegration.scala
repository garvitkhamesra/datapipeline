package com.kamil.datapipeline

import java.time.Period

import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.metamx.tranquility.spark.BeamFactory
import io.druid.granularity.QueryGranularities
import io.druid.query.aggregation.LongSumAggregatorFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.joda.time.DateTime

class DruidIntegration extends BeamFactory[ConsumerRecord[String, String]] {
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[ConsumerRecord[String, String]] = DruidIntegration.BeamInstance

}

object DruidIntegration
{
  val BeamInstance: Beam[ConsumerRecord[String, String]] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "localhost:2181",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo"
    val dimensions = IndexedSeq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))
    val isRollup = true
    // Expects simpleEvent.timestamp to return a Joda DateTime object.

    DruidBeams
      .builder((message: ConsumerRecord[String, String]) => new DateTime())
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation(indexService, "",dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          partitions = 1,
          replicants = 1
        )
      )
      .buildBeam()

    print("dfsd")
  }
}

