/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import akka.stream.alpakka.kinesis.scaladsl.KinesisSource
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{Answer, Stubber}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._

import scala.concurrent.duration.FiniteDuration

class KinesisSourceSpec extends WordSpecLike with Matchers with KinesisMock {

  implicit class recordToString(r: Record) {
    def utf8String: String = r.data.asUtf8String()
  }

  "KinesisSource" must {

    val shardSettings =
      ShardSettings("stream_name", "shard-id")
        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
    "poll for records" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess with MockitoHelper {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromUtf8String("1")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("2")).build(),
        )

        val probe = KinesisSource.basic(shardSettings, kinesisAsyncClient).runWith(TestSink.probe)

        probe.requestNext.utf8String shouldEqual "1"
        probe.requestNext.utf8String shouldEqual "2"
        probe.requestNext.utf8String shouldEqual "1"
        probe.requestNext.utf8String shouldEqual "2"
        probe.cancel()
      }
    }

    "poll for records with multiple requests" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromUtf8String("1")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("2")).build(),
        )

        val probe = KinesisSource.basic(shardSettings, kinesisAsyncClient).runWith(TestSink.probe)

        probe.request(2)
        probe.expectNext().utf8String shouldEqual "1"
        probe.expectNext().utf8String shouldEqual "2"
        probe.expectNoMessage(FiniteDuration(1, TimeUnit.SECONDS))
        probe.cancel()
      }
    }

    "wait for request before passing downstream" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromUtf8String("1")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("2")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("3")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("4")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("5")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("6")).build()
        )

        val probe = KinesisSource.basic(shardSettings, kinesisAsyncClient).runWith(TestSink.probe)

        probe.request(1)
        probe.expectNext().utf8String shouldEqual "1"
        probe.expectNoMessage(FiniteDuration(1, TimeUnit.SECONDS))
        probe.requestNext().utf8String shouldEqual "2"
        probe.requestNext().utf8String shouldEqual "3"
        probe.requestNext().utf8String shouldEqual "4"
        probe.requestNext().utf8String shouldEqual "5"
        probe.requestNext().utf8String shouldEqual "6"
        probe.requestNext().utf8String shouldEqual "1"
        probe.cancel()
      }
    }

    "merge multiple shards" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        val mergeSettings = List(
          shardSettings.withShardId("0"),
          shardSettings.withShardId("1")
        )

        override def shards: util.List[Shard] =
          util.Arrays.asList(Shard.builder().shardId("1").build(), Shard.builder().shardId("1").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromUtf8String("1")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("2")).build(),
          Record.builder().data(SdkBytes.fromUtf8String("3")).build()
        )

        val probe =
          KinesisSource.basicMerge(mergeSettings, kinesisAsyncClient).map(_.utf8String).runWith(TestSink.probe)

        probe.request(6)
        probe.expectNextUnordered("1", "1", "2", "2", "3", "3")
        probe.cancel()
      }
    }

    "complete stage when next shard iterator is null" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def records = util.Arrays.asList(Record.builder().data(SdkBytes.fromUtf8String("1")).build())

        val probe = KinesisSource.basic(shardSettings, kinesisAsyncClient).runWith(TestSink.probe)

        probe.requestNext.utf8String shouldEqual "1"
        nextShardIterator.set(null)
        probe.request(1)
        probe.expectNext()
        probe.expectComplete()
        probe.cancel()
      }
    }

    "fail with error when GetStreamRequest fails" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsFailure {
        val probe = KinesisSource.basic(shardSettings, kinesisAsyncClient).runWith(TestSink.probe)
        probe.request(1)
        probe.expectError() shouldBe an[KinesisErrors.GetRecordsError]
        probe.cancel()
      }
    }
  }

  trait MockitoHelper extends MockitoSugar {
    def doReturn(toBeReturned: Any): Stubber =
      Mockito.doReturn(toBeReturned, Nil: _*)
  }

  trait KinesisSpecContext {
    def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

    def shardIterator: String = "iterator"

    def records: util.List[Record] = new util.ArrayList()

    val nextShardIterator = new AtomicReference[String]("next")

    val describeStreamResponse = mock[DescribeStreamResponse]
    val streamDescription = mock[StreamDescription]

    when(kinesisAsyncClient.describeStream(DescribeStreamRequest.builder().streamName(anyString()).build()))
      .thenReturn(CompletableFuture.completedFuture(describeStreamResponse))
    when(describeStreamResponse.streamDescription()).thenReturn(streamDescription)
    when(streamDescription.shards()).thenReturn(shards)
    when(streamDescription.hasMoreShards).thenReturn(false)

    val getShardIteratorRequest = GetShardIteratorRequest.builder().build()
    val getShardIteratorResponse = GetShardIteratorResponse.builder().shardIterator(shardIterator).build()
    val getRecordsRequest = GetRecordsRequest.builder().build()

    def getRecordsResult =
      GetRecordsResponse.builder().records(records).nextShardIterator(nextShardIterator.get()).build()

  }

  trait WithGetShardIteratorSuccess { self: KinesisSpecContext =>
    when(kinesisAsyncClient.getShardIterator(getShardIteratorRequest))
      .thenReturn(CompletableFuture.completedFuture(getShardIteratorResponse))
  }

  trait WithGetShardIteratorFailure { self: KinesisSpecContext =>
    when(kinesisAsyncClient.getShardIterator(getShardIteratorRequest))
      .thenAnswer(new Answer[CompletableFuture[GetShardIteratorResponse]] {
        override def answer(invocation: InvocationOnMock) =
          CompletableFuture.completedFuture(getShardIteratorResponse)
      })
  }

  trait WithGetRecordsSuccess { self: KinesisSpecContext =>
    when(kinesisAsyncClient.getRecords(getRecordsRequest)).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef =
        CompletableFuture.completedFuture(getRecordsResult)
    })
  }

  trait WithGetRecordsFailure { self: KinesisSpecContext =>
    when(kinesisAsyncClient.getRecords(getRecordsRequest)).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef =
        CompletableFuture.completedFuture(getRecordsResult)
    })
  }
}
