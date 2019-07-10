/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.impl

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.stream.alpakka.kinesis.{ShardSettings, KinesisErrors => Errors}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Internal API
 */
@InternalApi
private[kinesis] object KinesisSourceStage {

  private[kinesis] final case class GetShardIteratorSuccess(response: GetShardIteratorResponse)

  private[kinesis] final case class GetShardIteratorFailure(ex: Exception)

  private[kinesis] final case class GetRecordsSuccess(records: GetRecordsResponse)

  private[kinesis] final case class GetRecordsFailure(ex: Exception)

  private[kinesis] final case object Pump

}

/**
 * Internal API
 */
@InternalApi
private[kinesis] class KinesisSourceStage(shardSettings: ShardSettings, kinesisAsyncClient: KinesisAsyncClient)
    extends GraphStage[SourceShape[Record]] {

  import KinesisSourceStage._

  private val out = Outlet[Record]("Records")

  override def shape: SourceShape[Record] = new SourceShape[Record](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {

      import shardSettings._

      private[this] var currentShardIterator: String = _
      private[this] val buffer = mutable.Queue.empty[Record]
      private[this] var self: StageActor = _

      implicit val ec = ExecutionContext.global

      override def preStart(): Unit = {
        self = getStageActor(awaitingShardIterator)
        requestShardIterator()
      }

      setHandler(shape.out, new OutHandler {
        override def onPull(): Unit = self.ref ! Pump
      })

      private def awaitingShardIterator(in: (ActorRef, Any)): Unit = in match {
        case (_, GetShardIteratorSuccess(response)) =>
          currentShardIterator = response.shardIterator
          self.become(awaitingRecords)
          requestRecords()

        case (_, GetShardIteratorFailure(ex)) =>
          log.error(ex, "Failed to get a shard iterator for shard {}", shardId)
          failStage(new Errors.GetShardIteratorError(shardId, ex))

        case (_, Pump) =>
        case (_, msg) =>
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      private def awaitingRecords(in: (ActorRef, Any)): Unit = in match {
        case (_, GetRecordsSuccess(response)) =>
          val records = response.records.asScala
          if (response.nextShardIterator == null) {
            log.info("Shard {} returned a null iterator and will now complete.", shardId)
            completeStage()
          } else {
            currentShardIterator = response.nextShardIterator
          }
          if (records.nonEmpty) {
            records.foreach(buffer.enqueue(_))
            self.become(ready)
            self.ref ! Pump
          } else {
            scheduleOnce('GET_RECORDS, refreshInterval)
          }

        case (_, GetRecordsFailure(ex)) =>
          log.error(ex, "Failed to fetch records from Kinesis for shard {}", shardId)
          failStage(new Errors.GetRecordsError(shardId, ex))

        case (_, Pump) =>
        case (_, msg) =>
          println("SOMETHING WRONG IS HAPPENING")
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      private def ready(in: (ActorRef, Any)): Unit = in match {
        case (_, Pump) =>
          if (isAvailable(shape.out)) {
            push(shape.out, buffer.dequeue())
            self.ref ! Pump
          }
          if (buffer.isEmpty) {
            self.become(awaitingRecords)
            requestRecords()
          }

        case (_, msg) =>
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case 'GET_RECORDS => requestRecords()
      }

      private[this] def requestRecords(): Unit = {
        val getRecordsResponse: Future[GetRecordsResponse] = toScala(
          kinesisAsyncClient.getRecords(
            GetRecordsRequest
              .builder()
              .limit(limit)
              .shardIterator(currentShardIterator)
              .build()
          )
        )
        getRecordsResponse.onComplete {
          case Success(response) =>
            self.ref ! GetRecordsSuccess(response)
          case Failure(throwable) =>
            self.ref ! GetRecordsFailure(new Exception(throwable))
        }

      }

      private[this] def requestShardIterator(): Unit = {
        val request: GetShardIteratorRequest =
          GetShardIteratorRequest
            .builder()
            .streamName(streamName)
            .shardId(shardId)
            .shardIteratorType(shardIteratorType)
            .build()

        val shardIteratorResponse: Future[GetShardIteratorResponse] =
          kinesisAsyncClient.getShardIterator(request).toScala

        shardIteratorResponse.onComplete {
          case Success(response) =>
            self.ref ! GetShardIteratorSuccess(response)
          case Failure(throwable) =>
            self.ref ! GetShardIteratorFailure(new Exception(throwable))
        }

      }

    }

}
