/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import org.mockito.Mockito.reset
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._

trait KinesisMock extends BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val materializer: Materializer = ActorMaterializer()
  implicit protected val kinesisAsyncClient: KinesisAsyncClient = mock[KinesisAsyncClient]

  override protected def beforeEach(): Unit =
    reset(kinesisAsyncClient)

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
