/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class KinesisTest {
  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static ShardSettings settings;
  private static KinesisAsyncClient amazonKinesisAsync;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("aws.accessKeyId", "someKeyId");
    System.setProperty("aws.secretKey", "someSecretKey");

    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);

    settings = ShardSettings.create("my-stream", "shard-id");
    amazonKinesisAsync = mock(KinesisAsyncClient.class);
  }

  @AfterClass
  public static void afterAll() {
    TestKit.shutdownActorSystem(system);
  }

  //  @Ignore("This test appears to trigger a deadlock, see
  // https://github.com/akka/alpakka/issues/390")
  @Test
  public void PullRecord() throws Exception {

    when(amazonKinesisAsync.describeStream(DescribeStreamRequest.builder().build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                DescribeStreamResponse.builder()
                    .streamDescription(
                        StreamDescription.builder()
                            .shards(Shard.builder().shardId("id").build())
                            .hasMoreShards(false)
                            .build())
                    .build()));
    when(amazonKinesisAsync.getShardIterator(GetShardIteratorRequest.builder().build()))
        .thenAnswer(
            (Answer)
                invocation -> {
                  return CompletableFuture.completedFuture(
                      GetShardIteratorResponse.builder().build());
                });

    when(amazonKinesisAsync.getRecords(GetRecordsRequest.builder().build()))
        .thenAnswer(
            (Answer)
                invocation -> {
                  return CompletableFuture.completedFuture(GetRecordsResponse.builder().build());
                });

    final Source<Record, NotUsed> source = KinesisSource.basic(settings, amazonKinesisAsync);
    final CompletionStage<Record> record = source.runWith(Sink.head(), materializer);

    assertEquals("1", record.toCompletableFuture().get(10, TimeUnit.SECONDS).sequenceNumber());
  }
}
