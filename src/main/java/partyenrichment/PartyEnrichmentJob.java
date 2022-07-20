/*
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

package partyenrichment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import partyenrichment.entities.PartyBase;
import partyenrichment.entities.PartyEnriched;
import partyenrichment.utils.PartyAsynchEnrichmentFunction;

import java.util.concurrent.TimeUnit;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Skeleton code for the datastream walkthrough
 */
public class PartyEnrichmentJob {

	private static final String TOPIC_IN = "Party.New";
	private static final String TOPIC_OUT = "Party.Enrichment";
	private static final String BOOTSTRAP_SERVER = "localhost:9092";

	private static final Gson partygson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	private static final Gson gson = new Gson();

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers(PartyEnrichmentJob.BOOTSTRAP_SERVER)
			.setTopics(PartyEnrichmentJob.TOPIC_IN)
			.setGroupId("party-enrichment-group")
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();

		//Grab the Kafka messages and stingify them
		DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Party Source");

		//Convert string JSON string to PartyBase
		DataStream<PartyBase> partyStream = stream.map(json -> partygson.fromJson(json, PartyBase.class));

		//Enrich party object
		DataStream<PartyEnriched> enrichingStream = AsyncDataStream.unorderedWait(partyStream, new PartyAsynchEnrichmentFunction(),50, TimeUnit.MILLISECONDS);

		//Stringify again
		DataStream<String> enrichedStream = enrichingStream.map(party -> gson.toJson(party));

		//Sink to Kafka
		KafkaSink<String> destination = KafkaSink.<String>builder()
				.setBootstrapServers(PartyEnrichmentJob.BOOTSTRAP_SERVER)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(PartyEnrichmentJob.TOPIC_OUT)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.build();

		enrichedStream.sinkTo(destination);

		env.execute("Party Enrichment Job");
	}
}
