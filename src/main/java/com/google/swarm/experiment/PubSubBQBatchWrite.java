package com.google.swarm.experiment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class PubSubBQBatchWrite {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubBQBatchWrite.class);

	public static void main(String args[]) {

		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);
		Pipeline p = Pipeline.create(options);
		// pub sub
		WriteResult pubSubDataCollection = p
				.apply("Read From Pub Sub",
						PubsubIO.readMessagesWithAttributes()
								.fromSubscription("projects/{id}/subscriptions/bqsub"))
				.apply("PubSub Converts", MapElements.via(new SimpleFunction<PubsubMessage, KV<String, TableRow>>() {
					@Override
					public KV<String, TableRow> apply(PubsubMessage json) {

						TableRow row;
						try (InputStream inputStream = new ByteArrayInputStream(
								new String(json.getPayload(), StandardCharsets.UTF_8).getBytes())) {
							row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

						} catch (IOException e) {
							throw new RuntimeException("Failed to serialize json to table row: " + e.getMessage());
						}
						LOG.info("msg {}",row.toString());
						return KV.of(new String("event-type"), row);
					}
				})).apply("Events Write", BigQueryIO.<KV<String, TableRow>>write()
						.to("{project_id}:{dataset_id}.events").withFormatFunction(element -> {
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withMethod(BigQueryIO.Write.Method.FILE_LOADS).withNumFileShards(1)
						.withTriggeringFrequency(Duration.standardSeconds(10))
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER).withoutValidation());

		p.run();

	}

}
