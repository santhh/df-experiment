package com.google.swarm.experiment;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")

public class AvgCalculation {

	public static final Logger LOG = LoggerFactory.getLogger(AvgCalculation.class);
	public static final TupleTag<KV<String, String>> START_TIME_TAG = new TupleTag<KV<String, String>>() {
	};
	public static final TupleTag<KV<String, String>> END_TIME_TAG = new TupleTag<KV<String, String>>() {
	};

	public static final TupleTag<String> START_TIME_TAG_OUTPUT = new TupleTag<String>() {
	};
	public static final TupleTag<String> END_TIME_TAG_OUTPUT = new TupleTag<String>() {
	};

	public static final String INPUT = "2017-02-01T10:00 Operation ABC Start\n" + "2017-02-01T10:01 Operation ABC End\n"
			+ "2017-02-01T10:02 Operation DEF Start\n" + "2017-02-01T10:08 Operation XYZ Start\n"
			+ "2017-02-01T20:09 Operation WXY Start\n" + "2017-02-01T10:10 Operation XYZ End\n"
			+ "2017-02-01T20:12 Operation WXY End";

	public static void main(String[] args) {

		Pipeline p = Pipeline.create();
		PCollectionTuple data = p.apply("Create Transform", Create.of(INPUT)).apply("Parse Input",
				ParDo.of(new DoFn<String, KV<String, String>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						List<String> records = Arrays.asList(c.element().split("\n"));

						records.forEach(record -> {

							String[] elements = record.split(" ");
							if (elements[3].contains("Start")) {

								c.output(KV.of(elements[2], elements[0]));
							} else if (elements[3].contains("End")) {

								c.output(END_TIME_TAG, KV.of(elements[2], elements[0]));

							}
						});
					}
				}).withOutputTags(START_TIME_TAG, TupleTagList.of(END_TIME_TAG)));

		PCollection<KV<String, CoGbkResult>> results = KeyedPCollectionTuple
				.of(START_TIME_TAG_OUTPUT, data.get(START_TIME_TAG)).and(END_TIME_TAG_OUTPUT, data.get(END_TIME_TAG))
				.apply("Group By Operation Id", CoGroupByKey.create());

		results.apply("Calculate Run Time", ParDo.of(new DoFn<KV<String, CoGbkResult>, Long>() {

			@ProcessElement
			public void processElement(ProcessContext c) {
				Iterator<String> startTimeItr = c.element().getValue().getAll(START_TIME_TAG_OUTPUT).iterator();
				Iterator<String> endTimeItr = c.element().getValue().getAll(END_TIME_TAG_OUTPUT).iterator();

				while (startTimeItr.hasNext() && endTimeItr.hasNext()) {
					LocalDateTime startTime = LocalDateTime.parse(startTimeItr.next());
					LocalDateTime endTime = LocalDateTime.parse(endTimeItr.next());
					c.output(ChronoUnit.MINUTES.between(startTime, endTime));
				}

			}
		})).apply("Average", Mean.<Long>globally()).apply("Output",
				MapElements.via(new SimpleFunction<Double, Double>() {
					public Double apply(Double input) {
						LOG.info("Average Runtime for ALL Operations {}", input);
						return input;

					}
				}));

		p.run().waitUntilFinish();

	}
}
