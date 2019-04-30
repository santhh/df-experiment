package com.google.swarm.experiment;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class SessionTime {

	public static final Logger LOG = LoggerFactory.getLogger(SessionTime.class);

	public static final TupleTag<KV<String, Long>> LOGIN_EVENT = new TupleTag<KV<String, Long>>() {
	};
	public static final TupleTag<KV<String, Long>> LOGOUT_EVENT = new TupleTag<KV<String, Long>>() {
	};
	public static final TupleTag<Long> LOGIN_EVENT_RESULT = new TupleTag<Long>() {
	};
	public static final TupleTag<Long> LOGOUT_EVENT_RESULT = new TupleTag<Long>() {
	};

	public static final String INPUT = "1532926994 User01 LogOutSuccessful\n" + "1532926981 User02 LogInSuccessful\n"
			+ "1532926982 User04 LogInFailed\n" + "1532926992 User01 LogInSuccessful\n"
			+ "1532926986 User02 LogOutSuccessful\n" + "1532927003 User03 LogOutSuccessful";

	public static void main(String[] args) {

		Pipeline p = Pipeline.create();

		PCollectionTuple data = p.apply("Create Transform", Create.of(INPUT)).apply("Parse Input Element",
				ParDo.of(new DoFn<String, KV<String, Long>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						List<String> records = Arrays.asList(c.element().split("\n"));

						records.forEach(record -> {

							String[] elements = record.split(" ");

							if (elements[2].contains("LogInSuccessful")) {

								c.output(LOGIN_EVENT, KV.of(elements[1], Long.parseLong(elements[0])));
							} else if (elements[2].contains("LogOutSuccessful")) {
								c.output(LOGOUT_EVENT, KV.of(elements[1], Long.parseLong(elements[0])));

							}

						});
					}
				}).withOutputTags(LOGIN_EVENT, TupleTagList.of(LOGOUT_EVENT)));

		PCollection<KV<String, CoGbkResult>> output = KeyedPCollectionTuple
				.of(LOGIN_EVENT_RESULT, data.get(LOGIN_EVENT)).and(LOGOUT_EVENT_RESULT, data.get(LOGOUT_EVENT))
				.apply(CoGroupByKey.create());

		output.apply("Printout Result", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				KV<String, CoGbkResult> e = c.element();
				String userId = e.getKey();

				Iterator<Long> loginIter = e.getValue().getAll(LOGIN_EVENT_RESULT).iterator();
				Iterator<Long> logoutIter = e.getValue().getAll(LOGOUT_EVENT_RESULT).iterator();

				while (loginIter.hasNext() && logoutIter.hasNext()) {
					Long duration = logoutIter.next() - loginIter.next();
					LOG.info("{\"user_id:\" {} , \"session_duration:\" {}}", userId, duration);
					c.output(StringUtils.EMPTY);
				}

			}

		}));

		p.run();

	}

}
