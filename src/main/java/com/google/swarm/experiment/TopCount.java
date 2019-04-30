package com.google.swarm.experiment;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.KvSwap;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopCount {

	public static final Logger LOG = LoggerFactory.getLogger(TopCount.class);

	public static final String INPUT = "147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.04.719,GET,/index.html,147.161.05.719,GET,/index.html,147.161.05.719,GET,/index.html,147.161.05.719,GET,/index.html,147.161.05.719,GET,/index.html,147.161.06.719,GET,/index.html,147.161.06.719,GET,/index.html,147.161.06.719,GET,/index.html,147.161.06.719,GET,/index.html";
	public static final String IP_REG_EX_PATTERN = "^\\d+\\.\\d+\\.\\d+\\.\\d+$";

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		Pipeline p = Pipeline.create();
		PCollection<String> data = p.apply("Create Transform", Create.of(INPUT))
				.apply("Parse Input", ParDo.of(new DoFn<String, KV<String, String>>() {

					@ProcessElement
					public void processElement(ProcessContext c) {
						List<String> findIps = Arrays.asList(c.element().split(","));
						findIps.forEach(ifip -> {
							if (ifip.matches(IP_REG_EX_PATTERN)) {
								c.output(KV.of(ifip.trim(), ifip.trim()));
							}
						});

					}
				})).apply("Count Par Key", Count.perKey()).apply("swap key", KvSwap.create())
				.apply("Group By key", GroupByKey.create())
				.apply("Top 10", Top.of(10, new SerializableComparator<KV<Long, Iterable<String>>>() {

					@Override
					public int compare(KV<Long, Iterable<String>> o1, KV<Long, Iterable<String>> o2) {
						return Long.compare(o1.getKey(), o2.getKey());
					}

				})).apply(MapElements.via(new SimpleFunction<List<KV<Long, Iterable<String>>>, String>() {

					public String apply(List<KV<Long, Iterable<String>>> input) {

						input.forEach(e -> {

							e.getValue().forEach(ip -> {
								LOG.info("IP {} , Count {}", ip, e.getKey());

							});

						});
						return StringUtils.EMPTY;
					}
				}));

		p.run().waitUntilFinish();
	}

}
