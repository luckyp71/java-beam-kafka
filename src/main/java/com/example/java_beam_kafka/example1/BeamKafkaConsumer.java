package com.example.java_beam_kafka.example1;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdks.java.io.kafka.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamKafkaConsumer {

	static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

	public static void main(String[] args) {
		
		// Create pipeline
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		
		// Read message from topic
		p.apply(KafkaIO.<Long, String>read().withBootstrapServers("localhost:9092").withTopic("your_topic")
				.withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
				.withMaxNumRecords(5).withoutMetadata())

				.apply(Values.<String>create()).apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						for (String word : c.element().split(TOKENIZER_PATTERN)) {
							if (!word.isEmpty()) {
								System.out.println(c.element());
								c.output(word);
							}
						}
					}
				})).apply(Count.<String>perElement())
				.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String apply(KV<String, Long> input) {
						return input.getKey() + ": " + input.getValue();
					}
				})).apply(TextIO.write().to("/home/lucky/Data-Lucky/Beam/wordcounts"));

		// Check pipeline status
		State pipelineStatus = p.run().waitUntilFinish();
		System.out.println(pipelineStatus);
	}
}