package com.example.java_beam_kafka.example1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class BeamKafkaProducer {


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        BufferedReader messages = new BufferedReader(new InputStreamReader(System.in));
       
        // sample data
        List<KV<Long, String>> kvs = new ArrayList<>();
        Boolean stop = false;
        Long key = 1L;
        
        while(stop == false) {
        	String message;
			try {
				System.out.print("Please enter message to send: ");
				message = messages.readLine();
	        	kvs.add(KV.of(key,message));
	        	
				System.out.print("Enter y to send another message or n to exit: ");
				String stopper = messages.readLine();
				
				if(stopper.equalsIgnoreCase("y")) {
					stop = true;
				} else {
					key++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} 
        }
        
        // Read input data
        PCollection<KV<Long, String>> input = p
                .apply(Create.of(kvs));

        // Send message to topic
        input.apply(KafkaIO.<Long, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("your_topic")

                .withKeySerializer(LongSerializer.class)
                .withValueSerializer(StringSerializer.class)
        );

        // Check pipeline status
        State pipelineStatus = p.run().waitUntilFinish();
        System.out.println(pipelineStatus);
    }
}