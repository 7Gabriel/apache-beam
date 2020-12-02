package com.loggi.beam;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


public class Main {

    public static void main(String[] args) {

        // Create the pipeline.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> stream = pipeline.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("etl-loggi")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
                .apply(Window.<KafkaRecord<Long, String>>into(FixedWindows.of(Duration.standardSeconds(30))))
        .apply(
                ParDo.of(new MainClase()));
        stream.apply("WriteFile", TextIO.write().to("/tmp/output/output.data")
                .withWindowedWrites()
                .withNumShards(1));
        pipeline.run();

    }
}
class MainClase extends DoFn<KafkaRecord<Long, String>, String>{
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        KafkaRecord<Long, String> record = processContext.element();
        System.out.println(record.getKV().getValue());
        processContext.output(record.getKV().getValue());
    }
}
