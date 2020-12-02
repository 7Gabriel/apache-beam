package com.loggi.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        // Create the pipeline.
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        // Apply Create, passing the list and the coder, to create the PCollection.
        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
    }


    }

