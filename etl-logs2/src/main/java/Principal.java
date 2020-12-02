import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Principal {

    public static void main(String[] args) throws Exception {
        // Begin constructing a pipeline configured by commandline flags.
        Options options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Read events from a text file and parse them.
        pipeline
                .apply(TextIO.read().from(options.getInput()))
                .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
                // Extract and sum username/score pairs from the event data.
                .apply("ExtractUserScore", new ExtractAndSumScore("user"))
                .apply(
                        "WriteUserScoreSums", new WriteToText<>(options.getOutput(), configureOutput(), false));

        // Run the batch pipeline.
        pipeline.run().waitUntilFinish();
    }




}
