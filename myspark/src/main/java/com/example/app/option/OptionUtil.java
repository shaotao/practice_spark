package com.example.app.option;

import com.example.app.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

@Slf4j
public class OptionUtil {
    public static Options createOptions() {
        Options options = new Options();

        Option sparkConfOption = Option.builder()
                .argName(Constants.sparkConfFileKey)
                .desc("spark config file")
                .hasArg(true)
                .required(true)
                .longOpt(Constants.sparkConfFileKey)
                .build();
        Option inputDirOption = Option.builder()
                .argName(Constants.inputDirKey)
                .desc("input directory")
                .hasArg(true)
                .required(true)
                .longOpt(Constants.inputDirKey)
                .build();
        Option outputDirOption = Option.builder()
                .argName(Constants.outputDirKey)
                .desc("output directory")
                .hasArg(true)
                .required(true)
                .longOpt(Constants.outputDirKey)
                .build();
        options.addOption(sparkConfOption);
        options.addOption(inputDirOption);
        options.addOption(outputDirOption);

        return options;
    }
}
