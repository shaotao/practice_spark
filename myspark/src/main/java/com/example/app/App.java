package com.example.app;

import com.example.app.config.ConfigUtil;
import com.example.app.converters.XmlToParquet;
import com.example.app.option.OptionUtil;
import com.example.app.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * my spark program
 *
 */
@Slf4j
public class App 
{
    private static String sparkConfFile;
    private static String inputDir;
    private static String outputDir;

    public static void main( String[] args ) throws Exception
    {
        log.info( "=== My Spark ===" );
        Options options = OptionUtil.createOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);
        sparkConfFile = line.getOptionValue(Constants.sparkConfFileKey);
        inputDir = line.getOptionValue(Constants.inputDirKey);
        outputDir = line.getOptionValue(Constants.outputDirKey);
        log.info("sparkConfFile = "+sparkConfFile);
        log.info("inputDir = "+inputDir);
        log.info("outputDir = "+outputDir);

        SparkConf sparkConf = ConfigUtil.getSparkConf(sparkConfFile);

        // create the spark session;
        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        XmlToParquet.run(sparkSession, inputDir, outputDir);
        sparkSession.close();
    }
}
