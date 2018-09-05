package com.example.app.converters;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

@Slf4j
public class XmlToParquet {
    public static boolean run(SparkSession sparkSession, String inputDir, String outputDir) {

        log.info("inputDir = "+inputDir);
        log.info("outputDir = "+outputDir);
        /*
        JavaRDD<String> javaRDD = sparkSession.sparkContext()
                .wholeTextFiles(inputDir, 1)
                .toJavaRDD()
                .map(t -> t._2());
                */
        JavaRDD<String> javaRDD = sparkSession.sparkContext()
                .textFile(inputDir, 1)
                .toJavaRDD();

        log.info("javaRDD = "+javaRDD.count());

        return true;
    }
}
