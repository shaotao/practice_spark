package com.example.app.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;

import java.io.File;
import java.io.IOException;

@Slf4j
public class ConfigUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static SparkConf getSparkConf(String sparkConfFile) throws IOException {
        String data = FileUtils.readFileToString(new File(sparkConfFile), "UTF-8");
        ConfInstance confInstance = mapper.readValue(data, ConfInstance.class);

        SparkConf sparkConf = new SparkConf();
        for (KeyValuePair pair : confInstance.getConf()) {
            sparkConf.set(pair.getKey(), pair.getValue());
        }
        return sparkConf;
    }
}
