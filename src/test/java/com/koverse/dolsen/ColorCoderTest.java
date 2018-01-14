package com.koverse.dolsen;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.data.SimpleRecord;
import java.util.Optional;

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
public class ColorCoderTest  extends SharedJavaSparkContext {

  @Test
  public void rddTest() {
    // Create the SimpleRecords we will put in our input RDD
    SimpleRecord record0 = new SimpleRecord();
    SimpleRecord record1 = new SimpleRecord();
    record0.put("calories", 15.22);
    record0.put("id", 0);
    record1.put("calories", 135);
    record1.put("id", 1);

    // Create the ranges
    int r1L = 0;
    int r1U = 25;
    int r2L = 26;
    int r2U = 100;
    int r3L = 101;
    int r3U = 250;

    // Create the input RDD
    JavaRDD<SimpleRecord> inputRecordsRdd = jsc().parallelize(Lists.newArrayList(record0, record1));

    // Create and run the color coder to get the output RDD
    ColorCoder colorCoder = new ColorCoder("calories", r1L, r1U, r2L, r2U, r3L, r3U);
    JavaRDD<SimpleRecord> outputRecordsRdd = colorCoder.code(inputRecordsRdd);

    assertEquals(outputRecordsRdd.count(), 2);

    List<SimpleRecord> outputRecords = outputRecordsRdd.collect();
    Optional<SimpleRecord> colorRecordOptional = outputRecords.stream()
     .filter(record -> record.get("color").equals("blue"))
     .findFirst();

    assertTrue(colorRecordOptional.isPresent());
    //assertEquals(colorRecordOptional.get().get("count"), 2);

  }
}
