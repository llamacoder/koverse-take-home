package com.koverse.dolsen;

import com.koverse.com.google.common.collect.Lists;

import com.koverse.sdk.data.SimpleRecord;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class ColorCoder implements java.io.Serializable {

  private static final long serialVersionUID = 8741666028339586272L;
  private final String textFieldName;
  private final int range1Lower;
  private final int range1Upper;
  private final int range2Lower;
  private final int range2Upper;
  private final int range3Lower;
  private final int range3Upper;
  private String color;

  /** Constructor takes the name of the field with the color coding info
   * and the upper and lower bounds of three ranges.
  **/
  public ColorCoder(String textFieldName, int intRange1Lower,
      int intRange1Upper, int intRange2Lower, int intRange2Upper,
      int intRange3Lower, int intRange3Upper) {
    this.textFieldName = textFieldName;
    this.range1Lower = intRange1Lower;
    this.range1Upper = intRange1Upper;
    this.range2Lower = intRange2Lower;
    this.range2Upper = intRange2Upper;
    this.range3Lower = intRange3Lower;
    this.range3Upper = intRange3Upper;
    this.color = "red";   //  default color for example, but get requirements
  }

  /**
   * Sets a record color based on the value in the textFieldName field compared
   * to the input ranges.
   * @param inputRecordsRdd input RDD of SimpleRecords
   * @return a JavaRDD of SimpleRecords that have an extra color field
   */
  public JavaRDD<SimpleRecord> code(JavaRDD<SimpleRecord> inputRecordsRdd) {

    // Get the field value

    JavaRDD<SimpleRecord> results = inputRecordsRdd.map(record -> {
      Object obj = record.get(textFieldName);
      if (obj != null) {
        String text = record.get(textFieldName).toString();
        if (tryParse(text) != null) {
          int val = Integer.parseInt(text);
          this.color = getColorForValue(val);
        } else {
          this.color = "red";  //  the value wasn't valid, so use default
        }
      } else {
        this.color = "red";    // the field name wasn't present, so use default
      }
      record.put("color", this.color);
      return record;
    });
    return results;

  }

  private static Integer tryParse(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  // Figure out a color based on the range values
  // Hard-coding for example, but you'd want to get requirements here
  private String getColorForValue(int val) {
    if (val >= this.range1Lower && val <= this.range1Upper) {
      this.color = "green";
    } else if (val >= this.range2Lower && val <= this.range2Upper) {
      this.color = "purple";
    } else if (val >= this.range3Lower && val <= this.range3Upper) {
      this.color = "blue";
    } else {
      this.color = "orange";
    }
    return this.color;
  }
}
