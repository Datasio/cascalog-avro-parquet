package parquet.avro;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import java.util.Map;
import org.apache.avro.Schema;

public class HMAvroParquetSupportOutputFormat extends DeprecatedParquetOutputFormat<Map>{

  // @inherited public static void setWriteSupportClass(Configuration configuration,  Class<?> writeSupportClass);

  public static void setSchema(Configuration configuration, Schema schema) {
    HMAvroWriteSupport.setSchema(configuration, schema);
  }
}
