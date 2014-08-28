package cascading.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.tuple.Fields;

import parquet.avro.HMAvroParquetSupportInputFormat;
import parquet.avro.HMAvroParquetSupportOutputFormat;
import parquet.avro.HMAvroReadSupport;
import parquet.avro.HMAvroWriteSupport;

import cascading.flow.FlowProcess;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

import cascading.scheme.Scheme;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.io.NullWritable;
import org.apache.avro.mapred.AvroWrapper;
import cascading.scheme.SinkCall;
import cascading.tuple.TupleEntry;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import cascading.tap.CompositeTap;
import org.apache.hadoop.fs.PathFilter;

import parquet.hadoop.metadata.CompressionCodecName;

import java.util.HashMap;
import parquet.hadoop.mapred.Container;


public class HMAvroParquetScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  //   private static final IFn into = Clojure.var("clojure.core", "into");

  private static final String DEFAULT_RECORD_NAME = "CascadingAvroRecord";

  private static final PathFilter filter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith("_");
    }
  };


  protected Schema schema;

  protected CompressionCodecName codec = null;

  public HMAvroParquetScheme() {
    this(null);
  }
  public HMAvroParquetScheme(Schema schema) {
    this.schema = schema;
    setSinkFields(Fields.FIRST);
    setSourceFields(Fields.FIRST);
  }
  public HMAvroParquetScheme(Schema schema, String compression) {
    this(schema);
    if(compression.equalsIgnoreCase("snappy")) {
        codec = CompressionCodecName.SNAPPY;
    } else if(compression.equalsIgnoreCase("gzip")) {
        codec = CompressionCodecName.GZIP;
    }
  }


  /**
     * Helper method to read in a schema when de-serializing the object
     *
     * @param in The ObjectInputStream containing the serialized object
     * @return Schema The parsed schema.
     */
  protected static Schema readSchema(java.io.ObjectInputStream in) throws IOException {
    final Schema.Parser parser = new Schema.Parser();
    return parser.parse(in.readUTF());
  }

  /**
     * Return the schema which has been set as a string
     *
     * @return String representing the schema
     */
  String getJsonSchema() {
    if (schema == null) {
      return "";
    } else {
      return schema.toString();
    }
  }

  /**
     * This method peeks at the source data to get a schema when none has been provided.
     *
     * @param flowProcess The cascading FlowProcess object for this flow.
     * @param tap         The cascading Tap object.
     * @return Schema The schema of the peeked at data, or Schema.NULL if none exists.
     */
  private Schema getSourceSchema(FlowProcess<JobConf> flowProcess, Tap tap) throws IOException {

    if (tap instanceof CompositeTap) {
      tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
    }
    final String path = tap.getIdentifier();
    Path p = new Path(path);
    final FileSystem fs = p.getFileSystem(flowProcess.getConfigCopy());
    // Get all the input dirs
    List<FileStatus> statuses = new LinkedList<FileStatus>(Arrays.asList(fs.globStatus(p, filter)));
    // Now get all the things that are one level down
    for (FileStatus status : new LinkedList<FileStatus>(statuses)) {
      if (status.isDir())
        for (FileStatus child : Arrays.asList(fs.listStatus(status.getPath(), filter))) {
        if (child.isDir()) {
          statuses.addAll(Arrays.asList(fs.listStatus(child.getPath(), filter)));
        } else if (fs.isFile(child.getPath())) {
          statuses.add(child);
        }
      }
    }
    for (FileStatus status : statuses) {
      Path statusPath = status.getPath();
      if (fs.isFile(statusPath)) {
        // no need to open them all
        InputStream stream = null;
        DataFileStream reader = null;
        try {
          stream = new BufferedInputStream(fs.open(statusPath));
          reader = new DataFileStream(stream, new GenericDatumReader());
          return reader.getSchema();
        }
        finally {
          if (reader == null) {
            if (stream != null) {
              stream.close();
            }
          }
          else {
            reader.close();
          }
        }

      }
    }
    // couldn't find any Avro files, return null schema
    return Schema.create(Schema.Type.NULL);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess,Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // Set the input schema and input class

    conf.setInputFormat(HMAvroParquetSupportInputFormat.class);
    HMAvroParquetSupportInputFormat.setRequestedProjection(conf, schema);
    HMAvroParquetSupportInputFormat.setReadSupportClass(conf, HMAvroReadSupport.class);
  }

  /**
   * Sets to Fields.UNKNOWN if no schema is present, otherwise uses the name of the Schema.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
   * @return Fields The source cascading fields.
   */
  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    if (schema == null) {
      setSourceFields(Fields.UNKNOWN);
    } else {
      setSourceFields(new Fields(schema.getName()));
    }
    return getSourceFields();
  }


  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    if (schema == null) {
      throw new RuntimeException("Must provide sink schema");
    }
    // Set the input schema and input class
    //conf.setOutputCommitter(AvroParquetOutputCommitter.class);
    conf.setOutputFormat(HMAvroParquetSupportOutputFormat.class);
    HMAvroParquetSupportOutputFormat.setWriteSupportClass(conf, HMAvroWriteSupport.class);
    if (codec != null) HMAvroParquetSupportOutputFormat.setCompression(conf, codec);
    HMAvroParquetSupportOutputFormat.setSchema(conf, schema);
  }

  /**
   * Sink method to take an outgoing tuple and write it to Avro. In this scheme the incoming avro is passed through.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws java.io.IOException
   */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    //noinspection unchecked
    sinkCall.getOutput().collect(null, tupleEntry.getObject(Fields.FIRST));
  }

  /**
   * In this schema nothing needs to be done for the sinkPrepare.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws java.io.IOException
   */
  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
    throws IOException {
  }

  /**
   * Reads in Avro records of type T and adds them as the first field in a tuple.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sourceCall  The cascading SourceCall object. Should be passed in by cascading automatically.
   * @return boolean true on successful parsing and collection, false on failure.
   * @throws java.io.IOException
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    @SuppressWarnings("unchecked") RecordReader<Void, Container<HashMap>>  input = sourceCall.getInput();
    Container<HashMap> value = input.createValue();
    if (!input.next(null, value)) {
      return false;
    }
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    tuple.clear();
    HashMap hm = value.get();
    //     PersistentArrayMap m = (PersistentArrayMap) ClojureAvroParquetScheme.into.invoke(PersistentArrayMap.EMPTY, hm);
    tuple.add(hm);
    return true;
  }

  private void writeObject(java.io.ObjectOutputStream out)
    throws IOException {
    out.writeUTF(this.schema.toString());
  }

  private void readObject(java.io.ObjectInputStream in)
    throws IOException {
    this.schema = readSchema(in);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    HMAvroParquetScheme that = (HMAvroParquetScheme) o;

    if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;

    return true;
  }

  @Override
  public String toString() {
    return "AvroScheme{" +
      "schema=" + schema +
      '}';
  }

  @Override
  public int hashCode() {

    return 31 * getSinkFields().hashCode() +
      (schema == null ? 0 : schema.hashCode());
  }


}
