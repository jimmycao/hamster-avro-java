package com.pivotal.hamster.avro;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class Test {

  public static void main(String[] args) throws IOException {

    // InputStream in = new FileInputStream("/tmp/dump2.avro");
    InputStream in = new FileInputStream(
        "/Users/caoj7/workspace/hamster-avro/data/person.db");
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    int c = 0;
    while (c != -1) {
      c = in.read();
      if (c != -1) {
        out.write(c);
      }
    }
    in.close();

    out.flush();
    byte[] data = out.toByteArray();
    System.out.println("data size:" + data.length);

    Schema.Parser parser = new Schema.Parser();
    InputStream schemaIn = new FileInputStream(
        "/Users/caoj7/workspace/hamster-avro/schema/person.schema");
    Schema schema = parser.parse(schemaIn);

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
        schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericRecord value = datumReader.read(null, decoder);
    System.out.println(value);

  }

}
