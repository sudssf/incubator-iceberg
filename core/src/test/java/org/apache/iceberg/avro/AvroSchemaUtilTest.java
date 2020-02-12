package org.apache.iceberg.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.junit.Test;


import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test avro schema logical types are parsed correctly during schema conversion.
 */
public class AvroSchemaUtilTest {
    @Test
    public void testSchemaConversion() throws IOException {
        String oldAvro =
                String.join("\n",
                        " { ",
                        " \"type\": \"record\", ",
                        " \"name\": \"ExampleRecord\", ",
                        " \"fields\" : [ ",
                        " {\"name\": \"myField1\", \"type\" : { \"type\" : \"long\", \"logicalType\" : \"timestamp-millis\",  \"adjust-to-utc\":true } }, ",
                        " {\"name\": \"otherField\", \"type\": \"string\"} ",
                        " ] ",
                        " }");

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(oldAvro);
        org.apache.iceberg.Schema iceSchema = AvroSchemaUtil.toIceberg(avroSchema);
        assertEquals(org.apache.iceberg.types.Types.TimestampType.withZone(), iceSchema.findField("myField1").type());
        System.out.println(avroSchema.toString());
    }
}
