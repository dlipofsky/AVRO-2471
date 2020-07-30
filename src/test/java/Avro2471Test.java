import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

public class Avro2471Test {
    // This happens to be 1978-11-07 08:39:13.455104 UTC but also serializes as
    // { -128, -128, -128, -128, -128, -128, 127 }
    // making a easy-to-spot value in the serialized array.
    private final Instant now = Instant.ofEpochSecond(279275953, 455104000);

    @Test
    public void test1_serialize() throws IOException {
        final Test1 t = Test1.newBuilder().setTime(now).build();
        // uncomment to work-around AVRO-2471
        // t.getSpecificData().addLogicalTypeConversion(new TimestampMicrosConversion());
        byte[] bytes = serialize(t);
        Test1 other = deserialize(t.getSchema(), bytes);
        assertEquals(t, other);
    }

    @Test
    public void test1_deserialize() throws IOException {
        byte[] bytes = new byte[] { 2, -128, -128, -128, -128, -128, -128, 127 };
        // fails because of AVRO-2471
        Test1 actual = deserialize(Test1.SCHEMA$, bytes);
        assertEquals(now, actual.getTime());
    }

    @Test
    public void test1_null_serialize() throws IOException {
        final Test1 t = Test1.newBuilder().setTime(null).build();
        byte[] bytes = serialize(t);
        Test1 other = deserialize(t.getSchema(), bytes);
        assertEquals(t, other);
    }

    @Test
    public void test1_null_deserialize() throws IOException {
        byte[] bytes = new byte[] { 0 };
        Test1 actual = deserialize(Test1.SCHEMA$, bytes);
        assertNull(actual.getTime());
    }

    @Test
    public void test2_serialize() throws IOException {
        final Test2 t = Test2.newBuilder().setTime(now).build();
        byte[] bytes = serialize(t);
        Test2 other = deserialize(t.getSchema(), bytes);
        assertEquals(t, other);
    }

    @Test
    public void test2_deserialize() throws IOException {
        byte[] bytes = new byte[] { -128, -128, -128, -128, -128, -128, 127 };
        Test2 actual = deserialize(Test2.SCHEMA$, bytes);
        assertEquals(now, actual.getTime());
    }

    @Test(expected = AvroRuntimeException.class)
    public void test2_null() throws IOException {
        Test2.newBuilder().setTime(null); // not allowed
    }

    private byte[] serialize(SpecificRecord record) throws IOException {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(
                record.getSchema());
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            final byte[] bytes = out.toByteArray();
            System.out.println(bytes.length + " " + toString(bytes, 0, bytes.length));
            return bytes;
        }
    }

    private <T extends SpecificRecord> T deserialize(Schema schema, byte[] bytes)
            throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    /**
     * Converts bytes to a String in the form of a Java byte[] declaration. Printable characters
     * will print as the char value (in single quotes) and non-printable values will print as the
     * numeric byte value.
     */
    public static String toString(final byte[] msg, final int offset, final int length) {
        final StringBuilder sb = new StringBuilder(length * 4);
        sb.append('{');
        final int stop = offset + length;
        for (int i = offset; i < stop; i++) {
            final byte b = msg[i];
            if (b == '\'') { // Need to escape the single-quote character
                sb.append("'\\''");
            } else if (b == '\\') { // Need to escape the backslash character
                sb.append("'\\\\'");
            } else if (isPrintable(b)) {
                sb.append('\'').append((char) b).append('\'');
            } else {
                sb.append(b);
            }
            if (i < stop - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public static boolean isPrintable(final byte b) {
        return b >= ' ' && b <= '~';
    }
}
