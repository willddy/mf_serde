package ca.sparkera.adapters.mainframe;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import junit.framework.TestCase;
import org.junit.Assert;

public class testCobolSerde extends TestCase {
	CobolSerDe csd = new CobolSerDe();
	Configuration conf = new Configuration();
	Properties tbl = new Properties();
	
	public void testDeserializeWritable_hardcode_01() throws SerDeException {
        try {
                tbl.setProperty("cobol.layout.literal",
                        "01 WS-OCONTROL-FILEA-FORMAT. " +
                        "10 WS-FILE-NAME PIC X(44) VALUE SPACES. " +
                        "10 FILLER PIC X(02) VALUE '~|'. " +
                        "10 WS-OUT1-CURRENT-DATE PIC 9(08) VALUE 0. " +
                        "10 FILLER PIC X(02) VALUE '~|'. " +
                        "10 WS-CNT-FILEA-EDIT PIC ZZZZZZ9999. " +
                        "10 FILLER PIC X(02) VALUE '\\n'. ");
                tbl.setProperty("fb.length","68");
                csd.initialize(conf, tbl);
                String[] result = csd.deserialize(new BytesWritable((
                        "EPSA.C.D.BEPSBU.JEPSFB02.DL(0)              ~|20160404~|0000000340\\n").getBytes("IBM-1047")))
                        .toString().replaceAll("\\[|\\]|( )+","").split(",");

                assertEquals(result[0],"EPSA.C.D.BEPSBU.JEPSFB02.DL(0)");
                assertEquals(result[2],"20160404");
                assertEquals(result[4],"340");

        } catch (UnsupportedEncodingException uee) {
            uee.printStackTrace();
        }

    }

    public void testDeserializeWritable_hardcode_02() throws SerDeException {
        try {
            tbl.setProperty("cobol.layout.literal",
                            "01  DCLPS-HRP-EE-ORGUNIT. " +
                                    "05 REC-DTL. " +
                                    "   10 EMPLID           PIC X(11). " +
                                    "   10 EMPL-RCD         PIC S9(04) USAGE COMP. " +
                                    "   10 HRP-ORGUNIT-ID   PIC X(10). " +
                                    "   10 FILLER           PIC X(14). " +
                                    "05 REC-TRL REDEFINES REC-DTL. " +
                                    "   10 REC-TYPE              PIC X(07). " +
                                    "       88 TRAILER-REC       VALUE \"TRAILER\". " +
                                    "   10 TRL-TIMESTAMP         PIC X(19). " +
                                    "   10 TRL-DETAIL-COUNT      PIC 9(11). "
            );
            tbl.setProperty("fb.length","37");
            csd.initialize(conf, tbl);
            String[] result = csd.deserialize(new BytesWritable((
                    "2869217    000000000846              ").getBytes("IBM-1047")))
                    .toString().replaceAll("\\[|\\]|( )+","").split(",");
            System.out.println(result.toString());

        } catch (UnsupportedEncodingException uee) {
            uee.printStackTrace();
        }
    }

    public void testDeserializeWritable_FromFile_01() throws SerDeException {

        testDeserializeWritable_FromFile(
                "src/test/resources/data/CTRL_DATA.DAT",
                "src/test/resources/data/CTRL_DATA.COPYBOOK.TXT", 68);
    }

    /**
     * A generic test function
     * @param testFilePath
     * @param copybookpath
     * @param recordLength
     * @return Map
     * @throws SerDeException
     */
    public Map<Integer, String[]> testDeserializeWritable_FromFile(String testFilePath, String copybookpath, Integer recordLength) throws SerDeException {

        FileInputStream fi = null;
        FileChannel fc = null;

        try {

            String[] result;
            tbl.setProperty("cobol.layout.url", copybookpath);
            tbl.setProperty("fb.length",recordLength.toString());
            csd.initialize(conf, tbl);

            Map<Integer, String[]> resultSet = new HashMap<Integer, String[]>();
            Integer key = 0;

            fi = new FileInputStream(testFilePath);
            fc = fi.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(recordLength);
            byte[] oneRow = new byte[recordLength];
            while (true) {
                buffer.clear();
                if(fc.read(buffer) == -1) break;
                buffer.flip();
                buffer.position(0);
                buffer.get(oneRow, 0, recordLength);
                result = csd.deserialize(new BytesWritable(oneRow)).toString().replaceAll("\\[|\\]|( )+","").split(",");
                System.out.println(Arrays.toString(result));
                resultSet.put(key, result);
                key ++;
            }
            return resultSet;

        } catch (BufferOverflowException boe) {
            boe.printStackTrace();
        } catch (UnsupportedEncodingException uee) {
            uee.printStackTrace();
        } catch (FileNotFoundException ffe ) {
            ffe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if (fc != null)
                    fc.close();
            } catch (IOException e) {
                // This should never happen
            }
            try {
                if (fi != null)
                    fi.close();
            } catch (IOException e) {
                // This should never happen
            }
        }

        return null;
    }

}
