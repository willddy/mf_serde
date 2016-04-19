package ca.sparkera.adapters.mainframe;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import junit.framework.TestCase;

public class testCobolSerde extends TestCase {
	CobolSerDe csd = new CobolSerDe();
	Configuration conf = new Configuration();
	Properties tbl = new Properties();
	
	public void testDeserializeWritable() throws SerDeException {
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

}
