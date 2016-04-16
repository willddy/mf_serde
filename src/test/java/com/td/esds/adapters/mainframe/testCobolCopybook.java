package com.td.esds.adapters.mainframe;

import junit.framework.TestCase;

public class testCobolCopybook extends TestCase {

	CobolCopybook ccb = new CobolCopybook(
			"01 WS-VAR. 05 WS-NAME PIC X(12). 05 WS-MARKS-LENGTH PIC 9(3). 05 WS-MARK OCCURS 0 to 25 TIMES DEPENDING ON WS-MARKS-LENGTH. 10 WS-MARK PIC 999. 05 WS-NICKNAME PIC X(6)");

	public void testGetFieldNames() {
		assertEquals(ccb.getFieldNames().get(0), "ws_name");
		assertEquals(ccb.getFieldNames().get(1), "ws_marks_length");
		assertEquals(ccb.getFieldNames().get(2), "ws_mark");
		assertEquals(ccb.getFieldNames().get(3), "ws_mark_1");
		assertEquals(ccb.getFieldNames().get(28), "ws_nickname");
	}

	public void testGetFiledTypes() {
		assertEquals(ccb.getFieldTypes().get(0), "varchar(12)");
		assertEquals(ccb.getFieldTypes().get(1), "smallint");
	}

	public void testGetFiledProperties() {
        assertEquals(ccb.getFieldProperties().get(1).toString(), "{length=3, id=2, depend.id=0, prev.col=1, occurance=0}");
		assertEquals(ccb.getFieldProperties().get(0).toString(), "{length=12, id=1, depend.id=0, prev.col=0, occurance=0}");
	}
}
