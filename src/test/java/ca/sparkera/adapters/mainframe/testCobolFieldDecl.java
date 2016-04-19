package ca.sparkera.adapters.mainframe;

import junit.framework.TestCase;

public class testCobolFieldDecl extends TestCase {

	public void testCobolFieldDecl() {
		CobolFieldDecl cfd0 = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd0.getFieldName(),"ws_names");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getFieldName(),"ws_names");
	}
	
	public void testGetPicClause(){
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getPicClause(),"");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getPicClause(),"");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getPicClause(),"x(10).");
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10).");
		assertEquals(cfd3.getPicClause(),"x(10).");
	}

	public void testGetValue(){
		CobolFieldDecl cfd = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd.getValue(),"");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getValue(),"");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getValue(),"");
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10).");
		assertEquals(cfd3.getValue(),"");
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10) VALUE 'ram'.");
		assertEquals(cfd4.getValue(),"'ram'.");
	}

	public void testGetFieldType(){
		CobolFieldDecl cfd0 = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd0.getFieldType(),"struct");
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getFieldType(),"struct");
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getFieldType(),"varchar(10)");
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 99V99.");
		assertEquals(cfd3.getFieldType(),"int");
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(5) VALUE 'apple'.");
		assertEquals(cfd4.getFieldType(),"varchar(5)");
		CobolFieldDecl cfd5 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10) VALUE 1234567899.");
		assertEquals(cfd5.getFieldType(),"bigint");
		CobolFieldDecl cfd6 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(2) VALUE '10'.");
		assertEquals(cfd6.getFieldType(),"tinyint");
		CobolFieldDecl cfd7 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(4) VALUE 1234.");
		assertEquals(cfd7.getFieldType(),"smallint");
		CobolFieldDecl cfd8 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(9) VALUE '1234567'.");
		assertEquals(cfd8.getFieldType(),"int");
		CobolFieldDecl cfd9 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(19) VALUE 99.");
		assertEquals(cfd9.getFieldType(),"string");
		CobolFieldDecl cfd10 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC S9(3)V9(2) VALUE -132.45.");
		assertEquals(cfd10.getFieldType(),"decimal(5,3)");
	}
	
	public void testGetFieldLength(){
		CobolFieldDecl cfd0 = new CobolFieldDecl("01 WS-NAMES.");
		assertEquals(cfd0.getFieldProperties().get("length"),(Integer)0);
		CobolFieldDecl cfd1 = new CobolFieldDecl("01 WS-NAMES PIC.");
		assertEquals(cfd1.getFieldProperties().get("length"),(Integer)0);
		CobolFieldDecl cfd2 = new CobolFieldDecl("01 WS-NAMES PIC X(10).");
		assertEquals(cfd2.getFieldProperties().get("length"),(Integer)10);
		CobolFieldDecl cfd3 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(10).");
		assertEquals(cfd3.getFieldProperties().get("length"),(Integer)10);
		CobolFieldDecl cfd4 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC X(5) VALUE 'apple'.");
		assertEquals(cfd4.getFieldProperties().get("length"),(Integer)5);
		CobolFieldDecl cfd5 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10) VALUE 'ram'.");
		assertEquals(cfd5.getFieldProperties().get("length"),(Integer)10);
		CobolFieldDecl cfd6 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10)V9(03) VALUE 'ram'.");
		assertEquals(cfd6.getFieldProperties().get("length"),(Integer)13);
		CobolFieldDecl cfd7 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 9(10)V99 VALUE 'ram'.");
		assertEquals(cfd7.getFieldProperties().get("length"),(Integer)12);
		CobolFieldDecl cfd8 = new CobolFieldDecl("01 WS-NAMES REDEFINES PIC 99999 VALUE 'ram'.");
		assertEquals(cfd8.getFieldProperties().get("length"),(Integer)5);
	}

}
