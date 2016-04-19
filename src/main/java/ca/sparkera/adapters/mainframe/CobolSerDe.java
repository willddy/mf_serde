/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.sparkera.adapters.mainframe;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * CobolSerde is under construction
 */
public final class CobolSerDe extends AbstractSerDe {

	private ObjectInspector inspector;
	private int numCols;
	private List<TypeInfo> columnTypes;
	private List<Map<String, Integer>> columnProperties;

	private CobolDeserializer cobolDeserializer = null;
	private CobolCopybook ccb;

	@Override
	@SuppressWarnings( "deprecation" )
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {

		// final int fixedRecordlLength =
		// Integer.parseInt(tbl.getProperty("fb.length"));
		// conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH,
		// fixedRecordlLength);
		try {
			this.ccb = new CobolCopybook(
					CobolSerdeUtils.determineLayoutOrThrowException(conf, tbl));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		numCols = ccb.getFieldNames().size();
		this.inspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(ccb.getFieldNames(),
						ccb.getFieldOIs());
		this.columnTypes = ccb.getFieldTypeInfos();
		this.columnProperties = ccb.getFieldProperties();

	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		// Serializing to cobol layout format is out-of-scope
		throw new SerDeException("Serializer not built");
		// return new Text("Out-of-scope");
	}

	@Override
	public Object deserialize(final Writable blob) throws SerDeException {
		return getDeserializer().deserialize(ccb.getFieldNames(),
				this.columnTypes, this.columnProperties, this.numCols, blob);
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	public SerDeStats getSerDeStats() {
		return null;
	}

	private CobolDeserializer getDeserializer() {
		if (cobolDeserializer == null) {
			cobolDeserializer = new CobolDeserializer();
		}

		return cobolDeserializer;
	}

}
