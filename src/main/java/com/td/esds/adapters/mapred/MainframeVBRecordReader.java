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

package com.savy3.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A reader to read VB length records from a split. Record offset is returned as
 * key and the record as bytes is returned in value.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MainframeVBRecordReader implements
		RecordReader<LongWritable, BytesWritable> {

	// Make use of the new API implementation to avoid code duplication.
	private com.savy3.mapreduce.MainframeVBRecordReader reader;

	public MainframeVBRecordReader(Configuration job, FileSplit split)
			throws IOException {
		reader = new com.savy3.mapreduce.MainframeVBRecordReader();
		reader.initialize(job, split.getStart(), split.getLength(),
				split.getPath());
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public synchronized boolean next(LongWritable key, BytesWritable value)
			throws IOException {
		boolean dataRead = reader.nextKeyValue();
		if (dataRead) {
			LongWritable newKey = reader.getCurrentKey();
			BytesWritable newValue = reader.getCurrentValue();
			key.set(newKey.get());
			value.set(newValue);
		}
		return dataRead;
	}

	@Override
	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	@Override
	public synchronized long getPos() throws IOException {
		return reader.getPos();
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

}
