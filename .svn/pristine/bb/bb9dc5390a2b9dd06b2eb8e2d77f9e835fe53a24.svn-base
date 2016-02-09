/**
 * 
 * DIRECTV PROPRIETARY
 * Copyright© 2014 DIRECTV, INC.
 * UNPUBLISHED WORK
 * ALL RIGHTS RESERVED
 * 
 * This software is the confidential and proprietary information of
 * DIRECTV, Inc. ("Proprietary Information").  Any use, reproduction, 
 * distribution or disclosure of the software or Proprietary Information, 
 * in whole or in part, must comply with the terms of the license 
 * agreement, nondisclosure agreement or contract entered into with 
 * DIRECTV providing access to this software.
 */
package com.dtv.ingestion.hbasebulk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Import each line into a hbase row, keyed by line number
 */
public class HFileGenMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	
	private byte[] familyName;
	private final ArrayList<byte[]> colNames = new ArrayList<byte[]>();
	private String inputSepPattern;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		familyName = Bytes.toBytes(context.getConfiguration().get(HFileGenDriver.BULKLOAD_FAMILYNAME));
		inputSepPattern = context.getConfiguration().get(HFileGenDriver.BULKLOAD_SEPPATTERN);
		String colNamesString = context.getConfiguration().get(HFileGenDriver.BULKLOAD_COLNAMES);
		StringTokenizer tokens = new StringTokenizer(colNamesString, ", ");
		while (tokens.hasMoreTokens()) {
			colNames.add(Bytes.toBytes(tokens.nextToken()));
		}
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		String[] tokens = value.toString().split(inputSepPattern, colNames.size());
		if (tokens == null || tokens.length == 0) {
			//empty input line, skip
			return;
		}
		
		hKey.set(Bytes.toBytes(tokens[0]));
		for (int i = 1; i < colNames.size(); i++) {
			if (i >= tokens.length) {
				//not enough fields, just skip to next data line
				return;
			}
			byte[] oneColName = colNames.get(i);
			KeyValue kv = new KeyValue(hKey.get(), familyName, oneColName, Bytes.toBytes(tokens[i]));
			context.write(hKey, kv);
		}
	}
}
