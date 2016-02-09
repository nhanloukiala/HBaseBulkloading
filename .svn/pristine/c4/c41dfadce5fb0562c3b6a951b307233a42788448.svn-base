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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Import each line into one hbase cell, keyed by first field of line
 */
public class HFileGenMapperMultiline extends Mapper<Text, Text, ImmutableBytesWritable, KeyValue> {
	
	private byte[] familyName;
	private String inputSepPattern;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		familyName = Bytes.toBytes(context.getConfiguration().get(HFileGenDriver.BULKLOAD_FAMILYNAME));
		inputSepPattern = context.getConfiguration().get(HFileGenDriver.BULKLOAD_SEPPATTERN);
	}
	
	/*public static void main(String[] args) {
		String[] s = "1;2;3;4".split(";", 3);
		System.out.println(s.length + " " + s[2]);
		//got 3 3;4
	}*/

	/** {@inheritDoc} */
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		//get rowkey from first field
		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		String[] tokens = value.toString().split(inputSepPattern, 3);
		if (tokens.length < 3) {
			//ignore incomplete data line
			return;
		}
		hKey.set(Bytes.toBytes(tokens[0]));
		KeyValue kv = new KeyValue(hKey.get(), familyName, Bytes.toBytes(tokens[1]), Bytes.toBytes(tokens[2]));
		context.write(hKey, kv);
		
		/*StringTokenizer tokens = new StringTokenizer(value.toString(), inputSepChars);
		if (!tokens.hasMoreTokens()) {
			//missing rowkey, can't do anything, skip
			return;
		}	
		hKey.set(Bytes.toBytes(tokens.nextToken()));
		
		//get column name from second field
		if (!tokens.hasMoreTokens()) {
			//missing column name, can't do anything, skip
			return;
		}
		final String colName = tokens.nextToken();
		
		//get column value from third field
		if (!tokens.hasMoreTokens()) {
			//missing column value, don't want to add empty cell to hbase -> skip
			return;
		}
		final String colValue = tokens.nextToken();

		KeyValue kv = new KeyValue(hKey.get(), familyName, Bytes.toBytes(colName), Bytes.toBytes(colValue));
		context.write(hKey, kv);*/
	}
}
