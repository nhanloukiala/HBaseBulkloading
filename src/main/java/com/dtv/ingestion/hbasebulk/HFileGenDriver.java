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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * HFile generating driver
 */
public class HFileGenDriver {

	static final String BULKLOAD_FAMILYNAME = "bulkload.familyName";
	static final String BULKLOAD_COLNAMES = "bulkload.colNames";
	static final String BULKLOAD_SEPPATTERN = "bulkload.sepPattern";

	/**
	 * @param args
	 *            args[0]: lookup table name args[1]: lookup key args[2]: input
	 *            folder args[3]: outputRoot folder
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		// name of the lookup table
		final String lookupTableName = args[0];

		// name of lookup family
		final String lookupFamily = args[1];

		// key to look up
		final String lookupKey = args[2];

		// input folder
		final String inputFolder = args[3];

		// temporary output folder
		final String outputRoot = args[4];
		final String outputFolder = outputRoot;

		boolean isSequenceFileInput = Boolean.parseBoolean(args[5]);

		boolean generateHFileOnly = true;
		if (args.length >= 7) {
			generateHFileOnly = Boolean.parseBoolean(args[6]);
		}

		// Print parameters to logs
		System.out.println();
		System.out.println();
		System.out.println();
		System.out
				.println("********************************************BULKLOAD PARAMS***********************************");
		System.out.println("Lookup Table: " + lookupTableName);
		System.out.println("Lookup Family: " + lookupFamily);
		System.out.println("Lookup Key: " + lookupKey);
		System.out.println("Input Folder: " + inputFolder);
		System.out.println("Output Folder" + outputRoot);
		System.out.println("Generate HFILE Only: " + generateHFileOnly);
		System.out
				.println("***********************************************************************************************");

		SwapTableSupport swapSupport = new SwapTableSupport(lookupTableName,
				lookupKey);
		Configuration hbaseConf = HBaseConfiguration.create();
		// hbaseConf.addResource("hbase-site.xml");
		// hbaseConf.set("hbase.zookeeper.quorum",
		// "hdpnn-dv-msdc02.ds.dtveng.net,hdpnn-dv-msdc01.ds.dtveng.net,hdpdn-dv-msdc03.ds.dtveng.net");
		// hdpnn-dv-msdc02.ds.dtveng.net,hdpnn-dv-msdc01.ds.dtveng.net,hdpdn-dv-msdc03.ds.dtveng.net
		// hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		swapSupport.loadSwapInfo(hbaseConf, lookupFamily);

		Configuration hdfsConf = new Configuration();
		hdfsConf.set("hbase.table.name", swapSupport.getActiveTableName());
		hdfsConf.set("key.value.separator.in.input.line", ",");

		// hadoop credential token
		String hadoopToken = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
		if (hadoopToken != null) {
			hdfsConf.set("mapreduce.job.credentials.binary", hadoopToken);
		}

		// Load hbase-site.xml
		HBaseConfiguration.addHbaseResources(hdfsConf);

		Job job = new Job(hdfsConf, "HFile Generator");
		String mapperShortName = swapSupport.getMapperClassName();
		Class<? extends Mapper<?, ?, ?, ?>> mapperClass;
		if (mapperShortName != null) {
			mapperClass = (Class<? extends Mapper<?, ?, ?, ?>>) Class
					.forName("com.dtv.ingestion.hbasebulk." + mapperShortName);
			System.out.println("Loaded configured mapper: "
					+ mapperClass.getName());
		} else {
			// no specific mapper class given, default to HFileGenMapper
			mapperClass = HFileGenMapper.class;
			System.out.println("No mapper class configured, use default "
					+ mapperClass.getName());
		}
		job.setJarByClass(mapperClass);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setInputFormatClass(isSequenceFileInput ? SequenceFileAsTextInputFormat.class
				: TextInputFormat.class);
		
		HTable hTable = new HTable(hbaseConf, swapSupport.getActiveTableName());

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		if (isSequenceFileInput) {
			SequenceFileAsTextInputFormat.addInputPath(job, new Path(
					inputFolder));
			job.setMapperClass(HFileGenMapperMultiline.class);
		} else {
			TextInputFormat.addInputPath(job, new Path(inputFolder));
			job.setMapperClass(HFileGenFromTextMapperMultiline.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		// pass family name, column names of inactive table to mappers to build
		// hfile rows based on these names
		setConfiguration(job.getConfiguration(), BULKLOAD_FAMILYNAME,
				swapSupport.getActiveFamilyName());
		String targetColNames = swapSupport.getTargetColumnNames();
		if (targetColNames != null) {
			setConfiguration(job.getConfiguration(), BULKLOAD_COLNAMES,
					targetColNames);
		}
		setConfiguration(job.getConfiguration(), BULKLOAD_SEPPATTERN,
				swapSupport.getInputSeparatorPattern());

		job.waitForCompletion(true);

		if (generateHFileOnly) {
			System.out.println("HFile is generated at " + outputFolder);
			System.out
					.println("Bulkload is NOT invoked, because of generateHFileOnly is flagged.");
			return;
		}

		BulkLoadAndSwapTable.finishBulkload(hbaseConf, swapSupport,
				outputFolder, lookupFamily);
	}

	private static void setConfiguration(Configuration conf,
			String propertyName, String value) {
		if (value == null || "".equals(value)) {
			throw new RuntimeException("Missing configuration property: "
					+ propertyName);
		}
		conf.set(propertyName, value);
	}
}
