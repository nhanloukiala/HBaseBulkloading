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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SwapTableSupport {
	
	//family name of the lookup table
	//private static final String lookupFamily = "cf";
	
	//name of column containing target table name
	private static final String activeTableNameLookupColumn = "table_name";
	
	//name of column containing target family name
	private static final String activeFamilyNameLookupColumn = "family_name";
	
	//name of column containing target table name
	private static final String inactiveTableNameLookupColumn = "table_name_inactive";
	
	//name of column containing target family name
	private static final String inactiveFamilyNameLookupColumn = "family_name_inactive";
	
	//name of column containing mapper class name
	private static final String mapperClassLookupColumn = "mapper_class";
	
	//name of column containing target family name
	private static final String colNamesLookupColumn = "col_names";
	
	//name of column containing target family name
	private static final String sepPatternLookupColumn = "sep_pattern";

    private String activeTableName;
	public String getActiveTableName() {
		return activeTableName;
	}

	public void setActiveTableName(String activeTableName) {
		this.activeTableName = activeTableName;
	}

	private String activeFamilyName;
	
    public String getActiveFamilyName() {
		return activeFamilyName;
	}

	public void setActiveFamilyName(String activeFamilyName) {
		this.activeFamilyName = activeFamilyName;
	}

	private String inactiveTableName;
	private String inactiveFamilyName;
	
    private String targetColumnNames;
	
	private String mapperClassName;
	private String inputSeparatorPattern;
	
	private final String lookupTableName;
	private final String lookupKey;
	
	SwapTableSupport(String lookupTableName, String lookupKey) {
		this.lookupTableName = lookupTableName;
		this.lookupKey = lookupKey;
	}
	
	/*void loadSwapInfo(Configuration hbaseConf, String[] args) throws IOException {
		
		//name of the lookup table
		final String lookupTableName = args[0];
		
		//family name of the lookup table
		//final String lookupFamily = args[1];
		
		//key to look up
		final String lookupKey = args[1];
		
		//name of column containing target table name
		//final String tableNameLookupColumn = args[3];
		
		//name of column containing target family name
		//final String familyNameLookupColumn = args[4];
		
		//name of column containing target family name
		//final String colNamesLookupColumn = args[5];
		
		loadSwapInfo(hbaseConf, lookupTableName, lookupKey);
	}*/
    
	/**
	 * @param lookupTableName 
	 * @param lookupKey, String lookupFamily, String tableNameLookupColumn, String familyNameLookupColumn 
	 * @param args
	 * @throws IOException 
	 */
	void loadSwapInfo(Configuration hbaseConf, String lookupFamily) throws IOException {
		
		HTable lookupTable = null;
	    try {
	    	System.out.println("Creating htable instance...");
	    	lookupTable = new HTable(hbaseConf, lookupTableName);
	    	Get lookupGet = new Get(Bytes.toBytes(lookupKey));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeTableNameLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeFamilyNameLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveTableNameLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveFamilyNameLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(colNamesLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(mapperClassLookupColumn));
	    	lookupGet.addColumn(Bytes.toBytes(lookupFamily), Bytes.toBytes(sepPatternLookupColumn));
	    	
	    	//get active table name
	    	Result result = lookupTable.get(lookupGet);
	    	byte[] resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeTableNameLookupColumn));
	    	activeTableName = Bytes.toString(resultBytes);
	    	
	    	//get target family name
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeFamilyNameLookupColumn));
	    	activeFamilyName = Bytes.toString(resultBytes);
	    	
	    	//get inactive table name
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveTableNameLookupColumn));
	    	inactiveTableName = Bytes.toString(resultBytes);
	    	
	    	//get target family name
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveFamilyNameLookupColumn));
	    	inactiveFamilyName = Bytes.toString(resultBytes);
	    	
	    	//list of column names
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(colNamesLookupColumn));
	    	targetColumnNames = Bytes.toString(resultBytes);
	    	
	    	//input separator chars
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(sepPatternLookupColumn));
	    	if (resultBytes != null && resultBytes.length > 0) {
	    		inputSeparatorPattern = Bytes.toString(resultBytes);
	    	} else {
	    		inputSeparatorPattern = ";";
	    		System.out.println("No input separator chars configured, use default of ';'");
	    	}
	    	
	    	//name of mapper class
	    	resultBytes = result.getValue(Bytes.toBytes(lookupFamily), Bytes.toBytes(mapperClassLookupColumn));
	    	if (resultBytes != null && resultBytes.length > 0) {
	    		mapperClassName = Bytes.toString(resultBytes);
	    	}
	    	
	    } finally {
	    	if (lookupTable != null) {
	    		lookupTable.close();
	    	}
	    }
	    
	    /*if (activeTableName == null || targetFamilyName == null) {
	    	return;
	    }
	    
	    if (activeTableName.endsWith("1")) {
	    	inactiveTableName = activeTableName.substring(0, activeTableName.length() - 1) + "2";
	    } else {
	    	inactiveTableName = activeTableName.substring(0, activeTableName.length() - 1) + "1";
	    }*/

		System.out.println("--- Loaded bulk configuration ---");
		System.out.println("Current active table/family: " + activeTableName + "/" + activeFamilyName);
		System.out.println("Current inactive table/family: " + inactiveTableName + "/" + inactiveFamilyName);
		System.out.println("Configured mapper class: " + mapperClassName);
	}
    
	/**
	 * @param lookupTableName 
	 * @param lookupKey, String lookupFamily, String tableNameLookupColumn, String familyNameLookupColumn 
	 * @param args
	 * @throws IOException 
	 */
	void swap(Configuration hbaseConf, String lookupFamily) throws IOException {
		
		HTable lookupTable = null;
	    try {
	    	//update active name
	    	lookupTable = new HTable(hbaseConf, lookupTableName);
	    	Put lookupPut = new Put(Bytes.toBytes(lookupKey));
	    	lookupPut.add(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeTableNameLookupColumn), Bytes.toBytes(this.inactiveTableName));
	    	lookupPut.add(Bytes.toBytes(lookupFamily), Bytes.toBytes(activeFamilyNameLookupColumn), Bytes.toBytes(this.inactiveFamilyName));
	    	
	    	//update inactive name
	    	lookupPut.add(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveTableNameLookupColumn), Bytes.toBytes(this.activeTableName));
	    	lookupPut.add(Bytes.toBytes(lookupFamily), Bytes.toBytes(inactiveFamilyNameLookupColumn), Bytes.toBytes(this.activeFamilyName));
	    	lookupTable.put(lookupPut);
	    	
	    } finally {
	    	if (lookupTable != null) {
	    		lookupTable.close();
	    	}
	    }
		System.out.println("--- Swapped active/inactive table names---");
		System.out.println("New active table name: " + inactiveTableName);
		System.out.println("New inactive table name: " + activeTableName);
	}
    
    /*public String getActiveTableName() {
		return activeTableName;
	}*/

	public String getInactiveTableName() {
		return inactiveTableName;
	}

	/*public String getActiveFamilyName() {
		return activeFamilyName;
	}*/

	public String getInactiveFamilyName() {
		return inactiveFamilyName;
	}

	public String getTargetColumnNames() {
		return targetColumnNames;
	}

	public String getMapperClassName() {
		return mapperClassName;
	}
	
	public String getInputSeparatorPattern() {
		return inputSeparatorPattern;
	}

	public static void main(String[] args) throws IOException {
		SwapTableSupport swap = new SwapTableSupport(args[0], args[1]);
		swap.loadSwapInfo(HBaseConfiguration.create(), args[2]);
	}
}
