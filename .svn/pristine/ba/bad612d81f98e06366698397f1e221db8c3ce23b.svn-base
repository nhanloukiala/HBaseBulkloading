
#$1 lookupTableName
#$2 lookupKey
#$3 inputFolder
#$4 temporary output

export HADOOP_CLASSPATH=/etc/hbase/conf/:/opt/cloudera/parcels/CDH-4.6.0-1.cdh4.6.0.p0.26/lib/hbase/*

hadoop jar bulkload-0.1-SNAPSHOT.jar com.dtv.ingestion.hbasebulk.HFileGenDriver $1 $2 $3 /data/dv/recommendation/staging/bulkload/$2 false