package com.ryan.widodo;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.logging.log4j.LogManager;

/**
 * This class is only for test record generation.
 */
public class RecordGenerator { 
    private Logger logger = LogManager.getLogger(RecordGenerator.class);
    List<Record> records;
    int batchSize=100000;
    int count=0;
    long size=0;
    Schema schema;

    RecordGenerator(Schema schema, long size){
        this.schema=schema;
        this.size=size;
    }

    public boolean next(){
        if(count>=size){
            return false;
        }
        records=null;
        records=new ArrayList<>();
        int recordCount=(int)min(batchSize,size-count);
        // logger.info(count+" add "+recordCount);
        for(int i=0;i<recordCount;i++){
            Record record=new Record(schema);
            record.put(0,(count+i) + " hi world of parquet!");
            record.put(1,(count+i));
            record.put(2,new Date().getTime()+(count+i)*(86400000));
            records.add(record);
        }
        count+=recordCount;
        return true;
    }

    public long min(long x, long y)
    {
        return Math.min(x,y);
    }
}