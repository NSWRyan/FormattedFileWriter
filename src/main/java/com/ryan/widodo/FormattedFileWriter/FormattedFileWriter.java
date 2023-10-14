package com.ryan.widodo.FormattedFileWriter;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.logging.log4j.Logger;

public abstract class FormattedFileWriter{ 
    Logger logger;
    Schema schema;
    File targetFile;
    boolean compress;
    String parent="output/", prefix="output", suffix="";
    int currentFile=0;
    int splitSizeMB;
    long totalSizeMB=0;
    boolean split=false;
    Object reservedObject;
    String tableName="";

    public FormattedFileWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, Object reservedObject, String tableName){
        this.schema=schema;
        this.compress=compress;
        this.split=split;
        this.reservedObject=reservedObject;
        this.splitSizeMB=splitSizeMB;
        this.tableName=tableName;
        if(splitSizeMB==0)
            this.splitSizeMB=50;
    }

    /**
     * Initialize file writer.
     */
    public abstract void initWriter();
    
    /**
     * For writing records.
     * @param record
     */
    public abstract void writeRecord(Record record);

    /**
     * For writing objects.
     * @param row
     */
    public abstract void writeObjects(List<Object> row);
    
    /**
     * For closing the IO stream.
     */
    public void close(){
        if(!split){
            totalSizeMB=targetFile.length()/1000000;
        }
    }

    /**
     * For closing the IO stream.
     */
    public abstract void flush();

    /**
     * Set the file.
     * @param parent The parent directory
     * @param prefix The prefix of the file
     * @param suffix The suffix of the file (the file format e.g. avro)
     */
    public void setFile(String parent, String prefix, String suffix){
        this.parent=parent==null?this.parent:parent;
        this.prefix=prefix==null?this.prefix:prefix;
        this.suffix=suffix==null?this.suffix:suffix;
        targetFile=new File(this.parent+this.tableName+"."+this.prefix+"."+(this.currentFile++)+"."+this.suffix);
    }

    /*
     * Split the file once it reached the splitSize
     */
    public void swapFile(){
        if(split){
            long fileSizeMB=targetFile.length()/1000000;
            if(fileSizeMB>splitSizeMB){
                logger.info("Size = "+targetFile.length()/1000000);
                totalSizeMB+=fileSizeMB;
                close();
                initWriter();
            }
        }
    }
}