package com.ryan.widodo.FormattedFileWriter;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.StringJoiner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;

public class CSVWriter extends FormattedFileWriter{ 
    BufferedWriter bw;
    int colCount=0;
    
    public CSVWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, String tableName) {
        super(schema, compress, split, splitSizeMB, null, tableName);
        logger=LogManager.getLogger(this.getClass());
        colCount=schema.getFields().size();
        initWriter();
    }

    @Override
    public void initWriter() {
        try {
            if(compress){
                setFile(null, "output", "csv.snappy");
                SnappyCodec snappyCodec=new SnappyCodec();
                snappyCodec.setConf(new Configuration());
                bw=new BufferedWriter(new OutputStreamWriter(snappyCodec.createOutputStream(new FileOutputStream(targetFile))));
            }else{
                setFile(null, "output", "csv");
                bw=new BufferedWriter(new FileWriter(targetFile));
            }
            // StringJoiner sj=new StringJoiner(",");
            // for(Field field:schema.getFields()){
            //     sj.add(field.name().toString());
            // }
            // bw.write(sj.toString()+"\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(Record record) {
        StringJoiner sj=new StringJoiner(",");
        for(int i=0;i<colCount;i++){
            sj.add(record.get(i).toString());
        }
        try {
            bw.write(sj.toString()+"\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void flush() {
        try {
            bw.flush();
            swapFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }

    @Override
    public void writeObjects(List<Object> row) {
        StringJoiner sj=new StringJoiner(",");
        for(int i=0;i<colCount;i++){
            sj.add(row.get(i).toString());
        }
        try {
            bw.write(sj.toString()+"\n");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}