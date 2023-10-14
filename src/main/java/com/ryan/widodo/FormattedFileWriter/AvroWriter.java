package com.ryan.widodo.FormattedFileWriter;

import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.logging.log4j.LogManager;

public class AvroWriter extends FormattedFileWriter{ 
    DatumWriter<GenericData.Record> writer;
    DataFileWriter<GenericData.Record> avroFileWriter;
    int colCount=0;
    
    public AvroWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, String tableName) {
        super(schema, compress, split, splitSizeMB, null, tableName);
        logger=LogManager.getLogger(this.getClass());
        colCount=schema.getFields().size();
        initWriter();
    }

    @Override
    public void initWriter(){
        CodecFactory codecFactory;
        if(compress){
            codecFactory=CodecFactory.snappyCodec();
            setFile(null, "output.snappy", "avro");
        }else{
            codecFactory=CodecFactory.nullCodec();
            setFile(null, "output.unc", "avro");
        }
        try {
            writer= new GenericDatumWriter<>(schema);
            avroFileWriter=new DataFileWriter<GenericData.Record>(writer);
            avroFileWriter.setCodec(codecFactory);
            avroFileWriter.create(schema, targetFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(Record record) {
        try {
            avroFileWriter.append(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            avroFileWriter.flush();
            swapFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            avroFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }

    @Override
    public void writeObjects(List<Object> row) {
        Record record=new Record(schema);
        for(int i=0;i<row.size();i++){
            record.put(i,row.get(i));
        }
        writeRecord(record);
    }
}