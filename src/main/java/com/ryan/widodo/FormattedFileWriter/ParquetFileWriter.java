package com.ryan.widodo.FormattedFileWriter;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;


public class ParquetFileWriter extends FormattedFileWriter {
    ParquetWriter<GenericData.Record> writer;

    public ParquetFileWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, String tableName) {
        super(schema, compress, split, splitSizeMB, null, tableName);
        logger = LogManager.getLogger(this.getClass());
        initWriter();
    }
    
    @Override
    public void initWriter() {
        CompressionCodecName compressionCodecName;
        Configuration conf=new Configuration();
        if (compress) {
            compressionCodecName = CompressionCodecName.SNAPPY;
            setFile(null, "output.snappy", "parquet");
        }else{
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
            setFile(null, "output.unc", "parquet");
        }
        if(targetFile.exists()){
            targetFile.delete();
        }

        Path path=new Path(targetFile.getPath());
        
        try {
            writer = AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path, new Configuration()))
                    .withSchema(schema)
                    .withCompressionCodec(compressionCodecName)
                    .build();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(Record record) {
        try {
            writer.write(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        super.close();
    }

    @Override
    public void flush() {
        // Do nothing.
        swapFile();
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