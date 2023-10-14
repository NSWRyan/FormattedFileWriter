package com.ryan.widodo;

import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescriptionPrettyPrint;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.StringJoiner;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;

import com.topstonesoftware.javaorc.ReadORCFile;

public class Main {
    private static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        String sSchema = "";
        {
            try {
                BufferedReader br = new BufferedReader(new FileReader(new File("schema.json")));
                String line = "";
                while ((line = br.readLine()) != null) {
                    sSchema += line;
                }
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(sSchema);
        long length=100000000;
        boolean compress=true;
        boolean split=true;
        int splitSizeMB=100;

        // test(new CSVWriter(schema, compress, split, splitSizeMB), new RecordGenerator(schema, length));
        // test(new ParquetFileWriter(schema, compress, split, splitSizeMB), new RecordGenerator(schema, length));
        // test(new AvroWriter(schema, compress, split, splitSizeMB), new RecordGenerator(schema, length));
        test(new ORCWriter(schema, compress, split, splitSizeMB,null, "orc_test"), new RecordGenerator(schema, length));
        // parquetReadTest();

        compress=true;
        int loop=4;
        while (loop--!=0){
            // test(new CSVWriter(schema, compress, split, splitSizeMB), new RecordGenerator(schema, length));
            // test(new ParquetFileWriter(schema, compress, split), new RecordGenerator(schema, length));
            // test(new AvroWriter(schema, compress, split), new RecordGenerator(schema, length));
            // test(new ORCWriter(schema, compress, split), new RecordGenerator(schema, length));
        }

        compress=false;
        loop=4;
        while (loop--!=0){
            // test(new CSVWriter(schema, compress, split), new RecordGenerator(schema, length));
            // test(new ParquetFileWriter(schema, compress, split), new RecordGenerator(schema, length));
            // test(new AvroWriter(schema, compress, split), new RecordGenerator(schema, length));
            // test(new ORCWriter(schema, compress, split), new RecordGenerator(schema, length));
        }
        // orcReadTest();
    }

    public static void test(FormattedFileWriter writer, RecordGenerator recordGenerator) {
        long writeTime, writeTime1;
        long flushTime, flushTime1;
        long closeTime, closeTime1;
        writeTime = 0;
        flushTime = 0;
        while (recordGenerator.next()) {
            for (Record record : recordGenerator.records) {
                writeTime1 = System.nanoTime();
                writer.writeRecord(record);
                writeTime += System.nanoTime() - writeTime1;
            }
            flushTime1 = System.nanoTime();
            writer.flush();
            flushTime += System.nanoTime() - flushTime1;
        }
        closeTime1 = System.nanoTime();
        writer.close();
        closeTime = System.nanoTime() - closeTime1;

        if(!writer.split){
            writer.totalSizeMB=writer.targetFile.length()/1000000;
        }

        // logger.info("Total time (ms) write: " + writeTime/1000 + " flush: " +
        // flushTime/1000 + " close: " + closeTime/1000 + " all: " +
        // (writeTime+flushTime+closeTime)/1000000 + " size: " +
        // writer.targetFile.length()/1000000);
        writer.logger.info(writer.compress+ ", " +writeTime / 1000 + ", " + flushTime / 1000 + ", " + closeTime / 1000 + ", "
                + (writeTime + flushTime + closeTime) / 1000000 + ", " + writer.totalSizeMB);

        recordGenerator=null;
        writer=null;
    }

    public static void orcReadTest(String orcFilePath) {
        File targetFile = new File(orcFilePath);
        // File targetFile = new File("output/output.orc.unc");
        try (ReadORCFile readORCFile = new ReadORCFile(targetFile.getAbsolutePath())) {
            List<Object> row;
            System.out.println(readORCFile.getNumberOfRows());
            while((row=readORCFile.readRow())!=null){
                System.out.println(row+"row");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // new OrcReader(targetFile.getAbsolutePath());
    }

    public static void parquetReadTest() {
        File targetFile = new File("output/output.parquet.snappy");
        Configuration conf = new Configuration();
        // conf.set("orc.compress", "Snappy");
        try (ReadORCFile readORCFile = new ReadORCFile(targetFile.getAbsolutePath())) {
            Path path=new Path(targetFile.getPath());
            InputFile inputFile=HadoopInputFile.fromPath(path, conf); 
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
            ParquetFileReader metaReader =  ParquetFileReader.open(HadoopInputFile.fromPath(new Path(targetFile.getPath()), conf));
            ParquetMetadata pMetaData=metaReader.getFooter();
            MessageType schema=pMetaData.getFileMetaData().getSchema();
            metaReader.close();

            GenericRecord nextRecord;
            while((nextRecord = reader.read())!=null){
                StringJoiner sj=new StringJoiner(",");
                for(int i = 0; i<schema.getFieldCount();i++){
                    sj.add(nextRecord.get(i).toString());
                }
                System.out.println(sj.toString());
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}