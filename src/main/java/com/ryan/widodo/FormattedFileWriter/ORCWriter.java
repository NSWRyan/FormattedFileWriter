package com.ryan.widodo.FormattedFileWriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.orc.TypeDescription;

import com.topstonesoftware.javaorc.WriteORCFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;

public class ORCWriter extends FormattedFileWriter {
    int colCount = 0;
    VectorizedRowBatch vectorizedRowBatch;
    TypeDescription typeDescription;
    WriteORCFile orcWriter;
    final int maxBatch = 1024;
    boolean useSchema=false;
    private Configuration conf=null;
    private Path parentPath=null;

    public ORCWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, TypeDescription typeDescription, String tableName) {
        super(schema, compress, split, splitSizeMB, typeDescription, tableName);
        this.typeDescription=typeDescription;
        logger = LogManager.getLogger(this.getClass());
        initWriter();
    }

    public ORCWriter(Schema schema, boolean compress, boolean split, int splitSizeMB, TypeDescription typeDescription, String tableName, Configuration conf, Path parentPath) {
        super(schema, compress, split, splitSizeMB, typeDescription, tableName);
        this.typeDescription=typeDescription;
        logger = LogManager.getLogger(this.getClass());
        this.conf=conf;
        this.parentPath=parentPath;
        initWriter();
    }

    @Override
    public void initWriter() {
        if(conf==null)
            conf = new Configuration();
        if (compress) {
            setFile(null, "output.snappy", "orc");
            conf.set("orc.compress", "Snappy");
        }else{
            setFile(null, "output.unc", "orc");
            conf.set("orc.compress", "NONE");
        }
        logger.info(targetFile.getAbsolutePath());
        if (targetFile.exists()) {
            targetFile.delete();
        }

        colCount = schema.getFields().size();
        if(typeDescription==null){
            typeDescription = avroSchemaToTypeDescription(schema);
        }
        vectorizedRowBatch = typeDescription.createRowBatch(maxBatch);
        if(parentPath!=null)
            orcWriter = new WriteORCFile(parentPath+"/"+targetFile.getPath(), typeDescription, conf);
        else
            orcWriter = new WriteORCFile(targetFile.getPath(), typeDescription, conf);

    }

    @Override
    public void writeRecord(Record record) {
        try {
            orcWriter.writeRow(recordToListObject(record));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeObjects(List<Object> row) {
        try {
            orcWriter.writeRow(row);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Object> recordToListObject(Record record) {
        List<Object> toReturn = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            toReturn.add(record.get(i));
        }
        return toReturn;
    }

    @Override
    public void flush() {
        // Do nothing.
        swapFile();
    }

    @Override
    public void close() {
        try {
            orcWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }

    public TypeDescription avroSchemaToTypeDescription(Schema schema) {
        TypeDescription typeDescription = TypeDescription.createStruct();
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            Type type = getType(field.schema());
            switch (type) {
                case INT:
                    typeDescription.addField(field.name(), TypeDescription.createInt());
                    break;
                case LONG:
                    typeDescription.addField(field.name(), TypeDescription.createLong());
                    break;
                case STRING:
                    typeDescription.addField(field.name(), TypeDescription.createString());
                    break;
                default:
                    typeDescription.addField(field.name(), TypeDescription.createString());
            }
        }
        return typeDescription;
    }

    public Type getType(Schema schema) {
        if (schema.getType() == Type.UNION) {
            for (Schema _schema : schema.getTypes()) {
                if (_schema.getType() != Type.NULL) {
                    return _schema.getType();
                }
            }
            return Type.NULL;
        }
        return schema.getType();
    }
}