package com.ryan.widodo.FormattedFileWriter;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class OrcReader {
    OrcReader(String path){
        try {
            Reader reader = OrcFile.createReader(null, new Path(path));
            StructObjectInspector inspector = (StructObjectInspector)reader.getObjectInspector();
            RecordReader records = reader.rows();
            Object row = null;
            //These objects are the metadata for each column.  They give you the type of each column and can parse it unless you
            //want to parse each column yourself
            List fields = inspector.getAllStructFieldRefs();
            for(int i = 0; i < fields.size(); ++i) {
                System.out.print(((StructField)fields.get(i)).getFieldObjectInspector().getTypeName() + '\t');
            }

            while(records.hasNext())
            {
                row = records.next(row);
                List value_lst = inspector.getStructFieldsDataAsList(row);
                StringBuilder builder = new StringBuilder();
                //iterate over the fields
                //Also fields can be null if a null was passed as the input field when processing wrote this file
                for(Object field : value_lst) {
                    if(field != null)
                        builder.append(field.toString());
                    builder.append('\t');
                }
                //this writes out the row as it would be if this were a Text tab seperated file
                System.out.println(builder.toString());
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}