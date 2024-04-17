package manager;

import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;
import index.bplusTree.BPlusTreeIndexFile;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import javafx.util.Pair;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }



    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    public int return_file_id(String table_name){
        return  file_to_fileid.get(table_name);
    }
    // number of records, offset  - LE    //
    public Object[] get_col(byte[] record_s, String table_name, int block_id, int rec_offs){
        int file_id = file_to_fileid.get(table_name);
        byte[] schema = db.get_data(file_id,0);
        int num_var_cols = 0;
        int num_col = (schema[1]<<8) | (schema[0] & 0xFF);

        for(int i = 0; i < num_col;i++){
            byte[] col_off = db.get_data(file_id,0,2+2*i,2);
            int off = (col_off[1]<<8) | (col_off[0] & 0xFF);
            byte[] col = db.get_data(file_id,0,off,2);
            if (ColumnType.values()[col[0]] == ColumnType.VARCHAR){
                num_var_cols++;
            }
        }
        int off_fixed = 4*num_var_cols;

        int num_fixed_cols = num_col - num_var_cols;
        Object[] req = new Object[num_col];
        for(int i = 0;i  < num_fixed_cols;i++){
            byte[] offset = db.get_data(file_id,0,2+2*i,2);
            int off = offset[1]<<8 | (offset[0] & 0xFF);
            byte[] colinfo = db.get_data(file_id,0,off,1);
            int datatypeByte = colinfo[0];
            if (ColumnType.values()[datatypeByte] == ColumnType.INTEGER){
                byte[] fx_col = Arrays.copyOfRange(record_s, off_fixed, off_fixed + 4);
                int temp = (fx_col[3]<<24) | (fx_col[2]<<16) | (fx_col[1]<<8) | (fx_col[0] & 0xFF);
                req[i] = temp;
                off_fixed += 4;
            }
            if (ColumnType.values()[datatypeByte] == ColumnType.FLOAT){
                byte[] fx_col = Arrays.copyOfRange(record_s, off_fixed, off_fixed + 4);
                req[i] = ByteBuffer.wrap(fx_col).getFloat();
                off_fixed += 4;
            }
            if (ColumnType.values()[datatypeByte] == ColumnType.DOUBLE){
                byte[] fx_col = Arrays.copyOfRange(record_s, off_fixed, off_fixed + 8);
                req[i] = ByteBuffer.wrap(fx_col).getDouble();
                off_fixed += 8;
            }
        }
        int offset=rec_offs;
        for(int i = 0; i < num_var_cols;i++){
            byte[] offs = db.get_data(file_id,block_id,offset,2);
            int offs_int = (offs[1]<<8) | (offs[0] & 0xFF);

            offset+=2;
            byte[] len = db.get_data(file_id,block_id,offset,2);
            offset+=2;

            int len_int = (len[1]<<8) | (len[0] & 0xFF);

            byte[] col = Arrays.copyOfRange(record_s, offs_int, offs_int + len_int);
            req[i+num_fixed_cols] = new String(col);
        }
        return req;
    }
    public static String array_to_string(byte[] byteArray) {
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        return new String(buffer.array());
    }

    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        if (!check_file_exists(table_name)) {
            return null;
        }
        List<Object[]> records = new ArrayList<>();
        byte[] num_rec = db.get_data(file_to_fileid.get(table_name), block_id,0,2);
        int num_recs = (num_rec[0]<<8)|(num_rec[1] & 0xFF);
        int end = 4096;
        for(int i = 0; i < num_recs;i++){
            byte[] b = db.get_data(file_to_fileid.get(table_name),block_id,2+2*i,2);
            int offset = (b[0] << 8)  | (b[1] & 0xFF);
            byte[] rec = db.get_data(file_to_fileid.get(table_name),block_id,offset,end-offset);
            records.add(get_col(rec, table_name, block_id,offset));
            end = offset;
        }
        return records;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        int file_id = file_to_fileid.get(table_name);
        if (check_index_exists(table_name,column_name)){
            System.out.println("Index already exists for column: " + column_name);
            return false;
        }
        if (file_to_fileid.get(table_name) != null){
            String index_file_name = table_name + "_" + column_name + "_index";

            byte[] schema = get_data_block(table_name,0);
            int num_col = (schema[1]<<8) | (schema[0] & 0xFF);
            int index_1 = 0;
            Pair<Integer, Integer> result = new Pair<>(-1, -1);
            for(int i = 0; i < num_col;i++){
                byte[] col_off = db.get_data(file_id,0,2+2*i,2);
                int off = (col_off[1]<<8) | (col_off[0] & 0xFF);
                byte[] col_dtype = db.get_data(file_id,0,off,2);
                int len_col_name = col_dtype[1];
                byte[] colname = db.get_data(file_id,0,off+2,len_col_name);
                String req_col_name = array_to_string(colname);
                if (req_col_name.compareTo(column_name) == 0){
                    int columnTypeValue = col_dtype[0];
                    result = new Pair<>(index_1, columnTypeValue);}
                index_1++;
            }
            Integer idx = result.getKey();
            ColumnType dataType = ColumnType.values()[result.getValue()];
            int counter,num_records,i;
            switch (dataType) {
                case INTEGER:
                    BPlusTreeIndexFile<Integer> bPlusTree = new BPlusTreeIndexFile<>(order, Integer.class);
                    num_records = 0;
                    i = 1;
                    while (num_records < db.get_num_records(file_id)){
                        List<Object[]> required = get_records_from_block(table_name, i);
                        for(int j = 0; j < required.size(); j++){
                            Object[] row = required.get(j);
                            Integer key = (Integer) row[idx];
                            bPlusTree.insert(key, i);
                        }
                        i++;
                        num_records += required.size();
                    }
                    counter = db.addFile(bPlusTree);
                    file_to_fileid.put(index_file_name, counter);

                    break;

                case FLOAT:
                    BPlusTreeIndexFile<Float> bPlusTree2 = new BPlusTreeIndexFile<>(order, Float.class);
                    counter = db.addFile(bPlusTree2);
                    file_to_fileid.put(index_file_name, counter);
                    num_records = 0;
                    i = 1;
                    while (num_records < db.get_num_records(file_id)){
                        List<Object[]> required = get_records_from_block(table_name, i);
                        for(int j = 0; j < required.size(); j++){
                            Object[] row = required.get(j);
                            Float key = (Float) row[idx];
                            bPlusTree2.insert(key, i);
                        }
                        i++;
                        num_records += required.size();
                    }
                    break;

                case DOUBLE:
                    BPlusTreeIndexFile<Double> bPlusTree3 = new BPlusTreeIndexFile<>(order, Double.class);
                    counter = db.addFile(bPlusTree3);
                    file_to_fileid.put(index_file_name, counter);
                    num_records = 0;
                    i = 1;
                    while (num_records < db.get_num_records(file_id)){
                        List<Object[]> required = get_records_from_block(table_name, i);
                        for(int j = 0; j < required.size(); j++){
                            Object[] row = required.get(j);
                            Double key = (Double) row[idx];
                            bPlusTree3.insert(key, i);
                        }
                        i++;
                        num_records += required.size();
                    }
                    break;
                case VARCHAR:
                    BPlusTreeIndexFile<String> bPlusTree1 = new BPlusTreeIndexFile<>(order, String.class);
                    counter = db.addFile(bPlusTree1);
                    file_to_fileid.put(index_file_name, counter);
                    num_records = 0;
                    i = 1;
                    while (num_records < db.get_num_records(file_id)){
                        List<Object[]> required = get_records_from_block(table_name, i);
                        for(int j = 0; j < required.size(); j++){
                            Object[] row = required.get(j);
                            String key = (String) row[idx];
                            bPlusTree1.insert(key, i);
                        }
                        i++;
                        num_records += required.size();
                    }
                    break;
                case BOOLEAN:
                    BPlusTreeIndexFile<Boolean> bPlusTree4 = new BPlusTreeIndexFile<>(order, Boolean.class);
                    counter = db.addFile(bPlusTree4);
                    file_to_fileid.put(index_file_name, counter);
                    num_records = 0;
                    i = 1;
                    while (num_records < db.get_num_records(file_id)){
                        List<Object[]> required = get_records_from_block(table_name, i);
                        for(int j = 0; j < required.size(); j++){
                            Object[] row = required.get(j);
                            Boolean key = (Boolean) row[idx];
                            bPlusTree4.insert(key, i);
                        }
                        i++;
                        num_records += required.size();
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + dataType);
            }

            return true;
        }
        else{
            System.out.println("Table not found: " + table_name);
            return false;
        }
    }


    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        if (!check_index_exists(table_name, column_name)) {
            System.out.println("Index does not exist for column: " + column_name);
            return -1;
        }


        // Retrieve index file
        String index_file_name = table_name + "_" + column_name + "_index";
        int index_file_id = file_to_fileid.get(index_file_name);
//
//        // Perform search in the B+ tree index
        switch (value.getType().getSqlTypeName()) {
            case INTEGER:
                //  not sure about the type casting of value
                return db.search_index(index_file_id,(Integer)value.getValue());

            case FLOAT:
                return db.search_index(index_file_id,(Float)value.getValue());
            case DOUBLE:
                return db.search_index(index_file_id,(Double)value.getValue());
            case BOOLEAN:
                return  db.search_index(index_file_id,(Boolean)value.getValue());
            case VARCHAR:
                return db.search_index(index_file_id,(String)value.getValue());
            default:
                throw new IllegalArgumentException("Unsupported data type: " + value.getType().getSqlTypeName());
        }

    }



    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index


        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}