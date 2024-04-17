package index.bplusTree;

import com.sun.org.apache.bcel.internal.generic.INEG;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/*
 * A LeafNode contains keys and block ids.
 * Looks Like -
 * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
 *
 * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 8; // Start after the header (prev node, next node, next free offset)

        for (int i = 0; i < numKeys; i++) {
            offset += 2;

            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset += 2;
//            System.out.print("The len is in get_keys func :"+keyLen+"\n");
            byte[] keyBytes = this.get_data(offset, keyLen); // Read the key bytes
            offset += keyLen;
            // this is right ...
            keys[i] = convertBytesToT(keyBytes, typeClass);
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    // s
    public int[] getBlockIds() {

        int numKeys = getNumKeys();
        int[] block_ids = new int[numKeys];

        int offset=8;
        for (int i = 0; i < numKeys; i++) {
            byte[] Block_id = this.get_data(offset,2);
            int block_id = ((Block_id[0] << 8) | (Block_id[1] & 0xFF));
            block_ids[i]=block_id;
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset+=2;
            offset+=keyLen;
        }
        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {
        int numKeys = this.getNumKeys();
//        System.out.print("Num of keys are : "+numKeys+"\n");
        int offset = 8; // Start after the header (prev node, next node, next free offset)
        // Find the correct position to insert the new key
//        System.out.print("Leaf node before insertion: ");
//        this.print();
//        System.out.print("\n");
//        System.out.print("Data is: "+(Integer)key+"\n");
        int insertIndex = 0;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLenBytes = this.get_data(offset + 2, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
//            System.out.print("The length is: "+keyLen+"\n");
            byte[] keyBytes = this.get_data(offset + 4, keyLen);
            T storedKey = convertBytesToT(keyBytes, typeClass);
            // Compare keys to find the insertion point
            if (compareKeys(key, storedKey) <= 0) {
                break;
            }

            // Move to the next key
            offset += (keyLen + 4);
            insertIndex++;
        }

        // Shift existing keys and block IDs to make space for the new entry
        // copy the rest of the data
        int offset_from_copy=offset;
        int data_length=0;
        for (int i = insertIndex; i < numKeys; i++) {
            byte[] keyLenBytes = this.get_data(offset + 2, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset += (keyLen + 4);
            data_length+=(keyLen + 4);
        }

        byte[] data_to_be_written= new byte[data_length];
        System.arraycopy(this.get_data(offset_from_copy,data_length),0,data_to_be_written,0,data_length);
        // insert the data
        // write the block_id
        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) (block_id >> 8);
        blockIdBytes[1] = (byte) block_id;
        this.write_data(offset_from_copy, blockIdBytes);
        // write the len of key
        byte[] keyLenBytes = new byte[2];
        // hope this works..
        int keyLen;
        if(key.getClass()==Integer.class){
            keyLen=4;
        }else if(key.getClass()==Float.class){
            keyLen=4;
        }else if(key.getClass()==Double.class){
            keyLen=8;
        }else if(key.getClass()==Boolean.class){
            keyLen=1;
        }else if(key.getClass()==String.class){
            String tmp =(String)key;
            keyLen = tmp.length();
        }else{
            throw new RuntimeException("Key type is wrong in leaf node insertion..");

        }
//        System.out.print("The key_length in the insert func  is "+keyLen+"\n"+"And key is "+ String.valueOf(key).getBytes()[0]+"\n");
        keyLenBytes[0] = (byte) (keyLen >> 8);
        keyLenBytes[1] = (byte) keyLen;
        this.write_data(offset_from_copy + 2, keyLenBytes);
        // write the key
        // Not sure whether this is correct way to right data
//        byte[] keyBytes = String.valueOf(key).getBytes();
        // this is the right way...

        byte[] keyBytes;
        if(key.getClass()== Integer.class){
            keyBytes = toByteArray((Integer)key);
        }else if(key.getClass()==Float.class){
            keyBytes = toByteArray((Float)key);
        }else if(key.getClass()==Double.class){
            keyBytes = toByteArray((Double)key);
        }else if(key.getClass()==Boolean.class){
            keyBytes = toByteArray((Boolean)key);
        }else if(key.getClass()==String.class){
            String tmp1 =(String)key;
            keyBytes = toByteArray(tmp1);
        }else{
            throw new RuntimeException("Key type is wrong in leaf node insertion..");
        }

//        int temp = (keyBytes[3]<<24) | (keyBytes[2]<<16) | (keyBytes[1]<<8) | (keyBytes[0] & 0xFF);
//        System.out.print("temp is: "+temp+"\n");
//        System.out.print("val is: "+val+"\n");

        this.write_data(offset_from_copy + 4, keyBytes);

        // write the shifted data...
        this.write_data(offset_from_copy+4+keyLen,data_to_be_written);


        // Update the number of keys
        numKeys++;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numKeys >> 8);
        numEntriesBytes[1] = (byte) numKeys;
        this.write_data(0, numEntriesBytes);

        // Update the pointer to the next free offset
        int nextFreeOffset = offset_from_copy + 4 + keyLen + data_length;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (nextFreeOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) nextFreeOffset;
        this.write_data(6, nextFreeOffsetBytes);

//        System.out.print("Leaf node After insertion: ");
//        this.print();
//        System.out.print("\n");
        return;
    }


    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        int num_keys = getNumKeys();

        int offset=8;
        for (int i = 0; i < num_keys; i++) {
            byte[] Block_id = this.get_data(offset,2);
            int block_id = ((Block_id[0] << 8) | (Block_id[1] & 0xFF));
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset+=2;
            byte[] keyBytes = this.get_data(offset, keyLen); // Read the key bytes
            T stored_key= convertBytesToT(keyBytes, typeClass);
            offset+=keyLen;
            if (compareKeys(stored_key,key)==0){
                return block_id;
            }
        }

        return -1;
    }

    // Helper method to compare keys
    // for s1.compareTo(s2)
//    s1 == s2 : The method returns 0.
//    s1 > s2 : The method returns a positive value.
//    s1 < s2 : The method returns a negative value.
    public int compareKeys(T key1, T key2) {
        // Implement comparison logic based on the type of keys
        if (typeClass == String.class) {
            return ((String) key1).compareTo((String) key2);
        } else if (typeClass == Integer.class) {
            return ((Integer) key1).compareTo((Integer) key2);
        } else if (typeClass == Boolean.class) {
            return ((Boolean) key1).compareTo((Boolean) key2);
        } else if (typeClass == Float.class) {
            return Float.compare((Float) key1, (Float) key2);
        } else if (typeClass == Double.class) {
            return Double.compare((Double) key1, (Double) key2);
        } else {
            // Default comparison: Convert keys to string and compare lexicographically
            return String.valueOf(key1).compareTo(String.valueOf(key2));
        }
    }

    public byte[] toByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public  byte[] toByteArray(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] toByteArray(double value) {
        return ByteBuffer.allocate(8).putDouble(value).array();
    }

    public  byte[] toByteArray(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }
    public byte[] toByteArray(boolean value) {
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) (value ? 1 : 0);
        return byteArray;
    }


}