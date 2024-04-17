package index.bplusTree;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/*
 * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
 * Only write code where specified

 * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;
    // write by Vikash meena . Not provided by them need to Review.
    public InternalNode(Class<T> typeClass) {
        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;
        this.write_data(0, numKeysBytes);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;
        this.write_data(2, nextFreeOffsetBytes);
    }


    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 4; // Start after the header (num keys, next free offset, left child ID)
        for (int i = 0; i < numKeys; i++) {
            // Read key length
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset += 2;

            // Read key bytes
            byte[] keyBytes = this.get_data(offset, keyLen);
            offset += keyLen;

            // Convert key bytes to type T
            keys[i] = convertBytesToT(keyBytes, typeClass);
        }


        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        int numKeys = getNumKeys();

        // Find the position to insert the key
//        System.out.print("Internal Node before insertion: ");
//        this.print();
//        System.out.print("\n");
//        System.out.print("Data is: "+(Integer)key+"\n");
        int insertIndex = 0;
        int offset = 6; // Start after the header
        for (int i = 0; i < numKeys; i++) {
            // Read key length
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset += 2;
            // Read key bytes
            byte[] storedKeyBytes = this.get_data(offset, keyLen);
            T storedKey = convertBytesToT(storedKeyBytes, typeClass);
            // Compare keys to find the insertion point
            if (compareKeys(key, storedKey) <= 0) {
                break;
            }
            // Move to the next key
            offset += keyLen;
            offset+=2;
            insertIndex++;
        }
        // Insert the key and update the pointer to the right child ID
        int offset_copy_from;
        if(numKeys==0){
            offset_copy_from = offset;
        }else if(insertIndex==numKeys){
            offset_copy_from = offset;
        }else{
            offset_copy_from=offset-2;
            offset=offset-2;
        }

        int data_length=0;
        for (int i = insertIndex; i < numKeys; i++) {
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset += (keyLen + 4);
            data_length+=(keyLen + 4);
        }

//        System.arraycopy();
        byte[] data_to_written = new byte[data_length];
        System.arraycopy(this.get_data(offset_copy_from,data_length),0,data_to_written,0,data_length);


        // write the length of the key
        byte[] keyLenBytes = new byte[2];
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
//        int keyLen = String.valueOf(key).getBytes().length;
        // for integer...
        keyLenBytes[0] = (byte) (keyLen >> 8);
        keyLenBytes[1] = (byte) keyLen;
        this.write_data(offset_copy_from, keyLenBytes);

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
        // write the data..
        this.write_data(offset_copy_from + 2, keyBytes);

        // write the right child
        byte[] rightChildIdBytes = new byte[2];
        rightChildIdBytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        rightChildIdBytes[1] = (byte) (right_block_id & 0xFF);
        this.write_data(offset_copy_from+2+keyLen, rightChildIdBytes);

        // Increment the number of keys
        this.write_data(offset_copy_from+2+keyLen+2,data_to_written);


        numKeys++;
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) (numKeys >> 8);
        numKeysBytes[1] = (byte) (numKeys & 0xFF);
        this.write_data(0, numKeysBytes);

        // Update the pointer to the next free offset
        int temp=offset_copy_from+2+keyLen+2+data_length;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (temp >> 8);
        nextFreeOffsetBytes[1] = (byte) (temp & 0xFF);
        this.write_data(2, nextFreeOffsetBytes);

        // print the keys of this internal_node after insertion
//        System.out.print("Internal Node After Insertion: ");
//        this.print();
//        System.out.print("\n");


    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        int numKeys = this.getNumKeys();
//        System.out.print("Num of keys are: "+numKeys+"\n");
        int offset = 4; // Start after the header (num keys)
        for (int i = 0; i < numKeys; i++) {
            // Read key length
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);

            // Read key bytes
            offset+=2;
//            System.out.print("Key_len in search of internal_node is: "+keyLen+" and offset is: "+offset+"\n");
            byte[] storedKeyBytes = this.get_data(offset, keyLen);
            if(storedKeyBytes==null){
                throw new IllegalArgumentException("Stored byte array is null");
            }
            // isme dikkat hain ...
//            byte[] fx_col = storedKeyBytes;
//            int stored_actual_key = (fx_col[0]<<24) | (fx_col[1]<<16) | (fx_col[2]<<8) | (fx_col[3] & 0xFF);
//            System.out.print("Actual key is "+stored_actual_key+"\n");
            // well it is right
            T storedKey = convertBytesToT(storedKeyBytes, typeClass);
            // Compare keys

//            System.out.print("Key is: "+(Integer)key+"\n");
//            System.out.print("Stored key is: "+(Integer)storedKey+"\n");
            // Not sure whether to use <= or <
            if (compareKeys(key, storedKey) < 0) {
                // If the search key is less than the current key,
                // go to the left child

                byte[] childIdBytes = this.get_data(offset-4, 2);
                int leftChildId = ((childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF));
//                System.out.print("return id from search in internal node "+leftChildId+"\n");
                return leftChildId;
            }else if(compareKeys(key, storedKey) == 0){
                byte[] childIdBytes = this.get_data(offset+keyLen, 2);
                int rightChildId = ((childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF));
                return rightChildId;
            }

            // Move to the next key and child pointer
            offset += keyLen;
        }

        // If the search key is greater than or equal to all keys in the node,
        // go to the rightmost child
//        byte[] childIdBytes = this.get_data(offset, 2);
//        int rightChildId = ((childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF));
        // return -1 that's mean that the rightmost child is return..
        return -1;
    }


    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        int numKeys = getNumKeys();

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int offset = 4; // Start after the header (num keys)
        for (int i = 0; i <= numKeys; i++) {
            byte[] childIdBytes = this.get_data(offset, 2);
            int childId = ((childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF));
            children[i] = childId;
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF));
            offset+=(2+keyLen);
        }
        return children;

    }
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