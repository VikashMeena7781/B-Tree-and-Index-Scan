package index.bplusTree;

import java.nio.ByteBuffer;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }

    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){

        /* Write your code here */
        if (bytes == null) {
            throw new IllegalArgumentException("Input byte array is null");
        }
        if (typeClass == String.class) {
            return (T) new String(bytes);
        } else if (typeClass == Integer.class) {
//            System.out.print("In the convertBytesToT func for int \n");
            return typeClass.cast(ByteBuffer.wrap(bytes).getInt());
        } else if (typeClass == Boolean.class) {
            return typeClass.cast(bytes[0] != 0);
        } else if (typeClass == Float.class) {
            return typeClass.cast(ByteBuffer.wrap(bytes).getFloat());
        } else if (typeClass == Double.class) {
            return typeClass.cast(ByteBuffer.wrap(bytes).getDouble());
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + typeClass.getName());
        }
    }

}