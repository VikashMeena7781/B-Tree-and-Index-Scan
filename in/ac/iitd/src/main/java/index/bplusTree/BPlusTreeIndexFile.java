package index.bplusTree;

import storage.AbstractFile;

import java.util.*;
/*
 * Tree is a collection of BlockNodes
 * The first BlockNode is the metadata block - stores the order and the block_id of the root node

 * The total number of keys in all leaf nodes is the total number of records in the records file.
 */

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return ((rootBlockIdBytes[0] << 8)) | (rootBlockIdBytes[1] & 0xFF);
    }

    // return the order of the B+ tree
    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return ((orderBytes[0] << 8)) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // will be evaluated

    public void insert(T key, int block_id) {
        if(key==null){
            return ;
        }
        int rootId = getRootId();
        int currentNodeId = rootId;
        Stack<Integer> path_nodes = new Stack<>();
//        System.out.print("root id is "+rootId+"\n");
//        System.out.print("Size of blocks is: "+blocks.size()+"\n");
//        System.out.print("Bfs before insertion: ");
//        this.print_bfs();
//        System.out.print("\n");
//        System.out.print("Key is: "+(Integer)key+"\n");
//        if(rootId>=blocks.size()){
//            throw new RuntimeException("root id >= blocks size");
//        }
//        print_bfs();
//        System.out.print("\n");
        if (isLeaf(rootId)) {
            // If the root is a leaf node, insert the key and block_id directly
            LeafNode<T> rootNode = (LeafNode<T>) blocks.get(rootId);
            if (isFull(rootId)) {
                // If the root node is full, split it and create a new root
                // need to review this function
//                System.out.print("Leaf node is full\n");
                path_nodes.push(currentNodeId);
                split_leaf_node(key,block_id,path_nodes);
            }else{
                rootNode.insert(key, block_id);

            }
        } else {
            // Start from the root and traverse down to the leaf level
            path_nodes.push(currentNodeId);
            while (!isLeaf(currentNodeId)) {
                InternalNode<T> currentNode = (InternalNode<T>) blocks.get(currentNodeId);
                int searchResult = currentNode.search(key);
                if(searchResult==-1){
                    currentNodeId = currentNode.getChildren()[currentNode.getNumKeys()];
                }else{
                    currentNodeId=searchResult;
                }
//                System.out.print("search_result is: "+searchResult+"\n");
//                currentNodeId = searchResult;
                path_nodes.push(currentNodeId);
            }

            // Insert the key and block_id into the leaf node
            LeafNode<T> leafNode = (LeafNode<T>) blocks.get(currentNodeId);
            if (isFull(currentNodeId)) {
                // If the leaf node is full, split it
                // needs to review this
                split_leaf_node(key,block_id,path_nodes);
            }else{
                leafNode.insert(key, block_id);
            }
        }
        return ;
//        System.out.print("Bfs After insertion: ");
//        this.print_bfs();
//        System.out.print("\n");
    }

    private void split_leaf_node(T key, int block_id, Stack<Integer> path) {
        // Split the full leaf node and create a new internal node as the parent
        int leafNodeId = path.peek();
        path.pop();
//        if(!isLeaf(leafNodeId)){
//            throw new RuntimeException("leaf node expected...");
//        }
        LeafNode<T> fullLeafNode = (LeafNode<T>) blocks.get(leafNodeId);
        int order = fullLeafNode.getNumKeys();
//        if(order!= fullLeafNode.getNumKeys()){
//            System.out.print("Order and num of keys not matched..");
////        }
        LeafNode<T> left_leaf_node = new LeafNode<>(typeClass);
        LeafNode<T> right_leaf_node = new LeafNode<>(typeClass);

        T[] keys = fullLeafNode.getKeys();
        int[] blockIds = fullLeafNode.getBlockIds();

        int left_size =(int)Math.floor((order+1)/2.0);
        int right_size = (int)Math.ceil((order+1)/2.0);
        // this whole thing is right ...
        if(fullLeafNode.compareKeys(key,keys[left_size-1])<0){
            for(int i=0;i<left_size-1;i++){
                left_leaf_node.insert(keys[i],blockIds[i]);
            }
            left_leaf_node.insert(key,block_id);
            for(int i=left_size-1;i<order;i++){
                right_leaf_node.insert(keys[i],blockIds[i]);
            }
        }else{
            for(int i=0;i<left_size;i++){
                left_leaf_node.insert(keys[i],blockIds[i]);
            }
            for(int i=left_size;i<order;i++){
                right_leaf_node.insert(keys[i],blockIds[i]);
            }
            right_leaf_node.insert(key,block_id);
        }
//        System.out.print("Size before add: "+blocks.size()+"\n");
//        blocks.add(left_leaf_node);
//        blocks.remove(fullLeafNode);
        blocks.set(leafNodeId,left_leaf_node);
        blocks.add(right_leaf_node);
//        System.out.print("The size after add is: "+blocks.size()+"\n");
        int right_node_id= blocks.size()-1;
        //set prev node id
        left_leaf_node.write_data(2,fullLeafNode.get_data(2,2));
//        byte[] byteArray = ByteBuffer.allocate(2).putInt(right_node_id).array();
        // set right node
        byte[] byteArray = int_to_barray(right_node_id,2);
        left_leaf_node.write_data(4,byteArray);
//        byte[] temp = ByteBuffer.allocate(2).putInt(leafNodeId).array();
        // set next node
        byte[] temp = int_to_barray(leafNodeId,2);
        right_leaf_node.write_data(2,temp);
        right_leaf_node.write_data(4,fullLeafNode.get_data(4,2));


        T key_insert = right_leaf_node.getKeys()[0];
//        System.out.print("Size of stack : "+path.size()+"\n");
        if(path.size()==0){
            // root hain...
            InternalNode<T> root = new InternalNode<>(key_insert,leafNodeId,right_node_id,typeClass);
//            blocks.remove(fullLeafNode);
//            blocks.set(leafNodeId,left_leaf_node);
//            blocks.add(right_leaf_node);
            blocks.add(root);
            int root_id = blocks.size()-1;
//            byte[] byteArray1 = ByteBuffer.allocate(2).putInt(root_id).array();
//            byte[] root_array = new byte[2];
//            root_array[0] = (byte) ((root_id >> 8) & 0xFF);
//            root_array[1] = (byte) (root_id & 0xFF);
            byte[] byteArray1 = int_to_barray(root_id,2);
//            int temp1 = (byteArray1[0] << 8) | (byteArray1[1] & 0xFF);
//            System.out.print("temp is :"+temp1+"\n");
//            System.out.print("root_id is :"+root_id+"\n");
            blocks.get(0).write_data(2,byteArray1);
        }else{
            insert_in_parent(key_insert,right_node_id,path);
        }

    }

    private void insert_in_parent(T key, int childNodeId, Stack<Integer> path_nodes) {
        if (path_nodes.size()==1){
            if(isFull(path_nodes.peek())){
                split_internal_node(path_nodes,key,childNodeId);
            }else{
                InternalNode<T> node = (InternalNode<T>)blocks.get(path_nodes.peek());
                node.insert(key,childNodeId);
            }
//            path_nodes.pop();
            return;
        }
        int parentNodeId = path_nodes.peek();
        // we should not pop this
//        path_nodes.pop();
        InternalNode<T> parentNode = (InternalNode<T>) blocks.get(parentNodeId);
        if (isFull(parentNodeId)) {
            // If the parent node is full, split it
            split_internal_node(path_nodes,key,childNodeId);
        }else{
            parentNode.insert(key, childNodeId);
        }
        return ;
    }

    private void split_internal_node(Stack<Integer> path_nodes, T key, int childNodeId) {
        int internalNodeId = path_nodes.peek();
        path_nodes.pop();
        InternalNode<T> fullInternalNode = (InternalNode<T>) blocks.get(internalNodeId);
        int order = fullInternalNode.getNumKeys();

        T[] keys = fullInternalNode.getKeys();
        int[] children = fullInternalNode.getChildren();

        InternalNode<T> left_internal_node;
        int left_size =(int)Math.floor((order+1)/2.0);
        int right_size = (int)Math.ceil((order+1)/2.0);
        InternalNode<T> right_internal_node;
        T key_req;
        int id_req;
        if(fullInternalNode.compareKeys(key,keys[left_size-1])<0){
            if(left_size==1){
                left_internal_node = new InternalNode<>(key,children[0],childNodeId,typeClass);
            }else{
                left_internal_node = new InternalNode<>(keys[0],children[0],children[1],typeClass);
                for(int i = 1; i < left_size-1;i++){
                    left_internal_node.insert(keys[i],children[i+1]);
                }
                left_internal_node.insert(key,childNodeId);
            }
            key_req = keys[left_size-1];
//            System.out.print("left_size is: "+left_size+" Right size is: "+right_size+"\n");
//            System.out.print("order is: "+keys.length+"\n");
//            System.out.print("Bfs: ");
//            fullInternalNode.print();
//            System.out.print("\nKey is: "+(Integer)key+"\n");
//            if(left_size+1>=children.length){
//                throw new RuntimeException("Size mismatch");
//            }
            right_internal_node = new InternalNode<>(keys[left_size],children[left_size],children[left_size+1],typeClass);
            for(int i  = left_size+1;i < keys.length;i++){
                right_internal_node.insert(keys[i],children[i+1]);
            }
        }
        else{
            left_internal_node = new InternalNode<>(keys[0],children[0],children[1],typeClass);
            for(int i = 1; i < left_size;i++){
                left_internal_node.insert(keys[i],children[i+1]);
            }
            boolean temp;
            if(fullInternalNode.compareKeys(key,keys[left_size])<=0){
                key_req = key;
                temp=true;
            }else{
                key_req=keys[left_size];
                temp=false;
            }
            if(temp){
                right_internal_node = new InternalNode<>(keys[left_size],children[left_size+1],childNodeId,typeClass);
                for(int i  = left_size+1;i < keys.length;i++){
                    right_internal_node.insert(keys[i],children[i+1]);
                }
            }else{
                if(right_size==2){
                    right_internal_node = new InternalNode<>(key,children[left_size+1],childNodeId,typeClass);
                }else{
                    right_internal_node = new InternalNode<>(keys[left_size+1],children[left_size+1],children[left_size+2],typeClass);
                    for(int i  = left_size+2;i < keys.length;i++){
                        right_internal_node.insert(keys[i],children[i+1]);
                    }
                    right_internal_node.insert(key,childNodeId);
                }

            }

        }

        if(path_nodes.size()==0){
            blocks.set(internalNodeId,left_internal_node);
            blocks.add(right_internal_node);
            id_req = blocks.size()-1;
            InternalNode<T> root_node = new InternalNode<>(key_req,internalNodeId,id_req,typeClass);
            blocks.add(root_node);
            int root_id = blocks.size()-1;
            byte[] byteArray1 = int_to_barray(root_id,2);
            blocks.get(0).write_data(2,byteArray1);
        }else{
            blocks.set(internalNodeId,left_internal_node);
            blocks.add(right_internal_node);
            id_req = blocks.size()-1;
            insert_in_parent(key_req,id_req,path_nodes);
        }


    }


    public static byte[] int_to_barray(int value, int length) {
        byte[] byteArray = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            byteArray[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return byteArray;
    }
    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {
        // need to ensure that it is the leftmost block id
        int rootId = getRootId();
        int currentNodeId = rootId;

        while (!isLeaf(currentNodeId)) {
            InternalNode<T> currentNode = (InternalNode<T>) blocks.get(currentNodeId);
            int searchResult = currentNode.search(key);
            if(searchResult==-1){
                currentNodeId = currentNode.getChildren()[currentNode.getNumKeys()];
            }else{
                currentNodeId = searchResult;
            }
        }
        LeafNode<T> leaf_node = (LeafNode<T>) blocks.get(currentNodeId);
        int result = leaf_node.search(key);
        if(result==-1){
            return -1;
        }
        return currentNodeId;
    }


    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}