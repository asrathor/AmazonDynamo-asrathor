package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by adityasinghrathore on 09/04/18.
 */

/*
 * Taken from PA3.
 * Instead of storing information about sequence numbers and messages (in previous project), it will contain information abouth the previous, current and next nodes.
 */

class MessageHandler implements Serializable {

    /*
     * The key-value pair are self-explanatory.
     * The hash map is used to store the key-value pair.
     * The operation type is added to differentiate between joining, inserting, deleting, querying.
     * The current node is the emulator.
     * The previous node is the predecessor in the chord ring.
     * The next node is the successor in the chord ring.
     * The destination node either be the next node when the key doesn't belong to the current node, or the current node.
     */
    public String key;
    public String value;
    public HashMap<String,String> dHT;
    public String operation_type;
    public String currentNode;
    public String previousNode;
    public String nextNode;
    public String destinationNode;

    /*
     * For initializing the message with all empty values.
     */
    public MessageHandler(){

        key = "";
        value = "";
        dHT = new HashMap<String, String>();
        currentNode = "";
        previousNode = "";
        nextNode = "";
        destinationNode = "";
    }
    /*
     * For initializing the message with respective information about nodes, key-value pair, map and operation type.
     * The design is in such a way that the current node and the previous node are initialized as the same.
     */
    public MessageHandler(String Key, String Value, HashMap<String,String> DHT, String Operation_Type, String Previous_Node, String Next_Node, String Dest_Node){

        /*
         * The usage of this is for reference to the current object (https://docs.oracle.com/javase/tutorial/java/javaOO/thiskey.html)
         */
        this();
        this.key = Key;
        this.value = Value;
        this.dHT = DHT;
        this.operation_type = Operation_Type;
        this.currentNode = Previous_Node;
        this.previousNode = Previous_Node;
        this.nextNode = Next_Node;
        this.destinationNode = Dest_Node;
    }

}

