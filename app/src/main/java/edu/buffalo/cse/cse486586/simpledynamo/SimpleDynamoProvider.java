package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.util.HashMap;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.Build;
import android.util.Log;
import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {

	/*
	 * Note: Alot of code and comments are similar to the PA3 since the whole code has the skeleton of PA3 which changes.
	 */

	/*
     * Similar to previous projects.
     */
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	/*
     * The app listens to one server socket on port 10000.
     */
	static final int SERVER_PORT = 10000;

	/*
     * Each emulator has a specific remote port it should connect to.
     * In this project, we will only need to explicitly call the port 11108 in the onCreate which is explained later.
     */
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";

	/*
     * The hash map will be used to store the key, value pair. A concurrent hash map is not necessary since that is not being tested.
     * Both the key and value are strings as per the requirement of the project.
     * TreeMap is for storing the local data after the insert is done.
     * NodeMap is for creating a map of nodes as 11124->11112->11108->11116->11120.
     * NOTE: All the functions for the hash map are referred from (https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html)
     * https://developer.android.com/reference/java/util/TreeMap
     */
	static ConcurrentHashMap<String,String> dHT = new ConcurrentHashMap<String, String>();
	static TreeMap<String,String> treeMap = new TreeMap<String, String>();
	static HashMap<String,String> nodeMap = new HashMap<String, String>();
	static ArrayList<String> failedPorts = new ArrayList<String>();
	static ArrayList<String> hashNodes = null;
	/*
     * An instance of a message handler. This will act as a global instance similar to PA3.
     */
	MessageHandler message_handler = new MessageHandler();

	Context context;
	ContentResolver contentResolver;

	/*
     * Instances of current node, previous node and the next node.
     * These are assigned in the onCreate method.
     */
	static String current_node;
	static String previous_node;
	static String next_node;
	static String gen_current_node;
	static String gen_previous_node;
	static String gen_next_node;
	static String smallest_node;
	static String largest_node;
	/*
     * The waiting is for making sure that the request is blocked until the response arrives.
     */
	static boolean block = true;

	static boolean new_request = true;

	static String node_del = "";
	/*
	 * At first, lock was intended to be used, but there were many problems with it. After consulting with professor, syncronized blocks are used to handle concurrency.
	 */
	static Lock lock = new ReentrantLock();

	static boolean exception = false;

	/*
	 * https://developer.android.com/reference/java/util/concurrent/atomic/AtomicInteger
	 */
	AtomicInteger atomicInteger = new AtomicInteger();

	static String failed_port;

	/*
	 * https://developer.android.com/reference/java/util/concurrent/CountDownLatch
	 */
	static CountDownLatch counter = new CountDownLatch(5);

	static int count = 0;

	static String[] neighbors = null;

	static String next_neighbor;

	static String previous_neighbor;

	static String previousToprevious_neighbor;

	static String nextTonext_neighbor;

	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	/*
     * To build a URI for content provider. Referred from PA3.
     */
	private Uri buildUri(String content, String s) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(s);
		uriBuilder.scheme(content);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        /*
         * First we check if the selection parameter is @ or * or a specific key.
         */
		synchronized (this) {
			Log.e(TAG, "Selection parameter in delete: " + selection);
			Log.e(TAG, "For delete, the emulator node: " + node_del + "is compared to next node: " + next_node);
			if (selection.equals("@")) {
            /*
             * The local data is cleared.
             */
				treeMap.clear();

			} else if (selection.equals("*")) {
            /*
             * The hash table is cleared.
             */
				dHT.clear();
            /*
             * A check to confirm whether the next node is the start node.
             */
				if (selection.equals("*") && !next_node.equals(node_del)) {
                /*
                 * If not, then we keep moving to the next node and perform the delete operation over there.
                 * https://developer.android.com/reference/android/os/AsyncTask.html
                 */
					MessageHandler local_handler = new MessageHandler(selection, null, null, "Delete", node_del, null, next_node);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
				}
			} else {
				try {
					/*
					 * Unlike PA3 where we check whether the key belongs to the current node and then remove individually,
					 * here the delete request for a particular key is sent to all the nodes which just checks their storage and removes the key.
					 * This was done because some of the keys were not getting deleted properly (using the commented code below) in the replicas because of the design of recovery function.
					 * Therefore, this simple method was adopted since at the end we just need to make sure that the emulators are empty.
					 */
					String[] ports = {"11124","11112","11108","11116","11120"};
					for(int i=0; i < 5;i++){
						MessageHandler local_handler = new MessageHandler(selection, null, null, "Delete_once", node_del, null, ports[i]);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
					}
//					Iterator<HashMap.Entry<String,String>> iterator = nodeMap.entrySet().iterator();
//					String key_location = "";
//					while(iterator.hasNext()) {
//								/*
//						 		* Every entry in the ring is acquired. https://developer.android.com/reference/java/util/Map.Entry
//						 		*/
//						HashMap.Entry entry1 = iterator.next();
//								/*
//				 		 		* Respective port is obtained from each entry.
//				 		 		*/
//						String port = entry1.getValue().toString();
//						Log.e(TAG, "In query, Port: " + port);
//								/*
//				         		* The location tracker function is used to obtain the node where the key belongs to.
//				         		*/
//						key_location = locationTracker(genHash(selection), port);
//						if(!key_location.equals("")){
//							break;
//						}
//					}
//					String[] ports = getPorts(key_location);
//					if(node_del.equals(key_location)){
//						//dHT.remove(selection);
//						//treeMap.remove(selection);
//						for(int i = 0; i < 3; i++) {
//							MessageHandler local_handler = new MessageHandler(selection, null, null, "Delete_once", node_del, null, ports[i]);
//							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
//						}
//					}
//
				 /*
                 * For a specific key, we check if the key belongs to the current node, and if so we can easily remove it from the hash table.
                 * If not, then we move to the next node, like previously, and will check again. Once we eventually reach the right node, we will perform the delete.
                 * PA3 code.
                 */
//					if (nodeCheck(genHash(selection))) {
//						dHT.remove(selection);
//						treeMap.remove(selection);
//					} else {
//						MessageHandler local_handler = new MessageHandler(selection, null, null, "Delete", node_del, null, next_node);
//						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
//					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return 0;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        /*
         * First, in order to insert a new row of (key, value) pair, it is critical to obtain the key and value from the ContentValues (which stores values that ContentResolver can process). https://developer.android.com/reference/android/content/ContentValues.html
         * The ContentValues has multiple methods for extraction of different types of values, such as boolean, float, byte etc.
         * Since the project description suggests that the all keys and values pair by provider are strings, we can use getAsString(). https://developer.android.com/reference/android/content/ContentValues.html#getAsString(java.lang.String)
         */
		//lock.lock();
		/*
		 * The countdownlatch is placed here to wait until the recovery is done. Very important step.
		 */
		try {
			counter.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String key = values.getAsString("key");
		String key_value = values.getAsString("value");

		Log.e(TAG, "Key: " + key + "Value: " + key_value);
		try {
            /*
             * The key must be hashed before the comparison with hashed port ids.
             */
			String key_hash = genHash(key);

			Log.e(TAG, "Before the nodeCheck");
			//Log.e(TAG, "Hast of the key: " + key_hash);

			//There is no need to try to check whether key belongs here and place the data here that we did in PA3. For simplicity, a new operation type is created which added the key-value pair to current and next two nodes.
			//dHT.put(key, key_value);
			//treeMap.put(key,key_value);

			/*
			 * An iterator can be used to parse the node ring and determine the location of the key. https://developer.android.com/reference/java/util/Iterator
			 */
			Iterator<HashMap.Entry<String,String>> iterator = nodeMap.entrySet().iterator();
			while(iterator.hasNext()){
				/*
				 * Every entry in the ring is acquired. https://developer.android.com/reference/java/util/Map.Entry
				 */
				HashMap.Entry entry = iterator.next();
				/*
				 * Respective port is obtained from each entry.
				 */
				String port = entry.getValue().toString();
				Log.e(TAG,"Port: "+ port);
				/*
				 * The location tracker function is used to obtain the node where the key belongs to.
				 */
				String node_location = locationTracker(key_hash,port);
				Log.e(TAG,"Key: " + key + "Location: " + node_location);
				/*
				 * If the location is found..
				 */
				if(!node_location.equals("")) {
					/*
					 * The current node that key belongs to and the next two sucessive nodes are obtained where the key will be stored.
					 */
					String[] ports = getPorts(node_location);
					Log.e(TAG,"Inserting to: "+ ports[0]+","+ports[1]+","+ports[2]);
					for (int i = 0; i < 3; i++) {
						/*
						 * For above mentioned nodes, the key-value pair is stored locally. Unlike PA3, a new operation type is created which just stores the data in the hashmap.
						 * Ports[] are the destination node where the insertion will take place, while current node is the sender.
						 */
						MessageHandler local_handler = new MessageHandler(key, key_value, null, "Insert_Once", current_node, null, ports[i]);
						//synchronized (local_handler) {
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
						//}
					}
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//lock.unlock();
		Log.v("insert", values.toString());
		return uri;


	}

	private String[] getPorts(String node){
		/*
		 * The goal is to get the current port, along with the next two successive ports. Since the ring is pre-defined, we can easily obtain this information.
		 * Note: all these are in the format of 11108 and so on, not 5554 because this makes it coding easier in case we want to quickly send information to these using ClientTask.
		 */
		if(node.equals(REMOTE_PORT0)){
			String[] ports = {REMOTE_PORT0, REMOTE_PORT2, REMOTE_PORT3};
			return ports;
		} else if(node.equals(REMOTE_PORT1)){
			String[] ports = {REMOTE_PORT1, REMOTE_PORT0, REMOTE_PORT2};
			return ports;
		} else if(node.equals(REMOTE_PORT2)){
			String[] ports = {REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
			return ports;
		} else if(node.equals(REMOTE_PORT3)){
			String[] ports = {REMOTE_PORT3, REMOTE_PORT4, REMOTE_PORT1};
			return ports;
		} else if(node.equals(REMOTE_PORT4)){
			String[] ports = {REMOTE_PORT4, REMOTE_PORT1, REMOTE_PORT0};
			return ports;
		}
		return null;
	}

	private String locationTracker (String key_hash, String port) throws NoSuchAlgorithmException {

		/*
		 * The goal is to find the location of the key, using the key hash and the current port.
		 * First the location is initialized to empty string in case it is not found anywhere in the ring (unlikely).
		 */
		String location = "";
		/*
		 * Now, for the current port, we get the previous and the current emulator id.
		 * Note that this time we are getting the ids (5554,5556..), unlike getPorts, so that we can directly get the hash of these values in next step.
		 */
		String[] both_ports = getMultiplePort(port);
		/*
		 * Two hash strings of previous and current port is obtained.
		 * Since the PA3 implementation of determing key placement, requires hash of previous and current node..
		 */
		String[] gen_both_ports = getGenPorts(both_ports);

		/*
		 * The hashs are initialized.
		 */
		String gen_previous_port = gen_both_ports[0];
		String gen_current_port = gen_both_ports[1];

		/*
		 * We check if the key belongs to the current node by using the values obtained above.
		 * If the location is found, the current port is returned.
		 */
		if(newNodeCheck(key_hash,gen_previous_port,gen_current_port)){
			location = port;
		}
		return location;

	}

	private boolean newNodeCheck(String key_hash, String gen_previous_port, String gen_current_port){

		/*
		 * This method is used to determine whether the key belongs to the current node.
		 * Note: This function is directly taken from the PA3. Only change is that we are providing the hashes as an argument, unlike the one in the PA3 (which is also present in this project as nodeChecker to verify).
		 */

		/*
         * The first if condition specifies the case when the previous node is less than current node, key is greater than previous node but less than or equal to current node.
         * For instance, previous node = 40, current node = 50 and key = 41~50.
         */
		if(gen_previous_port.compareTo(gen_current_port) < 0 && key_hash.compareTo(gen_previous_port) > 0 && key_hash.compareTo(gen_current_port) <= 0) {
			Log.e(TAG,"First if condition: true");
			return true;
		}
        /*
         * The second if condition specifies the case when the current node is less than previous node, key is less than previous node and less than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 0~10. This accounts for when the ring is completed.
         */
		else if(gen_current_port.compareTo(gen_previous_port) < 0 && key_hash.compareTo(gen_previous_port) < 0 && key_hash.compareTo(gen_current_port) <= 0) {
			Log.e(TAG,"Second if condition: true");
			return true;
		}
        /*
         * The third if condition specifies the case when the current node is less than previous node, key is greater than previous node and greater than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 51~60... This accounts for when the ring is completed.
         */
		else if(gen_previous_port.compareTo(gen_current_port) > 0 && key_hash.compareTo(gen_previous_port) > 0 && key_hash.compareTo(gen_current_port) >= 0) {
			Log.e(TAG,"Third if condition: true");
			return true;
		}
		return false;
	}

	private String[] getGenPorts(String[] ports) throws NoSuchAlgorithmException {

		/*
		 * It creates the hash of previous and current ports.
		 */
		String[] gen_ports = {genHash(ports[0]),genHash(ports[1])};
		return gen_ports;
	}

	private String[] getMultiplePort(String node){

		/*
		 * The goal is to get the previous port and the current port ids (such as 5554,5556....)
		 * The overall structure is similar to getPorts, however this time we have to calculate the previous node.
		 * Since the ring is predefined the implementation is trivial.
		 */
		String previous_node = "";
		String previous_port = "";
		String current_port = "";
		if(node.equals(REMOTE_PORT0)){
			previous_node = REMOTE_PORT1;
			previous_port = String.valueOf(Integer.parseInt(previous_node)/2);
			current_port = String.valueOf(Integer.parseInt(REMOTE_PORT0)/2);
			String[] ports = {previous_port,current_port};
			return ports;
		} else if(node.equals(REMOTE_PORT1)){
			previous_node = REMOTE_PORT4;
			previous_port = String.valueOf(Integer.parseInt(previous_node)/2);
			current_port = String.valueOf(Integer.parseInt(REMOTE_PORT1)/2);
			String[] ports = {previous_port,current_port};
			return ports;
		} else if(node.equals(REMOTE_PORT2)){
			previous_node = REMOTE_PORT0;
			previous_port = String.valueOf(Integer.parseInt(previous_node)/2);
			current_port = String.valueOf(Integer.parseInt(REMOTE_PORT2)/2);
			String[] ports = {previous_port,current_port};
			return ports;
		} else if(node.equals(REMOTE_PORT3)){
			previous_node = REMOTE_PORT2;
			previous_port = String.valueOf(Integer.parseInt(previous_node)/2);
			current_port = String.valueOf(Integer.parseInt(REMOTE_PORT3)/2);
			String[] ports = {previous_port,current_port};
			return ports;
		} else if(node.equals(REMOTE_PORT4)){
			previous_node = REMOTE_PORT3;
			previous_port = String.valueOf(Integer.parseInt(previous_node)/2);
			current_port = String.valueOf(Integer.parseInt(REMOTE_PORT4)/2);
			String[] ports = {previous_port,current_port};
			return ports;
		}
		return null;
	}

	private boolean nodeCheck(String key) throws NoSuchAlgorithmException {

        /*
         * This is the default case which will happen when inserting locally.
         * The previous id was initialized as an empty string, making sure that this condition will return true for first case.
         */
		if (gen_previous_node.length() == 0){
			Log.e(TAG,"First if condition: true");
			return true;
		}

        /*
         * For the comparison the hashed versions of ids are created.
         * It should be noted that the gen series corresponds to the emulators ids i.e. 5562 etc.
         */
		String temp_previous_node = genHash(gen_previous_node);
		String temp_current_node = genHash(gen_current_node);
		Log.e(TAG, "Gen_previous_node: " + gen_previous_node + "Hashed version: " + temp_previous_node);
		Log.e(TAG, "Gen_current_node: " + gen_current_node + "Hashed version: " + temp_current_node);

//---------------------------------------------------------------------
// This would have been the default condition if the previous node was initialized as equal to current node in the on create method.
//        if (temp_current_node.compareTo(temp_previous_node) == 0){
//            Log.e(TAG,"first iff");
//            return true;
//        }
//----------------------------------------------------------------------
        /*
         * The second if condition specifies the case when the previous node is less than current node, key is greater than previous node but less than or equal to current node.
         * For instance, previous node = 40, current node = 50 and key = 41~50.
         */
		if(temp_previous_node.compareTo(temp_current_node) < 0 && key.compareTo(temp_previous_node) > 0 && key.compareTo(temp_current_node) <= 0) {
			Log.e(TAG,"Second if condition: true");
			return true;
		}
        /*
         * The third if condition specifies the case when the current node is less than previous node, key is less than previous node and less than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 0~10. This accounts for when the ring is completed.
         */
		else if(temp_current_node.compareTo(temp_previous_node) < 0 && key.compareTo(temp_previous_node) < 0 && key.compareTo(temp_current_node) <= 0) {
			Log.e(TAG,"Third if condition: true");
			return true;
		}
        /*
         * The fourth if condition specifies the case when the current node is less than previous node, key is greater than previous node and greater than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 51~60... This accounts for when the ring is completed.
         */
		else if(temp_previous_node.compareTo(temp_current_node) > 0 && key.compareTo(temp_previous_node) > 0 && key.compareTo(temp_current_node) >= 0) {
			Log.e(TAG,"Fourth if condition: true");
			return true;
		}

		Log.e(TAG,"Outside if condition: false");
		return false;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		/*
		 * It was initially difficult to determine why there was wrong values for the keys. So the hashmaps were cleared to maintain consistency across testing phases, although the main reason is because of insert happening before recovery.
		 */
		treeMap.clear();
		dHT.clear();
		/*
         * Calculates the port number this AVD listens on.
         * Used from the previous projects.
         * Unlike the previous project, the context needs to be first obtained which calls the getSystemService.
         */
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        /*
         * The current node is initialized to the port AVD listening to.
         * The portStr are the emulator ids i.e. 5554, 5556... and myPort is 11108,11112...
         * The gen series contain the information of the emulator ids while the nodes contain the information of the port number.
         */
        /*
         * After failing to make the timeouts work, context and contentResolver was thought to be used to check failures. But there was no helpful method that could determine if emulator has failed.
         */
		context = getContext();
		contentResolver = context.getContentResolver();
		Log.e(TAG,"PortStr: "+ portStr);
		Log.e(TAG, "MyPort: "+ myPort);
		current_node = myPort;
		smallest_node = portStr;
		gen_current_node = portStr;
		gen_previous_node = "";
		gen_next_node = "";
		previous_node = "";
		next_node = "";

		String[] ports = {"11124","11112","11108","11116","11120"};
		try {
			/*
			 * The ring is predefined, whose order we know from PA3, so all the ports are just assigned to the hashmap.
			 * It was later observed that the implementation of getPorts and getMultiplePorts can be reduced but that is saved for later work.
			 */

			for (int i=0;i<ports.length;i++){
				nodeMap.put(genHash(ports[i]),ports[i]);
			}
			Log.e(TAG, "Serversocket try in onCreate");
			/*
             * A server socket needs to be created, in addition to a thread (AsyncTask), that listens on the server port.
             * PA3 code can be taken as a skeleton for the initialization purpose.
             * https://developer.android.com/reference/android/os/AsyncTask.html
             */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//Log.e(TAG,"Failedports size: " + failedPorts.size());
//			if(failedPorts.size()<5){
//				failedPorts.add(current_node);
//			}
			Log.v(TAG, "count"+count);
			Log.e(TAG, "Serversocket after in onCreate");

		} catch (IOException e) {
			Log.e(TAG, "Can't create a server socket");
			exception = true;
			failedPorts.add(current_node);
			atomicInteger.incrementAndGet();
			e.printStackTrace();
		}
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		/*
		 * Since the timeouts and exceptions were not working as intended, the design of the code was changed such that the recovery process was made to start everytime
		 */
		getRecovery();

//----------None of these worked----------------------
		//context.getApplicationInfo().flag
//		Log.v(TAG, String.valueOf(failedPorts.size()));
//		Log.v(TAG, String.valueOf(exception));
//		if(atomicInteger.get()>0){
//			Log.e(TAG,"Failed port: " + failed_port);
//			getRecovery();
//		}
//		if(failedPorts.contains(current_node) && exception){
//			getRecovery();
//		}
		//---------------------------------PA3 code not needed----------------------------------
		//Log.e(TAG, "Before the comparison with 11108");
        /*
         * If the current port is not equal to the 11108, then the client task is called to perform the join operation in order to eventually form a ring.
         */
//		if(!current_node.equals(REMOTE_PORT0)){
//			MessageHandler local_handler = null;
//            /*
//             * The emulator wants to join. All the nodes (prev.,curr.,next) are initialized for now as the current emulator port since the placement is not yet decided.
//             * The key can be initialized as emulator id since the hased version of it will be compared to respective ids later.
//             */
//			local_handler = new MessageHandler(gen_current_node, null, null, "Join", current_node, current_node, REMOTE_PORT0);
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,local_handler);
//		}
		//---------------------------------------------------------------------------------------
		return true;
	}

	public void getRecovery(){
		/*
		 * Here we start the recovery process.
		 * It is very important to clear the hashmap, otherwise it results in different value observed for a key error.
		 */
		counter = new CountDownLatch(4);
		dHT.clear();
		String[] ports = {"11124","11112","11108","11116","11120"};
		/*
		 * Rather than sending recovery request to specific neighbors, we send it to all the nodes in the ring except the current one.
		 * The placement of the key-value pair is handled in the servertask.
		 */
		for(int i = 0; i < 5; i++){
			if(!ports[i].equals(current_node)){
				/*
				 * We send an empty hashmap to all the ports except the current one.
				 */
				MessageHandler new_handler = new MessageHandler(null,null,new HashMap<String, String>(),"Start_Recovery",current_node,null,ports[i]);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
				/*
				 * Placing a wait over here causes the phase 1 to fail so it was removed. It was resulting in a key not found error.
				 */
				//holdOn();
			}
		}
	}

	/*
     * ServerTask is created to handle the incoming messages.
     */
	private class ServerTask extends AsyncTask<ServerSocket,String,Void>{

		@Override
		protected Void doInBackground(ServerSocket... serverSockets){

            /*
             * Most of the statements, for instance, initializing the ServerSocket are from PA3.
             * However, some new functionalities are added, depending of the operation_type.
             */

			ServerSocket serverSocket = serverSockets[0];
			Log.e(TAG, "serverSocket: " + serverSocket);
			try{
				Log.e(TAG, "It reaches inside the try phrase in ServerTask");
				do{
                    /*
                     * Socket is an endpoint for communication between two machines and underlying class implements CLIENT sockets. (https://developer.android.com/reference/java/net/Socket.html)
                     * The serverSocket waits for requests to come in over the network and underlying class implements SERVER sockets. (https://developer.android.com/reference/java/net/ServerSocket.html)
                     * Once serverSocket detects the incoming connection, it is first required to accept it and for communication, create a new instance of socket.
                     * The accept() method listens for a connection to be made to this socket and accepts it. (https://developer.android.com/reference/java/net/ServerSocket.html#accept())
                     */
					Log.e(TAG,"Before the socket accept");
					Socket socket = serverSocket.accept();
					//----------------------------------
					//The timeout was used to check why initially the socket was not accepting, but later the problem was found to be in non-continuous running of this loop which posed the problem.
					//----------------------------------
					Log.e(TAG, "In the serverSocket loop inside the ServerTask");

                    /*
                     * In the previous project, the BufferedReaders were utilized, but since this time an object is being sent, ObjectInputStream will be used.
                     * The whole object was sent since bufferedreaders would have involved sending information as strings, for which delimiters might be necessary (like PA2B). A scenario that should be avoided.
                     * An ObjectInputStream is opposite of ObjectOutputStream and decentralizes primitive data which is typically written by the ObjectOutputStream in our case. (https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html)
                     */
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					/*
					 * Even after placing a lot of socket timeouts, they were never caught. Therefore, the recovery process starts on ONcreate method all the time.
					 */
					//socket.setSoTimeout(1000);
//					if(input == null){
//						throw new Exception();
//					}
                    /*
                     * The readObject actually reads the data of type Object. We will need to convert it to MessageHandler type for further use.
                     */
					Object object = input.readObject();
					/*
                     * A normal typecasting can be performed.
                     */
					MessageHandler local_handler = (MessageHandler) object;
					Log.e(TAG, "operation_type:" + local_handler.operation_type);


//--------------------------------------------------------------------------------------------------
                    /*
                     * After posting a question on piazza, somebody suggested to read from all the ports no matter what and then detect exceptions.
                     * But doing that in the following caused the whole code to break :(.
                     */
//					String[] ports = {"11124", "11112", "11108", "11116", "11120"};
//                    for(int i = 0; i<ports.length; i++) {
//                    	//if(!ports[i].equals(current_node)) {
//							Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[i]));
//							if(socket1 != socket) {
//								try {
//									ObjectInputStream in = new ObjectInputStream(socket1.getInputStream());
//									Object ob = in.readObject();
//								} catch (IOException e) {
//									exception = true;
//									failed_port = ports[i];
//									Log.e(TAG, "Exception caught");
//									socket1.close();
//								}
//								socket1.close();
//								//}
//							}
//					}
//--------------------------------------------------------------------------------------------------
					/*
                     * When the operation type is join, the emulator is making a request to join the ring.
                     * PA3 code.
                     */
					if(local_handler.operation_type.equals("Join")){
						requestJoin(local_handler);
					}
                    /*
                     * The global next node is updated such that it is pointing to the next node of the handler.
                     * PA3 code.
                     */
					if(local_handler.operation_type.equals("Join_Next")){
						next_node = local_handler.nextNode;
						gen_next_node = local_handler.key;
					}
                    /*
                     * The previous node and the next node are updated upon the completion of the Join.
                     * PA3 code.
                     */
					if(local_handler.operation_type.equals("Join_Success")){
						previous_node = local_handler.previousNode;
						next_node = local_handler.nextNode;
						gen_next_node = local_handler.value;
						gen_previous_node = local_handler.key;
					}
                    /*
                     * The insert acquires the key and the value from the object and puts them in the content values which is later parsed in the Insert function.
                     * Same as PA3.
                     */
					if(local_handler.operation_type.equals("Insert")){
						ContentValues content_values = new ContentValues();
						content_values.put("key",local_handler.key);
						content_values.put("value", local_handler.value);
						//dHT.put(local_handler.key,local_handler.value);
						Log.e(TAG,"Inside the insert operation");
						insert(mUri, content_values);
					}
					/*
					 * New function: Insert once is used insert the key value pair in the hashmaps that are used for maintaining the key-value pair for three consecutive nodes.
					 */
					if(local_handler.operation_type.equals("Insert_Once")){
						//socket.setSoTimeout(1000);
						Log.e(TAG,"Current node:" + current_node + "is replicated");
						//lock.lock();
						/*
						 * The synchronized block is used for handling concurrency while insertions. The locks were not working properly.
						 */
						synchronized (this) {
							dHT.put(local_handler.key,local_handler.value);
							treeMap.put(local_handler.key, local_handler.value);
						}
						//lock.unlock();
					}
                    /*
                     * The query checks if the current node is at the local handler, implying that the data being queried is present at the local host.
                     * If so the block is removed and the local handler can be copied to the global handler.
                     * In the Query function, there is another block that waits until the response is arrived and then put all the key-value pairs from the global message handler to the cursor.
                     * Same as PA3.
                     */
					if(local_handler.operation_type.equals("Query")){
						//socket.setSoTimeout(1000);
						if(current_node.equals(local_handler.currentNode)){
							Log.e(TAG, "The message handler key: " + message_handler.key + "value: " + message_handler.value + "current: " + message_handler.currentNode);
							Log.e(TAG, "The local handler key: " + local_handler.key + "value: " + local_handler.value + "current: " + local_handler.currentNode);
							message_handler = local_handler;
							block = false;
						} else{
                            /*
                             * The code will give 3 points in case since the * and @ will work regardless of the block.
                             * But for accessing individual queries, we don't wish to generate requests multiple times or update for the query that arrived later than a previous one.
                             */
							if(local_handler.key.equals("*")||local_handler.key.equals("@")){
								Log.e(TAG,"Unexpected behavior!!");
							}
							handleQuery(local_handler);

						}
						//handleQuery(local_handler);
					}
					/*
					 * While testing it was observed that even the query was needed to be replicated since during one test in phase 1, it was querying all the keys from emulator 5554.
					 * The query replicate gets the query request and sends confirmation that the request is complete.
					 */
					if (local_handler.operation_type.equals("Query_replicate")){
						//socket.setSoTimeout(1000);
						Log.e(TAG, "Its coming here!");
						/*
						 * For the key, the value is obtained from the hashmap.
						 * The destination node will the current node to give response that replication was handled.
						 */
						MessageHandler new_handler = local_handler;
						new_handler.value = dHT.get(local_handler.key);
						new_handler.operation_type = "Replication_complete";
						new_handler.destinationNode = local_handler.currentNode;
						//synchronized (new_handler) {
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new_handler);
						//}
					}
					if(local_handler.operation_type.equals("Replication_complete")){
						//if(!local_handler.currentNode.equals(current_node)) {
						/*
						 * If the key and value pair are found, the pair is inserted in the hashmap.
						 * The synchronized block is used to handle the concurrency.
						 */
						if (local_handler.key != null && local_handler.value != null) {
							synchronized (this) {
								Log.e(TAG,"Replicating: " + local_handler.key + " for: " + local_handler.currentNode);
								dHT.put(local_handler.key, local_handler.value);
							}
						}
						//}
					}
                    /*
                     * For performing a delete of key-value pair from the hash map.
                     * Same as PA3.
                     */
					if(local_handler.operation_type.equals("Delete")){
						node_del = local_handler.currentNode;
						delete(mUri,local_handler.key,null);
					}
					if(local_handler.operation_type.equals("Delete_once")){
						/*
						 * This is a newly added method.
						 * Since the recovery process starts on all the nodes, there were some keys that was misplaced and the orginal delete from PA3 was having trouble removing them.
						 * Therefore, for each delete operation, we check all the nodes that may have the key and if so we remove them.
						 * It works since the testing script aims to delete all the keys from each node, so we just have to make sure all are empty at the end.
						 */
						if(treeMap.containsKey(local_handler.key)) {
							treeMap.remove(local_handler.key);
						}
						if(dHT.containsKey(local_handler.key)) {
							dHT.remove(local_handler.key);
						}
					}
					if(local_handler.operation_type.equals("Start_Recovery")) {
						Log.v("port recovering", current_node);
						/*
						 * It was found out that rather than redundantly calling iterator each time, simply the entries can be accessed in a for loop.
						 * Nevertheless, we start the recovery process by iterating through each key-value pair in our treemap.
						 */
						for (TreeMap.Entry<String, String> entry : treeMap.entrySet()) {
							/*
							 * For each pair, we loop across our node ring and try to find the correct key location by checking it agaist each entry.
							 */
							Iterator<HashMap.Entry<String,String>> iterator = nodeMap.entrySet().iterator();
							String key_location = "";
							while(iterator.hasNext()) {
								/*
						 		* Every entry in the ring is acquired. https://developer.android.com/reference/java/util/Map.Entry
						 		*/
								HashMap.Entry entry1 = iterator.next();
								/*
				 		 		* Respective port is obtained from each entry.
				 		 		*/
								String port = entry1.getValue().toString();
								Log.e(TAG, "In query, Port: " + port);
								/*
				         		* The location tracker function is used to obtain the node where the key belongs to.
				         		*/
								key_location = locationTracker(genHash(entry.getKey()), port);
								if(!key_location.equals("")){
									/*
									 * As soon as the location is obtained, break.
									 */
									break;
								}
							}
							Log.e(TAG,"In recover key location: " + key_location);
							/*
							 * Since in the design, we insert the key-value pair in next two successive nodes and do the same for query replication,
							 * during the recovery the corresponding key should exist in the previous two nodes of the current one.
							 * here we start by first acquiring the previous and previous to previous neighbors of the current node.
							 */
							previous_neighbor = String.valueOf(Integer.parseInt(getMultiplePort(local_handler.currentNode)[0])*2);
							previousToprevious_neighbor = String.valueOf(Integer.parseInt(getMultiplePort(previous_neighbor)[0])*2);

							//String key_location = locationTracker(genHash(entry.getKey()),local_handler.currentNode);
							/*
							 * Then, we check if the location where the key rightfully belongs is either of the current, previous or previous to previous node.
							 * If it is, then we place it in the hashmap of the current node.
							 */
							if(key_location.equals(local_handler.currentNode) || key_location.equals(previous_neighbor) || key_location.equals(previousToprevious_neighbor)){
								synchronized (this) {
									Log.e(TAG,"In recover, placing the pair in node: " + local_handler.currentNode + "with previous nodes: "+previous_neighbor + " " + previousToprevious_neighbor);
									local_handler.dHT.put(entry.getKey(), entry.getValue());
								}
							}
						}
						/*
						 * Once the key-value pair is placed, we send the response back with the updated hashmap.
						 * This is important because the whole goal of the recovery process is to place the key-value pair back in the recovered node.
						 */
						local_handler.operation_type =  "Recovered_data";
						local_handler.destinationNode = local_handler.currentNode;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,local_handler);
					}
					if(local_handler.operation_type.equals(("Recovered_data"))) {
						/*
						 * The synchronized block is placed for handling concurrency.
						 */
						synchronized (this) {
							/*
							 * Upon receiving the response from the recovery process, we put all the associated key-value pairs back in the recovered node.
							 * The hashmap (dht) is also updated since that is the one we are checking we are checking the key for in the single-query operation.
							 */
							for (HashMap.Entry<String,String> entry : local_handler.dHT.entrySet()) {
								Log.e(TAG,"Recovered key: "+ entry.getKey() + " value: " + entry.getValue());
								if(!treeMap.containsKey(entry.getKey())) {
									treeMap.put(entry.getKey(), entry.getValue());
								}
								if(!dHT.containsKey(entry.getKey())) {
									dHT.put(entry.getKey(), entry.getValue());
								}
							}
						}
						block = false;
						/*
						 * The counter is decreased by one. After 4 times, it will become 0 and the insert process that is waiting until now will start.
						 * This is necessary otherwise the recovery continue to happen while insert causing wrong key values.
						 */
						counter.countDown();
					}
					//socket.setSoTimeout(1000);
				} while(true);
			} catch (SocketTimeoutException e){
				exception = true;
				failedPorts.add(current_node);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
			}
			catch (IOException e){
				exception = true;
				failedPorts.add(current_node);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (Exception e) {
				exception = true;
				failedPorts.add(current_node);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
				e.printStackTrace();
			}

			return null;

		}

	}
	/*
     * This method handles the join request for the emulators.
     */
	private void requestJoin(MessageHandler local_handler){
		try {
            /*
             * First, similar to all the other functions, we will check if the key belongs to the current node, which is the new node that wants to join.
             */
			if(nodeCheck(genHash(local_handler.key))){
                /*
                 * If the gen series were assigned as equal to the current node, then the condition would be check if current node is equal to the previous node.
                 */
				Log.e(TAG, "The request for the join is handled by "+ current_node);
				Log.e(TAG, "The node that is requesting the join is: " + local_handler.nextNode);
				Log.e(TAG, "Gen_previous_node: " + gen_previous_node);

                /*
                 * The check when the first node joins the ring.
                 * In that case, the gen series will be null as initialized in the OnCreate method.
                 */
				if(gen_previous_node.length() == 0){
					masterHandle(local_handler);
				} else {
                    /*
                     * When more than one node has already joined the ring, it is essential to first get the node whose next will be the current node.
                     * After that, the previous node and next node will be updated implying that a new node has joined the ring.
                     */
					ClientJoinNext(local_handler);
					ClientJoinSuccess(local_handler);
					Log.e(TAG,"The previous node " + previous_node + " is the handlers previous node: " + local_handler.previousNode);
					Log.e(TAG, "The gen previous node " + gen_previous_node + "is updated to: " + local_handler.key);
					gen_previous_node = local_handler.key;
					previous_node = local_handler.previousNode;
				}
			} else {
                /*
                 * Similar to other functions, the key is sent to the next node in the ring.
                 */
				ClientSendToNext(local_handler);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private void masterHandle(MessageHandler local_handler){
		Log.e(TAG,"The gen previous node is empty implying that the request is managed by 11108");
		if(!current_node.equals("11108")){
			Log.e(TAG, "There is an error in the code :( ");
		}
        /*
         * The gen series are initialized as the message keys of the node that is requesting to join.
         * The previous node and the next node can be updated with prev/next node of the node that is requesting to join.
         * Since this case occurs when the first node is joined apart from the 11108, then we don't need to find the previous node for the requesting one, so there will be no need for Join_Next.
         */
		Log.e(TAG, "The gen previous and next nodes are changed to: " + local_handler.key);
		Log.e(TAG, "The previous node: " + previous_node + " is updated with: " + local_handler.previousNode);
		Log.e(TAG, "The next node: " + next_node + " is updated with: " + local_handler.nextNode);
		gen_previous_node = local_handler.key;
		previous_node = local_handler.previousNode;
        /*
         * Since the initialization of value is empty string, the gen_next_node will be assigned the value of key, not the value.
         * This can be changed depending on the design used to initialize the key-value pair upon the join.
         */
		gen_next_node = local_handler.key;
		next_node = local_handler.nextNode;
        /*
         * Although the initial intention was to call the ClientJoinSuccess method over here, the problem was that the key and previous node are initialized as current nodes since for this case.
         * The gen series are empty strings and only current values are assigned in the OnCreate method.
         * If used improperly, the join runs for an infinite loop :(
         */
		MessageHandler new_handler = new MessageHandler(gen_current_node,gen_current_node,null,"Join_Success",current_node,current_node,previous_node);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
	}

	private void ClientJoinNext(MessageHandler local_handler){
		Log.e(TAG, "The previous node whose next is the current join: " + previous_node);
		MessageHandler new_handler = new MessageHandler(local_handler.key,null,null,"Join_Next",null,local_handler.nextNode,previous_node);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
	}

	private void ClientJoinSuccess(MessageHandler local_handler){
		Log.e(TAG,"The current node for the handler " + local_handler.currentNode + "will be updated to: " + previous_node);
		MessageHandler new_handler = new MessageHandler(gen_previous_node,gen_current_node,null,"Join_Success",previous_node,current_node,local_handler.previousNode);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
	}

	private void ClientSendToNext(MessageHandler local_handler){
		Log.e(TAG,"The key does not belong to node: " + current_node + "and is sent to: " + next_node);
		MessageHandler new_handler = local_handler;
		new_handler.destinationNode = next_node;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
	}

	//-------------------------------------------------------------
//    private void updateNext(MessageHandler local_handler){
//
//        MessageHandler new_handler = new MessageHandler("","",null,"Join_Success", "",local_handler.key,previous_node);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
//    }
//
//    private void moveToNext(MessageHandler local_handler){
//
//        MessageHandler new_handler = new MessageHandler("","",null,"Join_Reply", previous_node, current_node, local_handler.key);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
//        previous_node = local_handler.key;
//    }
//-------------------------------------------------------------
    /*
     * ClientTask is an AsyncTask that sends the message (in form of string) over the network. Same as PA3.
     */
	private class ClientTask extends AsyncTask<MessageHandler,Void,Void>{

		@Override
		protected Void doInBackground(MessageHandler... handler) {

			MessageHandler local_handler = handler[0];

			try {
				int remote_port = Integer.parseInt(local_handler.destinationNode);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remote_port);

                /*
                 * OutputStream is superclass representing output stream of bytes (https://developer.android.com/reference/java/io/OutputStream.html).
                 * In the OutputStream there is a class designed for writing primitive data types (https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html)
                 * The primary intention was to use the BufferedWriters like PA2B but those don't support outputing objects.
                 * A string output cannot be sent, since the delimiter (';' in PA2B) can be included inside the message. Therefore, rather than making a string containing all the information, the whole object is sent.
                 */
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
//				if(output == null){
//					throw new Exception();
//				}
				//socket.setSoTimeout(1000);
				output.writeObject(local_handler);
				output.flush();
				output.close();
				socket.close();

			} catch (SocketTimeoutException e){
				exception = true;
				failedPorts.add(local_handler.destinationNode);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
			}
			catch (IOException e) {
				Log.v(TAG,"Operation_type:" + local_handler.operation_type);
				exception = true;
				failedPorts.add(local_handler.destinationNode);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
				e.printStackTrace();
			} catch (Exception e) {
				exception = true;
				failedPorts.add(local_handler.destinationNode);
				atomicInteger.incrementAndGet();
				Log.v(TAG,"FailedPorts size: "+ failedPorts.size());
				e.printStackTrace();
			}

			return null;

		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
        /*
         * A new request is generated by the current node where the operation type is changed to Query and key is based on the selection parameter used by the testing script or the gDump or lDump.
         * Same as PA3. Only the synchronized block is added.
         */
		//lock.lock();
		synchronized (this) {
			MessageHandler local_handler = new MessageHandler();
			local_handler.currentNode = current_node;
			local_handler.operation_type = "Query";
			local_handler.key = selection;
        /*
         * The matrix cursor containing the information about the key-value pair is acquired for the request.
         */
			if (local_handler.key.equals("*") || local_handler.key.equals("@")) {
				Log.e(TAG, "Excepted behavior!!");
			} else {
				Log.e(TAG, "The selection parameter is something else" + local_handler.key);
			}
        /*
         * The handleQuery will initiate immediately when the selection parameters is @ since that is a local dump. While for * it will have to wait.
         */
			Cursor matrix_cursor = handleQuery(local_handler);
			Log.v("query", selection);
			//lock.unlock();
			return matrix_cursor;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public Cursor handleQuery(MessageHandler local_handler){

        /*
         * Since the internal storage option is used, MatrixCursor needs to be built. https://developer.android.com/reference/android/database/MatrixCursor.html
         * Its documentation suggests that the argument required are array of string which denotes columns.
         * The array of string, in our case, only requires to contain key and value.
         * The code is from PA3 only with few changes.
         */
		//lock.lock();
		/*
		 * A synchronized block is added for handling concurrent access to the hashmaps.
		 */
		synchronized (this) {
			String[] column_names = {"key", "value"};
			MatrixCursor matrix_cursor;
			matrix_cursor = new MatrixCursor(column_names);
        /*
         * There are 3 high-level checks, for @,* and individual query.
         */
			if (local_handler.key.equals("@")) {
            /*
             * For each entry in the HashTable, the rows in the Matrix Cursor will be updated.
             * This operation is local.
             * https://developer.android.com/reference/java/util/Map.Entry.html
             * The structure is same as PA3. Only difference is that the key-value pairs are obtained from the local hashmap.
             */
				Log.e(TAG, "DHT size: " + dHT.size());
				Log.e(TAG, "Hashmap size:" + treeMap.size());
				//synchronized (matrix_cursor) {
				for (TreeMap.Entry<String, String> iterator : treeMap.entrySet()) {
					matrix_cursor = updateCursor(iterator, matrix_cursor);
				}
				//lock.unlock();
				return matrix_cursor;
				//}
			} else if (local_handler.key.equals("*")) {
            /*
             * This whole code is SAME as PA3.
             * The first check is to determine whether the next node is an empty string. You can compare the gen_previous too since both are empty initially.
             * If so then the selection parameter * if initiated will act locally and matrix cursor will be updated.
             */
				Log.e(TAG, "Selection *: Gen previous node: " + gen_previous_node + "gen next node: " + gen_next_node);
				if (gen_next_node.length() == 0) {
					for (HashMap.Entry<String, String> iterator : dHT.entrySet()) {
						matrix_cursor = updateCursor(iterator, matrix_cursor);
					}
					//lock.unlock();
					return matrix_cursor;
				} else {
                /*
                 * SAME as PA3.
                 * The current node's hash table needs to be updated with all the rows of the global hash table.
                 * Then, the updated handler comprising of updated hash table will be sent to the destination node.
                 */
					local_handler.dHT.putAll(dHT);
					Log.e(TAG, "The destination node for the * selection to send hash table to: " + local_handler.destinationNode);
					ClientSendToNext(local_handler);
                /*
                 * A check will be performed to verify whether the local handler is of the emulator whose global dump has been requested.
                 */
					if (local_handler.currentNode.equals(current_node)) {
                    /*
                     * It is important to put the block here otherwise it will result in incorrect number of rows.
                     */
						holdOn();
                    /*
                     * Unlike the previous update, here the hash table will be acquired from the message_handler, since that is updated when the block is released in the ServerTask.
                     */
						for (HashMap.Entry<String, String> iterator : message_handler.dHT.entrySet()) {
							matrix_cursor = updateCursor(iterator, matrix_cursor);
						}
					}
					//dHT.clear();
					//lock.unlock();
					return matrix_cursor;
				}
			} else {
				try {
                /*
                 * When the selection parameter is neither the * or @, a single query request is handled.
                 * Similar to Insert, the query request needs to be transferred to the current node that key belongs to and the successive two nodes where it is stored.
                 */
					/*
			         * An iterator can be used to parse the node ring and determine the location of the key. https://developer.android.com/reference/java/util/Iterator
			         */
					Iterator<HashMap.Entry<String,String>> iterator = nodeMap.entrySet().iterator();

					while(iterator.hasNext()){
						/*
						 * Every entry in the ring is acquired. https://developer.android.com/reference/java/util/Map.Entry
						 */
						HashMap.Entry entry = iterator.next();
						/*
				 		 * Respective port is obtained from each entry.
				 		 */
						String port = entry.getValue().toString();
						Log.e(TAG,"In query, Port: "+ port);
						/*
				         * The location tracker function is used to obtain the node where the key belongs to.
				         */
						String node_location = locationTracker(genHash(local_handler.key),port);
						Log.e(TAG,"Query Key: " + local_handler.key + "Location: " + node_location);
						/*
				         * If the location is found..
				         */
						if(!node_location.equals("")) {
							/*
					 		 * The current node that key belongs to and the next two sucessive nodes are obtained where the query needs to be replicated.
					 		 */
							String[] ports = getPorts(node_location);
							Log.e(TAG,"Querying to: "+ ports[0]+","+ports[1]+","+ports[2]);
							for (int i = 0; i < 3; i++) {
								/*
								 * For above mentioned nodes, the key-value pair is stored locally. Unlike PA3, a new operation type is created to handle the query replication.
						 		 * Ports[] are the destination node where the query will be replicated.
						 		 */
								MessageHandler new_handler = new MessageHandler(local_handler.key, null, null, "Query_replicate", current_node, null, ports[i]);
								//synchronized (local_handler) {
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new_handler);
								//}
							}
						}
					}

					if(dHT.containsKey(local_handler.key)){
						Log.v(TAG,"Key is in dht");
					}
					/*
					 * Wait till the key is found..
					 * https://developer.android.com/reference/java/lang/Thread
					 * A CountDownLatch may also be used here. https://developer.android.com/reference/java/util/concurrent/CountDownLatch
					 */
					while (!dHT.containsKey(local_handler.key)) {
						Thread.sleep(100);
					}
					/*
					 * Once the key is found, add the respective key-value pair in the matrix cursor.
					 * This code is taken from PA3.
					 */
					Log.e(TAG,"In single query, placing in the cursor: " + local_handler.key + " value: "+ dHT.get(local_handler.key));
					String temp_key = local_handler.key;
					String temp_value = "";
					temp_value = dHT.get(temp_key);
					String[] temp_array = {temp_key, temp_value};
					matrix_cursor.addRow(temp_array);
					//--------------------------------PA3 code that was not needed----------------------
//				if(nodeCheck(genHash(local_handler.key))){
//                    /*
//                     * It is to make sure that the global current node corresponds to the respective emulator which initiated the request.
//                     */
//					Log.e(TAG, "Comparing nodes in the query function, current node: " + current_node + "requesting node: " + local_handler.currentNode);
//					if(local_handler.currentNode.equals(current_node)){
//                        /*
//                         * The value in the pair is obtained by getting it from location of the key in the hash map.
//                         */
//						String temp_key = local_handler.key;
//						String temp_value = dHT.get(temp_key);
//						String[] temp_array = {temp_key,temp_value};
//						matrix_cursor.addRow(temp_array);
//					} else {
//                        /*
//                         * The current node is marked as as a destination and the key value pair is sent to that node which initially put the request.
//                         */
//						MessageHandler new_handler = local_handler;
//						new_handler.destinationNode = local_handler.currentNode;
//						new_handler.key = local_handler.key;
//						new_handler.value = dHT.get(local_handler.key);
//						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
//					}
//					return matrix_cursor;
//				} else {
//                    /*
//                     * Since the existing key does not belong to the current node, it is sent onwards.
//                     */
//					ClientSendToNext(local_handler);
//                    /*
//                     * Once the correct emulator is found..
//                     */
//					if(local_handler.currentNode.equals(current_node)) {
//                        /*
//                         * Hold on until the reponse has arrived and then add the key-value pair in the cursor.
//                         * Again, instead of getting the value from the hashtable directly, it is obtained from the message handler which was updated in the block.
//                         */
//						holdOn();
//						String temp_key = local_handler.key;
//						String temp_value = message_handler.value;
//						String[] temp_array = {temp_key, temp_value};
//						matrix_cursor.addRow(temp_array);
//					}
//				}
					//----------------------------------------------------------------------------------
				} catch (Exception e) {
					e.printStackTrace();
				}
				//dHT.clear();
			}
			//lock.unlock();
			return matrix_cursor;
		}
	}

	/*
     * This function just gets the key and the value from the hashmap and update the cursor object.
     */
	private MatrixCursor updateCursor(HashMap.Entry<String,String> iterator, MatrixCursor matrix_cursor){

		String temp_key = iterator.getKey();
		String temp_value = iterator.getValue();
		String[] temp_array = {temp_key,temp_value};
		matrix_cursor.addRow(temp_array);

		return matrix_cursor;
	}
	/*
     * This is used to block until the response is received.
     * Another way is to wait in the ContentProvider query until the network backend notify of the result using the wait() and Thread.sleep().
     */
	private void holdOn(){
		block = true;
		while(block);
	}
}