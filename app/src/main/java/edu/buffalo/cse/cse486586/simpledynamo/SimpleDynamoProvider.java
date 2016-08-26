package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDynamoProvider extends ContentProvider implements Constants{
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static NodeDetails myNodeDetails;
	static List<NodeDetails> chordNodeList;
	public static boolean isInitialized=false;
	public static boolean isCreated=false;
	public static boolean isInserting=false;
	public static boolean isQuerying=false;


	public static Context context = null;


	public static Context getProviderContext(){
		return context;
	}

	private String getMyNodeId() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String nodeId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return nodeId;
	}

	private void initChord() {
		chordNodeList = new ArrayList<NodeDetails>();
		NodeDetails node;
		Log.e(TAG,"******************Chord Initialization begins*********************");
		try {
			for (int i = 0; i < NODE_IDS.length; i++) {
				node = new NodeDetails();
				node.setPort(REMOTE_PORTS[i]);
				node.setNodeIdHash(Util.genHash(NODE_IDS[i]));
				chordNodeList.add(node);
			}
			Collections.sort(chordNodeList, new Comparator<NodeDetails>() {
				@Override
				public int compare(NodeDetails lhs, NodeDetails rhs) {
					return lhs.getNodeIdHash().compareTo(rhs.getNodeIdHash());
				}
			});
			for (int i = 0; i < chordNodeList.size(); i++){
				if (i == 0) {
					chordNodeList.get(i).setFirstNode(true);
					chordNodeList.get(i).setPredecessorPort(chordNodeList.get(chordNodeList.size() - 1).getPort());
					chordNodeList.get(i).setPredecessorNodeIdHash(chordNodeList.get(chordNodeList.size()-1).getNodeIdHash());
				}else{
					chordNodeList.get(i).setPredecessorPort(chordNodeList.get(i - 1).getPort());
					chordNodeList.get(i).setPredecessorNodeIdHash(chordNodeList.get(i - 1).getNodeIdHash());
				}

				if (i == chordNodeList.size()-1) {
					chordNodeList.get(i).setSuccessorPort(chordNodeList.get(0).getPort());
					chordNodeList.get(i).setSuccessorNodeIdHash(chordNodeList.get(0).getNodeIdHash());
				}else{
					chordNodeList.get(i).setSuccessorPort(chordNodeList.get(i + 1).getPort());
					chordNodeList.get(i).setSuccessorNodeIdHash(chordNodeList.get(i + 1).getNodeIdHash());
				}
			}
			Log.e(TAG,"******************Chord Initialization ends*********************");
		}catch(Exception e){
			Log.e(TAG, "****************************Exception in chord initialization***************************");
			e.printStackTrace();
		}

		Log.e(TAG,"*************************Chord Details : *******************************");
		Log.e(TAG,chordNodeList.toString());
		Log.e(TAG,"********************************************************");
		Log.e(TAG, "********************** initChord() Ends*****************");
	}

	private void initMyNodeDetails(){
		Log.e(TAG, "********************** initMyNodeDetails() Begins*****************");
		context = getContext();
		try {
			//port will store value of port used for connection
			//eg: port = 5554*2 = 11108
			String port = String.valueOf(Integer.parseInt(getMyNodeId()) * 2);
			//nodeIdHash will store hash of nodeId =>
			// eg: nodeIdHash = hashgen(5554)
			for (NodeDetails node : chordNodeList) {
				if (node.getPort().equals(port)){
					myNodeDetails = node;
					break;
				}
			}

		} catch (Exception e) {
			Log.e(TAG,"**************************Exception in initMyNodeDetails()**********************");
			e.printStackTrace();
		}
		Log.e(TAG, "********************** initMyNodeDetails() ends*****************");
	}

	private void recover(){
		Log.e(TAG, "***********************recover() begins************************");
		isInitialized=false;
		Log.e(TAG, "isInitialized set to: " + isInitialized + " in recover()");

		Message message = new Message();
		message.setMessageType(MessageType.Recover);
		message.setNodeDetails(myNodeDetails);
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);
		/*while(!isInitialized) {
			try {
				Thread.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}*/
		Log.e(TAG, "***********************recover() ends************************");
	}

	@Override
	public boolean onCreate() {
		Log.e(TAG, "********************** onCreate Begins*****************");
		if(isCreated){
			Log.e(TAG, "###OnCreate() called 2nd time....###");
			return true;
		}
		isCreated = true;
		initChord();
		initMyNodeDetails();
		SharedPreferences sharedPref = getContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
		if(sharedPref.getAll().size() == 0){
			Log.e(TAG, "Shared Preferences Empty");
			isInitialized = true;
		}else {
			Log.e(TAG, "Recovering elements...");
			recover();
		}


		//create server task to accept requests from other nodes

		Log.e(TAG, "Starting Server...");
		//Log.e(TAG, "isInitialized before starting serverSocket: " + isInitialized);
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 25);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
			return false;
		}
		Log.e(TAG, "********************** onCreate Ends*****************");
		return true;
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	/************************Insert logic begins*******************************/
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		isInserting = true;
		Log.e(TAG, "******************************insert() begins in Content Provider********************************");
		while(!isInitialized) {
			try {
				Thread.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		//create AsyncTask to perform parallel execution
		String key = values.getAsString(KEY_FIELD);
		String value = values.getAsString(VALUE_FIELD);
		Log.e(TAG, "******************************Key - Value pair to be inserted : " + key + " - " + value + "********************************");
		Message message = new Message();
		message.setMessageType(MessageType.Insert);
		message.setKey(key);
		message.setValue(value);
		String returnMessage = insert(message);
		Log.e(TAG, returnMessage);
		//new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);

		Log.e(TAG, "******************************insert() ends in Content Provider********************************");
		isInserting = false;
		return uri;

	}

	private String insert(Message message) {
		Log.e(TAG, "***********************insert() begins************************");
		String key = message.getKey();
		String[] insertionPorts = new String[3];
		String keyHash = "";
		String resultString = null;
		try {
			keyHash = Util.genHash(key);
		} catch (Exception e) {
			Log.e(TAG, "***********************Exception in insert() while generating keyHash************************");
			e.printStackTrace();
			Log.e(TAG, "***********************Exception message ends************************");
		}

		NodeDetails targetNode = Util.getTargetNode(keyHash);

		insertionPorts = Util.getReplicationPortNumbers(targetNode);
		insertionPorts[0] = targetNode.getPort();
		Log.e(TAG, "====================================");
		Log.e(TAG, "Sending Key : " + key + " target Node : " + insertionPorts[0] + " Replication Node 1 : " + insertionPorts[1] + " Replication Node 2 : " + insertionPorts[2]);
		Log.e(TAG, "====================================");
		ObjectOutputStream outputStream;
		ObjectInputStream inputStream;
		Socket socket;
		for (String port : insertionPorts) {
			int count = 1;
			Log.e(TAG, "====================================");
			while(count<=3){
				Log.e(TAG, "Attempt "+ count +" to connect port: "+port);
				try {
					if(count>1){
						Thread.sleep(100);
					}
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					//socket.setSoTimeout(500);
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					Log.e(TAG, "Insert Message with key : " + message.getKey() + " with keyHash : " + Util.genHash(message.getKey()) + " sent to port : " + port + " with node hash : " + Util.genHash(String.valueOf(Integer.parseInt(port) / 2)));
					outputStream.writeObject(message);
					outputStream.flush();
					Log.e(TAG, "Waiting for response of insert....");
					inputStream = new ObjectInputStream(socket.getInputStream());
					resultString = (String) inputStream.readObject();
					Log.e(TAG, "Response arrived.... resultString : " + resultString);
					Log.e(TAG, "Connection Successful!!!");
					break;
				} catch (Exception e) {
					Log.e(TAG, "************************Exception in ClientTask().sendInsertMessage() for port: " + port + "******************");
					e.printStackTrace();
					Log.e(TAG, "***********************Exception message ends************************");
				}
				count++;
			}
			Log.e(TAG, "====================================");
		}

		Log.e(TAG, "***********************insert() ends************************");
		return resultString;
	}

	/************************Insert logic ends*******************************/

	/************************Query logic begins*******************************/

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		isQuerying = true;
		while(!isInitialized) {
			try {
				Thread.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		context = getContext();
		String[] cursorColumns = new String[]{"key", "value"};
		Cursor returnCursor = null;
		MatrixCursor cursor = new MatrixCursor(cursorColumns);
		String key = selection;
		String keyHash = "";
		String value = "";
		try{
			keyHash = Util.genHash(key);
		}catch (Exception e){
			Log.e(TAG,"***********************Exception in query()...genHash()************************");
			e.printStackTrace();
		}

		if (key.equalsIgnoreCase("*")) {
			returnCursor = getGlobalDump();
		} else if (key.equalsIgnoreCase("@")) {
			returnCursor = getLocalDump();
		} else {
			/*if(Util.lookup(keyHash, myNodeDetails)){
				SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
				value = sharedPref.getString(key, "DEFAULT");
				String[] row = new String[]{key, value};
				cursor.addRow(row);
				returnCursor = cursor;
				Log.e(TAG, "Values retrieved : " + " key : " + key + "value : " + value);
			}else{*/
			Message message = new Message();
			message.setMessageType(MessageType.SingleQuery);
			message.setNodeDetails(myNodeDetails);
			message.setKey(key);
			String [] targetPorts = new String[3];
			NodeDetails targetNode = Util.getTargetNode(keyHash);
			targetPorts = Util.getReplicationPortNumbers(targetNode);
			targetPorts[0] = targetNode.getPort();
			boolean resultFetched = false;

			Log.e(TAG, "***********************querying ports to fetch result************************");
			ObjectOutputStream outputStream;
			ObjectInputStream inputStream;
			String queryResult=null;
			Socket socket;
			Map<String, String> responseMap = new HashMap<String, String>();
			for (String port: targetPorts) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					//socket.setSoTimeout(500);
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(message);
					outputStream.flush();
					Log.e(TAG, "Query sent to port : " + port + " for key : " + message.getKey());
					Log.e(TAG, "Waiting for response of query....");
					inputStream = new ObjectInputStream(socket.getInputStream());
					message = (Message)inputStream.readObject();
					queryResult = message.getValue();
					Log.e(TAG, "query result from port : "+port+" is : "+queryResult);
					responseMap.put(port, queryResult);
				} catch (Exception e) {
					Log.e(TAG, "************************Exception in query()******************");
					e.printStackTrace();
					Log.e(TAG, "***********************Exception message ends************************");
				}
			}
			Integer maxVersion = -1;
			String result = Util.getNewestVersionValue(targetPorts, responseMap);
			String[] row = new String[]{key, result};
			cursor.addRow(row);
			returnCursor = cursor;
			Log.e(TAG, "***********************query completed successfully************************");
			//}
		}
		Log.e(TAG, "Values retrieved and stored in cursor : " + DatabaseUtils.dumpCursorToString(returnCursor));
		isQuerying = false;
		return returnCursor;
	}

	private Cursor getGlobalDump(){
		Log.e(TAG, "***********************getGlobalDump begins************************");
		Message message = new Message();
		message.setMessageType(MessageType.GlobalDump);
		message.setNodeDetails(myNodeDetails);
		String[] cursorColumns = new String[]{"key", "value"};
		MatrixCursor cursor = new MatrixCursor(cursorColumns);
		ObjectOutputStream outputStream;
		ObjectInputStream inputStream;
		Socket socket;
		List<Message> messageList;
		Map<String, String> messageMap = new HashMap<String, String>();
		for (String port: REMOTE_PORTS) {
			Log.e(TAG,"====================================");
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				outputStream = new ObjectOutputStream(socket.getOutputStream());
				inputStream = new ObjectInputStream(socket.getInputStream());
				outputStream.writeObject(message);
				outputStream.flush();
				Log.e(TAG, "Waiting for Local Dump from port: " + port + "...");
				messageList = (List<Message>)inputStream.readObject();
				Log.e(TAG,"Response arrived... Local Dump from port : "+port+":");

				for (Message msg: messageList) {
					Log.e(TAG, "key: " + msg.getKey() + " value: " + msg.getValue());
					messageMap.put(msg.getKey(), msg.getValue().split(DELIMITER)[0]);
				}
			} catch (Exception e) {
				Log.e(TAG, "************************Exception in getGlobalDump()******************");
				e.printStackTrace();
				Log.e(TAG, "***********************Exception message ends************************");
			}
			for (Map.Entry<String, String> messageValue: messageMap.entrySet()) {
				String[] row = new String[]{messageValue.getKey(), messageValue.getValue()};
				cursor.addRow(row);
			}

			Log.e(TAG,"====================================");
		}
		Log.e(TAG, "***********************getGlobalDump ends************************");
		return cursor;
	}

	private MatrixCursor getLocalDump() {
		Log.e(TAG, "***********************getLocalDump begins************************");
		String[] cursorColumns = new String[]{"key", "value"};
		MatrixCursor cursor = new MatrixCursor(cursorColumns);
		Log.e(TAG, "Querying Local dump");
		Log.e(TAG, "Value dump from avd : " + getMyNodeId());
		SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
		Map<String, ?> keys = sharedPref.getAll();
		for (Map.Entry<String, ?> entry : keys.entrySet()) {
			Log.e(TAG,"====================================");
			String key = entry.getKey();
			String value = entry.getValue().toString().split(DELIMITER)[0];
			//try {
			//add the key-val pair to cursor only if it belongs to this node
			//	if (Util.lookup(Util.genHash(key), myNodeDetails)) {
			Log.e(TAG, "Key: " + key + ": " + " Value: " + value);
			String[] row = new String[]{key, value};
			cursor.addRow(row);
			//	}
			//}catch(Exception e) {
			//	Log.e(TAG, "************************Exception in getLocalDump()******************");
			//	e.printStackTrace();
			//	Log.e(TAG, "***********************Exception message ends************************");
			//}
			Log.e(TAG,"====================================");
		}
		Log.e(TAG, "***********************getLocalDump ends************************");
		return cursor;
	}

	/************************Query logic ends*******************************/

	/************************Delete logic begins*******************************/

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.e(TAG,"***********************delete() begins in Provider************************");
		while(!isInitialized) {
			try {
				Thread.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		int deleteCount = 0;
		String key = selection;
		String keyHash = "";
		try{
			keyHash = Util.genHash(key);
		}catch (Exception e){
			Log.e(TAG,"***********************Exception in query()...genHash()************************");
			e.printStackTrace();
		}

		if (key.equalsIgnoreCase("*")) {
			Log.e(TAG,"====================================");
			Log.e(TAG,"calling globalDeleteAll");
			Log.e(TAG,"====================================");
			deleteCount = globalDeleteAll();
		} else if (key.equalsIgnoreCase("@")) {
			Log.e(TAG,"====================================");
			Log.e(TAG,"calling localDeleteAll");
			Log.e(TAG,"====================================");
			deleteCount = localDeleteAll();
		} else {
			String[] deletePorts = new String[3];
			Message message = new Message();
			message.setKey(key);
			message.setNodeDetails(myNodeDetails);
			message.setMessageType(MessageType.SingleDelete);
			NodeDetails targetNode=Util.getTargetNode(keyHash);
			deletePorts = Util.getReplicationPortNumbers(targetNode);
			deletePorts[0] = targetNode.getPort();
			Log.e(TAG,"====================================");
			Log.e(TAG,"Key to be deleted : " + key + " keyHash : " + keyHash + " from target Node : " + deletePorts[0] + " and Replication Node 1 : " +deletePorts[1] + " and Replication Node 2 : " +deletePorts[2] );
			Log.e(TAG,"====================================");
			deleteCount = sendDeleteMessage(message, deletePorts);
		}
		Log.e(TAG,"***********************delete() ends in Provider************************");
		return deleteCount;
	}

	private int sendDeleteMessage(Message message, String[] ports){
		Log.e(TAG, "***********************sendDeleteMessage() begins************************");
		int deleteCount = 0;
		ObjectOutputStream outputStream;
		ObjectInputStream inputStream;
		Socket socket;
		for (String port: ports) {
			Log.e(TAG, "====================================");
			int localDeleteCount;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				outputStream = new ObjectOutputStream(socket.getOutputStream());
				inputStream = new ObjectInputStream(socket.getInputStream());
				Log.e(TAG,"Sending delete message to : " + port + " to delete key : " + message.getKey());
				outputStream.writeObject(message);
				outputStream.flush();
				Log.e(TAG, "Waiting for delete count...");
				localDeleteCount = (Integer)inputStream.readObject();
				Log.e(TAG,"Response arrived from port : "+port+" deleted : " + localDeleteCount + "entries");
				deleteCount+=localDeleteCount;
			} catch (Exception e) {
				Log.e(TAG, "************************Exception in sendDeleteMessage()******************");
				e.printStackTrace();
				Log.e(TAG, "***********************Exception message ends************************");
			}
			Log.e(TAG,"====================================");
		}
		Log.e(TAG, "***********************sendDeleteMessage() ends************************");
		return deleteCount;
	}

	private int globalDeleteAll() {
		Log.e(TAG, "***********************globalDeleteAll begins************************");
		Message message = new Message();
		message.setMessageType(MessageType.GlobalDelete);
		message.setNodeDetails(myNodeDetails);
		ObjectOutputStream outputStream;
		ObjectInputStream inputStream;
		Socket socket;
		int deleteCount=0;
		for (String port: REMOTE_PORTS) {
			int localDeleteCount = 0;
			Log.e(TAG,"====================================");
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				outputStream = new ObjectOutputStream(socket.getOutputStream());
				inputStream = new ObjectInputStream(socket.getInputStream());
				Log.e(TAG,"Sending delete message to : " + port + " to delete all data");
				outputStream.writeObject(message);
				outputStream.flush();
				Log.e(TAG, "Waiting for Local Delete from port: " + port + "...");
				localDeleteCount = (Integer)inputStream.readObject();
				Log.e(TAG,"Response arrived... Local Delete from port : "+port+" deleted : " + localDeleteCount + "entries");
				deleteCount += localDeleteCount;
			} catch (Exception e) {
				Log.e(TAG, "************************Exception in getGlobalDump()******************");
				e.printStackTrace();
				Log.e(TAG, "***********************Exception message ends************************");
			}
			Log.e(TAG,"====================================");
		}
		Log.e(TAG, "***********************globalDeleteAll ends************************");
		return deleteCount;
	}

	private int localDeleteAll(){
		Log.e(TAG, "***********************localDeleteAll begins************************");
		int deleteCount = 0;
		SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPref.edit();
		deleteCount = sharedPref.getAll().size();
		editor.clear();
		editor.commit();
		Log.e(TAG, "====================================");
		Log.e(TAG, "Total values Deleted from local system: " + deleteCount);
		Log.e(TAG,"====================================");
		Log.e(TAG, "***********************localDeleteAll ends************************");
		return deleteCount;
	}

	/************************Delete logic begins*******************************/

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


}
