package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tushar on 4/22/16.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> implements Constants {
    static final String TAG = ServerTask.class.getSimpleName();
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    static Map<String, Integer> objectVersionMap = new HashMap<String, Integer>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        ObjectInputStream inputStream;
        ObjectOutputStream outputStream;
        Socket socket;
        Integer deleteCount = -1;
        Log.e(TAG, "~~~~~~~~~~~~~~~~~~~Starting Server~~~~~~~~~~~~~~");
        Message message = new Message();
        while(true) {
            try {
                socket = serverSocket.accept();
                //Initialize input and output stream for full duplex sockect connection
                inputStream = new ObjectInputStream(socket.getInputStream());
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                message = (Message) inputStream.readObject();
                while(!SimpleDynamoProvider.isInitialized) {
                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                switch (message.getMessageType()) {
                    case Insert:
                        String returnMessage = insertValues(message);
                        outputStream.writeObject(returnMessage);
                        outputStream.flush();
                        break;
                    case SingleQuery:
                        message = singleQuery(message);
                        outputStream.writeObject(message);
                        outputStream.flush();
                        break;
                    case GlobalDump:
                        List<Message> messageList = getLocalDump();
                        outputStream.writeObject(messageList);
                        outputStream.flush();
                        break;
                    case SingleDelete:
                        deleteCount = singleDelete(message);
                        outputStream.writeObject(deleteCount);
                        outputStream.flush();
                        break;
                    case GlobalDelete:
                        deleteCount = localDeleteAll();
                        outputStream.writeObject(deleteCount);
                        outputStream.flush();
                        break;
                    case Recover:

                        List<Message> recoverList = getReplicatedMessages(message);
                        outputStream.writeObject(recoverList);
                        outputStream.flush();
                        break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private String insertValues(Message message){
        Log.e(TAG, "***********************Server Task insertValues begins************************");
                String key = message.getKey();
        String value = message.getValue();
        Integer versionObject = objectVersionMap.get(key);

        if(null != versionObject){
            versionObject = versionObject + 1;
        }else{
            versionObject = 1;
        }
        objectVersionMap.put(key,versionObject);
        value = value +DELIMITER+ versionObject;
        //Log.e(TAG, "====================================");
        //Log.e(TAG, "Key - Value pair to be inserted -> key: " + key + " - value: " + value);
        //Log.e(TAG, "====================================");
        Context context = SimpleDynamoProvider.getProviderContext();
        SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.putString(key, value);
        editor.commit();
        Log.e(TAG, "====================================");
        Log.e(TAG, "Key - Value pair inserttion complete-> key: " + key + " - value: " + value);
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************Server Task insertValues ends************************");

        return key+" : "+value;
    }

    private Message singleQuery(Message message){
        Log.e(TAG, "***********************Server Task singleQuery begins************************");
        SharedPreferences sharedPref = SimpleDynamoProvider.getProviderContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        String value = sharedPref.getString(message.getKey(), "DEFAULT");
        message.setValue(value);
        Log.e(TAG,"====================================");
        Log.e(TAG, "Single query result -> key: " + message.getKey() + " value: " + message.getValue());
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************Server Task singleQuery ends************************");
        return message;
    }

    private List<Message> getLocalDump(){
        List<Message> messageList = new ArrayList<Message>();
        Log.e(TAG, "***********************getLocalDump begins in ServerTask************************");
        Message msg;
        SharedPreferences sharedPref = SimpleDynamoProvider.getProviderContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        Map<String, ?> keys = sharedPref.getAll();
        for (Map.Entry<String, ?> entry : keys.entrySet()) {
            Log.e(TAG,"====================================");
            msg = new Message();
            String key = entry.getKey();
            String value = entry.getValue().toString();
            try {
                //add the key-val pair to cursor only if it belongs to this node
                //if (Util.lookup(Util.genHash(key), SimpleDynamoProvider.myNodeDetails)) {
                Log.e(TAG, "Key: " + key + ": " + " Value: " + value);
                msg.setKey(key);
                msg.setValue(value);
                messageList.add(msg);
                //}
            }catch(Exception e) {
                Log.e(TAG, "************************Exception in getLocalDump()******************");
                e.printStackTrace();
                Log.e(TAG, "***********************Exception message ends************************");
            }
            Log.e(TAG,"====================================");
        }
        Log.e(TAG, "***********************getLocalDump ends in ServerTask************************");
        return messageList;
    }

    private int singleDelete(Message message){
        Log.e(TAG, "***********************Server Task singleDelete begins************************");
        int deleteCount;
        SharedPreferences sharedPref = SimpleDynamoProvider.getProviderContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.remove(message.getKey());
        editor.apply();
        deleteCount = 1;
        Log.e(TAG, "====================================");
        Log.e(TAG, "Total deleted keys-value paris : " + deleteCount + " Deleted key : " + message.getKey() );
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************Server Task singleDelete ends************************");
        return deleteCount;
    }

    private int localDeleteAll(){
        Log.e(TAG, "***********************localDeleteAll begins in ServerTask************************");
        int deleteCount = 0;
        SharedPreferences sharedPref = SimpleDynamoProvider.getProviderContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        deleteCount = sharedPref.getAll().size();
        editor.clear();
        editor.commit();
        Log.e(TAG, "====================================");
        Log.e(TAG, "Total values Deleted from system : " + deleteCount);
        Log.e(TAG,"====================================");
        Log.e(TAG, "***********************localDeleteAll ends in ServerTask************************");
        return deleteCount;
    }


    private List<Message> getReplicatedMessages(Message message){
        List<Message> messageList = new ArrayList<Message>();
        Log.e(TAG, "***********************getReplicatedMessages begins in ServerTask************************");
        if(SimpleDynamoProvider.isInserting || SimpleDynamoProvider.isQuerying){
            try {
                Log.e(TAG, "Yielding Server task as insert/query in progress");
                Thread.yield();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Message msg;
        SharedPreferences sharedPref = SimpleDynamoProvider.getProviderContext().getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        Map<String, ?> keys = sharedPref.getAll();
        Log.e(TAG,"====================================");
        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            msg = new Message();
            String key = entry.getKey();
            String value = entry.getValue().toString();
            Log.e(TAG, "Key: " + key + ": " + " Value: " + value);
            msg.setKey(key);
            msg.setValue(value);
            messageList.add(msg);
        }
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************getReplicatedMessages ends in ServerTask************************");
        return messageList;
    }


}

