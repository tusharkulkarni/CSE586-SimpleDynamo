package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


// Created by tushar on 4/22/16.




public class ClientTask extends AsyncTask<Message, Void, Void> implements Constants {
    static final String TAG = ClientTask.class.getSimpleName();
    @Override
    protected Void doInBackground(Message... msg) {

        Message message = msg[0];

        switch (message.getMessageType()){
            case Insert:    //insert(message);
                break;
            case SingleQuery: //singleQuery(message);
                break;
            case GlobalDump: //queryGlobalDump(message);
                break;
            case SingleDelete: //singleDelete(message);
                break;
            case GlobalDelete: //executeGlobalDelete(message);
                break;
            case Recover: recover();
                break;
        }

        return null;
    }

    private String insertValue(Message message){
        Log.e(TAG, "***********************insertValue begins for Recovery************************");
        String key = message.getKey();
        String value = message.getValue();
        String resultVal="";
        Integer myVersion = ServerTask.objectVersionMap.get(key);
        Integer fetchedVersion = Integer.valueOf(value.split(DELIMITER)[1]);
        Integer finalVersion=null;
        if(null != myVersion){
            if(fetchedVersion>myVersion){
                finalVersion = fetchedVersion;
                resultVal = value;
            }else{
                return "MyVersion Greater than fetched Version... returning without inserting";
            }
        }else{
            finalVersion = fetchedVersion;
            resultVal = value;
        }

        ServerTask.objectVersionMap.put(key,finalVersion);

        Context context = SimpleDynamoProvider.getProviderContext();
        SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.putString(key, resultVal);
        editor.commit();
        Log.e(TAG, "====================================");
        Log.e(TAG, "Key - Value pair inserted -> key: " + key + " - value: " + value);
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************insertValue ends for Recovery************************");

        return "SUCCESS";
    }

    private List<NodeDetails> getReplicaNodeDetails(NodeDetails curentNode) {
        Log.e(TAG, "***********************getReplicaNodeDetails() begins ************************");
        List<NodeDetails> replicaNodeList = new ArrayList<NodeDetails>();
        NodeDetails prevNode1 = new NodeDetails();
        NodeDetails prevNode2 = new NodeDetails();

        for (NodeDetails node: SimpleDynamoProvider.chordNodeList) {
            if(node.getSuccessorPort().equals(curentNode.getPort())){
                prevNode1 = node;
            }
        }

        for (NodeDetails node: SimpleDynamoProvider.chordNodeList) {
            if(node.getSuccessorPort().equals(prevNode1.getPort())){
                prevNode2 = node;
            }
        }
        replicaNodeList.add(prevNode1);
        replicaNodeList.add(prevNode2);
        replicaNodeList.add(curentNode);
        Log.e(TAG, "====================================");
        Log.e(TAG, "Current node: " + curentNode.getNodeIdHash() + " should contain data within following ranges: ");
        Log.e(TAG, "Range 1 : " + prevNode2.getPredecessorNodeIdHash()+ " - " + prevNode2.getNodeIdHash());
        Log.e(TAG, "Range 2 : " + prevNode1.getPredecessorNodeIdHash()+ " - " + prevNode1.getNodeIdHash());
        Log.e(TAG, "Range 3 : " + curentNode.getPredecessorNodeIdHash()+ " - " + curentNode.getNodeIdHash());
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************getReplicaNodeDetails() ends ************************");
        return replicaNodeList;
    }


    private void recover(){
        Log.e(TAG, "***********************recover() begins in Client Task************************");

        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        List<NodeDetails> replicaNodeList = getReplicaNodeDetails(SimpleDynamoProvider.myNodeDetails);
        Message recoveryMessage = new Message();
        recoveryMessage.setMessageType(MessageType.Recover);
        List<Message> messageList = new ArrayList<Message>();
        List<Message> globalMessageList = new ArrayList<Message>();
        for (String port: REMOTE_PORTS) {
            Log.e(TAG,"====================================");
            try {
                if(port.equals(SimpleDynamoProvider.myNodeDetails.getPort())){
                    continue;
                }
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                inputStream = new ObjectInputStream(socket.getInputStream());
                outputStream.writeObject(recoveryMessage);
                outputStream.flush();
                Log.e(TAG, "Waiting for Data from port: " + port + "...");
                messageList = (List<Message>)inputStream.readObject();
                Log.e(TAG,"Data arrived from port : "+port+": " +messageList.toString());
                globalMessageList.addAll(messageList);
            } catch (Exception e) {
                Log.e(TAG, "************************Exception in getGlobalDump()******************");
                e.printStackTrace();
                Log.e(TAG, "***********************Exception message ends************************");
            }
            Log.e(TAG,"====================================");
        }

        NodeDetails node1 = replicaNodeList.get(0);
        NodeDetails node2 = replicaNodeList.get(1);
        NodeDetails node3 = replicaNodeList.get(2);

        for (Message message : globalMessageList) {
            try {
                String key = message.getKey();
                String keyHash = Util.genHash(key);
                if (Util.lookup(keyHash, node1) || Util.lookup(keyHash, node2) || Util.lookup(keyHash, node3)){
                    insertValue(message);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        SimpleDynamoProvider.isInitialized = true;
        Log.e(TAG, "***********************recover() ends in Client Task************************");
    }



/*
    //possibility of error during concurrency as new value might be overwritten by old value due to concurrency.... think of how to handle this if the issue arises.
    private void insert(Message message){
        Log.e(TAG, "***********************insert() begins************************");
        String key = message.getKey();
        String[] insertionPorts = new String[3];
        String keyHash="";
        try{
            keyHash = Util.genHash(key);
        }catch (Exception e){
            Log.e(TAG, "***********************Exception in insert() while generating keyHash************************");
            e.printStackTrace();
            Log.e(TAG, "***********************Exception message ends************************");
        }

        NodeDetails targetNode=Util.getTargetNode(keyHash);

        insertionPorts = Util.getReplicationPortNumbers(targetNode);
        insertionPorts[0] = targetNode.getPort();
        Log.e(TAG,"*******************************************************************");
        Log.e(TAG,"Key : " + key + " keyHash : " + keyHash + " target Node : " + insertionPorts[0] + " Replication Node 1 : " +insertionPorts[1] + " Replication Node 2 : " +insertionPorts[2] );
        Log.e(TAG,"*******************************************************************");

        sendInsertMessage(message, insertionPorts);

        Log.e(TAG, "***********************insert() ends************************");

    }

    private void sendInsertMessage(Message message, String[] ports){
        Log.e(TAG, "***********************sendInsertMessage() begins************************");
        ObjectOutputStream outputStream;
        Socket socket;
        for (String port: ports) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                Log.e(TAG, "Insert Message with key : " + message.getKey() + " with keyHash : " + Util.genHash(message.getKey()) + " sent to port : " + port + " with node hash : " + Util.genHash(String.valueOf(Integer.parseInt(port) / 2)));
                outputStream.writeObject(message);
                outputStream.flush();
            } catch (Exception e) {
                Log.e(TAG, "************************Exception in ClientTask().sendInsertMessage()******************");
                e.printStackTrace();
                Log.e(TAG, "***********************Exception message ends************************");
            }

        }
        Log.e(TAG, "***********************sendInsertMessage() ends************************");

    }

    private void singleDelete(Message message){
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        Integer deleteCount=0;


        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDynamoProvider.myNodeDetails.getSuccessorPort()));
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            inputStream = new ObjectInputStream(socket.getInputStream());
            Log.e(TAG, "****************************************");
            Log.e(TAG, "key to be deleted retrieved from message : " + message.getKey());

            outputStream.writeObject(message);
            outputStream.flush();
            deleteCount = (Integer)inputStream.readObject();

            SimpleDynamoProvider.globalDeleteCount = deleteCount;
            Log.e(TAG, "DeleteCount inside ClientTask(): " + deleteCount);
            Log.e(TAG, "VGlobal Delete Count inside ClientTask() : " + SimpleDynamoProvider.globalDeleteCount);
            Log.e(TAG, "****************************************");
        } catch (Exception e) {
            Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
            e.printStackTrace();
            Log.e(TAG, "***********************Exception message ends************************");
        }
    }

    private void executeGlobalDelete(Message message){
        int deleteCount = 0;
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;

        for (String port: REMOTE_PORTS) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                inputStream = new ObjectInputStream(socket.getInputStream());

                outputStream.writeObject(message);
                outputStream.flush();
                deleteCount += (Integer)inputStream.readObject();

            } catch (Exception e) {
                Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
                e.printStackTrace();
                Log.e(TAG, "***********************Exception message ends************************");
            }
        }
        SimpleDynamoProvider.globalDeleteCount = deleteCount;
    }*/

}

