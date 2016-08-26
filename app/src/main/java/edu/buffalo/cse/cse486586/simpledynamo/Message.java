package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;

import java.io.Serializable;

/**
 * Created by tushar on 4/7/16.
 */
public class Message implements Serializable {
    private MessageType messageType;
    private NodeDetails nodeDetails = new NodeDetails();
    //private ContentValues contentValues;
    private String key;
    private String value;


    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public NodeDetails getNodeDetails() {
        return nodeDetails;
    }

    public void setNodeDetails(NodeDetails nodeDetails) {
        this.nodeDetails = nodeDetails;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
/*
    public ContentValues getContentValues() {
        return contentValues;
    }

    public void setContentValues(ContentValues contentValues) {
        this.contentValues = contentValues;
    }*/

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", nodeDetails=" + nodeDetails +
               // ", contentValues=" + contentValues.toString() +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getKeyValueString() {
        return "Message{" +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                "}\n";
    }

}
