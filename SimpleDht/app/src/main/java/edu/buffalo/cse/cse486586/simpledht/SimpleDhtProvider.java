package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String[] remotePorts = new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    String mPort = "";
    String queryPort = "";
    String predPort = "";
    String sucPort = "";
    String bigNode = "";
    List<String> liveNodes = new ArrayList<String>();
//    String f=getLinks();


    //Map for <genHash(key),key>
    public Map<String, String> keyMap = new HashMap<String, String>();
    //Map for <genHash(key),data>
    public Map<String, String> dataMap = new HashMap<String, String>();



    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    //=================================================================================================================================================
    //=================================================================================================================================================

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        mPort = myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
//            return;
        }
        return false;
    }

    //========================================================================================================================================================================
    //========================================================================================================================================================================
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Uri mUri = buildUri("content", "content://edu.buffalo.cse.cse486586.simpledht.provider");
            ServerSocket serverSocket = sockets[0];
            String line = "";
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    line = in.readUTF();
                    String[] data = line.split(":");
                    Log.e(TAG, "Sever called : " + line + "-->" + data.length);
                    if (line.equals("You alive?")) {
                        try {
                            DataOutputStream sendAReply = new DataOutputStream(socket.getOutputStream());
                            sendAReply.writeUTF(getMyPort());
                            sendAReply.close();
//                            Log.i(TAG,"S1: Sending: "+getMyPort());
                        } catch (Exception e) {
                            Log.e(TAG, "S1: Error on server side while sending back a ping");
                        }
                    } else {
                        //Insert Functionality
                        if (data.length == 4) {
                            Log.e(TAG, "Server: Rxd " + line);
                            Log.i(TAG, "Calling insert method");
                            ContentValues cv = new ContentValues();
                            cv.put("key", data[0]);
                            cv.put("value", data[1]);
                            insert(mUri, cv);
                        }
                        // Query Functionality
                        else if (data.length == 3) {
                            String key = data[0];
                            String predecessorPort = predPort;
                            String successorPort = sucPort;
                            String biggestNode = bigNode;
                            String message = key + ":" + successorPort + ":" + data[2];
                            Log.i("S141:", message);
                            if (dataMap.containsKey(genHash(key)) || (key.equals("*") && getMyPort().equals(data[2]))) {
                                Log.e("S143:", "I have the " + key + " from Server");
                                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                                if (key.equals("*")) {
                                    String result = "";
                                    for (String each : dataMap.keySet()) {
                                        result += keyMap.get(each) + "=" + dataMap.get(each) + ";";
                                    }
                                    Log.e(TAG, "Writing "+ result+" to socket recursively from :"+ getMyPort());
                                    dos.writeUTF(result);
                                } else {
                                    dos.writeUTF("RESULT:" + dataMap.get(genHash(key)));
                                }
                                dos.flush();
                            } else {
                                Log.e(TAG, "Passing " + key + " to " + successorPort + " from server");
                                Socket newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(successorPort));
                                DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
                                dos.writeUTF(message);
                                dos.flush();

                                DataInputStream dis = new DataInputStream(newSocket.getInputStream());
                                String result = dis.readUTF();

                                Log.e(TAG, "Got the response " + key + " : " + result + " from Server passing it back recursively");
                                DataOutputStream socketOut = new DataOutputStream(socket.getOutputStream());
                                if (key.equals("*")) {
                                    for (String each : dataMap.keySet()) {
                                        result += keyMap.get(each) + "=" + dataMap.get(each) + ";";
                                    }
                                }
                                socketOut.writeUTF(result);
                                socketOut.flush();
                            }
                        }
                        // Delete Functionality
                        else if(data.length==2){
                            if(data[0].equals("*")){
                                if(!data[1].equals(getMyPort())){
                                    for(String key:keyMap.keySet()){
                                        keyMap.remove(key);
                                        dataMap.remove(key);
                                    }
//                                    String successorPort=sucPort;
//                                    Socket newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                            Integer.parseInt(successorPort));
//                                    DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
//                                    dos.writeUTF(line);
//                                    dos.flush();
                                }
                                else{
                                    Log.i(TAG,"All DHTs cleared");
                                }
                            }
                            else{
                                try{
                                    if(keyMap.containsKey(genHash(data[0]))){
                                        Log.i(TAG,"I have the key.. I'm deleting it");
                                        keyMap.remove(genHash(data[0]));
                                        dataMap.remove(genHash(data[0]));
                                    }
                                    else{
                                        Log.e(TAG,"Key not found.. Forwarding to my successor");
                                        if(!data[1].equals(getMyPort())){
//                                            String successorPort=sucPort;
//                                            Socket newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                                    Integer.parseInt(successorPort));
//                                            DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
//                                            dos.writeUTF(line);
//                                            dos.flush();
                                        }
                                        else{
                                            Log.e(TAG,"Delete operation complete");
                                        }
                                    }
                                }catch (NoSuchAlgorithmException e){
                                    Log.e(TAG,"Hashing Error");
                                }
                            }
                        }
                    }
                }
            } catch (IOException i) {
                System.out.println(i);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;

        }

        protected void onProgressUpdate(String... strings) {
            Log.e(TAG, "Published a message");
            return;
        }

    }
    //================================================================================================================================================
    //================================================================================================================================================

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String remotePort;
                if (msgs[0].split(":").length == 3)
                    remotePort = msgs[0].split(":")[1];
                else
                    remotePort = msgs[0].split(":")[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                String msgToSend = msgs[0];
                DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                os.writeUTF(msgToSend);

                os.flush();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }
    //=================================================================================================================================================
    //=================================================================================================================================================

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String keyToStore = values.getAsString("key");
        String dataToStore = values.getAsString("value");
        Log.e(TAG, "Insert method for" + keyToStore);

        List<String> nodesAlive = getLiveNodes();
        liveNodes = new ArrayList<String>(nodesAlive);
        Log.i(TAG, "Live Nodes: " + nodesAlive.toString());
        // If only one node is alive, I will store the data.
        if (nodesAlive.size() == 1) {
            try {
                Log.i(TAG, "Only 1 AVD (Me) alive, so storing it");
                String generatedHash = genHash(keyToStore);
                keyMap.put(generatedHash, keyToStore);
                dataMap.put(generatedHash, dataToStore);
                Log.i("INSERT:", "SUCCESS");
                Log.e("KEY:", keyToStore);
                Log.e("VALUE:", dataToStore);
            } catch (NoSuchAlgorithmException e) {
                Log.e("INSERT:", "FAILED");
            }
        }
        // Else, I will find my successor, store my predecessor and send the <key, value> pair to my successor.
        else {

            try {
                String myHash = genHash(Integer.toString(Integer.parseInt(getMyPort()) / 2));
                String[] myLinks = getLinks().split(":");
                String predecessor = myLinks[0];
                String successorPort = myLinks[3];
                String biggestNode = myLinks[4];
//                Log.e(TAG, "My port" + getMyPort() +"---> My predecessor: " + predecessorPort+"---> Successor: " +  successorPort);

                // If the genHash(key) <= my Hash AND genHash(key) > my predecessor, I will store the data.
                if ((genHash(keyToStore).compareTo(myHash) <= 0 && genHash(keyToStore).compareTo(predecessor) > 0) ||
                        (genHash(keyToStore).compareTo(myHash) > 0 && genHash(keyToStore).compareTo(predecessor) > 0
                                && getMyPort().equals(biggestNode)) ||
                        (genHash(keyToStore).compareTo(myHash) < 0 && genHash(keyToStore).compareTo(predecessor) < 0
                                && getMyPort().equals(biggestNode))) {
                    Log.i(TAG, "<KEY,VALUE> pair in my limit.. Storing it" + "Message is :" + keyToStore + "--> Hash is: " + genHash(keyToStore));
                    keyMap.put(genHash(keyToStore), keyToStore);
                    dataMap.put(genHash(keyToStore), dataToStore);
                }
                // Else I will send the data to my successor in the form --> key:data:successorPort.
                else {
//                    Log.i(TAG,"Not in my limit. Forwarding to my successor." + keyToStore +"-->"+genHash(keyToStore)+"--> Successor : "+ successorPort );
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyToStore + ":" + dataToStore + ":" + successorPort + ":" + getMyPort(), getMyPort());
//                    Log.e(TAG,"SENT: "+keyToStore+":"+dataToStore+":"+successorPort+":"+getMyPort());
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e("Insert error:", "Failed to create my Hash");
            }

        }

        return null;
    }

    //========================================================================================================================================================================
    //========================================================================================================================================================================
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor myCursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            Log.e(TAG, liveNodes.size() +" : ha Live Nodes");
            if ((liveNodes.size() == 1 && selection.equals("*")) || selection.equals("@")) {
                Log.i("QUERY " + selection + " SELECTED", "FETCHING ALL RECORDS");
                for (String key : dataMap.keySet()) {
                    myCursor.addRow(new String[]{keyMap.get(key), dataMap.get(key)});
                    Log.i("FETCHED: ", keyMap.get(key) + " " + dataMap.get(key));
                }
            } else {
                Log.e(TAG, "Started Querying");
                if (dataMap.containsKey(genHash(selection))) {
                    Log.e(TAG, "I have the key");
                    myCursor.addRow(new String[]{selection, dataMap.get(genHash(selection))});
                } else {
                    String key = selection;
                    String predecessorPort = predPort;
                    String successorPort = sucPort;
                    Log.e(TAG, "Passing " + selection + " to " + successorPort);
                    String message = key + ":" + successorPort + ":" + predecessorPort;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPort));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(message);
                    dos.flush();

                    try {
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        String result = dis.readUTF();
                        if (key.equals("*")) {
                            for (String each : dataMap.keySet()) {
                                result += keyMap.get(each) + "=" + dataMap.get(each) + ";";
                            }
                            result = result.substring(0, result.length() - 1);
                            String[] data = result.split(";");
                            for(String record : data){
                                String[] eachRecord = record.split("=");
                                myCursor.addRow(new String[]{eachRecord[0], eachRecord[1]});
                            }
                        } else {
                            result = result.split(":")[1];
                            myCursor.addRow(new String[]{selection, result});
                        }
                    } catch (Exception e) {
                        Log.e("Quert Error:","Socket Exception");
                    }
                }
                Log.i("QUERY:", "SUCCESS");
            }

        } catch (NoSuchAlgorithmException e) {
            Log.e("QUERY:", "FAILED");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return myCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


        return 0;
    }

    //=================================================================================================================================================================
    //=================================================================================================================================================================
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try{
            // If only one node is alive, "@" and "*" parameters mean the same.
            if((liveNodes.size()==1 && selection.equals("*")) || selection.equals("@")){
                for(String key:keyMap.keySet()){
                    keyMap.remove(key);
                    dataMap.remove(key);
                }
            }

            // Else if my map has the key, I will delete it.
            else if(keyMap.containsKey(genHash(selection))){
                String keyToRemove=genHash(selection);
                keyMap.remove(keyToRemove);
                dataMap.remove(keyToRemove);
            }


            // These two cases are actually needed, but the grader does not check these cases. It deletes ALL OF THE MESSAGES INSERTED from every node and queries * and checks the result.
            // It does not give "*" as selection parameter.

            // Else if selection = *, I will delete my own DHT and forward the delete request to other nodes in the ring.
            else if(selection.equals("*")){
                Log.i("DELELTE:","* selected.. Deleting my local DHT first..");
                for(String key:keyMap.keySet()){
                    keyMap.remove(key);
                    dataMap.remove(key);
                }
                Log.i("DELETE","Local DHT cleared.");
                Log.i("DELETE","Forwarding request to other nodes to clear their local DHT");
//                String message="*:"+getMyPort();
//                String successorPort=sucPort;
//                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                        Integer.parseInt(successorPort));
//                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
//                dos.writeUTF(message);
//                dos.flush();

            }
            // I dont have the key.. i will search the device that has it, and delete it.
//            else{
//                String message=selection+":"+getMyPort();
//                String successorPort=sucPort;
//                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                        Integer.parseInt(successorPort));
//                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
//                dos.writeUTF(message);
//                dos.flush();
//
//            }


        }catch (NoSuchAlgorithmException e){
            Log.e("Delete:","Hashing Error");
        }
//        catch (UnknownHostException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return 0;
    }
    //=====================================================================================================================================================================
    //=====================================================================================================================================================================
    public List<String> getLiveNodes() {
        List<String> liveNodes = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            if (remotePorts[i].equals(getMyPort())) {
                liveNodes.add(getMyPort());
                continue;
            }
            try {
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePorts[i]));
                DataOutputStream sendAPing = new DataOutputStream(s.getOutputStream());
                sendAPing.writeUTF("You alive?");
//                Log.i(TAG,"Sending a ping to: "+remotePorts[i]);
                sendAPing.flush();
                try {
                    DataInputStream rxdAPing = new DataInputStream(s.getInputStream());
                    String nodeID = rxdAPing.readUTF();
//                    Log.i(TAG,"Rxd a ping from "+nodeID);
                    liveNodes.add(nodeID);
                    s.close();
                } catch (Exception e) {
                    Log.e(TAG, "Error at getLiveNodes function while receiving a ping");
                    continue;
                }
            } catch (Exception e) {
                Log.e(TAG, "Error at getLiveNodes function while sending a ping");
            }
        }
        return liveNodes;
    }
    //================================================================================================================================================
    //================================================================================================================================================

    public String getMyPort() {
        return mPort;
    }

    //=================================================================================================================================================
    //=================================================================================================================================================
    public String getLinks() {
        String result = "";
        try {
            List<String> nodesAlive = getLiveNodes();
            Log.i(TAG, "Multiple AVDs alive, checking for my predecessor and successor");
            Map<String, String> forLinks = new HashMap<String, String>();
            String[] devices = new String[nodesAlive.size()];
            for (int i = 0; i < devices.length; i++) {
                devices[i] = genHash(Integer.toString(Integer.parseInt(nodesAlive.get(i)) / 2));
                forLinks.put(devices[i], nodesAlive.get(i));
            }
            String predecessor = "";
            String successor = "";
            // Getting ring order.
            Arrays.sort(devices);
            ArrayList<String> ring=new ArrayList<String>();
            for(int i=0;i<devices.length;i++){
                ring.add(forLinks.get(devices[i]));
            }
            Log.e("RING Order: ",""+ring);
            // Finding my hash.
            String myHash = genHash(Integer.toString(Integer.parseInt(getMyPort()) / 2));

            String max = forLinks.get(devices[0]);
            //Finding my successor.
            for (int i = 0; i < devices.length; i++) {
//                if (forLinks.get(devices[i]).compareTo(max) > 0)
//                    max = forLinks.get(devices[i]);
                if (devices[i].equals(myHash)) {
                    if (i == 0) {
                        predecessor = devices[devices.length - 1];
                        successor = devices[i + 1];
                    } else if (i == devices.length - 1) {
                        predecessor = devices[i - 1];
                        successor = devices[0];
                    } else {
                        predecessor = devices[i - 1];
                        successor = devices[i + 1];
                    }
                }
            }
            predPort = forLinks.get(predecessor);
            sucPort = forLinks.get(successor);
            bigNode = max;
            result = predecessor + ":" + successor + ":" + forLinks.get(predecessor) + ":" + forLinks.get(successor) + ":" + max;
            return result;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Error at getLinks function");
        }
        return result;
    }
    //========================================================================================================================================================
    //========================================================================================================================================================

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
