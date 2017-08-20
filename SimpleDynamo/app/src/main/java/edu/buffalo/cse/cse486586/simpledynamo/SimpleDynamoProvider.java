package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static ArrayList<String> nodeList = new ArrayList<String>();
    static ArrayList<String> emulatorList = new ArrayList<String>();
    static String myPort = "";
    static String nodeId = "";
    static ArrayList<String> nodeListClient = new ArrayList<String>();
    static ArrayList<String> globallist = new ArrayList<String>();
    static String successor = "";
    static String predecessor = "";


    //static String globalKey = "";
    static ArrayList<String> starContainer = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.i(TAG, "Delete : Delete hit");
        String path = getContext().getFilesDir().getPath();
        File f = new File(path);
        f.mkdirs();
        Boolean found = false;
        File[] file = f.listFiles();
        for (File fi : file) {
            if (fi.getName().contains(selection)) {
                Log.i(TAG, "Delete : Deleting :" + selection);
                found = true;
                fi.delete();
            }
        }
        if (!found) {
            Log.i(TAG, "Delete : Not Found, so calling client");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "delete", selection);
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.i(TAG, "Insert : Insert hit");
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        Log.i(TAG, "Insert : key is " + key);
        Log.i(TAG, "Insert : value is " + value);
        String[] splitKey = key.split(" ");
        Log.i(TAG, "Insert : splitKey size is " + splitKey.length);
        if (splitKey.length == 2) {
            key = splitKey[1];
        }
        String fileName = "";
        if (splitKey.length == 2) {
            Log.i(TAG, "Insert : splitKey length is 2 : " + splitKey[1]);
            fileName = splitKey[1];
        } else if (splitKey.length == 1) {
            Log.i(TAG, "Insert : splitKey length is 1 : " + splitKey[0]);
            fileName = myPort + splitKey[0];
        } else {//split key length is  3 // asked for  is there
            Log.i(TAG, "split key length is 3:" + splitKey[2]);
            fileName = splitKey[2];
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (IOException e) {
                Log.e(TAG, "Failure of writing data into a file");
            }

        }


        String hashKey = "";
        try {
            if (splitKey.length == 1 || splitKey[0].equals("dummy")) {
                hashKey = genHash(key);
                Log.i(TAG, "Insert : Hash key is " + hashKey);
                // Determine target nodeId
                String targetNode = "";
                if (!splitKey[0].equals("dummy")) {
                    Log.i(TAG, "Insert : emulator size is :" + emulatorList.size());
                    Log.i(TAG, "Insert : node size is :" + nodeList.size());
                    if ((hashKey.compareTo(genHash(emulatorList.get(0))) > 0 &&
                            hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) > 0)) {
                        Log.i(TAG, "Insert : Setting target node as 0, first if check :" + nodeList.get(0));
                        targetNode = nodeList.get(0);
                    }
                    if ((hashKey.compareTo(genHash(emulatorList.get(0))) < 0 &&
                            hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) < 0)) {
                        Log.i(TAG, "Insert : Setting target node as 0, second if check :" + nodeList.get(0));
                        targetNode = nodeList.get(0);
                    }
                    if ((hashKey.compareTo(genHash(emulatorList.get(0))) <= 0 &&
                            hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) > 0)) {
                        Log.i(TAG, "Insert : Setting target node as 0, third if check :" + nodeList.get(0));
                        targetNode = nodeList.get(0);
                    }
                    for (int i = 1; i < emulatorList.size(); i++) {
                        Log.i(TAG, "Insert : emulator size is :" + emulatorList.size());
                        if ((hashKey.compareTo(genHash(emulatorList.get(i - 1))) > 0 &&
                                hashKey.compareTo(genHash(emulatorList.get(i))) <= 0)) {
                            Log.i(TAG, "Insert : Setting target node as i+1 :" + nodeList.get(i));
                            targetNode = nodeList.get(i);
                        }
                    }
                } else if (splitKey[0].equals("dummy")) {
                    targetNode = myPort;
                }
                Log.i(TAG, "Insert : Target Node is :" + targetNode);

                // If I am the target node just store and send to my two successors
                if (targetNode.equals(myPort)) {
                    Log.i(TAG, "Insert : I am the target node");
                    FileOutputStream outputStream;
                    try {
                        outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                    } catch (IOException e) {
                        Log.e(TAG, "Failure of writing data into a file");
                    }
                    if (!splitKey[0].equals("dummy")) {
                        String successorOne = "";
                        String successorTwo = "";
                        int indexToDetermineSuccessor = nodeList.indexOf(myPort);
                        Log.i(TAG, "Insert : indexToDetermineSuccessor is :" + indexToDetermineSuccessor);
                        if (indexToDetermineSuccessor == nodeList.size() - 2) {
                            // Last but two index
                            successorOne = nodeList.get(indexToDetermineSuccessor + 1);
                            successorTwo = nodeList.get(0);

                        } else if (indexToDetermineSuccessor == nodeList.size() - 1) {
                            successorOne = nodeList.get(0);
                            successorTwo = nodeList.get(1);
                        } else {
                            successorOne = nodeList.get(indexToDetermineSuccessor + 1);
                            successorTwo = nodeList.get(indexToDetermineSuccessor + 2);
                        }
                        Log.i(TAG, "Insert : Calling client task to send to my successors");
                        Log.i(TAG, "Insert : My successors are :\n 1." + successorOne + "\n" + "2." + successorTwo);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "send", "successors",
                                successorOne, successorTwo, key, value);
                    }
                } else {
                    // I am not the target node
                    // Ask the client task to send it to the target node only
                    Log.i(TAG, "Insert : I am not the target node");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "send", "target", targetNode, key, value);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such algorithm exception");
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.i(TAG, "On create hit");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i(TAG, "Provider myPort is :" + myPort);
        //nodeList.add(myPort);
        // Based on myPort, the node id is determined
        switch (Integer.parseInt(myPort)) {
            case 11108:
                nodeId = String.valueOf(5554);
                break;
            case 11112:
                nodeId = String.valueOf(5556);
                break;
            case 11116:
                nodeId = String.valueOf(5558);
                break;
            case 11120:
                nodeId = String.valueOf(5560);
                break;
            case 11124:
                nodeId = String.valueOf(5562);
                break;
        }
        try {
            Log.i(TAG, "Just before server task invoking");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Error in creating a socket");
            return false;
        }
        nodeList.add("11108");
        nodeList.add("11112");
        nodeList.add("11116");
        nodeList.add("11120");
        nodeList.add("11124");
        emulatorList.add("5554");
        emulatorList.add("5556");
        emulatorList.add("5558");
        emulatorList.add("5560");
        emulatorList.add("5562");
        Collections.sort(nodeList, new Dummy());
        Collections.sort(emulatorList, new Dummy());
        int ok = nodeList.indexOf(myPort);
        if(ok == 4) {
            successor = nodeList.get(0);
            predecessor = nodeList.get(ok - 1);
        } else if(ok == 0) {
            successor = nodeList.get(ok + 1);
            predecessor = nodeList.get(4);
        } else {
            successor = nodeList.get(ok + 1);
            predecessor = nodeList.get(ok - 1);
        }

        Log.i(TAG, "emulator list is : ");
        for (String s : emulatorList) {
            Log.i(TAG, s + " ");
        }
        Log.i(TAG, "node list is : ");
        for (String s : nodeList) {
            Log.i(TAG, s + " ");
        }
        String path = getContext().getFilesDir().getPath();
        File f = new File(path);
        f.mkdirs();
        //Boolean found = false;
      /*  File[] file = f.listFiles();
        for (File fi : file) {

               // Log.i(TAG, "Delete : Deleting :" + selection);
                //found = true;
                fi.delete();

        } */
        Log.i(TAG, "Just before client task invoking");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "ask");

        // TODO Auto-generated method stub
        return false;
    }

    @Override

    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        // TODO Auto-generated method stub
        Log.i(TAG, "Query method  with selection as :" + selection);
        FileInputStream inputStream;
        String value = "";

        byte[] buffer = new byte[1024];
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        String path = getContext().getFilesDir().getPath();
        File f = new File(path);
        f.mkdirs();
        File[] file = f.listFiles();
        if (selection.equals("@")) {
            Log.i(TAG, "File array size is :" + file.length);

            int n = 0;
            for (int i = 0; i < file.length; i++) {
                StringBuffer sb = new StringBuffer("");
                try {
                    inputStream = getContext().openFileInput(file[i].getName());
                    while ((n = inputStream.read(buffer)) != -1) {
                        sb.append(new String(buffer, 0, n));
                    }
                    Log.i(TAG, "sb is :" + sb.toString());
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File not found");
                } catch (IOException e) {

                    Log.e(TAG, "IO Eception");
                }
                value = sb.toString();
                String temp = file[i].getName();
                String[] columnValues = {temp.substring(5, temp.length()), value};
                cursor.addRow(columnValues);

            }
        } else if ((selection.equals("*"))) {
            Log.i(TAG, "selection element is *");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "star");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.i(TAG, "starContainer size is :" + starContainer.size());
            for (int i = 0; i < starContainer.size(); i = i + 2) {
                String key = starContainer.get(i);
                String values = starContainer.get(i + 1);
                String[] columnValues = {key.substring(5, key.length()), values};
                cursor.addRow(columnValues);
            }
        } else {
            Log.i(TAG,"selcetion is :" + selection);
            String hashKey = "";
            String targetNode = "";
            int n2 = 0;
            StringBuilder sb2 = new StringBuilder();
            FileInputStream inputStream2;
            byte[] buffer2 = new byte[1024];
            Boolean found = false;

            for (File fi : file) {
                try {
                    if (fi.getName().contains(selection)) {
                        Log.i(TAG,"ENtered if check : "+fi.getName());
                        inputStream2 = getContext().openFileInput(fi.getName());
                        found = true;
                        while ((n2 = inputStream2.read(buffer2)) != -1) {
                            sb2.append(new String(buffer2, 0, n2));
                           // checkFreq.add(sb2.toString());
                        }
                    }
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "Queryy : File not found");
                } catch (IOException e) {
                    Log.e(TAG, "IO Eception");
                }
            }

           //if(!found) {
            try {
                hashKey = genHash(selection);
                Log.i(TAG, "Query : hashkey is :" + hashKey);
                if ((hashKey.compareTo(genHash(emulatorList.get(0))) > 0 &&
                        hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) > 0)) {
                    Log.i(TAG, "Query : Setting target node as 0, first if check :" + nodeList.get(0));
                    targetNode = nodeList.get(0);
                }
                if ((hashKey.compareTo(genHash(emulatorList.get(0))) < 0 &&
                        hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) < 0)) {
                    Log.i(TAG, "Query : Setting target node as 0, second if check :" + nodeList.get(0));
                    targetNode = nodeList.get(0);
                }
                if ((hashKey.compareTo(genHash(emulatorList.get(0))) <= 0 &&
                        hashKey.compareTo(genHash(emulatorList.get(emulatorList.size() - 1))) > 0)) {
                    Log.i(TAG, "Query : Setting target node as 0, third if check :" + nodeList.get(0));
                    targetNode = nodeList.get(0);
                }
                for (int i = 1; i < emulatorList.size(); i++) {
                    Log.i(TAG, "Query : emulator size is :" + emulatorList.size());
                    if ((hashKey.compareTo(genHash(emulatorList.get(i - 1))) > 0 &&
                            hashKey.compareTo(genHash(emulatorList.get(i))) <= 0)) {
                        Log.i(TAG, "Query : Setting target node as i+1 :" + nodeList.get(i));
                        targetNode = nodeList.get(i);
                    }
                }
                Log.i(TAG,"Target node is : "+targetNode);
                    Log.i(TAG, "Query : Querying target :" + targetNode);
                    String key = null;

                    key = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "target",
                            targetNode, selection).get();

                    //Thread.sleep(135);
                Log.i(TAG, "Query : Updating the cursor when asked for non-existing key :" + key);
                ArrayList<String> checkFreq = new ArrayList<String>();
                ArrayList<String> checkFreq1 = new ArrayList<String>();
                String mainValue = "";
                String ke[] = key.split("\\s+");
                for(int i = 0; i < ke.length; i= i+2) checkFreq1.add(ke[i]);

                for(int i = 1; i < ke.length; i= i+2) checkFreq.add(ke[i]);
                Log.i(TAG,"Length of ke is :"+ ke.length);
                Set<String> unique = new HashSet<String>(checkFreq);
                int max = Integer.MIN_VALUE;
                   int i = 0;
                int j = 25;

                for (String k : unique) {
                    i = Collections.frequency(checkFreq, k);
                    Log.i(TAG,"value is i is :"+i + "and k is :"+k);
                    if(max <= i)  {
                        mainValue = k;
                        max = i;
                        Log.i(TAG,"Main value is :"+mainValue);
                    }
                }
                String[] columnValues = {selection, mainValue};
                cursor.addRow(columnValues);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i(TAG, "Server Task : In server task");
            ServerSocket serverSocket = sockets[0];
            Socket server = null;
            String keyToSend = ""; // In case of query msg recevived
            String msgToSend = "";

            while (true) {
                try {
                    server = serverSocket.accept();
                    InputStream is = server.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    Log.i(TAG, "Server Task : Accepted some connection");
                    String msgReceived = br.readLine();
                    Log.i(TAG, "Server Task : Msg received is :" + msgReceived);
                    String[] msgRecArray = msgReceived.trim().split(" ");
                    if (msgRecArray[0].equals("insert")) {
                        msgToSend = "";
                        Log.i(TAG, "Server Task : Got insert msg with :" + msgRecArray[1]);
                        Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
                        // Building ContentValues Object
                        ContentValues contVal = new ContentValues();
                        //if (msgRecArray[1].equals("dummy")) {
                        contVal.put(KEY_FIELD, "dummy " + msgRecArray[2] + msgRecArray[3]);
                        contVal.put(VALUE_FIELD, msgRecArray[4]);
                        //}
                        getContext().getContentResolver().insert(uri, contVal);
                        msgToSend = "ACK MSG" + " \n";
                        Log.i(TAG, "Server Task : msgToSend :" + msgToSend);
                        DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                        outToServer.writeBytes(msgToSend);
                    } else if (msgRecArray[0].equals("query")) {
                        msgToSend = "";
                        if (msgRecArray[1].equals("target")) {
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            File[] file = f.listFiles();
                            StringBuilder sb2 = new StringBuilder();
                            FileInputStream inputStream2;
                            byte[] buffer2 = new byte[1024];
                            int n2 = 0;
                            for (File fi : file) {
                                if (fi.getName().contains(msgRecArray[2])) {
                                    inputStream2 = getContext().openFileInput(fi.getName());
                                    while ((n2 = inputStream2.read(buffer2)) != -1) {
                                        sb2.append(new String(buffer2, 0, n2));
                                    }
                                }
                            }
                            msgToSend = sb2.toString() + " \n";
                            Log.i(TAG, "Server Task : msgToSend :" + msgToSend);
                            Log.i(TAG, "Server Task : msgToSend :" + msgToSend);
                            DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                            outToServer.writeBytes(msgToSend);
                        } else if (msgRecArray[1].equals("star")) {
                            String msgToSendStar = "";
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            File[] file = f.listFiles();
                            FileInputStream inputStream;
                            int n = 0;
                            byte[] buffer = new byte[1024];
                            StringBuilder sb = new StringBuilder();
                            Log.i(TAG, "Server Task : File size is to fill msgToSendStar:" + file.length);
                            for (File fi : file) {
                                msgToSendStar += fi.getName() + " ";
                                inputStream = getContext().openFileInput(fi.getName());
                                while ((n = inputStream.read(buffer)) != -1) {
                                    sb.append(new String(buffer, 0, n));
                                }
                                msgToSendStar += sb.toString() + " ";
                                sb = new StringBuilder();
                            }
                            msgToSendStar += "\n";
                            Log.i(TAG, "Server Task : msgToSend :" + msgToSendStar);
                            DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                            outToServer.writeBytes(msgToSendStar);

                        } else if (msgRecArray[1].equals("delete") ){
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            Boolean  found = false;
                            File[] file = f.listFiles();
                            for(File fi : file) {
                                if(fi.getName().contains(msgRecArray[2])) {
                                    Log.i(TAG, "Delete : Deleting :" + msgRecArray[2]);
                                    found = true;
                                    fi.delete();
                                }
                            }
                            String msgToSendB = "ACK MSG " + "\n";
                            DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                            Log.i(TAG, "Server Task : Wrinting msgToSend :" + msgToSendB);
                            outToServer.writeBytes(msgToSendB);
                        }
                    } else { // asking for in the received msg
                        Log.i(TAG, "Server Task : Size of the msgRecArray is, got asking for: " + msgRecArray.length);
                        //for (String m : msgRecArray) {
                        //msgRecArray[0] asking, msgRecArray[1] = for, msgRecArray[2],[3],[4]
                        String path = getContext().getFilesDir().getPath();
                        File f = new File(path);
                        f.mkdirs();
                        File[] file = f.listFiles();

                        FileInputStream inputStream2;
                        byte[] buffer2 = new byte[1024];
                        int n2 = 0;
                        Log.i(TAG, "Server task : file length is :" + file.length);
                        msgToSend = "";
                       // if(msgRecArray.length == 3) {
                          /*  for (File fi : file) {
                                String tempName = fi.getName();
                                if (tempName.contains(msgRecArray[2])) {
                                    inputStream2 = getContext().openFileInput(tempName);
                                    StringBuilder sb2 = new StringBuilder();
                                    while ((n2 = inputStream2.read(buffer2)) != -1) {
                                        sb2.append(new String(buffer2, 0, n2));
                                    }
                                    Log.i(TAG, "Server Task : Adding to msgToSend");
                                    msgToSend += tempName + " " + sb2.toString() + " ";

                                }
                            }*/
                        //} else {
                           /* for (File fi : file) {
                                String tempName = fi.getName();
                                if (tempName.contains(msgRecArray[2]) ||
                                        tempName.contains(msgRecArray[3])) {
                                    inputStream2 = getContext().openFileInput(tempName);
                                    StringBuilder sb2 = new StringBuilder();
                                    while ((n2 = inputStream2.read(buffer2)) != -1) {
                                        sb2.append(new String(buffer2, 0, n2));
                                    }
                                    Log.i(TAG, "Server Task : Adding to msgToSend");
                                    msgToSend += tempName + " " + sb2.toString() + " ";

                                }
                            }*/
                        //}
                        for(int i =0; i< globallist.size(); i= i+2) {
                            msgToSend += globallist.get(i) + " " + globallist.get(i+1) + " ";
                        }
                        msgToSend += "\n";
                        globallist = new ArrayList<String>();
                        DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                        Log.i(TAG, "Server Task : Writing msgToSend :" + msgToSend);
                        outToServer.writeBytes(msgToSend);
                    }
                    server.close();
                } catch (IOException e) {
                    Log.e(TAG, "publish progress failed");
                }
                Log.i(TAG, "Leaving server task");
            }
        }

        protected void onProgressUpdate(String... strings) {
            Log.i(TAG, "Server Task : In progress update, string.length is " + strings.length);
            ;
            if (strings.length == 3) {
                // insert key value : This is to be sent when the node needs to just insert the key and not forward
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1], strings[2]);
            }
            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            try {
                Log.i(TAG, "Client Task : In client task : msgs[0] is " + msgs[0] + " and msgs size is :" + msgs.length);
                for (String s : msgs)
                    Log.i(TAG, " " + s);
                String msgToSend = "";
                String msgToSend2 = "";
                if (msgs[0].equals("send")) {
                    if (msgs[1].equals("successors")) {
                        Log.i(TAG, "Client Task : Insert called me to send msg to successors :" + msgs[2] +
                                ", " + msgs[3]);
                        Log.i(TAG, "Client Task : key is :" + msgs[4] + " value is :" + msgs[5]);

                        String[] remotePorts = {msgs[2], msgs[3]};
                        for (int i = 0; i < 2; i++) {
                             try {
                            if (!remotePorts[i].equals("")) {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePorts[i]));
                                Log.i(TAG, "Client Task : socket selected for i :" + remotePorts[i]);
                               Thread.sleep(100);
                                msgToSend = "insert " + "dummy " + myPort + " " + msgs[4] + " " + msgs[5] + " \n";
                                Log.i(TAG, "Client Task : msgToSend for i :" + i + " is :" + msgToSend);
                                DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                                outToServer.writeBytes(msgToSend);
                                outToServer.flush();

                                InputStream is = socket.getInputStream();
                                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                String msgReceived = br.readLine();
                                if (msgReceived.equals("ACK MSG")) {
                                    Log.i(TAG, "Client Task : Closing socket");
                                    socket.close();
                                } else {
                                }
                            }
                        } catch (NullPointerException e) {
                                 Log.e(TAG, "ClientTask socket Null pointer exception");

                                     Log.i(TAG,"Adding to global list ele check:"+msgs[3]+msgs[4]);

                                     //msgToSend = "insert " + "dummy " + msgs[3] + " " + msgs[4] + " " + msgs[5] + " \n";
                                     globallist.add(myPort+msgs[4]);
                                     globallist.add(msgs[5]);

                             } catch (InterruptedException e) {
                                 e.printStackTrace();
                             }
                        }
                    } else if (msgs[1].equals("target")) {
                        Log.i(TAG, "Client Task : Insert called me to send msg to target :" + msgs[2]);
                        Log.i(TAG, "Client Task : key is :" + msgs[3] + " value is :" + msgs[4]);
                        int inde = nodeList.indexOf(msgs[2]);
                        ArrayList<String> remoPo = new ArrayList<String>();
                        remoPo.add(nodeList.get(inde));
                        if(!msgs[2].equals("special")) {
                            if (inde >= 0 && inde <= 2) {
                                remoPo.add(nodeList.get(inde + 1));
                                remoPo.add(nodeList.get(inde + 2));
                            } else if (inde == 3) {
                                remoPo.add(nodeList.get(inde + 1));
                                remoPo.add(nodeList.get(0));
                            } else {//if inde is 4
                                remoPo.add(nodeList.get(0));
                                remoPo.add(nodeList.get(1));
                            }
                        }

                        for (String s : remoPo) {
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(s));
                                Log.i(TAG, "Client Task : socket selected for i :" + s);
                                if(!msgs[2].equals("special"))
                                msgToSend = "insert " + "dummy " + msgs[2] + " " + msgs[3] + " " + msgs[4] + " \n";
                                else
                                    msgToSend = "insert " + "dummy " + msgs[3] + " " + msgs[4] + " " + msgs[5] + " \n";

                                Log.i(TAG, "Client Task : msgToSend is :" + msgToSend);
                                DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                                outToServer.writeBytes(msgToSend);
                                outToServer.flush();

                                InputStream is = socket.getInputStream();
                                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                String msgReceived = br.readLine();
                                if (msgReceived.equals("ACK MSG")) {
                                    Log.i(TAG, "Client Task : Closing socket");
                                    socket.close();
                                } else {
                                    Log.i(TAG, "Client Task : Did not receive ACK");
                                }
                            } catch (NullPointerException e) {
                                Log.e(TAG, "ClientTask socket Null pointer exception");
                                if(!msgs[2].equals("special")) {
                                    Log.i(TAG,"Adding to global list if check:" +msgs[2]+msgs[3]);
                                    //msgToSend = "insert " + "dummy " + msgs[2] + " " + msgs[3] + " " + msgs[4] + " \n";
                                    globallist.add(msgs[2]+msgs[3]);
                                    globallist.add(msgs[4]);
                                }
                                else {
                                    Log.i(TAG,"Adding to global list ele check:"+msgs[3]+msgs[4]);

                                    //msgToSend = "insert " + "dummy " + msgs[3] + " " + msgs[4] + " " + msgs[5] + " \n";
                                    globallist.add(msgs[3]+msgs[4]);
                                    globallist.add(msgs[5]);
                                }

                            }
                        }

                    }

                } else if (msgs[0].equals("query")) {
                    if (msgs[1].equals("target")) {

                        // Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);
                            Log.i(TAG, "i AM NOT TARGET SO SENDQUERY TO ALL  OTHERs");
                            int inde = nodeList.indexOf(msgs[2]);
                            ArrayList<String> remoPo = new ArrayList<String>();
                            //if(!msgs[2].equals(myPort))
                            remoPo.add(nodeList.get(inde));
                            if (inde >= 0 && inde <= 2) {
                                remoPo.add(nodeList.get(inde + 1));
                                remoPo.add(nodeList.get(inde + 2));
                            } else if (inde == 3) {
                                remoPo.add(nodeList.get(inde + 1));
                                remoPo.add(nodeList.get(0));
                            } else {//if inde is 4
                                remoPo.add(nodeList.get(0));
                                remoPo.add(nodeList.get(1));
                            }
                            Log.i(TAG, "REMOPO list size is :" + remoPo.size());
                            String msgRec = "";
                             String returningKey = "";
                        String save = "";
                            for (String s : remoPo) {
                                try {
                                    Log.i(TAG, "Sending for socket :" + s);
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(s));
                                    DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                                    msgToSend = msgs[0] + " " + msgs[1] + " " + msgs[3] + " \n";
                                    outToServer.writeBytes(msgToSend);
                                    Log.i(TAG, "Client Task : msgToSend is - " + msgToSend + "target is  :" + msgs[2]);
                                    InputStream is = socket.getInputStream();
                                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                    //globalKey = br.readLine();
                                    msgRec = br.readLine();

                                    if (msgRec != null && !msgRec.equals(" ") && !msgRec.equals("") && !msgRec.isEmpty()) {
                                        returningKey += s + " " + msgRec + " ";
                                        save = msgRec;
                                        Log.i(TAG,"Adding to returning key");
                                    } else {
                                       // Log.i(TAG,"Adding to returning  in else check");
                                        //returningKey += s + " " + save +" ";
                                    }
                                    Log.i(TAG, "Client Task : Closing socket :" + msgRec);
                                    Log.i(TAG,"msgRec.isEmpty() : "+msgRec.isEmpty());
                                    socket.close();
                                    /*if (msgRec != null && !msgRec.equals(" ") && !msgRec.equals("") && !msgRec.isEmpty()) {
                                        Log.i(TAG, "bREAKING");
                                        break;
                                    }*/
                                } catch (NullPointerException e) {
                                    Log.e(TAG, "ClientTask socket Null pointer exception");
                                   // returningKey += s + " " + save +" ";
                                }
                            }
                        Log.i(TAG," returning key is:"+returningKey);
                            return returningKey;


                    } else if (msgs[1].equals("star")) {
                        String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                        for (int i = 0; i < 5; i++) {
                            msgToSend2 = "";
                            try {
                                Log.i(TAG, "Client Task : node list size is :" + nodeList.size());
                                //if (myPort.equals(remotePorts[0])) {
                                //   if (!nodeList.contains(remotePorts[i])) continue;
                                //}
                                if (myPort.equals(remotePorts[i])) {
                                    Log.i(TAG, "Client Task : Continuing because it is me , i = " + i);
                                    continue;
                                }
                                Log.i(TAG, "Client Task : Sending msg for  i = " + i);
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePorts[i]));
                                Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);
                                DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                                msgToSend2 = "query star " + " \n";
                                Log.i(TAG, "Client Task : msgToSend2 is - " + msgToSend2);
                                outToServer.writeBytes(msgToSend2);
                                InputStream is = socket.getInputStream();
                                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                Log.i(TAG, "Client Task : Next to Read object");
                                String msgReceived = br.readLine();
                                Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                                String[] msgArr = msgReceived.trim().split(" ");
                                for (int j = 0; j < msgArr.length; j++) {
                                    Log.i(TAG, "Adding to star container");
                                    starContainer.add(msgArr[j]);
                                }
                                Log.i(TAG, "Client Task : Closing socket");
                                socket.close();
                            } catch (NullPointerException e) {
                                Log.e(TAG, "ClientTask socket Null pointer exception");

                            }
                        }
                    } else if (msgs[1].equals("delete")) {
                        Log.i(TAG, "Client Task : Got delete keyword");
                        String msgToSendQuery = "";
                        String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                        //Log.i(TAG, "Client Task : Entering the else check beacuase of ask from onCreate");
                        //ArrayList<String> diedContainer = new ArrayList<String>();
                        for (int i = 0; i < 5; i++) {
                            try {
                                msgToSendQuery = "query " + "delete " + msgs[2] + "\n";
                                Log.i(TAG, "msgToSendQuery :"+msgToSendQuery);
                                if (myPort.equals(remotePorts[i])) {
                                    Log.i(TAG, "Client Task : Continuing because it is me , i = " + i);
                                    continue;
                                }
                                Log.i(TAG, "Client Task : Sending msg for  i = " + i);
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePorts[i]));
                                Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);
                                DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());

                                Log.i(TAG, "Client Task : msgToSendQuery is - " + msgToSendQuery);
                                outToServer.writeBytes(msgToSendQuery);
                                InputStream is = socket.getInputStream();
                                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                Log.i(TAG, "Client Task : Next to Read object");
                                String msgReceived = br.readLine();
                                Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                                if (msgReceived.equals("ACK MSG")) {
                                    Log.i(TAG, "Client Task : Closing socket");
                                    socket.close();
                                } else {
                                    Log.i(TAG, "Client Task : Did not receive ACK");
                                }
                            } catch (NullPointerException e) {
                                Log.e(TAG, "ClientTask socket Null pointer exception");
                            }
                        }
                    }
                } else { // coming from oncreate with ask in args
                    String[] remotePorts = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
                    Log.i(TAG, "Client Task : Entering the else check beacuase of ask from onCreate");
                    ArrayList<String> diedContainer = new ArrayList<String>();
                    for (int i = 0; i < 5; i++) {
                        msgToSend2 = "";
                        Log.i(TAG, "Client Task : node list size is :" + nodeList.size());
                        if (myPort.equals(remotePorts[i])) {
                            Log.i(TAG, "Client Task : Continuing because it is me , i = " + i);
                            continue;
                        }
                        Log.i(TAG, "Client Task : Sending msg for  i = " + i);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePorts[i]));
                        Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);
                        DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                       /* String predecessorOne = "";
                        String predecessorTwo = "";
                        int ind = nodeList.indexOf(myPort);
                        Log.i(TAG, "Client task : my ind is :" + ind);
                        if (ind >= 2 && ind <= 4) {
                            predecessorOne = nodeList.get(ind - 1);
                            predecessorTwo = nodeList.get(ind - 2);
                        } else if (ind == 1) {
                            predecessorOne = nodeList.get(0);
                            predecessorTwo = nodeList.get(4);
                        } else {
                            predecessorOne = nodeList.get(4);
                            predecessorTwo = nodeList.get(3);
                        }*/
                        //if(i == 0) {
                          //  msgToSend2 = "asking for " + myPort + " \n";
                        //}
                        //else {
                           // msgToSend2 = "asking for " +  predecessorOne + " " + predecessorTwo + " \n";
                        msgToSend2 = "asking for " +   " \n";

                        //}
                        Log.i(TAG, "Client Task : msgToSend2 is - " + msgToSend2);
                        outToServer.writeBytes(msgToSend2);
                        // toServer.writeObject(msgToSend2);

                        //ObjectInputStream is = new ObjectInputStream(socket.getInputStream());

                        InputStream is = socket.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        //Thread.sleep(1000);
                        //Log.i(TAG, "Client Task : Next to Read object");
                        String msgReceived = br.readLine();
                        Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                        String[] msgRecievedArray = msgReceived.trim().split(" ");
                        Log.i(TAG, "Splitted the received msg, length = :" + msgRecievedArray.length);
                        //String[] msgArr = msgReceived.trim().split(" ");
                        if(msgRecievedArray.length >= 2) {
                            for (int j = 0; j < msgRecievedArray.length; j++) {
                                if (msgRecievedArray[j] != " " || msgRecievedArray[j] != "") {
                                    Log.i(TAG, "Adding to died container");
                                    diedContainer.add(msgRecievedArray[j]);
                                }
                            }
                        }
                        socket.close();
                    }
                    Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
                    ContentValues contVal = new ContentValues();
                    Log.i(TAG, "Oncreate : diedContainer size is :" + diedContainer.size());
                    for (int i = 0; i < diedContainer.size() - 1; i = i + 2) {
                        contVal.put(KEY_FIELD, "asked for " + diedContainer.get(i));
                        contVal.put(VALUE_FIELD, diedContainer.get(i + 1));
                        getContext().getContentResolver().insert(uri, contVal);
                    }

                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (NullPointerException e) {
                Log.e(TAG, "ClientTask socket Null pointer exception");

            } /*catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            Log.i(TAG, "Client Task : Leaving client task");
            return null;
        }
    }

    private class Dummy implements Comparator<String> {
        public int compare(String s1, String s2) {
            try {
                switch (Integer.parseInt(s1)) {
                    case 11108:
                        s1 = "5554";
                        break;
                    case 11112:
                        s1 = "5556";
                        break;
                    case 11116:
                        s1 = "5558";
                        break;
                    case 11120:
                        s1 = "5560";
                        break;
                    case 11124:
                        s1 = "5562";
                        break;
                }
                switch (Integer.parseInt(s2)) {
                    case 11108:
                        s2 = "5554";
                        break;
                    case 11112:
                        s2 = "5556";
                        break;
                    case 11116:
                        s2 = "5558";
                        break;
                    case 11120:
                        s2 = "5560";
                        break;
                    case 11124:
                        s2 = "5562";
                        break;
                }
                s1 = genHash(s1);
                s2 = genHash(s2);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "No such algorithm error");
            }
            return s1.compareTo(s2);
        }
    }
}
