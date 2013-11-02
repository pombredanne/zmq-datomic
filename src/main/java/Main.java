import datomic.ListenableFuture;
import datomic.Peer;
import org.jeromq.ZMQ;
import datomic.Connection;

import static datomic.Peer.connect;
import static datomic.Peer.createDatabase;
import static datomic.Peer.q;


import java.io.ByteArrayInputStream;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import sun.io.ByteToCharUTF8;
import java.util.HashMap;

import java.util.*;

import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.template.Template;
import org.msgpack.unpacker.Unpacker;
import static org.msgpack.template.Templates.tList;
import static org.msgpack.template.Templates.tMap;
import static org.msgpack.template.Templates.TString;

public class Main {

    public static void main(String[] args) throws Exception {

        // existing datomic db connections
        Map<String, Connection> conns = new HashMap<String, Connection>();

        //
        MessagePack msgpack = new MessagePack();

        String respMsg;
        int    respCode;

        //  Socket to talk to clients
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REP);
        socket.bind ("tcp://*:5555");
        System.out.println("Server Starting");
        while (!Thread.currentThread ().isInterrupted ()) {

            // the request
            byte[] rawRequest = socket.recv(0);

            ByteBuffer buf = ByteBuffer.wrap(rawRequest);
            buf.order(ByteOrder.nativeOrder());
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes, 0, bytes.length);
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);

            //
            // deserialize
            //
            Unpacker unpacker = msgpack.createUnpacker(in);

            String[] dbCmd    = unpacker.read(String[].class);
            int cmd           = new Integer(dbCmd[0]);
            String db         = new String(dbCmd[1]);
            System.out.println (cmd);
            System.out.println (db);
            //System.out.println (dbCmd.length);

            // connection for requested db exists?
            if(conns.containsKey(db)) {
                System.out.println("use existing connection");
            } else if(cmd > 1) {
                System.out.println("setup new connection");
                conns.put(db, Peer.connect(db));
            }

            // handle the command
            respCode = 200;
            respMsg  = "";

            if(cmd == 1) {
                //
                // create database
                //
                createDatabase(db);
                respMsg = "Created DB";

            } else if (cmd == 2) {
                //
                // transact
                //
                System.out.println("doing transaction");
                Connection conn = conns.get(db);

                List<String> txList = new LinkedList<String>();

                String tx_data = new String(dbCmd[2]);
                System.out.println(tx_data);

                txList.add(tx_data);

                Map txResult = conn.transact(txList).get();

                System.out.println(txResult);
                respMsg = "Ran TX";


            /**
            } else if (cmd == 3) {
                //
                // query
                //


            } else if (cmd == 4) {
                //
                // entity
                //

            } else if (cmd == 5) {
                //
                // tempid
                //
            */

            } else {
                respMsg = "Invalid Command";
            }

            socket.send(respMsg.getBytes(), 0);
        }
        socket.close();
        context.term();
    }

    @Message //annotation
    public static class Rq
    {
        public Integer cmd;
        public String db;
    }
}
