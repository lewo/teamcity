import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.MulticastSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.BindException;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map;
import java.util.HashMap;

class MCast {
    public final static int GROUP_PORT = 8437;
    private static class Message implements Serializable {
        public long num;
        public String msg;
        public String toString() {
            return String.format("Message %d: %s", num, msg);
        }
    }

    private static class HeartbeatMessage extends Message {
        public String toString() {
            return String.format("Heartbeat num %d", num);
        }
    }

    private static class HeartbeatAcknowledge extends Message {
        public String toString() {
            return String.format("I'm alive %d", num);
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException {
        DatagramSocket psock = getDatagramSocket();
        MulticastSocket sock = new MulticastSocket(GROUP_PORT);
        InetAddress    addr  = InetAddress.getByName("224.0.0.4");
        sock.joinGroup(addr);
        if (args.length > 0 && args[0].equals("leader")) {
            playLeader(sock, psock, addr);
        } else {
            playFollower(sock, psock, addr);
        }
        sock.leaveGroup(addr);
    }

    private static void playFollower(MulticastSocket mSock, DatagramSocket dSock, InetAddress groupAddr) throws IOException, ClassNotFoundException{
        boolean hasLeader = false;
        System.out.println("PLaying follower");
        long lastSentMsg = 0;
        while (true) {
            DatagramPacket packet = new DatagramPacket(new byte[8192], 8192);
            mSock.receive(packet);
            Message message       = decodeMessage(packet);
            if (message instanceof HeartbeatMessage) {
                System.out.printf("Receive Heartbeat from %s : %s\n", packet.getSocketAddress(), message);
                hasLeader = true;
                Message reply = new HeartbeatAcknowledge();
                reply.num = lastSentMsg++;
                reply.msg = "OH HAI";
                DatagramPacket replyPacket = encodeMessage(reply, packet.getAddress(), packet.getPort());
                dSock.send(replyPacket);
            }
        }
    }

    private static class Heartbeater extends TimerTask {
        private DatagramSocket sock;
        private InetAddress addr;
        private long lastSentMessage = 0;
        public Heartbeater(DatagramSocket dSock, InetAddress groupAddr) {
            this.sock = dSock;
            this.addr = groupAddr;
        }

        public void run() {
            System.out.println("Sending out heartbeat");
            HeartbeatMessage msg = new HeartbeatMessage();
            msg.num = lastSentMessage++;
            msg.msg = "HEY";
            try {
                DatagramPacket pkt = encodeMessage(msg, addr, GROUP_PORT);
                sock.send(pkt);
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }
    }

    private static void playLeader(MulticastSocket mSock, DatagramSocket dSock, InetAddress groupAddr) throws IOException, ClassCastException, ClassNotFoundException {
        System.out.println("Playing leader");
        Timer timer = new Timer();
        Heartbeater beater = new Heartbeater(dSock, groupAddr);
        timer.schedule(beater, 1000, 1000);
        while (true) {
            DatagramPacket pkt = new DatagramPacket(new byte[8192], 8192);
            dSock.receive(pkt);
            Message msg = decodeMessage(pkt);
            System.out.printf("%s: %s\n", pkt.getSocketAddress(), msg);
        }
    }

    private static DatagramPacket encodeMessage(Message message, InetAddress addr, int port) throws IOException {
       ByteArrayOutputStream stream = new ByteArrayOutputStream(8192);
       ObjectOutputStream objstream = new ObjectOutputStream(stream);
       objstream.writeObject(message);
       objstream.close();
       return new DatagramPacket(stream.toByteArray(), stream.size(), addr, port);
    }

    private static Message decodeMessage(DatagramPacket packet) throws IOException, ClassCastException, ClassNotFoundException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet.getData(), packet.getOffset(), packet.getLength());
        ObjectInputStream objstream = new ObjectInputStream(stream);
        Message             message = (Message)objstream.readObject();
        return message;
    }


    private static DatagramSocket getDatagramSocket() throws SocketException {
        for (int i = 0; i < 100; i++) {
            try {
                return new DatagramSocket(8000 + i);
            } catch (BindException e) {
                continue;
            }
        }
        System.err.println("Exhausted options.");
        System.exit(-1);
        return null;
    }

}