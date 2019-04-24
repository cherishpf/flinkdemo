package table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * @author yangpf
 * @date 2019/4/24
 * 在本地启动socket服务，并按指定格式生成数据
 */
public class SocketServer implements Runnable {
    private Socket socketClient = null;

    public static void main(String[] args) throws IOException {

        ServerSocket listener = new ServerSocket(12341);
        try {
            while (true) {
                Socket socket = listener.accept();
                SocketServer hand = new SocketServer(socket);

                (new Thread(hand)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                listener.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //Constructor for the socket class
    public SocketServer(Socket socket) {
        socketClient = socket;
    }

    /**
     * We send a dummy event generated random with the following format
     * Long,Integer,String,String,Long,Double
     * Each field can be configured with custom values from keyboard by specifying -f#
     * There is no verification for the correctness of the input
     * eg: -f0 123 -f1 23 -f2 user2 -f3 note2 -f4 9874 -f5 832.456
     */


    @Override
    public void run() {

        Random rnd = new Random(11);

        String message;
        long timestamp = 1460730000050L;
        int id = 1;
        String user = "user1";
        String note = "default";
        long specificNumber = 123456789L;
        double amount = 123.1;

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                System.out.print("Continue?");
                String line = br.readLine();

                if (line.equals("exit")) {
                    break;
                }
                if (line.contains("-f0")) {
                    timestamp = Long.parseLong(line.split(" ")[1]);
                } else {
                    timestamp += 7200000;
                }

                if (line.contains("-f1")) {
                    id = Integer.parseInt(line.split(" ")[3]);
                }
                if (line.contains("-f2")) {
                    user = line.split(" ")[5];
                }
                if (line.contains("-f3")) {
                    note = line.split(" ")[7];
                }
                if (line.contains("-f4")) {
                    specificNumber = Long.parseLong(line.split(" ")[9]);
                }
                if (line.contains("-f5")) {
                    amount = Double.parseDouble(line.split(" ")[11]);
                }

                message = timestamp + "," + id + "," + user + "," + note + "," + specificNumber + "," + amount;
                PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);
                out.println(message);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socketClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}
