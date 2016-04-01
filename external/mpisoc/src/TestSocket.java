/* TesSocket.java */

import java.io.*;
import java.net.*;

public class TestSocket {
    public static void main(String[] args) throws Exception {
	if (args.length != 2) {
	    System.err.println("Usage: mpirun -np 2 java TestMPISoc "
			       + "host-name-of-rank0 port-number");
	    System.exit(1);
	}
	String host = args[0];
	int port = Integer.parseInt(args[1]);
	mpi.MPIMux.init(args);
	mpi.MPIMux mux = new mpi.MPIMux(mpi.MPI.COMM_WORLD);
	/*mux.install_socket_factories();*/
	mpi.MPISoc.Socket.mux = mux;
	mpi.MPISoc.ServerSocket.mux = mux;
	if (mux.rank == 0) {
	    System.out.println("client");
	    Thread.sleep(3);
	    EchoClient.main(host, port);
	} else if (mux.rank == 1) {
	    System.out.println("server");
	    EchoServer.main(host, port);
	} else {
	    /*nothing*/
	}
	mux.finish();
	mpi.MPIMux.fin();
    }

    public static class EchoClient {
	public static void main(String host, int port) throws IOException {
	    try {
		Socket so = new mpi.MPISoc.Socket(host, port);
		PrintWriter os = new PrintWriter(so.getOutputStream(), true);
		BufferedReader is = new BufferedReader
		    (new InputStreamReader(so.getInputStream()));
		//BufferedReader stdin = new BufferedReader
		//(new InputStreamReader(System.in)))
		BufferedReader stdin = new BufferedReader
		    (new StringReader("aho aho aho\n"
				      + "boke boke boke\n"
				      + "aho aho aho\n"
				      + "boke boke boke\n"));
		String line;
		while ((line = stdin.readLine()) != null) {
		    System.out.println("line: " + line);
		    os.println(line);
		    System.out.println("echo: " + is.readLine());
		}
		os.close();
		//System.out.println("client done");
	    } catch (UnknownHostException e) {
		throw new Error(e);
	    }
	}
    }

    public static class EchoServer {
	public static void main(String host, int port) throws IOException {
	    ServerSocket ss = new mpi.MPISoc.ServerSocket(port);
	    System.err.println("accept()...");
	    Socket so = ss.accept();
	    System.err.println("accept() done");
	    PrintWriter os = new PrintWriter(so.getOutputStream(), true);
	    BufferedReader is = new BufferedReader
		(new InputStreamReader(so.getInputStream()));
	    String line;
	    while ((line = is.readLine()) != null) {
		os.println(line);
	    }
	    //System.out.println("server done");
	}
    }
}
