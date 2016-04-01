/* MPISoc.java */
/* Copyright (C) 2016 RIKEN AICS */

/* Socket for MPI (mpiJava).  The MPISoc part implements sockets.  The
   MPIMux part do actual communication with MPI. */

/* MEMO: (1) Users should NOT install a factory for SocketImpl,
   because this still use ordinary (plain/socks) sockets for accepting
   connections from TCP sockets (see PlainSocketAccept).  Note the
   ServerSocket implementation explicitly hold SocketImpl, because
   there is no way to replace it. */

package mpi;

import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.Vector;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Hashtable;

public class MPISoc {
    final static int ACCEPT_POLL_INTERVAL_MS = 100;
    final static boolean ACCEPT_TCP_SOCKETS = true;

    static java.net.SocketAddress sockaddr(String host, int port)
	throws java.net.UnknownHostException, java.io.IOException {
	if (host != null) {
	    return new java.net.InetSocketAddress(host, port);
	} else {
	    java.net.InetAddress ia = java.net.InetAddress.getByName(null);
	    return new java.net.InetSocketAddress(ia, port);
	}
    }

    static java.net.SocketAddress sockaddr(java.net.InetAddress ia, int port)
	throws java.io.IOException {
	return new java.net.InetSocketAddress(ia, port);
    }

    static class SocImpl extends java.net.SocketImpl {
	/* SocketImpl fields: */

	//protected InetAddress address;
	//protected FileDescriptor fd;
	//protected int localport;
	//protected int port;

	/* SocketImpl abstract methods: */

	//protected void create(boolean stream);
	//protected void bind(java.net.InetAddress host, int port);
	//protected void listen(int backlog);
	//protected void accept(java.net.SocketImpl s);
	//protected void connect(java.net.InetAddress address, int port);
	//protected void connect(java.net.SocketAddress address, int timeout);
	//protected void connect(String host, int port);
	//protected void close();
	//protected InputStream getInputStream();
	//protected OutputStream getOutputStream();
	//protected int available();
	//protected void sendUrgentData(int data);

	/* SocketOptions interface methods: */

	//Object getOption(int optID);
	//void setOption(int optID, Object value);

	/* SocketImpl non-abstract methods: */

	//protected FileDescriptor getFileDescriptor();
	//protected InetAddress getInetAddress();
	//protected int getLocalPort();
	//protected int getPort();
	//protected void setPerformancePreferences(int co, int lt, int bw);
	//protected void shutdownInput();
	//protected void shutdownOutput();
	//protected boolean supportsUrgentData();
	//String toString();

	/* Either ep or so is non-null on a (non-server) socket. */

	final MPIMux mux;
	boolean stream;
	MPIMux.Endp ep = null;
	java.net.Socket so = null;
	PlainSocketAccept backupss = null;

	public SocImpl(MPIMux mux) {
	    this.mux = mux;
	}

	public SocImpl(MPIMux.Endp ep) {
	    this.mux = ep.mux;
	    this.ep = ep;
	}

	protected void create(boolean stream)
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.create()[" + mux.rank + "]");
	    assert (stream == true);
	    this.stream = stream;
	}

	protected void bind(java.net.InetAddress host, int port)
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.bind()[" + mux.rank + "]");
	    mux.bind_mpi(port);
	    this.address = host;
	    this.port = port;
	}

	protected void listen(int backlog)
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.listen()[" + mux.rank + "]");
	}

	void mpisoc_make_backup_socket(int backlog)
	    throws java.io.IOException {
	    java.net.ServerSocket
		ss = new java.net.ServerSocket(port, backlog, address);
	    backupss = new PlainSocketAccept(ss);
	    new Thread(backupss).start();
	    mux.backupss.add(backupss);
	}

	/* Accepts a new (client) socket.  It is not used. */

	protected void accept(java.net.SocketImpl impl0)
	    throws java.io.IOException {
	    throw new Error("impl.accept() unused");
	}

	java.net.Socket mpisoc_accept()
	    throws java.io.IOException {
	    System.err.println("impl.accept()[" + mux.rank + "]");
	    java.net.Socket so = null;
	    MPIMux.Endp ep = null;
	    backupss.start();
	    try {
		for (;;) {
		    so = backupss.accept();
		    if (so != null) {
			break;
		    }
		    ep = mux.accept_mpi(port);
		    if (ep != null) {
			break;
		    }
		    try {
			Thread.sleep(ACCEPT_POLL_INTERVAL_MS);
		    } catch (InterruptedException ex) {}
		}
	    } finally {
		backupss.stop();
	    }
	    assert (ep == null || so == null);
	    if (so != null) {
		System.err.println("impl.accept(so)");
		return so;
	    } else if (ep != null) {
		System.err.println("impl.accept(ep)");
		SocImpl impl = new SocImpl(mux);
		impl.ep = ep;
		Socket so1 = new Socket(impl);
		return so1;
	    } else {
		assert false;
		return null;
	    }
	}

	/* Tries to connect via MPI first, then tries to connect via a
	   plain socket when it timeouts. */

	protected void connect(java.net.InetAddress ia, int port)
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.connect()[" + mux.rank + "]");
	    assert (this.ep == null);
	    MPIMux.Endp ep = mux.connect_mpi(ia, port);
	    if (ep != null) {
		this.ep = ep;
	    } else {
		this.so = new java.net.Socket(ia, port);
	    }
	}

	protected void connect(java.net.SocketAddress a, int timeout)
	    throws java.io.IOException {
	    /*abstract*/
	    if (false) System.err.println("(impl.connect())");
	    assert (a instanceof java.net.InetSocketAddress);
	    java.net.InetSocketAddress sa = (java.net.InetSocketAddress)a;
	    java.net.InetAddress ia = sa.getAddress();
	    int port = sa.getPort();
	    this.connect(ia, port);
	}

	protected void connect(String host, int port)
	    throws java.io.IOException {
	    /*abstract*/
	    if (false) System.err.println("(impl.connect())");
	    java.net.InetAddress ia = java.net.InetAddress.getByName(host);
	    this.connect(ia, port);
	}

	protected void close()
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.close()[" + mux.rank + "]");
	    assert ((ep != null) || (so != null) || (backupss != null));
	    if (ep != null) {
		ep.close_is();
		ep.close_os();
	    } else if (so != null) {
		so.close();
	    } else if (backupss != null) {
		backupss.close();
	    }
	}

	protected java.io.InputStream getInputStream()
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.getInputStream()[" + mux.rank + "]");
	    assert ((ep != null) || (so != null));
	    if (ep != null) {
		return ep.is();
	    } else if (so != null) {
		return so.getInputStream();
	    } else {
		throw new Error();
	    }
	}

	protected java.io.OutputStream getOutputStream()
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.getOutputStream()[" + mux.rank + "]");
	    assert ((ep != null) || (so != null));
	    if (ep != null) {
		return ep.os();
	    } else if (so != null) {
		return so.getOutputStream();
	    } else {
		throw new Error();
	    }
	}

	protected int available()
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.available()[" + mux.rank + "]");
	    assert ((ep != null) || (so != null));
	    if (ep != null) {
		return ep.available_mpi();
	    } else if (so != null) {
		java.io.InputStream is = so.getInputStream();
		return is.available();
	    } else {
		throw new Error();
	    }
	}

	protected void sendUrgentData(int data)
	    throws java.io.IOException {
	    /*abstract*/
	    System.err.println("impl.sendUrgentData()");
	    assert ((ep != null) || (so != null));
	    if (ep != null) {
		throw new Error("sendUrgentData");
	    } else if (so != null) {
		so.sendUrgentData(data);
	    }
	}

	public Object getOption(int optID)
	    throws java.net.SocketException {
	    /*abstract*/
	    System.err.println("impl.getOption()");
	    assert ((ep != null) || (so != null) || (backupss != null));
	    if (ep != null) {
		return null;
	    } else if (so != null) {
		return null;
	    } else if (backupss != null) {
		return null;
	    } else {
		throw new Error();
	    }
	}

	public void setOption(int optID, Object value)
	    throws java.net.SocketException {
	    /*abstract*/
	    System.err.println("impl.setOption()");
	    assert ((ep != null) || (so != null) || (backupss != null));
	    if (ep != null) {
	    } else if (so != null) {
	    } else if (backupss != null) {
		/*backupso.setOption(optID, value);*/
	    } else {
		throw new Error();
	    }
	}
    }

    /* Socket for MPI with SocketIml. */

    public static class Socket extends java.net.Socket {
	public static MPIMux mux;
	final SocImpl impl;

	public Socket(java.net.SocketImpl impl)
	    throws java.io.IOException {
	    super(impl);
	    this.impl = (SocImpl)impl;
	}

	public Socket()
	    throws java.io.IOException {
	    this(new SocImpl(mux));
	}

	public Socket(java.net.Proxy proxy) {
	    throw new Error("No proxy");
	}

	public Socket(String host, int port)
	    throws java.net.UnknownHostException, java.io.IOException {
	    this(sockaddr(host, port), null, true);
	}

	public Socket(java.net.InetAddress ia, int port)
	    throws java.io.IOException {
	    this(sockaddr(ia, port), null, true);
	}

	public Socket(String host, int port,
		      java.net.InetAddress la, int lport)
	    throws java.io.IOException {
	    this(sockaddr(host, port), sockaddr(la, lport), true);
	}

	public Socket(java.net.InetAddress ia, int port,
		      java.net.InetAddress la, int lport)
	    throws java.io.IOException {
	    this(sockaddr(ia, port), sockaddr(la, lport), true);
	}

	Socket(java.net.SocketAddress sa, java.net.SocketAddress la,
	       boolean stream)
	    throws java.io.IOException {
	    this(new SocImpl(mux));
	    try {
		if (la != null) {
		    bind(la);
		}
		connect(sa);
	    } catch (java.io.IOException
		     | IllegalArgumentException
		     | SecurityException ex) {
		try {
		    close();
		} catch (java.io.IOException ex1) {
		    ex.addSuppressed(ex1);
		}
		throw ex;
	    }
	}

	public java.io.InputStream getInputStream()
	    throws java.io.IOException {
	    return impl.ep.is();
	}

	public java.io.OutputStream getOutputStream()
	    throws java.io.IOException {
	    return impl.ep.os();
	}
    }

    /* Server-socket for MPI with SocketIml.  Note the super socket is
       opened but not used. */

    public static class ServerSocket extends java.net.ServerSocket {
	public static MPIMux mux;
	SocImpl ssimpl;

	public ServerSocket() throws java.io.IOException {
	    super();
	    ssimpl = new SocImpl(mux);
            ssimpl.create(true);
            /*ssimpl.setServerSocket(this);*/
	}

	public ServerSocket(int port)
	    throws java.io.IOException {
	    this(port, 50, null);
	}

	public ServerSocket(int port, int backlog)
	    throws java.io.IOException {
	    this(port, backlog, null);
	}

	public ServerSocket(int port, int backlog, java.net.InetAddress la)
	    throws java.io.IOException {
	    this();
	    if (port < 0 || port > 0xFFFF) {
		throw new IllegalArgumentException
		    ("Port value out of range: " + port);
	    }
	    if (backlog < 1) {
		backlog = 50;
	    }
	    this.bind(sockaddr(la, port), backlog);
	}

	@Override public void bind(java.net.SocketAddress sa0, int backlog)
	    throws java.io.IOException {
	    /*super.bind(sa0, backlog);*/
	    java.net.InetSocketAddress sa = (java.net.InetSocketAddress)sa0;
	    ssimpl.bind(sa.getAddress(), sa.getPort());
	    ssimpl.listen(backlog);
	    if (ACCEPT_TCP_SOCKETS) {
		ssimpl.mpisoc_make_backup_socket(backlog);
	    }
	}

	@Override public java.net.Socket accept()
	    throws java.io.IOException {
	    if (isClosed()) {
		throw new java.net.SocketException("Socket is closed");
	    }
	    if (!isBound()) {
		throw new java.net.SocketException("Socket is not bound yet");
	    }
	    //impl.address = new InetAddress();
	    //impl.fd = new FileDescriptor();
	    java.net.Socket so = ssimpl.mpisoc_accept();
	    return so;
	}

	@Override public void close()
	    throws java.io.IOException {
	    super.close();
	    ssimpl.close();
	}

	@Override public boolean isBound() {
	    return (ssimpl.backupss != null);
	}
    }

    public class SocketFactory extends javax.net.SocketFactory {
	public SocketFactory() {
	    super();
	}

	@Override public java.net.Socket createSocket()
	    throws java.io.IOException {
	    return new Socket();
	}

	public java.net.Socket createSocket(String host, int port)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createSocket(sockaddr(host, port), null, true);
	}

	public java.net.Socket createSocket(String host,
					    int port,
					    java.net.InetAddress la,
					    int lport)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createSocket(sockaddr(host, port),
				     sockaddr(la, lport), true);
	}

	public java.net.Socket createSocket(java.net.InetAddress ia,
					    int port)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createSocket(sockaddr(ia, port), null, true);
	}

	public java.net.Socket createSocket(java.net.InetAddress ia,
					    int port,
					    java.net.InetAddress la,
					    int lport)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createSocket(sockaddr(ia, port),
				     sockaddr(la, lport), true);
	}

	java.net.Socket createSocket(java.net.SocketAddress sa,
				     java.net.SocketAddress la,
				     boolean stream)
	    throws java.io.IOException {
	    return new Socket(sa, la, stream);
	}
    }

    public class ServerSocketFactory extends javax.net.ServerSocketFactory {
	public ServerSocketFactory() {
	    super();
	}

	@Override public java.net.ServerSocket createServerSocket()
	    throws java.io.IOException {
	    return new ServerSocket();
	}

	public java.net.ServerSocket createServerSocket(int port)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createServerSocket(port, 50, null);
	}

	public java.net.ServerSocket createServerSocket(int port,
							int backlog)
	    throws java.io.IOException {
	    /*abstract*/
	    return this.createServerSocket(port, backlog, null);
	}

	public java.net.ServerSocket
	    createServerSocket(int port, int backlog,
			       java.net.InetAddress la)
	    throws java.io.IOException {
	    /*abstract*/
	    return new ServerSocket(port, backlog, la);
	}
    }

    /* Runs a thread for accepting a plain server socket.  It queues
       accepted sockets.  Calling accept() starts the thread, and
       stop() stops the thread. */

    static class PlainSocketAccept implements Runnable {
	final java.net.ServerSocket ss;
	int accepting = 0;
	Vector<java.net.Socket> queue = new Vector<java.net.Socket>();
	Thread thread = null;

	PlainSocketAccept(java.net.ServerSocket ss) {
	    this.ss = ss;
	}

	/* Returns an accepted socket (if already exists), or returns
	   null and runs an accepting thread. */

	synchronized java.net.Socket accept() {
	    if (queue.size() > 0) {
		return queue.remove(0);
	    } else {
		return null;
	    }
	}

	synchronized void start() {
	    accepting++;
	    if (accepting == 1) {
		this.notifyAll();
	    }
	}

	synchronized void stop() {
	    accepting--;
	}

	/* Closes the server socket which wakens it from accept(). */

	synchronized void close() throws java.io.IOException {
	    assert (queue.size() == 0);
	    ss.close();
	    this.notifyAll();
	}

	synchronized boolean domaint() {
	    if ((!ss.isClosed()) && (accepting == 0)) {
		try {
		    this.wait();
		} catch (InterruptedException ex) {}
	    }
	    return (!ss.isClosed());
	}

	synchronized void accepted(java.net.Socket s) {
	    queue.add(s);
	}

	public void run() {
	    thread = Thread.currentThread();
	    thread.setName("PlainSocketAccept");
	    try {
		for (;;) {
		    boolean running = domaint();
		    if (!running) {
			break;
		    }
		    java.net.Socket s = ss.accept();
		    accepted(s);
		}
	    } catch (java.io.IOException ex) {}
	    System.err.println("PlainSocketAccept thread done");
	}
    }
}
