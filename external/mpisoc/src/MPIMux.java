/* MPIMux.java */
/* Copyright (C) 2016 RIKEN AICS */

/* Socket for MPI (mpiJava).  The MPISoc part implements sockets.  The
   MPIMux part do actual communication with MPI. */

/* NOTES: Maximum number of buffers for receives is
   (CREDITS*nprocs+PULLS).  Java's Vector and Hashtable are
   synchronized. */

package mpi;

import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.Vector;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Hashtable;

public class MPIMux {
    static final Serial serial = new Serial();

    /* Low-level tracing (very low-level). */

    static final boolean TR = true;

    static final boolean WRITE_BUFFER_UNLIMITED = false;

    /* MTU is 8KB, because it is the default buffering size of the I/O
       streams and it limits the size of a single write. */

    static final int MTU = (8 * 1024);
    static final int PULLS = 100;
    static final int CREDITS = 100;

    static final int MAX_EPID = 80000;
    static final int CONTROL_EPID = 0;
    static final int CONTROL_SIZE = 16;

    static final int COMMAND_CREDITS = 10;
    static final int COMMAND_CONNECT = 11;
    static final int COMMAND_ACCEPTED = 12;

    static final int POLL_INTERVAL_BACKGROUND_MS = 100;
    static final int POLL_INTERVAL_EAGER_MS = 10;

    /* Mapping from an ID of endpoint to an endpoint. */

    final Hashtable<Integer, Endp>
	endpoints = new Hashtable<Integer, Endp>();

    /* Mapping from a listening port to a set of endpoints of a
       pending connect, which are established but are not accepted
       yet. */

    final Hashtable<Integer, Vector<Endp>>
	listenings = new Hashtable<Integer, Vector<Endp>>();

    final MbufVec requests = new MbufVec();
    int sends;
    int recvs;

    final public mpi.Comm comm;
    final public int rank;
    final public int nprocs;

    final MuxPoll polling;

    /* Map from a node address to (multiple) ranks. */

    final Hashtable<java.net.InetAddress, Set<Integer>>
	addresses = new Hashtable<java.net.InetAddress, Set<Integer>>();

    final Vector<MPISoc.PlainSocketAccept>
	backupss = new Vector<MPISoc.PlainSocketAccept>();

    public MPIMux(mpi.Comm comm)
	throws mpi.MPIException, java.net.UnknownHostException {
	this.comm = comm;
	this.rank = comm.getRank();
	this.nprocs = comm.getSize();
	exchange_addresses();

	this.polling = new MuxPoll(this);
	new Thread(this.polling).start();
    }

    public static void init(String[] args) throws mpi.MPIException {
	MPI.Init(args);
	Integer v = (Integer)MPI.COMM_WORLD.getAttr(MPI.TAG_UB);
	int tagub = v.intValue();
	assert (MAX_EPID < tagub);
    }

    public static void fin() throws mpi.MPIException {
	serial.fin();
    }

    public void finish() {
	serial.set_finishing();
	serial.barrier(this);
	for (MPISoc.PlainSocketAccept ss : backupss) {
	    try {
		ss.close();
	    } catch (java.io.IOException ex) {}
	}
	polling.stop();
	for (;;) {
	    try {
		polling.thread.join();
	    } catch (InterruptedException ex) {
		continue;
	    }
	    break;
	}
	requests.compact();
	for (Mbuf m : requests.bufs) {
	    try {
		m.request.cancel();
		m.request.free();
	    } catch (mpi.MPIException ex) {}
	}
	try {poll();} catch (java.io.IOException ex) {}
	try {Thread.sleep(100);} catch (InterruptedException ex) {}
	try {poll();} catch (java.io.IOException ex) {}
	try {Thread.sleep(100);} catch (InterruptedException ex) {}
	try {poll();} catch (java.io.IOException ex) {}
    }

    static byte[] marshall(java.net.InetAddress a) {
	byte[] b;
	try {
	    java.io.ByteArrayOutputStream
		bb = new java.io.ByteArrayOutputStream();
	    java.io.ObjectOutputStream os = new java.io.ObjectOutputStream(bb);
	    os.writeObject(a);
	    os.flush();
	    b = bb.toByteArray();
	} catch (java.io.IOException e) {
	    throw new Error("marshall InetAddress", e);
	}
	return b;
    }

    static java.net.InetAddress unmarshall(byte[] b) {
	java.net.InetAddress a;
	try {
	    java.io.ByteArrayInputStream
		bb = new java.io.ByteArrayInputStream(b);
	    java.io.ObjectInputStream is = new java.io.ObjectInputStream(bb);
	    a = (java.net.InetAddress)is.readObject();
	} catch (ClassNotFoundException e) {
	    throw new Error("unmarshall InetAddress", e);
	} catch (java.io.IOException e) {
	    throw new Error("unmarshall InetAddress", e);
	}
	return a;
    }

    /* Exchanges inet addresses, which are used for mapping hosts
       addresses passed by the socket layer to MPI ranks. */

    void exchange_addresses()
	throws mpi.MPIException, java.net.UnknownHostException {
	final int ADDRSIZE = 256;
	java.net.InetAddress a0 = java.net.InetAddress.getLocalHost();
	byte[] b0 = marshall(a0);
	assert (b0.length < ADDRSIZE);
	byte[] addr = Arrays.copyOf(b0, ADDRSIZE);

	byte[] addrs = new byte[ADDRSIZE * nprocs];
	mpi.MPI.COMM_WORLD.allGather(addr, ADDRSIZE, mpi.MPI.BYTE,
				     addrs, ADDRSIZE, mpi.MPI.BYTE);
	for (int i = 0; i < nprocs; i++) {
	    byte[] bi = Arrays.copyOfRange(addrs, (ADDRSIZE * i),
					   (ADDRSIZE * (i + 1)));
	    java.net.InetAddress ai = unmarshall(bi);
	    Set<Integer> bag;
	    bag = addresses.get(ai);
	    if (bag == null) {
		bag = new HashSet<Integer>();
		addresses.put(ai, bag);
	    }
	    bag.add(i);
	}
    }

    void insert(Mbuf m) {
	requests.insert(m);
    }

    /* Polls messages. */

    public void poll() throws java.io.IOException {
	Vector<Mbuf> bufs = serial.poll_some(this);
	for (Mbuf m : bufs) {
	    int peer = m.source;
	    int epid = m.tag;
	    int size = m.size;
	    if (m.send) {
		sends--;
		m.done();
	    } else {
		recvs--;
		if (epid == CONTROL_EPID) {
		    /*AHO*/
		    if (size != CONTROL_SIZE) {System.err.println
			    ("m.source=" + m.source
			     + " m.tag=" + m.tag
			     + " m.size=" + m.size);}
		    assert (size == CONTROL_SIZE);
		    m.data.limit(size);
		    handle_control(m);
		} else {
		    assert (size <= MTU);
		    m.data.limit(size);
		    Endp ep = endpoints.get(epid);
		    if (ep != null) {
			ep.recv(m);
		    } else {
			System.err.println("stray message for ep=" + epid
					   + " len=" + size);
		    }
		}
		java.nio.ByteBuffer
		    data = java.nio.ByteBuffer.allocateDirect(MTU);
		serial.post_pull(this, data);
		recvs++;
	    }
	}
	while (recvs < PULLS) {
	    java.nio.ByteBuffer
		data = java.nio.ByteBuffer.allocateDirect(MTU);
	    serial.post_pull(this, data);
	    recvs++;
	}
    }

    void handle_control(Mbuf m) throws java.io.IOException {
	int cmd = m.data.getInt();
	int arg0 = m.data.getInt();
	int arg1 = m.data.getInt();
	int arg2 = m.data.getInt();
	if (cmd == COMMAND_CREDITS) {
	    if (TR) System.err.println("COMMAND_CREDITS[" + rank + "]");
	    int epid = arg0;
	    int count = arg1;
	    Endp ep = endpoints.get(epid);
	    assert (ep != null);
	    ep.receive_credit(count);
	    /*ep.send_push();*/
	} else if (cmd == COMMAND_CONNECT) {
	    if (TR) System.err.println("COMMAND_CONNECT[" + rank + "]");
	    int port = arg0;
	    int peer = arg1;
	    int peerepid = arg2;
	    accept_ep(port, peer, peerepid);
	} else if (cmd == COMMAND_ACCEPTED) {
	    if (TR) System.err.println("COMMAND_ACCEPTED[" + rank + "]" + arg2);
	    int epid = arg0;
	    int peerrank = arg1;
	    int peerepid = arg2;
	    connect_accept(epid, peerrank, peerepid);
	} else {
	    throw new Error("Unknown control message");
	}
    }

    /* Creates an endpoint by assigning a local endpoint-ID, but
       without a remote endpoint-ID. */

    synchronized Endp allocate_ep(int peer) {
	int epid = 0;
	for (int e = 1; e < MAX_EPID; e++) {
	    Endp ep = endpoints.get(e);
	    if (ep == null) {
		epid = e;
		break;
	    }
	}
	assert (epid != 0);
	Endp ep = new Endp(this, epid, peer);
	endpoints.put(epid, ep);
	return ep;
    }

    /* Tries to connect a peer's listening port, and waits for an
       acknowledge.  It returns a new EP on success or null. */

    Endp connect_ep(int peerrank, int port) throws mpi.MPIException {
	Endp ep = allocate_ep(peerrank);
	java.nio.ByteBuffer
	    data = java.nio.ByteBuffer.allocateDirect(CONTROL_SIZE);
	data.putInt(COMMAND_CONNECT);
	data.putInt(port);
	data.putInt(rank);
	data.putInt(ep.epid);
	data.flip();
	assert (data.limit() == CONTROL_SIZE);
	serial.post_send(this, true, ep, data, data.limit());
	ep.connect_wait();
	if (ep.peerepid != 0) {
	    return ep;
	} else {
	    endpoints.remove(ep.epid);
	    return null;
	}
    }

    /* Accepts a connect request.  It accpets when it is listening. */

    void accept_ep(int port, int peerrank, int peerepid) {
	Vector<Endp> epset = listenings.get(port);
	Endp ep;
	if (epset != null) {
	    /* Accept a connection. */
	    ep = allocate_ep(peerrank);
	    ep.peerepid = peerepid;
	    epset.add(ep);
	} else {
	    /* Reject a connection.  Use a dummy EP with ID=0. */
	    ep = new Endp(this, 0, peerrank);
	    ep.peerepid = 0;
	}
	java.nio.ByteBuffer
	    data = java.nio.ByteBuffer.allocateDirect(CONTROL_SIZE);
	data.putInt(COMMAND_ACCEPTED);
	data.putInt(peerepid);
	data.putInt(rank);
	data.putInt(ep.epid);
	data.flip();
	assert (data.limit() == CONTROL_SIZE);
	serial.post_send(this, true, ep, data, data.limit());
    }

    /* Handles an acknowledge from a peer.  Call with EP-ID zero
       indicates rejecting an accept. */

    void connect_accept(int epid, int peerrank, int peerepid) {
	Endp ep = endpoints.get(epid);
	if (ep != null) {
	    if (peerepid != 0) {
		/* (Accepted) */
		ep.peerepid = peerepid;
	    } else {
		/* (Rejected) */
		ep.peerepid = 0;
	    }
	    ep.connect_wake();
	} else {
	    System.err.println("stray message for ep=" + epid
			       + " (connect)");
	}
    }

    /* Binds a listening port (it is an interface to MPISoc). */

    synchronized void bind_mpi(int port)
	throws java.io.IOException {
	if (listenings.get(port) != null) {
	    throw new java.net.BindException("Address already in use");
	}
	listenings.put(port, new Vector<Endp>());
    }

    /* Returns a endpoint if some are accepted, or returns null (it is
       an interface to MPISoc). */

    Endp accept_mpi(int port) {
	Vector<Endp> epset = listenings.get(port);
	assert (epset != null);
	try {
	    /* (Atomic remove). */
	    Endp ep = epset.remove(0);
	    return ep;
	} catch (ArrayIndexOutOfBoundsException ex) {
	    return null;
	}
    }

    /* Returns an endpoint if a try to connection to a rank is
       successful, or returns null (it is an interface to MPISoc). */

    Endp connect_mpi(java.net.InetAddress ia, int port) {
	Set<Integer> ranks = addresses.get(ia);
	if (ranks.size() == 0) {
	    return null;
	} else {
	    Endp ep = null;
	    for (int r : ranks) {
		try {
		    ep = connect_ep(r, port);
		    if (ep != null) {
			break;
		    }
		} catch (mpi.MPIException ex) {}
	    }
	    return ep;
	}
    }

    /* A single instance of Serial is created whose instance is used
       to mutex MPI calls. */

    static class Serial {
	boolean finishing = false;

	Serial() {}

	/* Sets a finishing state, which makes ignore polling errors
	   durling finishing. */

	synchronized void set_finishing() {
	    finishing = true;
	}

	/* Post a send (MPI interface).  It serializes calls. */

	synchronized void post_send(MPIMux mux, boolean control, Endp ep,
				    java.nio.ByteBuffer data, int size) {
	    try {
		int tag = (control ? CONTROL_EPID : ep.peerepid);
		mpi.Request r = mux.comm.iSend(data, size,
					       mpi.MPI.BYTE, ep.peerrank, tag);
		Mbuf m = new Mbuf(true, ep, data, r);
		mux.insert(m);
	    } catch (mpi.MPIException ex) {
		throw new Error(ex);
	    }
	}

	/* Post a recv (MPI interface).  It serializes calls. */

	synchronized void post_pull(MPIMux mux, java.nio.ByteBuffer data) {
	    try {
		mpi.Request r = mux.comm.iRecv(data, MTU, mpi.MPI.BYTE,
					       mpi.MPI.ANY_SOURCE,
					       mpi.MPI.ANY_TAG);
		Mbuf m = new Mbuf(false, null, data, r);
		mux.insert(m);
	    } catch (mpi.MPIException ex) {
		throw new Error(ex);
	    }
	}

	/* Polls MPI (MPI interface).  It serializes calls. */

	synchronized Vector<Mbuf> poll_some(MPIMux mux) {
	    mux.requests.compact();
	    Vector<Mbuf> v = new Vector<Mbuf>();
	    if (mux.requests.reqs.length != 0) {
		try {
		    mpi.Status[]
			s = mpi.Request.testSomeStatus(mux.requests.reqs);
		    if (s != null) {
			for (int i = 0; i < s.length; i++) {
			    mpi.Status si = s[i];
			    Mbuf m = mux.requests.remove(si.getIndex());
			    assert (m != null);
			    m.source = si.getSource();
			    m.tag = si.getTag();
			    m.size = si.getCount(mpi.MPI.BYTE);
			    v.add(m);
			}
		    }
		} catch (mpi.MPIException ex) {
		    if (!finishing) {
			throw new Error(ex);
		    }
		}
	    }
	    return v;
	}

	synchronized void barrier(MPIMux mux) {
	    if (TR) System.err.println("MPI_Barrier");
	    try {
		mux.comm.barrier();
	    } catch (mpi.MPIException ex) {}
	}

	/* Finalize (MPI interface).  It serializes calls. */

	synchronized void fin() throws mpi.MPIException {
	    if (TR) System.err.println("MPI_Finalize");
	    MPI.Finalize();
	}
    }

    static class Mbuf {
	final java.nio.ByteBuffer data;
	final boolean send;
	final mpi.Request request;
	Endp ep;
	int source;
	int tag;
	int size;
	int remaining;
	boolean sent_ = false;
	boolean waiting = false;

	/* Makes a send queue entry. */

	public Mbuf(java.nio.ByteBuffer data) {
	    this.data = data;
	    this.send = true;
	    this.ep = null;
	    this.request = null;
	    remaining = data.remaining();
	}

	/* Makes an iSend/iRecv entry. */

	public Mbuf(boolean send, Endp ep, java.nio.ByteBuffer data,
		    mpi.Request request) {
	    this.send = send;
	    this.ep = ep;
	    this.data = data;
	    this.request = request;
	    remaining = 0;
	    sent_ = false;
	}

	synchronized void done() {
	    sent_ = true;
	}
    }

    /* Vector of Mbufs.  It keeps correspondence between a vector and
       an array of requests, which is needed in MPI operations.
       Removing an entry just stores null at the index to avoid
       changes to indexes.  Later compaction does actual removal.
       Calls to remove and compact are from a single thread, while
       calls to insert can be any threads. */

    static class MbufVec {
	Vector<Mbuf> bufs;
	Vector<Mbuf> adds;
	mpi.Request[] reqs;
	boolean changed = false;

	MbufVec() {
	    bufs = new Vector<Mbuf>();
	    adds = new Vector<Mbuf>();
	    reqs = new mpi.Request[0];
	    changed = false;
	}

	synchronized void insert(Mbuf m) {
	    adds.add(m);
	    changed = true;
	}

	synchronized Mbuf remove(int index) {
	    Mbuf m = bufs.set(index, null);
	    changed = true;
	    return m;
	}

	synchronized void compact() {
	    if (changed) {
		int index = 0;
		while (index < bufs.size()) {
		    if (bufs.get(index) == null) {
			Mbuf b = bufs.remove(index);
			assert (b == null);
		    } else {
			index++;
		    }
		}
		bufs.addAll(adds);
		adds.clear();
		reqs = new mpi.Request[bufs.size()];
		for (int i = 0; i < reqs.length; i++) {
		    Mbuf m = bufs.get(i);
		    reqs[i] = m.request;
		}
	    }
	}
    }

    static class Endp {
	final MPIMux mux;
	final int epid;
	final int peerrank;
	int peerepid = 0;
	int credits = CREDITS;
	int consumes = 0;
	boolean recv_suspended = false;
	boolean send_suspended = false;
	boolean recv_closed = false;
	boolean send_closing = false;
	final Vector<Mbuf> recvqueue = new Vector<Mbuf>();
	final Vector<Mbuf> sendqueue = new Vector<Mbuf>();

	java.io.InputStream is = null;
	java.io.OutputStream os = null;

	/* Note the EPID of a peer is assigned later. */

	Endp(MPIMux mux, int epid, int peerrank) {
	    this.mux = mux;
	    this.epid = epid;
	    this.peerrank = peerrank;
	}

	synchronized void receive_credit(int count) {
	    credits += count;
	    if (send_suspended) {
		send_suspended = false;
		this.notifyAll();
	    }
	}

	synchronized void connect_wait() {
	    try {
		this.wait();
	    } catch (InterruptedException ex) {}
	}

	synchronized void connect_wake() {
	    this.notifyAll();
	}

	synchronized int available_mpi() {
	    int count = 0;
	    for (Mbuf m : recvqueue) {
		count += m.data.remaining();
	    }
	    return count;
	}

	synchronized void recv(Mbuf m) {
	    recvqueue.add(m);
	    if (recv_suspended) {
		recv_suspended = false;
		this.notifyAll();
	    }
	}

	synchronized int read_from_mpi(byte b[], int off, int len)
	    throws java.io.IOException {
	    if (recv_closed) {
		assert (recvqueue.size() == 0);
		return -1;
	    } else {
		while (recvqueue.size() == 0) {
		    recv_suspended = true;
		    try {
			this.wait();
		    } catch (InterruptedException ex) {}
		}
		int xoff = off;
		int xlen = len;
		while (xlen > 0 && recvqueue.size() > 0) {
		    Mbuf m = recvqueue.get(0);
		    if (m.data.remaining() == 0) {
			/* Close indicator. */
			if (TR) System.err.println("recv close");
			set_recv_closed();
		    } else {
			int cc = Math.min(m.data.remaining(), xlen);
			m.data.get(b, xoff, cc);
			xoff += cc;
			xlen -= cc;
		    }
		    if (!m.data.hasRemaining()) {
			recvqueue.remove(0);
			consumes++;
		    }
		}
		inform_credit(consumes);
		consumes = 0;
		if ((xoff - off) > 0) {
		    return (xoff - off);
		} else {
		    assert (recv_closed);
		    assert (recvqueue.size() == 0);
		    return -1;
		}
	    }
	}

	void write_to_mpi(byte b[], int off, int len)
	    throws java.io.IOException {
	    assert (!check_send_closing());
	    if (len != 0) {
		/*ByteBuffer data = ByteBuffer.wrap(b, off, len);*/
		java.nio.ByteBuffer
		    data = java.nio.ByteBuffer.allocateDirect(len);
		data.put(b, off, len);
		data.flip();
		assert (data.remaining() == len);
		sendqueue.add(new Mbuf(data));
		send_push();
	    }
	}

	void inform_credit(int count)
	    throws java.io.IOException {
	    java.nio.ByteBuffer
		data = java.nio.ByteBuffer.allocateDirect(CONTROL_SIZE);
	    data.putInt(COMMAND_CREDITS);
	    data.putInt(epid);
	    data.putInt(count);
	    data.putInt(0);
	    data.flip();
	    assert (data.limit() == CONTROL_SIZE);
	    serial.post_send(mux, true, this, data, data.limit());
	}

	/* Sends a zero-legth message to inform closure. */

	void inform_close()
	    throws java.io.IOException {
	    byte b[] = new byte[0];
	    /*ByteBuffer data = ByteBuffer.wrap(b, 0, 0);*/
	    java.nio.ByteBuffer
		data = java.nio.ByteBuffer.allocateDirect(0);
	    data.flip();
	    assert (data.remaining() == 0);
	    sendqueue.add(new Mbuf(data));
	    send_push();
	}

	synchronized void set_recv_closed()
	    throws java.io.IOException {
	    if (recv_suspended || send_suspended) {
		throw new java.net.SocketException();
	    }
	    recv_closed = true;
	}

	void close_is()
	    throws java.io.IOException {
	    /*nothing*/
	}

	synchronized boolean check_send_closing() {
	    return send_closing;
	}

	synchronized boolean set_send_closing()
	    throws java.io.IOException {
	    if (recv_suspended || send_suspended) {
		throw new java.net.SocketException();
	    }
	    boolean alreadyclosing = send_closing;
	    send_closing = true;
	    return alreadyclosing;
	}

	void close_os()
	    throws java.io.IOException {
	    boolean alreadyclosing = set_send_closing();
	    if (!alreadyclosing) {
		inform_close();
	    }
	}

	synchronized void send_push()
	    throws java.io.IOException {
	    while (credits > 0 && sendqueue.size() > 0) {
		Mbuf m = sendqueue.get(0);
		if (m.remaining <= MTU) {
		    if (m.remaining == 0) {
			if (TR) System.err.println("send close");
		    }
		    serial.post_send(mux, false, this, m.data, m.remaining);
		    credits--;
		    m.remaining = 0;
		    sendqueue.remove(0);
		    m.done();
		} else {
		    assert (m.remaining > 0);
		    serial.post_send(mux, false, this, m.data, MTU);
		    credits--;
		    m.remaining -= MTU;
		}
	    }
	    if (!WRITE_BUFFER_UNLIMITED) {
		while (credits == 0 && sendqueue.size() > 0) {
		    send_suspended = true;
		    try {
			this.wait();
		    } catch (InterruptedException ex) {}
		}
	    }
	}

	synchronized java.io.InputStream is() {
	    if (is == null) {
		is = new InputStream(this);
	    }
	    return is;
	}

	synchronized java.io.OutputStream os() {
	    if (os == null) {
		os = new OutputStream(this);
	    }
	    return os;
	}
    }

    /* Background polling (periodic). */

    static class MuxPoll implements Runnable {
	final MPIMux mux;
	int running = 1;
	Thread thread = null;

	MuxPoll(MPIMux mux) {
	    this.mux = mux;
	}

	synchronized void stop() {
	    running = 0;
	}

	synchronized void set(int running) {
	    assert (running > 0);
	    this.running = running;
	}

	synchronized int running() {
	    return running;
	}

	public void run() {
	    thread = Thread.currentThread();
	    thread.setName("MuxPoll");
	    while (running() > 0) {
		try {
		    /*AHO*/
		    mux.poll();
		} catch (java.io.IOException ex) {}
		int interval;
		if (running() == 1) {
		    interval = POLL_INTERVAL_BACKGROUND_MS;
		} else {
		    interval = POLL_INTERVAL_EAGER_MS;
		}
		try {
		    Thread.sleep(interval);
		} catch (InterruptedException ex) {}
	    }
	}
    }

    static class InputStream extends java.io.InputStream {
	final Endp ep;
	boolean open = true;

	InputStream(Endp ep) {
	    super();
	    this.ep = ep;
	}

	public int read() throws java.io.IOException {
	    /*abstract*/
	    byte[] b = new byte[1];
	    int cc = read(b, 0, 1);
	    if (cc == -1) {
		return -1;
	    } else {
		return (int)b[0];
	    }
	}

	@Override public int read(byte[] b)
	    throws java.io.IOException {
	    return this.read(b, 0, b.length);
	}

	@Override public int read(byte b[], int off, int len)
	    throws java.io.IOException {
	    try {ep.mux.poll();} catch (java.io.IOException ex) {}
	    int cc = ep.read_from_mpi(b, off, len);
	    return cc;
	}

	@Override public synchronized void close()
	    throws java.io.IOException {
	    super.close();
	    if (TR) System.err.println("InputStream.close()");
	    if (open) {
		ep.close_is();
		open = false;
	    }
	}
    }

    static class OutputStream extends java.io.OutputStream {
	final Endp ep;
	boolean open = true;

	OutputStream(Endp ep) {
	    super();
	    this.ep = ep;
	}

	public void write(int c)
	    /*abstract*/
	    throws java.io.IOException {
	    byte[] b = new byte[1];
	    b[0] = (byte)c;
	    this.write(b, 0, 1);
	}

	@Override public void write(byte[] b)
	    throws java.io.IOException {
	    this.write(b, 0, b.length);
	}

	@Override public void write(byte[] b, int off, int len)
	    throws java.io.IOException {
	    try {ep.mux.poll();} catch (java.io.IOException ex) {}
	    ep.write_to_mpi(b, off, len);
	}

	@Override public synchronized void close()
	    throws java.io.IOException {
	    super.close();
	    if (TR) System.err.println("OutputStream.close()");
	    if (open) {
		ep.close_os();
		open = false;
	    }
	}
    }
}
