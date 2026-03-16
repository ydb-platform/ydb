/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2006, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef _DB_REPMGR_H_
#define	_DB_REPMGR_H_

#include <contrib/deprecated/bdb/src/dbinc_auto/repmgr_automsg.h>

#if defined(__cplusplus)
extern "C" {
#endif

/*
 * Replication Manager message format types.  These few format codes identify
 * enough information to describe, at the lowest level, how a message should be
 * read from the wire, including how much memory should be allocated to hold the
 * result.  (Often we want to allocate more than just enough to hold the
 * received bytes, if we know that we will need more during processing.)
 *
 * These values are transmitted between sites, even sites running differing BDB
 * versions.  Therefore, once assigned, the values are permanently "frozen".
 *
 * For example, in repmgr wire protocol version 1 the highest assigned message
 * type value was 3, for REPMGR_REP_MESSAGE.  Wire protocol version 2 added the
 * HEARTBEAT message type (4).
 *
 * New message types added in later versions always get new (higher) values.  We
 * still list them in alphabetical order, for ease of reference.  But this
 * generally does not correspond to numerical order.
 */
#define	REPMGR_APP_MESSAGE	5	/* Msg sent from app. on DB_CHANNEL. */
#define	REPMGR_APP_RESPONSE	6	/* Response to a channel request. */
#define	REPMGR_OWN_MSG		8	/* Repmgr's own messages, to peers. */
#define	REPMGR_HANDSHAKE	2	/* Connection establishment sequence. */
#define	REPMGR_HEARTBEAT	4	/* Monitor connection health. */
#define	REPMGR_PERMLSN		1	/* My perm LSN. */
#define	REPMGR_REP_MESSAGE	3	/* Normal replication message. */
#define	REPMGR_RESP_ERROR	7	/* Sys-gen'd error resp to request. */

/*
 * Largest known message type code known in each protocol version we support.
 * In protocol version one there were only three message types: 1, 2, and 3; so
 * 3 was the max.  In protocol version 2 we introduced heartbeats, type 4.
 * (Protocol version 3 did not introduce any new message types.)  In version 4
 * we introduced a few more new message types, the largest of which had value 7.
 */
#define	REPMGR_MAX_V1_MSG_TYPE	3
#define	REPMGR_MAX_V2_MSG_TYPE	4
#define	REPMGR_MAX_V3_MSG_TYPE	4
#define	REPMGR_MAX_V4_MSG_TYPE	8
#define	HEARTBEAT_MIN_VERSION	2
#define	CHANNEL_MIN_VERSION	4
#define	CONN_COLLISION_VERSION	4
#define	GM_MIN_VERSION		4
#define	OWN_MIN_VERSION		4

/* The range of protocol versions we're willing to support. */
#define	DB_REPMGR_VERSION	4
#define	DB_REPMGR_MIN_VERSION	1

/*
 * For messages with the "REPMGR_OWN_MSG" format code, a message type (see
 * REPMGR_OWN_MSG_TYPE, below) is included in the header.  While at the lowest
 * level, the format codes identify only enough to read and allocate memory, at
 * the next higher level the following message type codes identify the content
 * of the message: how to unmarshal and dispatch it.
 *
 * Like the message format types, these message type values should be
 * permanently frozen.
 */
#define	REPMGR_CONNECT_REJECT	1
#define	REPMGR_GM_FAILURE	2
#define	REPMGR_GM_FORWARD	3
#define	REPMGR_JOIN_REQUEST	4
#define	REPMGR_JOIN_SUCCESS	5
#define	REPMGR_PARM_REFRESH	6
#define	REPMGR_REJOIN		7
#define	REPMGR_REMOVE_REQUEST	8
#define	REPMGR_REMOVE_SUCCESS	9
#define	REPMGR_RESOLVE_LIMBO	10
#define	REPMGR_SHARING		11


struct __repmgr_connection;
    typedef struct __repmgr_connection REPMGR_CONNECTION;
struct __repmgr_queue; typedef struct __repmgr_queue REPMGR_QUEUE;
struct __queued_output; typedef struct __queued_output QUEUED_OUTPUT;
struct __repmgr_response; typedef struct __repmgr_response REPMGR_RESPONSE;
struct __repmgr_retry; typedef struct __repmgr_retry REPMGR_RETRY;
struct __repmgr_runnable; typedef struct __repmgr_runnable REPMGR_RUNNABLE;
struct __repmgr_site; typedef struct __repmgr_site REPMGR_SITE;
struct __cond_waiters_table;
    typedef struct __cond_waiters_table COND_WAITERS_TABLE;

/* Current Group Membership DB format ID. */
#define	REPMGR_GMDB_FMT_VERSION	1

#ifdef DB_WIN32
typedef SOCKET socket_t;
typedef HANDLE thread_id_t;
typedef HANDLE mgr_mutex_t;
typedef HANDLE cond_var_t;

typedef COND_WAITERS_TABLE *waiter_t;
typedef WSABUF db_iovec_t;
#else
typedef int socket_t;
typedef pthread_t thread_id_t;
typedef pthread_mutex_t mgr_mutex_t;
typedef pthread_cond_t cond_var_t;
typedef pthread_cond_t waiter_t;
typedef struct iovec db_iovec_t;
#endif

/*
 * The (arbitrary) maximum number of outgoing messages we're willing to hold, on
 * a queue per connection, waiting for TCP buffer space to become available in
 * the kernel.  Rather than exceeding this limit, we simply discard additional
 * messages (since this is always allowed by the replication protocol).
 *    As a special dispensation, if a message is destined for a specific remote
 * site (i.e., it's not a broadcast), then we first try blocking the sending
 * thread, waiting for space to become available (though we only wait a limited
 * time).  This is so as to be able to handle the immediate flood of (a
 * potentially large number of) outgoing messages that replication generates, in
 * a tight loop, when handling PAGE_REQ, LOG_REQ and ALL_REQ requests.
 */
#define	OUT_QUEUE_LIMIT	10

/*
 * The system value is available from sysconf(_SC_HOST_NAME_MAX).
 * Historically, the maximum host name was 256.
 */
#ifndef MAXHOSTNAMELEN
#define	MAXHOSTNAMELEN	256
#endif

/* A buffer big enough for the string "site host.domain.com:65535". */
#define	MAX_SITE_LOC_STRING (MAXHOSTNAMELEN+20)
typedef char SITE_STRING_BUFFER[MAX_SITE_LOC_STRING+1];

#define	MAX_MSG_BUF	(__REPMGR_MAXMSG_SIZE + MAXHOSTNAMELEN + 1)

/* Default timeout values, in seconds. */
#define	DB_REPMGR_DEFAULT_ACK_TIMEOUT		(1 * US_PER_SEC)
#define	DB_REPMGR_DEFAULT_CONNECTION_RETRY	(30 * US_PER_SEC)
#define	DB_REPMGR_DEFAULT_ELECTION_RETRY	(10 * US_PER_SEC)
#define	DB_REPMGR_DEFAULT_CHANNEL_TIMEOUT	(5 * US_PER_SEC)

typedef TAILQ_HEAD(__repmgr_conn_list, __repmgr_connection) CONNECTION_LIST;
typedef STAILQ_HEAD(__repmgr_out_q_head, __queued_output) OUT_Q_HEADER;
typedef TAILQ_HEAD(__repmgr_retry_q, __repmgr_retry) RETRY_Q_HEADER;

/* Information about threads managed by Replication Framework. */
struct __repmgr_runnable {
	ENV *env;
	thread_id_t thread_id;
	void *(*run) __P((void *));
	int finished;		/* Boolean: thread is exiting, may be joined. */
	int quit_requested;	/* Boolean: thread has been asked to quit. */
#ifdef DB_WIN32
	HANDLE quit_event;
#endif
	union {

/*
 * Options governing requested behavior of election thread.
 */
#define	ELECT_F_EVENT_NOTIFY	0x01 /* Notify application of master failure. */
#define	ELECT_F_FAST		0x02 /* First election "fast" (n-1 trick). */
#define	ELECT_F_IMMED		0x04 /* Start with immediate election. */
#define	ELECT_F_INVITEE		0x08 /* Honor (remote) inviter's nsites. */
#define	ELECT_F_STARTUP		0x10 /* Observe repmgr_start() policy. */
		u_int32_t flags;

		int eid;	/* For Connector thread. */

		/*
		 * Args for other thread types can be added here in the future
		 * as needed.
		 */
	} args;
};

/*
 * Information about pending connection establishment retry operations.
 *
 * We keep these in order by time.  This works, under the assumption that the
 * DB_REP_CONNECTION_RETRY never changes once we get going (though that
 * assumption is of course wrong, so this needs to be fixed).
 *
 * Usually, we put things onto the tail end of the list.  But when we add a new
 * site while threads are running, we trigger its first connection attempt by
 * scheduling a retry for "0" microseconds from now, putting its retry element
 * at the head of the list instead.
 *
 * TODO: I think this can be fixed by defining "time" to be the time the element
 * was added (with some convention like "0" meaning immediate), rather than the
 * deadline time.
 */
struct __repmgr_retry {
	TAILQ_ENTRY(__repmgr_retry) entries;
	int eid;
	db_timespec time;
};

/*
 * We use scatter/gather I/O for both reading and writing.  Repmgr messages
 * (including rep messages) use 3 segments: envelope, control and rec.
 * Application messages can have any number of segments (the number they
 * specify, plus 1 for our envelope).  REPMGR_IOVECS_ALLOC_SZ should (only) be
 * used when n > 3.
 */
#define	REPMGR_IOVECS_ALLOC_SZ(n) \
	(sizeof(REPMGR_IOVECS) + ((n) - MIN_IOVEC) * sizeof(db_iovec_t))
typedef struct {
	/*
	 * Index of the first iovec to be used.  Initially of course this is
	 * zero.  But as we progress through partial I/O transfers, it ends up
	 * pointing to the first iovec to be used on the next operation.
	 */
	int offset;

	/*
	 * Total number of pieces defined for this message; equal to the number
	 * of times add_buffer and/or add_dbt were called to populate it.  We do
	 * *NOT* revise this as we go along.  So subsequent I/O operations must
	 * use count-offset to get the number of active vector pieces still
	 * remaining.
	 */
	int count;

	/*
	 * Total number of bytes accounted for in all the pieces of this
	 * message.  We do *NOT* revise this as we go along.
	 */
	size_t total_bytes;

#define	MIN_IOVEC	3
	db_iovec_t vectors[MIN_IOVEC];	/* Variable length array. */
} REPMGR_IOVECS;

typedef struct {
	size_t length;		/* number of bytes in data */
	int ref_count;		/* # of sites' send queues pointing to us */
	u_int8_t data[1];	/* variable size data area */
} REPMGR_FLAT;

struct __queued_output {
	STAILQ_ENTRY(__queued_output) entries;
	REPMGR_FLAT *msg;
	size_t offset;
};

/*
 * The following is for input.  Once we know the sizes of the pieces of an
 * incoming message, we can create this struct (and also the data areas for the
 * pieces themselves, in the same memory allocation).  This is also the struct
 * in which the message lives while it's waiting to be processed by message
 * threads.
 */
typedef struct __repmgr_message {
	STAILQ_ENTRY(__repmgr_message) entries;
	__repmgr_msg_hdr_args msg_hdr;
	union {
		struct {
			int originating_eid;
			DBT control, rec;
		} repmsg;
		struct {
			REPMGR_CONNECTION *conn;
			DBT request;
		} gmdb_msg;
		struct {
			/*
			 * Connection from which the message arrived; NULL if
			 * generated on the local site.
			 */
			REPMGR_CONNECTION *conn;

			DBT buf; /* for reading */
			DBT segments[1]; /* expanded in msg th. before callbk */
		} appmsg;
	} v;			/* Variants */
} REPMGR_MESSAGE;

typedef enum {
	SIZES_PHASE,
	DATA_PHASE
} phase_t;

typedef enum {
	APP_CONNECTION,
	REP_CONNECTION,
	UNKNOWN_CONN_TYPE
} conn_type_t;

struct __repmgr_connection {
	TAILQ_ENTRY(__repmgr_connection) entries;

	socket_t fd;
#ifdef DB_WIN32
	WSAEVENT event_object;
#endif

	/*
	 * Number of other structures referring to this conn struct.  This
	 * ref_count must be reduced to zero before this conn struct can be
	 * destroyed.  Referents include:
	 *
	 * - the select() loop, which owns the right to do all reading, as well
	 *   as the exclusive right to eventually close the socket
	 *
	 * - a "channel" that owns this APP_CONNECTION (on the originating side)
	 *
	 * - a message received on this APP_CONNECTION, queued for processing
	 *
	 * - any writer blocked on waiting for the outbound queue to drain
	 */
	u_int32_t	ref_count;

	conn_type_t type;
	u_int32_t version;	/* Wire protocol version on this connection. */
				/* (0 means not yet determined.) */

/*
 * When we make an outgoing connection, it starts in CONNECTED state.  When we
 * get the response to our version negotiation, we move to READY.
 *     For incoming connections that we accept, we start in NEGOTIATE, then to
 * PARAMETERS, and then to READY.
 *     CONGESTED is a hierarchical substate of READY: it's just like READY, with
 * the additional wrinkle that we don't bother waiting for the outgoing queue to
 * drain in certain circumstances.
 */
#define	CONN_CONGESTED	1	/* Long-lived full outgoing queue. */
#define	CONN_CONNECTED	2	/* Awaiting reply to our version negotiation. */
#define	CONN_DEFUNCT	3	/* Basically dead, awaiting clean-up. */
#define	CONN_NEGOTIATE	4	/* Awaiting version proposal. */
#define	CONN_PARAMETERS	5	/* Awaiting parameters handshake. */
#define	CONN_READY	6	/* Everything's fine. */
	int state;

	/*
	 * Input: while we're reading a message, we keep track of what phase
	 * we're in.  In both phases, we use a REPMGR_IOVECS to keep track of
	 * our progress within the phase.  Depending upon the message type, we
	 * end up with either a rep_message (which is a wrapper for the control
	 * and rec DBTs), or a single generic DBT.
	 *     Any time we're in DATA_PHASE, it means we have already received
	 * the message header (consisting of msg_type and 2 sizes), and
	 * therefore we have allocated buffer space to read the data.  (This is
	 * important for resource clean-up.)
	 */
	phase_t		reading_phase;
	REPMGR_IOVECS iovecs;

	u_int8_t	msg_type;
	u_int8_t	msg_hdr_buf[__REPMGR_MSG_HDR_SIZE];

	union {
		REPMGR_MESSAGE *rep_message;
		struct {
			DBT cntrl, rec;
		} repmgr_msg;
	} input;

	/*
	 * Output: usually we just simply write messages right in line, in the
	 * send() function's thread.  But if TCP doesn't have enough network
	 * buffer space for us when we first try it, we instead allocate some
	 * memory, and copy the message, and then send it as space becomes
	 * available in our main select() thread.  In some cases, if the queue
	 * gets too long we wait until it's drained, and then append to it.
	 * This condition variable's associated mutex is the normal per-repmgr
	 * db_rep->mutex, because that mutex is always held anyway whenever the
	 * output queue is consulted.
	 */
	OUT_Q_HEADER outbound_queue;
	int out_queue_length;
	cond_var_t drained;

	/* =-=-=-=-= app-channel stuff =-=-=-=-= */
	waiter_t	response_waiters;

	/*
	 * Array of info about pending responses to requests.  This info is here
	 * (rather than on the stack of the thread calling send_request())
	 * because it provides an easy way to allocate available numbers for
	 * message tags, and also so that we can easily find the right info when
	 * we get the tag back in the msg header of the response.
	 */
	REPMGR_RESPONSE *responses;
	u_int32_t	aresp;	/* Array size. */
	u_int32_t	cur_resp; /* Index of response currently reading. */

	/* =-=-=-=-= for normal repmgr connections =-=-=-=-= */
	/*
	 * Generally on a REP_CONNECTION type, we have an associated EID (which
	 * is an index into the sites array, by the way).  When we initiate the
	 * connection ("outgoing"), we know from the start what the EID is; the
	 * connection struct is linked from the site struct.  On the other hand,
	 * when we receive an incoming connection, we don't know at first what
	 * site it may be associated with (or even whether it's an
	 * APP_CONNECTION or REP_CONNECTION, for that matter).  During that
	 * initial uncertain time, the eid is -1.  Also, when a connection
	 * becomes defunct, but the conn struct hasn't yet been destroyed, the
	 * eid also becomes -1.
	 *
	 * The eid should be -1 if and only if the connection is on the orphans
	 * list.
	 */
	int eid;

};

#define	IS_READY_STATE(s)	((s) == CONN_READY || (s) == CONN_CONGESTED)

#ifdef HAVE_GETADDRINFO
typedef struct addrinfo	ADDRINFO;
typedef struct sockaddr_storage ACCEPT_ADDR;
#else
typedef struct sockaddr_in ACCEPT_ADDR;
/*
 * Some windows platforms have getaddrinfo (Windows XP), some don't.  We don't
 * support conditional compilation in our Windows build, so we always use our
 * own getaddrinfo implementation.  Rename everything so that we don't collide
 * with the system libraries.
 */
#undef	AI_PASSIVE
#define	AI_PASSIVE	0x01
#undef	AI_CANONNAME
#define	AI_CANONNAME	0x02
#undef	AI_NUMERICHOST
#define	AI_NUMERICHOST	0x04

typedef struct __addrinfo {
	int ai_flags;		/* AI_PASSIVE, AI_CANONNAME, AI_NUMERICHOST */
	int ai_family;		/* PF_xxx */
	int ai_socktype;	/* SOCK_xxx */
	int ai_protocol;	/* 0 or IPPROTO_xxx for IPv4 and IPv6 */
	size_t ai_addrlen;	/* length of ai_addr */
	char *ai_canonname;	/* canonical name for nodename */
	struct sockaddr *ai_addr;	/* binary address */
	struct __addrinfo *ai_next;	/* next structure in linked list */
} ADDRINFO;
#endif /* HAVE_GETADDRINFO */

/*
 * Unprocessed network address configuration.
 */
typedef struct {
	roff_t host;		/* Separately allocated copy of string. */
	u_int16_t port;		/* Stored in plain old host-byte-order. */
} SITEADDR;

/*
 * Site information, as stored in shared region.
 */
typedef struct {
	SITEADDR addr;		/* Unprocessed network address of site. */
	u_int32_t config;	/* Configuration flags: peer, helper, etc. */
	u_int32_t status;	/* Group membership status. */
} SITEINFO;

/*
 * A site address, as stored locally.
 */
typedef struct {
	char *host;		/* Separately allocated copy of string. */
	u_int16_t port;		/* Stored in plain old host-byte-order. */
} repmgr_netaddr_t;

/*
 * We store site structs in a dynamically allocated, growable array, indexed by
 * EID.  We allocate EID numbers for all sites simply according to their
 * index within this array.
 */
#define	SITE_FROM_EID(eid)	(&db_rep->sites[eid])
#define	EID_FROM_SITE(s)	((int)((s) - (&db_rep->sites[0])))
#define	IS_VALID_EID(e)		((e) >= 0)
#define	IS_KNOWN_REMOTE_SITE(e)	((e) >= 0 && ((e) != db_rep->self_eid) && \
	    (((u_int)(e)) < db_rep->site_cnt))
#define	FOR_EACH_REMOTE_SITE_INDEX(i)                    \
	for ((i) = (db_rep->self_eid == 0 ? 1 : 0);	\
	     ((u_int)i) < db_rep->site_cnt;		 \
	     (int)(++(i)) == db_rep->self_eid ? ++(i) : i)

struct __repmgr_site {
	repmgr_netaddr_t net_addr;

	/*
	 * Group membership status: a copy of the status from the membership
	 * database, or the out-of-band value 0, meaning that it doesn't exist.
	 * We keep track of a "non-existent" site because the associated
	 * host/port network address is promised to be associated with the
	 * locally known EID for the life of the environment.
	 */
	u_int32_t	membership; /* Status flags from GMDB. */
	u_int32_t	config;	    /* Flags from site->set_config() */

	/*
	 * Everything below here is applicable only to remote sites.
	 */
	DB_LSN max_ack;		/* Best ack we've heard from this site. */
	int ack_policy;		/* Or 0 if unknown. */
	u_int16_t alignment;	/* Requirements for app channel msgs. */
	db_timespec last_rcvd_timestamp;

	/* Contents depends on state. */
	struct {
		struct {		 /* when CONNECTED */
			/*
			 * The only time we ever have two connections is in case
			 * of a "collision" on the "server" side.  In that case,
			 * the incoming connection either will be closed
			 * promptly by the remote "client", or it is a half-open
			 * connection due to the remote client system having
			 * crashed and rebooted, in which case KEEPALIVE will
			 * eventually clear it.
			 */ 
			REPMGR_CONNECTION *in; /* incoming connection */
			REPMGR_CONNECTION *out; /* outgoing connection */
		} conn;
		REPMGR_RETRY *retry; /* when PAUSING */
		/* Unused when CONNECTING. */
	} ref;

	/*
	 * Subordinate connections (connections from subordinate processes at a
	 * multi-process site).  Note that the SITE_CONNECTED state, and all the
	 * ref.retry stuff above is irrelevant to subordinate connections.  If a
	 * connection is on this list, it exists; and we never bother trying to
	 * reconnect lost connections (indeed we can't, for these are always
	 * incoming-only).
	 */
	CONNECTION_LIST	sub_conns;
	REPMGR_RUNNABLE	*connector;	/* Thread to open a connection. */

#define	SITE_CONNECTED 1	/* We have a (main) connection. */
#define	SITE_CONNECTING 2	/* Trying to establish (main) connection. */
#define	SITE_IDLE 3		/* Doing nothing. */
#define	SITE_PAUSING 4		/* Waiting til time to retry connecting. */
	int state;

#define	SITE_HAS_PRIO	0x01	/* Set if "electable" flag bit is valid. */
#define	SITE_ELECTABLE	0x02
#define	SITE_TOUCHED	0x04	/* Seen GMDB record during present scan. */
	u_int32_t flags;
};

/*
 * Flag values for the public DB_SITE handle.
 */
#define	DB_SITE_PREOPEN	0x01	/* Provisional EID; may change at env open. */

struct __repmgr_response {
	DBT		dbt;
	int		ret;

#define	RESP_COMPLETE		0x01
#define	RESP_DUMMY_BUF		0x02
#define	RESP_IN_USE		0x04
#define	RESP_READING		0x08
#define	RESP_THREAD_WAITING	0x10
	u_int32_t	flags;
};

/*
 * Private structure for managing comms "channels."  This is separate from
 * DB_CHANNEL so as to avoid dragging in other private structures (e.g.,
 * REPMGR_CONNECTION) into db.h, similar to the relationship between DB_ENV and
 * ENV.
 */
struct __channel {
	DB_CHANNEL *db_channel;
	ENV *env;

	union {
		/* For simple, specific-EID channels. */
		REPMGR_CONNECTION *conn;

		/* For EID_MASTER or EID_BROADCAST channels. */
		struct {
			mgr_mutex_t *mutex;  /* For connection establishment. */
			REPMGR_CONNECTION **array;
			u_int32_t cnt;
		} conns;
	} c;
	REPMGR_MESSAGE *msg;	/* Incoming channel only; NULL otherwise. */
	int	responded;	/* Boolean flag. */
	__repmgr_msg_metadata_args *meta;

	/* Used only in send-to-self request case. */
	struct __repmgr_response	response;
};

/*
 * Repmgr keeps track of references to connection information (instances
 * of struct __repmgr_connection).  There are three kinds of places
 * connections may be found: (1) SITE->ref.conn, (2) SITE->sub_conns, and
 * (3) db_rep->connections.
 *
 * 1. SITE->ref.conn points to our connection with the main process running
 * at the given site, if such a connection exists.  We may have initiated
 * the connection to the site ourselves, or we may have received it as an
 * incoming connection.  Once it is established there is very little
 * difference between those two cases.
 *
 * 2. SITE->sub_conns is a list of connections we have with subordinate
 * processes running at the given site.  There can be any number of these
 * connections, one per subordinate process.  Note that these connections
 * are always incoming: there's no way for us to initiate this kind of
 * connection because subordinate processes do not "listen".
 *
 * 3. The db_rep->connections list contains the references to any
 * connections that are not actively associated with any site (we
 * sometimes call these "orphans").  There are two times when this can
 * be:
 *
 *   a) When we accept an incoming connection, we don't know what site it
 *      comes from until we read the initial handshake message.
 *
 *   b) When an error occurs on a connection, we first mark it as DEFUNCT
 *      and stop using it.  Then, at a later, well-defined time, we close
 *      the connection's file descriptor and get rid of the connection
 *      struct.
 *
 * In light of the above, we can see that the following describes the
 * rules for how connections may be moved among these three kinds of
 * "places":
 *
 * - when we initiate an outgoing connection, we of course know what site
 *   it's going to be going to, and so we immediately put the pointer to
 *   the connection struct into SITE->ref.conn
 *
 * - when we accept an incoming connection, we don't immediately know
 *   whom it's from, so we have to put it on the orphans list
 *   (db_rep->connections).
 *
 * - (incoming, cont.) But as soon as we complete the initial "handshake"
 *   message exchange, we will know which site it's from and whether it's
 *   a subordinate or main connection.  At that point we remove it from
 *   db_rep->connections and either point to it by SITE->ref.conn, or add
 *   it to the SITE->sub_conns list.
 *
 * - (for any active connection) when an error occurs, we move the
 *   connection to the orphans list until we have a chance to close it.
 */

/*
 * Repmgr message formats.
 *
 * Declarative definitions of current message formats appear in repmgr.msg.
 * (The s_message/gen_msg.awk utility generates C code.)  In general, we send
 * the buffers marshaled from those structure formats in the "control" portion
 * of a message.
 *
 * Each message is prefaced by a 9-byte message header (as described in
 * repmgr_net.c).  Different message types use the two available 32-bit integers
 * in different ways, as codified here:
 */
#define	REPMGR_HDR1(hdr)		((hdr).word1)
#define	REPMGR_HDR2(hdr)		((hdr).word2)

/* REPMGR_APP_MESSAGE */
#define APP_MSG_BUFFER_SIZE		REPMGR_HDR1
#define	APP_MSG_SEGMENT_COUNT		REPMGR_HDR2

/* REPMGR_REP_MESSAGE and the other traditional repmgr message types. */
#define	REP_MSG_CONTROL_SIZE		REPMGR_HDR1
#define	REP_MSG_REC_SIZE		REPMGR_HDR2

/* REPMGR_APP_RESPONSE */
#define	APP_RESP_BUFFER_SIZE		REPMGR_HDR1
#define	APP_RESP_TAG			REPMGR_HDR2

/* REPMGR_RESP_ERROR.  Note that a zero-length message body is implied. */
#define	RESP_ERROR_CODE			REPMGR_HDR1
#define	RESP_ERROR_TAG			REPMGR_HDR2

/* REPMGR_OWN_MSG */
#define	REPMGR_OWN_BUF_SIZE		REPMGR_HDR1
#define	REPMGR_OWN_MSG_TYPE		REPMGR_HDR2

/*
 * Flags for the handshake message.  As with repmgr message types, these values
 * are transmitted between sites, and must therefore be "frozen" permanently.
 * Names are alphabetized here for easy reference, but values reflect historical
 * usage.
 */
#define	APP_CHANNEL_CONNECTION	0x02	/* Connection used for app channel. */
#define	ELECTABLE_SITE		0x04
#define	REPMGR_SUBORDINATE	0x01	/* This is a subordinate connection. */

/*
 * Flags for application-message meta-data.
 */
#define	REPMGR_MULTI_RESP	0x01
#define	REPMGR_REQUEST_MSG_TYPE	0x02
#define	REPMGR_RESPONSE_LIMIT	0x04

/*
 * Legacy V1 handshake message format.  For compatibility, we send this as part
 * of version negotiation upon connection establishment.
 */
typedef struct {
	u_int32_t version;
	u_int16_t port;
	u_int32_t priority;
} DB_REPMGR_V1_HANDSHAKE;

/*
 * Storage formats.
 *
 * As with message formats, stored formats are defined in repmgr.msg.
 */
/*
 * Flags for the Group Membership data portion of a record.  Like message type
 * codes, these values are frozen across releases, in order to avoid pointless
 * churn.
 */
#define	SITE_ADDING	0x01
#define	SITE_DELETING	0x02
#define	SITE_PRESENT	0x04

/*
 * Message types whose processing could take a long time.  We're careful to
 * avoid using up all our message processing threads on these message types, so
 * that we don't starve out the more important rep messages.
 */ 
#define	IS_DEFERRABLE(t) ((t) == REPMGR_OWN_MSG || (t) == REPMGR_APP_MESSAGE)
/*
 * When using leases there are times when a thread processing a message
 * must block, waiting for leases to be refreshed.  But refreshing the
 * leases requires another thread to accept the lease grant messages.
 */
#define	RESERVED_MSG_TH(env) (IS_USING_LEASES(env) ? 2 : 1)

#define	IS_SUBORDINATE(db_rep)	(db_rep->listen_fd == INVALID_SOCKET)

#define	IS_PEER_POLICY(p) ((p) == DB_REPMGR_ACKS_ALL_PEERS ||		\
    (p) == DB_REPMGR_ACKS_QUORUM ||		\
    (p) == DB_REPMGR_ACKS_ONE_PEER)

/*
 * Most of the code in repmgr runs while holding repmgr's main mutex, which
 * resides in db_rep->mutex.  This mutex is owned by a single repmgr process,
 * and serializes access to the (large) critical sections among threads in the
 * process.  Unlike many other mutexes in DB, it is specifically coded as either
 * a POSIX threads mutex or a Win32 mutex.  Note that although it's a large
 * fraction of the code, it's a tiny fraction of the time: repmgr spends most of
 * its time in a call to select(), and as well a bit in calls into the Base
 * replication API.  All of those release the mutex.
 *     Access to repmgr's shared list of site addresses is protected by
 * another mutex: mtx_repmgr.  And, when changing space allocation for that site
 * list we conform to the convention of acquiring renv->mtx_regenv.  These are
 * less frequent of course.
 *     When it's necessary to acquire more than one of these mutexes, the
 * ordering priority (or "lock ordering protocol") is:
 *        db_rep->mutex (first)
 *        mtx_repmgr    (briefly)
 *        mtx_regenv    (last, and most briefly)
 *
 * There are also mutexes for app message "channels".  Each channel has a mutex,
 * which is used to serialize any connection re-establishment that may become
 * necessary during its lifetime (such as when a master changes).  This never
 * happens on a simple, specific-EID channel, but in other cases multiple app
 * threads could be making send_xxx() calls concurrently, and it would not do to
 * have two of them try to re-connect concurrently.
 *     When re-establishing a connection, the channel lock is held while
 * grabbing first the mtx_repmgr, and then the db_rep mutex (but not both
 * together).  I.e., we have:
 *        channel->mutex (first)
 *        [mtx_repmgr (very briefly)] and then [db_rep->mutex (very briefly)]
 */

#define	LOCK_MUTEX(m) do {						\
	if (__repmgr_lock_mutex(m) != 0)				\
		return (DB_RUNRECOVERY);				\
} while (0)

#define	UNLOCK_MUTEX(m) do {						\
		if (__repmgr_unlock_mutex(m) != 0)			\
		return (DB_RUNRECOVERY);				\
} while (0)

/* POSIX/Win32 socket (and other) portability. */
#ifdef DB_WIN32
#define	WOULDBLOCK		WSAEWOULDBLOCK
#undef	DB_REPMGR_EAGAIN

#define	net_errno		WSAGetLastError()
typedef int socklen_t;
typedef char * sockopt_t;
#define	sendsocket(s, buf, len, flags) send((s), (buf), (int)(len), (flags))

#define	iov_len len
#define	iov_base buf

typedef DWORD threadsync_timeout_t;

#define	REPMGR_INITED(db_rep) (db_rep->signaler != NULL)
#else

#define	INVALID_SOCKET		-1
#define	SOCKET_ERROR		-1
#define	WOULDBLOCK		EWOULDBLOCK
#define	DB_REPMGR_EAGAIN	EAGAIN

#define	net_errno		errno
typedef void * sockopt_t;

#define	sendsocket(s, buf, len, flags) send((s), (buf), (len), (flags))
#define	closesocket(fd)		close(fd)

typedef struct timespec threadsync_timeout_t;

#define	REPMGR_INITED(db_rep) (db_rep->read_pipe >= 0)
#endif

#define	SELECTOR_RUNNING(db_rep)	((db_rep)->selector != NULL)

/*
 * Generic definition of some action to be performed on each connection, in the
 * form of a call-back function.
 */
typedef int (*CONNECTION_ACTION) __P((ENV *, REPMGR_CONNECTION *, void *));

/*
 * Generic predicate to test a condition that a thread is waiting for.
 */
typedef int (*PREDICATE) __P((ENV *, void *));

#include <contrib/deprecated/bdb/src/dbinc_auto/repmgr_ext.h>

#if defined(__cplusplus)
}
#endif
#endif /* !_DB_REPMGR_H_ */
