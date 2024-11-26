/* ----------
 * pgstat.c
 *
 *	All the statistics collector stuff hacked up in one big, ugly file.
 *
 *	TODO:	- Separate collector, postmaster and backend stuff
 *			  into different files.
 *
 *			- Add some automatic call for pgstat vacuuming.
 *
 *			- Add a pgstat config column to pg_database, so this
 *			  entire thing can be enabled/disabled on a per db basis.
 *
 *	Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "common/ip.h"
#include "executor/instrument.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/backendid.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_STAT_INTERVAL	500 /* Minimum time between stats file
									 * updates; in milliseconds. */

#define PGSTAT_RETRY_DELAY		10	/* How long to wait between checks for a
									 * new file; in milliseconds. */

#define PGSTAT_MAX_WAIT_TIME	10000	/* Maximum time to wait for a stats
										 * file update; in milliseconds. */

#define PGSTAT_INQ_INTERVAL		640 /* How often to ping the collector for a
									 * new file; in milliseconds. */

#define PGSTAT_RESTART_INTERVAL 60	/* How often to attempt to restart a
									 * failed statistics collector; in
									 * seconds. */

#define PGSTAT_POLL_LOOP_COUNT	(PGSTAT_MAX_WAIT_TIME / PGSTAT_RETRY_DELAY)
#define PGSTAT_INQ_LOOP_COUNT	(PGSTAT_INQ_INTERVAL / PGSTAT_RETRY_DELAY)

/* Minimum receive buffer size for the collector's socket. */
#define PGSTAT_MIN_RCVBUF		(100 * 1024)


/* ----------
 * The initial size hints for the hash tables used in the collector.
 * ----------
 */
#define PGSTAT_DB_HASH_SIZE		16
#define PGSTAT_TAB_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512
#define PGSTAT_REPLSLOT_HASH_SIZE	32


/* ----------
 * GUC parameters
 * ----------
 */
__thread bool		pgstat_track_counts = false;
__thread int			pgstat_track_functions = TRACK_FUNC_OFF;

/* ----------
 * Built from GUC parameter
 * ----------
 */
__thread char	   *pgstat_stat_directory = NULL;
__thread char	   *pgstat_stat_filename = NULL;
__thread char	   *pgstat_stat_tmpname = NULL;

/*
 * BgWriter and WAL global statistics counters.
 * Stored directly in a stats message structure so they can be sent
 * without needing to copy things around.  We assume these init to zeroes.
 */
__thread PgStat_MsgBgWriter BgWriterStats;
__thread PgStat_MsgWal WalStats;

/*
 * WAL usage counters saved from pgWALUsage at the previous call to
 * pgstat_send_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_send_wal() calls, by substracting
 * the previous counters from the current ones.
 */
static __thread WalUsage prevWalUsage;

/*
 * List of SLRU names that we keep stats for.  There is no central registry of
 * SLRUs, so we use this fixed list instead.  The "other" entry is used for
 * all SLRUs without an explicit entry (e.g. SLRUs in extensions).
 */
static const char *const slru_names[] = {
	"CommitTs",
	"MultiXactMember",
	"MultiXactOffset",
	"Notify",
	"Serial",
	"Subtrans",
	"Xact",
	"other"						/* has to be last */
};

#define SLRU_NUM_ELEMENTS	lengthof(slru_names)

/*
 * SLRU statistics counts waiting to be sent to the collector.  These are
 * stored directly in stats message format so they can be sent without needing
 * to copy things around.  We assume this variable inits to zeroes.  Entries
 * are one-to-one with slru_names[].
 */
static __thread PgStat_MsgSLRU SLRUStats[SLRU_NUM_ELEMENTS];

/* ----------
 * Local data
 * ----------
 */
__thread NON_EXEC_STATIC pgsocket pgStatSock = PGINVALID_SOCKET;

static __thread struct sockaddr_storage pgStatAddr;

static __thread time_t last_pgstat_start_time;

static __thread bool pgStatRunningInCollector = false;

/*
 * Structures in which backends store per-table info that's waiting to be
 * sent to the collector.
 *
 * NOTE: once allocated, TabStatusArray structures are never moved or deleted
 * for the life of the backend.  Also, we zero out the t_id fields of the
 * contained PgStat_TableStatus structs whenever they are not actively in use.
 * This allows relcache pgstat_info pointers to be treated as long-lived data,
 * avoiding repeated searches in pgstat_initstats() when a relation is
 * repeatedly opened during a transaction.
 */
#define TABSTAT_QUANTUM		100 /* we alloc this many at a time */

typedef struct TabStatusArray
{
	struct TabStatusArray *tsa_next;	/* link to next array, if any */
	int			tsa_used;		/* # entries currently used */
	PgStat_TableStatus tsa_entries[TABSTAT_QUANTUM];	/* per-table data */
} TabStatusArray;

static __thread TabStatusArray *pgStatTabList = NULL;

/*
 * pgStatTabHash entry: map from relation OID to PgStat_TableStatus pointer
 */
typedef struct TabStatHashEntry
{
	Oid			t_id;
	PgStat_TableStatus *tsa_entry;
} TabStatHashEntry;

/*
 * Hash table for O(1) t_id -> tsa_entry lookup
 */
static __thread HTAB *pgStatTabHash = NULL;

/*
 * Backends store per-function info that's waiting to be sent to the collector
 * in this hash table (indexed by function OID).
 */
static __thread HTAB *pgStatFunctions = NULL;

/*
 * Indicates if backend has some function stats that it hasn't yet
 * sent to the collector.
 */
static __thread bool have_function_stats = false;

/*
 * Tuple insertion/deletion counts for an open transaction can't be propagated
 * into PgStat_TableStatus counters until we know if it is going to commit
 * or abort.  Hence, we keep these counts in per-subxact structs that live
 * in TopTransactionContext.  This data structure is designed on the assumption
 * that subxacts won't usually modify very many tables.
 */
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */
	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;

static __thread PgStat_SubXactStatus *pgStatXactStack = NULL;

static __thread int	pgStatXactCommit = 0;
static __thread int	pgStatXactRollback = 0;
__thread PgStat_Counter pgStatBlockReadTime = 0;
__thread PgStat_Counter pgStatBlockWriteTime = 0;
static __thread PgStat_Counter pgLastSessionReportTime = 0;
__thread PgStat_Counter pgStatActiveTime = 0;
__thread PgStat_Counter pgStatTransactionIdleTime = 0;
__thread SessionEndType pgStatSessionEndCause = DISCONNECT_NORMAL;

/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	PgStat_Counter inserted_pre_trunc;	/* tuples inserted prior to truncate */
	PgStat_Counter updated_pre_trunc;	/* tuples updated prior to truncate */
	PgStat_Counter deleted_pre_trunc;	/* tuples deleted prior to truncate */
	Oid			t_id;			/* table's OID */
	bool		t_shared;		/* is it a shared catalog? */
	bool		t_truncated;	/* was the relation truncated? */
} TwoPhasePgStatRecord;

/*
 * Info about current "snapshot" of stats file
 */
static __thread MemoryContext pgStatLocalContext = NULL;
static __thread HTAB *pgStatDBHash = NULL;

/*
 * Cluster wide statistics, kept in the stats collector.
 * Contains statistics that are not collected per database
 * or per table.
 */
static __thread PgStat_ArchiverStats archiverStats;
static __thread PgStat_GlobalStats globalStats;
static __thread PgStat_WalStats walStats;
static __thread PgStat_SLRUStats slruStats[SLRU_NUM_ELEMENTS];
static __thread HTAB *replSlotStatHash = NULL;

/*
 * List of OIDs of databases we need to write out.  If an entry is InvalidOid,
 * it means to write only the shared-catalog stats ("DB 0"); otherwise, we
 * will write both that DB's data and the shared stats.
 */
static __thread List *pending_write_requests = NIL;

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static __thread instr_time total_func_time;


/* ----------
 * Local function forward declarations
 * ----------
 */
#ifdef EXEC_BACKEND
static pid_t pgstat_forkexec(void);
#endif

NON_EXEC_STATIC void PgstatCollectorMain(int argc, char *argv[]) pg_attribute_noreturn();

static PgStat_StatDBEntry *pgstat_get_db_entry(Oid databaseid, bool create);
static PgStat_StatTabEntry *pgstat_get_tab_entry(PgStat_StatDBEntry *dbentry,
												 Oid tableoid, bool create);
static void pgstat_write_statsfiles(bool permanent, bool allDbs);
static void pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry, bool permanent);
static HTAB *pgstat_read_statsfiles(Oid onlydb, bool permanent, bool deep);
static void pgstat_read_db_statsfile(Oid databaseid, HTAB *tabhash, HTAB *funchash, bool permanent);
static void backend_read_statsfile(void);

static bool pgstat_write_statsfile_needed(void);
static bool pgstat_db_requested(Oid databaseid);

static PgStat_StatReplSlotEntry *pgstat_get_replslot_entry(NameData name, bool create_it);
static void pgstat_reset_replslot(PgStat_StatReplSlotEntry *slotstats, TimestampTz ts);

static void pgstat_send_tabstat(PgStat_MsgTabstat *tsmsg, TimestampTz now);
static void pgstat_send_funcstats(void);
static void pgstat_send_slru(void);
static HTAB *pgstat_collect_oids(Oid catalogid, AttrNumber anum_oid);
static bool pgstat_should_report_connstat(void);
static void pgstat_report_disconnect(Oid dboid);

static PgStat_TableStatus *get_tabstat_entry(Oid rel_id, bool isshared);

static void pgstat_setup_memcxt(void);

static void pgstat_setheader(PgStat_MsgHdr *hdr, StatMsgType mtype);
static void pgstat_send(void *msg, int len);

static void pgstat_recv_inquiry(PgStat_MsgInquiry *msg, int len);
static void pgstat_recv_tabstat(PgStat_MsgTabstat *msg, int len);
static void pgstat_recv_tabpurge(PgStat_MsgTabpurge *msg, int len);
static void pgstat_recv_dropdb(PgStat_MsgDropdb *msg, int len);
static void pgstat_recv_resetcounter(PgStat_MsgResetcounter *msg, int len);
static void pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter *msg, int len);
static void pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter *msg, int len);
static void pgstat_recv_resetslrucounter(PgStat_MsgResetslrucounter *msg, int len);
static void pgstat_recv_resetreplslotcounter(PgStat_MsgResetreplslotcounter *msg, int len);
static void pgstat_recv_autovac(PgStat_MsgAutovacStart *msg, int len);
static void pgstat_recv_vacuum(PgStat_MsgVacuum *msg, int len);
static void pgstat_recv_analyze(PgStat_MsgAnalyze *msg, int len);
static void pgstat_recv_archiver(PgStat_MsgArchiver *msg, int len);
static void pgstat_recv_bgwriter(PgStat_MsgBgWriter *msg, int len);
static void pgstat_recv_wal(PgStat_MsgWal *msg, int len);
static void pgstat_recv_slru(PgStat_MsgSLRU *msg, int len);
static void pgstat_recv_funcstat(PgStat_MsgFuncstat *msg, int len);
static void pgstat_recv_funcpurge(PgStat_MsgFuncpurge *msg, int len);
static void pgstat_recv_recoveryconflict(PgStat_MsgRecoveryConflict *msg, int len);
static void pgstat_recv_deadlock(PgStat_MsgDeadlock *msg, int len);
static void pgstat_recv_checksum_failure(PgStat_MsgChecksumFailure *msg, int len);
static void pgstat_recv_connect(PgStat_MsgConnect *msg, int len);
static void pgstat_recv_disconnect(PgStat_MsgDisconnect *msg, int len);
static void pgstat_recv_replslot(PgStat_MsgReplSlot *msg, int len);
static void pgstat_recv_tempfile(PgStat_MsgTempFile *msg, int len);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_init() -
 *
 *	Called from postmaster at startup. Create the resources required
 *	by the statistics collector process.  If unable to do so, do not
 *	fail --- better to let the postmaster start with stats collection
 *	disabled.
 * ----------
 */
void
pgstat_init(void)
{
	ACCEPT_TYPE_ARG3 alen;
	struct addrinfo *addrs = NULL,
			   *addr,
				hints;
	int			ret;
	fd_set		rset;
	struct timeval tv;
	char		test_byte;
	int			sel_res;
	int			tries = 0;

#define TESTBYTEVAL ((char) 199)

	/*
	 * This static assertion verifies that we didn't mess up the calculations
	 * involved in selecting maximum payload sizes for our UDP messages.
	 * Because the only consequence of overrunning PGSTAT_MAX_MSG_SIZE would
	 * be silent performance loss from fragmentation, it seems worth having a
	 * compile-time cross-check that we didn't.
	 */
	StaticAssertStmt(sizeof(PgStat_Msg) <= PGSTAT_MAX_MSG_SIZE,
					 "maximum stats message size exceeds PGSTAT_MAX_MSG_SIZE");

	/*
	 * Create the UDP socket for sending and receiving statistic messages
	 */
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;
	hints.ai_next = NULL;
	ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
	if (ret || !addrs)
	{
		ereport(LOG,
				(errmsg("could not resolve \"localhost\": %s",
						gai_strerror(ret))));
		goto startup_failed;
	}

	/*
	 * On some platforms, pg_getaddrinfo_all() may return multiple addresses
	 * only one of which will actually work (eg, both IPv6 and IPv4 addresses
	 * when kernel will reject IPv6).  Worse, the failure may occur at the
	 * bind() or perhaps even connect() stage.  So we must loop through the
	 * results till we find a working combination. We will generate LOG
	 * messages, but no error, for bogus combinations.
	 */
	for (addr = addrs; addr; addr = addr->ai_next)
	{
#ifdef HAVE_UNIX_SOCKETS
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;
#endif

		if (++tries > 1)
			ereport(LOG,
					(errmsg("trying another address for the statistics collector")));

		/*
		 * Create the socket.
		 */
		if ((pgStatSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not create socket for statistics collector: %m")));
			continue;
		}

		/*
		 * Bind it to a kernel assigned port on localhost and get the assigned
		 * port via getsockname().
		 */
		if (bind(pgStatSock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		alen = sizeof(pgStatAddr);
		if (getsockname(pgStatSock, (struct sockaddr *) &pgStatAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * Connect the socket to its own address.  This saves a few cycles by
		 * not having to respecify the target address on every send. This also
		 * provides a kernel-level check that only packets from this same
		 * address will be received.
		 */
		if (connect(pgStatSock, (struct sockaddr *) &pgStatAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not connect socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * Try to send and receive a one-byte test message on the socket. This
		 * is to catch situations where the socket can be created but will not
		 * actually pass data (for instance, because kernel packet filtering
		 * rules prevent it).
		 */
		test_byte = TESTBYTEVAL;

retry1:
		if (send(pgStatSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not send test message on socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * There could possibly be a little delay before the message can be
		 * received.  We arbitrarily allow up to half a second before deciding
		 * it's broken.
		 */
		for (;;)				/* need a loop to handle EINTR */
		{
			FD_ZERO(&rset);
			FD_SET(pgStatSock, &rset);

			tv.tv_sec = 0;
			tv.tv_usec = 500000;
			sel_res = select(pgStatSock + 1, &rset, NULL, NULL, &tv);
			if (sel_res >= 0 || errno != EINTR)
				break;
		}
		if (sel_res < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("select() failed in statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}
		if (sel_res == 0 || !FD_ISSET(pgStatSock, &rset))
		{
			/*
			 * This is the case we actually think is likely, so take pains to
			 * give a specific message for it.
			 *
			 * errno will not be set meaningfully here, so don't use it.
			 */
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("test message did not get through on socket for statistics collector")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		test_byte++;			/* just make sure variable is changed */

retry2:
		if (recv(pgStatSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry2;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive test message on socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		if (test_byte != TESTBYTEVAL)	/* strictly paranoia ... */
		{
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("incorrect test message transmission on socket for statistics collector")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/* If we get here, we have a working socket */
		break;
	}

	/* Did we find a working address? */
	if (!addr || pgStatSock == PGINVALID_SOCKET)
		goto startup_failed;

	/*
	 * Set the socket to non-blocking IO.  This ensures that if the collector
	 * falls behind, statistics messages will be discarded; backends won't
	 * block waiting to send messages to the collector.
	 */
	if (!pg_set_noblock(pgStatSock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set statistics collector socket to nonblocking mode: %m")));
		goto startup_failed;
	}

	/*
	 * Try to ensure that the socket's receive buffer is at least
	 * PGSTAT_MIN_RCVBUF bytes, so that it won't easily overflow and lose
	 * data.  Use of UDP protocol means that we are willing to lose data under
	 * heavy load, but we don't want it to happen just because of ridiculously
	 * small default buffer sizes (such as 8KB on older Windows versions).
	 */
	{
		int			old_rcvbuf;
		int			new_rcvbuf;
		ACCEPT_TYPE_ARG3 rcvbufsize = sizeof(old_rcvbuf);

		if (getsockopt(pgStatSock, SOL_SOCKET, SO_RCVBUF,
					   (char *) &old_rcvbuf, &rcvbufsize) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "SO_RCVBUF")));
			/* if we can't get existing size, always try to set it */
			old_rcvbuf = 0;
		}

		new_rcvbuf = PGSTAT_MIN_RCVBUF;
		if (old_rcvbuf < new_rcvbuf)
		{
			if (setsockopt(pgStatSock, SOL_SOCKET, SO_RCVBUF,
						   (char *) &new_rcvbuf, sizeof(new_rcvbuf)) < 0)
				ereport(LOG,
						(errmsg("%s(%s) failed: %m", "setsockopt", "SO_RCVBUF")));
		}
	}

	pg_freeaddrinfo_all(hints.ai_family, addrs);

	/* Now that we have a long-lived socket, tell fd.c about it. */
	ReserveExternalFD();

	return;

startup_failed:
	ereport(LOG,
			(errmsg("disabling statistics collector for lack of working socket")));

	if (addrs)
		pg_freeaddrinfo_all(hints.ai_family, addrs);

	if (pgStatSock != PGINVALID_SOCKET)
		closesocket(pgStatSock);
	pgStatSock = PGINVALID_SOCKET;

	/*
	 * Adjust GUC variables to suppress useless activity, and for debugging
	 * purposes (seeing track_counts off is a clue that we failed here). We
	 * use PGC_S_OVERRIDE because there is no point in trying to turn it back
	 * on from postgresql.conf without a restart.
	 */
	SetConfigOption("track_counts", "off", PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * subroutine for pgstat_reset_all
 */
static void
pgstat_reset_remove_files(const char *directory)
{
	DIR		   *dir;
	struct dirent *entry;
	char		fname[MAXPGPATH * 2];

	dir = AllocateDir(directory);
	while ((entry = ReadDir(dir, directory)) != NULL)
	{
		int			nchars;
		Oid			tmp_oid;

		/*
		 * Skip directory entries that don't match the file names we write.
		 * See get_dbstat_filename for the database-specific pattern.
		 */
		if (strncmp(entry->d_name, "global.", 7) == 0)
			nchars = 7;
		else
		{
			nchars = 0;
			(void) sscanf(entry->d_name, "db_%u.%n",
						  &tmp_oid, &nchars);
			if (nchars <= 0)
				continue;
			/* %u allows leading whitespace, so reject that */
			if (strchr("0123456789", entry->d_name[3]) == NULL)
				continue;
		}

		if (strcmp(entry->d_name + nchars, "tmp") != 0 &&
			strcmp(entry->d_name + nchars, "stat") != 0)
			continue;

		snprintf(fname, sizeof(fname), "%s/%s", directory,
				 entry->d_name);
		unlink(fname);
	}
	FreeDir(dir);
}

/*
 * pgstat_reset_all() -
 *
 * Remove the stats files.  This is currently used only if WAL
 * recovery is needed after a crash.
 */
void
pgstat_reset_all(void)
{
	pgstat_reset_remove_files(pgstat_stat_directory);
	pgstat_reset_remove_files(PGSTAT_STAT_PERMANENT_DIRECTORY);
}

#ifdef EXEC_BACKEND

/*
 * pgstat_forkexec() -
 *
 * Format up the arglist for, then fork and exec, statistics collector process
 */
static pid_t
pgstat_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkcol";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif							/* EXEC_BACKEND */


/*
 * pgstat_start() -
 *
 *	Called from postmaster at startup or after an existing collector
 *	died.  Attempt to fire up a fresh statistics collector.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
int
pgstat_start(void)
{
	time_t		curtime;
	pid_t		pgStatPid;

	/*
	 * Check that the socket is there, else pgstat_init failed and we can do
	 * nothing useful.
	 */
	if (pgStatSock == PGINVALID_SOCKET)
		return 0;

	/*
	 * Do nothing if too soon since last collector start.  This is a safety
	 * valve to protect against continuous respawn attempts if the collector
	 * is dying immediately at launch.  Note that since we will be re-called
	 * from the postmaster main loop, we will get another chance later.
	 */
	curtime = time(NULL);
	if ((unsigned int) (curtime - last_pgstat_start_time) <
		(unsigned int) PGSTAT_RESTART_INTERVAL)
		return 0;
	last_pgstat_start_time = curtime;

	/*
	 * Okay, fork off the collector.
	 */
#ifdef EXEC_BACKEND
	switch ((pgStatPid = pgstat_forkexec()))
#else
	switch ((pgStatPid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork statistics collector: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Drop our connection to postmaster's shared memory, as well */
			dsm_detach_all();
			PGSharedMemoryDetach();

			PgstatCollectorMain(0, NULL);
			break;
#endif

		default:
			return (int) pgStatPid;
	}

	/* shouldn't get here */
	return 0;
}

void
allow_immediate_pgstat_restart(void)
{
	last_pgstat_start_time = 0;
}

/* ------------------------------------------------------------
 * Public functions used by backends follow
 *------------------------------------------------------------
 */


/* ----------
 * pgstat_report_stat() -
 *
 *	Must be called by processes that performs DML: tcop/postgres.c, logical
 *	receiver processes, SPI worker, etc. to send the so far collected
 *	per-table and function usage statistics to the collector.  Note that this
 *	is called only when not within a transaction, so it is fair to use
 *	transaction stop time as an approximation of current time.
 *
 *	"disconnect" is "true" only for the last call before the backend
 *	exits.  This makes sure that no data is lost and that interrupted
 *	sessions are reported correctly.
 * ----------
 */
void
pgstat_report_stat(bool disconnect)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_TableCounts all_zeroes;
	static __thread TimestampTz last_report = 0;

	TimestampTz now;
	PgStat_MsgTabstat regular_msg;
	PgStat_MsgTabstat shared_msg;
	TabStatusArray *tsa;
	int			i;

	/*
	 * Don't expend a clock check if nothing to do.
	 *
	 * To determine whether any WAL activity has occurred since last time, not
	 * only the number of generated WAL records but also the numbers of WAL
	 * writes and syncs need to be checked. Because even transaction that
	 * generates no WAL records can write or sync WAL data when flushing the
	 * data pages.
	 */
	if ((pgStatTabList == NULL || pgStatTabList->tsa_used == 0) &&
		pgStatXactCommit == 0 && pgStatXactRollback == 0 &&
		pgWalUsage.wal_records == prevWalUsage.wal_records &&
		WalStats.m_wal_write == 0 && WalStats.m_wal_sync == 0 &&
		!have_function_stats && !disconnect)
		return;

	/*
	 * Don't send a message unless it's been at least PGSTAT_STAT_INTERVAL
	 * msec since we last sent one, or the backend is about to exit.
	 */
	now = GetCurrentTransactionStopTimestamp();
	if (!disconnect &&
		!TimestampDifferenceExceeds(last_report, now, PGSTAT_STAT_INTERVAL))
		return;

	last_report = now;

	if (disconnect)
		pgstat_report_disconnect(MyDatabaseId);

	/*
	 * Destroy pgStatTabHash before we start invalidating PgStat_TableEntry
	 * entries it points to.  (Should we fail partway through the loop below,
	 * it's okay to have removed the hashtable already --- the only
	 * consequence is we'd get multiple entries for the same table in the
	 * pgStatTabList, and that's safe.)
	 */
	if (pgStatTabHash)
		hash_destroy(pgStatTabHash);
	pgStatTabHash = NULL;

	/*
	 * Scan through the TabStatusArray struct(s) to find tables that actually
	 * have counts, and build messages to send.  We have to separate shared
	 * relations from regular ones because the databaseid field in the message
	 * header has to depend on that.
	 */
	regular_msg.m_databaseid = MyDatabaseId;
	shared_msg.m_databaseid = InvalidOid;
	regular_msg.m_nentries = 0;
	shared_msg.m_nentries = 0;

	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		for (i = 0; i < tsa->tsa_used; i++)
		{
			PgStat_TableStatus *entry = &tsa->tsa_entries[i];
			PgStat_MsgTabstat *this_msg;
			PgStat_TableEntry *this_ent;

			/* Shouldn't have any pending transaction-dependent counts */
			Assert(entry->trans == NULL);

			/*
			 * Ignore entries that didn't accumulate any actual counts, such
			 * as indexes that were opened by the planner but not used.
			 */
			if (memcmp(&entry->t_counts, &all_zeroes,
					   sizeof(PgStat_TableCounts)) == 0)
				continue;

			/*
			 * OK, insert data into the appropriate message, and send if full.
			 */
			this_msg = entry->t_shared ? &shared_msg : &regular_msg;
			this_ent = &this_msg->m_entry[this_msg->m_nentries];
			this_ent->t_id = entry->t_id;
			memcpy(&this_ent->t_counts, &entry->t_counts,
				   sizeof(PgStat_TableCounts));
			if (++this_msg->m_nentries >= PGSTAT_NUM_TABENTRIES)
			{
				pgstat_send_tabstat(this_msg, now);
				this_msg->m_nentries = 0;
			}
		}
		/* zero out PgStat_TableStatus structs after use */
		MemSet(tsa->tsa_entries, 0,
			   tsa->tsa_used * sizeof(PgStat_TableStatus));
		tsa->tsa_used = 0;
	}

	/*
	 * Send partial messages.  Make sure that any pending xact commit/abort
	 * and connection stats get counted, even if there are no table stats to
	 * send.
	 */
	if (regular_msg.m_nentries > 0 ||
		pgStatXactCommit > 0 || pgStatXactRollback > 0 || disconnect)
		pgstat_send_tabstat(&regular_msg, now);
	if (shared_msg.m_nentries > 0)
		pgstat_send_tabstat(&shared_msg, now);

	/* Now, send function statistics */
	pgstat_send_funcstats();

	/* Send WAL statistics */
	pgstat_send_wal(true);

	/* Finally send SLRU statistics */
	pgstat_send_slru();
}

/*
 * Subroutine for pgstat_report_stat: finish and send a tabstat message
 */
static void
pgstat_send_tabstat(PgStat_MsgTabstat *tsmsg, TimestampTz now)
{
	int			n;
	int			len;

	/* It's unlikely we'd get here with no socket, but maybe not impossible */
	if (pgStatSock == PGINVALID_SOCKET)
		return;

	/*
	 * Report and reset accumulated xact commit/rollback and I/O timings
	 * whenever we send a normal tabstat message
	 */
	if (OidIsValid(tsmsg->m_databaseid))
	{
		tsmsg->m_xact_commit = pgStatXactCommit;
		tsmsg->m_xact_rollback = pgStatXactRollback;
		tsmsg->m_block_read_time = pgStatBlockReadTime;
		tsmsg->m_block_write_time = pgStatBlockWriteTime;

		if (pgstat_should_report_connstat())
		{
			long		secs;
			int			usecs;

			/*
			 * pgLastSessionReportTime is initialized to MyStartTimestamp by
			 * pgstat_report_connect().
			 */
			TimestampDifference(pgLastSessionReportTime, now, &secs, &usecs);
			pgLastSessionReportTime = now;
			tsmsg->m_session_time = (PgStat_Counter) secs * 1000000 + usecs;
			tsmsg->m_active_time = pgStatActiveTime;
			tsmsg->m_idle_in_xact_time = pgStatTransactionIdleTime;
		}
		else
		{
			tsmsg->m_session_time = 0;
			tsmsg->m_active_time = 0;
			tsmsg->m_idle_in_xact_time = 0;
		}
		pgStatXactCommit = 0;
		pgStatXactRollback = 0;
		pgStatBlockReadTime = 0;
		pgStatBlockWriteTime = 0;
		pgStatActiveTime = 0;
		pgStatTransactionIdleTime = 0;
	}
	else
	{
		tsmsg->m_xact_commit = 0;
		tsmsg->m_xact_rollback = 0;
		tsmsg->m_block_read_time = 0;
		tsmsg->m_block_write_time = 0;
		tsmsg->m_session_time = 0;
		tsmsg->m_active_time = 0;
		tsmsg->m_idle_in_xact_time = 0;
	}

	n = tsmsg->m_nentries;
	len = offsetof(PgStat_MsgTabstat, m_entry[0]) +
		n * sizeof(PgStat_TableEntry);

	pgstat_setheader(&tsmsg->m_hdr, PGSTAT_MTYPE_TABSTAT);
	pgstat_send(tsmsg, len);
}

/*
 * Subroutine for pgstat_report_stat: populate and send a function stat message
 */
static void
pgstat_send_funcstats(void)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_FunctionCounts all_zeroes;

	PgStat_MsgFuncstat msg;
	PgStat_BackendFunctionEntry *entry;
	HASH_SEQ_STATUS fstat;

	if (pgStatFunctions == NULL)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_FUNCSTAT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_nentries = 0;

	hash_seq_init(&fstat, pgStatFunctions);
	while ((entry = (PgStat_BackendFunctionEntry *) hash_seq_search(&fstat)) != NULL)
	{
		PgStat_FunctionEntry *m_ent;

		/* Skip it if no counts accumulated since last time */
		if (memcmp(&entry->f_counts, &all_zeroes,
				   sizeof(PgStat_FunctionCounts)) == 0)
			continue;

		/* need to convert format of time accumulators */
		m_ent = &msg.m_entry[msg.m_nentries];
		m_ent->f_id = entry->f_id;
		m_ent->f_numcalls = entry->f_counts.f_numcalls;
		m_ent->f_total_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_total_time);
		m_ent->f_self_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_self_time);

		if (++msg.m_nentries >= PGSTAT_NUM_FUNCENTRIES)
		{
			pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) +
						msg.m_nentries * sizeof(PgStat_FunctionEntry));
			msg.m_nentries = 0;
		}

		/* reset the entry's counts */
		MemSet(&entry->f_counts, 0, sizeof(PgStat_FunctionCounts));
	}

	if (msg.m_nentries > 0)
		pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) +
					msg.m_nentries * sizeof(PgStat_FunctionEntry));

	have_function_stats = false;
}


/* ----------
 * pgstat_vacuum_stat() -
 *
 *	Will tell the collector about objects he can get rid of.
 * ----------
 */
void
pgstat_vacuum_stat(void)
{
	HTAB	   *htab;
	PgStat_MsgTabpurge msg;
	PgStat_MsgFuncpurge f_msg;
	HASH_SEQ_STATUS hstat;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;
	int			len;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Read pg_database and make a list of OIDs of all existing databases
	 */
	htab = pgstat_collect_oids(DatabaseRelationId, Anum_pg_database_oid);

	/*
	 * Search the database hash table for dead databases and tell the
	 * collector to drop them.
	 */
	hash_seq_init(&hstat, pgStatDBHash);
	while ((dbentry = (PgStat_StatDBEntry *) hash_seq_search(&hstat)) != NULL)
	{
		Oid			dbid = dbentry->databaseid;

		CHECK_FOR_INTERRUPTS();

		/* the DB entry for shared tables (with InvalidOid) is never dropped */
		if (OidIsValid(dbid) &&
			hash_search(htab, (void *) &dbid, HASH_FIND, NULL) == NULL)
			pgstat_drop_database(dbid);
	}

	/* Clean up */
	hash_destroy(htab);

	/*
	 * Search for all the dead replication slots in stats hashtable and tell
	 * the stats collector to drop them.
	 */
	if (replSlotStatHash)
	{
		PgStat_StatReplSlotEntry *slotentry;

		hash_seq_init(&hstat, replSlotStatHash);
		while ((slotentry = (PgStat_StatReplSlotEntry *) hash_seq_search(&hstat)) != NULL)
		{
			CHECK_FOR_INTERRUPTS();

			if (SearchNamedReplicationSlot(NameStr(slotentry->slotname), true) == NULL)
				pgstat_report_replslot_drop(NameStr(slotentry->slotname));
		}
	}

	/*
	 * Lookup our own database entry; if not found, nothing more to do.
	 */
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &MyDatabaseId,
												 HASH_FIND, NULL);
	if (dbentry == NULL || dbentry->tables == NULL)
		return;

	/*
	 * Similarly to above, make a list of all known relations in this DB.
	 */
	htab = pgstat_collect_oids(RelationRelationId, Anum_pg_class_oid);

	/*
	 * Initialize our messages table counter to zero
	 */
	msg.m_nentries = 0;

	/*
	 * Check for all tables listed in stats hashtable if they still exist.
	 */
	hash_seq_init(&hstat, dbentry->tables);
	while ((tabentry = (PgStat_StatTabEntry *) hash_seq_search(&hstat)) != NULL)
	{
		Oid			tabid = tabentry->tableid;

		CHECK_FOR_INTERRUPTS();

		if (hash_search(htab, (void *) &tabid, HASH_FIND, NULL) != NULL)
			continue;

		/*
		 * Not there, so add this table's Oid to the message
		 */
		msg.m_tableid[msg.m_nentries++] = tabid;

		/*
		 * If the message is full, send it out and reinitialize to empty
		 */
		if (msg.m_nentries >= PGSTAT_NUM_TABPURGE)
		{
			len = offsetof(PgStat_MsgTabpurge, m_tableid[0])
				+ msg.m_nentries * sizeof(Oid);

			pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
			msg.m_databaseid = MyDatabaseId;
			pgstat_send(&msg, len);

			msg.m_nentries = 0;
		}
	}

	/*
	 * Send the rest
	 */
	if (msg.m_nentries > 0)
	{
		len = offsetof(PgStat_MsgTabpurge, m_tableid[0])
			+ msg.m_nentries * sizeof(Oid);

		pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
		msg.m_databaseid = MyDatabaseId;
		pgstat_send(&msg, len);
	}

	/* Clean up */
	hash_destroy(htab);

	/*
	 * Now repeat the above steps for functions.  However, we needn't bother
	 * in the common case where no function stats are being collected.
	 */
	if (dbentry->functions != NULL &&
		hash_get_num_entries(dbentry->functions) > 0)
	{
		htab = pgstat_collect_oids(ProcedureRelationId, Anum_pg_proc_oid);

		pgstat_setheader(&f_msg.m_hdr, PGSTAT_MTYPE_FUNCPURGE);
		f_msg.m_databaseid = MyDatabaseId;
		f_msg.m_nentries = 0;

		hash_seq_init(&hstat, dbentry->functions);
		while ((funcentry = (PgStat_StatFuncEntry *) hash_seq_search(&hstat)) != NULL)
		{
			Oid			funcid = funcentry->functionid;

			CHECK_FOR_INTERRUPTS();

			if (hash_search(htab, (void *) &funcid, HASH_FIND, NULL) != NULL)
				continue;

			/*
			 * Not there, so add this function's Oid to the message
			 */
			f_msg.m_functionid[f_msg.m_nentries++] = funcid;

			/*
			 * If the message is full, send it out and reinitialize to empty
			 */
			if (f_msg.m_nentries >= PGSTAT_NUM_FUNCPURGE)
			{
				len = offsetof(PgStat_MsgFuncpurge, m_functionid[0])
					+ f_msg.m_nentries * sizeof(Oid);

				pgstat_send(&f_msg, len);

				f_msg.m_nentries = 0;
			}
		}

		/*
		 * Send the rest
		 */
		if (f_msg.m_nentries > 0)
		{
			len = offsetof(PgStat_MsgFuncpurge, m_functionid[0])
				+ f_msg.m_nentries * sizeof(Oid);

			pgstat_send(&f_msg, len);
		}

		hash_destroy(htab);
	}
}


/* ----------
 * pgstat_collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static HTAB *
pgstat_collect_oids(Oid catalogid, AttrNumber anum_oid)
{
	HTAB	   *htab;
	HASHCTL		hash_ctl;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hcxt = CurrentMemoryContext;
	htab = hash_create("Temporary table of OIDs",
					   PGSTAT_TAB_HASH_SIZE,
					   &hash_ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	rel = table_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid;
		bool		isnull;

		thisoid = heap_getattr(tup, anum_oid, RelationGetDescr(rel), &isnull);
		Assert(!isnull);

		CHECK_FOR_INTERRUPTS();

		(void) hash_search(htab, (void *) &thisoid, HASH_ENTER, NULL);
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);

	return htab;
}


/* ----------
 * pgstat_drop_database() -
 *
 *	Tell the collector that we just dropped a database.
 *	(If the message gets lost, we will still clean the dead DB eventually
 *	via future invocations of pgstat_vacuum_stat().)
 * ----------
 */
void
pgstat_drop_database(Oid databaseid)
{
	PgStat_MsgDropdb msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DROPDB);
	msg.m_databaseid = databaseid;
	pgstat_send(&msg, sizeof(msg));
}


/* ----------
 * pgstat_drop_relation() -
 *
 *	Tell the collector that we just dropped a relation.
 *	(If the message gets lost, we will still clean the dead entry eventually
 *	via future invocations of pgstat_vacuum_stat().)
 *
 *	Currently not used for lack of any good place to call it; we rely
 *	entirely on pgstat_vacuum_stat() to clean out stats for dead rels.
 * ----------
 */
#ifdef NOT_USED
void
pgstat_drop_relation(Oid relid)
{
	PgStat_MsgTabpurge msg;
	int			len;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	msg.m_tableid[0] = relid;
	msg.m_nentries = 1;

	len = offsetof(PgStat_MsgTabpurge, m_tableid[0]) + sizeof(Oid);

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, len);
}
#endif							/* NOT_USED */

/* ----------
 * pgstat_reset_counters() -
 *
 *	Tell the statistics collector to reset counters for our database.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_counters(void)
{
	PgStat_MsgResetcounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETCOUNTER);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Tell the statistics collector to reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	PgStat_MsgResetsharedcounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	if (strcmp(target, "archiver") == 0)
		msg.m_resettarget = RESET_ARCHIVER;
	else if (strcmp(target, "bgwriter") == 0)
		msg.m_resettarget = RESET_BGWRITER;
	else if (strcmp(target, "wal") == 0)
		msg.m_resettarget = RESET_WAL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\", \"bgwriter\", or \"wal\".")));

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSHAREDCOUNTER);
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Tell the statistics collector to reset a single counter.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_single_counter(Oid objoid, PgStat_Single_Reset_Type type)
{
	PgStat_MsgResetsinglecounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSINGLECOUNTER);
	msg.m_databaseid = MyDatabaseId;
	msg.m_resettype = type;
	msg.m_objectid = objoid;

	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_slru_counter() -
 *
 *	Tell the statistics collector to reset a single SLRU counter, or all
 *	SLRU counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_slru_counter(const char *name)
{
	PgStat_MsgResetslrucounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSLRUCOUNTER);
	msg.m_index = (name) ? pgstat_slru_index(name) : -1;

	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_replslot_counter() -
 *
 *	Tell the statistics collector to reset a single replication slot
 *	counter, or all replication slots counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_replslot_counter(const char *name)
{
	PgStat_MsgResetreplslotcounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	if (name)
	{
		namestrcpy(&msg.m_slotname, name);
		msg.clearall = false;
	}
	else
		msg.clearall = true;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETREPLSLOTCOUNTER);

	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_report_autovac() -
 *
 *	Called from autovacuum.c to report startup of an autovacuum process.
 *	We are called before InitPostgres is done, so can't rely on MyDatabaseId;
 *	the db OID must be passed in, instead.
 * ----------
 */
void
pgstat_report_autovac(Oid dboid)
{
	PgStat_MsgAutovacStart msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_AUTOVAC_START);
	msg.m_databaseid = dboid;
	msg.m_start_time = GetCurrentTimestamp();

	pgstat_send(&msg, sizeof(msg));
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Tell the collector about the table we just vacuumed.
 * ---------
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
	PgStat_MsgVacuum msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_VACUUM);
	msg.m_databaseid = shared ? InvalidOid : MyDatabaseId;
	msg.m_tableoid = tableoid;
	msg.m_autovacuum = IsAutoVacuumWorkerProcess();
	msg.m_vacuumtime = GetCurrentTimestamp();
	msg.m_live_tuples = livetuples;
	msg.m_dead_tuples = deadtuples;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_analyze() -
 *
 *	Tell the collector about the table we just analyzed.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the changes_since_analyze counter.
 * --------
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter)
{
	PgStat_MsgAnalyze msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from what we send to the collector now, else they'll
	 * be double-counted after commit.  (This approach also ensures that the
	 * collector ends up with the right numbers if we abort instead of
	 * committing.)
	 *
	 * Waste no time on partitioned tables, though.
	 */
	if (rel->pgstat_info != NULL &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->t_counts.t_delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_ANALYZE);
	msg.m_databaseid = rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId;
	msg.m_tableoid = RelationGetRelid(rel);
	msg.m_autovacuum = IsAutoVacuumWorkerProcess();
	msg.m_resetcounter = resetcounter;
	msg.m_analyzetime = GetCurrentTimestamp();
	msg.m_live_tuples = livetuples;
	msg.m_dead_tuples = deadtuples;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Tell the collector about a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_MsgRecoveryConflict msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RECOVERYCONFLICT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_reason = reason;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Tell the collector about a deadlock detected.
 * --------
 */
void
pgstat_report_deadlock(void)
{
	PgStat_MsgDeadlock msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DEADLOCK);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}



/* --------
 * pgstat_report_checksum_failures_in_db() -
 *
 *	Tell the collector about one or more checksum failures.
 * --------
 */
void
pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount)
{
	PgStat_MsgChecksumFailure msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_CHECKSUMFAILURE);
	msg.m_databaseid = dboid;
	msg.m_failurecount = failurecount;
	msg.m_failure_time = GetCurrentTimestamp();

	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_checksum_failure() -
 *
 *	Tell the collector about a checksum failure.
 * --------
 */
void
pgstat_report_checksum_failure(void)
{
	pgstat_report_checksum_failures_in_db(MyDatabaseId, 1);
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Tell the collector about a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_MsgTempFile msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TEMPFILE);
	msg.m_databaseid = MyDatabaseId;
	msg.m_filesize = filesize;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_connect() -
 *
 *	Tell the collector about a new connection.
 * --------
 */
void
pgstat_report_connect(Oid dboid)
{
	PgStat_MsgConnect msg;

	if (!pgstat_should_report_connstat())
		return;

	pgLastSessionReportTime = MyStartTimestamp;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_CONNECT);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(PgStat_MsgConnect));
}

/* --------
 * pgstat_report_disconnect() -
 *
 *	Tell the collector about a disconnect.
 * --------
 */
static void
pgstat_report_disconnect(Oid dboid)
{
	PgStat_MsgDisconnect msg;

	if (!pgstat_should_report_connstat())
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DISCONNECT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_cause = pgStatSessionEndCause;
	pgstat_send(&msg, sizeof(PgStat_MsgDisconnect));
}

/* --------
 * pgstat_should_report_connstats() -
 *
 *	We report session statistics only for normal backend processes.  Parallel
 *	workers run in parallel, so they don't contribute to session times, even
 *	though they use CPU time. Walsender processes could be considered here,
 *	but they have different session characteristics from normal backends (for
 *	example, they are always "active"), so they would skew session statistics.
 * ----------
 */
static bool
pgstat_should_report_connstat(void)
{
	return MyBackendType == B_BACKEND;
}

/* ----------
 * pgstat_report_replslot() -
 *
 *	Tell the collector about replication slot statistics.
 * ----------
 */
void
pgstat_report_replslot(const PgStat_StatReplSlotEntry *repSlotStat)
{
	PgStat_MsgReplSlot msg;

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_REPLSLOT);
	namestrcpy(&msg.m_slotname, NameStr(repSlotStat->slotname));
	msg.m_create = false;
	msg.m_drop = false;
	msg.m_spill_txns = repSlotStat->spill_txns;
	msg.m_spill_count = repSlotStat->spill_count;
	msg.m_spill_bytes = repSlotStat->spill_bytes;
	msg.m_stream_txns = repSlotStat->stream_txns;
	msg.m_stream_count = repSlotStat->stream_count;
	msg.m_stream_bytes = repSlotStat->stream_bytes;
	msg.m_total_txns = repSlotStat->total_txns;
	msg.m_total_bytes = repSlotStat->total_bytes;
	pgstat_send(&msg, sizeof(PgStat_MsgReplSlot));
}

/* ----------
 * pgstat_report_replslot_create() -
 *
 *	Tell the collector about creating the replication slot.
 * ----------
 */
void
pgstat_report_replslot_create(const char *slotname)
{
	PgStat_MsgReplSlot msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_REPLSLOT);
	namestrcpy(&msg.m_slotname, slotname);
	msg.m_create = true;
	msg.m_drop = false;
	pgstat_send(&msg, sizeof(PgStat_MsgReplSlot));
}

/* ----------
 * pgstat_report_replslot_drop() -
 *
 *	Tell the collector about dropping the replication slot.
 * ----------
 */
void
pgstat_report_replslot_drop(const char *slotname)
{
	PgStat_MsgReplSlot msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_REPLSLOT);
	namestrcpy(&msg.m_slotname, slotname);
	msg.m_create = false;
	msg.m_drop = true;
	pgstat_send(&msg, sizeof(PgStat_MsgReplSlot));
}

/* ----------
 * pgstat_ping() -
 *
 *	Send some junk data to the collector to increase traffic.
 * ----------
 */
void
pgstat_ping(void)
{
	PgStat_MsgDummy msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DUMMY);
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_inquiry() -
 *
 *	Notify collector that we need fresh data.
 * ----------
 */
static void
pgstat_send_inquiry(TimestampTz clock_time, TimestampTz cutoff_time, Oid databaseid)
{
	PgStat_MsgInquiry msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_INQUIRY);
	msg.clock_time = clock_time;
	msg.cutoff_time = cutoff_time;
	msg.databaseid = databaseid;
	pgstat_send(&msg, sizeof(msg));
}


/*
 * Initialize function call usage data.
 * Called by the executor before invoking a function.
 */
void
pgstat_init_function_usage(FunctionCallInfo fcinfo,
						   PgStat_FunctionCallUsage *fcu)
{
	PgStat_BackendFunctionEntry *htabent;
	bool		found;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	if (!pgStatFunctions)
	{
		/* First time through - initialize function stat table */
		HASHCTL		hash_ctl;

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(PgStat_BackendFunctionEntry);
		pgStatFunctions = hash_create("Function stat entries",
									  PGSTAT_FUNCTION_HASH_SIZE,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
	}

	/* Get the stats entry for this function, create if necessary */
	htabent = hash_search(pgStatFunctions, &fcinfo->flinfo->fn_oid,
						  HASH_ENTER, &found);
	if (!found)
		MemSet(&htabent->f_counts, 0, sizeof(PgStat_FunctionCounts));

	fcu->fs = &htabent->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = htabent->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/*
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	if (pgStatFunctions == NULL)
		return NULL;

	return (PgStat_BackendFunctionEntry *) hash_search(pgStatFunctions,
													   (void *) &func_id,
													   HASH_FIND, NULL);
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
 */
void
pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	PgStat_FunctionCounts *fs = fcu->fs;
	instr_time	f_total;
	instr_time	f_others;
	instr_time	f_self;

	/* stats not wanted? */
	if (fs == NULL)
		return;

	/* total elapsed time in this function call */
	INSTR_TIME_SET_CURRENT(f_total);
	INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

	/* self usage: elapsed minus anything already charged to other calls */
	f_others = total_func_time;
	INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
	f_self = f_total;
	INSTR_TIME_SUBTRACT(f_self, f_others);

	/* update backend-wide total time */
	INSTR_TIME_ADD(total_func_time, f_self);

	/*
	 * Compute the new f_total_time as the total elapsed time added to the
	 * pre-call value of f_total_time.  This is necessary to avoid
	 * double-counting any time taken by recursive calls of myself.  (We do
	 * not need any similar kluge for self time, since that already excludes
	 * any recursive calls.)
	 */
	INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

	/* update counters in function stats table */
	if (finalize)
		fs->f_numcalls++;
	fs->f_total_time = f_total;
	INSTR_TIME_ADD(fs->f_self_time, f_self);

	/* indicate that we have something to send */
	have_function_stats = true;
}


/* ----------
 * pgstat_initstats() -
 *
 *	Initialize a relcache entry to count access statistics.
 *	Called whenever a relation is opened.
 *
 *	We assume that a relcache entry's pgstat_info field is zeroed by
 *	relcache.c when the relcache entry is made; thereafter it is long-lived
 *	data.  We can avoid repeated searches of the TabStatus arrays when the
 *	same relation is touched repeatedly within a transaction.
 * ----------
 */
void
pgstat_initstats(Relation rel)
{
	Oid			rel_id = rel->rd_id;
	char		relkind = rel->rd_rel->relkind;

	/*
	 * We only count stats for relations with storage and partitioned tables
	 */
	if (!RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE)
	{
		rel->pgstat_info = NULL;
		return;
	}

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
	{
		/* We're not counting at all */
		rel->pgstat_info = NULL;
		return;
	}

	/*
	 * If we already set up this relation in the current transaction, nothing
	 * to do.
	 */
	if (rel->pgstat_info != NULL &&
		rel->pgstat_info->t_id == rel_id)
		return;

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = get_tabstat_entry(rel_id, rel->rd_rel->relisshared);
}

/*
 * get_tabstat_entry - find or create a PgStat_TableStatus entry for rel
 */
static PgStat_TableStatus *
get_tabstat_entry(Oid rel_id, bool isshared)
{
	TabStatHashEntry *hash_entry;
	PgStat_TableStatus *entry;
	TabStatusArray *tsa;
	bool		found;

	/*
	 * Create hash table if we don't have it already.
	 */
	if (pgStatTabHash == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TabStatHashEntry);

		pgStatTabHash = hash_create("pgstat TabStatusArray lookup hash table",
									TABSTAT_QUANTUM,
									&ctl,
									HASH_ELEM | HASH_BLOBS);
	}

	/*
	 * Find an entry or create a new one.
	 */
	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new entry with null pointer */
		hash_entry->tsa_entry = NULL;
	}

	/*
	 * If entry is already valid, we're done.
	 */
	if (hash_entry->tsa_entry)
		return hash_entry->tsa_entry;

	/*
	 * Locate the first pgStatTabList entry with free space, making a new list
	 * entry if needed.  Note that we could get an OOM failure here, but if so
	 * we have left the hashtable and the list in a consistent state.
	 */
	if (pgStatTabList == NULL)
	{
		/* Set up first pgStatTabList entry */
		pgStatTabList = (TabStatusArray *)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(TabStatusArray));
	}

	tsa = pgStatTabList;
	while (tsa->tsa_used >= TABSTAT_QUANTUM)
	{
		if (tsa->tsa_next == NULL)
			tsa->tsa_next = (TabStatusArray *)
				MemoryContextAllocZero(TopMemoryContext,
									   sizeof(TabStatusArray));
		tsa = tsa->tsa_next;
	}

	/*
	 * Allocate a PgStat_TableStatus entry within this list entry.  We assume
	 * the entry was already zeroed, either at creation or after last use.
	 */
	entry = &tsa->tsa_entries[tsa->tsa_used++];
	entry->t_id = rel_id;
	entry->t_shared = isshared;

	/*
	 * Now we can fill the entry in pgStatTabHash.
	 */
	hash_entry->tsa_entry = entry;

	return entry;
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 * If no entry, return NULL, don't create a new one
 *
 * Note: if we got an error in the most recent execution of pgstat_report_stat,
 * it's possible that an entry exists but there's no hashtable entry for it.
 * That's okay, we'll treat this case as "doesn't exist".
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	TabStatHashEntry *hash_entry;

	/* If hashtable doesn't exist, there are no entries at all */
	if (!pgStatTabHash)
		return NULL;

	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_FIND, NULL);
	if (!hash_entry)
		return NULL;

	/* Note that this step could also return NULL, but that's correct */
	return hash_entry->tsa_entry;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static PgStat_SubXactStatus *
get_tabstat_stack_level(int nest_level)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		xact_state = (PgStat_SubXactStatus *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(PgStat_SubXactStatus));
		xact_state->nest_level = nest_level;
		xact_state->prev = pgStatXactStack;
		xact_state->first = NULL;
		pgStatXactStack = xact_state;
	}
	return xact_state;
}

/*
 * add_tabstat_xact_level - add a new (sub)transaction state record
 */
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = get_tabstat_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}

/*
 * pgstat_count_heap_insert - count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * pgstat_count_heap_update - count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_updated++;

		/* t_tuples_hot_updated is nontransactional, so just advance it */
		if (hot)
			pgstat_info->t_counts.t_tuples_hot_updated++;
	}
}

/*
 * pgstat_count_heap_delete - count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * pgstat_truncate_save_counters
 *
 * Whenever a table is truncated, we save its i/u/d counters so that they can
 * be cleared, and if the (sub)xact that executed the truncate later aborts,
 * the counters can be restored to the saved (pre-truncate) values.  Note we do
 * this on the first truncate in any particular subxact level only.
 */
static void
pgstat_truncate_save_counters(PgStat_TableXactStatus *trans)
{
	if (!trans->truncated)
	{
		trans->inserted_pre_trunc = trans->tuples_inserted;
		trans->updated_pre_trunc = trans->tuples_updated;
		trans->deleted_pre_trunc = trans->tuples_deleted;
		trans->truncated = true;
	}
}

/*
 * pgstat_truncate_restore_counters - restore counters when a truncate aborts
 */
static void
pgstat_truncate_restore_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncated)
	{
		trans->tuples_inserted = trans->inserted_pre_trunc;
		trans->tuples_updated = trans->updated_pre_trunc;
		trans->tuples_deleted = trans->deleted_pre_trunc;
	}
}

/*
 * pgstat_count_truncate - update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_truncate_save_counters(pgstat_info->trans);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * pgstat_update_heap_dead_tuples - update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so t_delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
		pgstat_info->t_counts.t_delta_dead_tuples -= delta;
}


/* ----------
 * AtEOXact_PgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void
AtEOXact_PgStat(bool isCommit, bool parallel)
{
	PgStat_SubXactStatus *xact_state;

	/* Don't count parallel worker transaction stats */
	if (!parallel)
	{
		/*
		 * Count transaction commit or abort.  (We use counters, not just
		 * bools, in case the reporting message isn't sent right away.)
		 */
		if (isCommit)
			pgStatXactCommit++;
		else
			pgStatXactRollback++;
	}

	/*
	 * Transfer transactional insert/update counts into the base tabstat
	 * entries.  We don't bother to free any of the transactional state, since
	 * it's all in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			/* restore pre-truncate stats (if any) in case of aborted xact */
			if (!isCommit)
				pgstat_truncate_restore_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
			tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
			tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
			if (isCommit)
			{
				tabstat->t_counts.t_truncated = trans->truncated;
				if (trans->truncated)
				{
					/* forget live/dead stats seen by backend thus far */
					tabstat->t_counts.t_delta_live_tuples = 0;
					tabstat->t_counts.t_delta_dead_tuples = 0;
				}
				/* insert adds a live tuple, delete removes one */
				tabstat->t_counts.t_delta_live_tuples +=
					trans->tuples_inserted - trans->tuples_deleted;
				/* update and delete each create a dead tuple */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_updated + trans->tuples_deleted;
				/* insert, update, delete each count as one change event */
				tabstat->t_counts.t_changed_tuples +=
					trans->tuples_inserted + trans->tuples_updated +
					trans->tuples_deleted;
			}
			else
			{
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				/* an aborted xact generates no changed_tuple events */
			}
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/* ----------
 * AtEOSubXact_PgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void
AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * Transfer transactional insert/update counts into the next higher
	 * subtransaction state.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		PgStat_TableXactStatus *trans;
		PgStat_TableXactStatus *next_trans;

		/* delink xact_state from stack immediately to simplify reuse case */
		pgStatXactStack = xact_state->prev;

		for (trans = xact_state->first; trans != NULL; trans = next_trans)
		{
			PgStat_TableStatus *tabstat;

			next_trans = trans->next;
			Assert(trans->nest_level == nestDepth);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			if (isCommit)
			{
				if (trans->upper && trans->upper->nest_level == nestDepth - 1)
				{
					if (trans->truncated)
					{
						/* propagate the truncate status one level up */
						pgstat_truncate_save_counters(trans->upper);
						/* replace upper xact stats with ours */
						trans->upper->tuples_inserted = trans->tuples_inserted;
						trans->upper->tuples_updated = trans->tuples_updated;
						trans->upper->tuples_deleted = trans->tuples_deleted;
					}
					else
					{
						trans->upper->tuples_inserted += trans->tuples_inserted;
						trans->upper->tuples_updated += trans->tuples_updated;
						trans->upper->tuples_deleted += trans->tuples_deleted;
					}
					tabstat->trans = trans->upper;
					pfree(trans);
				}
				else
				{
					/*
					 * When there isn't an immediate parent state, we can just
					 * reuse the record instead of going through a
					 * palloc/pfree pushup (this works since it's all in
					 * TopTransactionContext anyway).  We have to re-link it
					 * into the parent level, though, and that might mean
					 * pushing a new entry into the pgStatXactStack.
					 */
					PgStat_SubXactStatus *upper_xact_state;

					upper_xact_state = get_tabstat_stack_level(nestDepth - 1);
					trans->next = upper_xact_state->first;
					upper_xact_state->first = trans;
					trans->nest_level = nestDepth - 1;
				}
			}
			else
			{
				/*
				 * On abort, update top-level tabstat counts, then forget the
				 * subtransaction
				 */

				/* first restore values obliterated by truncate */
				pgstat_truncate_restore_counters(trans);
				/* count attempted actions regardless of commit/abort */
				tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
				tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
				tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				tabstat->trans = trans->upper;
				pfree(trans);
			}
		}
		pfree(xact_state);
	}
}


/*
 * AtPrepare_PgStat
 *		Save the transactional stats state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending
 * transaction-dependent stats work.
 */
void
AtPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;
			TwoPhasePgStatRecord record;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);

			record.tuples_inserted = trans->tuples_inserted;
			record.tuples_updated = trans->tuples_updated;
			record.tuples_deleted = trans->tuples_deleted;
			record.inserted_pre_trunc = trans->inserted_pre_trunc;
			record.updated_pre_trunc = trans->updated_pre_trunc;
			record.deleted_pre_trunc = trans->deleted_pre_trunc;
			record.t_id = tabstat->t_id;
			record.t_shared = tabstat->t_shared;
			record.t_truncated = trans->truncated;

			RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
								   &record, sizeof(TwoPhasePgStatRecord));
		}
	}
}

/*
 * PostPrepare_PgStat
 *		Clean up after successful PREPARE.
 *
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the stats collector immediately, while the effects on live
 * and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void
PostPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * We don't bother to free any of the transactional state, since it's all
	 * in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			tabstat = trans->parent;
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_truncated = rec->t_truncated;
	if (rec->t_truncated)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->t_counts.t_delta_live_tuples = 0;
		pgstat_info->t_counts.t_delta_dead_tuples = 0;
	}
	pgstat_info->t_counts.t_delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->t_counts.t_changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->t_truncated)
	{
		rec->tuples_inserted = rec->inserted_pre_trunc;
		rec->tuples_updated = rec->updated_pre_trunc;
		rec->tuples_deleted = rec->deleted_pre_trunc;
	}
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}


/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one database or NULL. NULL doesn't mean
 *	that the database doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid)
{
	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Lookup the requested database; return NULL if not found
	 */
	return (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
											  (void *) &dbid,
											  HASH_FIND, NULL);
}


/* ----------
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	Oid			dbid;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Lookup our database, then look in its table hash table.
	 */
	dbid = MyDatabaseId;
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &dbid,
												 HASH_FIND, NULL);
	if (dbentry != NULL && dbentry->tables != NULL)
	{
		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &relid,
													   HASH_FIND, NULL);
		if (tabentry)
			return tabentry;
	}

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	dbid = InvalidOid;
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &dbid,
												 HASH_FIND, NULL);
	if (dbentry != NULL && dbentry->tables != NULL)
	{
		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &relid,
													   HASH_FIND, NULL);
		if (tabentry)
			return tabentry;
	}

	return NULL;
}


/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatFuncEntry *funcentry = NULL;

	/* load the stats file if needed */
	backend_read_statsfile();

	/* Lookup our database, then find the requested function.  */
	dbentry = pgstat_fetch_stat_dbentry(MyDatabaseId);
	if (dbentry != NULL && dbentry->functions != NULL)
	{
		funcentry = (PgStat_StatFuncEntry *) hash_search(dbentry->functions,
														 (void *) &func_id,
														 HASH_FIND, NULL);
	}

	return funcentry;
}


/*
 * ---------
 * pgstat_fetch_stat_archiver() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the archiver statistics struct.
 * ---------
 */
PgStat_ArchiverStats *
pgstat_fetch_stat_archiver(void)
{
	backend_read_statsfile();

	return &archiverStats;
}


/*
 * ---------
 * pgstat_fetch_global() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the global statistics struct.
 * ---------
 */
PgStat_GlobalStats *
pgstat_fetch_global(void)
{
	backend_read_statsfile();

	return &globalStats;
}

/*
 * ---------
 * pgstat_fetch_stat_wal() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the WAL statistics struct.
 * ---------
 */
PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	backend_read_statsfile();

	return &walStats;
}

/*
 * ---------
 * pgstat_fetch_slru() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the slru statistics struct.
 * ---------
 */
PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	backend_read_statsfile();

	return slruStats;
}

/*
 * ---------
 * pgstat_fetch_replslot() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the replication slot statistics struct.
 * ---------
 */
PgStat_StatReplSlotEntry *
pgstat_fetch_replslot(NameData slotname)
{
	backend_read_statsfile();

	return pgstat_get_replslot_entry(slotname, false);
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 */
static void
pgstat_shutdown_hook(int code, Datum arg)
{
	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the collector.  Otherwise, we'd be sending an invalid
	 * database ID, so forget it.  (This means that accesses to pg_database
	 * during failed backend starts might never get counted.)
	 */
	if (OidIsValid(MyDatabaseId))
		pgstat_report_stat(true);
}

/* ----------
 * pgstat_initialize() -
 *
 *	Initialize pgstats state, and set up our on-proc-exit hook.
 *	Called from InitPostgres and AuxiliaryProcessMain.
 *
 *	NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 * ----------
 */
void
pgstat_initialize(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_send_wal() can
	 * calculate how much pgWalUsage counters are increased by substracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;

	/* Set up a process-exit hook to clean up */
	on_shmem_exit(pgstat_shutdown_hook, 0);
}

/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */


/* ----------
 * pgstat_setheader() -
 *
 *		Set common header fields in a statistics message
 * ----------
 */
static void
pgstat_setheader(PgStat_MsgHdr *hdr, StatMsgType mtype)
{
	hdr->m_type = mtype;
}


/* ----------
 * pgstat_send() -
 *
 *		Send out one statistics message to the collector
 * ----------
 */
static void
pgstat_send(void *msg, int len)
{
	int			rc;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	((PgStat_MsgHdr *) msg)->m_size = len;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(pgStatSock, msg, len, 0);
	} while (rc < 0 && errno == EINTR);

#ifdef USE_ASSERT_CHECKING
	/* In debug builds, log send failures ... */
	if (rc < 0)
		elog(LOG, "could not send to statistics collector: %m");
#endif
}

/* ----------
 * pgstat_send_archiver() -
 *
 *	Tell the collector about the WAL file that we successfully
 *	archived or failed to archive.
 * ----------
 */
void
pgstat_send_archiver(const char *xlog, bool failed)
{
	PgStat_MsgArchiver msg;

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_ARCHIVER);
	msg.m_failed = failed;
	strlcpy(msg.m_xlog, xlog, sizeof(msg.m_xlog));
	msg.m_timestamp = GetCurrentTimestamp();
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_bgwriter() -
 *
 *		Send bgwriter statistics to the collector
 * ----------
 */
void
pgstat_send_bgwriter(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_MsgBgWriter all_zeroes;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid sending a completely empty message to the stats
	 * collector.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_MsgBgWriter)) == 0)
		return;

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&BgWriterStats.m_hdr, PGSTAT_MTYPE_BGWRITER);
	pgstat_send(&BgWriterStats, sizeof(BgWriterStats));

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}

/* ----------
 * pgstat_send_wal() -
 *
 *	Send WAL statistics to the collector.
 *
 * If 'force' is not set, WAL stats message is only sent if enough time has
 * passed since last one was sent to reach PGSTAT_STAT_INTERVAL.
 * ----------
 */
void
pgstat_send_wal(bool force)
{
	static __thread TimestampTz sendTime = 0;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid sending a completely empty message to the stats
	 * collector.
	 *
	 * Check wal_records counter to determine whether any WAL activity has
	 * happened since last time. Note that other WalUsage counters don't need
	 * to be checked because they are incremented always together with
	 * wal_records counter.
	 *
	 * m_wal_buffers_full also doesn't need to be checked because it's
	 * incremented only when at least one WAL record is generated (i.e.,
	 * wal_records counter is incremented). But for safely, we assert that
	 * m_wal_buffers_full is always zero when no WAL record is generated
	 *
	 * This function can be called by a process like walwriter that normally
	 * generates no WAL records. To determine whether any WAL activity has
	 * happened at that process since the last time, the numbers of WAL writes
	 * and syncs are also checked.
	 */
	if (pgWalUsage.wal_records == prevWalUsage.wal_records &&
		WalStats.m_wal_write == 0 && WalStats.m_wal_sync == 0)
	{
		Assert(WalStats.m_wal_buffers_full == 0);
		return;
	}

	if (!force)
	{
		TimestampTz now = GetCurrentTimestamp();

		/*
		 * Don't send a message unless it's been at least PGSTAT_STAT_INTERVAL
		 * msec since we last sent one to avoid overloading the stats
		 * collector.
		 */
		if (!TimestampDifferenceExceeds(sendTime, now, PGSTAT_STAT_INTERVAL))
			return;
		sendTime = now;
	}

	/*
	 * Set the counters related to generated WAL data if the counters were
	 * updated.
	 */
	if (pgWalUsage.wal_records != prevWalUsage.wal_records)
	{
		WalUsage	walusage;

		/*
		 * Calculate how much WAL usage counters were increased by
		 * substracting the previous counters from the current ones. Fill the
		 * results in WAL stats message.
		 */
		MemSet(&walusage, 0, sizeof(WalUsage));
		WalUsageAccumDiff(&walusage, &pgWalUsage, &prevWalUsage);

		WalStats.m_wal_records = walusage.wal_records;
		WalStats.m_wal_fpi = walusage.wal_fpi;
		WalStats.m_wal_bytes = walusage.wal_bytes;

		/*
		 * Save the current counters for the subsequent calculation of WAL
		 * usage.
		 */
		prevWalUsage = pgWalUsage;
	}

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&WalStats.m_hdr, PGSTAT_MTYPE_WAL);
	pgstat_send(&WalStats, sizeof(WalStats));

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&WalStats, 0, sizeof(WalStats));
}

/* ----------
 * pgstat_send_slru() -
 *
 *		Send SLRU statistics to the collector
 * ----------
 */
static void
pgstat_send_slru(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_MsgSLRU all_zeroes;

	for (int i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		/*
		 * This function can be called even if nothing at all has happened. In
		 * this case, avoid sending a completely empty message to the stats
		 * collector.
		 */
		if (memcmp(&SLRUStats[i], &all_zeroes, sizeof(PgStat_MsgSLRU)) == 0)
			continue;

		/* set the SLRU type before each send */
		SLRUStats[i].m_index = i;

		/*
		 * Prepare and send the message
		 */
		pgstat_setheader(&SLRUStats[i].m_hdr, PGSTAT_MTYPE_SLRU);
		pgstat_send(&SLRUStats[i], sizeof(PgStat_MsgSLRU));

		/*
		 * Clear out the statistics buffer, so it can be re-used.
		 */
		MemSet(&SLRUStats[i], 0, sizeof(PgStat_MsgSLRU));
	}
}


/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.  This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
NON_EXEC_STATIC void
PgstatCollectorMain(int argc, char *argv[])
{
	int			len;
	PgStat_Msg	msg;
	int			wr;
	WaitEvent	event;
	WaitEventSet *wes;

	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGHUP and SIGQUIT.  Note we don't need a SIGUSR1 handler to
	 * support latch operations, because we only use a local latch.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, SignalHandlerForShutdownRequest);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	PG_SETMASK(&UnBlockSig);

	MyBackendType = B_STATS_COLLECTOR;
	init_ps_display(NULL);

	/*
	 * Read in existing stats files or initialize the stats to zero.
	 */
	pgStatRunningInCollector = true;
	pgStatDBHash = pgstat_read_statsfiles(InvalidOid, true, true);

	/* Prepare to wait for our latch or data in our socket. */
	wes = CreateWaitEventSet(CurrentMemoryContext, 3);
	AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
	AddWaitEventToSet(wes, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(wes, WL_SOCKET_READABLE, pgStatSock, NULL, NULL);

	/*
	 * Loop to process messages until we get SIGQUIT or detect ungraceful
	 * death of our parent postmaster.
	 *
	 * For performance reasons, we don't want to do ResetLatch/WaitLatch after
	 * every message; instead, do that only after a recv() fails to obtain a
	 * message.  (This effectively means that if backends are sending us stuff
	 * like mad, we won't notice postmaster death until things slack off a
	 * bit; which seems fine.)	To do that, we have an inner loop that
	 * iterates as long as recv() succeeds.  We do check ConfigReloadPending
	 * inside the inner loop, which means that such interrupts will get
	 * serviced but the latch won't get cleared until next time there is a
	 * break in the action.
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Quit if we get SIGQUIT from the postmaster.
		 */
		if (ShutdownRequestPending)
			break;

		/*
		 * Inner loop iterates as long as we keep getting messages, or until
		 * ShutdownRequestPending becomes set.
		 */
		while (!ShutdownRequestPending)
		{
			/*
			 * Reload configuration if we got SIGHUP from the postmaster.
			 */
			if (ConfigReloadPending)
			{
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			/*
			 * Write the stats file(s) if a new request has arrived that is
			 * not satisfied by existing file(s).
			 */
			if (pgstat_write_statsfile_needed())
				pgstat_write_statsfiles(false, false);

			/*
			 * Try to receive and process a message.  This will not block,
			 * since the socket is set to non-blocking mode.
			 *
			 * XXX On Windows, we have to force pgwin32_recv to cooperate,
			 * despite the previous use of pg_set_noblock() on the socket.
			 * This is extremely broken and should be fixed someday.
			 */
#ifdef WIN32
			pgwin32_noblock = 1;
#endif

			len = recv(pgStatSock, (char *) &msg,
					   sizeof(PgStat_Msg), 0);

#ifdef WIN32
			pgwin32_noblock = 0;
#endif

			if (len < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					break;		/* out of inner loop */
				ereport(ERROR,
						(errcode_for_socket_access(),
						 errmsg("could not read statistics message: %m")));
			}

			/*
			 * We ignore messages that are smaller than our common header
			 */
			if (len < sizeof(PgStat_MsgHdr))
				continue;

			/*
			 * The received length must match the length in the header
			 */
			if (msg.msg_hdr.m_size != len)
				continue;

			/*
			 * O.K. - we accept this message.  Process it.
			 */
			switch (msg.msg_hdr.m_type)
			{
				case PGSTAT_MTYPE_DUMMY:
					break;

				case PGSTAT_MTYPE_INQUIRY:
					pgstat_recv_inquiry(&msg.msg_inquiry, len);
					break;

				case PGSTAT_MTYPE_TABSTAT:
					pgstat_recv_tabstat(&msg.msg_tabstat, len);
					break;

				case PGSTAT_MTYPE_TABPURGE:
					pgstat_recv_tabpurge(&msg.msg_tabpurge, len);
					break;

				case PGSTAT_MTYPE_DROPDB:
					pgstat_recv_dropdb(&msg.msg_dropdb, len);
					break;

				case PGSTAT_MTYPE_RESETCOUNTER:
					pgstat_recv_resetcounter(&msg.msg_resetcounter, len);
					break;

				case PGSTAT_MTYPE_RESETSHAREDCOUNTER:
					pgstat_recv_resetsharedcounter(&msg.msg_resetsharedcounter,
												   len);
					break;

				case PGSTAT_MTYPE_RESETSINGLECOUNTER:
					pgstat_recv_resetsinglecounter(&msg.msg_resetsinglecounter,
												   len);
					break;

				case PGSTAT_MTYPE_RESETSLRUCOUNTER:
					pgstat_recv_resetslrucounter(&msg.msg_resetslrucounter,
												 len);
					break;

				case PGSTAT_MTYPE_RESETREPLSLOTCOUNTER:
					pgstat_recv_resetreplslotcounter(&msg.msg_resetreplslotcounter,
													 len);
					break;

				case PGSTAT_MTYPE_AUTOVAC_START:
					pgstat_recv_autovac(&msg.msg_autovacuum_start, len);
					break;

				case PGSTAT_MTYPE_VACUUM:
					pgstat_recv_vacuum(&msg.msg_vacuum, len);
					break;

				case PGSTAT_MTYPE_ANALYZE:
					pgstat_recv_analyze(&msg.msg_analyze, len);
					break;

				case PGSTAT_MTYPE_ARCHIVER:
					pgstat_recv_archiver(&msg.msg_archiver, len);
					break;

				case PGSTAT_MTYPE_BGWRITER:
					pgstat_recv_bgwriter(&msg.msg_bgwriter, len);
					break;

				case PGSTAT_MTYPE_WAL:
					pgstat_recv_wal(&msg.msg_wal, len);
					break;

				case PGSTAT_MTYPE_SLRU:
					pgstat_recv_slru(&msg.msg_slru, len);
					break;

				case PGSTAT_MTYPE_FUNCSTAT:
					pgstat_recv_funcstat(&msg.msg_funcstat, len);
					break;

				case PGSTAT_MTYPE_FUNCPURGE:
					pgstat_recv_funcpurge(&msg.msg_funcpurge, len);
					break;

				case PGSTAT_MTYPE_RECOVERYCONFLICT:
					pgstat_recv_recoveryconflict(&msg.msg_recoveryconflict,
												 len);
					break;

				case PGSTAT_MTYPE_DEADLOCK:
					pgstat_recv_deadlock(&msg.msg_deadlock, len);
					break;

				case PGSTAT_MTYPE_TEMPFILE:
					pgstat_recv_tempfile(&msg.msg_tempfile, len);
					break;

				case PGSTAT_MTYPE_CHECKSUMFAILURE:
					pgstat_recv_checksum_failure(&msg.msg_checksumfailure,
												 len);
					break;

				case PGSTAT_MTYPE_REPLSLOT:
					pgstat_recv_replslot(&msg.msg_replslot, len);
					break;

				case PGSTAT_MTYPE_CONNECT:
					pgstat_recv_connect(&msg.msg_connect, len);
					break;

				case PGSTAT_MTYPE_DISCONNECT:
					pgstat_recv_disconnect(&msg.msg_disconnect, len);
					break;

				default:
					break;
			}
		}						/* end of inner message-processing loop */

		/* Sleep until there's something to do */
#ifndef WIN32
		wr = WaitEventSetWait(wes, -1L, &event, 1, WAIT_EVENT_PGSTAT_MAIN);
#else

		/*
		 * Windows, at least in its Windows Server 2003 R2 incarnation,
		 * sometimes loses FD_READ events.  Waking up and retrying the recv()
		 * fixes that, so don't sleep indefinitely.  This is a crock of the
		 * first water, but until somebody wants to debug exactly what's
		 * happening there, this is the best we can do.  The two-second
		 * timeout matches our pre-9.2 behavior, and needs to be short enough
		 * to not provoke "using stale statistics" complaints from
		 * backend_read_statsfile.
		 */
		wr = WaitEventSetWait(wes, 2 * 1000L /* msec */ , &event, 1,
							  WAIT_EVENT_PGSTAT_MAIN);
#endif

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (wr == 1 && event.events == WL_POSTMASTER_DEATH)
			break;
	}							/* end of outer loop */

	/*
	 * Save the final stats to reuse at next startup.
	 */
	pgstat_write_statsfiles(true, true);

	FreeWaitEventSet(wes);

	exit(0);
}

/*
 * Subroutine to clear stats in a database entry
 *
 * Tables and functions hashes are initialized to empty.
 */
static void
reset_dbentry_counters(PgStat_StatDBEntry *dbentry)
{
	HASHCTL		hash_ctl;

	dbentry->n_xact_commit = 0;
	dbentry->n_xact_rollback = 0;
	dbentry->n_blocks_fetched = 0;
	dbentry->n_blocks_hit = 0;
	dbentry->n_tuples_returned = 0;
	dbentry->n_tuples_fetched = 0;
	dbentry->n_tuples_inserted = 0;
	dbentry->n_tuples_updated = 0;
	dbentry->n_tuples_deleted = 0;
	dbentry->last_autovac_time = 0;
	dbentry->n_conflict_tablespace = 0;
	dbentry->n_conflict_lock = 0;
	dbentry->n_conflict_snapshot = 0;
	dbentry->n_conflict_bufferpin = 0;
	dbentry->n_conflict_startup_deadlock = 0;
	dbentry->n_temp_files = 0;
	dbentry->n_temp_bytes = 0;
	dbentry->n_deadlocks = 0;
	dbentry->n_checksum_failures = 0;
	dbentry->last_checksum_failure = 0;
	dbentry->n_block_read_time = 0;
	dbentry->n_block_write_time = 0;
	dbentry->n_sessions = 0;
	dbentry->total_session_time = 0;
	dbentry->total_active_time = 0;
	dbentry->total_idle_in_xact_time = 0;
	dbentry->n_sessions_abandoned = 0;
	dbentry->n_sessions_fatal = 0;
	dbentry->n_sessions_killed = 0;

	dbentry->stat_reset_timestamp = GetCurrentTimestamp();
	dbentry->stats_timestamp = 0;

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
	dbentry->tables = hash_create("Per-database table",
								  PGSTAT_TAB_HASH_SIZE,
								  &hash_ctl,
								  HASH_ELEM | HASH_BLOBS);

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
	dbentry->functions = hash_create("Per-database function",
									 PGSTAT_FUNCTION_HASH_SIZE,
									 &hash_ctl,
									 HASH_ELEM | HASH_BLOBS);
}

/*
 * Lookup the hash table entry for the specified database. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatDBEntry *
pgstat_get_db_entry(Oid databaseid, bool create)
{
	PgStat_StatDBEntry *result;
	bool		found;
	HASHACTION	action = (create ? HASH_ENTER : HASH_FIND);

	/* Lookup or create the hash table entry for this database */
	result = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												&databaseid,
												action, &found);

	if (!create && !found)
		return NULL;

	/*
	 * If not found, initialize the new one.  This creates empty hash tables
	 * for tables and functions, too.
	 */
	if (!found)
		reset_dbentry_counters(result);

	return result;
}


/*
 * Lookup the hash table entry for the specified table. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatTabEntry *
pgstat_get_tab_entry(PgStat_StatDBEntry *dbentry, Oid tableoid, bool create)
{
	PgStat_StatTabEntry *result;
	bool		found;
	HASHACTION	action = (create ? HASH_ENTER : HASH_FIND);

	/* Lookup or create the hash table entry for this table */
	result = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
												 &tableoid,
												 action, &found);

	if (!create && !found)
		return NULL;

	/* If not found, initialize the new one. */
	if (!found)
	{
		result->numscans = 0;
		result->tuples_returned = 0;
		result->tuples_fetched = 0;
		result->tuples_inserted = 0;
		result->tuples_updated = 0;
		result->tuples_deleted = 0;
		result->tuples_hot_updated = 0;
		result->n_live_tuples = 0;
		result->n_dead_tuples = 0;
		result->changes_since_analyze = 0;
		result->inserts_since_vacuum = 0;
		result->blocks_fetched = 0;
		result->blocks_hit = 0;
		result->vacuum_timestamp = 0;
		result->vacuum_count = 0;
		result->autovac_vacuum_timestamp = 0;
		result->autovac_vacuum_count = 0;
		result->analyze_timestamp = 0;
		result->analyze_count = 0;
		result->autovac_analyze_timestamp = 0;
		result->autovac_analyze_count = 0;
	}

	return result;
}


/* ----------
 * pgstat_write_statsfiles() -
 *		Write the global statistics file, as well as requested DB files.
 *
 *	'permanent' specifies writing to the permanent files not temporary ones.
 *	When true (happens only when the collector is shutting down), also remove
 *	the temporary files so that backends starting up under a new postmaster
 *	can't read old data before the new collector is ready.
 *
 *	When 'allDbs' is false, only the requested databases (listed in
 *	pending_write_requests) will be written; otherwise, all databases
 *	will be written.
 * ----------
 */
static void
pgstat_write_statsfiles(bool permanent, bool allDbs)
{
	HASH_SEQ_STATUS hstat;
	PgStat_StatDBEntry *dbentry;
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = permanent ? PGSTAT_STAT_PERMANENT_TMPFILE : pgstat_stat_tmpname;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;
	int			rc;

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Set the timestamp of the stats file.
	 */
	globalStats.stats_timestamp = GetCurrentTimestamp();

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write global stats struct
	 */
	rc = fwrite(&globalStats, sizeof(globalStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver stats struct
	 */
	rc = fwrite(&archiverStats, sizeof(archiverStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write WAL stats struct
	 */
	rc = fwrite(&walStats, sizeof(walStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write SLRU stats struct
	 */
	rc = fwrite(slruStats, sizeof(slruStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database table.
	 */
	hash_seq_init(&hstat, pgStatDBHash);
	while ((dbentry = (PgStat_StatDBEntry *) hash_seq_search(&hstat)) != NULL)
	{
		/*
		 * Write out the table and function stats for this DB into the
		 * appropriate per-DB stat file, if required.
		 */
		if (allDbs || pgstat_db_requested(dbentry->databaseid))
		{
			/* Make DB's timestamp consistent with the global stats */
			dbentry->stats_timestamp = globalStats.stats_timestamp;

			pgstat_write_db_statsfile(dbentry, permanent);
		}

		/*
		 * Write out the DB entry. We don't write the tables or functions
		 * pointers, since they're of no use to any other process.
		 */
		fputc('D', fpout);
		rc = fwrite(dbentry, offsetof(PgStat_StatDBEntry, tables), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * Write replication slot stats struct
	 */
	if (replSlotStatHash)
	{
		PgStat_StatReplSlotEntry *slotent;

		hash_seq_init(&hstat, replSlotStatHash);
		while ((slotent = (PgStat_StatReplSlotEntry *) hash_seq_search(&hstat)) != NULL)
		{
			fputc('R', fpout);
			rc = fwrite(slotent, sizeof(PgStat_StatReplSlotEntry), 1, fpout);
			(void) rc;			/* we'll check for error with ferror */
		}
	}

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}

	if (permanent)
		unlink(pgstat_stat_filename);

	/*
	 * Now throw away the list of requests.  Note that requests sent after we
	 * started the write are still waiting on the network socket.
	 */
	list_free(pending_write_requests);
	pending_write_requests = NIL;
}

/*
 * return the filename for a DB stat file; filename is the output buffer,
 * of length len.
 */
static void
get_dbstat_filename(bool permanent, bool tempname, Oid databaseid,
					char *filename, int len)
{
	int			printed;

	/* NB -- pgstat_reset_remove_files knows about the pattern this uses */
	printed = snprintf(filename, len, "%s/db_%u.%s",
					   permanent ? PGSTAT_STAT_PERMANENT_DIRECTORY :
					   pgstat_stat_directory,
					   databaseid,
					   tempname ? "tmp" : "stat");
	if (printed >= len)
		elog(ERROR, "overlength pgstat path");
}

/* ----------
 * pgstat_write_db_statsfile() -
 *		Write the stat file for a single database.
 *
 *	If writing to the permanent file (happens when the collector is
 *	shutting down only), remove the temporary file so that backends
 *	starting up under a new postmaster can't read the old data before
 *	the new collector is ready.
 * ----------
 */
static void
pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry, bool permanent)
{
	HASH_SEQ_STATUS tstat;
	HASH_SEQ_STATUS fstat;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpout;
	int32		format_id;
	Oid			dbid = dbentry->databaseid;
	int			rc;
	char		tmpfile[MAXPGPATH];
	char		statfile[MAXPGPATH];

	get_dbstat_filename(permanent, true, dbid, tmpfile, MAXPGPATH);
	get_dbstat_filename(permanent, false, dbid, statfile, MAXPGPATH);

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database's access stats per table.
	 */
	hash_seq_init(&tstat, dbentry->tables);
	while ((tabentry = (PgStat_StatTabEntry *) hash_seq_search(&tstat)) != NULL)
	{
		fputc('T', fpout);
		rc = fwrite(tabentry, sizeof(PgStat_StatTabEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * Walk through the database's function stats table.
	 */
	hash_seq_init(&fstat, dbentry->functions);
	while ((funcentry = (PgStat_StatFuncEntry *) hash_seq_search(&fstat)) != NULL)
	{
		fputc('F', fpout);
		rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}

	if (permanent)
	{
		get_dbstat_filename(false, false, dbid, statfile, MAXPGPATH);

		elog(DEBUG2, "removing temporary stats file \"%s\"", statfile);
		unlink(statfile);
	}
}

/* ----------
 * pgstat_read_statsfiles() -
 *
 *	Reads in some existing statistics collector files and returns the
 *	databases hash table that is the top level of the data.
 *
 *	If 'onlydb' is not InvalidOid, it means we only want data for that DB
 *	plus the shared catalogs ("DB 0").  We'll still populate the DB hash
 *	table for all databases, but we don't bother even creating table/function
 *	hash tables for other databases.
 *
 *	'permanent' specifies reading from the permanent files not temporary ones.
 *	When true (happens only when the collector is starting up), remove the
 *	files after reading; the in-memory status is now authoritative, and the
 *	files would be out of date in case somebody else reads them.
 *
 *	If a 'deep' read is requested, table/function stats are read, otherwise
 *	the table/function hash tables remain empty.
 * ----------
 */
static HTAB *
pgstat_read_statsfiles(Oid onlydb, bool permanent, bool deep)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatDBEntry dbbuf;
	HASHCTL		hash_ctl;
	HTAB	   *dbhash;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;
	int			i;

	/*
	 * The tables will live in pgStatLocalContext.
	 */
	pgstat_setup_memcxt();

	/*
	 * Create the DB hashtable
	 */
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatDBEntry);
	hash_ctl.hcxt = pgStatLocalContext;
	dbhash = hash_create("Databases hash", PGSTAT_DB_HASH_SIZE, &hash_ctl,
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Clear out global, archiver, WAL and SLRU statistics so they start from
	 * zero in case we can't load an existing statsfile.
	 */
	memset(&globalStats, 0, sizeof(globalStats));
	memset(&archiverStats, 0, sizeof(archiverStats));
	memset(&walStats, 0, sizeof(walStats));
	memset(&slruStats, 0, sizeof(slruStats));

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	globalStats.stat_reset_timestamp = GetCurrentTimestamp();
	archiverStats.stat_reset_timestamp = globalStats.stat_reset_timestamp;
	walStats.stat_reset_timestamp = globalStats.stat_reset_timestamp;

	/*
	 * Set the same reset timestamp for all SLRU items too.
	 */
	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
		slruStats[i].stat_reset_timestamp = globalStats.stat_reset_timestamp;

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return dbhash;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(&globalStats, 1, sizeof(globalStats), fpin) != sizeof(globalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&globalStats, 0, sizeof(globalStats));
		goto done;
	}

	/*
	 * In the collector, disregard the timestamp we read from the permanent
	 * stats file; we should be willing to write a temp stats file immediately
	 * upon the first request from any backend.  This only matters if the old
	 * file's timestamp is less than PGSTAT_STAT_INTERVAL ago, but that's not
	 * an unusual scenario.
	 */
	if (pgStatRunningInCollector)
		globalStats.stats_timestamp = 0;

	/*
	 * Read archiver stats struct
	 */
	if (fread(&archiverStats, 1, sizeof(archiverStats), fpin) != sizeof(archiverStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&archiverStats, 0, sizeof(archiverStats));
		goto done;
	}

	/*
	 * Read WAL stats struct
	 */
	if (fread(&walStats, 1, sizeof(walStats), fpin) != sizeof(walStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&walStats, 0, sizeof(walStats));
		goto done;
	}

	/*
	 * Read SLRU stats struct
	 */
	if (fread(slruStats, 1, sizeof(slruStats), fpin) != sizeof(slruStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&slruStats, 0, sizeof(slruStats));
		goto done;
	}

	/*
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'D'	A PgStat_StatDBEntry struct describing a database
				 * follows.
				 */
			case 'D':
				if (fread(&dbbuf, 1, offsetof(PgStat_StatDBEntry, tables),
						  fpin) != offsetof(PgStat_StatDBEntry, tables))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Add to the DB hash
				 */
				dbentry = (PgStat_StatDBEntry *) hash_search(dbhash,
															 (void *) &dbbuf.databaseid,
															 HASH_ENTER,
															 &found);
				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(dbentry, &dbbuf, sizeof(PgStat_StatDBEntry));
				dbentry->tables = NULL;
				dbentry->functions = NULL;

				/*
				 * In the collector, disregard the timestamp we read from the
				 * permanent stats file; we should be willing to write a temp
				 * stats file immediately upon the first request from any
				 * backend.
				 */
				if (pgStatRunningInCollector)
					dbentry->stats_timestamp = 0;

				/*
				 * Don't create tables/functions hashtables for uninteresting
				 * databases.
				 */
				if (onlydb != InvalidOid)
				{
					if (dbbuf.databaseid != onlydb &&
						dbbuf.databaseid != InvalidOid)
						break;
				}

				hash_ctl.keysize = sizeof(Oid);
				hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
				hash_ctl.hcxt = pgStatLocalContext;
				dbentry->tables = hash_create("Per-database table",
											  PGSTAT_TAB_HASH_SIZE,
											  &hash_ctl,
											  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

				hash_ctl.keysize = sizeof(Oid);
				hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
				hash_ctl.hcxt = pgStatLocalContext;
				dbentry->functions = hash_create("Per-database function",
												 PGSTAT_FUNCTION_HASH_SIZE,
												 &hash_ctl,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

				/*
				 * If requested, read the data from the database-specific
				 * file.  Otherwise we just leave the hashtables empty.
				 */
				if (deep)
					pgstat_read_db_statsfile(dbentry->databaseid,
											 dbentry->tables,
											 dbentry->functions,
											 permanent);

				break;

				/*
				 * 'R'	A PgStat_StatReplSlotEntry struct describing a
				 * replication slot follows.
				 */
			case 'R':
				{
					PgStat_StatReplSlotEntry slotbuf;
					PgStat_StatReplSlotEntry *slotent;

					if (fread(&slotbuf, 1, sizeof(PgStat_StatReplSlotEntry), fpin)
						!= sizeof(PgStat_StatReplSlotEntry))
					{
						ereport(pgStatRunningInCollector ? LOG : WARNING,
								(errmsg("corrupted statistics file \"%s\"",
										statfile)));
						goto done;
					}

					/* Create hash table if we don't have it already. */
					if (replSlotStatHash == NULL)
					{
						HASHCTL		hash_ctl;

						hash_ctl.keysize = sizeof(NameData);
						hash_ctl.entrysize = sizeof(PgStat_StatReplSlotEntry);
						hash_ctl.hcxt = pgStatLocalContext;
						replSlotStatHash = hash_create("Replication slots hash",
													   PGSTAT_REPLSLOT_HASH_SIZE,
													   &hash_ctl,
													   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
					}

					slotent = (PgStat_StatReplSlotEntry *) hash_search(replSlotStatHash,
																	   (void *) &slotbuf.slotname,
																	   HASH_ENTER, NULL);
					memcpy(slotent, &slotbuf, sizeof(PgStat_StatReplSlotEntry));
					break;
				}

			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	/* If requested to read the permanent file, also get rid of it. */
	if (permanent)
	{
		elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
		unlink(statfile);
	}

	return dbhash;
}


/* ----------
 * pgstat_read_db_statsfile() -
 *
 *	Reads in the existing statistics collector file for the given database,
 *	filling the passed-in tables and functions hash tables.
 *
 *	As in pgstat_read_statsfiles, if the permanent file is requested, it is
 *	removed after reading.
 *
 *	Note: this code has the ability to skip storing per-table or per-function
 *	data, if NULL is passed for the corresponding hashtable.  That's not used
 *	at the moment though.
 * ----------
 */
static void
pgstat_read_db_statsfile(Oid databaseid, HTAB *tabhash, HTAB *funchash,
						 bool permanent)
{
	PgStat_StatTabEntry *tabentry;
	PgStat_StatTabEntry tabbuf;
	PgStat_StatFuncEntry funcbuf;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	char		statfile[MAXPGPATH];

	get_dbstat_filename(permanent, false, databaseid, statfile, MAXPGPATH);

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'T'	A PgStat_StatTabEntry follows.
				 */
			case 'T':
				if (fread(&tabbuf, 1, sizeof(PgStat_StatTabEntry),
						  fpin) != sizeof(PgStat_StatTabEntry))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Skip if table data not wanted.
				 */
				if (tabhash == NULL)
					break;

				tabentry = (PgStat_StatTabEntry *) hash_search(tabhash,
															   (void *) &tabbuf.tableid,
															   HASH_ENTER, &found);

				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(tabentry, &tabbuf, sizeof(tabbuf));
				break;

				/*
				 * 'F'	A PgStat_StatFuncEntry follows.
				 */
			case 'F':
				if (fread(&funcbuf, 1, sizeof(PgStat_StatFuncEntry),
						  fpin) != sizeof(PgStat_StatFuncEntry))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Skip if function data not wanted.
				 */
				if (funchash == NULL)
					break;

				funcentry = (PgStat_StatFuncEntry *) hash_search(funchash,
																 (void *) &funcbuf.functionid,
																 HASH_ENTER, &found);

				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(funcentry, &funcbuf, sizeof(funcbuf));
				break;

				/*
				 * 'E'	The EOF marker of a complete stats file.
				 */
			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	if (permanent)
	{
		elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
		unlink(statfile);
	}
}

/* ----------
 * pgstat_read_db_statsfile_timestamp() -
 *
 *	Attempt to determine the timestamp of the last db statfile write.
 *	Returns true if successful; the timestamp is stored in *ts. The caller must
 *	rely on timestamp stored in *ts iff the function returns true.
 *
 *	This needs to be careful about handling databases for which no stats file
 *	exists, such as databases without a stat entry or those not yet written:
 *
 *	- if there's a database entry in the global file, return the corresponding
 *	stats_timestamp value.
 *
 *	- if there's no db stat entry (e.g. for a new or inactive database),
 *	there's no stats_timestamp value, but also nothing to write so we return
 *	the timestamp of the global statfile.
 * ----------
 */
static bool
pgstat_read_db_statsfile_timestamp(Oid databaseid, bool permanent,
								   TimestampTz *ts)
{
	PgStat_StatDBEntry dbentry;
	PgStat_GlobalStats myGlobalStats;
	PgStat_ArchiverStats myArchiverStats;
	PgStat_WalStats myWalStats;
	PgStat_SLRUStats mySLRUStats[SLRU_NUM_ELEMENTS];
	PgStat_StatReplSlotEntry myReplSlotStats;
	FILE	   *fpin;
	int32		format_id;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;

	/*
	 * Try to open the stats file.  As above, anything but ENOENT is worthy of
	 * complaining about.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return false;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(&myGlobalStats, 1, sizeof(myGlobalStats),
			  fpin) != sizeof(myGlobalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read archiver stats struct
	 */
	if (fread(&myArchiverStats, 1, sizeof(myArchiverStats),
			  fpin) != sizeof(myArchiverStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read WAL stats struct
	 */
	if (fread(&myWalStats, 1, sizeof(myWalStats), fpin) != sizeof(myWalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read SLRU stats struct
	 */
	if (fread(mySLRUStats, 1, sizeof(mySLRUStats), fpin) != sizeof(mySLRUStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/* By default, we're going to return the timestamp of the global file. */
	*ts = myGlobalStats.stats_timestamp;

	/*
	 * We found an existing collector stats file.  Read it and look for a
	 * record for the requested database.  If found, use its timestamp.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'D'	A PgStat_StatDBEntry struct describing a database
				 * follows.
				 */
			case 'D':
				if (fread(&dbentry, 1, offsetof(PgStat_StatDBEntry, tables),
						  fpin) != offsetof(PgStat_StatDBEntry, tables))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					FreeFile(fpin);
					return false;
				}

				/*
				 * If this is the DB we're looking for, save its timestamp and
				 * we're done.
				 */
				if (dbentry.databaseid == databaseid)
				{
					*ts = dbentry.stats_timestamp;
					goto done;
				}

				break;

				/*
				 * 'R'	A PgStat_StatReplSlotEntry struct describing a
				 * replication slot follows.
				 */
			case 'R':
				if (fread(&myReplSlotStats, 1, sizeof(PgStat_StatReplSlotEntry), fpin)
					!= sizeof(PgStat_StatReplSlotEntry))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					FreeFile(fpin);
					return false;
				}
				break;

			case 'E':
				goto done;

			default:
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					FreeFile(fpin);
					return false;
				}
		}
	}

done:
	FreeFile(fpin);
	return true;
}

/*
 * If not already done, read the statistics collector stats file into
 * some hash tables.  The results will be kept until pgstat_clear_snapshot()
 * is called (typically, at end of transaction).
 */
static void
backend_read_statsfile(void)
{
	TimestampTz min_ts = 0;
	TimestampTz ref_ts = 0;
	Oid			inquiry_db;
	int			count;

	/* already read it? */
	if (pgStatDBHash)
		return;
	Assert(!pgStatRunningInCollector);

	/*
	 * In a normal backend, we check staleness of the data for our own DB, and
	 * so we send MyDatabaseId in inquiry messages.  In the autovac launcher,
	 * check staleness of the shared-catalog data, and send InvalidOid in
	 * inquiry messages so as not to force writing unnecessary data.
	 */
	if (IsAutoVacuumLauncherProcess())
		inquiry_db = InvalidOid;
	else
		inquiry_db = MyDatabaseId;

	/*
	 * Loop until fresh enough stats file is available or we ran out of time.
	 * The stats inquiry message is sent repeatedly in case collector drops
	 * it; but not every single time, as that just swamps the collector.
	 */
	for (count = 0; count < PGSTAT_POLL_LOOP_COUNT; count++)
	{
		bool		ok;
		TimestampTz file_ts = 0;
		TimestampTz cur_ts;

		CHECK_FOR_INTERRUPTS();

		ok = pgstat_read_db_statsfile_timestamp(inquiry_db, false, &file_ts);

		cur_ts = GetCurrentTimestamp();
		/* Calculate min acceptable timestamp, if we didn't already */
		if (count == 0 || cur_ts < ref_ts)
		{
			/*
			 * We set the minimum acceptable timestamp to PGSTAT_STAT_INTERVAL
			 * msec before now.  This indirectly ensures that the collector
			 * needn't write the file more often than PGSTAT_STAT_INTERVAL. In
			 * an autovacuum worker, however, we want a lower delay to avoid
			 * using stale data, so we use PGSTAT_RETRY_DELAY (since the
			 * number of workers is low, this shouldn't be a problem).
			 *
			 * We don't recompute min_ts after sleeping, except in the
			 * unlikely case that cur_ts went backwards.  So we might end up
			 * accepting a file a bit older than PGSTAT_STAT_INTERVAL.  In
			 * practice that shouldn't happen, though, as long as the sleep
			 * time is less than PGSTAT_STAT_INTERVAL; and we don't want to
			 * tell the collector that our cutoff time is less than what we'd
			 * actually accept.
			 */
			ref_ts = cur_ts;
			if (IsAutoVacuumWorkerProcess())
				min_ts = TimestampTzPlusMilliseconds(ref_ts,
													 -PGSTAT_RETRY_DELAY);
			else
				min_ts = TimestampTzPlusMilliseconds(ref_ts,
													 -PGSTAT_STAT_INTERVAL);
		}

		/*
		 * If the file timestamp is actually newer than cur_ts, we must have
		 * had a clock glitch (system time went backwards) or there is clock
		 * skew between our processor and the stats collector's processor.
		 * Accept the file, but send an inquiry message anyway to make
		 * pgstat_recv_inquiry do a sanity check on the collector's time.
		 */
		if (ok && file_ts > cur_ts)
		{
			/*
			 * A small amount of clock skew between processors isn't terribly
			 * surprising, but a large difference is worth logging.  We
			 * arbitrarily define "large" as 1000 msec.
			 */
			if (file_ts >= TimestampTzPlusMilliseconds(cur_ts, 1000))
			{
				char	   *filetime;
				char	   *mytime;

				/* Copy because timestamptz_to_str returns a static buffer */
				filetime = pstrdup(timestamptz_to_str(file_ts));
				mytime = pstrdup(timestamptz_to_str(cur_ts));
				ereport(LOG,
						(errmsg("statistics collector's time %s is later than backend local time %s",
								filetime, mytime)));
				pfree(filetime);
				pfree(mytime);
			}

			pgstat_send_inquiry(cur_ts, min_ts, inquiry_db);
			break;
		}

		/* Normal acceptance case: file is not older than cutoff time */
		if (ok && file_ts >= min_ts)
			break;

		/* Not there or too old, so kick the collector and wait a bit */
		if ((count % PGSTAT_INQ_LOOP_COUNT) == 0)
			pgstat_send_inquiry(cur_ts, min_ts, inquiry_db);

		pg_usleep(PGSTAT_RETRY_DELAY * 1000L);
	}

	if (count >= PGSTAT_POLL_LOOP_COUNT)
		ereport(LOG,
				(errmsg("using stale statistics instead of current ones "
						"because stats collector is not responding")));

	/*
	 * Autovacuum launcher wants stats about all databases, but a shallow read
	 * is sufficient.  Regular backends want a deep read for just the tables
	 * they can see (MyDatabaseId + shared catalogs).
	 */
	if (IsAutoVacuumLauncherProcess())
		pgStatDBHash = pgstat_read_statsfiles(InvalidOid, false, false);
	else
		pgStatDBHash = pgstat_read_statsfiles(MyDatabaseId, false, true);
}


/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatLocalContext, if not already done.
 * ----------
 */
static void
pgstat_setup_memcxt(void)
{
	if (!pgStatLocalContext)
		pgStatLocalContext = AllocSetContextCreate(TopMemoryContext,
												   "Statistics snapshot",
												   ALLOCSET_SMALL_SIZES);
}


/* ----------
 * pgstat_clear_snapshot() -
 *
 *	Discard any data collected in the current transaction.  Any subsequent
 *	request will cause new snapshots to be read.
 *
 *	This is also invoked during transaction commit or abort to discard
 *	the no-longer-wanted snapshot.
 * ----------
 */
void
pgstat_clear_snapshot(void)
{
	/* Release memory, if any was allocated */
	if (pgStatLocalContext)
		MemoryContextDelete(pgStatLocalContext);

	/* Reset variables */
	pgStatLocalContext = NULL;
	pgStatDBHash = NULL;
	replSlotStatHash = NULL;

	/*
	 * Historically the backend_status.c facilities lived in this file, and
	 * were reset with the same function. For now keep it that way, and
	 * forward the reset request.
	 */
	pgstat_clear_backend_activity_snapshot();
}


/* ----------
 * pgstat_recv_inquiry() -
 *
 *	Process stat inquiry requests.
 * ----------
 */
static void
pgstat_recv_inquiry(PgStat_MsgInquiry *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	elog(DEBUG2, "received inquiry for database %u", msg->databaseid);

	/*
	 * If there's already a write request for this DB, there's nothing to do.
	 *
	 * Note that if a request is found, we return early and skip the below
	 * check for clock skew.  This is okay, since the only way for a DB
	 * request to be present in the list is that we have been here since the
	 * last write round.  It seems sufficient to check for clock skew once per
	 * write round.
	 */
	if (list_member_oid(pending_write_requests, msg->databaseid))
		return;

	/*
	 * Check to see if we last wrote this database at a time >= the requested
	 * cutoff time.  If so, this is a stale request that was generated before
	 * we updated the DB file, and we don't need to do so again.
	 *
	 * If the requestor's local clock time is older than stats_timestamp, we
	 * should suspect a clock glitch, ie system time going backwards; though
	 * the more likely explanation is just delayed message receipt.  It is
	 * worth expending a GetCurrentTimestamp call to be sure, since a large
	 * retreat in the system clock reading could otherwise cause us to neglect
	 * to update the stats file for a long time.
	 */
	dbentry = pgstat_get_db_entry(msg->databaseid, false);
	if (dbentry == NULL)
	{
		/*
		 * We have no data for this DB.  Enter a write request anyway so that
		 * the global stats will get updated.  This is needed to prevent
		 * backend_read_statsfile from waiting for data that we cannot supply,
		 * in the case of a new DB that nobody has yet reported any stats for.
		 * See the behavior of pgstat_read_db_statsfile_timestamp.
		 */
	}
	else if (msg->clock_time < dbentry->stats_timestamp)
	{
		TimestampTz cur_ts = GetCurrentTimestamp();

		if (cur_ts < dbentry->stats_timestamp)
		{
			/*
			 * Sure enough, time went backwards.  Force a new stats file write
			 * to get back in sync; but first, log a complaint.
			 */
			char	   *writetime;
			char	   *mytime;

			/* Copy because timestamptz_to_str returns a static buffer */
			writetime = pstrdup(timestamptz_to_str(dbentry->stats_timestamp));
			mytime = pstrdup(timestamptz_to_str(cur_ts));
			ereport(LOG,
					(errmsg("stats_timestamp %s is later than collector's time %s for database %u",
							writetime, mytime, dbentry->databaseid)));
			pfree(writetime);
			pfree(mytime);
		}
		else
		{
			/*
			 * Nope, it's just an old request.  Assuming msg's clock_time is
			 * >= its cutoff_time, it must be stale, so we can ignore it.
			 */
			return;
		}
	}
	else if (msg->cutoff_time <= dbentry->stats_timestamp)
	{
		/* Stale request, ignore it */
		return;
	}

	/*
	 * We need to write this DB, so create a request.
	 */
	pending_write_requests = lappend_oid(pending_write_requests,
										 msg->databaseid);
}


/* ----------
 * pgstat_recv_tabstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void
pgstat_recv_tabstat(PgStat_MsgTabstat *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	int			i;
	bool		found;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	/*
	 * Update database-wide stats.
	 */
	dbentry->n_xact_commit += (PgStat_Counter) (msg->m_xact_commit);
	dbentry->n_xact_rollback += (PgStat_Counter) (msg->m_xact_rollback);
	dbentry->n_block_read_time += msg->m_block_read_time;
	dbentry->n_block_write_time += msg->m_block_write_time;

	dbentry->total_session_time += msg->m_session_time;
	dbentry->total_active_time += msg->m_active_time;
	dbentry->total_idle_in_xact_time += msg->m_idle_in_xact_time;

	/*
	 * Process all table entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		PgStat_TableEntry *tabmsg = &(msg->m_entry[i]);

		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &(tabmsg->t_id),
													   HASH_ENTER, &found);

		if (!found)
		{
			/*
			 * If it's a new table entry, initialize counters to the values we
			 * just got.
			 */
			tabentry->numscans = tabmsg->t_counts.t_numscans;
			tabentry->tuples_returned = tabmsg->t_counts.t_tuples_returned;
			tabentry->tuples_fetched = tabmsg->t_counts.t_tuples_fetched;
			tabentry->tuples_inserted = tabmsg->t_counts.t_tuples_inserted;
			tabentry->tuples_updated = tabmsg->t_counts.t_tuples_updated;
			tabentry->tuples_deleted = tabmsg->t_counts.t_tuples_deleted;
			tabentry->tuples_hot_updated = tabmsg->t_counts.t_tuples_hot_updated;
			tabentry->n_live_tuples = tabmsg->t_counts.t_delta_live_tuples;
			tabentry->n_dead_tuples = tabmsg->t_counts.t_delta_dead_tuples;
			tabentry->changes_since_analyze = tabmsg->t_counts.t_changed_tuples;
			tabentry->inserts_since_vacuum = tabmsg->t_counts.t_tuples_inserted;
			tabentry->blocks_fetched = tabmsg->t_counts.t_blocks_fetched;
			tabentry->blocks_hit = tabmsg->t_counts.t_blocks_hit;

			tabentry->vacuum_timestamp = 0;
			tabentry->vacuum_count = 0;
			tabentry->autovac_vacuum_timestamp = 0;
			tabentry->autovac_vacuum_count = 0;
			tabentry->analyze_timestamp = 0;
			tabentry->analyze_count = 0;
			tabentry->autovac_analyze_timestamp = 0;
			tabentry->autovac_analyze_count = 0;
		}
		else
		{
			/*
			 * Otherwise add the values to the existing entry.
			 */
			tabentry->numscans += tabmsg->t_counts.t_numscans;
			tabentry->tuples_returned += tabmsg->t_counts.t_tuples_returned;
			tabentry->tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
			tabentry->tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
			tabentry->tuples_updated += tabmsg->t_counts.t_tuples_updated;
			tabentry->tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
			tabentry->tuples_hot_updated += tabmsg->t_counts.t_tuples_hot_updated;
			/* If table was truncated, first reset the live/dead counters */
			if (tabmsg->t_counts.t_truncated)
			{
				tabentry->n_live_tuples = 0;
				tabentry->n_dead_tuples = 0;
				tabentry->inserts_since_vacuum = 0;
			}
			tabentry->n_live_tuples += tabmsg->t_counts.t_delta_live_tuples;
			tabentry->n_dead_tuples += tabmsg->t_counts.t_delta_dead_tuples;
			tabentry->changes_since_analyze += tabmsg->t_counts.t_changed_tuples;
			tabentry->inserts_since_vacuum += tabmsg->t_counts.t_tuples_inserted;
			tabentry->blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
			tabentry->blocks_hit += tabmsg->t_counts.t_blocks_hit;
		}

		/* Clamp n_live_tuples in case of negative delta_live_tuples */
		tabentry->n_live_tuples = Max(tabentry->n_live_tuples, 0);
		/* Likewise for n_dead_tuples */
		tabentry->n_dead_tuples = Max(tabentry->n_dead_tuples, 0);

		/*
		 * Add per-table stats to the per-database entry, too.
		 */
		dbentry->n_tuples_returned += tabmsg->t_counts.t_tuples_returned;
		dbentry->n_tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
		dbentry->n_tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
		dbentry->n_tuples_updated += tabmsg->t_counts.t_tuples_updated;
		dbentry->n_tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
		dbentry->n_blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
		dbentry->n_blocks_hit += tabmsg->t_counts.t_blocks_hit;
	}
}


/* ----------
 * pgstat_recv_tabpurge() -
 *
 *	Arrange for dead table removal.
 * ----------
 */
static void
pgstat_recv_tabpurge(PgStat_MsgTabpurge *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	int			i;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || !dbentry->tables)
		return;

	/*
	 * Process all table entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		/* Remove from hashtable if present; we don't care if it's not. */
		(void) hash_search(dbentry->tables,
						   (void *) &(msg->m_tableid[i]),
						   HASH_REMOVE, NULL);
	}
}


/* ----------
 * pgstat_recv_dropdb() -
 *
 *	Arrange for dead database removal
 * ----------
 */
static void
pgstat_recv_dropdb(PgStat_MsgDropdb *msg, int len)
{
	Oid			dbid = msg->m_databaseid;
	PgStat_StatDBEntry *dbentry;

	/*
	 * Lookup the database in the hashtable.
	 */
	dbentry = pgstat_get_db_entry(dbid, false);

	/*
	 * If found, remove it (along with the db statfile).
	 */
	if (dbentry)
	{
		char		statfile[MAXPGPATH];

		get_dbstat_filename(false, false, dbid, statfile, MAXPGPATH);

		elog(DEBUG2, "removing stats file \"%s\"", statfile);
		unlink(statfile);

		if (dbentry->tables != NULL)
			hash_destroy(dbentry->tables);
		if (dbentry->functions != NULL)
			hash_destroy(dbentry->functions);

		if (hash_search(pgStatDBHash,
						(void *) &dbid,
						HASH_REMOVE, NULL) == NULL)
			ereport(ERROR,
					(errmsg("database hash table corrupted during cleanup --- abort")));
	}
}


/* ----------
 * pgstat_recv_resetcounter() -
 *
 *	Reset the statistics for the specified database.
 * ----------
 */
static void
pgstat_recv_resetcounter(PgStat_MsgResetcounter *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	/*
	 * Lookup the database in the hashtable.  Nothing to do if not there.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	if (!dbentry)
		return;

	/*
	 * We simply throw away all the database's table entries by recreating a
	 * new hash table for them.
	 */
	if (dbentry->tables != NULL)
		hash_destroy(dbentry->tables);
	if (dbentry->functions != NULL)
		hash_destroy(dbentry->functions);

	dbentry->tables = NULL;
	dbentry->functions = NULL;

	/*
	 * Reset database-level stats, too.  This creates empty hash tables for
	 * tables and functions.
	 */
	reset_dbentry_counters(dbentry);
}

/* ----------
 * pgstat_recv_resetsharedcounter() -
 *
 *	Reset some shared statistics of the cluster.
 * ----------
 */
static void
pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter *msg, int len)
{
	if (msg->m_resettarget == RESET_BGWRITER)
	{
		/* Reset the global background writer statistics for the cluster. */
		memset(&globalStats, 0, sizeof(globalStats));
		globalStats.stat_reset_timestamp = GetCurrentTimestamp();
	}
	else if (msg->m_resettarget == RESET_ARCHIVER)
	{
		/* Reset the archiver statistics for the cluster. */
		memset(&archiverStats, 0, sizeof(archiverStats));
		archiverStats.stat_reset_timestamp = GetCurrentTimestamp();
	}
	else if (msg->m_resettarget == RESET_WAL)
	{
		/* Reset the WAL statistics for the cluster. */
		memset(&walStats, 0, sizeof(walStats));
		walStats.stat_reset_timestamp = GetCurrentTimestamp();
	}

	/*
	 * Presumably the sender of this message validated the target, don't
	 * complain here if it's not valid
	 */
}

/* ----------
 * pgstat_recv_resetsinglecounter() -
 *
 *	Reset a statistics for a single object
 * ----------
 */
static void
pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	if (!dbentry)
		return;

	/* Set the reset timestamp for the whole database */
	dbentry->stat_reset_timestamp = GetCurrentTimestamp();

	/* Remove object if it exists, ignore it if not */
	if (msg->m_resettype == RESET_TABLE)
		(void) hash_search(dbentry->tables, (void *) &(msg->m_objectid),
						   HASH_REMOVE, NULL);
	else if (msg->m_resettype == RESET_FUNCTION)
		(void) hash_search(dbentry->functions, (void *) &(msg->m_objectid),
						   HASH_REMOVE, NULL);
}

/* ----------
 * pgstat_recv_resetslrucounter() -
 *
 *	Reset some SLRU statistics of the cluster.
 * ----------
 */
static void
pgstat_recv_resetslrucounter(PgStat_MsgResetslrucounter *msg, int len)
{
	int			i;
	TimestampTz ts = GetCurrentTimestamp();

	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		/* reset entry with the given index, or all entries (index is -1) */
		if ((msg->m_index == -1) || (msg->m_index == i))
		{
			memset(&slruStats[i], 0, sizeof(slruStats[i]));
			slruStats[i].stat_reset_timestamp = ts;
		}
	}
}

/* ----------
 * pgstat_recv_resetreplslotcounter() -
 *
 *	Reset some replication slot statistics of the cluster.
 * ----------
 */
static void
pgstat_recv_resetreplslotcounter(PgStat_MsgResetreplslotcounter *msg,
								 int len)
{
	PgStat_StatReplSlotEntry *slotent;
	TimestampTz ts;

	/* Return if we don't have replication slot statistics */
	if (replSlotStatHash == NULL)
		return;

	ts = GetCurrentTimestamp();
	if (msg->clearall)
	{
		HASH_SEQ_STATUS sstat;

		hash_seq_init(&sstat, replSlotStatHash);
		while ((slotent = (PgStat_StatReplSlotEntry *) hash_seq_search(&sstat)) != NULL)
			pgstat_reset_replslot(slotent, ts);
	}
	else
	{
		/* Get the slot statistics to reset */
		slotent = pgstat_get_replslot_entry(msg->m_slotname, false);

		/*
		 * Nothing to do if the given slot entry is not found.  This could
		 * happen when the slot with the given name is removed and the
		 * corresponding statistics entry is also removed before receiving the
		 * reset message.
		 */
		if (!slotent)
			return;

		/* Reset the stats for the requested replication slot */
		pgstat_reset_replslot(slotent, ts);
	}
}


/* ----------
 * pgstat_recv_autovac() -
 *
 *	Process an autovacuum signaling message.
 * ----------
 */
static void
pgstat_recv_autovac(PgStat_MsgAutovacStart *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	/*
	 * Store the last autovacuum time in the database's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->last_autovac_time = msg->m_start_time;
}

/* ----------
 * pgstat_recv_vacuum() -
 *
 *	Process a VACUUM message.
 * ----------
 */
static void
pgstat_recv_vacuum(PgStat_MsgVacuum *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true);

	tabentry->n_live_tuples = msg->m_live_tuples;
	tabentry->n_dead_tuples = msg->m_dead_tuples;

	/*
	 * It is quite possible that a non-aggressive VACUUM ended up skipping
	 * various pages, however, we'll zero the insert counter here regardless.
	 * It's currently used only to track when we need to perform an "insert"
	 * autovacuum, which are mainly intended to freeze newly inserted tuples.
	 * Zeroing this may just mean we'll not try to vacuum the table again
	 * until enough tuples have been inserted to trigger another insert
	 * autovacuum.  An anti-wraparound autovacuum will catch any persistent
	 * stragglers.
	 */
	tabentry->inserts_since_vacuum = 0;

	if (msg->m_autovacuum)
	{
		tabentry->autovac_vacuum_timestamp = msg->m_vacuumtime;
		tabentry->autovac_vacuum_count++;
	}
	else
	{
		tabentry->vacuum_timestamp = msg->m_vacuumtime;
		tabentry->vacuum_count++;
	}
}

/* ----------
 * pgstat_recv_analyze() -
 *
 *	Process an ANALYZE message.
 * ----------
 */
static void
pgstat_recv_analyze(PgStat_MsgAnalyze *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true);

	tabentry->n_live_tuples = msg->m_live_tuples;
	tabentry->n_dead_tuples = msg->m_dead_tuples;

	/*
	 * If commanded, reset changes_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (msg->m_resetcounter)
		tabentry->changes_since_analyze = 0;

	if (msg->m_autovacuum)
	{
		tabentry->autovac_analyze_timestamp = msg->m_analyzetime;
		tabentry->autovac_analyze_count++;
	}
	else
	{
		tabentry->analyze_timestamp = msg->m_analyzetime;
		tabentry->analyze_count++;
	}
}


/* ----------
 * pgstat_recv_archiver() -
 *
 *	Process a ARCHIVER message.
 * ----------
 */
static void
pgstat_recv_archiver(PgStat_MsgArchiver *msg, int len)
{
	if (msg->m_failed)
	{
		/* Failed archival attempt */
		++archiverStats.failed_count;
		memcpy(archiverStats.last_failed_wal, msg->m_xlog,
			   sizeof(archiverStats.last_failed_wal));
		archiverStats.last_failed_timestamp = msg->m_timestamp;
	}
	else
	{
		/* Successful archival operation */
		++archiverStats.archived_count;
		memcpy(archiverStats.last_archived_wal, msg->m_xlog,
			   sizeof(archiverStats.last_archived_wal));
		archiverStats.last_archived_timestamp = msg->m_timestamp;
	}
}

/* ----------
 * pgstat_recv_bgwriter() -
 *
 *	Process a BGWRITER message.
 * ----------
 */
static void
pgstat_recv_bgwriter(PgStat_MsgBgWriter *msg, int len)
{
	globalStats.timed_checkpoints += msg->m_timed_checkpoints;
	globalStats.requested_checkpoints += msg->m_requested_checkpoints;
	globalStats.checkpoint_write_time += msg->m_checkpoint_write_time;
	globalStats.checkpoint_sync_time += msg->m_checkpoint_sync_time;
	globalStats.buf_written_checkpoints += msg->m_buf_written_checkpoints;
	globalStats.buf_written_clean += msg->m_buf_written_clean;
	globalStats.maxwritten_clean += msg->m_maxwritten_clean;
	globalStats.buf_written_backend += msg->m_buf_written_backend;
	globalStats.buf_fsync_backend += msg->m_buf_fsync_backend;
	globalStats.buf_alloc += msg->m_buf_alloc;
}

/* ----------
 * pgstat_recv_wal() -
 *
 *	Process a WAL message.
 * ----------
 */
static void
pgstat_recv_wal(PgStat_MsgWal *msg, int len)
{
	walStats.wal_records += msg->m_wal_records;
	walStats.wal_fpi += msg->m_wal_fpi;
	walStats.wal_bytes += msg->m_wal_bytes;
	walStats.wal_buffers_full += msg->m_wal_buffers_full;
	walStats.wal_write += msg->m_wal_write;
	walStats.wal_sync += msg->m_wal_sync;
	walStats.wal_write_time += msg->m_wal_write_time;
	walStats.wal_sync_time += msg->m_wal_sync_time;
}

/* ----------
 * pgstat_recv_slru() -
 *
 *	Process a SLRU message.
 * ----------
 */
static void
pgstat_recv_slru(PgStat_MsgSLRU *msg, int len)
{
	slruStats[msg->m_index].blocks_zeroed += msg->m_blocks_zeroed;
	slruStats[msg->m_index].blocks_hit += msg->m_blocks_hit;
	slruStats[msg->m_index].blocks_read += msg->m_blocks_read;
	slruStats[msg->m_index].blocks_written += msg->m_blocks_written;
	slruStats[msg->m_index].blocks_exists += msg->m_blocks_exists;
	slruStats[msg->m_index].flush += msg->m_flush;
	slruStats[msg->m_index].truncate += msg->m_truncate;
}

/* ----------
 * pgstat_recv_recoveryconflict() -
 *
 *	Process a RECOVERYCONFLICT message.
 * ----------
 */
static void
pgstat_recv_recoveryconflict(PgStat_MsgRecoveryConflict *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	switch (msg->m_reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			dbentry->n_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			dbentry->n_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			dbentry->n_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			dbentry->n_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			dbentry->n_conflict_startup_deadlock++;
			break;
	}
}

/* ----------
 * pgstat_recv_deadlock() -
 *
 *	Process a DEADLOCK message.
 * ----------
 */
static void
pgstat_recv_deadlock(PgStat_MsgDeadlock *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->n_deadlocks++;
}

/* ----------
 * pgstat_recv_checksum_failure() -
 *
 *	Process a CHECKSUMFAILURE message.
 * ----------
 */
static void
pgstat_recv_checksum_failure(PgStat_MsgChecksumFailure *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->n_checksum_failures += msg->m_failurecount;
	dbentry->last_checksum_failure = msg->m_failure_time;
}

/* ----------
 * pgstat_recv_replslot() -
 *
 *	Process a REPLSLOT message.
 * ----------
 */
static void
pgstat_recv_replslot(PgStat_MsgReplSlot *msg, int len)
{
	if (msg->m_drop)
	{
		Assert(!msg->m_create);

		/* Remove the replication slot statistics with the given name */
		if (replSlotStatHash != NULL)
			(void) hash_search(replSlotStatHash,
							   (void *) &(msg->m_slotname),
							   HASH_REMOVE,
							   NULL);
	}
	else
	{
		PgStat_StatReplSlotEntry *slotent;

		slotent = pgstat_get_replslot_entry(msg->m_slotname, true);
		Assert(slotent);

		if (msg->m_create)
		{
			/*
			 * If the message for dropping the slot with the same name gets
			 * lost, slotent has stats for the old slot. So we initialize all
			 * counters at slot creation.
			 */
			pgstat_reset_replslot(slotent, 0);
		}
		else
		{
			/* Update the replication slot statistics */
			slotent->spill_txns += msg->m_spill_txns;
			slotent->spill_count += msg->m_spill_count;
			slotent->spill_bytes += msg->m_spill_bytes;
			slotent->stream_txns += msg->m_stream_txns;
			slotent->stream_count += msg->m_stream_count;
			slotent->stream_bytes += msg->m_stream_bytes;
			slotent->total_txns += msg->m_total_txns;
			slotent->total_bytes += msg->m_total_bytes;
		}
	}
}

/* ----------
 * pgstat_recv_connect() -
 *
 *	Process a CONNECT message.
 * ----------
 */
static void
pgstat_recv_connect(PgStat_MsgConnect *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
	dbentry->n_sessions++;
}

/* ----------
 * pgstat_recv_disconnect() -
 *
 *	Process a DISCONNECT message.
 * ----------
 */
static void
pgstat_recv_disconnect(PgStat_MsgDisconnect *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	switch (msg->m_cause)
	{
		case DISCONNECT_NOT_YET:
		case DISCONNECT_NORMAL:
			/* we don't collect these */
			break;
		case DISCONNECT_CLIENT_EOF:
			dbentry->n_sessions_abandoned++;
			break;
		case DISCONNECT_FATAL:
			dbentry->n_sessions_fatal++;
			break;
		case DISCONNECT_KILLED:
			dbentry->n_sessions_killed++;
			break;
	}
}

/* ----------
 * pgstat_recv_tempfile() -
 *
 *	Process a TEMPFILE message.
 * ----------
 */
static void
pgstat_recv_tempfile(PgStat_MsgTempFile *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->n_temp_bytes += msg->m_filesize;
	dbentry->n_temp_files += 1;
}

/* ----------
 * pgstat_recv_funcstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void
pgstat_recv_funcstat(PgStat_MsgFuncstat *msg, int len)
{
	PgStat_FunctionEntry *funcmsg = &(msg->m_entry[0]);
	PgStat_StatDBEntry *dbentry;
	PgStat_StatFuncEntry *funcentry;
	int			i;
	bool		found;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	/*
	 * Process all function entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++, funcmsg++)
	{
		funcentry = (PgStat_StatFuncEntry *) hash_search(dbentry->functions,
														 (void *) &(funcmsg->f_id),
														 HASH_ENTER, &found);

		if (!found)
		{
			/*
			 * If it's a new function entry, initialize counters to the values
			 * we just got.
			 */
			funcentry->f_numcalls = funcmsg->f_numcalls;
			funcentry->f_total_time = funcmsg->f_total_time;
			funcentry->f_self_time = funcmsg->f_self_time;
		}
		else
		{
			/*
			 * Otherwise add the values to the existing entry.
			 */
			funcentry->f_numcalls += funcmsg->f_numcalls;
			funcentry->f_total_time += funcmsg->f_total_time;
			funcentry->f_self_time += funcmsg->f_self_time;
		}
	}
}

/* ----------
 * pgstat_recv_funcpurge() -
 *
 *	Arrange for dead function removal.
 * ----------
 */
static void
pgstat_recv_funcpurge(PgStat_MsgFuncpurge *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	int			i;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || !dbentry->functions)
		return;

	/*
	 * Process all function entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		/* Remove from hashtable if present; we don't care if it's not. */
		(void) hash_search(dbentry->functions,
						   (void *) &(msg->m_functionid[i]),
						   HASH_REMOVE, NULL);
	}
}

/* ----------
 * pgstat_write_statsfile_needed() -
 *
 *	Do we need to write out any stats files?
 * ----------
 */
static bool
pgstat_write_statsfile_needed(void)
{
	if (pending_write_requests != NIL)
		return true;

	/* Everything was written recently */
	return false;
}

/* ----------
 * pgstat_db_requested() -
 *
 *	Checks whether stats for a particular DB need to be written to a file.
 * ----------
 */
static bool
pgstat_db_requested(Oid databaseid)
{
	/*
	 * If any requests are outstanding at all, we should write the stats for
	 * shared catalogs (the "database" with OID 0).  This ensures that
	 * backends will see up-to-date stats for shared catalogs, even though
	 * they send inquiry messages mentioning only their own DB.
	 */
	if (databaseid == InvalidOid && pending_write_requests != NIL)
		return true;

	/* Search to see if there's an open request to write this database. */
	if (list_member_oid(pending_write_requests, databaseid))
		return true;

	return false;
}

/* ----------
 * pgstat_replslot_entry
 *
 * Return the entry of replication slot stats with the given name. Return
 * NULL if not found and the caller didn't request to create it.
 *
 * create tells whether to create the new slot entry if it is not found.
 * ----------
 */
static PgStat_StatReplSlotEntry *
pgstat_get_replslot_entry(NameData name, bool create)
{
	PgStat_StatReplSlotEntry *slotent;
	bool		found;

	if (replSlotStatHash == NULL)
	{
		HASHCTL		hash_ctl;

		/*
		 * Quick return NULL if the hash table is empty and the caller didn't
		 * request to create the entry.
		 */
		if (!create)
			return NULL;

		hash_ctl.keysize = sizeof(NameData);
		hash_ctl.entrysize = sizeof(PgStat_StatReplSlotEntry);
		replSlotStatHash = hash_create("Replication slots hash",
									   PGSTAT_REPLSLOT_HASH_SIZE,
									   &hash_ctl,
									   HASH_ELEM | HASH_BLOBS);
	}

	slotent = (PgStat_StatReplSlotEntry *) hash_search(replSlotStatHash,
													   (void *) &name,
													   create ? HASH_ENTER : HASH_FIND,
													   &found);

	if (!slotent)
	{
		/* not found */
		Assert(!create && !found);
		return NULL;
	}

	/* initialize the entry */
	if (create && !found)
	{
		namestrcpy(&(slotent->slotname), NameStr(name));
		pgstat_reset_replslot(slotent, 0);
	}

	return slotent;
}

/* ----------
 * pgstat_reset_replslot
 *
 * Reset the given replication slot stats.
 * ----------
 */
static void
pgstat_reset_replslot(PgStat_StatReplSlotEntry *slotent, TimestampTz ts)
{
	/* reset only counters. Don't clear slot name */
	slotent->spill_txns = 0;
	slotent->spill_count = 0;
	slotent->spill_bytes = 0;
	slotent->stream_txns = 0;
	slotent->stream_count = 0;
	slotent->stream_bytes = 0;
	slotent->total_txns = 0;
	slotent->total_bytes = 0;
	slotent->stat_reset_timestamp = ts;
}

/*
 * pgstat_slru_index
 *
 * Determine index of entry for a SLRU with a given name. If there's no exact
 * match, returns index of the last "other" entry used for SLRUs defined in
 * external projects.
 */
int
pgstat_slru_index(const char *name)
{
	int			i;

	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		if (strcmp(slru_names[i], name) == 0)
			return i;
	}

	/* return index of the last entry (which is the "other" one) */
	return (SLRU_NUM_ELEMENTS - 1);
}

/*
 * pgstat_slru_name
 *
 * Returns SLRU name for an index. The index may be above SLRU_NUM_ELEMENTS,
 * in which case this returns NULL. This allows writing code that does not
 * know the number of entries in advance.
 */
const char *
pgstat_slru_name(int slru_idx)
{
	if (slru_idx < 0 || slru_idx >= SLRU_NUM_ELEMENTS)
		return NULL;

	return slru_names[slru_idx];
}

/*
 * slru_entry
 *
 * Returns pointer to entry with counters for given SLRU (based on the name
 * stored in SlruCtl as lwlock tranche name).
 */
static inline PgStat_MsgSLRU *
slru_entry(int slru_idx)
{
	/*
	 * The postmaster should never register any SLRU statistics counts; if it
	 * did, the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

	return &SLRUStats[slru_idx];
}

/*
 * SLRU statistics count accumulation functions --- called from slru.c
 */

void
pgstat_count_slru_page_zeroed(int slru_idx)
{
	slru_entry(slru_idx)->m_blocks_zeroed += 1;
}

void
pgstat_count_slru_page_hit(int slru_idx)
{
	slru_entry(slru_idx)->m_blocks_hit += 1;
}

void
pgstat_count_slru_page_exists(int slru_idx)
{
	slru_entry(slru_idx)->m_blocks_exists += 1;
}

void
pgstat_count_slru_page_read(int slru_idx)
{
	slru_entry(slru_idx)->m_blocks_read += 1;
}

void
pgstat_count_slru_page_written(int slru_idx)
{
	slru_entry(slru_idx)->m_blocks_written += 1;
}

void
pgstat_count_slru_flush(int slru_idx)
{
	slru_entry(slru_idx)->m_flush += 1;
}

void
pgstat_count_slru_truncate(int slru_idx)
{
	slru_entry(slru_idx)->m_truncate += 1;
}
