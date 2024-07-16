/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/file_perm.h"
#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"


__thread ProtocolVersion FrontendProtocol;

__thread volatile sig_atomic_t InterruptPending = false;
__thread volatile sig_atomic_t QueryCancelPending = false;
__thread volatile sig_atomic_t ProcDiePending = false;
__thread volatile sig_atomic_t CheckClientConnectionPending = false;
__thread volatile sig_atomic_t ClientConnectionLost = false;
__thread volatile sig_atomic_t IdleInTransactionSessionTimeoutPending = false;
__thread volatile sig_atomic_t IdleSessionTimeoutPending = false;
__thread volatile sig_atomic_t ProcSignalBarrierPending = false;
__thread volatile sig_atomic_t LogMemoryContextPending = false;
__thread volatile sig_atomic_t IdleStatsUpdateTimeoutPending = false;
__thread volatile uint32 InterruptHoldoffCount = 0;
__thread volatile uint32 QueryCancelHoldoffCount = 0;
__thread volatile uint32 CritSectionCount = 0;

__thread int			MyProcPid;
__thread pg_time_t	MyStartTime;
__thread TimestampTz MyStartTimestamp;
__thread struct Port *MyProcPort;
__thread int32		MyCancelKey;
__thread int			MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
__thread struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
__thread char	   *DataDir = NULL;

/*
 * Mode of the data directory.  The default is 0700 but it may be changed in
 * checkDataDir() to 0750 if the data directory actually has that mode.
 */
__thread int			data_directory_mode = PG_DIR_MODE_OWNER;

__thread char		OutputFileName[MAXPGPATH];	/* debugging output file */

__thread char		my_exec_path[MAXPGPATH];	/* full path to my executable */
__thread char		pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

__thread BackendId	MyBackendId = InvalidBackendId;

__thread BackendId	ParallelLeaderBackendId = InvalidBackendId;

__thread Oid			MyDatabaseId = InvalidOid;

__thread Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
__thread char	   *DatabasePath = NULL;

__thread pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
__thread bool		IsPostmasterEnvironment = false;
__thread bool		IsUnderPostmaster = false;
__thread bool		IsBinaryUpgrade = false;
__thread bool		IsBackgroundWorker = false;

__thread bool		ExitOnAnyError = false;

__thread int			DateStyle = USE_ISO_DATES;
__thread int			DateOrder = DATEORDER_MDY;
__thread int			IntervalStyle = INTSTYLE_POSTGRES;

__thread bool		enableFsync = true;
__thread bool		allowSystemTableMods = false;
__thread int			work_mem = 4096;
__thread double		hash_mem_multiplier = 2.0;
__thread int			maintenance_work_mem = 65536;
__thread int			max_parallel_maintenance_workers = 2;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
__thread int			NBuffers = 16384;
__thread int			MaxConnections = 100;
__thread int			max_worker_processes = 8;
__thread int			max_parallel_workers = 8;
__thread int			MaxBackends = 0;

/* GUC parameters for vacuum */
__thread int			VacuumBufferUsageLimit = 256;

__thread int			VacuumCostPageHit = 1;
__thread int			VacuumCostPageMiss = 2;
__thread int			VacuumCostPageDirty = 20;
__thread int			VacuumCostLimit = 200;
__thread double		VacuumCostDelay = 0;

__thread int64		VacuumPageHit = 0;
__thread int64		VacuumPageMiss = 0;
__thread int64		VacuumPageDirty = 0;

__thread int			VacuumCostBalance = 0;	/* working state for vacuum */
__thread bool		VacuumCostActive = false;
