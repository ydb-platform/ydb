#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

static struct uwsgi_lock_item *uwsgi_register_lock(char *id, int rw) {

	struct uwsgi_lock_item *uli = uwsgi.registered_locks;
	if (!uli) {
		uwsgi.registered_locks = uwsgi_malloc_shared(sizeof(struct uwsgi_lock_item));
		uwsgi.registered_locks->id = id;
		uwsgi.registered_locks->pid = 0;
		if (rw) {
			uwsgi.registered_locks->lock_ptr = uwsgi_malloc_shared(uwsgi.rwlock_size);
		}
		else {
			uwsgi.registered_locks->lock_ptr = uwsgi_malloc_shared(uwsgi.lock_size);
		}
		uwsgi.registered_locks->rw = rw;
		uwsgi.registered_locks->next = NULL;
		return uwsgi.registered_locks;
	}

	while (uli) {
		if (!uli->next) {
			uli->next = uwsgi_malloc_shared(sizeof(struct uwsgi_lock_item));
			if (rw) {
				uli->next->lock_ptr = uwsgi_malloc_shared(uwsgi.rwlock_size);
			}
			else {
				uli->next->lock_ptr = uwsgi_malloc_shared(uwsgi.lock_size);
			}
			uli->next->id = id;
			uli->next->pid = 0;
			uli->next->rw = rw;
			uli->next->next = NULL;
			return uli->next;
		}
		uli = uli->next;
	}

	uwsgi_log("*** DANGER: unable to allocate lock %s ***\n", id);
	exit(1);

}

#ifdef UWSGI_LOCK_USE_MUTEX

#ifdef OBSOLETE_LINUX_KERNEL
#undef EOWNERDEAD
#endif

#ifdef EOWNERDEAD
#define UWSGI_LOCK_ENGINE_NAME "pthread robust mutexes"
int uwsgi_pthread_robust_mutexes_enabled = 1;
#else
#define UWSGI_LOCK_ENGINE_NAME "pthread mutexes"
#endif

#define UWSGI_LOCK_SIZE	sizeof(pthread_mutex_t)

#ifdef OBSOLETE_LINUX_KERNEL
#define UWSGI_RWLOCK_SIZE	sizeof(pthread_mutex_t)
#else
#define UWSGI_RWLOCK_SIZE	sizeof(pthread_rwlock_t)
#endif

#ifndef PTHREAD_PRIO_INHERIT
int pthread_mutexattr_setprotocol (pthread_mutexattr_t *__attr,
                                          int __protocol);
#define PTHREAD_PRIO_INHERIT 1
#endif

// REMEMBER lock must contains space for both pthread_mutex_t and pthread_mutexattr_t !!! 
struct uwsgi_lock_item *uwsgi_lock_fast_init(char *id) {

	pthread_mutexattr_t attr;

	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);

#ifdef EOWNERDEAD
retry:
#endif
	if (pthread_mutexattr_init(&attr)) {
		uwsgi_log("unable to allocate mutexattr structure\n");
		exit(1);
	}

	if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
		uwsgi_log("unable to share mutex\n");
		exit(1);
	}

#ifdef EOWNERDEAD
	if (uwsgi_pthread_robust_mutexes_enabled) {
		int ret;
		if ((ret = pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT)) != 0) {
			switch (ret) {
			case ENOTSUP:
				// PTHREAD_PRIO_INHERIT will only prevent
				// priority inversion when SCHED_FIFO or
				// SCHED_RR is used, so this is non-fatal and
				// also currently unsupported on musl.
				break;
			default:
				uwsgi_log("unable to set PTHREAD_PRIO_INHERIT\n");
				exit(1);
			}
		}
		if (pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST)) {
			uwsgi_log("unable to make the mutex 'robust'\n");
			exit(1);
		}
	}
#endif

	if (pthread_mutex_init((pthread_mutex_t *) uli->lock_ptr, &attr)) {
#ifdef EOWNERDEAD
		if (uwsgi_pthread_robust_mutexes_enabled) {
			uwsgi_log("!!! it looks like your kernel does not support pthread robust mutexes !!!\n");
			uwsgi_log("!!! falling back to standard pthread mutexes !!!\n");
			uwsgi_pthread_robust_mutexes_enabled = 0;
			pthread_mutexattr_destroy(&attr);
			goto retry;
		}
#endif
		uwsgi_log("unable to initialize mutex\n");
		exit(1);
	}

	pthread_mutexattr_destroy(&attr);

#ifdef EOWNERDEAD
	if (!uwsgi_pthread_robust_mutexes_enabled) {
		uli->can_deadlock = 1;
	}
#else
	uli->can_deadlock = 1;
#endif

	return uli;
}

pid_t uwsgi_lock_fast_check(struct uwsgi_lock_item * uli) {

	if (pthread_mutex_trylock((pthread_mutex_t *) uli->lock_ptr) == 0) {
		pthread_mutex_unlock((pthread_mutex_t *) uli->lock_ptr);
		return 0;
	}
	return uli->pid;
}

pid_t uwsgi_rwlock_fast_check(struct uwsgi_lock_item * uli) {
#ifdef OBSOLETE_LINUX_KERNEL
	return uwsgi_lock_fast_check(uli);
#else

	if (pthread_rwlock_trywrlock((pthread_rwlock_t *) uli->lock_ptr) == 0) {
		pthread_rwlock_unlock((pthread_rwlock_t *) uli->lock_ptr);
		return 0;
	}
	return uli->pid;
#endif
}


void uwsgi_lock_fast(struct uwsgi_lock_item *uli) {

#ifdef EOWNERDEAD
	if (pthread_mutex_lock((pthread_mutex_t *) uli->lock_ptr) == EOWNERDEAD) {
		uwsgi_log("[deadlock-detector] a process holding a robust mutex died. recovering...\n");
		pthread_mutex_consistent((pthread_mutex_t *) uli->lock_ptr);
	}
#else
	pthread_mutex_lock((pthread_mutex_t *) uli->lock_ptr);
#endif
	uli->pid = uwsgi.mypid;
}

void uwsgi_unlock_fast(struct uwsgi_lock_item *uli) {

	pthread_mutex_unlock((pthread_mutex_t *) uli->lock_ptr);
	uli->pid = 0;

}

void uwsgi_rlock_fast(struct uwsgi_lock_item *uli) {
#ifdef OBSOLETE_LINUX_KERNEL
	uwsgi_lock_fast(uli);
#else
	pthread_rwlock_rdlock((pthread_rwlock_t *) uli->lock_ptr);
	uli->pid = uwsgi.mypid;
#endif
}

void uwsgi_wlock_fast(struct uwsgi_lock_item *uli) {
#ifdef OBSOLETE_LINUX_KERNEL
	uwsgi_lock_fast(uli);
#else
	pthread_rwlock_wrlock((pthread_rwlock_t *) uli->lock_ptr);
	uli->pid = uwsgi.mypid;
#endif
}

void uwsgi_rwunlock_fast(struct uwsgi_lock_item *uli) {
#ifdef OBSOLETE_LINUX_KERNEL
	uwsgi_unlock_fast(uli);
#else
	pthread_rwlock_unlock((pthread_rwlock_t *) uli->lock_ptr);
	uli->pid = 0;
#endif
}

struct uwsgi_lock_item *uwsgi_rwlock_fast_init(char *id) {

#ifdef OBSOLETE_LINUX_KERNEL
	return uwsgi_lock_fast_init(id);
#else

	pthread_rwlockattr_t attr;

	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 1);

	if (pthread_rwlockattr_init(&attr)) {
		uwsgi_log("unable to allocate rwlock structure\n");
		exit(1);
	}

	if (pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
		uwsgi_log("unable to share rwlock\n");
		exit(1);
	}

	if (pthread_rwlock_init((pthread_rwlock_t *) uli->lock_ptr, &attr)) {
		uwsgi_log("unable to initialize rwlock\n");
		exit(1);
	}

	pthread_rwlockattr_destroy(&attr);

	uli->can_deadlock = 1;

	return uli;
#endif



}



#elif defined(UWSGI_LOCK_USE_UMTX)

/* Warning: FreeBSD is still not ready for process-shared UMTX */

#include <machine/atomic.h>
#include <sys/umtx.h>

#define UWSGI_LOCK_SIZE		sizeof(struct umtx)
#define UWSGI_RWLOCK_SIZE	sizeof(struct umtx)
#define UWSGI_LOCK_ENGINE_NAME	"FreeBSD umtx"

struct uwsgi_lock_item *uwsgi_rwlock_fast_init(char *id) {
	return uwsgi_lock_fast_init(id);
}
void uwsgi_rlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_wlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_rwunlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_unlock_fast(uli);
}

struct uwsgi_lock_item *uwsgi_lock_fast_init(char *id) {
	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);
	umtx_init((struct umtx *) uli->lock_ptr);
	return uli;
}

void uwsgi_lock_fast(struct uwsgi_lock_item *uli) {
	umtx_lock((struct umtx *) uli->lock_ptr, (u_long) getpid());
	uli->pid = uwsgi.mypid;
}

void uwsgi_unlock_fast(struct uwsgi_lock_item *uli) {
	umtx_unlock((struct umtx *) uli->lock_ptr, (u_long) getpid());
	uli->pid = 0;
}

pid_t uwsgi_lock_fast_check(struct uwsgi_lock_item *uli) {
	if (umtx_trylock((struct umtx *) uli->lock_ptr, (u_long) getpid())) {
		umtx_unlock((struct umtx *) uli->lock_ptr, (u_long) getpid());
		return 0;
	}
	return uli->pid;
}

pid_t uwsgi_rwlock_fast_check(struct uwsgi_lock_item * uli) {
	return uwsgi_lock_fast_check(uli);
}

#elif defined(UWSGI_LOCK_USE_POSIX_SEM)

#define UWSGI_LOCK_SIZE         sizeof(sem_t)
#define UWSGI_RWLOCK_SIZE       sizeof(sem_t)
#define UWSGI_LOCK_ENGINE_NAME  "POSIX semaphores"

#include <semaphore.h>

struct uwsgi_lock_item *uwsgi_lock_fast_init(char *id) {
	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);
	sem_init((sem_t *) uli->lock_ptr, 1, 1);
	uli->can_deadlock = 1;
	return uli;
}

struct uwsgi_lock_item *uwsgi_rwlock_fast_init(char *id) {
	return uwsgi_lock_fast_init(id);
}

void uwsgi_lock_fast(struct uwsgi_lock_item *uli) {
	sem_wait((sem_t *) uli->lock_ptr);
	uli->pid = uwsgi.mypid;
}

void uwsgi_unlock_fast(struct uwsgi_lock_item *uli) {
	sem_post((sem_t *) uli->lock_ptr);
	uli->pid = 0;
}

pid_t uwsgi_lock_fast_check(struct uwsgi_lock_item *uli) {
	if (sem_trywait((sem_t *) uli->lock_ptr) == 0) {
		sem_post((sem_t *) uli->lock_ptr);
		return 0;
	}
	return uli->pid;
}

pid_t uwsgi_rwlock_fast_check(struct uwsgi_lock_item * uli) {
	return uwsgi_lock_fast_check(uli);
}
void uwsgi_rlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_wlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_rwunlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_unlock_fast(uli);
}


#elif defined(UWSGI_LOCK_USE_OSX_SPINLOCK)

#define UWSGI_LOCK_ENGINE_NAME "OSX spinlocks"
#define UWSGI_LOCK_SIZE		sizeof(OSSpinLock)
#define UWSGI_RWLOCK_SIZE	sizeof(OSSpinLock)


struct uwsgi_lock_item *uwsgi_lock_fast_init(char *id) {

	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);
	memset(uli->lock_ptr, 0, UWSGI_LOCK_SIZE);
	uli->can_deadlock = 1;
	return uli;
}

void uwsgi_lock_fast(struct uwsgi_lock_item *uli) {

	OSSpinLockLock((OSSpinLock *) uli->lock_ptr);
	uli->pid = uwsgi.mypid;
}

void uwsgi_unlock_fast(struct uwsgi_lock_item *uli) {

	OSSpinLockUnlock((OSSpinLock *) uli->lock_ptr);
	uli->pid = 0;
}

pid_t uwsgi_lock_fast_check(struct uwsgi_lock_item *uli) {
	if (OSSpinLockTry((OSSpinLock *) uli->lock_ptr)) {
		OSSpinLockUnlock((OSSpinLock *) uli->lock_ptr);
		return 0;
	}
	return uli->pid;
}

struct uwsgi_lock_item *uwsgi_rwlock_fast_init(char *id) {
	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 1);
	memset(uli->lock_ptr, 0, UWSGI_LOCK_SIZE);
	uli->can_deadlock = 1;
	return uli;
}

void uwsgi_rlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_wlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}

pid_t uwsgi_rwlock_fast_check(struct uwsgi_lock_item *uli) {
	return uwsgi_lock_fast_check(uli);
}

void uwsgi_rwunlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_unlock_fast(uli);
}

#elif defined(UWSGI_LOCK_USE_WINDOWS_MUTEX)

#define UWSGI_LOCK_ENGINE_NAME "windows mutexes"
#define UWSGI_LOCK_SIZE         sizeof(HANDLE)
#define UWSGI_RWLOCK_SIZE       sizeof(HANDLE)


struct uwsgi_lock_item *uwsgi_lock_fast_init(char *id) {

        struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);
	struct _SECURITY_ATTRIBUTES sa;
	memset(&sa, 0, sizeof(struct _SECURITY_ATTRIBUTES));
	sa.bInheritHandle = 1;
	uli->lock_ptr = CreateMutex(&sa, FALSE, NULL);
        return uli;
}

void uwsgi_lock_fast(struct uwsgi_lock_item *uli) {
	WaitForSingleObject(uli->lock_ptr, INFINITE);
        uli->pid = uwsgi.mypid;
}

void uwsgi_unlock_fast(struct uwsgi_lock_item *uli) {
	ReleaseMutex(uli->lock_ptr);
        uli->pid = 0;
}

pid_t uwsgi_lock_fast_check(struct uwsgi_lock_item *uli) {
	if (WaitForSingleObject(uli->lock_ptr, 0) == WAIT_TIMEOUT) {
		return 0;
	}
        return uli->pid;
}

struct uwsgi_lock_item *uwsgi_rwlock_fast_init(char *id) {
	return uwsgi_lock_fast_init(id);
}

void uwsgi_rlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}
void uwsgi_wlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_lock_fast(uli);
}

pid_t uwsgi_rwlock_fast_check(struct uwsgi_lock_item *uli) {
	return uwsgi_lock_fast_check(uli);
}

void uwsgi_rwunlock_fast(struct uwsgi_lock_item *uli) {
	uwsgi_unlock_fast(uli);
}


#else

#define uwsgi_lock_fast_init uwsgi_lock_ipcsem_init
#define uwsgi_lock_fast_check uwsgi_lock_ipcsem_check
#define uwsgi_lock_fast uwsgi_lock_ipcsem
#define uwsgi_unlock_fast uwsgi_unlock_ipcsem

#define uwsgi_rwlock_fast_init uwsgi_rwlock_ipcsem_init
#define uwsgi_rwlock_fast_check uwsgi_rwlock_ipcsem_check

#define uwsgi_rlock_fast uwsgi_rlock_ipcsem
#define uwsgi_wlock_fast uwsgi_wlock_ipcsem
#define uwsgi_rwunlock_fast uwsgi_rwunlock_ipcsem

#define UWSGI_LOCK_SIZE sizeof(int)
#define UWSGI_RWLOCK_SIZE sizeof(int)

#define UWSGI_LOCK_ENGINE_NAME "ipcsem"

#endif

struct uwsgi_lock_item *uwsgi_lock_ipcsem_init(char *id) {

	// used by ftok
	static int counter = 1;
	union semun {
		int val;
		struct semid_ds *buf;
		ushort *array;
	} semu;
	int semid;
	key_t myKey;

	struct uwsgi_lock_item *uli = uwsgi_register_lock(id, 0);

	if (uwsgi.ftok) {
		myKey = ftok(uwsgi.ftok, counter);
		if (myKey < 0) {
			uwsgi_error("uwsgi_lock_ipcsem_init()/ftok()");
			exit(1);
		}
		counter++;
		semid = semget(myKey, 1, IPC_CREAT | 0666);
	}
	else {
		semid = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
	}

	if (semid < 0) {
		uwsgi_error("uwsgi_lock_ipcsem_init()/semget()");
		exit(1);
	}
	// do this now, to allows triggering of atexit hook in case of problems
	memcpy(uli->lock_ptr, &semid, sizeof(int));

	semu.val = 1;
	if (semctl(semid, 0, SETVAL, semu)) {
		uwsgi_error("uwsgi_lock_ipcsem_init()/semctl()");
		exit(1);
	}

	return uli;
}

void uwsgi_lock_ipcsem(struct uwsgi_lock_item *uli) {

	int semid;
	struct sembuf sb;
	sb.sem_num = 0;
	sb.sem_op = -1;
	sb.sem_flg = SEM_UNDO;

	memcpy(&semid, uli->lock_ptr, sizeof(int));

retry:
	if (semop(semid, &sb, 1)) {
		if (errno == EINTR) goto retry; 
		uwsgi_error("uwsgi_lock_ipcsem()/semop()");
#ifdef EIDRM
		if (errno == EIDRM) {
			exit(UWSGI_BRUTAL_RELOAD_CODE);
		}
#endif
		exit(1);
	}
}

void uwsgi_unlock_ipcsem(struct uwsgi_lock_item *uli) {

	int semid;
	struct sembuf sb;
	sb.sem_num = 0;
	sb.sem_op = 1;
	sb.sem_flg = SEM_UNDO;

	memcpy(&semid, uli->lock_ptr, sizeof(int));

retry:
	if (semop(semid, &sb, 1)) {
		if (errno == EINTR) goto retry; 
		uwsgi_error("uwsgi_unlock_ipcsem()/semop()");
#ifdef EIDRM
		if (errno == EIDRM) {
			exit(UWSGI_BRUTAL_RELOAD_CODE);
		}
#endif
		exit(1);
	}

}

struct uwsgi_lock_item *uwsgi_rwlock_ipcsem_init(char *id) {
	return uwsgi_lock_ipcsem_init(id);
}
void uwsgi_rlock_ipcsem(struct uwsgi_lock_item *uli) {
	uwsgi_lock_ipcsem(uli);
}
void uwsgi_wlock_ipcsem(struct uwsgi_lock_item *uli) {
	uwsgi_lock_ipcsem(uli);
}
void uwsgi_rwunlock_ipcsem(struct uwsgi_lock_item *uli) {
	uwsgi_unlock_ipcsem(uli);
}

// ipc cannot deadlock
pid_t uwsgi_lock_ipcsem_check(struct uwsgi_lock_item *uli) {
	return 0;
}

void uwsgi_ipcsem_clear(void) {

	if (uwsgi.persistent_ipcsem) return;

	struct uwsgi_lock_item *uli = uwsgi.registered_locks;

	if (!uwsgi.workers)
		goto clear;

	if (uwsgi.mywid == 0)
		goto clear;

	if (uwsgi.master_process && getpid() == uwsgi.workers[0].pid)
		goto clear;

	if (!uwsgi.master_process && uwsgi.mywid == 1)
		goto clear;

	return;

clear:

#ifdef UWSGI_DEBUG
	uwsgi_log("removing sysvipc semaphores...\n");
#endif
#ifdef GETPID
	while (uli) {
		int semid = 0;
		memcpy(&semid, uli->lock_ptr, sizeof(int));
		int ret = semctl(semid, 0, GETPID);
		if (ret > 0) {
			if (ret != (int) getpid() && !kill((pid_t) ret, 0)) {
				uwsgi_log("found ipcsem mapped to alive pid %d. skipping ipcsem removal.\n", ret);
				return;
			}
		}
		uli = uli->next;
	}
	uli = uwsgi.registered_locks;
#endif
	while (uli) {
		int semid = 0;
		memcpy(&semid, uli->lock_ptr, sizeof(int));
		if (semctl(semid, 0, IPC_RMID)) {
			uwsgi_error("uwsgi_ipcsem_clear()/semctl()");
		}
		uli = uli->next;
	}
}


pid_t uwsgi_rwlock_ipcsem_check(struct uwsgi_lock_item *uli) {
	return uwsgi_lock_ipcsem_check(uli);
}

#ifdef UNBIT
/*
	Unbit-specific workaround for robust-mutexes
*/
void *uwsgi_robust_mutexes_watchdog_loop(void *arg) {
	for(;;) {
		uwsgi_lock(uwsgi.the_thunder_lock);
		uwsgi_unlock(uwsgi.the_thunder_lock);
		sleep(1);
	}
	return NULL;
}
void uwsgi_robust_mutexes_watchdog() {
	pthread_t tid;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        // 32K should be more than enough...
        pthread_attr_setstacksize(&attr, 32 * 1024);

        if (pthread_create(&tid, &attr, uwsgi_robust_mutexes_watchdog_loop, NULL)) {
                uwsgi_error("uwsgi_robust_mutexes_watchdog()/pthread_create()");
		exit(1);
        }
}

#endif

void uwsgi_setup_locking() {

	int i;

	if (uwsgi.locking_setup) return;

	// use the fastest available locking
	if (uwsgi.lock_engine) {
		if (!strcmp(uwsgi.lock_engine, "ipcsem")) {
			uwsgi_log_initial("lock engine: ipcsem\n");
			atexit(uwsgi_ipcsem_clear);
			uwsgi.lock_ops.lock_init = uwsgi_lock_ipcsem_init;
			uwsgi.lock_ops.lock_check = uwsgi_lock_ipcsem_check;
			uwsgi.lock_ops.lock = uwsgi_lock_ipcsem;
			uwsgi.lock_ops.unlock = uwsgi_unlock_ipcsem;
			uwsgi.lock_ops.rwlock_init = uwsgi_rwlock_ipcsem_init;
			uwsgi.lock_ops.rwlock_check = uwsgi_rwlock_ipcsem_check;
			uwsgi.lock_ops.rlock = uwsgi_rlock_ipcsem;
			uwsgi.lock_ops.wlock = uwsgi_wlock_ipcsem;
			uwsgi.lock_ops.rwunlock = uwsgi_rwunlock_ipcsem;
			uwsgi.lock_size = 8;
			uwsgi.rwlock_size = 8;
			goto ready;
		}
		uwsgi_log("unable to find lock engine \"%s\"\n", uwsgi.lock_engine);
		exit(1);
	}

	uwsgi_log_initial("lock engine: %s\n", UWSGI_LOCK_ENGINE_NAME);
#ifdef UWSGI_IPCSEM_ATEXIT
	atexit(uwsgi_ipcsem_clear);
#endif
	uwsgi.lock_ops.lock_init = uwsgi_lock_fast_init;
	uwsgi.lock_ops.lock_check = uwsgi_lock_fast_check;
	uwsgi.lock_ops.lock = uwsgi_lock_fast;
	uwsgi.lock_ops.unlock = uwsgi_unlock_fast;
	uwsgi.lock_ops.rwlock_init = uwsgi_rwlock_fast_init;
	uwsgi.lock_ops.rwlock_check = uwsgi_rwlock_fast_check;
	uwsgi.lock_ops.rlock = uwsgi_rlock_fast;
	uwsgi.lock_ops.wlock = uwsgi_wlock_fast;
	uwsgi.lock_ops.rwunlock = uwsgi_rwunlock_fast;
	uwsgi.lock_size = UWSGI_LOCK_SIZE;
	uwsgi.rwlock_size = UWSGI_RWLOCK_SIZE;

ready:
	// application generic lock
	uwsgi.user_lock = uwsgi_malloc(sizeof(void *) * (uwsgi.locks + 1));
	for (i = 0; i < uwsgi.locks + 1; i++) {
		char *num = uwsgi_num2str(i);
		uwsgi.user_lock[i] = uwsgi_lock_init(uwsgi_concat2("user ", num));
		free(num);
	}

	// event queue lock (mitigate same event on multiple queues)
	if (uwsgi.threads > 1) {
		pthread_mutex_init(&uwsgi.thunder_mutex, NULL);
	}

	if (uwsgi.master_process) {
		// signal table lock
		uwsgi.signal_table_lock = uwsgi_lock_init("signal");

		// fmon table lock
		uwsgi.fmon_table_lock = uwsgi_lock_init("filemon");

		// timer table lock
		uwsgi.timer_table_lock = uwsgi_lock_init("timer");

		// rb_timer table lock
		uwsgi.rb_timer_table_lock = uwsgi_lock_init("rbtimer");

		// cron table lock
		uwsgi.cron_table_lock = uwsgi_lock_init("cron");
	}

	if (uwsgi.use_thunder_lock) {
		// process shared thunder lock
		uwsgi.the_thunder_lock = uwsgi_lock_init("thunder");	
#ifdef UNBIT
		// we have a serious bug on Unbit (and very probably on older libc)
		// when all of the workers die in the same moment the pthread robust mutes is left
		// in inconsistent state and we have no way to recover
		// we span a thread in the master constantly ensuring the lock is ok
		// for now we apply it only for Unbit (where thunder-lock is automatically enabled)
		uwsgi_robust_mutexes_watchdog();		
#endif
	}

	uwsgi.rpc_table_lock = uwsgi_lock_init("rpc");

#ifdef UWSGI_SSL
	// register locking for legions
	struct uwsgi_legion *ul = uwsgi.legions;
	while(ul) {
		ul->lock = uwsgi_lock_init(uwsgi_concat2("legion_", ul->legion));
		ul = ul->next;
	}
#endif
	uwsgi.locking_setup = 1;
}


int uwsgi_fcntl_lock(int fd) {
	struct flock fl;
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	fl.l_start = 0;
	fl.l_len = 0;
	fl.l_pid = 0;

	int ret = fcntl(fd, F_SETLKW, &fl);
	if (ret < 0)
		uwsgi_error("fcntl()");

	return ret;
}

int uwsgi_fcntl_is_locked(int fd) {

	struct flock fl;
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	fl.l_start = 0;
	fl.l_len = 0;
	fl.l_pid = 0;

	if (fcntl(fd, F_SETLK, &fl)) {
		return 1;
	}

	return 0;

}

void uwsgi_deadlock_check(pid_t diedpid) {
	struct uwsgi_lock_item *uli = uwsgi.registered_locks;
	while (uli) {
		if (!uli->can_deadlock)
			goto nextlock;
		pid_t locked_pid = 0;
		if (uli->rw) {
			locked_pid = uwsgi_rwlock_check(uli);
		}
		else {
			locked_pid = uwsgi_lock_check(uli);
		}
		if (locked_pid == diedpid) {
			uwsgi_log("[deadlock-detector] pid %d was holding lock %s (%p)\n", (int) diedpid, uli->id, uli->lock_ptr);
			if (uli->rw) {
				uwsgi_rwunlock(uli);
			}
			else {
				uwsgi_unlock(uli);
			}
		}
nextlock:
		uli = uli->next;
	}

}

int uwsgi_user_lock(int lock_num) {
	if (lock_num < 0 || lock_num > uwsgi.locks) {
		return -1;
	}
	uwsgi_lock(uwsgi.user_lock[lock_num]);
	return 0;
}

int uwsgi_user_unlock(int lock_num) {
	if (lock_num < 0 || lock_num > uwsgi.locks) {
		return -1;
	}
	uwsgi_unlock(uwsgi.user_lock[lock_num]);
	return 0;
}
