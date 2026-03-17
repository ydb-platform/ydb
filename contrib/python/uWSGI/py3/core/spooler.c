#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

static void spooler_readdir(struct uwsgi_spooler *, char *dir);
#ifdef __linux__
static void spooler_scandir(struct uwsgi_spooler *, char *dir);
#endif
static void spooler_manage_task(struct uwsgi_spooler *, char *, char *);

// increment it whenever a signal is raised
static uint64_t wakeup = 0;

// function to allow waking up the spooler if blocked in event_wait
void spooler_wakeup(int signum) {
	wakeup++;
}

void uwsgi_opt_add_spooler(char *opt, char *directory, void *mode) {

	int i;
	struct uwsgi_spooler *us;

	if (access(directory, R_OK | W_OK | X_OK) &&
	    mkdir(directory, S_IRWXU | S_IXGRP | S_IRGRP)) {
		uwsgi_error("[spooler directory] access()");
		exit(1);
	}

	if (uwsgi.spooler_numproc > 0) {
		for (i = 0; i < uwsgi.spooler_numproc; i++) {
			us = uwsgi_new_spooler(directory);
			if (mode)
				us->mode = (long) mode;
		}
	}
	else {
		us = uwsgi_new_spooler(directory);
		if (mode)
			us->mode = (long) mode;
	}

}


struct uwsgi_spooler *uwsgi_new_spooler(char *dir) {

	struct uwsgi_spooler *uspool = uwsgi.spoolers;

	if (!uspool) {
		uwsgi.spoolers = uwsgi_calloc_shared(sizeof(struct uwsgi_spooler));
		uspool = uwsgi.spoolers;
	}
	else {
		while (uspool) {
			if (uspool->next == NULL) {
				uspool->next = uwsgi_calloc_shared(sizeof(struct uwsgi_spooler));
				uspool = uspool->next;
				break;
			}
			uspool = uspool->next;
		}
	}

	if (!realpath(dir, uspool->dir)) {
		uwsgi_error("[spooler] realpath()");
		exit(1);
	}

	uspool->next = NULL;

	return uspool;
}


struct uwsgi_spooler *uwsgi_get_spooler_by_name(char *name, size_t name_len) {

	struct uwsgi_spooler *uspool = uwsgi.spoolers;

	while (uspool) {
		if (!uwsgi_strncmp(uspool->dir, strlen(uspool->dir), name, name_len)) {
			return uspool;
		}
		uspool = uspool->next;
	}

	return NULL;
}

pid_t spooler_start(struct uwsgi_spooler * uspool) {

	int i;

	pid_t pid = uwsgi_fork("uWSGI spooler");
	if (pid < 0) {
		uwsgi_error("fork()");
		exit(1);
	}
	else if (pid == 0) {

		signal(SIGALRM, SIG_IGN);
                signal(SIGHUP, SIG_IGN);
                signal(SIGINT, end_me);
                signal(SIGTERM, end_me);
		// USR1 will be used to wake up the spooler
		uwsgi_unix_signal(SIGUSR1, spooler_wakeup);
                signal(SIGUSR2, SIG_IGN);
                signal(SIGPIPE, SIG_IGN);
                signal(SIGSTOP, SIG_IGN);
                signal(SIGTSTP, SIG_IGN);

		uwsgi.mypid = getpid();
		uspool->pid = uwsgi.mypid;
		// avoid race conditions !!!
		uwsgi.i_am_a_spooler = uspool;

		uwsgi_fixup_fds(0, 0, NULL);
		uwsgi_close_all_sockets();

		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->post_fork) {
				uwsgi.p[i]->post_fork();
			}
		}

		uwsgi_spooler_run();
	}
	else if (pid > 0) {
		uwsgi_log("spawned the uWSGI spooler on dir %s with pid %d\n", uspool->dir, pid);
	}

	return pid;
}

void uwsgi_spooler_run() {
	int i;
	struct uwsgi_spooler *uspool = uwsgi.i_am_a_spooler;
	uwsgi.signal_socket = uwsgi.shared->spooler_signal_pipe[1];

                for (i = 0; i < 256; i++) {
                        if (uwsgi.p[i]->spooler_init) {
                                uwsgi.p[i]->spooler_init();
                        }
                }

                for (i = 0; i < uwsgi.gp_cnt; i++) {
                        if (uwsgi.gp[i]->spooler_init) {
                                uwsgi.gp[i]->spooler_init();
                        }
                }

                spooler(uspool);
}

void destroy_spool(char *dir, char *file) {

	if (chdir(dir)) {
		uwsgi_error("chdir()");
		uwsgi_log("[spooler] something horrible happened to the spooler. Better to kill it.\n");
		exit(1);
	}

	if (unlink(file)) {
		uwsgi_error("unlink()");
		uwsgi_log("[spooler] something horrible happened to the spooler. Better to kill it.\n");
		exit(1);
	}

}

struct spooler_req {
	char *spooler;
	size_t spooler_len;
	char *priority;
	size_t priority_len;
	time_t at;
};

static void spooler_req_parser_hook(char *key, uint16_t key_len, char *value, uint16_t value_len, void *data) {
	struct spooler_req *sr = (struct spooler_req *) data;
	if (!uwsgi_strncmp(key, key_len, "spooler", 7)) {
		sr->spooler = value;
		sr->spooler_len = value_len;
		return;
	} 

	if (!uwsgi_strncmp(key, key_len, "priority", 8)) {
                sr->priority = value;
                sr->priority_len = value_len;
                return;
        }

	if (!uwsgi_strncmp(key, key_len, "at", 2)) {
		// at can be a float...
		char *dot = memchr(value, '.', value_len);
		if (dot) {
			value_len = dot - value;
		}
		sr->at = uwsgi_str_num(value, value_len);
		return;
	}
}

/*
CHANGED in 2.0.7: wsgi_req is useless !
*/
char *uwsgi_spool_request(struct wsgi_request *wsgi_req, char *buf, size_t len, char *body, size_t body_len) {

	struct timeval tv;
	static uint64_t internal_counter = 0;
	int fd = -1;
	struct spooler_req sr;

	if (len > 0xffff) {
		uwsgi_log("[uwsgi-spooler] args buffer is limited to 64k, use the 'body' for bigger values\n");
		return NULL;
	}

	// parse the request buffer
	memset(&sr, 0, sizeof(struct spooler_req));
	uwsgi_hooked_parse(buf, len, spooler_req_parser_hook, &sr);
	
	struct uwsgi_spooler *uspool = uwsgi.spoolers;
	if (!uspool) {
		uwsgi_log("[uwsgi-spooler] no spooler available\n");
		return NULL;
	}

	// if it is a number, get the spooler by id instead of by name
	if (sr.spooler && sr.spooler_len) {
		uspool = uwsgi_get_spooler_by_name(sr.spooler, sr.spooler_len);
		if (!uspool) {
			uwsgi_log("[uwsgi-spooler] unable to find spooler \"%.*s\"\n", sr.spooler_len, sr.spooler);
			return NULL;
		}
	}

	// this lock is for threads, the pid value in filename will avoid multiprocess races
	uwsgi_lock(uspool->lock);

	// we increase it even if the request fails
	internal_counter++;

	gettimeofday(&tv, NULL);

	char *filename = NULL;
	size_t filename_len = 0;

	if (sr.priority && sr.priority_len) {
		filename_len = strlen(uspool->dir) + sr.priority_len + strlen(uwsgi.hostname) + 256;	
		filename = uwsgi_malloc(filename_len);
		int ret = snprintf(filename, filename_len, "%s/%.*s", uspool->dir, (int) sr.priority_len, sr.priority);
		if (ret <= 0 || ret >= (int) filename_len) {
			uwsgi_log("[uwsgi-spooler] error generating spooler filename\n");
			free(filename);
			uwsgi_unlock(uspool->lock);
			return NULL;
		}
		// no need to check for errors...
		(void) mkdir(filename, 0777);

		ret = snprintf(filename, filename_len, "%s/%.*s/uwsgi_spoolfile_on_%s_%d_%llu_%d_%llu_%llu", uspool->dir, (int)sr.priority_len, sr.priority, uwsgi.hostname, (int) getpid(), (unsigned long long) internal_counter, rand(),
				(unsigned long long) tv.tv_sec, (unsigned long long) tv.tv_usec);
		if (ret <= 0 || ret >=(int)  filename_len) {
                        uwsgi_log("[uwsgi-spooler] error generating spooler filename\n");
			free(filename);
			uwsgi_unlock(uspool->lock);
			return NULL;
		}
	}
	else {
		filename_len = strlen(uspool->dir) + strlen(uwsgi.hostname) + 256;
                filename = uwsgi_malloc(filename_len);
		int ret = snprintf(filename, filename_len, "%s/uwsgi_spoolfile_on_%s_%d_%llu_%d_%llu_%llu", uspool->dir, uwsgi.hostname, (int) getpid(), (unsigned long long) internal_counter,
				rand(), (unsigned long long) tv.tv_sec, (unsigned long long) tv.tv_usec);
		if (ret <= 0 || ret >= (int) filename_len) {
                        uwsgi_log("[uwsgi-spooler] error generating spooler filename\n");
			free(filename);
			uwsgi_unlock(uspool->lock);
			return NULL;
		}
	}

	fd = open(filename, O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
	if (fd < 0) {
		uwsgi_error_open(filename);
		free(filename);
		uwsgi_unlock(uspool->lock);
		return NULL;
	}

	// now lock the file, it will no be runnable, until the lock is not removed
	// a race could come if the spooler take the file before fcntl is called
	// in such case the spooler will detect a zeroed file and will retry later
	if (uwsgi_fcntl_lock(fd)) {
		close(fd);
		free(filename);
		uwsgi_unlock(uspool->lock);
		return NULL;
	}

	struct uwsgi_header uh;
	uh.modifier1 = 17;
	uh.modifier2 = 0;
	uh.pktsize = (uint16_t) len;
#ifdef __BIG_ENDIAN__
	uh.pktsize = uwsgi_swap16(uh.pktsize);
#endif

	if (write(fd, &uh, 4) != 4) {
		uwsgi_log("[spooler] unable to write header for %s\n", filename);
		goto clear;
	}

	if (write(fd, buf, len) != (ssize_t) len) {
		uwsgi_log("[spooler] unable to write args for %s\n", filename);
		goto clear;
	}

	if (body && body_len > 0) {
		if ((size_t) write(fd, body, body_len) != body_len) {
			uwsgi_log("[spooler] unable to write body for %s\n", filename);
			goto clear;
		}
	}

	if (sr.at > 0) {
#ifdef __UCLIBC__
		struct timespec ts[2]; 
		ts[0].tv_sec = sr.at; 
		ts[0].tv_nsec = 0;
		ts[1].tv_sec = sr.at;
		ts[1].tv_nsec = 0; 
		if (futimens(fd, ts)) {
			uwsgi_error("uwsgi_spooler_request()/futimens()");	
		}
#else
		struct timeval tv[2];
		tv[0].tv_sec = sr.at;
		tv[0].tv_usec = 0;
		tv[1].tv_sec = sr.at;
		tv[1].tv_usec = 0;
#ifdef __sun__
		if (futimesat(fd, NULL, tv)) {
#else
		if (futimes(fd, tv)) {
#endif
			uwsgi_error("uwsgi_spooler_request()/futimes()");
		}
#endif
	}

	// here the file will be unlocked too
	close(fd);

	if (!uwsgi.spooler_quiet)
		uwsgi_log("[spooler] written %lu bytes to file %s\n", (unsigned long) len + body_len + 4, filename);

	// and here waiting threads can continue
	uwsgi_unlock(uspool->lock);

/*	wake up the spoolers attached to the specified dir ... (HACKY) 
	no need to fear races, as USR1 is harmless an all of the uWSGI processes...
	it could be a problem if a new process takes the old pid, but modern systems should avoid that
*/

	struct uwsgi_spooler *spoolers = uwsgi.spoolers;
	while (spoolers) {
		if (!strcmp(spoolers->dir, uspool->dir)) {
			if (spoolers->pid > 0 && spoolers->running == 0) {
				(void) kill(spoolers->pid, SIGUSR1);
			}
		}
		spoolers = spoolers->next;
	}

	return filename;


clear:
	uwsgi_unlock(uspool->lock);
	uwsgi_error("uwsgi_spool_request()/write()");
	if (unlink(filename)) {
		uwsgi_error("uwsgi_spool_request()/unlink()");
	}
	free(filename);
	// unlock the file too
	close(fd);
	return NULL;
}



void spooler(struct uwsgi_spooler *uspool) {

	// prevent process blindly reading stdin to make mess
	int nullfd;

	// asked by Marco Beri
#ifdef __HAIKU__
#ifdef UWSGI_DEBUG
	uwsgi_log("lowering spooler priority to %d\n", B_LOW_PRIORITY);
#endif
	set_thread_priority(find_thread(NULL), B_LOW_PRIORITY);
#else
#ifdef UWSGI_DEBUG
	uwsgi_log("lowering spooler priority to %d\n", PRIO_MAX);
#endif
	setpriority(PRIO_PROCESS, getpid(), PRIO_MAX);
#endif

	nullfd = open("/dev/null", O_RDONLY);
	if (nullfd < 0) {
		uwsgi_error_open("/dev/null");
		exit(1);
	}

	if (nullfd != 0) {
		dup2(nullfd, 0);
		close(nullfd);
	}

	int spooler_event_queue = event_queue_init();
	int interesting_fd = -1;

	if (uwsgi.master_process) {
		event_queue_add_fd_read(spooler_event_queue, uwsgi.shared->spooler_signal_pipe[1]);
	}

	// reset the tasks counter
	uspool->tasks = 0;

	for (;;) {


		if (chdir(uspool->dir)) {
			uwsgi_error("chdir()");
			exit(1);
		}

		if (uwsgi.spooler_ordered) {
#ifdef __linux__
			spooler_scandir(uspool, NULL);
#else
			spooler_readdir(uspool, NULL);
#endif
		}
		else {
			spooler_readdir(uspool, NULL);
		}

		int timeout = uwsgi.shared->spooler_frequency ? uwsgi.shared->spooler_frequency : uwsgi.spooler_frequency;
		if (wakeup > 0) {
			timeout = 0;
		}

		if (event_queue_wait(spooler_event_queue, timeout, &interesting_fd) > 0) {
			if (uwsgi.master_process) {
				if (interesting_fd == uwsgi.shared->spooler_signal_pipe[1]) {
					uwsgi_receive_signal(interesting_fd, "spooler", (int) getpid());
				}
			}
		}

		// avoid races
		uint64_t tmp_wakeup = wakeup;
		if (tmp_wakeup > 0) {
			tmp_wakeup--;
		}
		wakeup = tmp_wakeup;

	}
}

#ifdef __linux__
static void spooler_scandir(struct uwsgi_spooler *uspool, char *dir) {

	struct dirent **tasklist;
	int n, i;

	if (!dir)
		dir = uspool->dir;

	n = scandir(dir, &tasklist, NULL, versionsort);
	if (n < 0) {
		uwsgi_error("scandir()");
		return;
	}

	for (i = 0; i < n; i++) {
		spooler_manage_task(uspool, dir, tasklist[i]->d_name);
		free(tasklist[i]);
	}

	free(tasklist);
}
#endif


static void spooler_readdir(struct uwsgi_spooler *uspool, char *dir) {

	DIR *sdir;
	struct dirent *dp;

	if (!dir)
		dir = uspool->dir;

	sdir = opendir(dir);
	if (sdir) {
		while ((dp = readdir(sdir)) != NULL) {
			spooler_manage_task(uspool, dir, dp->d_name);
		}
		closedir(sdir);
	}
	else {
		uwsgi_error("opendir()");
	}
}

int uwsgi_spooler_read_header(char *task, int spool_fd, struct uwsgi_header *uh) {

	// check if the file is locked by another process
	if (uwsgi_fcntl_is_locked(spool_fd)) {
		uwsgi_protected_close(spool_fd);
		return -1;
	}

	// unlink() can destroy the lock !!!
	if (access(task, R_OK|W_OK)) {
		uwsgi_protected_close(spool_fd);
		return -1;
	}

	ssize_t rlen = uwsgi_protected_read(spool_fd, uh, 4);

	if (rlen != 4) {
		// it could be here for broken file or just opened one
		if (rlen < 0)
			uwsgi_error("spooler_manage_task()/read()");
		uwsgi_protected_close(spool_fd);
		return -1;
	}

#ifdef __BIG_ENDIAN__
	uh->pktsize = uwsgi_swap16(uh->pktsize);
#endif

	return 0;
}

int uwsgi_spooler_read_content(int spool_fd, char *spool_buf, char **body, size_t *body_len, struct uwsgi_header *uh, struct stat *sf_lstat) {

	if (uwsgi_protected_read(spool_fd, spool_buf, uh->pktsize) != uh->pktsize) {
		uwsgi_error("spooler_manage_task()/read()");
		uwsgi_protected_close(spool_fd);
		return 1;
	}

	// body available ?
	if (sf_lstat->st_size > (uh->pktsize + 4)) {
		*body_len = sf_lstat->st_size - (uh->pktsize + 4);
		*body = uwsgi_malloc(*body_len);
		if ((size_t) uwsgi_protected_read(spool_fd, *body, *body_len) != *body_len) {
			uwsgi_error("spooler_manage_task()/read()");
			uwsgi_protected_close(spool_fd);
			free(*body);
			return 1;
		}
	}

	return 0;
}

void spooler_manage_task(struct uwsgi_spooler *uspool, char *dir, char *task) {

	int i, ret;

	char spool_buf[0xffff];
	struct uwsgi_header uh;
	char *body = NULL;
	size_t body_len = 0;

	int spool_fd;

	if (!dir)
		dir = uspool->dir;

	if (!strncmp("uwsgi_spoolfile_on_", task, 19) || (uwsgi.spooler_ordered && is_a_number(task))) {
		struct stat sf_lstat;
		if (lstat(task, &sf_lstat)) {
			return;
		}

		// a spool request for the future
		if (sf_lstat.st_mtime > uwsgi_now()) {
			return;
		}

#ifdef __linux__
		if (S_ISDIR(sf_lstat.st_mode) && uwsgi.spooler_ordered) {
			if (chdir(task)) {
				uwsgi_error("chdir()");
				return;
			}
			char *prio_path = realpath(".", NULL);
			spooler_scandir(uspool, prio_path);
			free(prio_path);
			if (chdir(dir)) {
				uwsgi_error("chdir()");
			}
			return;
		}
#endif
		if (!S_ISREG(sf_lstat.st_mode)) {
			return;
		}
		if (!access(task, R_OK | W_OK)) {

			spool_fd = open(task, O_RDWR);

			if (spool_fd < 0) {
				if (errno != ENOENT)
					uwsgi_error_open(task);
				return;
			}

			if (uwsgi_spooler_read_header(task, spool_fd, &uh))
				return;

			// access lstat second time after getting a lock
			// first-time lstat could be dirty (for example between writes in master)
			if (lstat(task, &sf_lstat)) {
				return;
			}

			if (uwsgi_spooler_read_content(spool_fd, spool_buf, &body, &body_len, &uh, &sf_lstat)) {
				destroy_spool(dir, task);
				return;
			}

			// now the task is running and should not be woken up
			uspool->running = 1;

			if (!uwsgi.spooler_quiet)
				uwsgi_log("[spooler %s pid: %d] managing request %s ...\n", uspool->dir, (int) uwsgi.mypid, task);


			// chdir before running the task (if requested)
			if (uwsgi.spooler_chdir) {
				if (chdir(uwsgi.spooler_chdir)) {
					uwsgi_error("chdir()");
				}
			}

			int callable_found = 0;
			for (i = 0; i < 256; i++) {
				if (uwsgi.p[i]->spooler) {
					time_t now = uwsgi_now();
					if (uwsgi.harakiri_options.spoolers > 0) {
						set_spooler_harakiri(uwsgi.harakiri_options.spoolers);
					}
					ret = uwsgi.p[i]->spooler(task, spool_buf, uh.pktsize, body, body_len);
					if (uwsgi.harakiri_options.spoolers > 0) {
						set_spooler_harakiri(0);
					}
					if (ret == 0)
						continue;
					callable_found = 1;
					// increase task counter
					uspool->tasks++;
					if (ret == -2) {
						if (!uwsgi.spooler_quiet)
							uwsgi_log("[spooler %s pid: %d] done with task %s after %lld seconds\n", uspool->dir, (int) uwsgi.mypid, task, (long long) uwsgi_now() - now);
						destroy_spool(dir, task);
					}
					// re-spool it
					break;
				}
			}

			if (body)
				free(body);

			// here we free and unlock the task
			uwsgi_protected_close(spool_fd);
			uspool->running = 0;


			// need to recycle ?
			if (uwsgi.spooler_max_tasks > 0 && uspool->tasks >= (uint64_t) uwsgi.spooler_max_tasks) {
				uwsgi_log("[spooler %s pid: %d] maximum number of tasks reached (%d) recycling ...\n", uspool->dir, (int) uwsgi.mypid, uwsgi.spooler_max_tasks);
				end_me(0);
			}


			if (chdir(dir)) {
				uwsgi_error("chdir()");
				uwsgi_log("[spooler] something horrible happened to the spooler. Better to kill it.\n");
				exit(1);
			}

			if (!callable_found) {
				uwsgi_log("unable to find the spooler function, have you loaded it into the spooler process ?\n");
			}

		}
	}
}
