#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

void worker_wakeup(int sig) {
}

uint64_t uwsgi_worker_exceptions(int wid) {
	uint64_t total = 0;
	int i;
	for(i=0;i<uwsgi.cores;i++) {
		total += uwsgi.workers[wid].cores[i].exceptions;
	}

	return total;
}

void uwsgi_curse(int wid, int sig) {
	uwsgi.workers[wid].cursed_at = uwsgi_now();
        uwsgi.workers[wid].no_mercy_at = uwsgi.workers[wid].cursed_at + uwsgi.worker_reload_mercy;

	if (sig) {
		(void) kill(uwsgi.workers[wid].pid, sig);
	}
}

void uwsgi_curse_mule(int mid, int sig) {
	uwsgi.mules[mid].cursed_at = uwsgi_now();
	uwsgi.mules[mid].no_mercy_at = uwsgi.mules[mid].cursed_at + uwsgi.mule_reload_mercy;

	if (sig) {
		(void) kill(uwsgi.mules[mid].pid, sig);
	}
}

static void uwsgi_signal_spoolers(int signum) {

        struct uwsgi_spooler *uspool = uwsgi.spoolers;
        while (uspool) {
                if (uspool->pid > 0) {
                        kill(uspool->pid, signum);
                        uwsgi_log("killing the spooler with pid %d\n", uspool->pid);
                }
                uspool = uspool->next;
        }

}
void uwsgi_destroy_processes() {

	int i;
	int waitpid_status;

        uwsgi_signal_spoolers(SIGKILL);

        uwsgi_detach_daemons();

        for (i = 0; i < ushared->gateways_cnt; i++) {
                if (ushared->gateways[i].pid > 0) {
                        kill(ushared->gateways[i].pid, SIGKILL);
			waitpid(ushared->gateways[i].pid, &waitpid_status, 0);
			uwsgi_log("gateway \"%s %d\" has been buried (pid: %d)\n", ushared->gateways[i].name, ushared->gateways[i].num, (int) ushared->gateways[i].pid);
		}
        }

	if (uwsgi.emperor_pid > 0) {
                kill(uwsgi.emperor_pid, SIGINT);
		time_t timeout = uwsgi_now() + (uwsgi.reload_mercy ? uwsgi.reload_mercy : 3);
		// increase timeout for being more tolerant
		timeout+=2;
		int waitpid_status;
		while (uwsgi_now() < timeout) {
			pid_t diedpid = waitpid(uwsgi.emperor_pid, &waitpid_status, WNOHANG);
			if (diedpid == uwsgi.emperor_pid) {
				goto nomoremperor;
			}
			uwsgi_log("waiting for Emperor death...\n");
			sleep(1);
		}
		kill(uwsgi.emperor_pid, SIGKILL);
		waitpid(uwsgi.emperor_pid, &waitpid_status, 0);
nomoremperor:
		uwsgi_log("The Emperor has been buried (pid: %d)\n", (int) uwsgi.emperor_pid);
	}
}



void uwsgi_master_cleanup_hooks(void) {

	int j;

	// could be an inherited atexit hook
	if (uwsgi.mypid != uwsgi.workers[0].pid)
		return;

	uwsgi.status.is_cleaning = 1;

	for (j = 0; j < uwsgi.gp_cnt; j++) {
		if (uwsgi.gp[j]->master_cleanup) {
			uwsgi.gp[j]->master_cleanup();
		}
	}

	for (j = 0; j < 256; j++) {
		if (uwsgi.p[j]->master_cleanup) {
			uwsgi.p[j]->master_cleanup();
		}
	}

}


int uwsgi_calc_cheaper(void) {

	int i;
	static time_t last_check = 0;
	int check_interval = uwsgi.master_interval;

	if (!last_check)
		last_check = uwsgi_now();

	time_t now = uwsgi_now();
	if (!check_interval)
		check_interval = 1;

	if ((now - last_check) < check_interval)
		return 1;

	last_check = now;

	int ignore_algo = 0;
	int needed_workers = 0;

	// first check if memory usage is not exceeded
	if (uwsgi.cheaper_rss_limit_soft) {
		unsigned long long total_rss = 0;
		int i;
		int active_workers = 0;
		for(i=1;i<=uwsgi.numproc;i++) {
			if (!uwsgi.workers[i].cheaped) {
				total_rss += uwsgi.workers[i].rss_size;
				active_workers++;
			}
		}
		if (uwsgi.cheaper_rss_limit_hard && active_workers > 1 && total_rss >= uwsgi.cheaper_rss_limit_hard) {
			uwsgi_log("cheaper hard rss memory limit exceeded, cheap one of %d workers\n", active_workers);
			needed_workers = -1;
			ignore_algo = 1;
		}
		else if (total_rss >= uwsgi.cheaper_rss_limit_soft) {
#ifdef UWSGI_DEBUG
			uwsgi_log("cheaper soft rss memory limit exceeded, can't spawn more workers\n");
#endif
			ignore_algo = 1;
		}
	}

	// then check for fifo
	if (uwsgi.cheaper_fifo_delta != 0) {
		if (!ignore_algo) {
			needed_workers = uwsgi.cheaper_fifo_delta;
			ignore_algo = 1;
		}
		uwsgi.cheaper_fifo_delta = 0;
		goto safe;
	}

	// if cheaper limits wants to change worker count, then skip cheaper algo
	if (!needed_workers) needed_workers = uwsgi.cheaper_algo(!ignore_algo);
	// safe check to verify if cheaper algo obeyed ignore_algo value
	if (ignore_algo && needed_workers > 0) {
		uwsgi_log("BUG! cheaper algo returned %d but it cannot spawn any worker at this time!\n", needed_workers);
		needed_workers = 0;
	}

safe:
	if (needed_workers > 0) {
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].cheaped == 1 && uwsgi.workers[i].pid == 0) {
				if (uwsgi_respawn_worker(i)) {
					uwsgi.cheaper_fifo_delta += needed_workers;
					return 0;
				}
				needed_workers--;
			}
			if (needed_workers == 0)
				break;
		}
	}
	else if (needed_workers < 0) {
		while (needed_workers < 0) {
			int oldest_worker = 0;
			time_t oldest_worker_spawn = INT_MAX;
			for (i = 1; i <= uwsgi.numproc; i++) {
				if (uwsgi.workers[i].cheaped == 0 && uwsgi.workers[i].pid > 0) {
					if (uwsgi_worker_is_busy(i) == 0) {
						if (uwsgi.workers[i].last_spawn < oldest_worker_spawn) {
							oldest_worker_spawn = uwsgi.workers[i].last_spawn;
							oldest_worker = i;
						}
					}
				}
			}
			if (oldest_worker > 0) {
#ifdef UWSGI_DEBUG
				uwsgi_log("worker %d should die...\n", oldest_worker);
#endif
				uwsgi.workers[oldest_worker].cheaped = 1;
				uwsgi.workers[oldest_worker].rss_size = 0;
				uwsgi.workers[oldest_worker].vsz_size = 0;
				uwsgi.workers[oldest_worker].manage_next_request = 0;
				uwsgi_curse(oldest_worker, SIGWINCH);
			}
			else {
				// Return it to the pool
				uwsgi.cheaper_fifo_delta--;
			}
			needed_workers++;
		}
	}

	return 1;
}

// fake algo to allow control with the fifo
int uwsgi_cheaper_algo_manual(int can_spawn) {
	return 0;
}

/*

        -- Cheaper, spare algorithm, adapted from old-fashioned spare system --
        
        when all of the workers are busy, the overload_count is incremented.
        as soon as overload_count is higher than uwsgi.cheaper_overload (--cheaper-overload options)
        at most cheaper_step (default to 1) new workers are spawned.

        when at least one worker is free, the overload_count is decremented and the idle_count is incremented.
        If overload_count reaches 0, the system will count active workers (the ones uncheaped) and busy workers (the ones running a request)
	if there is exacly 1 free worker we are in "stable state" (1 spare worker available). no worker will be touched.
	if the number of active workers is higher than uwsgi.cheaper_count and at least uwsgi.cheaper_overload cycles are passed from the last
        "cheap it" procedure, then cheap a worker.

        Example:
            10 processes
            2 cheaper
            2 cheaper step
            3 cheaper_overload 
            1 second master cycle
    
            there are 7 workers running (triggered by some kind of spike activity).
	    Of this, 6 are busy, 1 is free. We are in stable state.
            After a bit the spike disappear and idle_count start to increase.

	    After 3 seconds (uwsgi.cheaper_overload cycles) the oldest worker will be cheaped. This will happens
	    every  seconds (uwsgi.cheaper_overload cycles) til the number of workers is == uwsgi.cheaper_count.

	    If during the "cheap them all" procedure, an overload condition come again (another spike) the "cheap them all"
            will be interrupted.


*/


int uwsgi_cheaper_algo_spare(int can_spawn) {

	int i;
	static uint64_t overload_count = 0;
	static uint64_t idle_count = 0;

	// step 1 -> count the number of busy workers
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].cheaped == 0 && uwsgi.workers[i].pid > 0) {
			// if a non-busy worker is found, the overload_count is decremented and stop the cycle
			if (uwsgi_worker_is_busy(i) == 0) {
				if (overload_count > 0)
					overload_count--;
				goto healthy;
			}
		}
	}

	overload_count++;
	idle_count = 0;

healthy:

	// are we overloaded ?
	if (can_spawn && overload_count > uwsgi.cheaper_overload) {

#ifdef UWSGI_DEBUG
		uwsgi_log("overloaded !!!\n");
#endif

		// activate the first available worker (taking step into account)
		int decheaped = 0;
		// search for cheaped workers
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].cheaped == 1 && uwsgi.workers[i].pid == 0) {
				decheaped++;
				if (decheaped >= uwsgi.cheaper_step)
					break;
			}
		}
		// reset overload
		overload_count = 0;
		// return the maximum number of workers to spawn
		return decheaped;
	}
	// we are no more overloaded
	else if (overload_count == 0) {
		// how many active workers ?
		int active_workers = 0;
		int busy_workers = 0;
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].cheaped == 0 && uwsgi.workers[i].pid > 0) {
				active_workers++;
				if (uwsgi_worker_is_busy(i) == 1)
					busy_workers++;
			}
		}

#ifdef UWSGI_DEBUG
		uwsgi_log("active workers %d busy_workers %d\n", active_workers, busy_workers);
#endif

		// special condition: uwsgi.cheaper running workers and 1 free
		if (active_workers > busy_workers && active_workers - busy_workers == 1) {
#ifdef UWSGI_DEBUG
			uwsgi_log("stable status: 1 spare worker\n");
#endif
			return 0;
		}

		idle_count++;

		if (active_workers > uwsgi.cheaper_count && idle_count % uwsgi.cheaper_overload == 0) {
			// we are in "cheap them all"
			return -1;
		}
	}

	return 0;

}


/*

	-- Cheaper,  backlog algorithm (supported only on Linux) --

        increse the number of workers when the listen queue is higher than uwsgi.cheaper_overload.
	Decrese when lower.

*/

int uwsgi_cheaper_algo_backlog(int can_spawn) {

	int i;
#ifdef __linux__
	int backlog = uwsgi.shared->backlog;
#else
	int backlog = 0;
#endif

	if (can_spawn && backlog > (int) uwsgi.cheaper_overload) {
		// activate the first available worker (taking step into account)
		int decheaped = 0;
		// search for cheaped workers
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].cheaped == 1 && uwsgi.workers[i].pid == 0) {
				decheaped++;
				if (decheaped >= uwsgi.cheaper_step)
					break;
			}
		}
		// return the maximum number of workers to spawn
		return decheaped;

	}
	else if (backlog < (int) uwsgi.cheaper_overload) {
		int active_workers = 0;
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].cheaped == 0 && uwsgi.workers[i].pid > 0) {
				active_workers++;
			}
		}

		if (active_workers > uwsgi.cheaper_count) {
			return -1;
		}
	}

	return 0;
}


// reload uWSGI, close unneded file descriptor, restore the original environment and re-exec the binary

void uwsgi_reload(char **argv) {
	int i;
	int waitpid_status;

	// hack for removing garbage generated by embedded loaders (like pyuwsgi)
	if (uwsgi.new_argc) {
		char **tmp_argv = argv;
		for(;;) {
			char *arg = *tmp_argv;
			if (!arg)
				break;
			if (strlen(arg) == 0)
			{
				*tmp_argv = NULL;
			}
			tmp_argv++;
		}
	}

	if (!uwsgi.master_is_reforked) {

		// call a series of waitpid to ensure all processes (gateways, mules and daemons) are dead
		for (i = 0; i < (ushared->gateways_cnt + uwsgi.daemons_cnt + uwsgi.mules_cnt); i++) {
			waitpid(WAIT_ANY, &waitpid_status, WNOHANG);
		}

		// call master cleanup hooks
		uwsgi_master_cleanup_hooks();

		if (uwsgi.exit_on_reload) {
			uwsgi_log("uWSGI: GAME OVER (insert coin)\n");
			exit(0);
		}

		// call atexit user exec
		uwsgi_exec_atexit();

		uwsgi_log("binary reloading uWSGI...\n");
	}
	else {
		uwsgi_log("fork()'ing uWSGI...\n");
	}

	// ask for configuration (if needed)
	if (uwsgi.has_emperor && uwsgi.emperor_fd_config > -1) {
		char byte = 2;
                if (write(uwsgi.emperor_fd, &byte, 1) != 1) {
                        uwsgi_error("uwsgi_reload()/write()");
                }
	}

	uwsgi_log("chdir() to %s\n", uwsgi.cwd);
	if (chdir(uwsgi.cwd)) {
		uwsgi_error("uwsgi_reload()/chdir()");
	}

	/* check fd table (a module can obviosly open some fd on initialization...) */
	uwsgi_log("closing all non-uwsgi socket fds > 2 (max_fd = %d)...\n", (int) uwsgi.max_fd);
	for (i = 3; i < (int) uwsgi.max_fd; i++) {
		if (uwsgi.close_on_exec2) fcntl(i, F_SETFD, 0);

		if (uwsgi_fd_is_safe(i)) continue;

		int found = 0;

		struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
		while (uwsgi_sock) {
			if (i == uwsgi_sock->fd) {
				uwsgi_log("found fd %d mapped to socket %d (%s)\n", i, uwsgi_get_socket_num(uwsgi_sock), uwsgi_sock->name);
				found = 1;
				break;
			}
			uwsgi_sock = uwsgi_sock->next;
		}

		uwsgi_sock = uwsgi.shared_sockets;
		while (uwsgi_sock) {
			if (i == uwsgi_sock->fd) {
				uwsgi_log("found fd %d mapped to shared socket %d (%s)\n", i, uwsgi_get_shared_socket_num(uwsgi_sock), uwsgi_sock->name);
				found = 1;
				break;
			}
			uwsgi_sock = uwsgi_sock->next;
		}

		if (found) continue;

		if (uwsgi.has_emperor) {
			if (i == uwsgi.emperor_fd) {
				continue;
			}

			if (i == uwsgi.emperor_fd_config) {
				continue;
			}
		}

		if (uwsgi.alarm_thread) {
			if (i == uwsgi.alarm_thread->queue) continue;
			if (i == uwsgi.alarm_thread->pipe[0]) continue;
			if (i == uwsgi.alarm_thread->pipe[1]) continue;
		}

		if (uwsgi.log_master) {
			if (uwsgi.original_log_fd > -1) {
				if (i == uwsgi.original_log_fd) {
					continue;
				}
			}

			if (uwsgi.shared->worker_log_pipe[0] > -1) {
				if (i == uwsgi.shared->worker_log_pipe[0]) {
					continue;
				}
			}

			if (uwsgi.shared->worker_log_pipe[1] > -1) {
				if (i == uwsgi.shared->worker_log_pipe[1]) {
					continue;
				}
			}

		}

#ifdef __APPLE__
		fcntl(i, F_SETFD, FD_CLOEXEC);
#else
		close(i);
#endif
	}

#ifndef UWSGI_IPCSEM_ATEXIT
	// free ipc semaphores if in use
	if (uwsgi.lock_engine && !strcmp(uwsgi.lock_engine, "ipcsem")) {
		uwsgi_ipcsem_clear();
	}
#endif

	uwsgi_log("running %s\n", uwsgi.binary_path);
	uwsgi_flush_logs();
	argv[0] = uwsgi.binary_path;
	//strcpy (argv[0], uwsgi.binary_path);
	if (uwsgi.log_master) {
		if (uwsgi.original_log_fd > -1) {
			dup2(uwsgi.original_log_fd, 1);
			dup2(1, 2);
		}
		if (uwsgi.shared->worker_log_pipe[0] > -1) {
			close(uwsgi.shared->worker_log_pipe[0]);
		}
		if (uwsgi.shared->worker_log_pipe[1] > -1) {
			close(uwsgi.shared->worker_log_pipe[1]);
		}
	}
	execvp(uwsgi.binary_path, argv);
	uwsgi_error("execvp()");
	// never here
	exit(1);

}

void uwsgi_fixup_fds(int wid, int muleid, struct uwsgi_gateway *ug) {

	int i;

	if (uwsgi.master_process) {
		if (uwsgi.master_queue > -1)
			close(uwsgi.master_queue);
		// close gateways
		if (!ug) {
			for (i = 0; i < ushared->gateways_cnt; i++) {
				close(ushared->gateways[i].internal_subscription_pipe[0]);
				close(ushared->gateways[i].internal_subscription_pipe[1]);
			}
		}
		struct uwsgi_gateway_socket *ugs = uwsgi.gateway_sockets;
		while (ugs) {
			if (ug && !strcmp(ug->name, ugs->owner)) {
				ugs = ugs->next;
				continue;
			}
			// do not close shared sockets !!!
			if (!ugs->shared) {
				close(ugs->fd);
			}
			ugs = ugs->next;
		}
		// fix the communication pipe
		close(uwsgi.shared->worker_signal_pipe[0]);
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].signal_pipe[0] != -1)
				close(uwsgi.workers[i].signal_pipe[0]);
			if (i != wid) {
				if (uwsgi.workers[i].signal_pipe[1] != -1)
					close(uwsgi.workers[i].signal_pipe[1]);
			}
		}

		
		if (uwsgi.shared->spooler_signal_pipe[0] != -1)
                	close(uwsgi.shared->spooler_signal_pipe[0]);
		if (uwsgi.i_am_a_spooler && uwsgi.i_am_a_spooler->pid != getpid()) {
			if (uwsgi.shared->spooler_signal_pipe[1] != -1)
				close(uwsgi.shared->spooler_signal_pipe[1]);
		}

		if (uwsgi.shared->mule_signal_pipe[0] != -1)
			close(uwsgi.shared->mule_signal_pipe[0]);

		if (muleid == 0) {
			if (uwsgi.shared->mule_signal_pipe[1] != -1)
				close(uwsgi.shared->mule_signal_pipe[1]);
			if (uwsgi.shared->mule_queue_pipe[1] != -1)
				close(uwsgi.shared->mule_queue_pipe[1]);
		}

		for (i = 0; i < uwsgi.mules_cnt; i++) {
			if (uwsgi.mules[i].signal_pipe[0] != -1)
				close(uwsgi.mules[i].signal_pipe[0]);
			if (muleid != i + 1) {
				if (uwsgi.mules[i].signal_pipe[1] != -1)
					close(uwsgi.mules[i].signal_pipe[1]);
				if (uwsgi.mules[i].queue_pipe[1] != -1)
					close(uwsgi.mules[i].queue_pipe[1]);
			}
		}

		for (i = 0; i < uwsgi.farms_cnt; i++) {
			if (uwsgi.farms[i].signal_pipe[0] != -1)
				close(uwsgi.farms[i].signal_pipe[0]);

			if (muleid == 0) {
				if (uwsgi.farms[i].signal_pipe[1] != -1)
					close(uwsgi.farms[i].signal_pipe[1]);
				if (uwsgi.farms[i].queue_pipe[1] != -1)
					close(uwsgi.farms[i].queue_pipe[1]);
			}
		}

		if (uwsgi.master_fifo_fd > -1) close(uwsgi.master_fifo_fd);

		if (uwsgi.notify_socket_fd > -1) close(uwsgi.notify_socket_fd);

#ifdef __linux__
		for(i=0;i<uwsgi.setns_fds_count;i++) {
			close(uwsgi.setns_fds[i]);
		}
#endif

		// fd alarms
		struct uwsgi_alarm_fd *uafd = uwsgi.alarm_fds;
        	while(uafd) {
			close(uafd->fd);
                	uafd = uafd->next;
        	}
	}


}

int uwsgi_respawn_worker(int wid) {

	int respawns = uwsgi.workers[wid].respawn_count;
	// the workers is not accepting (obviously)
	uwsgi.workers[wid].accepting = 0;
	// we count the respawns before errors...
	uwsgi.workers[wid].respawn_count++;
	// ... same for update time
	uwsgi.workers[wid].last_spawn = uwsgi.current_time;
	// ... and memory/harakiri
	uwsgi.workers[wid].harakiri = 0;
	uwsgi.workers[wid].user_harakiri = 0;
	uwsgi.workers[wid].pending_harakiri = 0;
	uwsgi.workers[wid].rss_size = 0;
	uwsgi.workers[wid].vsz_size = 0;
	// ... reset stopped_at
	uwsgi.workers[wid].cursed_at = 0;
	uwsgi.workers[wid].no_mercy_at = 0;

	// internal statuses should be reset too

	uwsgi.workers[wid].cheaped = 0;
	// SUSPENSION is managed by the user, not the master...
	//uwsgi.workers[wid].suspended = 0;
	uwsgi.workers[wid].sig = 0;

	// this is required for various checks
	uwsgi.workers[wid].delta_requests = 0;

	int i;

	if (uwsgi.threaded_logger) {
		pthread_mutex_lock(&uwsgi.threaded_logger_lock);
	}


	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->pre_uwsgi_fork) {
			uwsgi.p[i]->pre_uwsgi_fork();
		}
	}

	pid_t pid = uwsgi_fork(uwsgi.workers[wid].name);

	if (pid == 0) {
		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->post_uwsgi_fork) {
				uwsgi.p[i]->post_uwsgi_fork(1);
			}
		}

		signal(SIGWINCH, worker_wakeup);
		signal(SIGTSTP, worker_wakeup);
		uwsgi.mywid = wid;
		uwsgi.mypid = getpid();
		// pid is updated by the master
		//uwsgi.workers[uwsgi.mywid].pid = uwsgi.mypid;
		// OVERENGINEERING (just to be safe)
		uwsgi.workers[uwsgi.mywid].id = uwsgi.mywid;
		/*
		   uwsgi.workers[uwsgi.mywid].harakiri = 0;
		   uwsgi.workers[uwsgi.mywid].user_harakiri = 0;
		   uwsgi.workers[uwsgi.mywid].rss_size = 0;
		   uwsgi.workers[uwsgi.mywid].vsz_size = 0;
		 */
		// do not reset worker counters on reload !!!
		//uwsgi.workers[uwsgi.mywid].requests = 0;
		// ...but maintain a delta counter (yes this is racy in multithread)
		//uwsgi.workers[uwsgi.mywid].delta_requests = 0;
		//uwsgi.workers[uwsgi.mywid].failed_requests = 0;
		//uwsgi.workers[uwsgi.mywid].respawn_count++;
		//uwsgi.workers[uwsgi.mywid].last_spawn = uwsgi.current_time;
		uwsgi.workers[uwsgi.mywid].manage_next_request = 1;
		/*
		   uwsgi.workers[uwsgi.mywid].cheaped = 0;
		   uwsgi.workers[uwsgi.mywid].suspended = 0;
		   uwsgi.workers[uwsgi.mywid].sig = 0;
		 */

		// reset the apps count with a copy from the master 
		uwsgi.workers[uwsgi.mywid].apps_cnt = uwsgi.workers[0].apps_cnt;

		// reset wsgi_request structures
		for(i=0;i<uwsgi.cores;i++) {
			uwsgi.workers[uwsgi.mywid].cores[i].in_request = 0;
			memset(&uwsgi.workers[uwsgi.mywid].cores[i].req, 0, sizeof(struct wsgi_request));
			memset(uwsgi.workers[uwsgi.mywid].cores[i].buffer, 0, sizeof(struct uwsgi_header));
		}

		uwsgi_fixup_fds(wid, 0, NULL);

		uwsgi.my_signal_socket = uwsgi.workers[wid].signal_pipe[1];

		if (uwsgi.master_process) {
			if ((uwsgi.workers[uwsgi.mywid].respawn_count || uwsgi.status.is_cheap)) {
				for (i = 0; i < 256; i++) {
					if (uwsgi.p[i]->master_fixup) {
						uwsgi.p[i]->master_fixup(1);
					}
				}
			}
		}

		return 1;
	}
	else if (pid < 1) {
		uwsgi_error("fork()");
	}
	else {
		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->post_uwsgi_fork) {
				uwsgi.p[i]->post_uwsgi_fork(0);
			}
		}

		// the pid is set only in the master, as the worker should never use it
		uwsgi.workers[wid].pid = pid;

		if (respawns > 0) {
			uwsgi_log("Respawned uWSGI worker %d (new pid: %d)\n", wid, (int) pid);
		}
		else {
			uwsgi_log("spawned uWSGI worker %d (pid: %d, cores: %d)\n", wid, pid, uwsgi.cores);
		}
	}

	if (uwsgi.threaded_logger) {
		pthread_mutex_unlock(&uwsgi.threaded_logger_lock);
	}


	return 0;
}

struct uwsgi_stats *uwsgi_master_generate_stats() {

	int i;

	struct uwsgi_stats *us = uwsgi_stats_new(8192);

	if (uwsgi_stats_keyval_comma(us, "version", UWSGI_VERSION))
		goto end;

#ifdef __linux__
	if (uwsgi_stats_keylong_comma(us, "listen_queue", (unsigned long long) uwsgi.shared->backlog))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "listen_queue_errors", (unsigned long long) uwsgi.shared->backlog_errors))
		goto end;
#endif

	int signal_queue = 0;
	if (ioctl(uwsgi.shared->worker_signal_pipe[1], FIONREAD, &signal_queue)) {
		uwsgi_error("uwsgi_master_generate_stats() -> ioctl()\n");
	}

	if (uwsgi_stats_keylong_comma(us, "signal_queue", (unsigned long long) signal_queue))
		goto end;

	if (uwsgi_stats_keylong_comma(us, "load", (unsigned long long) uwsgi.shared->load))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) getpid()))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "uid", (unsigned long long) getuid()))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "gid", (unsigned long long) getgid()))
		goto end;

	char *cwd = uwsgi_get_cwd();
	if (uwsgi_stats_keyval_comma(us, "cwd", cwd)) {
		free(cwd);
		goto end;
	}
	free(cwd);

	if (uwsgi.daemons) {
		if (uwsgi_stats_key(us, "daemons"))
			goto end;
		if (uwsgi_stats_list_open(us))
			goto end;

		struct uwsgi_daemon *ud = uwsgi.daemons;
		while (ud) {
			if (uwsgi_stats_object_open(us))
				goto end;

			// allocate 2x the size of original command
			// in case we need to escape all chars
			char *cmd = uwsgi_malloc((strlen(ud->command)*2)+1);
			escape_json(ud->command, strlen(ud->command), cmd);
			if (uwsgi_stats_keyval_comma(us, "cmd", cmd)) {
				free(cmd);
				goto end;
			}
			free(cmd);

			if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) (ud->pid < 0) ? 0 : ud->pid))
				goto end;
			if (uwsgi_stats_keylong(us, "respawns", (unsigned long long) ud->respawns ? 0 : ud->respawns))
				goto end;
			if (uwsgi_stats_object_close(us))
				goto end;
			if (ud->next) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
			ud = ud->next;
		}
		if (uwsgi_stats_list_close(us))
			goto end;
		if (uwsgi_stats_comma(us))
			goto end;
	}

	if (uwsgi_stats_key(us, "locks"))
		goto end;
	if (uwsgi_stats_list_open(us))
		goto end;

	struct uwsgi_lock_item *uli = uwsgi.registered_locks;
	while (uli) {
		if (uwsgi_stats_object_open(us))
			goto end;
		if (uwsgi_stats_keylong(us, uli->id, (unsigned long long) uli->pid))
			goto end;
		if (uwsgi_stats_object_close(us))
			goto end;
		if (uli->next) {
			if (uwsgi_stats_comma(us))
				goto end;
		}
		uli = uli->next;
	}

	if (uwsgi_stats_list_close(us))
		goto end;
	if (uwsgi_stats_comma(us))
		goto end;

	if (uwsgi.caches) {

		
		if (uwsgi_stats_key(us, "caches"))
                goto end;

		if (uwsgi_stats_list_open(us)) goto end;

		struct uwsgi_cache *uc = uwsgi.caches;
		while(uc) {
			if (uwsgi_stats_object_open(us))
                        	goto end;

			if (uwsgi_stats_keyval_comma(us, "name", uc->name ? uc->name : "default"))
                        	goto end;

			if (uwsgi_stats_keyval_comma(us, "hash", uc->hash->name))
                        	goto end;

			if (uwsgi_stats_keylong_comma(us, "hashsize", (unsigned long long) uc->hashsize))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "keysize", (unsigned long long) uc->keysize))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "max_items", (unsigned long long) uc->max_items))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "blocks", (unsigned long long) uc->blocks))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "blocksize", (unsigned long long) uc->blocksize))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "items", (unsigned long long) uc->n_items))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "hits", (unsigned long long) uc->hits))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "miss", (unsigned long long) uc->miss))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "full", (unsigned long long) uc->full))
				goto end;

			if (uwsgi_stats_keylong(us, "last_modified_at", (unsigned long long) uc->last_modified_at))
				goto end;

			if (uwsgi_stats_object_close(us))
				goto end;

			if (uc->next) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
			uc = uc->next;
		}

		if (uwsgi_stats_list_close(us))
		goto end;

		if (uwsgi_stats_comma(us))
		goto end;
	}

	if (uwsgi.has_metrics && !uwsgi.stats_no_metrics) {
		if (uwsgi_stats_key(us, "metrics"))
                	goto end;

		if (uwsgi_stats_object_open(us))
			goto end;

		uwsgi_rlock(uwsgi.metrics_lock);
		struct uwsgi_metric *um = uwsgi.metrics;
		while(um) {
        		int64_t um_val = *um->value;

			if (uwsgi_stats_key(us, um->name)) {
				uwsgi_rwunlock(uwsgi.metrics_lock);
                		goto end;
			}

			if (uwsgi_stats_object_open(us)) {
				uwsgi_rwunlock(uwsgi.metrics_lock);
                                goto end;
			}

			if (uwsgi_stats_keylong(us, "type", (long long) um->type)) {
        			uwsgi_rwunlock(uwsgi.metrics_lock);
				goto end;
			} 

			if (uwsgi_stats_comma(us)) {
        			uwsgi_rwunlock(uwsgi.metrics_lock);
				goto end;
			}

			if (uwsgi_stats_keyval_comma(us, "oid", um->oid ? um->oid : "")) {
                                uwsgi_rwunlock(uwsgi.metrics_lock);
                                goto end;
                        }

			if (uwsgi_stats_keyslong(us, "value", (long long) um_val)) {
        			uwsgi_rwunlock(uwsgi.metrics_lock);
				goto end;
			} 

			if (uwsgi_stats_object_close(us)) {
                                uwsgi_rwunlock(uwsgi.metrics_lock);
                                goto end;
                        }

			um = um->next;
			if (um) {
				if (uwsgi_stats_comma(us)) {
        				uwsgi_rwunlock(uwsgi.metrics_lock);
					goto end;
				}
			}
		}
        	uwsgi_rwunlock(uwsgi.metrics_lock);

		if (uwsgi_stats_object_close(us))
			goto end;

		if (uwsgi_stats_comma(us))
		goto end;
	}

	if (uwsgi_stats_key(us, "sockets"))
		goto end;

	if (uwsgi_stats_list_open(us))
		goto end;

	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	while (uwsgi_sock) {
		if (uwsgi_stats_object_open(us))
			goto end;

		if (uwsgi_stats_keyval_comma(us, "name", uwsgi_sock->name))
			goto end;

		if (uwsgi_stats_keyval_comma(us, "proto", uwsgi_sock->proto_name ? uwsgi_sock->proto_name : "uwsgi"))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "queue", (unsigned long long) uwsgi_sock->queue))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "max_queue", (unsigned long long) uwsgi_sock->max_queue))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "shared", (unsigned long long) uwsgi_sock->shared))
			goto end;

		if (uwsgi_stats_keylong(us, "can_offload", (unsigned long long) uwsgi_sock->can_offload))
			goto end;

		if (uwsgi_stats_object_close(us))
			goto end;

		uwsgi_sock = uwsgi_sock->next;
		if (uwsgi_sock) {
			if (uwsgi_stats_comma(us))
				goto end;
		}
	}

	if (uwsgi_stats_list_close(us))
		goto end;

	if (uwsgi_stats_comma(us))
		goto end;

	if (uwsgi_stats_key(us, "workers"))
		goto end;
	if (uwsgi_stats_list_open(us))
		goto end;

	for (i = 0; i < uwsgi.numproc; i++) {
		if (uwsgi_stats_object_open(us))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "id", (unsigned long long) uwsgi.workers[i + 1].id))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) uwsgi.workers[i + 1].pid))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "accepting", (unsigned long long) uwsgi.workers[i + 1].accepting))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "requests", (unsigned long long) uwsgi.workers[i + 1].requests))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "delta_requests", (unsigned long long) uwsgi.workers[i + 1].delta_requests))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "exceptions", (unsigned long long) uwsgi_worker_exceptions(i + 1)))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "harakiri_count", (unsigned long long) uwsgi.workers[i + 1].harakiri_count))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "signals", (unsigned long long) uwsgi.workers[i + 1].signals))
			goto end;

		if (ioctl(uwsgi.workers[i + 1].signal_pipe[1], FIONREAD, &signal_queue)) {
			uwsgi_error("uwsgi_master_generate_stats() -> ioctl()\n");
		}

		if (uwsgi_stats_keylong_comma(us, "signal_queue", (unsigned long long) signal_queue))
			goto end;

		if (uwsgi.workers[i + 1].cheaped) {
			if (uwsgi_stats_keyval_comma(us, "status", "cheap"))
				goto end;
		}
		else if (uwsgi.workers[i + 1].suspended && !uwsgi_worker_is_busy(i+1)) {
			if (uwsgi_stats_keyval_comma(us, "status", "pause"))
				goto end;
		}
		else {
			if (uwsgi.workers[i + 1].sig) {
				if (uwsgi_stats_keyvalnum_comma(us, "status", "sig", (unsigned long long) uwsgi.workers[i + 1].signum))
					goto end;
			}
			else if (uwsgi_worker_is_busy(i+1)) {
				if (uwsgi_stats_keyval_comma(us, "status", "busy"))
					goto end;
			}
			else {
				if (uwsgi_stats_keyval_comma(us, "status", "idle"))
					goto end;
			}
		}

		if (uwsgi_stats_keylong_comma(us, "rss", (unsigned long long) uwsgi.workers[i + 1].rss_size))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "vsz", (unsigned long long) uwsgi.workers[i + 1].vsz_size))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "running_time", (unsigned long long) uwsgi.workers[i + 1].running_time))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "last_spawn", (unsigned long long) uwsgi.workers[i + 1].last_spawn))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "respawn_count", (unsigned long long) uwsgi.workers[i + 1].respawn_count))
			goto end;

		if (uwsgi_stats_keylong_comma(us, "tx", (unsigned long long) uwsgi.workers[i + 1].tx))
			goto end;
		if (uwsgi_stats_keylong_comma(us, "avg_rt", (unsigned long long) uwsgi.workers[i + 1].avg_response_time))
			goto end;

		// applications list
		if (uwsgi_stats_key(us, "apps"))
			goto end;
		if (uwsgi_stats_list_open(us))
			goto end;

		int j;

		for (j = 0; j < uwsgi.workers[i + 1].apps_cnt; j++) {
			struct uwsgi_app *ua = &uwsgi.workers[i + 1].apps[j];

			if (uwsgi_stats_object_open(us))
				goto end;
			if (uwsgi_stats_keylong_comma(us, "id", (unsigned long long) j))
				goto end;
			if (uwsgi_stats_keylong_comma(us, "modifier1", (unsigned long long) ua->modifier1))
				goto end;

			if (uwsgi_stats_keyvaln_comma(us, "mountpoint", ua->mountpoint, ua->mountpoint_len))
				goto end;
			if (uwsgi_stats_keylong_comma(us, "startup_time", ua->startup_time))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "requests", ua->requests))
				goto end;
			if (uwsgi_stats_keylong_comma(us, "exceptions", ua->exceptions))
				goto end;

			if (*ua->chdir) {
				if (uwsgi_stats_keyval(us, "chdir", ua->chdir))
					goto end;
			}
			else {
				if (uwsgi_stats_keyval(us, "chdir", ""))
					goto end;
			}

			if (uwsgi_stats_object_close(us))
				goto end;

			if (j < uwsgi.workers[i + 1].apps_cnt - 1) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
		}


		if (uwsgi_stats_list_close(us))
			goto end;

		if (uwsgi.stats_no_cores) goto nocores;

		if (uwsgi_stats_comma(us))
			goto end;

		// cores list
		if (uwsgi_stats_key(us, "cores"))
			goto end;
		if (uwsgi_stats_list_open(us))
			goto end;

		for (j = 0; j < uwsgi.cores; j++) {
			struct uwsgi_core *uc = &uwsgi.workers[i + 1].cores[j];
			if (uwsgi_stats_object_open(us))
				goto end;
			if (uwsgi_stats_keylong_comma(us, "id", (unsigned long long) j))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "requests", (unsigned long long) uc->requests))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "static_requests", (unsigned long long) uc->static_requests))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "routed_requests", (unsigned long long) uc->routed_requests))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "offloaded_requests", (unsigned long long) uc->offloaded_requests))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "write_errors", (unsigned long long) uc->write_errors))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "read_errors", (unsigned long long) uc->read_errors))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "in_request", (unsigned long long) uc->in_request))
				goto end;

			if (uwsgi_stats_key(us, "vars"))
				goto end;

			if (uwsgi_stats_list_open(us))
                        	goto end;

			if (uwsgi_stats_dump_vars(us, uc)) goto end;

			if (uwsgi_stats_list_close(us))
				goto end;

			if (uwsgi_stats_comma(us)) goto end;

			if (uwsgi_stats_key(us, "req_info"))
				goto end;

			if (uwsgi_stats_object_open(us))
				goto end;

			if (uwsgi_stats_dump_request(us, uc)) goto end;

			if (uwsgi_stats_object_close(us))
				goto end;

			if (uwsgi_stats_object_close(us))
				goto end;

			if (j < uwsgi.cores - 1) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
		}

		if (uwsgi_stats_list_close(us))
			goto end;

nocores:

		if (uwsgi_stats_object_close(us))
			goto end;


		if (i < uwsgi.numproc - 1) {
			if (uwsgi_stats_comma(us))
				goto end;
		}
	}

	if (uwsgi_stats_list_close(us))
		goto end;

	struct uwsgi_spooler *uspool = uwsgi.spoolers;
	if (uspool) {
		if (uwsgi_stats_comma(us))
			goto end;
		if (uwsgi_stats_key(us, "spoolers"))
			goto end;
		if (uwsgi_stats_list_open(us))
			goto end;
		while (uspool) {
			if (uwsgi_stats_object_open(us))
				goto end;

			if (uwsgi_stats_keyval_comma(us, "dir", uspool->dir))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) uspool->pid))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "tasks", (unsigned long long) uspool->tasks))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "respawns", (unsigned long long) uspool->respawned))
				goto end;

			if (uwsgi_stats_keylong(us, "running", (unsigned long long) uspool->running))
				goto end;

			if (uwsgi_stats_object_close(us))
				goto end;
			uspool = uspool->next;
			if (uspool) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
		}
		if (uwsgi_stats_list_close(us))
			goto end;
	}

	struct uwsgi_cron *ucron = uwsgi.crons;
	if (ucron) {
		if (uwsgi_stats_comma(us))
			goto end;
		if (uwsgi_stats_key(us, "crons"))
			goto end;
		if (uwsgi_stats_list_open(us))
			goto end;
		while (ucron) {
			if (uwsgi_stats_object_open(us))
				goto end;

			if (uwsgi_stats_keyslong_comma(us, "minute", (long long) ucron->minute))
				goto end;

			if (uwsgi_stats_keyslong_comma(us, "hour", (long long) ucron->hour))
				goto end;

			if (uwsgi_stats_keyslong_comma(us, "day", (long long) ucron->day))
				goto end;

			if (uwsgi_stats_keyslong_comma(us, "month", (long long) ucron->month))
				goto end;

			if (uwsgi_stats_keyslong_comma(us, "week", (long long) ucron->week))
				goto end;

			char *cmd = uwsgi_malloc((strlen(ucron->command)*2)+1);
			escape_json(ucron->command, strlen(ucron->command), cmd);
			if (uwsgi_stats_keyval_comma(us, "command", cmd)) {
				free(cmd);
				goto end;
			}
			free(cmd);

			if (uwsgi_stats_keylong_comma(us, "unique", (unsigned long long) ucron->unique))
				goto end;

#ifdef UWSGI_SSL
			if (uwsgi_stats_keyval_comma(us, "legion", ucron->legion ? ucron->legion : ""))
				goto end;
#endif

			if (uwsgi_stats_keyslong_comma(us, "pid", (long long) ucron->pid))
				goto end;

			if (uwsgi_stats_keylong(us, "started_at", (unsigned long long) ucron->started_at))
				goto end;

			if (uwsgi_stats_object_close(us))
				goto end;

			ucron = ucron->next;
			if (ucron) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
		}
		if (uwsgi_stats_list_close(us))
			goto end;
	}

#ifdef UWSGI_SSL
	struct uwsgi_legion *legion = NULL;
	if (uwsgi.legions) {

		if (uwsgi_stats_comma(us))
			goto end;

		if (uwsgi_stats_key(us, "legions"))
			goto end;

		if (uwsgi_stats_list_open(us))
			goto end;

		legion = uwsgi.legions;
		while (legion) {
			if (uwsgi_stats_object_open(us))
				goto end;

			if (uwsgi_stats_keyval_comma(us, "legion", legion->legion))
				goto end;

			if (uwsgi_stats_keyval_comma(us, "addr", legion->addr))
				goto end;

			if (uwsgi_stats_keyval_comma(us, "uuid", legion->uuid))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "valor", (unsigned long long) legion->valor))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "checksum", (unsigned long long) legion->checksum))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "quorum", (unsigned long long) legion->quorum))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "i_am_the_lord", (unsigned long long) legion->i_am_the_lord))
				goto end;

			if (uwsgi_stats_keylong_comma(us, "lord_valor", (unsigned long long) legion->lord_valor))
				goto end;

			if (uwsgi_stats_keyvaln_comma(us, "lord_uuid", legion->lord_uuid, 36))
				goto end;

			// legion nodes start
			if (uwsgi_stats_key(us, "nodes"))
                                goto end;

                        if (uwsgi_stats_list_open(us))
                                goto end;

                        struct uwsgi_string_list *nodes = legion->nodes;
                        while (nodes) {

				if (uwsgi_stats_str(us, nodes->value))
                                	goto end;

                                nodes = nodes->next;
                                if (nodes) {
                                        if (uwsgi_stats_comma(us))
                                                goto end;
                                }
                        }

			if (uwsgi_stats_list_close(us))
				goto end;

                        if (uwsgi_stats_comma(us))
                        	goto end;


			// legion members start
			if (uwsgi_stats_key(us, "members"))
				goto end;

			if (uwsgi_stats_list_open(us))
				goto end;

			uwsgi_rlock(legion->lock);
			struct uwsgi_legion_node *node = legion->nodes_head;
			while (node) {
				if (uwsgi_stats_object_open(us))
					goto unlock_legion_mutex;

				if (uwsgi_stats_keyvaln_comma(us, "name", node->name, node->name_len))
					goto unlock_legion_mutex;

				if (uwsgi_stats_keyval_comma(us, "uuid", node->uuid))
					goto unlock_legion_mutex;

				if (uwsgi_stats_keylong_comma(us, "valor", (unsigned long long) node->valor))
					goto unlock_legion_mutex;

				if (uwsgi_stats_keylong_comma(us, "checksum", (unsigned long long) node->checksum))
					goto unlock_legion_mutex;

				if (uwsgi_stats_keylong(us, "last_seen", (unsigned long long) node->last_seen))
					goto unlock_legion_mutex;

				if (uwsgi_stats_object_close(us))
					goto unlock_legion_mutex;

				node = node->next;
				if (node) {
					if (uwsgi_stats_comma(us))
						goto unlock_legion_mutex;
				}
			}
			uwsgi_rwunlock(legion->lock);

			if (uwsgi_stats_list_close(us))
				goto end;
			// legion nodes end

			if (uwsgi_stats_object_close(us))
				goto end;

			legion = legion->next;
			if (legion) {
				if (uwsgi_stats_comma(us))
					goto end;
			}
		}

		if (uwsgi_stats_list_close(us))
			goto end;

	}
#endif

	if (uwsgi_stats_object_close(us))
		goto end;

	return us;
#ifdef UWSGI_SSL
unlock_legion_mutex:
	if (legion)
		uwsgi_rwunlock(legion->lock);
#endif
end:
	free(us->base);
	free(us);
	return NULL;
}

void uwsgi_register_cheaper_algo(char *name, int (*func) (int)) {

	struct uwsgi_cheaper_algo *uca = uwsgi.cheaper_algos;

	if (!uca) {
		uwsgi.cheaper_algos = uwsgi_malloc(sizeof(struct uwsgi_cheaper_algo));
		uca = uwsgi.cheaper_algos;
	}
	else {
		while (uca) {
			if (!uca->next) {
				uca->next = uwsgi_malloc(sizeof(struct uwsgi_cheaper_algo));
				uca = uca->next;
				break;
			}
			uca = uca->next;
		}

	}

	uca->name = name;
	uca->func = func;
	uca->next = NULL;

#ifdef UWSGI_DEBUG
	uwsgi_log("[uwsgi-cheaper-algo] registered \"%s\"\n", uca->name);
#endif
}

void trigger_harakiri(int i) {
	int j;
	uwsgi_log_verbose("*** HARAKIRI ON WORKER %d (pid: %d, try: %d, graceful: %s) ***\n", i,
				uwsgi.workers[i].pid,
				uwsgi.workers[i].pending_harakiri + 1,
				uwsgi.workers[i].pending_harakiri > 0 ? "no": "yes");
	if (uwsgi.harakiri_verbose) {
#ifdef __linux__
		int proc_file;
		char proc_buf[4096];
		char proc_name[64];
		ssize_t proc_len;

		if (snprintf(proc_name, 64, "/proc/%d/syscall", uwsgi.workers[i].pid) > 0) {
			memset(proc_buf, 0, 4096);
			proc_file = open(proc_name, O_RDONLY);
			if (proc_file >= 0) {
				proc_len = read(proc_file, proc_buf, 4096);
				if (proc_len > 0) {
					uwsgi_log("HARAKIRI: -- syscall> %s", proc_buf);
				}
				close(proc_file);
			}
		}

		if (snprintf(proc_name, 64, "/proc/%d/wchan", uwsgi.workers[i].pid) > 0) {
			memset(proc_buf, 0, 4096);

			proc_file = open(proc_name, O_RDONLY);
			if (proc_file >= 0) {
				proc_len = read(proc_file, proc_buf, 4096);
				if (proc_len > 0) {
					uwsgi_log("HARAKIRI: -- wchan> %s\n", proc_buf);
				}
				close(proc_file);
			}
		}

#endif
	}

	if (uwsgi.workers[i].pid > 0) {
		for (j = 0; j < uwsgi.gp_cnt; j++) {
			if (uwsgi.gp[j]->harakiri) {
				uwsgi.gp[j]->harakiri(i);
			}
		}
		for (j = 0; j < 256; j++) {
			if (uwsgi.p[j]->harakiri) {
				uwsgi.p[j]->harakiri(i);
			}
		}

		uwsgi_dump_worker(i, "HARAKIRI");
		if (uwsgi.workers[i].pending_harakiri == 0 && uwsgi.harakiri_graceful_timeout > 0) {
			kill(uwsgi.workers[i].pid, uwsgi.harakiri_graceful_signal);
		} else {
			kill(uwsgi.workers[i].pid, SIGKILL);
		}
		if (!uwsgi.workers[i].pending_harakiri)
			uwsgi.workers[i].harakiri_count++;
		uwsgi.workers[i].pending_harakiri++;
	}

}

void uwsgi_master_fix_request_counters() {
	int i;
	uint64_t total_counter = 0;
        for (i = 1; i <= uwsgi.numproc;i++) {
		uint64_t tmp_counter = 0;
		int j;
		for(j=0;j<uwsgi.cores;j++) {
			tmp_counter += uwsgi.workers[i].cores[j].requests;
		}
		uwsgi.workers[i].requests = tmp_counter;
		total_counter += tmp_counter;
	}

	uwsgi.workers[0].requests = total_counter;
}


int uwsgi_cron_task_needs_execution(struct tm *uwsgi_cron_delta, int minute, int hour, int day, int month, int week) {

	int uc_minute, uc_hour, uc_day, uc_month, uc_week;

	uc_minute = minute;
	uc_hour = hour;
	uc_day = day;
	uc_month = month;
	// support 7 as alias for sunday (0) to match crontab behaviour
	uc_week = week == 7 ? 0 : week;

	// negative values as interval -1 = * , -5 = */5
	if (minute < 0) {
		if ((uwsgi_cron_delta->tm_min % abs(minute)) == 0) {
			uc_minute = uwsgi_cron_delta->tm_min;
		}
	}
	if (hour < 0) {
		if ((uwsgi_cron_delta->tm_hour % abs(hour)) == 0) {
			uc_hour = uwsgi_cron_delta->tm_hour;
		}
	}
	if (month < 0) {
		if ((uwsgi_cron_delta->tm_mon % abs(month)) == 0) {
			uc_month = uwsgi_cron_delta->tm_mon;
		}
	}
	if (day < 0) {
		if ((uwsgi_cron_delta->tm_mday % abs(day)) == 0) {
			uc_day = uwsgi_cron_delta->tm_mday;
		}
	}
	if (week < 0) {
		if ((uwsgi_cron_delta->tm_wday % abs(week)) == 0) {
			uc_week = uwsgi_cron_delta->tm_wday;
		}
	}

	int run_task = 0;
	// mday and wday are ORed
	if (day >= 0 && week >= 0) {
		if (uwsgi_cron_delta->tm_min == uc_minute && uwsgi_cron_delta->tm_hour == uc_hour && uwsgi_cron_delta->tm_mon == uc_month && (uwsgi_cron_delta->tm_mday == uc_day || uwsgi_cron_delta->tm_wday == uc_week)) {
			run_task = 1;
		}
	}
	else {
		if (uwsgi_cron_delta->tm_min == uc_minute && uwsgi_cron_delta->tm_hour == uc_hour && uwsgi_cron_delta->tm_mon == uc_month && uwsgi_cron_delta->tm_mday == uc_day && uwsgi_cron_delta->tm_wday == uc_week) {
			run_task = 1;
		}
	}

	return run_task;

}

static void add_reload_fds(struct uwsgi_string_list *list, char *type) {
	struct uwsgi_string_list *usl = list;
	while(usl) {
		char *strc = uwsgi_str(usl->value);
		char *space = strchr(strc, ' ');
		if (space) {
			*space = 0;
			usl->custom_ptr = space+1;
		}
		char *colon = strchr(strc, ':');
		if (colon) {
			*colon = 0;
			usl->custom2 = strtoul(colon+1, NULL, 10);
		}
		usl->custom = strtoul(strc, NULL, 10);
		if (!usl->custom2) usl->custom2 = 1;
		event_queue_add_fd_read(uwsgi.master_queue, usl->custom);
		uwsgi_add_safe_fd(usl->custom);
		uwsgi_log("added %s reload monitor for fd %d (read size: %llu)\n", type, (int) usl->custom, usl->custom2);
		usl = usl->next;
	}
}

void uwsgi_add_reload_fds() {
	add_reload_fds(uwsgi.reload_on_fd, "graceful");
	add_reload_fds(uwsgi.brutal_reload_on_fd, "brutal");
}

void uwsgi_refork_master() {
	pid_t pid = fork();
	if (pid < 0) {
		uwsgi_error("uwsgi_refork_master()/fork()");
		return;
	}

	if (pid > 0) {
		uwsgi_log_verbose("new master copy spawned with pid %d\n", (int) pid);
		return;
	}

	// detach from the old master
	setsid();

	uwsgi.master_is_reforked = 1;
	uwsgi_reload(uwsgi.argv);
	// never here
	exit(1);
}

void uwsgi_cheaper_increase() {
	uwsgi.cheaper_fifo_delta++;
}

void uwsgi_cheaper_decrease() {
        uwsgi.cheaper_fifo_delta--;
}

void uwsgi_go_cheap() {
	int i;
	int waitpid_status;
	if (uwsgi.status.is_cheap) return;
	uwsgi_log_verbose("going cheap...\n");
	uwsgi.status.is_cheap = 1;
                for (i = 1; i <= uwsgi.numproc; i++) {
                        uwsgi.workers[i].cheaped = 1;
			uwsgi.workers[i].rss_size = 0;
        		uwsgi.workers[i].vsz_size = 0;
                        if (uwsgi.workers[i].pid == 0)
                                continue;
			uwsgi_log("killing worker %d (pid: %d)\n", i, (int) uwsgi.workers[i].pid);
                        kill(uwsgi.workers[i].pid, SIGKILL);
                        if (waitpid(uwsgi.workers[i].pid, &waitpid_status, 0) < 0) {
                                if (errno != ECHILD)
                                        uwsgi_error("uwsgi_go_cheap()/waitpid()");
                        }
                }
                uwsgi_add_sockets_to_queue(uwsgi.master_queue, -1);
                uwsgi_log("cheap mode enabled: waiting for socket connection...\n");
}

#ifdef __linux__
void uwsgi_setns_preopen() {
	struct dirent *de;
        DIR *ns = opendir("/proc/self/ns");
        if (!ns) {
                uwsgi_error("uwsgi_setns_preopen()/opendir()");
		exit(1);
        }
        while ((de = readdir(ns)) != NULL) {
                if (strlen(de->d_name) > 0 && de->d_name[0] == '.') continue;
		if (!strcmp(de->d_name, "user")) continue;
                struct uwsgi_string_list *usl = NULL;
                int found = 0;
                uwsgi_foreach(usl, uwsgi.setns_socket_skip) {
                        if (!strcmp(de->d_name, usl->value)) {
                                found = 1;
                                break;
                        }
                }
                if (found) continue;
                char *filename = uwsgi_concat2("/proc/self/ns/", de->d_name);
                uwsgi.setns_fds[uwsgi.setns_fds_count] = open(filename, O_RDONLY);
                if (uwsgi.setns_fds[uwsgi.setns_fds_count] < 0) {
                        uwsgi_error_open(filename);
                        free(filename);
			exit(1);
                }
                free(filename);
                uwsgi.setns_fds_count++;
        }
	closedir(ns);
}
void uwsgi_master_manage_setns(int fd) {

        struct sockaddr_un snsun;
        socklen_t snsun_len = sizeof(struct sockaddr_un);

        int setns_client = accept(fd, (struct sockaddr *) &snsun, &snsun_len);
        if (setns_client < 0) {
                uwsgi_error("uwsgi_master_manage_setns()/accept()");
                return;
        }

	int i;
	int tmp_fds[64];
	int *fds = tmp_fds;
        int num_fds = 0;

	struct msghdr sn_msg;
        void *sn_msg_control;
        struct iovec sn_iov[2];
        struct cmsghdr *cmsg;
	DIR *ns = NULL;

	if (uwsgi.setns_fds_count) {
		fds = uwsgi.setns_fds;
		num_fds = uwsgi.setns_fds_count;
		goto send;
	}

	struct dirent *de;
	ns = opendir("/proc/self/ns");
	if (!ns) {
		close(setns_client);
		uwsgi_error("uwsgi_master_manage_setns()/opendir()");
		return;
	}
	while ((de = readdir(ns)) != NULL) {
		if (strlen(de->d_name) > 0 && de->d_name[0] == '.') continue;
		if (!strcmp(de->d_name, "user")) continue;
		struct uwsgi_string_list *usl = NULL;
		int found = 0;
		uwsgi_foreach(usl, uwsgi.setns_socket_skip) {
			if (!strcmp(de->d_name, usl->value)) {
				found = 1;
				break;
			}
		}
		if (found) continue;
		char *filename = uwsgi_concat2("/proc/self/ns/", de->d_name);
		fds[num_fds] = open(filename, O_RDONLY);
		if (fds[num_fds] < 0) {
			uwsgi_error_open(filename);
			free(filename);
			goto clear;
		}
		free(filename);
		num_fds++;
	}

send:

        sn_msg_control = uwsgi_malloc(CMSG_SPACE(sizeof(int) * num_fds));

        sn_iov[0].iov_base = "uwsgi-setns";
        sn_iov[0].iov_len = 11;
        sn_iov[1].iov_base = &num_fds;
        sn_iov[1].iov_len = sizeof(int);

        sn_msg.msg_name = NULL;
        sn_msg.msg_namelen = 0;

        sn_msg.msg_iov = sn_iov;
        sn_msg.msg_iovlen = 2;

        sn_msg.msg_flags = 0;
        sn_msg.msg_control = sn_msg_control;
        sn_msg.msg_controllen = CMSG_SPACE(sizeof(int) * num_fds);

        cmsg = CMSG_FIRSTHDR(&sn_msg);
        cmsg->cmsg_len = CMSG_LEN(sizeof(int) * num_fds);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;

        int *sn_fd_ptr = (int *) CMSG_DATA(cmsg);
	for(i=0;i<num_fds;i++) {
		sn_fd_ptr[i] = fds[i];
	}

        if (sendmsg(setns_client, &sn_msg, 0) < 0) {
                uwsgi_error("uwsgi_master_manage_setns()/sendmsg()");
        }

        free(sn_msg_control);

clear:
	close(setns_client);
	if (ns) {
		closedir(ns);
		for(i=0;i<num_fds;i++) {
			close(fds[i]);
		}
	}
}

#endif

/*
	this is racey, but the worst thing would be printing garbage in the logs...
*/
void uwsgi_dump_worker(int wid, char *msg) {
	int i;
	uwsgi_log_verbose("%s !!! worker %d status !!!\n", msg, wid);
	for(i=0;i<uwsgi.cores;i++) {
		struct uwsgi_core *uc = &uwsgi.workers[wid].cores[i];
		struct wsgi_request *wsgi_req = &uc->req;
		if (uc->in_request) {
			uwsgi_log_verbose("%s [core %d] %.*s - %.*s %.*s since %llu\n", msg, i, wsgi_req->remote_addr_len, wsgi_req->remote_addr, wsgi_req->method_len, wsgi_req->method, wsgi_req->uri_len, wsgi_req->uri, (unsigned long long) (wsgi_req->start_of_request/(1000*1000)));
		}
	}
	uwsgi_log_verbose("%s !!! end of worker %d status !!!\n",msg, wid);
}
