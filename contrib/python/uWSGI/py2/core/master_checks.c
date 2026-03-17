#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

// check if all of the workers are dead and exit uWSGI
void uwsgi_master_check_death() {
	if (uwsgi_instance_is_dying) {
		int i;
		for(i=1;i<=uwsgi.numproc;i++) {
			if (uwsgi.workers[i].pid > 0) {
				return;
			}
		}
		for(i=0;i<uwsgi.mules_cnt;i++) {
			if (uwsgi.mules[i].pid > 0) {
				return;
			}
		}
		uwsgi_log("goodbye to uWSGI.\n");
		exit(uwsgi.status.dying_for_need_app ? UWSGI_FAILED_APP_CODE : 0);
	}
}

// check if all of the workers are dead, and trigger a reload
int uwsgi_master_check_reload(char **argv) {
        if (uwsgi_instance_is_reloading) {
                int i;
                for(i=1;i<=uwsgi.numproc;i++) {
                        if (uwsgi.workers[i].pid > 0) {
                                return 0;
                        }
                }
		for(i=0;i<uwsgi.mules_cnt;i++) {
			if (uwsgi.mules[i].pid > 0) {
				return 0;
			}
		}
		uwsgi_reload(argv);
		// never here (unless in shared library mode)
		return -1;
        }
	return 0;
}

// check for chain reload
void uwsgi_master_check_chain() {
	static time_t last_check = 0;

	if (!uwsgi.status.chain_reloading) return;

	// we need to ensure the previous worker (if alive) is accepting new requests
	// before going on
	if (uwsgi.status.chain_reloading > 1) {
		struct uwsgi_worker *previous_worker = &uwsgi.workers[uwsgi.status.chain_reloading-1];
		// is the previous worker alive ?
		if (previous_worker->pid > 0 && !previous_worker->cheaped) {
			// the worker has been respawned but it is still not ready
			if (previous_worker->accepting == 0) {
				time_t now = uwsgi_now();
				if (now != last_check) {
					uwsgi_log_verbose("chain is still waiting for worker %d...\n", uwsgi.status.chain_reloading-1);
					last_check = now;
				}
				return;
			}
		}
	}

	// if all the processes are recycled, the chain is over
	if (uwsgi.status.chain_reloading > uwsgi.numproc) {
		uwsgi.status.chain_reloading = 0;
                uwsgi_log_verbose("chain reloading complete\n");
		return;
	}

	uwsgi_block_signal(SIGHUP);
	int i;
	for(i=uwsgi.status.chain_reloading;i<=uwsgi.numproc;i++) {
		struct uwsgi_worker *uw = &uwsgi.workers[i];
		if (uw->pid > 0 && !uw->cheaped && uw->accepting) {
			// the worker could have been already cursed
			if (uw->cursed_at == 0) {
				uwsgi_log_verbose("chain next victim is worker %d\n", i);
				uwsgi_curse(i, SIGHUP);
			}
			break;
		}
		else {
			uwsgi.status.chain_reloading++;
		}
        }
	uwsgi_unblock_signal(SIGHUP);
}


// special function for assuming all of the workers are dead
void uwsgi_master_commit_status() {
	int i;
	for(i=1;i<=uwsgi.numproc;i++) {
		uwsgi.workers[i].pid = 0;
	}
}

void uwsgi_master_check_idle() {

	static time_t last_request_timecheck = 0;
	static uint64_t last_request_count = 0;
	int i;
	int waitpid_status;

	if (!uwsgi.idle || uwsgi.status.is_cheap)
		return;

	uwsgi.current_time = uwsgi_now();
	if (!last_request_timecheck)
		last_request_timecheck = uwsgi.current_time;

	// security check, stop the check if there are busy workers
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].cheaped == 0 && uwsgi.workers[i].pid > 0) {
			if (uwsgi_worker_is_busy(i)) {
				return;
			}
		}
	}

	if (last_request_count != uwsgi.workers[0].requests) {
		last_request_timecheck = uwsgi.current_time;
		last_request_count = uwsgi.workers[0].requests;
	}
	// a bit of over-engeneering to avoid clock skews
	else if (last_request_timecheck < uwsgi.current_time && (uwsgi.current_time - last_request_timecheck > uwsgi.idle)) {
		uwsgi_log("workers have been inactive for more than %d seconds (%llu-%llu)\n", uwsgi.idle, (unsigned long long) uwsgi.current_time, (unsigned long long) last_request_timecheck);
		uwsgi.status.is_cheap = 1;
		if (uwsgi.die_on_idle) {
			if (uwsgi.has_emperor) {
				char byte = 22;
				if (write(uwsgi.emperor_fd, &byte, 1) != 1) {
					uwsgi_error("write()");
					kill_them_all(0);
				}
			}
			else {
				kill_them_all(0);
			}
			return;
		}
		for (i = 1; i <= uwsgi.numproc; i++) {
			uwsgi.workers[i].cheaped = 1;
			if (uwsgi.workers[i].pid == 0)
				continue;
			// first send SIGINT
			kill(uwsgi.workers[i].pid, SIGINT);
			// and start waiting upto 3 seconds
			int j;
			for(j=0;j<3;j++) {
				sleep(1);
				int ret = waitpid(uwsgi.workers[i].pid, &waitpid_status, WNOHANG);
				if (ret == 0) continue;
				if (ret == (int) uwsgi.workers[i].pid) goto done;
				// on error, directly send SIGKILL
				break;
			}
			kill(uwsgi.workers[i].pid, SIGKILL);
			if (waitpid(uwsgi.workers[i].pid, &waitpid_status, 0) < 0) {
				if (errno != ECHILD)
					uwsgi_error("uwsgi_master_check_idle()/waitpid()");
			}
			else {
done:
				uwsgi.workers[i].pid = 0;
				uwsgi.workers[i].rss_size = 0;
				uwsgi.workers[i].vsz_size = 0;
			}
		}
		uwsgi_add_sockets_to_queue(uwsgi.master_queue, -1);
		uwsgi_log("cheap mode enabled: waiting for socket connection...\n");
		last_request_timecheck = 0;
	}

}

int uwsgi_master_check_harakiri(int w, int c, time_t harakiri) {
/**
 * Triggers a harakiri when the following conditions are met:
 * - harakiri timeout > current time
 * - listen queue pressure (ie backlog > harakiri_queue_threshold)
 *
 * The first harakiri attempt on a worker will be graceful if harakiri_graceful_timeout > 0,
 * then the worker has harakiri_graceful_timeout seconds to shutdown cleanly, otherwise
 * a second harakiri will trigger a SIGKILL
 *
 */
#ifdef __linux__
	int backlog = uwsgi.shared->backlog;
#else
	int backlog = 0;
#endif
	if (harakiri == 0 || harakiri > (time_t) uwsgi.current_time) {
		return 0;
	}
	// no pending harakiri for the worker and no backlog pressure, safe to skip
	if (uwsgi.workers[w].pending_harakiri == 0 &&  backlog < uwsgi.harakiri_queue_threshold) {
		uwsgi_log_verbose("HARAKIRI: Skipping harakiri on worker %d. Listen queue is smaller than the threshold (%d < %d)\n",
			w, backlog, uwsgi.harakiri_queue_threshold);
		return 0;
	}

	trigger_harakiri(w);
	if (uwsgi.harakiri_graceful_timeout > 0) {
		uwsgi.workers[w].harakiri = harakiri + uwsgi.harakiri_graceful_timeout;
		uwsgi_log_verbose("HARAKIRI: graceful termination attempt on worker %d with signal %d. Next harakiri: %d\n",
			w, uwsgi.harakiri_graceful_signal, uwsgi.workers[w].harakiri);
	}
	return 1;
}

int uwsgi_master_check_workers_deadline() {
	int i,j;
	int ret = 0;
	for (i = 1; i <= uwsgi.numproc; i++) {
		for(j=0;j<uwsgi.cores;j++) {
			/* first check for harakiri */
			if (uwsgi_master_check_harakiri(i, j, uwsgi.workers[i].harakiri)) {
				uwsgi_log_verbose("HARAKIRI triggered by worker %d core %d !!!\n", i, j);
				ret = 1;
				break;
			}
			/* then user-defined harakiri */
			if (uwsgi_master_check_harakiri(i, j, uwsgi.workers[i].user_harakiri)) {
				uwsgi_log_verbose("HARAKIRI (user) triggered by worker %d core %d !!!\n", i, j);
				ret = 1;
				break;
			}
		}
		// then for evil memory checkers
		if (uwsgi.evil_reload_on_as) {
			if ((rlim_t) uwsgi.workers[i].vsz_size >= uwsgi.evil_reload_on_as) {
				uwsgi_log("*** EVIL RELOAD ON WORKER %d ADDRESS SPACE: %lld (pid: %d) ***\n", i, (long long) uwsgi.workers[i].vsz_size, uwsgi.workers[i].pid);
				kill(uwsgi.workers[i].pid, SIGKILL);
				uwsgi.workers[i].vsz_size = 0;
				ret = 1;
			}
		}
		if (uwsgi.evil_reload_on_rss) {
			if ((rlim_t) uwsgi.workers[i].rss_size >= uwsgi.evil_reload_on_rss) {
				uwsgi_log("*** EVIL RELOAD ON WORKER %d RSS: %lld (pid: %d) ***\n", i, (long long) uwsgi.workers[i].rss_size, uwsgi.workers[i].pid);
				kill(uwsgi.workers[i].pid, SIGKILL);
				uwsgi.workers[i].rss_size = 0;
				ret = 1;
			}
		}
		// check if worker was running longer than allowed lifetime
		if (uwsgi.workers[i].pid > 0 && uwsgi.workers[i].cheaped == 0 && uwsgi.max_worker_lifetime > 0) {
			uint64_t lifetime = uwsgi_now() - uwsgi.workers[i].last_spawn;
			if (lifetime > (uwsgi.max_worker_lifetime + (i-1) * uwsgi.max_worker_lifetime_delta)  && uwsgi.workers[i].manage_next_request == 1) {
				uwsgi_log("worker %d lifetime reached, it was running for %llu second(s)\n", i, (unsigned long long) lifetime);
				uwsgi.workers[i].manage_next_request = 0;
				kill(uwsgi.workers[i].pid, SIGWINCH);
				ret = 1;
			}
		}

		// need to find a better way
		//uwsgi.workers[i].last_running_time = uwsgi.workers[i].running_time;
	}


	return ret;
}


int uwsgi_master_check_gateways_deadline() {

	int i;
	int ret = 0;
	for (i = 0; i < ushared->gateways_cnt; i++) {
		if (ushared->gateways_harakiri[i] > 0) {
			if (ushared->gateways_harakiri[i] < (time_t) uwsgi.current_time) {
				if (ushared->gateways[i].pid > 0) {
					uwsgi_log("*** HARAKIRI ON GATEWAY %s %d (pid: %d) ***\n", ushared->gateways[i].name, ushared->gateways[i].num, ushared->gateways[i].pid);
					kill(ushared->gateways[i].pid, SIGKILL);
					ret = 1;
				}
				ushared->gateways_harakiri[i] = 0;
			}
		}
	}
	return ret;
}

int uwsgi_master_check_mules_deadline() {
	int i;
	int ret = 0;

	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].harakiri > 0) {
			if (uwsgi.mules[i].harakiri < (time_t) uwsgi.current_time) {
				uwsgi_log("*** HARAKIRI ON MULE %d HANDLING SIGNAL %d (pid: %d) ***\n", i + 1, uwsgi.mules[i].signum, uwsgi.mules[i].pid);
				kill(uwsgi.mules[i].pid, SIGKILL);
				uwsgi.mules[i].harakiri = 0;
				ret = 1;
			}
		}
		// user harakiri
		if (uwsgi.mules[i].user_harakiri > 0) {
                        if (uwsgi.mules[i].user_harakiri < (time_t) uwsgi.current_time) {
                                uwsgi_log("*** HARAKIRI ON MULE %d (pid: %d) ***\n", i + 1, uwsgi.mules[i].pid);
                                kill(uwsgi.mules[i].pid, SIGKILL);
                                uwsgi.mules[i].user_harakiri = 0;
				ret = 1;
                        }
                }
	}
	return ret;
}

int uwsgi_master_check_spoolers_deadline() {
	int ret = 0;
	struct uwsgi_spooler *uspool = uwsgi.spoolers;
	while (uspool) {
		if (uspool->harakiri > 0 && uspool->harakiri < (time_t) uwsgi.current_time) {
			uwsgi_log("*** HARAKIRI ON THE SPOOLER (pid: %d) ***\n", uspool->pid);
			kill(uspool->pid, SIGKILL);
			uspool->harakiri = 0;
			ret = 1;
		}
		if (uspool->user_harakiri > 0 && uspool->user_harakiri < (time_t) uwsgi.current_time) {
                        uwsgi_log("*** HARAKIRI ON THE SPOOLER (pid: %d) ***\n", uspool->pid);
                        kill(uspool->pid, SIGKILL);
                        uspool->user_harakiri = 0;
			ret = 1;
                }
		uspool = uspool->next;
	}
	return ret;
}


int uwsgi_master_check_spoolers_death(int diedpid) {

	struct uwsgi_spooler *uspool = uwsgi.spoolers;

	while (uspool) {
		if (uspool->pid > 0 && diedpid == uspool->pid) {
			if (uspool->cursed_at) {
				uspool->pid = 0;
				uspool->cursed_at = 0;
				uspool->no_mercy_at = 0;
			}
			uwsgi_log("OOOPS the spooler is no more...trying respawn...\n");
			uspool->respawned++;
			uspool->pid = spooler_start(uspool);
			return -1;
		}
		uspool = uspool->next;
	}
	return 0;
}

int uwsgi_master_check_emperor_death(int diedpid) {
	if (uwsgi.emperor_pid >= 0 && diedpid == uwsgi.emperor_pid) {
		uwsgi_log_verbose("!!! Emperor died !!!\n");
		uwsgi_emperor_start();
		return -1;
	}
	return 0;
}

int uwsgi_master_check_mules_death(int diedpid) {
	int i;
	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (!(uwsgi.mules[i].pid == diedpid)) continue;
		if (!uwsgi.mules[i].cursed_at) {
			uwsgi_log("OOOPS mule %d (pid: %d) crippled...trying respawn...\n", i + 1, uwsgi.mules[i].pid);
		}
		uwsgi.mules[i].no_mercy_at = 0;
		uwsgi.mules[i].cursed_at = 0;
		uwsgi_mule(i + 1);
		return -1;
	}
	return 0;
}

int uwsgi_master_check_gateways_death(int diedpid) {
	int i;
	for (i = 0; i < ushared->gateways_cnt; i++) {
		if (ushared->gateways[i].pid == diedpid) {
			gateway_respawn(i);
			return -1;
		}
	}
	return 0;
}

int uwsgi_master_check_daemons_death(int diedpid) {
	/* reload the daemons */
	if (uwsgi_daemon_check_pid_reload(diedpid)) {
		return -1;
	}
	return 0;
}

int uwsgi_worker_is_busy(int wid) {
	int i;
	if (uwsgi.workers[wid].sig) return 1;
	for(i=0;i<uwsgi.cores;i++) {
		if (uwsgi.workers[wid].cores[i].in_request) {
			return 1;
		}
	}
	return 0;
}

int uwsgi_master_check_cron_death(int diedpid) {
	struct uwsgi_cron *uc = uwsgi.crons;
	while (uc) {
		if (uc->pid == (pid_t) diedpid) {
			uwsgi_log("[uwsgi-cron] command \"%s\" running with pid %d exited after %d second(s)\n", uc->command, uc->pid, uwsgi_now() - uc->started_at);
			uc->pid = -1;
			uc->started_at = 0;
			return -1;
		}
		uc = uc->next;
	}
	return 0;
}

int uwsgi_master_check_crons_deadline() {
	int ret = 0;
	struct uwsgi_cron *uc = uwsgi.crons;
	while (uc) {
		if (uc->pid >= 0 && uc->harakiri > 0 && uc->harakiri < (time_t) uwsgi.current_time) {
			uwsgi_log("*** HARAKIRI ON CRON \"%s\" (pid: %d) ***\n", uc->command, uc->pid);
			kill(-uc->pid, SIGKILL);
			ret = 1;
		}
		uc = uc->next;
	}
	return ret;
}

void uwsgi_master_check_mountpoints() {
	struct uwsgi_string_list *usl;
	uwsgi_foreach(usl, uwsgi.mountpoints_check) {
		if (uwsgi_check_mountpoint(usl->value)) {
			uwsgi_log_verbose("mountpoint %s failed, triggering detonation...\n", usl->value);
			uwsgi_nuclear_blast();
			//never here
			exit(1);
		}
	}
}
