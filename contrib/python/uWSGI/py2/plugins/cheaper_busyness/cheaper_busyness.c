#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

/*

	Busyness cheaper algorithm (by Łukasz Mierzwa)

*/

extern struct uwsgi_server uwsgi;

// this global struct containes all of the relevant values
struct uwsgi_cheaper_busyness_global {
	uint64_t busyness_max;
	uint64_t busyness_min;
	uint64_t *last_values;
	uint64_t *current_busyness;
	uint64_t total_avg_busyness;
	int *was_busy;
	uint64_t tcheck;
	uint64_t last_cheaped;      // last time worker was cheaped due to low busyness
	uint64_t next_cheap;        // timestamp, we can cheap worker after it
	uint64_t penalty;       // penalty for respawning to fast, it will be added to multiplier
	uint64_t min_multi;     //initial multiplier will be stored here
	uint64_t cheap_multi;   // current multiplier value
	int last_action;            // 1 - spawn workers ; 2 - cheap worker
	int verbose;                // 1 - show debug logs, 0 - only important
	uint64_t tolerance_counter; // used to keep track of what to do if min <= busyness <= max for few cycles in row
	int emergency_workers; // counts the number of running emergency workers
#ifdef __linux__
	int backlog_alert;
	int backlog_step;
	uint64_t backlog_multi; // multiplier used to cheap emergency workers
	uint64_t backlog_nonzero_alert;
	int backlog_is_nonzero;
	uint64_t backlog_nonzero_since; // since when backlog is > 0
#endif
} uwsgi_cheaper_busyness_global;

struct uwsgi_option uwsgi_cheaper_busyness_options[] = {

	{"cheaper-busyness-max", required_argument, 0,
		"set the cheaper busyness high percent limit, above that value worker is considered loaded (default 50)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.busyness_max, 0},

	{"cheaper-busyness-min", required_argument, 0,
		"set the cheaper busyness low percent limit, below that value worker is considered idle (default 25)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.busyness_min, 0},

	{"cheaper-busyness-multiplier", required_argument, 0,
		"set initial cheaper multiplier, worker needs to be idle for cheaper-overload*multiplier seconds to be cheaped (default 10)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.cheap_multi, 0},

	{"cheaper-busyness-penalty", required_argument, 0,
		"penalty for respawning workers to fast, it will be added to the current multiplier value if worker is cheaped and than respawned back too fast (default 2)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.penalty, 0},

	{"cheaper-busyness-verbose", no_argument, 0, "enable verbose log messages from busyness algorithm",
		uwsgi_opt_true, &uwsgi_cheaper_busyness_global.verbose, 0},

#ifdef __linux__
	{"cheaper-busyness-backlog-alert", required_argument, 0,
		"spawn emergency worker(s) if any time listen queue is higher than this value (default 33)",
		uwsgi_opt_set_int, &uwsgi_cheaper_busyness_global.backlog_alert, 0},
	{"cheaper-busyness-backlog-multiplier", required_argument, 0,
		"set cheaper multiplier used for emergency workers (default 3)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.backlog_multi, 0},
	{"cheaper-busyness-backlog-step", required_argument, 0,
		"number of emergency workers to spawn at a time (default 1)",
		uwsgi_opt_set_int, &uwsgi_cheaper_busyness_global.backlog_step, 0},
	{"cheaper-busyness-backlog-nonzero", required_argument, 0,
		"spawn emergency worker(s) if backlog is > 0 for more then N seconds (default 60)",
		uwsgi_opt_set_64bit, &uwsgi_cheaper_busyness_global.backlog_nonzero_alert, 0},
#endif

	{0, 0, 0, 0, 0, 0 ,0},

};


// used to set time after when we allow to cheap workers
void set_next_cheap_time(void) {
	uint64_t now = uwsgi_micros();

#ifdef __linux__
	if (uwsgi_cheaper_busyness_global.emergency_workers > 0) {
		// we have some emergency workers running, we will use minimum delay (2 cycles) to cheap workers
		// to have quicker recovery from big but short load spikes
		// otherwise we might wait a lot before cheaping all emergency workers
		if (uwsgi_cheaper_busyness_global.verbose)
			uwsgi_log("[busyness] %d emergency worker(s) running, using %llu seconds cheaper timer\n",
				uwsgi_cheaper_busyness_global.emergency_workers, uwsgi.cheaper_overload*uwsgi_cheaper_busyness_global.backlog_multi);
		uwsgi_cheaper_busyness_global.next_cheap = now + uwsgi.cheaper_overload*uwsgi_cheaper_busyness_global.backlog_multi*1000000;
	} else {
#endif
		// no emergency workers running, we use normal math for setting timer
		uwsgi_cheaper_busyness_global.next_cheap = now + uwsgi.cheaper_overload*uwsgi_cheaper_busyness_global.cheap_multi*1000000;
#ifdef __linux__
	}
#endif
}


void decrease_multi(void) {
	// will decrease multiplier but only down to initial value
	if (uwsgi_cheaper_busyness_global.cheap_multi > uwsgi_cheaper_busyness_global.min_multi) {
		uwsgi_cheaper_busyness_global.cheap_multi--;
		uwsgi_log("[busyness] decreasing cheaper multiplier to %llu\n", uwsgi_cheaper_busyness_global.cheap_multi);
	}
}


#ifdef __linux__
int spawn_emergency_worker(int backlog) {
	// reset cheaper multiplier to minimum value so we can start cheaping workers sooner
	// if this was just random spike
	uwsgi_cheaper_busyness_global.cheap_multi = uwsgi_cheaper_busyness_global.min_multi;

	// set last action to spawn
	uwsgi_cheaper_busyness_global.last_action = 1;

	int decheaped = 0;
	int i;
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].cheaped == 1 && uwsgi.workers[i].pid == 0) {
			decheaped++;
			if (decheaped >= uwsgi_cheaper_busyness_global.backlog_step) break;
		}
	}

	// reset cheap timer, so that we need to start counting idle cycles from zero
	set_next_cheap_time();

	if (decheaped > 0) {
		uwsgi_cheaper_busyness_global.emergency_workers += decheaped;
		uwsgi_log("[busyness] %d requests in listen queue, spawning %d emergency worker(s) (%d)!\n",
			backlog, decheaped, uwsgi_cheaper_busyness_global.emergency_workers);
	} else {
		uwsgi_log("[busyness] %d requests in listen queue but we are already started maximum number of workers (%d)\n",
			backlog, uwsgi.numproc);
	}

	return decheaped;
}
#endif


int cheaper_busyness_algo(int can_spawn) {

	int i;
	// we use microseconds
	uint64_t t = uwsgi.cheaper_overload*1000000;

	int active_workers = 0;
	uint64_t total_busyness = 0;
	uint64_t avg_busyness = 0;

	for (i = 0; i < uwsgi.numproc; i++) {
		if (uwsgi.workers[i+1].cheaped == 0 && uwsgi.workers[i+1].pid > 0) {
			active_workers++;
			uwsgi_cheaper_busyness_global.was_busy[i] += uwsgi_worker_is_busy(i+1);
		} else {
			uwsgi_cheaper_busyness_global.was_busy[i] = 0;
		}
	}

#ifdef __linux__
	int backlog = uwsgi.shared->backlog;
#endif

	uint64_t now = uwsgi_micros();
	if (now - uwsgi_cheaper_busyness_global.tcheck >= t) {
		uwsgi_cheaper_busyness_global.tcheck = now;
		for (i = 0; i < uwsgi.numproc; i++) {
			if (uwsgi.workers[i+1].cheaped == 0 && uwsgi.workers[i+1].pid > 0) {
				uint64_t percent = (( (uwsgi.workers[i+1].running_time-uwsgi_cheaper_busyness_global.last_values[i])*100)/t);
				if (percent > 100) {
					percent = 100;
				}
				else if (uwsgi.workers[i+1].running_time-uwsgi_cheaper_busyness_global.last_values[i] == 0 && percent == 0 && uwsgi_cheaper_busyness_global.was_busy[i] > 0) {
					// running_time did not change but workers were busy
					// this means that workers had response times > busyness check interval
					if (uwsgi_cheaper_busyness_global.verbose) {
						uwsgi_log("[busyness] worker %d was busy %d time(s) in last cycle but no request was completed during this time, marking as 100%% busy\n",
							i+1, uwsgi_cheaper_busyness_global.was_busy[i]);
					}
					percent = 100;
				}
				uwsgi_cheaper_busyness_global.was_busy[i] = 0;
				total_busyness += percent;
				if (uwsgi_cheaper_busyness_global.verbose && active_workers > 1)
					uwsgi_log("[busyness] worker nr %d %llus average busyness is at %llu%%\n",
						i+1, uwsgi.cheaper_overload, percent);
				if (uwsgi.has_metrics) {
					// update metrics
					uwsgi_wlock(uwsgi.metrics_lock);
					uwsgi_cheaper_busyness_global.current_busyness[i] = percent;
					uwsgi_rwunlock(uwsgi.metrics_lock);
				}
			}
			uwsgi_cheaper_busyness_global.last_values[i] = uwsgi.workers[i+1].running_time;
		}

		avg_busyness = (active_workers ? total_busyness / active_workers : 0);
		if (uwsgi.has_metrics) {
			uwsgi_wlock(uwsgi.metrics_lock);
			uwsgi_cheaper_busyness_global.total_avg_busyness = avg_busyness;
			uwsgi_rwunlock(uwsgi.metrics_lock);
		}

		if (uwsgi_cheaper_busyness_global.verbose)
			uwsgi_log("[busyness] %ds average busyness of %d worker(s) is at %d%%\n",
				(int) uwsgi.cheaper_overload, (int) active_workers, (int) avg_busyness);

		if (avg_busyness > uwsgi_cheaper_busyness_global.busyness_max) {

			// we need to reset this to 0 since this is not idle cycle
			uwsgi_cheaper_busyness_global.tolerance_counter = 0;

			int decheaped = 0;
			if (can_spawn) {
				for (i = 1; i <= uwsgi.numproc; i++) {
					if (uwsgi.workers[i].cheaped == 1 && uwsgi.workers[i].pid == 0) {
						decheaped++;
						if (decheaped >= uwsgi.cheaper_step) break;
					}
				}
			}

			if (decheaped > 0) {
				// store information that we just spawned new workers
				uwsgi_cheaper_busyness_global.last_action = 1;

				// calculate number of seconds since last worker was cheaped
				if ((now - uwsgi_cheaper_busyness_global.last_cheaped)/uwsgi.cheaper_overload/1000000 <= uwsgi_cheaper_busyness_global.cheap_multi) {
					// worker was cheaped and then spawned back in less than current multiplier*cheaper_overload seconds
					// we will increase the multiplier so that next time worker will need to wait longer before being cheaped
					uwsgi_cheaper_busyness_global.cheap_multi += uwsgi_cheaper_busyness_global.penalty;
					uwsgi_log("[busyness] worker(s) respawned to fast, increasing cheaper multiplier to %llu (+%llu)\n",
						uwsgi_cheaper_busyness_global.cheap_multi, uwsgi_cheaper_busyness_global.penalty);
				} else {
					decrease_multi();
				}

				set_next_cheap_time();

				uwsgi_log("[busyness] %llus average busyness is at %llu%%, will spawn %d new worker(s)\n",
					uwsgi.cheaper_overload, avg_busyness, decheaped);
			} else {
				uwsgi_log("[busyness] %llus average busyness is at %llu%% but we already started maximum number of workers available with current limits (%d)\n",
					uwsgi.cheaper_overload, avg_busyness, active_workers);
			}

			// return the maximum number of workers to spawn
			return decheaped;

#ifdef __linux__
		} else if (can_spawn && backlog > uwsgi_cheaper_busyness_global.backlog_alert && active_workers < uwsgi.numproc) {
			return spawn_emergency_worker(backlog);
#endif

		} else if (avg_busyness < uwsgi_cheaper_busyness_global.busyness_min) {

			// with only 1 worker running there is no point in doing all that magic
			if (active_workers == 1) return 0;

			// we need to reset this to 0 since this is not idle cycle
			uwsgi_cheaper_busyness_global.tolerance_counter = 0;

			if (active_workers > uwsgi.cheaper_count) {
				// cheap a worker if too much are running
				if (now >= uwsgi_cheaper_busyness_global.next_cheap) {
					// lower cheaper multiplier if this is subsequent cheap
					if (uwsgi_cheaper_busyness_global.last_action == 2) decrease_multi();
					set_next_cheap_time();

					uwsgi_log("[busyness] %llus average busyness is at %llu%%, cheap one of %d running workers\n",
						uwsgi.cheaper_overload, avg_busyness, (int) active_workers);
					// store timestamp
					uwsgi_cheaper_busyness_global.last_cheaped = uwsgi_micros();

					// store information that last action performed was cheaping worker
					uwsgi_cheaper_busyness_global.last_action = 2;

					if (uwsgi_cheaper_busyness_global.emergency_workers > 0)
						uwsgi_cheaper_busyness_global.emergency_workers--;

					return -1;
				} else if (uwsgi_cheaper_busyness_global.verbose)
					uwsgi_log("[busyness] need to wait %llu more second(s) to cheap worker\n", (uwsgi_cheaper_busyness_global.next_cheap - now)/1000000);
			}

		} else {
			// with only 1 worker running there is no point in doing all that magic
			if (active_workers == 1) return 0;

			if (uwsgi_cheaper_busyness_global.emergency_workers > 0)
				// we had emergency workers running and we went down to the busyness
				// level that is high enough to slow down cheaping workers at extra speed
				uwsgi_cheaper_busyness_global.emergency_workers--;

			// we have min <= busyness <= max we need to check what happened before

			uwsgi_cheaper_busyness_global.tolerance_counter++;
			if (uwsgi_cheaper_busyness_global.tolerance_counter >= 3) {
				// we had three or more cycles when min <= busyness <= max, lets reset the cheaper timer
				// this is to prevent workers from being cheaped if we had idle cycles for almost all
				// time needed to cheap them, than a lot min<busy<max when we do not reset timer
				// and then another idle cycle than would trigger cheaping
				if (uwsgi_cheaper_busyness_global.verbose)
					uwsgi_log("[busyness] %llus average busyness is at %llu%%, %llu non-idle cycle(s), resetting cheaper timer\n",
						uwsgi.cheaper_overload, avg_busyness, uwsgi_cheaper_busyness_global.tolerance_counter);
				set_next_cheap_time();
			} else {
				// we had < 3 idle cycles in a row so we won't reset idle timer yet since this might be just short load spike
				// but we need to add cheaper-overload seconds to the cheaper timer so this cycle isn't counted as idle
				if (uwsgi_cheaper_busyness_global.verbose)
					uwsgi_log("[busyness] %llus average busyness is at %llu%%, %llu non-idle cycle(s), adjusting cheaper timer\n",
						uwsgi.cheaper_overload, avg_busyness, uwsgi_cheaper_busyness_global.tolerance_counter);
				uwsgi_cheaper_busyness_global.next_cheap += uwsgi.cheaper_overload*1000000;
			}
		}
	}

#ifdef __linux__
	else if (can_spawn && backlog > uwsgi_cheaper_busyness_global.backlog_alert && active_workers < uwsgi.numproc) {
		// we check for backlog overload every cycle
		return spawn_emergency_worker(backlog);
	}
	else if (backlog > 0) {
		if (uwsgi_cheaper_busyness_global.backlog_is_nonzero) {
			// backlog was > 0 last time, check timestamp and spawn workers if needed
			if (can_spawn && (now - uwsgi_cheaper_busyness_global.backlog_nonzero_since)/1000000 >= uwsgi_cheaper_busyness_global.backlog_nonzero_alert) {
				uwsgi_log("[busyness] backlog was non-zero for %llu second(s), spawning new worker(s)\n", (now - uwsgi_cheaper_busyness_global.backlog_nonzero_since)/1000000);
				uwsgi_cheaper_busyness_global.backlog_nonzero_since = now;
				return spawn_emergency_worker(backlog);
			}
		}
		else {
			// this is first > 0 pass, setup timer
			if (uwsgi_cheaper_busyness_global.verbose)
				uwsgi_log("[busyness] backlog is starting to fill (%d)\n", backlog);
			uwsgi_cheaper_busyness_global.backlog_is_nonzero = 1;
			uwsgi_cheaper_busyness_global.backlog_nonzero_since = now;
		}
	}
	else if (uwsgi_cheaper_busyness_global.backlog_is_nonzero) {
		if (uwsgi_cheaper_busyness_global.verbose)
			uwsgi_log("[busyness] backlog is now empty\n");
		uwsgi_cheaper_busyness_global.backlog_is_nonzero = 0;
	}
#endif

	return 0;
}


// registration hook
void uwsgi_cheaper_register_busyness(void) {
	uwsgi_register_cheaper_algo("busyness", cheaper_busyness_algo);
}

static int uwsgi_cheaper_busyness_init(void) {
	if (!uwsgi.requested_cheaper_algo || strcmp(uwsgi.requested_cheaper_algo, "busyness")) return 0;
	// this happens on the first run, the required memory is allocated
	uwsgi_cheaper_busyness_global.last_values = uwsgi_calloc(sizeof(uint64_t) * uwsgi.numproc);
	uwsgi_cheaper_busyness_global.was_busy = uwsgi_calloc(sizeof(int) * uwsgi.numproc);

	if (uwsgi.has_metrics) {
		// allocate metrics memory
		uwsgi_cheaper_busyness_global.current_busyness = uwsgi_calloc(sizeof(uint64_t) * uwsgi.numproc);
	}

	// set defaults
	if (!uwsgi_cheaper_busyness_global.busyness_max) uwsgi_cheaper_busyness_global.busyness_max = 50;
	if (!uwsgi_cheaper_busyness_global.busyness_min) uwsgi_cheaper_busyness_global.busyness_min = 25;
	if (!uwsgi_cheaper_busyness_global.cheap_multi) uwsgi_cheaper_busyness_global.cheap_multi = 10;
	if (!uwsgi_cheaper_busyness_global.penalty) uwsgi_cheaper_busyness_global.penalty = 2;

#ifdef __linux__
	if (!uwsgi_cheaper_busyness_global.backlog_alert) uwsgi_cheaper_busyness_global.backlog_alert = 33;
	if (!uwsgi_cheaper_busyness_global.backlog_multi) uwsgi_cheaper_busyness_global.backlog_multi = 3;
	if (!uwsgi_cheaper_busyness_global.backlog_step) uwsgi_cheaper_busyness_global.backlog_step = 1;
	if (!uwsgi_cheaper_busyness_global.backlog_nonzero_alert) uwsgi_cheaper_busyness_global.backlog_nonzero_alert = 60;
#endif

	// store initial multiplier so we don't loose that value
	uwsgi_cheaper_busyness_global.min_multi = uwsgi_cheaper_busyness_global.cheap_multi;
	// since this is first run we will print current values
	uwsgi_log("[busyness] settings: min=%llu%%, max=%llu%%, overload=%llu, multiplier=%llu, respawn penalty=%llu\n",
		uwsgi_cheaper_busyness_global.busyness_min, uwsgi_cheaper_busyness_global.busyness_max,
		uwsgi.cheaper_overload, uwsgi_cheaper_busyness_global.cheap_multi, uwsgi_cheaper_busyness_global.penalty);
#ifdef __linux__
	uwsgi_log("[busyness] backlog alert is set to %d request(s), step is %d\n",
		uwsgi_cheaper_busyness_global.backlog_alert, uwsgi_cheaper_busyness_global.backlog_step);
	uwsgi_log("[busyness] backlog non-zero alert is set to %llu second(s)\n", uwsgi_cheaper_busyness_global.backlog_nonzero_alert);
#endif

	// register metrics if enabled
	if (uwsgi.has_metrics) {
		int i;
		char buf[4096];
		char buf2[4096];
		for (i = 0; i < uwsgi.numproc; i++) {
			if (snprintf(buf, 4096, "worker.%d.plugin.cheaper_busyness.busyness", i+1) <= 0) {
				uwsgi_log("[busyness] unable to register busyness metric for worker %d\n", i+1);
				exit(1);
			}
			if (snprintf(buf2, 4096, "3.%d.100.1", i+1) <= 0) {
				uwsgi_log("[busyness] unable to register busyness metric oid for worker %d\n", i+1);
				exit(1);
			}
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_GAUGE, "ptr", &uwsgi_cheaper_busyness_global.current_busyness[i], 0, NULL);
		}
		uwsgi_register_metric("plugin.cheaper_busyness.total_avg_busyness", "4.100.1", UWSGI_METRIC_GAUGE, "ptr", &uwsgi_cheaper_busyness_global.total_avg_busyness, 0, NULL);
		uwsgi_log("[busyness] metrics registered\n");
	}

	// initialize timers
	uwsgi_cheaper_busyness_global.tcheck = uwsgi_micros();
	set_next_cheap_time();

	return 0;
}

struct uwsgi_plugin cheaper_busyness_plugin = {

	.name = "cheaper_busyness",
	.on_load = uwsgi_cheaper_register_busyness,
	.options = uwsgi_cheaper_busyness_options,
	.init = uwsgi_cheaper_busyness_init
};
