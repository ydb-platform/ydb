#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

struct http_status_codes {
        const char      key[4];
        const char      *message;
        int             message_size;
};

/* statistically ordered */
struct http_status_codes hsc[] = {
        {"200", "OK"},
        {"302", "Found"},
        {"404", "Not Found"},
        {"500", "Internal Server Error"},
        {"301", "Moved Permanently"},
        {"304", "Not Modified"},
        {"303", "See Other"},
        {"403", "Forbidden"},
        {"307", "Temporary Redirect"},
        {"401", "Unauthorized"},
        {"400", "Bad Request"},
        {"405", "Method Not Allowed"},
        {"408", "Request Timeout"},

        {"100", "Continue"},
        {"101", "Switching Protocols"},
        {"201", "Created"},
        {"202", "Accepted"},
        {"203", "Non-Authoritative Information"},
        {"204", "No Content"},
        {"205", "Reset Content"},
        {"206", "Partial Content"},
        {"300", "Multiple Choices"},
        {"305", "Use Proxy"},
        {"402", "Payment Required"},
        {"406", "Not Acceptable"},
        {"407", "Proxy Authentication Required"},
        {"409", "Conflict"},
        {"410", "Gone"},
        {"411", "Length Required"},
        {"412", "Precondition Failed"},
        {"413", "Request Entity Too Large"},
        {"414", "Request-URI Too Long"},
        {"415", "Unsupported Media Type"},
        {"416", "Requested Range Not Satisfiable"},
        {"417", "Expectation Failed"},
        {"418", "I'm a teapot"},
        {"422", "Unprocessable Entity"},
        {"425", "Too Early"},
        {"426", "Upgrade Required"},
        {"428", "Precondition Required"},
        {"429", "Too Many Requests"},
        {"431", "Request Header Fields Too Large"},
        {"451", "Unavailable For Legal Reasons"},
        {"501", "Not Implemented"},
        {"502", "Bad Gateway"},
        {"503", "Service Unavailable"},
        {"504", "Gateway Timeout"},
        {"505", "HTTP Version Not Supported"},
        {"509", "Bandwidth Limit Exceeded"},
        {"511", "Network Authentication Required"},
        {"", NULL},
};




void uwsgi_init_default() {

	uwsgi.cpus = 1;
	uwsgi.new_argc = -1;

	uwsgi.backtrace_depth = 64;
	uwsgi.max_apps = 64;

	uwsgi.master_queue = -1;

	uwsgi.signal_socket = -1;
	uwsgi.my_signal_socket = -1;
	uwsgi.stats_fd = -1;

	uwsgi.stats_pusher_default_freq = 3;

	uwsgi.original_log_fd = 2;

	uwsgi.emperor_fd_config = -1;
	uwsgi.emperor_fd_proxy = -1;
	// default emperor scan frequency
	uwsgi.emperor_freq = 3;
	uwsgi.emperor_throttle = 1000;
	uwsgi.emperor_heartbeat = 30;
	uwsgi.emperor_curse_tolerance = 30;
	// max 3 minutes throttling
	uwsgi.emperor_max_throttle = 1000 * 180;
	uwsgi.emperor_pid = -1;

	uwsgi.subscribe_freq = 10;
	uwsgi.subscription_tolerance = 17;

	uwsgi.cores = 1;
	uwsgi.threads = 1;

	// default max number of rpc slot
	uwsgi.rpc_max = 64;

	uwsgi.offload_threads_events = 64;

	uwsgi.default_app = -1;

	uwsgi.buffer_size = 4096;
	uwsgi.body_read_warning = 8;
	uwsgi.numproc = 1;

	uwsgi.forkbomb_delay = 2;

	uwsgi.async = 1;
	uwsgi.listen_queue = 100;

	uwsgi.cheaper_overload = 3;

	uwsgi.log_master_bufsize = 8192;

	uwsgi.worker_reload_mercy = 60;
	uwsgi.mule_reload_mercy = 60;

	uwsgi.max_vars = MAX_VARS;
	uwsgi.vec_size = 4 + 1 + (4 * MAX_VARS);

	uwsgi.socket_timeout = 4;
	uwsgi.logging_options.enabled = 1;

	// a workers hould be running for at least 10 seconds
	uwsgi.min_worker_lifetime = 10;

	uwsgi.spooler_frequency = 30;

	uwsgi.shared->spooler_signal_pipe[0] = -1;
	uwsgi.shared->spooler_signal_pipe[1] = -1;

	uwsgi.shared->mule_signal_pipe[0] = -1;
	uwsgi.shared->mule_signal_pipe[1] = -1;

	uwsgi.shared->mule_queue_pipe[0] = -1;
	uwsgi.shared->mule_queue_pipe[1] = -1;

	uwsgi.shared->worker_log_pipe[0] = -1;
	uwsgi.shared->worker_log_pipe[1] = -1;

	uwsgi.shared->worker_req_log_pipe[0] = -1;
	uwsgi.shared->worker_req_log_pipe[1] = -1;

	uwsgi.req_log_fd = 2;

#ifdef UWSGI_SSL
	// 1 day of tolerance
	uwsgi.subscriptions_sign_check_tolerance = 3600 * 24;
	uwsgi.ssl_sessions_timeout = 300;
	uwsgi.ssl_verify_depth = 1;
#endif

	uwsgi.alarm_freq = 3;
	uwsgi.alarm_msg_size = 8192;

	uwsgi.exception_handler_msg_size = 65536;

	uwsgi.multicast_ttl = 1;
	uwsgi.multicast_loop = 1;

	// filling http status codes
	struct http_status_codes *http_sc;
        for (http_sc = hsc; http_sc->message != NULL; http_sc++) {
                http_sc->message_size = strlen(http_sc->message);
        }

	uwsgi.empty = "";

#ifdef __linux__
	uwsgi.cgroup_dir_mode = "0700";
#endif

	uwsgi.wait_read_hook = uwsgi_simple_wait_read_hook;
	uwsgi.wait_write_hook = uwsgi_simple_wait_write_hook;
	uwsgi.wait_milliseconds_hook = uwsgi_simple_wait_milliseconds_hook;
	uwsgi.wait_read2_hook = uwsgi_simple_wait_read2_hook;

	uwsgi_websockets_init();
	
	// 1 MB default limit
	uwsgi.chunked_input_limit = 1024*1024;

	// clear reforked status
	uwsgi.master_is_reforked = 0;

	uwsgi.master_fifo_fd = -1;
	uwsgi_master_fifo_prepare();

	uwsgi.notify_socket_fd = -1;

	uwsgi.harakiri_graceful_signal = SIGTERM;
}

void uwsgi_setup_reload() {

	char env_reload_buf[11];

	char *env_reloads = getenv("UWSGI_RELOADS");
	if (env_reloads) {
		//convert env value to int
		uwsgi.reloads = atoi(env_reloads);
		uwsgi.reloads++;
		//convert reloads to string
		int rlen = snprintf(env_reload_buf, 10, "%u", uwsgi.reloads);
		if (rlen > 0 && rlen < 10) {
			env_reload_buf[rlen] = 0;
			if (setenv("UWSGI_RELOADS", env_reload_buf, 1)) {
				uwsgi_error("setenv()");
			}
		}
		uwsgi.is_a_reload = 1;
	}
	else {
		if (setenv("UWSGI_RELOADS", "0", 1)) {
			uwsgi_error("setenv()");
		}
	}

}

void uwsgi_autoload_plugins_by_name(char *argv_zero) {

	char *plugins_requested = NULL;

	char *original_proc_name = getenv("UWSGI_ORIGINAL_PROC_NAME");
	if (!original_proc_name) {
		// here we use argv[0];
		original_proc_name = argv_zero;
		setenv("UWSGI_ORIGINAL_PROC_NAME", original_proc_name, 1);
	}
	char *p = strrchr(original_proc_name, '/');
	if (p == NULL)
		p = original_proc_name;
	p = strstr(p, "uwsgi_");
	if (p != NULL) {
		char *ctx = NULL;
		uwsgi_foreach_token(uwsgi_str(p + 6), "_", plugins_requested, ctx) {
			uwsgi_log("[uwsgi] implicit plugin requested %s\n", plugins_requested);
			uwsgi_load_plugin(-1, plugins_requested, NULL);
		}
	}

	plugins_requested = getenv("UWSGI_PLUGINS");
	if (plugins_requested) {
		plugins_requested = uwsgi_concat2(plugins_requested, "");
		char *p, *ctx = NULL;
		uwsgi_foreach_token(plugins_requested, ",", p, ctx) {
			uwsgi_load_plugin(-1, p, NULL);
		}
	}

}

void uwsgi_commandline_config() {
	int i;

	uwsgi.option_index = -1;

	int argc = uwsgi.argc;
	char **argv = uwsgi.argv;

	if (uwsgi.new_argc > -1 && uwsgi.new_argv) {
		argc = uwsgi.new_argc;
		argv = uwsgi.new_argv;
	}

	char *optname;
	while ((i = getopt_long(argc, argv, uwsgi.short_options, uwsgi.long_options, &uwsgi.option_index)) != -1) {

		if (i == '?') {
			uwsgi_log("getopt_long() error\n");
			exit(1);
		}

		if (uwsgi.option_index > -1) {
			optname = (char *) uwsgi.long_options[uwsgi.option_index].name;
		}
		else {
			optname = uwsgi_get_optname_by_index(i);
		}
		if (!optname) {
			uwsgi_log("unable to parse command line options\n");
			exit(1);
		}
		uwsgi.option_index = -1;
		add_exported_option(optname, optarg, 0);
	}


#ifdef UWSGI_DEBUG
	uwsgi_log("optind:%d argc:%d\n", optind, uwsgi.argc);
#endif

	if (optind < argc) {
		for (i = optind; i < argc; i++) {
			char *lazy = argv[i];
			if (lazy[0] != '[') {
				uwsgi_opt_load(NULL, lazy, NULL);
				// manage magic mountpoint
				int magic = 0;
				int j;
				for (j = 0; j < uwsgi.gp_cnt; j++) {
					if (uwsgi.gp[j]->magic) {
						if (uwsgi.gp[j]->magic(NULL, lazy)) {
							magic = 1;
							break;
						}
					}
				}
				if (!magic) {
					for (j = 0; j < 256; j++) {
						if (uwsgi.p[j]->magic) {
							if (uwsgi.p[j]->magic(NULL, lazy)) {
								magic = 1;
								break;
							}
						}
					}
				}
			}
		}
	}

}

void uwsgi_setup_workers() {
	int i, j;
	// allocate shared memory for workers + master
	uwsgi.workers = (struct uwsgi_worker *) uwsgi_calloc_shared(sizeof(struct uwsgi_worker) * (uwsgi.numproc + 1));

	for (i = 0; i <= uwsgi.numproc; i++) {
		// allocate memory for apps
		uwsgi.workers[i].apps = (struct uwsgi_app *) uwsgi_calloc_shared(sizeof(struct uwsgi_app) * uwsgi.max_apps);

		// allocate memory for cores
		uwsgi.workers[i].cores = (struct uwsgi_core *) uwsgi_calloc_shared(sizeof(struct uwsgi_core) * uwsgi.cores);

		// this is a trick for avoiding too much memory areas
		void *ts = uwsgi_calloc_shared(sizeof(void *) * uwsgi.max_apps * uwsgi.cores);
		// add 4 bytes for uwsgi header
		void *buffers = uwsgi_malloc_shared((uwsgi.buffer_size+4) * uwsgi.cores);
		void *hvec = uwsgi_malloc_shared(sizeof(struct iovec) * uwsgi.vec_size * uwsgi.cores);
		void *post_buf = NULL;
		if (uwsgi.post_buffering > 0)
			post_buf = uwsgi_malloc_shared(uwsgi.post_buffering_bufsize * uwsgi.cores);


		for (j = 0; j < uwsgi.cores; j++) {
			// allocate shared memory for thread states (required for some language, like python)
			uwsgi.workers[i].cores[j].ts = ts + ((sizeof(void *) * uwsgi.max_apps) * j);
			// raw per-request buffer (+4 bytes for uwsgi header)
			uwsgi.workers[i].cores[j].buffer = buffers + ((uwsgi.buffer_size+4) * j);
			// iovec for uwsgi vars
			uwsgi.workers[i].cores[j].hvec = hvec + ((sizeof(struct iovec) * uwsgi.vec_size) * j);
			if (post_buf)
				uwsgi.workers[i].cores[j].post_buf = post_buf + (uwsgi.post_buffering_bufsize * j);
		}

		// master does not need to following steps...
		if (i == 0)
			continue;
		uwsgi.workers[i].signal_pipe[0] = -1;
		uwsgi.workers[i].signal_pipe[1] = -1;
		snprintf(uwsgi.workers[i].name, 0xff, "uWSGI worker %d", i);
	}

	uint64_t total_memory = (sizeof(struct uwsgi_app) * uwsgi.max_apps) + (sizeof(struct uwsgi_core) * uwsgi.cores) + (sizeof(void *) * uwsgi.max_apps * uwsgi.cores) + (uwsgi.buffer_size * uwsgi.cores) + (sizeof(struct iovec) * uwsgi.vec_size * uwsgi.cores);
	if (uwsgi.post_buffering > 0) {
		total_memory += (uwsgi.post_buffering_bufsize * uwsgi.cores);
	}

	total_memory *= (uwsgi.numproc + uwsgi.master_process);
	if (uwsgi.numproc > 0)
		uwsgi_log("mapped %llu bytes (%llu KB) for %d cores\n", (unsigned long long) total_memory, (unsigned long long) (total_memory / 1024), uwsgi.cores * uwsgi.numproc);

	// allocate signal table
        uwsgi.shared->signal_table = uwsgi_calloc_shared(sizeof(struct uwsgi_signal_entry) * 256 * (uwsgi.numproc + 1));

#ifdef UWSGI_ROUTING
	uwsgi_fixup_routes(uwsgi.routes);
	uwsgi_fixup_routes(uwsgi.error_routes);
	uwsgi_fixup_routes(uwsgi.response_routes);
	uwsgi_fixup_routes(uwsgi.final_routes);
#endif

}

pid_t uwsgi_daemonize2() {
	if (uwsgi.has_emperor) {
		logto(uwsgi.daemonize2);
	}
	else {
		if (!uwsgi.is_a_reload) {
			uwsgi_log("*** daemonizing uWSGI ***\n");
			daemonize(uwsgi.daemonize2);
		}
		else if (uwsgi.log_reopen) {
			logto(uwsgi.daemonize2);
		}
	}
	uwsgi.mypid = getpid();

	uwsgi.workers[0].pid = uwsgi.mypid;

	if (uwsgi.pidfile && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile(uwsgi.pidfile);
	}

	if (uwsgi.pidfile2 && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile(uwsgi.pidfile2);
	}

	if (uwsgi.log_master) uwsgi_setup_log_master();

	return uwsgi.mypid;
}

// fix/check related options
void sanitize_args() {

        if (uwsgi.async > 1) {
                uwsgi.cores = uwsgi.async;
        }

        uwsgi.has_threads = 1;
        if (uwsgi.threads > 1) {
                uwsgi.cores = uwsgi.threads;
        }

        if (uwsgi.harakiri_options.workers > 0) {
                if (!uwsgi.post_buffering) {
                        uwsgi_log(" *** WARNING: you have enabled harakiri without post buffering. Slow upload could be rejected on post-unbuffered webservers *** \n");
                }
        }

        if (uwsgi.write_errors_exception_only) {
                uwsgi.ignore_sigpipe = 1;
                uwsgi.ignore_write_errors = 1;
        }

        if (uwsgi.cheaper_count == 0) uwsgi.cheaper = 0;

        if (uwsgi.cheaper_count > 0 && uwsgi.cheaper_count >= uwsgi.numproc) {
                uwsgi_log("invalid cheaper value: must be lower than processes\n");
                exit(1);
        }

        if (uwsgi.cheaper && uwsgi.cheaper_count) {
		if (uwsgi.cheaper_initial) {
                	if (uwsgi.cheaper_initial < uwsgi.cheaper_count) {
                        	uwsgi_log("warning: invalid cheaper-initial value (%d), must be equal or higher than cheaper (%d), using %d as initial number of workers\n",
                                	uwsgi.cheaper_initial, uwsgi.cheaper_count, uwsgi.cheaper_count);
                        	uwsgi.cheaper_initial = uwsgi.cheaper_count;
                	}
                	else if (uwsgi.cheaper_initial > uwsgi.numproc) {
                        	uwsgi_log("warning: invalid cheaper-initial value (%d), must be lower or equal than worker count (%d), using %d as initial number of workers\n",
                                	uwsgi.cheaper_initial, uwsgi.numproc, uwsgi.numproc);
                        	uwsgi.cheaper_initial = uwsgi.numproc;
                	}
		}
		else {
                        uwsgi.cheaper_initial = uwsgi.cheaper_count;
		}
        }

	if (uwsgi.max_worker_lifetime > 0 && uwsgi.min_worker_lifetime >= uwsgi.max_worker_lifetime) {
		uwsgi_log("invalid min-worker-lifetime value (%d), must be lower than max-worker-lifetime (%d)\n",
			uwsgi.min_worker_lifetime, uwsgi.max_worker_lifetime);
		exit(1);
	}

	if (uwsgi.cheaper_rss_limit_soft && uwsgi.logging_options.memory_report != 1 && uwsgi.force_get_memusage != 1) {
		uwsgi_log("enabling cheaper-rss-limit-soft requires enabling also memory-report\n");
		exit(1);
	}
	if (uwsgi.cheaper_rss_limit_hard && !uwsgi.cheaper_rss_limit_soft) {
		uwsgi_log("enabling cheaper-rss-limit-hard requires setting also cheaper-rss-limit-soft\n");
		exit(1);
	}
	if ( uwsgi.cheaper_rss_limit_hard && uwsgi.cheaper_rss_limit_hard <= uwsgi.cheaper_rss_limit_soft) {
		uwsgi_log("cheaper-rss-limit-hard value must be higher than cheaper-rss-limit-soft value\n");
		exit(1);
	}

	if (uwsgi.evil_reload_on_rss || uwsgi.evil_reload_on_as) {
		if (!uwsgi.mem_collector_freq) uwsgi.mem_collector_freq = 3;
	}

	/* here we try to choose if thunder lock is a good thing */
#ifdef UNBIT
	if (uwsgi.numproc > 1 && !uwsgi.map_socket) {
		uwsgi.use_thunder_lock = 1;
	}
#endif
}

const char *uwsgi_http_status_msg(char *status, uint16_t *len) {
	struct http_status_codes *http_sc;
	for (http_sc = hsc; http_sc->message != NULL; http_sc++) {
                if (!strncmp(http_sc->key, status, 3)) {
                        *len = http_sc->message_size;
			return http_sc->message;
                }
        }
	return NULL;
}
