#include <contrib/python/uWSGI/py3/config.h>
/*

 *** uWSGI ***

 Copyright (C) 2009-2017 Unbit S.a.s. <info@unbit.it>

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

*/


#include "uwsgi.h"

struct uwsgi_server uwsgi;
pid_t masterpid;

extern char **environ;
#define UWSGI_ENVIRON environ

UWSGI_DECLARE_EMBEDDED_PLUGINS;

static struct uwsgi_option uwsgi_base_options[] = {
	{"socket", required_argument, 's', "bind to the specified UNIX/TCP socket using default protocol", uwsgi_opt_add_socket, NULL, 0},
	{"uwsgi-socket", required_argument, 's', "bind to the specified UNIX/TCP socket using uwsgi protocol", uwsgi_opt_add_socket, "uwsgi", 0},
#ifdef UWSGI_SSL
	{"suwsgi-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using uwsgi protocol over SSL", uwsgi_opt_add_ssl_socket, "suwsgi", 0},
	{"ssl-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using uwsgi protocol over SSL", uwsgi_opt_add_ssl_socket, "suwsgi", 0},
#endif

	{"http-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using HTTP protocol", uwsgi_opt_add_socket, "http", 0},
	{"http-socket-modifier1", required_argument, 0, "force the specified modifier1 when using HTTP protocol", uwsgi_opt_set_64bit, &uwsgi.http_modifier1, 0},
	{"http-socket-modifier2", required_argument, 0, "force the specified modifier2 when using HTTP protocol", uwsgi_opt_set_64bit, &uwsgi.http_modifier2, 0},

	{"http11-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using HTTP 1.1 (Keep-Alive) protocol", uwsgi_opt_add_socket, "http11", 0},

#ifdef UWSGI_SSL
	{"https-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using HTTPS protocol", uwsgi_opt_add_ssl_socket, "https", 0},
	{"https-socket-modifier1", required_argument, 0, "force the specified modifier1 when using HTTPS protocol", uwsgi_opt_set_64bit, &uwsgi.https_modifier1, 0},
	{"https-socket-modifier2", required_argument, 0, "force the specified modifier2 when using HTTPS protocol", uwsgi_opt_set_64bit, &uwsgi.https_modifier2, 0},
#endif

	{"fastcgi-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using FastCGI protocol", uwsgi_opt_add_socket, "fastcgi", 0},
	{"fastcgi-nph-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using FastCGI protocol (nph mode)", uwsgi_opt_add_socket, "fastcgi-nph", 0},
	{"fastcgi-modifier1", required_argument, 0, "force the specified modifier1 when using FastCGI protocol", uwsgi_opt_set_64bit, &uwsgi.fastcgi_modifier1, 0},
	{"fastcgi-modifier2", required_argument, 0, "force the specified modifier2 when using FastCGI protocol", uwsgi_opt_set_64bit, &uwsgi.fastcgi_modifier2, 0},

	{"scgi-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using SCGI protocol", uwsgi_opt_add_socket, "scgi", 0},
	{"scgi-nph-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using SCGI protocol (nph mode)", uwsgi_opt_add_socket, "scgi-nph", 0},
	{"scgi-modifier1", required_argument, 0, "force the specified modifier1 when using SCGI protocol", uwsgi_opt_set_64bit, &uwsgi.scgi_modifier1, 0},
	{"scgi-modifier2", required_argument, 0, "force the specified modifier2 when using SCGI protocol", uwsgi_opt_set_64bit, &uwsgi.scgi_modifier2, 0},

	{"raw-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using RAW protocol", uwsgi_opt_add_socket_no_defer, "raw", 0},
	{"raw-modifier1", required_argument, 0, "force the specified modifier1 when using RAW protocol", uwsgi_opt_set_64bit, &uwsgi.raw_modifier1, 0},
	{"raw-modifier2", required_argument, 0, "force the specified modifier2 when using RAW protocol", uwsgi_opt_set_64bit, &uwsgi.raw_modifier2, 0},

	{"puwsgi-socket", required_argument, 0, "bind to the specified UNIX/TCP socket using persistent uwsgi protocol (puwsgi)", uwsgi_opt_add_socket, "puwsgi", 0},

	{"protocol", required_argument, 0, "force the specified protocol for default sockets", uwsgi_opt_set_str, &uwsgi.protocol, 0},
	{"socket-protocol", required_argument, 0, "force the specified protocol for default sockets", uwsgi_opt_set_str, &uwsgi.protocol, 0},
	{"shared-socket", required_argument, 0, "create a shared socket for advanced jailing or ipc", uwsgi_opt_add_shared_socket, NULL, 0},
	{"undeferred-shared-socket", required_argument, 0, "create a shared socket for advanced jailing or ipc (undeferred mode)", uwsgi_opt_add_shared_socket, NULL, 0},
	{"processes", required_argument, 'p', "spawn the specified number of workers/processes", uwsgi_opt_set_int, &uwsgi.numproc, 0},
	{"workers", required_argument, 'p', "spawn the specified number of workers/processes", uwsgi_opt_set_int, &uwsgi.numproc, 0},
	{"thunder-lock", no_argument, 0, "serialize accept() usage (if possible)", uwsgi_opt_true, &uwsgi.use_thunder_lock, 0},
	{"harakiri", required_argument, 't', "set harakiri timeout", uwsgi_opt_set_int, &uwsgi.harakiri_options.workers, 0},
	{"harakiri-verbose", no_argument, 0, "enable verbose mode for harakiri", uwsgi_opt_true, &uwsgi.harakiri_verbose, 0},
	{"harakiri-graceful-timeout", required_argument, 0, "interval between graceful harakiri attempt and a sigkill", uwsgi_opt_set_int, &uwsgi.harakiri_graceful_timeout, 0},
	{"harakiri-graceful-signal", required_argument, 0, "use this signal instead of sigterm for graceful harakiri attempts", uwsgi_opt_set_int, &uwsgi.harakiri_graceful_signal, 0},
	{"harakiri-queue-threshold", required_argument, 0, "only trigger harakiri if queue is greater than this threshold", uwsgi_opt_set_int, &uwsgi.harakiri_queue_threshold, 0},
	{"harakiri-no-arh", no_argument, 0, "do not enable harakiri during after-request-hook", uwsgi_opt_true, &uwsgi.harakiri_no_arh, 0},
	{"no-harakiri-arh", no_argument, 0, "do not enable harakiri during after-request-hook", uwsgi_opt_true, &uwsgi.harakiri_no_arh, 0},
	{"no-harakiri-after-req-hook", no_argument, 0, "do not enable harakiri during after-request-hook", uwsgi_opt_true, &uwsgi.harakiri_no_arh, 0},
	{"backtrace-depth", required_argument, 0, "set backtrace depth", uwsgi_opt_set_int, &uwsgi.backtrace_depth, 0},
	{"mule-harakiri", required_argument, 0, "set harakiri timeout for mule tasks", uwsgi_opt_set_int, &uwsgi.harakiri_options.mules, 0},
#ifdef UWSGI_XML
	{"xmlconfig", required_argument, 'x', "load config from xml file", uwsgi_opt_load_xml, NULL, UWSGI_OPT_IMMEDIATE},
	{"xml", required_argument, 'x', "load config from xml file", uwsgi_opt_load_xml, NULL, UWSGI_OPT_IMMEDIATE},
#endif
	{"config", required_argument, 0, "load configuration using the pluggable system", uwsgi_opt_load_config, NULL, UWSGI_OPT_IMMEDIATE},
	{"fallback-config", required_argument, 0, "re-exec uwsgi with the specified config when exit code is 1", uwsgi_opt_set_str, &uwsgi.fallback_config, UWSGI_OPT_IMMEDIATE},
	{"strict", no_argument, 0, "enable strict mode (placeholder cannot be used)", uwsgi_opt_true, &uwsgi.strict, UWSGI_OPT_IMMEDIATE},

	{"skip-zero", no_argument, 0, "skip check of file descriptor 0", uwsgi_opt_true, &uwsgi.skip_zero, 0},
	{"skip-atexit", no_argument, 0, "skip atexit hooks (ignored by the master)", uwsgi_opt_true, &uwsgi.skip_atexit, 0},
	{"skip-atexit-teardown", no_argument, 0, "skip atexit teardown (ignored by the master)", uwsgi_opt_true, &uwsgi.skip_atexit_teardown, 0},

	{"set", required_argument, 'S', "set a placeholder or an option", uwsgi_opt_set_placeholder, NULL, UWSGI_OPT_IMMEDIATE},
	{"set-placeholder", required_argument, 0, "set a placeholder", uwsgi_opt_set_placeholder, (void *) 1, UWSGI_OPT_IMMEDIATE},
	{"set-ph", required_argument, 0, "set a placeholder", uwsgi_opt_set_placeholder, (void *) 1, UWSGI_OPT_IMMEDIATE},
	{"get", required_argument, 0, "print the specified option value and exit", uwsgi_opt_add_string_list, &uwsgi.get_list, UWSGI_OPT_NO_INITIAL},
	{"declare-option", required_argument, 0, "declare a new uWSGI custom option", uwsgi_opt_add_custom_option, NULL, UWSGI_OPT_IMMEDIATE},
	{"declare-option2", required_argument, 0, "declare a new uWSGI custom option (non-immediate)", uwsgi_opt_add_custom_option, NULL, 0},

	{"resolve", required_argument, 0, "place the result of a dns query in the specified placeholder, sytax: placeholder=name (immediate option)", uwsgi_opt_resolve, NULL, UWSGI_OPT_IMMEDIATE},

	{"for", required_argument, 0, "(opt logic) for cycle", uwsgi_opt_logic, (void *) uwsgi_logic_opt_for, UWSGI_OPT_IMMEDIATE},
	{"for-glob", required_argument, 0, "(opt logic) for cycle (expand glob)", uwsgi_opt_logic, (void *) uwsgi_logic_opt_for_glob, UWSGI_OPT_IMMEDIATE},
	{"for-times", required_argument, 0, "(opt logic) for cycle (expand the specified num to a list starting from 1)", uwsgi_opt_logic, (void *) uwsgi_logic_opt_for_times, UWSGI_OPT_IMMEDIATE},
	{"for-readline", required_argument, 0, "(opt logic) for cycle (expand the specified file to a list of lines)", uwsgi_opt_logic, (void *) uwsgi_logic_opt_for_readline, UWSGI_OPT_IMMEDIATE},
	{"endfor", optional_argument, 0, "(opt logic) end for cycle", uwsgi_opt_noop, NULL, UWSGI_OPT_IMMEDIATE},
	{"end-for", optional_argument, 0, "(opt logic) end for cycle", uwsgi_opt_noop, NULL, UWSGI_OPT_IMMEDIATE},

	{"if-opt", required_argument, 0, "(opt logic) check for option", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_opt, UWSGI_OPT_IMMEDIATE},
	{"if-not-opt", required_argument, 0, "(opt logic) check for option", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_opt, UWSGI_OPT_IMMEDIATE},

	{"if-env", required_argument, 0, "(opt logic) check for environment variable", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_env, UWSGI_OPT_IMMEDIATE},
	{"if-not-env", required_argument, 0, "(opt logic) check for environment variable", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_env, UWSGI_OPT_IMMEDIATE},
	{"ifenv", required_argument, 0, "(opt logic) check for environment variable", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_env, UWSGI_OPT_IMMEDIATE},

	{"if-reload", no_argument, 0, "(opt logic) check for reload", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_reload, UWSGI_OPT_IMMEDIATE},
	{"if-not-reload", no_argument, 0, "(opt logic) check for reload", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_reload, UWSGI_OPT_IMMEDIATE},

	{"if-hostname", required_argument, 0, "(opt logic) check for hostname", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_hostname, UWSGI_OPT_IMMEDIATE},
	{"if-not-hostname", required_argument, 0, "(opt logic) check for hostname", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_hostname, UWSGI_OPT_IMMEDIATE},

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"if-hostname-match", required_argument, 0, "(opt logic) try to match hostname against a regular expression", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_hostname_match, UWSGI_OPT_IMMEDIATE},
	{"if-not-hostname-match", required_argument, 0, "(opt logic) try to match hostname against a regular expression", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_hostname_match, UWSGI_OPT_IMMEDIATE},
#endif

	{"if-exists", required_argument, 0, "(opt logic) check for file/directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_exists, UWSGI_OPT_IMMEDIATE},
	{"if-not-exists", required_argument, 0, "(opt logic) check for file/directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_exists, UWSGI_OPT_IMMEDIATE},
	{"ifexists", required_argument, 0, "(opt logic) check for file/directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_exists, UWSGI_OPT_IMMEDIATE},

	{"if-plugin", required_argument, 0, "(opt logic) check for plugin", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_plugin, UWSGI_OPT_IMMEDIATE},
	{"if-not-plugin", required_argument, 0, "(opt logic) check for plugin", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_plugin, UWSGI_OPT_IMMEDIATE},
	{"ifplugin", required_argument, 0, "(opt logic) check for plugin", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_plugin, UWSGI_OPT_IMMEDIATE},

	{"if-file", required_argument, 0, "(opt logic) check for file existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_file, UWSGI_OPT_IMMEDIATE},
	{"if-not-file", required_argument, 0, "(opt logic) check for file existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_file, UWSGI_OPT_IMMEDIATE},
	{"if-dir", required_argument, 0, "(opt logic) check for directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_dir, UWSGI_OPT_IMMEDIATE},
	{"if-not-dir", required_argument, 0, "(opt logic) check for directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_not_dir, UWSGI_OPT_IMMEDIATE},

	{"ifdir", required_argument, 0, "(opt logic) check for directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_dir, UWSGI_OPT_IMMEDIATE},
	{"if-directory", required_argument, 0, "(opt logic) check for directory existence", uwsgi_opt_logic, (void *) uwsgi_logic_opt_if_dir, UWSGI_OPT_IMMEDIATE},

	{"endif", optional_argument, 0, "(opt logic) end if", uwsgi_opt_noop, NULL, UWSGI_OPT_IMMEDIATE},
	{"end-if", optional_argument, 0, "(opt logic) end if", uwsgi_opt_noop, NULL, UWSGI_OPT_IMMEDIATE},

	{"blacklist", required_argument, 0, "set options blacklist context", uwsgi_opt_set_str, &uwsgi.blacklist_context, UWSGI_OPT_IMMEDIATE},
	{"end-blacklist", no_argument, 0, "clear options blacklist context", uwsgi_opt_set_null, &uwsgi.blacklist_context, UWSGI_OPT_IMMEDIATE},

	{"whitelist", required_argument, 0, "set options whitelist context", uwsgi_opt_set_str, &uwsgi.whitelist_context, UWSGI_OPT_IMMEDIATE},
	{"end-whitelist", no_argument, 0, "clear options whitelist context", uwsgi_opt_set_null, &uwsgi.whitelist_context, UWSGI_OPT_IMMEDIATE},

	{"ignore-sigpipe", no_argument, 0, "do not report (annoying) SIGPIPE", uwsgi_opt_true, &uwsgi.ignore_sigpipe, 0},
	{"ignore-write-errors", no_argument, 0, "do not report (annoying) write()/writev() errors", uwsgi_opt_true, &uwsgi.ignore_write_errors, 0},
	{"write-errors-tolerance", required_argument, 0, "set the maximum number of allowed write errors (default: no tolerance)", uwsgi_opt_set_64bit, &uwsgi.write_errors_tolerance, 0},
	{"write-errors-exception-only", no_argument, 0, "only raise an exception on write errors giving control to the app itself", uwsgi_opt_true, &uwsgi.write_errors_exception_only, 0},
	{"disable-write-exception", no_argument, 0, "disable exception generation on write()/writev()", uwsgi_opt_true, &uwsgi.disable_write_exception, 0},

	{"inherit", required_argument, 0, "use the specified file as config template", uwsgi_opt_load, NULL, 0},
	{"include", required_argument, 0, "include the specified file as immediate configuration", uwsgi_opt_load, NULL, UWSGI_OPT_IMMEDIATE},
	{"inject-before", required_argument, 0, "inject a text file before the config file (advanced templating)", uwsgi_opt_add_string_list, &uwsgi.inject_before, UWSGI_OPT_IMMEDIATE},
	{"inject-after", required_argument, 0, "inject a text file after the config file (advanced templating)", uwsgi_opt_add_string_list, &uwsgi.inject_after, UWSGI_OPT_IMMEDIATE},
	{"daemonize", required_argument, 'd', "daemonize uWSGI", uwsgi_opt_set_str, &uwsgi.daemonize, 0},
	{"daemonize2", required_argument, 0, "daemonize uWSGI after app loading", uwsgi_opt_set_str, &uwsgi.daemonize2, 0},
	{"stop", required_argument, 0, "stop an instance", uwsgi_opt_pidfile_signal, (void *) SIGINT, UWSGI_OPT_IMMEDIATE},
	{"reload", required_argument, 0, "reload an instance", uwsgi_opt_pidfile_signal, (void *) SIGHUP, UWSGI_OPT_IMMEDIATE},
	{"pause", required_argument, 0, "pause an instance", uwsgi_opt_pidfile_signal, (void *) SIGTSTP, UWSGI_OPT_IMMEDIATE},
	{"suspend", required_argument, 0, "suspend an instance", uwsgi_opt_pidfile_signal, (void *) SIGTSTP, UWSGI_OPT_IMMEDIATE},
	{"resume", required_argument, 0, "resume an instance", uwsgi_opt_pidfile_signal, (void *) SIGTSTP, UWSGI_OPT_IMMEDIATE},

	{"connect-and-read", required_argument, 0, "connect to a socket and wait for data from it", uwsgi_opt_connect_and_read, NULL, UWSGI_OPT_IMMEDIATE},
	{"extract", required_argument, 0, "fetch/dump any supported address to stdout", uwsgi_opt_extract, NULL, UWSGI_OPT_IMMEDIATE},

	{"listen", required_argument, 'l', "set the socket listen queue size", uwsgi_opt_set_int, &uwsgi.listen_queue, UWSGI_OPT_IMMEDIATE},
	{"max-vars", required_argument, 'v', "set the amount of internal iovec/vars structures", uwsgi_opt_max_vars, NULL, 0},
	{"max-apps", required_argument, 0, "set the maximum number of per-worker applications", uwsgi_opt_set_int, &uwsgi.max_apps, 0},
	{"buffer-size", required_argument, 'b', "set internal buffer size", uwsgi_opt_set_16bit, &uwsgi.buffer_size, 0},
	{"memory-report", no_argument, 'm', "enable memory report", uwsgi_opt_true, &uwsgi.logging_options.memory_report, 0},
	{"profiler", required_argument, 0, "enable the specified profiler", uwsgi_opt_set_str, &uwsgi.profiler, 0},
	{"cgi-mode", no_argument, 'c', "force CGI-mode for plugins supporting it", uwsgi_opt_true, &uwsgi.cgi_mode, 0},
	{"abstract-socket", no_argument, 'a', "force UNIX socket in abstract mode (Linux only)", uwsgi_opt_true, &uwsgi.abstract_socket, 0},
	{"chmod-socket", optional_argument, 'C', "chmod-socket", uwsgi_opt_chmod_socket, NULL, 0},
	{"chmod", optional_argument, 'C', "chmod-socket", uwsgi_opt_chmod_socket, NULL, 0},
	{"chown-socket", required_argument, 0, "chown unix sockets", uwsgi_opt_set_str, &uwsgi.chown_socket, 0},
	{"umask", required_argument, 0, "set umask", uwsgi_opt_set_umask, NULL, UWSGI_OPT_IMMEDIATE},
#ifdef __linux__
	{"freebind", no_argument, 0, "put socket in freebind mode", uwsgi_opt_true, &uwsgi.freebind, 0},
#endif
	{"map-socket", required_argument, 0, "map sockets to specific workers", uwsgi_opt_add_string_list, &uwsgi.map_socket, 0},
	{"enable-threads", no_argument, 'T', "enable threads (stub option this is true by default)", uwsgi_opt_true, &uwsgi.has_threads, 0},
	{"no-threads-wait", no_argument, 0, "do not wait for threads cancellation on quit/reload", uwsgi_opt_true, &uwsgi.no_threads_wait, 0},

	{"auto-procname", no_argument, 0, "automatically set processes name to something meaningful", uwsgi_opt_true, &uwsgi.auto_procname, 0},
	{"procname-prefix", required_argument, 0, "add a prefix to the process names", uwsgi_opt_set_str, &uwsgi.procname_prefix, UWSGI_OPT_PROCNAME},
	{"procname-prefix-spaced", required_argument, 0, "add a spaced prefix to the process names", uwsgi_opt_set_str_spaced, &uwsgi.procname_prefix, UWSGI_OPT_PROCNAME},
	{"procname-append", required_argument, 0, "append a string to process names", uwsgi_opt_set_str, &uwsgi.procname_append, UWSGI_OPT_PROCNAME},
	{"procname", required_argument, 0, "set process names", uwsgi_opt_set_str, &uwsgi.procname, UWSGI_OPT_PROCNAME},
	{"procname-master", required_argument, 0, "set master process name", uwsgi_opt_set_str, &uwsgi.procname_master, UWSGI_OPT_PROCNAME},

	{"single-interpreter", no_argument, 'i', "do not use multiple interpreters (where available)", uwsgi_opt_true, &uwsgi.single_interpreter, 0},
	{"need-app", optional_argument, 0, "exit if no app can be loaded", uwsgi_opt_true, &uwsgi.need_app, 0},
	{"master", no_argument, 'M', "enable master process", uwsgi_opt_true, &uwsgi.master_process, 0},
	{"honour-stdin", no_argument, 0, "do not remap stdin to /dev/null", uwsgi_opt_true, &uwsgi.honour_stdin, 0},
	{"emperor", required_argument, 0, "run the Emperor", uwsgi_opt_add_string_list, &uwsgi.emperor, 0},
	{"emperor-proxy-socket", required_argument, 0, "force the vassal to became an Emperor proxy", uwsgi_opt_set_str, &uwsgi.emperor_proxy, 0},
	{"emperor-wrapper", required_argument, 0, "set a binary wrapper for vassals", uwsgi_opt_set_str, &uwsgi.emperor_wrapper, 0},
	{"emperor-wrapper-override", required_argument, 0, "set a binary wrapper for vassals to try before the default one", uwsgi_opt_add_string_list, &uwsgi.emperor_wrapper_override, 0},
	{"emperor-wrapper-fallback", required_argument, 0, "set a binary wrapper for vassals to try as a last resort", uwsgi_opt_add_string_list, &uwsgi.emperor_wrapper_fallback, 0},
	{"emperor-nofollow", no_argument, 0, "do not follow symlinks when checking for mtime", uwsgi_opt_true, &uwsgi.emperor_nofollow, 0},
	{"emperor-procname", required_argument, 0, "set the Emperor process name", uwsgi_opt_set_str, &uwsgi.emperor_procname, 0},
	{"emperor-freq", required_argument, 0, "set the Emperor scan frequency (default 3 seconds)", uwsgi_opt_set_int, &uwsgi.emperor_freq, 0},
	{"emperor-required-heartbeat", required_argument, 0, "set the Emperor tolerance about heartbeats", uwsgi_opt_set_int, &uwsgi.emperor_heartbeat, 0},
	{"emperor-curse-tolerance", required_argument, 0, "set the Emperor tolerance about cursed vassals", uwsgi_opt_set_int, &uwsgi.emperor_curse_tolerance, 0},
	{"emperor-pidfile", required_argument, 0, "write the Emperor pid in the specified file", uwsgi_opt_set_str, &uwsgi.emperor_pidfile, 0},
	{"emperor-tyrant", no_argument, 0, "put the Emperor in Tyrant mode", uwsgi_opt_true, &uwsgi.emperor_tyrant, 0},
	{"emperor-tyrant-nofollow", no_argument, 0, "do not follow symlinks when checking for uid/gid in Tyrant mode", uwsgi_opt_true, &uwsgi.emperor_tyrant_nofollow, 0},
	{"emperor-stats", required_argument, 0, "run the Emperor stats server", uwsgi_opt_set_str, &uwsgi.emperor_stats, 0},
	{"emperor-stats-server", required_argument, 0, "run the Emperor stats server", uwsgi_opt_set_str, &uwsgi.emperor_stats, 0},
	{"early-emperor", no_argument, 0, "spawn the emperor as soon as possible", uwsgi_opt_true, &uwsgi.early_emperor, 0},
	{"emperor-broodlord", required_argument, 0, "run the emperor in BroodLord mode", uwsgi_opt_set_int, &uwsgi.emperor_broodlord, 0},
	{"emperor-throttle", required_argument, 0, "set throttling level (in milliseconds) for bad behaving vassals (default 1000)", uwsgi_opt_set_int, &uwsgi.emperor_throttle, 0},
	{"emperor-max-throttle", required_argument, 0, "set max throttling level (in milliseconds) for bad behaving vassals (default 3 minutes)", uwsgi_opt_set_int, &uwsgi.emperor_max_throttle, 0},
	{"emperor-magic-exec", no_argument, 0, "prefix vassals config files with exec:// if they have the executable bit", uwsgi_opt_true, &uwsgi.emperor_magic_exec, 0},
	{"emperor-on-demand-extension", required_argument, 0, "search for text file (vassal name + extension) containing the on demand socket name", uwsgi_opt_set_str, &uwsgi.emperor_on_demand_extension, 0},
	{"emperor-on-demand-ext", required_argument, 0, "search for text file (vassal name + extension) containing the on demand socket name", uwsgi_opt_set_str, &uwsgi.emperor_on_demand_extension, 0},
	{"emperor-on-demand-directory", required_argument, 0, "enable on demand mode binding to the unix socket in the specified directory named like the vassal + .socket", uwsgi_opt_set_str, &uwsgi.emperor_on_demand_directory, 0},
	{"emperor-on-demand-dir", required_argument, 0, "enable on demand mode binding to the unix socket in the specified directory named like the vassal + .socket", uwsgi_opt_set_str, &uwsgi.emperor_on_demand_directory, 0},
	{"emperor-on-demand-exec", required_argument, 0, "use the output of the specified command as on demand socket name (the vassal name is passed as the only argument)", uwsgi_opt_set_str, &uwsgi.emperor_on_demand_exec, 0},
	{"emperor-extra-extension", required_argument, 0, "allows the specified extension in the Emperor (vassal will be called with --config)", uwsgi_opt_add_string_list, &uwsgi.emperor_extra_extension, 0},
	{"emperor-extra-ext", required_argument, 0, "allows the specified extension in the Emperor (vassal will be called with --config)", uwsgi_opt_add_string_list, &uwsgi.emperor_extra_extension, 0},
	{"emperor-no-blacklist", no_argument, 0, "disable Emperor blacklisting subsystem", uwsgi_opt_true, &uwsgi.emperor_no_blacklist, 0},
#if defined(__linux__) && !defined(OBSOLETE_LINUX_KERNEL)
	{"emperor-use-clone", required_argument, 0, "use clone() instead of fork() passing the specified unshare() flags", uwsgi_opt_set_unshare, &uwsgi.emperor_clone, 0},
#endif
	{"emperor-graceful-shutdown", no_argument, 0, "use vassals graceful shutdown during ragnarok", uwsgi_opt_true, &uwsgi.emperor_graceful_shutdown, 0},
#ifdef UWSGI_CAP
	{"emperor-cap", required_argument, 0, "set vassals capability", uwsgi_opt_set_emperor_cap, NULL, 0},
	{"vassals-cap", required_argument, 0, "set vassals capability", uwsgi_opt_set_emperor_cap, NULL, 0},
	{"vassal-cap", required_argument, 0, "set vassals capability", uwsgi_opt_set_emperor_cap, NULL, 0},
#endif
	{"imperial-monitor-list", no_argument, 0, "list enabled imperial monitors", uwsgi_opt_true, &uwsgi.imperial_monitor_list, 0},
	{"imperial-monitors-list", no_argument, 0, "list enabled imperial monitors", uwsgi_opt_true, &uwsgi.imperial_monitor_list, 0},
	{"vassals-inherit", required_argument, 0, "add config templates to vassals config (uses --inherit)", uwsgi_opt_add_string_list, &uwsgi.vassals_templates, 0},
	{"vassals-include", required_argument, 0, "include config templates to vassals config (uses --include instead of --inherit)", uwsgi_opt_add_string_list, &uwsgi.vassals_includes, 0},
	{"vassals-inherit-before", required_argument, 0, "add config templates to vassals config (uses --inherit, parses before the vassal file)", uwsgi_opt_add_string_list, &uwsgi.vassals_templates_before, 0},
	{"vassals-include-before", required_argument, 0, "include config templates to vassals config (uses --include instead of --inherit, parses before the vassal file)", uwsgi_opt_add_string_list, &uwsgi.vassals_includes_before, 0},
	{"vassals-start-hook", required_argument, 0, "run the specified command before each vassal starts", uwsgi_opt_set_str, &uwsgi.vassals_start_hook, 0},
	{"vassals-stop-hook", required_argument, 0, "run the specified command after vassal's death", uwsgi_opt_set_str, &uwsgi.vassals_stop_hook, 0},
	{"vassal-sos", required_argument, 0, "ask emperor for reinforcement when overloaded", uwsgi_opt_set_int, &uwsgi.vassal_sos, 0},
	{"vassal-sos-backlog", required_argument, 0, "ask emperor for sos if backlog queue has more items than the value specified", uwsgi_opt_set_int, &uwsgi.vassal_sos_backlog, 0},
	{"vassals-set", required_argument, 0, "automatically set the specified option (via --set) for every vassal", uwsgi_opt_add_string_list, &uwsgi.vassals_set, 0},
	{"vassal-set", required_argument, 0, "automatically set the specified option (via --set) for every vassal", uwsgi_opt_add_string_list, &uwsgi.vassals_set, 0},

	{"heartbeat", required_argument, 0, "announce healthiness to the emperor", uwsgi_opt_set_int, &uwsgi.heartbeat, 0},

	{"reload-mercy", required_argument, 0, "set the maximum time (in seconds) we wait for workers and other processes to die during reload/shutdown", uwsgi_opt_set_int, &uwsgi.reload_mercy, 0},
	{"worker-reload-mercy", required_argument, 0, "set the maximum time (in seconds) a worker can take to reload/shutdown (default is 60)", uwsgi_opt_set_int, &uwsgi.worker_reload_mercy, 0},
	{"mule-reload-mercy", required_argument, 0, "set the maximum time (in seconds) a mule can take to reload/shutdown (default is 60)", uwsgi_opt_set_int, &uwsgi.mule_reload_mercy, 0},
	{"exit-on-reload", no_argument, 0, "force exit even if a reload is requested", uwsgi_opt_true, &uwsgi.exit_on_reload, 0},
	{"die-on-term", no_argument, 0, "exit instead of brutal reload on SIGTERM", uwsgi_opt_true, &uwsgi.die_on_term, 0},
	{"force-gateway", no_argument, 0, "force the spawn of the first registered gateway without a master", uwsgi_opt_true, &uwsgi.force_gateway, 0},
	{"help", no_argument, 'h', "show this help", uwsgi_help, NULL, UWSGI_OPT_IMMEDIATE},
	{"usage", no_argument, 'h', "show this help", uwsgi_help, NULL, UWSGI_OPT_IMMEDIATE},

	{"print-sym", required_argument, 0, "print content of the specified binary symbol", uwsgi_print_sym, NULL, UWSGI_OPT_IMMEDIATE},
	{"print-symbol", required_argument, 0, "print content of the specified binary symbol", uwsgi_print_sym, NULL, UWSGI_OPT_IMMEDIATE},

	{"reaper", no_argument, 'r', "call waitpid(-1,...) after each request to get rid of zombies", uwsgi_opt_true, &uwsgi.reaper, 0},
	{"max-requests", required_argument, 'R', "reload workers after the specified amount of managed requests", uwsgi_opt_set_64bit, &uwsgi.max_requests, 0},
	{"max-requests-delta", required_argument, 0, "add (worker_id * delta) to the max_requests value of each worker", uwsgi_opt_set_64bit, &uwsgi.max_requests_delta, 0},
	{"min-worker-lifetime", required_argument, 0, "number of seconds worker must run before being reloaded (default is 10)", uwsgi_opt_set_64bit, &uwsgi.min_worker_lifetime, 0},
	{"max-worker-lifetime", required_argument, 0, "reload workers after the specified amount of seconds (default is disabled)", uwsgi_opt_set_64bit, &uwsgi.max_worker_lifetime, 0},
	{"max-worker-lifetime-delta", required_argument, 0, "add (worker_id * delta) seconds to the max_worker_lifetime value of each worker", uwsgi_opt_set_int, &uwsgi.max_worker_lifetime_delta, 0},

	{"socket-timeout", required_argument, 'z', "set internal sockets timeout", uwsgi_opt_set_int, &uwsgi.socket_timeout, 0},
	{"no-fd-passing", no_argument, 0, "disable file descriptor passing", uwsgi_opt_true, &uwsgi.no_fd_passing, 0},
	{"locks", required_argument, 0, "create the specified number of shared locks", uwsgi_opt_set_int, &uwsgi.locks, 0},
	{"lock-engine", required_argument, 0, "set the lock engine", uwsgi_opt_set_str, &uwsgi.lock_engine, 0},
	{"ftok", required_argument, 0, "set the ipcsem key via ftok() for avoiding duplicates", uwsgi_opt_set_str, &uwsgi.ftok, 0},
	{"persistent-ipcsem", no_argument, 0, "do not remove ipcsem's on shutdown", uwsgi_opt_true, &uwsgi.persistent_ipcsem, 0},
	{"sharedarea", required_argument, 'A', "create a raw shared memory area of specified pages (note: it supports keyval too)", uwsgi_opt_add_string_list, &uwsgi.sharedareas_list, 0},

	{"safe-fd", required_argument, 0, "do not close the specified file descriptor", uwsgi_opt_safe_fd, NULL, 0},
	{"fd-safe", required_argument, 0, "do not close the specified file descriptor", uwsgi_opt_safe_fd, NULL, 0},

	{"cache", required_argument, 0, "create a shared cache containing given elements", uwsgi_opt_set_64bit, &uwsgi.cache_max_items, 0},
	{"cache-blocksize", required_argument, 0, "set cache blocksize", uwsgi_opt_set_64bit, &uwsgi.cache_blocksize, 0},
	{"cache-store", required_argument, 0, "enable persistent cache to disk", uwsgi_opt_set_str, &uwsgi.cache_store, UWSGI_OPT_MASTER},
	{"cache-store-sync", required_argument, 0, "set frequency of sync for persistent cache", uwsgi_opt_set_int, &uwsgi.cache_store_sync, 0},
	{"cache-no-expire", no_argument, 0, "disable auto sweep of expired items", uwsgi_opt_true, &uwsgi.cache_no_expire, 0},
	{"cache-expire-freq", required_argument, 0, "set the frequency of cache sweeper scans (default 3 seconds)", uwsgi_opt_set_int, &uwsgi.cache_expire_freq, 0},
	{"cache-report-freed-items", no_argument, 0, "constantly report the cache item freed by the sweeper (use only for debug)", uwsgi_opt_true, &uwsgi.cache_report_freed_items, 0},
	{"cache-udp-server", required_argument, 0, "bind the cache udp server (used only for set/update/delete) to the specified socket", uwsgi_opt_add_string_list, &uwsgi.cache_udp_server, UWSGI_OPT_MASTER},
	{"cache-udp-node", required_argument, 0, "send cache update/deletion to the specified cache udp server", uwsgi_opt_add_string_list, &uwsgi.cache_udp_node, UWSGI_OPT_MASTER},
	{"cache-sync", required_argument, 0, "copy the whole content of another uWSGI cache server on server startup", uwsgi_opt_set_str, &uwsgi.cache_sync, 0},
	{"cache-use-last-modified", no_argument, 0, "update last_modified_at timestamp on every cache item modification (default is disabled)", uwsgi_opt_true, &uwsgi.cache_use_last_modified, 0},

	{"add-cache-item", required_argument, 0, "add an item in the cache", uwsgi_opt_add_string_list, &uwsgi.add_cache_item, 0},
	{"load-file-in-cache", required_argument, 0, "load a static file in the cache", uwsgi_opt_add_string_list, &uwsgi.load_file_in_cache, 0},
#ifdef UWSGI_ZLIB
	{"load-file-in-cache-gzip", required_argument, 0, "load a static file in the cache with gzip compression", uwsgi_opt_add_string_list, &uwsgi.load_file_in_cache_gzip, 0},
#endif

	{"cache2", required_argument, 0, "create a new generation shared cache (keyval syntax)", uwsgi_opt_add_string_list, &uwsgi.cache2, 0},


	{"queue", required_argument, 0, "enable shared queue", uwsgi_opt_set_int, &uwsgi.queue_size, 0},
	{"queue-blocksize", required_argument, 0, "set queue blocksize", uwsgi_opt_set_int, &uwsgi.queue_blocksize, 0},
	{"queue-store", required_argument, 0, "enable persistent queue to disk", uwsgi_opt_set_str, &uwsgi.queue_store, UWSGI_OPT_MASTER},
	{"queue-store-sync", required_argument, 0, "set frequency of sync for persistent queue", uwsgi_opt_set_int, &uwsgi.queue_store_sync, 0},

	{"spooler", required_argument, 'Q', "run a spooler on the specified directory", uwsgi_opt_add_spooler, NULL, UWSGI_OPT_MASTER},
	{"spooler-external", required_argument, 0, "map spoolers requests to a spooler directory managed by an external instance", uwsgi_opt_add_spooler, (void *) UWSGI_SPOOLER_EXTERNAL, UWSGI_OPT_MASTER},
	{"spooler-ordered", no_argument, 0, "try to order the execution of spooler tasks", uwsgi_opt_true, &uwsgi.spooler_ordered, 0},
	{"spooler-chdir", required_argument, 0, "chdir() to specified directory before each spooler task", uwsgi_opt_set_str, &uwsgi.spooler_chdir, 0},
	{"spooler-processes", required_argument, 0, "set the number of processes for spoolers", uwsgi_opt_set_int, &uwsgi.spooler_numproc, UWSGI_OPT_IMMEDIATE},
	{"spooler-quiet", no_argument, 0, "do not be verbose with spooler tasks", uwsgi_opt_true, &uwsgi.spooler_quiet, 0},
	{"spooler-max-tasks", required_argument, 0, "set the maximum number of tasks to run before recycling a spooler", uwsgi_opt_set_int, &uwsgi.spooler_max_tasks, 0},
	{"spooler-harakiri", required_argument, 0, "set harakiri timeout for spooler tasks", uwsgi_opt_set_int, &uwsgi.harakiri_options.spoolers, 0},
	{"spooler-frequency", required_argument, 0, "set spooler frequency", uwsgi_opt_set_int, &uwsgi.spooler_frequency, 0},
	{"spooler-freq", required_argument, 0, "set spooler frequency", uwsgi_opt_set_int, &uwsgi.spooler_frequency, 0},

	{"mule", optional_argument, 0, "add a mule", uwsgi_opt_add_mule, NULL, UWSGI_OPT_MASTER},
	{"mules", required_argument, 0, "add the specified number of mules", uwsgi_opt_add_mules, NULL, UWSGI_OPT_MASTER},
	{"farm", required_argument, 0, "add a mule farm", uwsgi_opt_add_farm, NULL, UWSGI_OPT_MASTER},
	{"mule-msg-size", optional_argument, 0, "set mule message buffer size", uwsgi_opt_set_int, &uwsgi.mule_msg_size, UWSGI_OPT_MASTER},

	{"signal", required_argument, 0, "send a uwsgi signal to a server", uwsgi_opt_signal, NULL, UWSGI_OPT_IMMEDIATE},
	{"signal-bufsize", required_argument, 0, "set buffer size for signal queue", uwsgi_opt_set_int, &uwsgi.signal_bufsize, 0},
	{"signals-bufsize", required_argument, 0, "set buffer size for signal queue", uwsgi_opt_set_int, &uwsgi.signal_bufsize, 0},

	{"signal-timer", required_argument, 0, "add a timer (syntax: <signal> <seconds>)", uwsgi_opt_add_string_list, &uwsgi.signal_timers, UWSGI_OPT_MASTER},
	{"timer", required_argument, 0, "add a timer (syntax: <signal> <seconds>)", uwsgi_opt_add_string_list, &uwsgi.signal_timers, UWSGI_OPT_MASTER},

	{"signal-rbtimer", required_argument, 0, "add a redblack timer (syntax: <signal> <seconds>)", uwsgi_opt_add_string_list, &uwsgi.rb_signal_timers, UWSGI_OPT_MASTER},
	{"rbtimer", required_argument, 0, "add a redblack timer (syntax: <signal> <seconds>)", uwsgi_opt_add_string_list, &uwsgi.rb_signal_timers, UWSGI_OPT_MASTER},

	{"rpc-max", required_argument, 0, "maximum number of rpc slots (default: 64)", uwsgi_opt_set_64bit, &uwsgi.rpc_max, 0},

	{"disable-logging", no_argument, 'L', "disable request logging", uwsgi_opt_false, &uwsgi.logging_options.enabled, 0},

	{"flock", required_argument, 0, "lock the specified file before starting, exit if locked", uwsgi_opt_flock, NULL, UWSGI_OPT_IMMEDIATE},
	{"flock-wait", required_argument, 0, "lock the specified file before starting, wait if locked", uwsgi_opt_flock_wait, NULL, UWSGI_OPT_IMMEDIATE},

	{"flock2", required_argument, 0, "lock the specified file after logging/daemon setup, exit if locked", uwsgi_opt_set_str, &uwsgi.flock2, UWSGI_OPT_IMMEDIATE},
	{"flock-wait2", required_argument, 0, "lock the specified file after logging/daemon setup, wait if locked", uwsgi_opt_set_str, &uwsgi.flock_wait2, UWSGI_OPT_IMMEDIATE},

	{"pidfile", required_argument, 0, "create pidfile (before privileges drop)", uwsgi_opt_set_str, &uwsgi.pidfile, 0},
	{"pidfile2", required_argument, 0, "create pidfile (after privileges drop)", uwsgi_opt_set_str, &uwsgi.pidfile2, 0},
	{"safe-pidfile", required_argument, 0, "create safe pidfile (before privileges drop)", uwsgi_opt_set_str, &uwsgi.safe_pidfile, 0},
	{"safe-pidfile2", required_argument, 0, "create safe pidfile (after privileges drop)", uwsgi_opt_set_str, &uwsgi.safe_pidfile2, 0},
	{"chroot", required_argument, 0, "chroot() to the specified directory", uwsgi_opt_set_str, &uwsgi.chroot, 0},
#ifdef __linux__
	{"pivot-root", required_argument, 0, "pivot_root() to the specified directories (new_root and put_old must be separated with a space)", uwsgi_opt_set_str, &uwsgi.pivot_root, 0},
	{"pivot_root", required_argument, 0, "pivot_root() to the specified directories (new_root and put_old must be separated with a space)", uwsgi_opt_set_str, &uwsgi.pivot_root, 0},
#endif

	{"uid", required_argument, 0, "setuid to the specified user/uid", uwsgi_opt_set_uid, NULL, 0},
	{"gid", required_argument, 0, "setgid to the specified group/gid", uwsgi_opt_set_gid, NULL, 0},
	{"add-gid", required_argument, 0, "add the specified group id to the process credentials", uwsgi_opt_add_string_list, &uwsgi.additional_gids, 0},
	{"immediate-uid", required_argument, 0, "setuid to the specified user/uid IMMEDIATELY", uwsgi_opt_set_immediate_uid, NULL, UWSGI_OPT_IMMEDIATE},
	{"immediate-gid", required_argument, 0, "setgid to the specified group/gid IMMEDIATELY", uwsgi_opt_set_immediate_gid, NULL, UWSGI_OPT_IMMEDIATE},
	{"no-initgroups", no_argument, 0, "disable additional groups set via initgroups()", uwsgi_opt_true, &uwsgi.no_initgroups, 0},
#ifdef UWSGI_CAP
	{"cap", required_argument, 0, "set process capability", uwsgi_opt_set_cap, NULL, 0},
#endif
#ifdef __linux__
	{"unshare", required_argument, 0, "unshare() part of the processes and put it in a new namespace", uwsgi_opt_set_unshare, &uwsgi.unshare, 0},
	{"unshare2", required_argument, 0, "unshare() part of the processes and put it in a new namespace after rootfs change", uwsgi_opt_set_unshare, &uwsgi.unshare2, 0},
	{"setns-socket", required_argument, 0, "expose a unix socket returning namespace fds from /proc/self/ns", uwsgi_opt_set_str, &uwsgi.setns_socket, UWSGI_OPT_MASTER},
	{"setns-socket-skip", required_argument, 0, "skip the specified entry when sending setns file descriptors", uwsgi_opt_add_string_list, &uwsgi.setns_socket_skip, 0},
	{"setns-skip", required_argument, 0, "skip the specified entry when sending setns file descriptors", uwsgi_opt_add_string_list, &uwsgi.setns_socket_skip, 0},
	{"setns", required_argument, 0, "join a namespace created by an external uWSGI instance", uwsgi_opt_set_str, &uwsgi.setns, 0},
	{"setns-preopen", no_argument, 0, "open /proc/self/ns as soon as possible and cache fds", uwsgi_opt_true, &uwsgi.setns_preopen, 0},
#endif
	{"jailed", no_argument, 0, "mark the instance as jailed (force the execution of post_jail hooks)", uwsgi_opt_true, &uwsgi.jailed, 0},
#if defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
	{"jail", required_argument, 0, "put the instance in a FreeBSD jail", uwsgi_opt_set_str, &uwsgi.jail, 0},
	{"jail-ip4", required_argument, 0, "add an ipv4 address to the FreeBSD jail", uwsgi_opt_add_string_list, &uwsgi.jail_ip4, 0},
	{"jail-ip6", required_argument, 0, "add an ipv6 address to the FreeBSD jail", uwsgi_opt_add_string_list, &uwsgi.jail_ip6, 0},
	{"jidfile", required_argument, 0, "save the jid of a FreeBSD jail in the specified file", uwsgi_opt_set_str, &uwsgi.jidfile, 0},
	{"jid-file", required_argument, 0, "save the jid of a FreeBSD jail in the specified file", uwsgi_opt_set_str, &uwsgi.jidfile, 0},
#ifdef UWSGI_HAS_FREEBSD_LIBJAIL
	{"jail2", required_argument, 0, "add an option to the FreeBSD jail", uwsgi_opt_add_string_list, &uwsgi.jail2, 0},
	{"libjail", required_argument, 0, "add an option to the FreeBSD jail", uwsgi_opt_add_string_list, &uwsgi.jail2, 0},
	{"jail-attach", required_argument, 0, "attach to the FreeBSD jail", uwsgi_opt_set_str, &uwsgi.jail_attach, 0},
#endif
#endif
	{"refork", no_argument, 0, "fork() again after privileges drop. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork, 0},
	{"re-fork", no_argument, 0, "fork() again after privileges drop. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork, 0},
	{"refork-as-root", no_argument, 0, "fork() again before privileges drop. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork_as_root, 0},
	{"re-fork-as-root", no_argument, 0, "fork() again before privileges drop. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork_as_root, 0},
	{"refork-post-jail", no_argument, 0, "fork() again after jailing. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork_post_jail, 0},
	{"re-fork-post-jail", no_argument, 0, "fork() again after jailing. Useful for jailing systems", uwsgi_opt_true, &uwsgi.refork_post_jail, 0},

	{"hook-asap", required_argument, 0, "run the specified hook as soon as possible", uwsgi_opt_add_string_list, &uwsgi.hook_asap, 0},
	{"hook-pre-jail", required_argument, 0, "run the specified hook before jailing", uwsgi_opt_add_string_list, &uwsgi.hook_pre_jail, 0},
        {"hook-post-jail", required_argument, 0, "run the specified hook after jailing", uwsgi_opt_add_string_list, &uwsgi.hook_post_jail, 0},
        {"hook-in-jail", required_argument, 0, "run the specified hook in jail after initialization", uwsgi_opt_add_string_list, &uwsgi.hook_in_jail, 0},
        {"hook-as-root", required_argument, 0, "run the specified hook before privileges drop", uwsgi_opt_add_string_list, &uwsgi.hook_as_root, 0},
        {"hook-as-user", required_argument, 0, "run the specified hook after privileges drop", uwsgi_opt_add_string_list, &uwsgi.hook_as_user, 0},
        {"hook-as-user-atexit", required_argument, 0, "run the specified hook before app exit and reload", uwsgi_opt_add_string_list, &uwsgi.hook_as_user_atexit, 0},
        {"hook-pre-app", required_argument, 0, "run the specified hook before app loading", uwsgi_opt_add_string_list, &uwsgi.hook_pre_app, 0},
        {"hook-post-app", required_argument, 0, "run the specified hook after app loading", uwsgi_opt_add_string_list, &uwsgi.hook_post_app, 0},
	{"hook-post-fork", required_argument, 0, "run the specified hook after each fork", uwsgi_opt_add_string_list, &uwsgi.hook_post_fork, 0},
        {"hook-accepting", required_argument, 0, "run the specified hook after each worker enter the accepting phase", uwsgi_opt_add_string_list, &uwsgi.hook_accepting, 0},
        {"hook-accepting1", required_argument, 0, "run the specified hook after the first worker enters the accepting phase", uwsgi_opt_add_string_list, &uwsgi.hook_accepting1, 0},
        {"hook-accepting-once", required_argument, 0, "run the specified hook after each worker enter the accepting phase (once per-instance)", uwsgi_opt_add_string_list, &uwsgi.hook_accepting_once, 0},
        {"hook-accepting1-once", required_argument, 0, "run the specified hook after the first worker enters the accepting phase (once per instance)", uwsgi_opt_add_string_list, &uwsgi.hook_accepting1_once, 0},

        {"hook-master-start", required_argument, 0, "run the specified hook when the Master starts", uwsgi_opt_add_string_list, &uwsgi.hook_master_start, 0},

        {"hook-touch", required_argument, 0, "run the specified hook when the specified file is touched (syntax: <file> <action>)", uwsgi_opt_add_string_list, &uwsgi.hook_touch, 0},

        {"hook-emperor-start", required_argument, 0, "run the specified hook when the Emperor starts", uwsgi_opt_add_string_list, &uwsgi.hook_emperor_start, 0},
        {"hook-emperor-stop", required_argument, 0, "run the specified hook when the Emperor send a stop message", uwsgi_opt_add_string_list, &uwsgi.hook_emperor_stop, 0},
        {"hook-emperor-reload", required_argument, 0, "run the specified hook when the Emperor send a reload message", uwsgi_opt_add_string_list, &uwsgi.hook_emperor_reload, 0},
        {"hook-emperor-lost", required_argument, 0, "run the specified hook when the Emperor connection is lost", uwsgi_opt_add_string_list, &uwsgi.hook_emperor_lost, 0},

        {"hook-as-vassal", required_argument, 0, "run the specified hook before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.hook_as_vassal, 0},
        {"hook-as-emperor", required_argument, 0, "run the specified hook in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.hook_as_emperor, 0},

        {"hook-as-mule", required_argument, 0, "run the specified hook in each mule", uwsgi_opt_add_string_list, &uwsgi.hook_as_mule, 0},

        {"hook-as-gateway", required_argument, 0, "run the specified hook in each gateway", uwsgi_opt_add_string_list, &uwsgi.hook_as_gateway, 0},

        {"after-request-hook", required_argument, 0, "run the specified function/symbol after each request", uwsgi_opt_add_string_list, &uwsgi.after_request_hooks, 0},
        {"after-request-call", required_argument, 0, "run the specified function/symbol after each request", uwsgi_opt_add_string_list, &uwsgi.after_request_hooks, 0},

	{"exec-asap", required_argument, 0, "run the specified command as soon as possible", uwsgi_opt_add_string_list, &uwsgi.exec_asap, 0},
	{"exec-pre-jail", required_argument, 0, "run the specified command before jailing", uwsgi_opt_add_string_list, &uwsgi.exec_pre_jail, 0},
	{"exec-post-jail", required_argument, 0, "run the specified command after jailing", uwsgi_opt_add_string_list, &uwsgi.exec_post_jail, 0},
	{"exec-in-jail", required_argument, 0, "run the specified command in jail after initialization", uwsgi_opt_add_string_list, &uwsgi.exec_in_jail, 0},
	{"exec-as-root", required_argument, 0, "run the specified command before privileges drop", uwsgi_opt_add_string_list, &uwsgi.exec_as_root, 0},
	{"exec-as-user", required_argument, 0, "run the specified command after privileges drop", uwsgi_opt_add_string_list, &uwsgi.exec_as_user, 0},
	{"exec-as-user-atexit", required_argument, 0, "run the specified command before app exit and reload", uwsgi_opt_add_string_list, &uwsgi.exec_as_user_atexit, 0},
	{"exec-pre-app", required_argument, 0, "run the specified command before app loading", uwsgi_opt_add_string_list, &uwsgi.exec_pre_app, 0},
	{"exec-post-app", required_argument, 0, "run the specified command after app loading", uwsgi_opt_add_string_list, &uwsgi.exec_post_app, 0},

	{"exec-as-vassal", required_argument, 0, "run the specified command before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.exec_as_vassal, 0},
	{"exec-as-emperor", required_argument, 0, "run the specified command in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.exec_as_emperor, 0},

	{"mount-asap", required_argument, 0, "mount filesystem as soon as possible", uwsgi_opt_add_string_list, &uwsgi.mount_asap, 0},
	{"mount-pre-jail", required_argument, 0, "mount filesystem before jailing", uwsgi_opt_add_string_list, &uwsgi.mount_pre_jail, 0},
        {"mount-post-jail", required_argument, 0, "mount filesystem after jailing", uwsgi_opt_add_string_list, &uwsgi.mount_post_jail, 0},
        {"mount-in-jail", required_argument, 0, "mount filesystem in jail after initialization", uwsgi_opt_add_string_list, &uwsgi.mount_in_jail, 0},
        {"mount-as-root", required_argument, 0, "mount filesystem before privileges drop", uwsgi_opt_add_string_list, &uwsgi.mount_as_root, 0},

        {"mount-as-vassal", required_argument, 0, "mount filesystem before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.mount_as_vassal, 0},
        {"mount-as-emperor", required_argument, 0, "mount filesystem in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.mount_as_emperor, 0},

	{"umount-asap", required_argument, 0, "unmount filesystem as soon as possible", uwsgi_opt_add_string_list, &uwsgi.umount_asap, 0},
	{"umount-pre-jail", required_argument, 0, "unmount filesystem before jailing", uwsgi_opt_add_string_list, &uwsgi.umount_pre_jail, 0},
        {"umount-post-jail", required_argument, 0, "unmount filesystem after jailing", uwsgi_opt_add_string_list, &uwsgi.umount_post_jail, 0},
        {"umount-in-jail", required_argument, 0, "unmount filesystem in jail after initialization", uwsgi_opt_add_string_list, &uwsgi.umount_in_jail, 0},
        {"umount-as-root", required_argument, 0, "unmount filesystem before privileges drop", uwsgi_opt_add_string_list, &uwsgi.umount_as_root, 0},

        {"umount-as-vassal", required_argument, 0, "unmount filesystem before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.umount_as_vassal, 0},
        {"umount-as-emperor", required_argument, 0, "unmount filesystem in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.umount_as_emperor, 0},

	{"wait-for-interface", required_argument, 0, "wait for the specified network interface to come up before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_interface, 0},
	{"wait-for-interface-timeout", required_argument, 0, "set the timeout for wait-for-interface", uwsgi_opt_set_int, &uwsgi.wait_for_interface_timeout, 0},

	{"wait-interface", required_argument, 0, "wait for the specified network interface to come up before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_interface, 0},
	{"wait-interface-timeout", required_argument, 0, "set the timeout for wait-for-interface", uwsgi_opt_set_int, &uwsgi.wait_for_interface_timeout, 0},

	{"wait-for-iface", required_argument, 0, "wait for the specified network interface to come up before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_interface, 0},
	{"wait-for-iface-timeout", required_argument, 0, "set the timeout for wait-for-interface", uwsgi_opt_set_int, &uwsgi.wait_for_interface_timeout, 0},

	{"wait-iface", required_argument, 0, "wait for the specified network interface to come up before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_interface, 0},
	{"wait-iface-timeout", required_argument, 0, "set the timeout for wait-for-interface", uwsgi_opt_set_int, &uwsgi.wait_for_interface_timeout, 0},

	{"wait-for-fs", required_argument, 0, "wait for the specified filesystem item to appear before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_fs, 0},
	{"wait-for-file", required_argument, 0, "wait for the specified file to appear before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_fs, 0},
	{"wait-for-dir", required_argument, 0, "wait for the specified directory to appear before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_fs, 0},
	{"wait-for-mountpoint", required_argument, 0, "wait for the specified mountpoint to appear before running root hooks", uwsgi_opt_add_string_list, &uwsgi.wait_for_mountpoint, 0},
	{"wait-for-fs-timeout", required_argument, 0, "set the timeout for wait-for-fs/file/dir", uwsgi_opt_set_int, &uwsgi.wait_for_fs_timeout, 0},

	{"wait-for-socket", required_argument, 0, "wait for the specified socket to be ready before loading apps", uwsgi_opt_add_string_list, &uwsgi.wait_for_socket, 0},
	{"wait-for-socket-timeout", required_argument, 0, "set the timeout for wait-for-socket", uwsgi_opt_set_int, &uwsgi.wait_for_socket_timeout, 0},

	{"call-asap", required_argument, 0, "call the specified function as soon as possible", uwsgi_opt_add_string_list, &uwsgi.call_asap, 0},
	{"call-pre-jail", required_argument, 0, "call the specified function before jailing", uwsgi_opt_add_string_list, &uwsgi.call_pre_jail, 0},
	{"call-post-jail", required_argument, 0, "call the specified function after jailing", uwsgi_opt_add_string_list, &uwsgi.call_post_jail, 0},
	{"call-in-jail", required_argument, 0, "call the specified function in jail after initialization", uwsgi_opt_add_string_list, &uwsgi.call_in_jail, 0},
	{"call-as-root", required_argument, 0, "call the specified function before privileges drop", uwsgi_opt_add_string_list, &uwsgi.call_as_root, 0},
	{"call-as-user", required_argument, 0, "call the specified function after privileges drop", uwsgi_opt_add_string_list, &uwsgi.call_as_user, 0},
	{"call-as-user-atexit", required_argument, 0, "call the specified function before app exit and reload", uwsgi_opt_add_string_list, &uwsgi.call_as_user_atexit, 0},
	{"call-pre-app", required_argument, 0, "call the specified function before app loading", uwsgi_opt_add_string_list, &uwsgi.call_pre_app, 0},
	{"call-post-app", required_argument, 0, "call the specified function after app loading", uwsgi_opt_add_string_list, &uwsgi.call_post_app, 0},

	{"call-as-vassal", required_argument, 0, "call the specified function() before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.call_as_vassal, 0},
	{"call-as-vassal1", required_argument, 0, "call the specified function(char *) before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.call_as_vassal1, 0},
	{"call-as-vassal3", required_argument, 0, "call the specified function(char *, uid_t, gid_t) before exec()ing the vassal", uwsgi_opt_add_string_list, &uwsgi.call_as_vassal3, 0},

	{"call-as-emperor", required_argument, 0, "call the specified function() in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.call_as_emperor, 0},
	{"call-as-emperor1", required_argument, 0, "call the specified function(char *) in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.call_as_emperor1, 0},
	{"call-as-emperor2", required_argument, 0, "call the specified function(char *, pid_t) in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.call_as_emperor2, 0},
	{"call-as-emperor4", required_argument, 0, "call the specified function(char *, pid_t, uid_t, gid_t) in the emperor after the vassal has been started", uwsgi_opt_add_string_list, &uwsgi.call_as_emperor4, 0},



	{"ini", required_argument, 0, "load config from ini file", uwsgi_opt_load_ini, NULL, UWSGI_OPT_IMMEDIATE},
#ifdef UWSGI_YAML
	{"yaml", required_argument, 'y', "load config from yaml file", uwsgi_opt_load_yml, NULL, UWSGI_OPT_IMMEDIATE},
	{"yml", required_argument, 'y', "load config from yaml file", uwsgi_opt_load_yml, NULL, UWSGI_OPT_IMMEDIATE},
#endif
#ifdef UWSGI_JSON
	{"json", required_argument, 'j', "load config from json file", uwsgi_opt_load_json, NULL, UWSGI_OPT_IMMEDIATE},
	{"js", required_argument, 'j', "load config from json file", uwsgi_opt_load_json, NULL, UWSGI_OPT_IMMEDIATE},
#endif
	{"weight", required_argument, 0, "weight of the instance (used by clustering/lb/subscriptions)", uwsgi_opt_set_64bit, &uwsgi.weight, 0},
	{"auto-weight", required_argument, 0, "set weight of the instance (used by clustering/lb/subscriptions) automatically", uwsgi_opt_true, &uwsgi.auto_weight, 0},
	{"no-server", no_argument, 0, "force no-server mode", uwsgi_opt_true, &uwsgi.no_server, 0},
	{"command-mode", no_argument, 0, "force command mode", uwsgi_opt_true, &uwsgi.command_mode, UWSGI_OPT_IMMEDIATE},
	{"no-defer-accept", no_argument, 0, "disable deferred-accept on sockets", uwsgi_opt_true, &uwsgi.no_defer_accept, 0},
	{"tcp-nodelay", no_argument, 0, "enable TCP NODELAY on each request", uwsgi_opt_true, &uwsgi.tcp_nodelay, 0},
	{"so-keepalive", no_argument, 0, "enable TCP KEEPALIVEs", uwsgi_opt_true, &uwsgi.so_keepalive, 0},
	{"so-send-timeout", no_argument, 0, "set SO_SNDTIMEO", uwsgi_opt_set_int, &uwsgi.so_send_timeout, 0},
	{"socket-send-timeout", no_argument, 0, "set SO_SNDTIMEO", uwsgi_opt_set_int, &uwsgi.so_send_timeout, 0},
	{"so-write-timeout", no_argument, 0, "set SO_SNDTIMEO", uwsgi_opt_set_int, &uwsgi.so_send_timeout, 0},
	{"socket-write-timeout", no_argument, 0, "set SO_SNDTIMEO", uwsgi_opt_set_int, &uwsgi.so_send_timeout, 0},
	{"socket-sndbuf", required_argument, 0, "set SO_SNDBUF", uwsgi_opt_set_64bit, &uwsgi.so_sndbuf, 0},
	{"socket-rcvbuf", required_argument, 0, "set SO_RCVBUF", uwsgi_opt_set_64bit, &uwsgi.so_rcvbuf, 0},
	{"shutdown-sockets", no_argument, 0, "force calling shutdown() in addition to close() when sockets are destroyed", uwsgi_opt_true, &uwsgi.shutdown_sockets, 0},
	{"limit-as", required_argument, 0, "limit processes address space/vsz", uwsgi_opt_set_megabytes, &uwsgi.rl.rlim_max, 0},
	{"limit-nproc", required_argument, 0, "limit the number of spawnable processes", uwsgi_opt_set_int, &uwsgi.rl_nproc.rlim_max, 0},
	{"reload-on-as", required_argument, 0, "reload if address space is higher than specified megabytes", uwsgi_opt_set_megabytes, &uwsgi.reload_on_as, UWSGI_OPT_MEMORY},
	{"reload-on-rss", required_argument, 0, "reload if rss memory is higher than specified megabytes", uwsgi_opt_set_megabytes, &uwsgi.reload_on_rss, UWSGI_OPT_MEMORY},
	{"evil-reload-on-as", required_argument, 0, "force the master to reload a worker if its address space is higher than specified megabytes", uwsgi_opt_set_megabytes, &uwsgi.evil_reload_on_as, UWSGI_OPT_MASTER | UWSGI_OPT_MEMORY},
	{"evil-reload-on-rss", required_argument, 0, "force the master to reload a worker if its rss memory is higher than specified megabytes", uwsgi_opt_set_megabytes, &uwsgi.evil_reload_on_rss, UWSGI_OPT_MASTER | UWSGI_OPT_MEMORY},
	{"mem-collector-freq", required_argument, 0, "set the memory collector frequency when evil reloads are in place", uwsgi_opt_set_int, &uwsgi.mem_collector_freq, 0},

	{"reload-on-fd", required_argument, 0, "reload if the specified file descriptor is ready", uwsgi_opt_add_string_list, &uwsgi.reload_on_fd, UWSGI_OPT_MASTER},
	{"brutal-reload-on-fd", required_argument, 0, "brutal reload if the specified file descriptor is ready", uwsgi_opt_add_string_list, &uwsgi.brutal_reload_on_fd, UWSGI_OPT_MASTER},

#ifdef __linux__
#ifdef MADV_MERGEABLE
	{"ksm", optional_argument, 0, "enable Linux KSM", uwsgi_opt_set_int, &uwsgi.linux_ksm, 0},
#endif
#endif
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"pcre-jit", no_argument, 0, "enable pcre jit (if available)", uwsgi_opt_pcre_jit, NULL, UWSGI_OPT_IMMEDIATE},
#endif
	{"never-swap", no_argument, 0, "lock all memory pages avoiding swapping", uwsgi_opt_true, &uwsgi.never_swap, 0},
	{"touch-reload", required_argument, 0, "reload uWSGI if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_reload, UWSGI_OPT_MASTER},
	{"touch-workers-reload", required_argument, 0, "trigger reload of (only) workers if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_workers_reload, UWSGI_OPT_MASTER},
	{"touch-mules-reload", required_argument, 0, "reload mules if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_mules_reload, UWSGI_OPT_MASTER},
	{"touch-spoolers-reload", required_argument, 0, "reload spoolers if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_spoolers_reload, UWSGI_OPT_MASTER},
	{"touch-chain-reload", required_argument, 0, "trigger chain reload if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_chain_reload, UWSGI_OPT_MASTER},
	{"touch-logrotate", required_argument, 0, "trigger logrotation if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_logrotate, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"touch-logreopen", required_argument, 0, "trigger log reopen if the specified file is modified/touched", uwsgi_opt_add_string_list, &uwsgi.touch_logreopen, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"touch-exec", required_argument, 0, "run command when the specified file is modified/touched (syntax: file command)", uwsgi_opt_add_string_list, &uwsgi.touch_exec, UWSGI_OPT_MASTER},
	{"touch-signal", required_argument, 0, "signal when the specified file is modified/touched (syntax: file signal)", uwsgi_opt_add_string_list, &uwsgi.touch_signal, UWSGI_OPT_MASTER},

	{"fs-reload", required_argument, 0, "graceful reload when the specified filesystem object is modified", uwsgi_opt_add_string_list, &uwsgi.fs_reload, UWSGI_OPT_MASTER},
	{"fs-brutal-reload", required_argument, 0, "brutal reload when the specified filesystem object is modified", uwsgi_opt_add_string_list, &uwsgi.fs_brutal_reload, UWSGI_OPT_MASTER},
	{"fs-signal", required_argument, 0, "raise a uwsgi signal when the specified filesystem object is modified (syntax: file signal)", uwsgi_opt_add_string_list, &uwsgi.fs_signal, UWSGI_OPT_MASTER},

	{"check-mountpoint", required_argument, 0, "destroy the instance if a filesystem is no more reachable (useful for reliable Fuse management)", uwsgi_opt_add_string_list, &uwsgi.mountpoints_check, UWSGI_OPT_MASTER},
	{"mountpoint-check", required_argument, 0, "destroy the instance if a filesystem is no more reachable (useful for reliable Fuse management)", uwsgi_opt_add_string_list, &uwsgi.mountpoints_check, UWSGI_OPT_MASTER},
	{"check-mount", required_argument, 0, "destroy the instance if a filesystem is no more reachable (useful for reliable Fuse management)", uwsgi_opt_add_string_list, &uwsgi.mountpoints_check, UWSGI_OPT_MASTER},
	{"mount-check", required_argument, 0, "destroy the instance if a filesystem is no more reachable (useful for reliable Fuse management)", uwsgi_opt_add_string_list, &uwsgi.mountpoints_check, UWSGI_OPT_MASTER},

	{"propagate-touch", no_argument, 0, "over-engineering option for system with flaky signal management", uwsgi_opt_true, &uwsgi.propagate_touch, 0},
	{"limit-post", required_argument, 0, "limit request body", uwsgi_opt_set_64bit, &uwsgi.limit_post, 0},
	{"no-orphans", no_argument, 0, "automatically kill workers if master dies (can be dangerous for availability)", uwsgi_opt_true, &uwsgi.no_orphans, 0},
	{"prio", required_argument, 0, "set processes/threads priority", uwsgi_opt_set_rawint, &uwsgi.prio, 0},
	{"cpu-affinity", required_argument, 0, "set cpu affinity", uwsgi_opt_set_int, &uwsgi.cpu_affinity, 0},
	{"post-buffering", required_argument, 0, "set size in bytes after which will buffer to disk instead of memory", uwsgi_opt_set_64bit, &uwsgi.post_buffering, 0},
	{"post-buffering-bufsize", required_argument, 0, "set buffer size for read() in post buffering mode", uwsgi_opt_set_64bit, &uwsgi.post_buffering_bufsize, 0},
	{"body-read-warning", required_argument, 0, "set the amount of allowed memory allocation (in megabytes) for request body before starting printing a warning", uwsgi_opt_set_64bit, &uwsgi.body_read_warning, 0},
	{"upload-progress", required_argument, 0, "enable creation of .json files in the specified directory during a file upload", uwsgi_opt_set_str, &uwsgi.upload_progress, 0},
	{"no-default-app", no_argument, 0, "do not fallback to default app", uwsgi_opt_true, &uwsgi.no_default_app, 0},
	{"manage-script-name", no_argument, 0, "automatically rewrite SCRIPT_NAME and PATH_INFO", uwsgi_opt_true, &uwsgi.manage_script_name, 0},
	{"ignore-script-name", no_argument, 0, "ignore SCRIPT_NAME", uwsgi_opt_true, &uwsgi.ignore_script_name, 0},

	{"catch-exceptions", no_argument, 0, "report exception as http output (discouraged, use only for testing)", uwsgi_opt_true, &uwsgi.catch_exceptions, 0},
	{"reload-on-exception", no_argument, 0, "reload a worker when an exception is raised", uwsgi_opt_true, &uwsgi.reload_on_exception, 0},
	{"reload-on-exception-type", required_argument, 0, "reload a worker when a specific exception type is raised", uwsgi_opt_add_string_list, &uwsgi.reload_on_exception_type, 0},
	{"reload-on-exception-value", required_argument, 0, "reload a worker when a specific exception value is raised", uwsgi_opt_add_string_list, &uwsgi.reload_on_exception_value, 0},
	{"reload-on-exception-repr", required_argument, 0, "reload a worker when a specific exception type+value (language-specific) is raised", uwsgi_opt_add_string_list, &uwsgi.reload_on_exception_repr, 0},
	{"exception-handler", required_argument, 0, "add an exception handler", uwsgi_opt_add_string_list, &uwsgi.exception_handlers_instance, UWSGI_OPT_MASTER},

	{"enable-metrics", no_argument, 0, "enable metrics subsystem", uwsgi_opt_true, &uwsgi.has_metrics, UWSGI_OPT_MASTER},
	{"metric", required_argument, 0, "add a custom metric", uwsgi_opt_add_string_list, &uwsgi.additional_metrics, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metric-threshold", required_argument, 0, "add a metric threshold/alarm", uwsgi_opt_add_string_list, &uwsgi.metrics_threshold, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metric-alarm", required_argument, 0, "add a metric threshold/alarm", uwsgi_opt_add_string_list, &uwsgi.metrics_threshold, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"alarm-metric", required_argument, 0, "add a metric threshold/alarm", uwsgi_opt_add_string_list, &uwsgi.metrics_threshold, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metrics-dir", required_argument, 0, "export metrics as text files to the specified directory", uwsgi_opt_set_str, &uwsgi.metrics_dir, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metrics-dir-restore", no_argument, 0, "restore last value taken from the metrics dir", uwsgi_opt_true, &uwsgi.metrics_dir_restore, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metric-dir", required_argument, 0, "export metrics as text files to the specified directory", uwsgi_opt_set_str, &uwsgi.metrics_dir, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metric-dir-restore", no_argument, 0, "restore last value taken from the metrics dir", uwsgi_opt_true, &uwsgi.metrics_dir_restore, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},
	{"metrics-no-cores", no_argument, 0, "disable generation of cores-related metrics", uwsgi_opt_true, &uwsgi.metrics_no_cores, UWSGI_OPT_METRICS|UWSGI_OPT_MASTER},

	{"udp", required_argument, 0, "run the udp server on the specified address", uwsgi_opt_set_str, &uwsgi.udp_socket, UWSGI_OPT_MASTER},
	{"stats", required_argument, 0, "enable the stats server on the specified address", uwsgi_opt_set_str, &uwsgi.stats, UWSGI_OPT_MASTER},
	{"stats-server", required_argument, 0, "enable the stats server on the specified address", uwsgi_opt_set_str, &uwsgi.stats, UWSGI_OPT_MASTER},
	{"stats-http", no_argument, 0, "prefix stats server json output with http headers", uwsgi_opt_true, &uwsgi.stats_http, UWSGI_OPT_MASTER},
	{"stats-minified", no_argument, 0, "minify statistics json output", uwsgi_opt_true, &uwsgi.stats_minified, UWSGI_OPT_MASTER},
	{"stats-min", no_argument, 0, "minify statistics json output", uwsgi_opt_true, &uwsgi.stats_minified, UWSGI_OPT_MASTER},
	{"stats-push", required_argument, 0, "push the stats json to the specified destination", uwsgi_opt_add_string_list, &uwsgi.requested_stats_pushers, UWSGI_OPT_MASTER|UWSGI_OPT_METRICS},
	{"stats-pusher-default-freq", required_argument, 0, "set the default frequency of stats pushers", uwsgi_opt_set_int, &uwsgi.stats_pusher_default_freq, UWSGI_OPT_MASTER},
	{"stats-pushers-default-freq", required_argument, 0, "set the default frequency of stats pushers", uwsgi_opt_set_int, &uwsgi.stats_pusher_default_freq, UWSGI_OPT_MASTER},
	{"stats-no-cores", no_argument, 0, "disable generation of cores-related stats", uwsgi_opt_true, &uwsgi.stats_no_cores, UWSGI_OPT_MASTER},
	{"stats-no-metrics", no_argument, 0, "do not include metrics in stats output", uwsgi_opt_true, &uwsgi.stats_no_metrics, UWSGI_OPT_MASTER},
	{"multicast", required_argument, 0, "subscribe to specified multicast group", uwsgi_opt_set_str, &uwsgi.multicast_group, UWSGI_OPT_MASTER},
	{"multicast-ttl", required_argument, 0, "set multicast ttl", uwsgi_opt_set_int, &uwsgi.multicast_ttl, 0},
	{"multicast-loop", required_argument, 0, "set multicast loop (default 1)", uwsgi_opt_set_int, &uwsgi.multicast_loop, 0},

	{"master-fifo", required_argument, 0, "enable the master fifo", uwsgi_opt_add_string_list, &uwsgi.master_fifo, UWSGI_OPT_MASTER},

	{"notify-socket", required_argument, 0, "enable the notification socket", uwsgi_opt_set_str, &uwsgi.notify_socket, UWSGI_OPT_MASTER},
	{"subscription-notify-socket", required_argument, 0, "set the notification socket for subscriptions", uwsgi_opt_set_str, &uwsgi.subscription_notify_socket, UWSGI_OPT_MASTER},

#ifdef UWSGI_SSL
	{"legion", required_argument, 0, "became a member of a legion", uwsgi_opt_legion, NULL, UWSGI_OPT_MASTER},
	{"legion-mcast", required_argument, 0, "became a member of a legion (shortcut for multicast)", uwsgi_opt_legion_mcast, NULL, UWSGI_OPT_MASTER},
	{"legion-node", required_argument, 0, "add a node to a legion", uwsgi_opt_legion_node, NULL, UWSGI_OPT_MASTER},
	{"legion-freq", required_argument, 0, "set the frequency of legion packets", uwsgi_opt_set_int, &uwsgi.legion_freq, UWSGI_OPT_MASTER},
	{"legion-tolerance", required_argument, 0, "set the tolerance of legion subsystem", uwsgi_opt_set_int, &uwsgi.legion_tolerance, UWSGI_OPT_MASTER},
	{"legion-death-on-lord-error", required_argument, 0, "declare itself as a dead node for the specified amount of seconds if one of the lord hooks fails", uwsgi_opt_set_int, &uwsgi.legion_death_on_lord_error, UWSGI_OPT_MASTER},
	{"legion-skew-tolerance", required_argument, 0, "set the clock skew tolerance of legion subsystem (default 60 seconds)", uwsgi_opt_set_int, &uwsgi.legion_skew_tolerance, UWSGI_OPT_MASTER},
	{"legion-lord", required_argument, 0, "action to call on Lord election", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-unlord", required_argument, 0, "action to call on Lord dismiss", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-setup", required_argument, 0, "action to call on legion setup", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-death", required_argument, 0, "action to call on legion death (shutdown of the instance)", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-join", required_argument, 0, "action to call on legion join (first time quorum is reached)", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-node-joined", required_argument, 0, "action to call on new node joining legion", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-node-left", required_argument, 0, "action to call node leaving legion", uwsgi_opt_legion_hook, NULL, UWSGI_OPT_MASTER},
	{"legion-quorum", required_argument, 0, "set the quorum of a legion", uwsgi_opt_legion_quorum, NULL, UWSGI_OPT_MASTER},
	{"legion-scroll", required_argument, 0, "set the scroll of a legion", uwsgi_opt_legion_scroll, NULL, UWSGI_OPT_MASTER},
	{"legion-scroll-max-size", required_argument, 0, "set max size of legion scroll buffer", uwsgi_opt_set_16bit, &uwsgi.legion_scroll_max_size, 0},
	{"legion-scroll-list-max-size", required_argument, 0, "set max size of legion scroll list buffer", uwsgi_opt_set_64bit, &uwsgi.legion_scroll_list_max_size, 0},
	{"subscriptions-sign-check", required_argument, 0, "set digest algorithm and certificate directory for secured subscription system", uwsgi_opt_scd, NULL, UWSGI_OPT_MASTER},
	{"subscriptions-sign-check-tolerance", required_argument, 0, "set the maximum tolerance (in seconds) of clock skew for secured subscription system", uwsgi_opt_set_int, &uwsgi.subscriptions_sign_check_tolerance, UWSGI_OPT_MASTER},
	{"subscriptions-sign-skip-uid", required_argument, 0, "skip signature check for the specified uid when using unix sockets credentials", uwsgi_opt_add_string_list, &uwsgi.subscriptions_sign_skip_uid, UWSGI_OPT_MASTER},
#endif
	{"subscriptions-credentials-check", required_argument, 0, "add a directory to search for subscriptions key credentials", uwsgi_opt_add_string_list, &uwsgi.subscriptions_credentials_check_dir, UWSGI_OPT_MASTER},
	{"subscriptions-use-credentials", no_argument, 0, "enable management of SCM_CREDENTIALS in subscriptions UNIX sockets", uwsgi_opt_true, &uwsgi.subscriptions_use_credentials, 0},
	{"subscription-algo", required_argument, 0, "set load balancing algorithm for the subscription system", uwsgi_opt_ssa, NULL, 0},
	{"subscription-dotsplit", no_argument, 0, "try to fallback to the next part (dot based) in subscription key", uwsgi_opt_true, &uwsgi.subscription_dotsplit, 0},
	{"subscribe-to", required_argument, 0, "subscribe to the specified subscription server", uwsgi_opt_add_string_list, &uwsgi.subscriptions, UWSGI_OPT_MASTER},
	{"st", required_argument, 0, "subscribe to the specified subscription server", uwsgi_opt_add_string_list, &uwsgi.subscriptions, UWSGI_OPT_MASTER},
	{"subscribe", required_argument, 0, "subscribe to the specified subscription server", uwsgi_opt_add_string_list, &uwsgi.subscriptions, UWSGI_OPT_MASTER},
	{"subscribe2", required_argument, 0, "subscribe to the specified subscription server using advanced keyval syntax", uwsgi_opt_add_string_list, &uwsgi.subscriptions2, UWSGI_OPT_MASTER},
	{"subscribe-freq", required_argument, 0, "send subscription announce at the specified interval", uwsgi_opt_set_int, &uwsgi.subscribe_freq, 0},
	{"subscription-tolerance", required_argument, 0, "set tolerance for subscription servers", uwsgi_opt_set_int, &uwsgi.subscription_tolerance, 0},
	{"unsubscribe-on-graceful-reload", no_argument, 0, "force unsubscribe request even during graceful reload", uwsgi_opt_true, &uwsgi.unsubscribe_on_graceful_reload, 0},
	{"start-unsubscribed", no_argument, 0, "configure subscriptions but do not send them (useful with master fifo)", uwsgi_opt_true, &uwsgi.subscriptions_blocked, 0},

	{"subscribe-with-modifier1", required_argument, 0, "force the specififed modifier1 when subscribing", uwsgi_opt_set_str, &uwsgi.subscribe_with_modifier1, UWSGI_OPT_MASTER},

	{"snmp", optional_argument, 0, "enable the embedded snmp server", uwsgi_opt_snmp, NULL, 0},
	{"snmp-community", required_argument, 0, "set the snmp community string", uwsgi_opt_snmp_community, NULL, 0},
#ifdef UWSGI_SSL
	{"ssl-verbose", no_argument, 0, "be verbose about SSL errors", uwsgi_opt_true, &uwsgi.ssl_verbose, 0},
	{"ssl-verify-depth", optional_argument, 0, "set maximum certificate verification depth", uwsgi_opt_set_int, &uwsgi.ssl_verify_depth, 0},
#ifdef UWSGI_SSL_SESSION_CACHE
	// force master, as ssl sessions caching initialize locking early
	{"ssl-sessions-use-cache", optional_argument, 0, "use uWSGI cache for ssl sessions storage", uwsgi_opt_set_str, &uwsgi.ssl_sessions_use_cache, UWSGI_OPT_MASTER},
	{"ssl-session-use-cache", optional_argument, 0, "use uWSGI cache for ssl sessions storage", uwsgi_opt_set_str, &uwsgi.ssl_sessions_use_cache, UWSGI_OPT_MASTER},
	{"ssl-sessions-timeout", required_argument, 0, "set SSL sessions timeout (default: 300 seconds)", uwsgi_opt_set_int, &uwsgi.ssl_sessions_timeout, 0},
	{"ssl-session-timeout", required_argument, 0, "set SSL sessions timeout (default: 300 seconds)", uwsgi_opt_set_int, &uwsgi.ssl_sessions_timeout, 0},
#endif
	{"sni", required_argument, 0, "add an SNI-governed SSL context", uwsgi_opt_sni, NULL, 0},
	{"sni-dir", required_argument, 0, "check for cert/key/client_ca file in the specified directory and create a sni/ssl context on demand", uwsgi_opt_set_str, &uwsgi.sni_dir, 0},
	{"sni-dir-ciphers", required_argument, 0, "set ssl ciphers for sni-dir option", uwsgi_opt_set_str, &uwsgi.sni_dir_ciphers, 0},
	{"ssl-enable3", no_argument, 0, "enable SSLv3 (insecure)", uwsgi_opt_true, &uwsgi.sslv3, 0},
	{"ssl-enable-sslv3", no_argument, 0, "enable SSLv3 (insecure)", uwsgi_opt_true, &uwsgi.sslv3, 0},
	{"ssl-enable-tlsv1", no_argument, 0, "enable TLSv1 (insecure)", uwsgi_opt_true, &uwsgi.tlsv1, 0},
	{"ssl-option", no_argument, 0, "set a raw ssl option (numeric value)", uwsgi_opt_add_string_list, &uwsgi.ssl_options, 0},
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"sni-regexp", required_argument, 0, "add an SNI-governed SSL context (the key is a regexp)", uwsgi_opt_sni, NULL, 0},
#endif
	{"ssl-tmp-dir", required_argument, 0, "store ssl-related temp files in the specified directory", uwsgi_opt_set_str, &uwsgi.ssl_tmp_dir, 0},
#endif
	{"check-interval", required_argument, 0, "set the interval (in seconds) of master checks", uwsgi_opt_set_int, &uwsgi.master_interval, UWSGI_OPT_MASTER},
	{"forkbomb-delay", required_argument, 0, "sleep for the specified number of seconds when a forkbomb is detected", uwsgi_opt_set_int, &uwsgi.forkbomb_delay, UWSGI_OPT_MASTER},
	{"binary-path", required_argument, 0, "force binary path", uwsgi_opt_set_str, &uwsgi.binary_path, 0},
	{"privileged-binary-patch", required_argument, 0, "patch the uwsgi binary with a new command (before privileges drop)", uwsgi_opt_set_str, &uwsgi.privileged_binary_patch, 0},
	{"unprivileged-binary-patch", required_argument, 0, "patch the uwsgi binary with a new command (after privileges drop)", uwsgi_opt_set_str, &uwsgi.unprivileged_binary_patch, 0},
	{"privileged-binary-patch-arg", required_argument, 0, "patch the uwsgi binary with a new command and arguments (before privileges drop)", uwsgi_opt_set_str, &uwsgi.privileged_binary_patch_arg, 0},
	{"unprivileged-binary-patch-arg", required_argument, 0, "patch the uwsgi binary with a new command and arguments (after privileges drop)", uwsgi_opt_set_str, &uwsgi.unprivileged_binary_patch_arg, 0},
	{"async", required_argument, 0, "enable async mode with specified cores", uwsgi_opt_set_int, &uwsgi.async, 0},
	{"max-fd", required_argument, 0, "set maximum number of file descriptors (requires root privileges)", uwsgi_opt_set_int, &uwsgi.requested_max_fd, 0},
	{"logto", required_argument, 0, "set logfile/udp address", uwsgi_opt_set_str, &uwsgi.logfile, 0},
	{"logto2", required_argument, 0, "log to specified file or udp address after privileges drop", uwsgi_opt_set_str, &uwsgi.logto2, 0},
	{"log-format", required_argument, 0, "set advanced format for request logging", uwsgi_opt_set_str, &uwsgi.logformat, 0},
	{"logformat", required_argument, 0, "set advanced format for request logging", uwsgi_opt_set_str, &uwsgi.logformat, 0},
	{"logformat-strftime", no_argument, 0, "apply strftime to logformat output", uwsgi_opt_true, &uwsgi.logformat_strftime, 0},
	{"log-format-strftime", no_argument, 0, "apply strftime to logformat output", uwsgi_opt_true, &uwsgi.logformat_strftime, 0},
	{"logfile-chown", no_argument, 0, "chown logfiles", uwsgi_opt_true, &uwsgi.logfile_chown, 0},
	{"logfile-chmod", required_argument, 0, "chmod logfiles", uwsgi_opt_logfile_chmod, NULL, 0},
	{"log-syslog", optional_argument, 0, "log to syslog", uwsgi_opt_set_logger, "syslog", UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"log-socket", required_argument, 0, "send logs to the specified socket", uwsgi_opt_set_logger, "socket", UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"req-logger", required_argument, 0, "set/append a request logger", uwsgi_opt_set_req_logger, NULL, UWSGI_OPT_REQ_LOG_MASTER},
	{"logger-req", required_argument, 0, "set/append a request logger", uwsgi_opt_set_req_logger, NULL, UWSGI_OPT_REQ_LOG_MASTER},
	{"logger", required_argument, 0, "set/append a logger", uwsgi_opt_set_logger, NULL, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"logger-list", no_argument, 0, "list enabled loggers", uwsgi_opt_true, &uwsgi.loggers_list, 0},
	{"loggers-list", no_argument, 0, "list enabled loggers", uwsgi_opt_true, &uwsgi.loggers_list, 0},
	{"threaded-logger", no_argument, 0, "offload log writing to a thread", uwsgi_opt_true, &uwsgi.threaded_logger, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},


	{"log-encoder", required_argument, 0, "add an item in the log encoder chain", uwsgi_opt_add_string_list, &uwsgi.requested_log_encoders, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"log-req-encoder", required_argument, 0, "add an item in the log req encoder chain", uwsgi_opt_add_string_list, &uwsgi.requested_log_req_encoders, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"log-drain", required_argument, 0, "drain (do not show) log lines matching the specified regexp", uwsgi_opt_add_regexp_list, &uwsgi.log_drain_rules, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"log-filter", required_argument, 0, "show only log lines matching the specified regexp", uwsgi_opt_add_regexp_list, &uwsgi.log_filter_rules, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"log-route", required_argument, 0, "log to the specified named logger if regexp applied on logline matches", uwsgi_opt_add_regexp_custom_list, &uwsgi.log_route, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"log-req-route", required_argument, 0, "log requests to the specified named logger if regexp applied on logline matches", uwsgi_opt_add_regexp_custom_list, &uwsgi.log_req_route, UWSGI_OPT_REQ_LOG_MASTER},
#endif

	{"use-abort", no_argument, 0, "call abort() on segfault/fpe, could be useful for generating a core dump", uwsgi_opt_true, &uwsgi.use_abort, 0},

	{"alarm", required_argument, 0, "create a new alarm, syntax: <alarm> <plugin:args>", uwsgi_opt_add_string_list, &uwsgi.alarm_list, UWSGI_OPT_MASTER},
	{"alarm-cheap", required_argument, 0, "use main alarm thread rather than create dedicated threads for curl-based alarms", uwsgi_opt_true, &uwsgi.alarm_cheap, 0},
	{"alarm-freq", required_argument, 0, "tune the anti-loop alarm system (default 3 seconds)", uwsgi_opt_set_int, &uwsgi.alarm_freq, 0},
	{"alarm-fd", required_argument, 0, "raise the specified alarm when an fd is read for read (by default it reads 1 byte, set 8 for eventfd)", uwsgi_opt_add_string_list, &uwsgi.alarm_fd_list, UWSGI_OPT_MASTER},
	{"alarm-segfault", required_argument, 0, "raise the specified alarm when the segmentation fault handler is executed", uwsgi_opt_add_string_list, &uwsgi.alarm_segfault, UWSGI_OPT_MASTER},
	{"segfault-alarm", required_argument, 0, "raise the specified alarm when the segmentation fault handler is executed", uwsgi_opt_add_string_list, &uwsgi.alarm_segfault, UWSGI_OPT_MASTER},
	{"alarm-backlog", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
	{"backlog-alarm", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
	{"lq-alarm", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
	{"alarm-lq", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
	{"alarm-listen-queue", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
	{"listen-queue-alarm", required_argument, 0, "raise the specified alarm when the socket backlog queue is full", uwsgi_opt_add_string_list, &uwsgi.alarm_backlog, UWSGI_OPT_MASTER},
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"log-alarm", required_argument, 0, "raise the specified alarm when a log line matches the specified regexp, syntax: <alarm>[,alarm...] <regexp>", uwsgi_opt_add_string_list, &uwsgi.alarm_logs_list, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"alarm-log", required_argument, 0, "raise the specified alarm when a log line matches the specified regexp, syntax: <alarm>[,alarm...] <regexp>", uwsgi_opt_add_string_list, &uwsgi.alarm_logs_list, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"not-log-alarm", required_argument, 0, "skip the specified alarm when a log line matches the specified regexp, syntax: <alarm>[,alarm...] <regexp>", uwsgi_opt_add_string_list_custom, &uwsgi.alarm_logs_list, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
	{"not-alarm-log", required_argument, 0, "skip the specified alarm when a log line matches the specified regexp, syntax: <alarm>[,alarm...] <regexp>", uwsgi_opt_add_string_list_custom, &uwsgi.alarm_logs_list, UWSGI_OPT_MASTER | UWSGI_OPT_LOG_MASTER},
#endif
	{"alarm-list", no_argument, 0, "list enabled alarms", uwsgi_opt_true, &uwsgi.alarms_list, 0},
	{"alarms-list", no_argument, 0, "list enabled alarms", uwsgi_opt_true, &uwsgi.alarms_list, 0},
	{"alarm-msg-size", required_argument, 0, "set the max size of an alarm message (default 8192)", uwsgi_opt_set_64bit, &uwsgi.alarm_msg_size, 0},
	{"log-master", no_argument, 0, "delegate logging to master process", uwsgi_opt_true, &uwsgi.log_master, UWSGI_OPT_MASTER|UWSGI_OPT_LOG_MASTER},
	{"log-master-bufsize", required_argument, 0, "set the buffer size for the master logger. bigger log messages will be truncated", uwsgi_opt_set_64bit, &uwsgi.log_master_bufsize, 0},
	{"log-master-stream", no_argument, 0, "create the master logpipe as SOCK_STREAM", uwsgi_opt_true, &uwsgi.log_master_stream, 0},
	{"log-master-req-stream", no_argument, 0, "create the master requests logpipe as SOCK_STREAM", uwsgi_opt_true, &uwsgi.log_master_req_stream, 0},
	{"log-reopen", no_argument, 0, "reopen log after reload", uwsgi_opt_true, &uwsgi.log_reopen, 0},
	{"log-truncate", no_argument, 0, "truncate log on startup", uwsgi_opt_true, &uwsgi.log_truncate, 0},
	{"log-maxsize", required_argument, 0, "set maximum logfile size", uwsgi_opt_set_64bit, &uwsgi.log_maxsize, UWSGI_OPT_MASTER|UWSGI_OPT_LOG_MASTER},
	{"log-backupname", required_argument, 0, "set logfile name after rotation", uwsgi_opt_set_str, &uwsgi.log_backupname, 0},

	{"logdate", optional_argument, 0, "prefix logs with date or a strftime string", uwsgi_opt_log_date, NULL, 0},
	{"log-date", optional_argument, 0, "prefix logs with date or a strftime string", uwsgi_opt_log_date, NULL, 0},
	{"log-prefix", optional_argument, 0, "prefix logs with a string", uwsgi_opt_log_date, NULL, 0},

	{"log-zero", no_argument, 0, "log responses without body", uwsgi_opt_true, &uwsgi.logging_options.zero, 0},
	{"log-slow", required_argument, 0, "log requests slower than the specified number of milliseconds", uwsgi_opt_set_int, &uwsgi.logging_options.slow, 0},
	{"log-4xx", no_argument, 0, "log requests with a 4xx response", uwsgi_opt_true, &uwsgi.logging_options._4xx, 0},
	{"log-5xx", no_argument, 0, "log requests with a 5xx response", uwsgi_opt_true, &uwsgi.logging_options._5xx, 0},
	{"log-big", required_argument, 0, "log requests bigger than the specified size", uwsgi_opt_set_64bit,  &uwsgi.logging_options.big, 0},
	{"log-sendfile", required_argument, 0, "log sendfile requests", uwsgi_opt_true, &uwsgi.logging_options.sendfile, 0},
	{"log-ioerror", required_argument, 0, "log requests with io errors", uwsgi_opt_true, &uwsgi.logging_options.ioerror, 0},
	{"log-micros", no_argument, 0, "report response time in microseconds instead of milliseconds", uwsgi_opt_true, &uwsgi.log_micros, 0},
	{"log-x-forwarded-for", no_argument, 0, "use the ip from X-Forwarded-For header instead of REMOTE_ADDR", uwsgi_opt_true, &uwsgi.logging_options.log_x_forwarded_for, 0},
	{"master-as-root", no_argument, 0, "leave master process running as root", uwsgi_opt_true, &uwsgi.master_as_root, 0},

	{"drop-after-init", no_argument, 0, "run privileges drop after plugin initialization, superseded by drop-after-apps", uwsgi_opt_true, &uwsgi.drop_after_init, 0},
	{"drop-after-apps", no_argument, 0, "run privileges drop after apps loading, superseded by master-as-root", uwsgi_opt_true, &uwsgi.drop_after_apps, 0},

	{"force-cwd", required_argument, 0, "force the initial working directory to the specified value", uwsgi_opt_set_str, &uwsgi.force_cwd, 0},
	{"binsh", required_argument, 0, "override /bin/sh (used by exec hooks, it always fallback to /bin/sh)", uwsgi_opt_add_string_list, &uwsgi.binsh, 0},
	{"chdir", required_argument, 0, "chdir to specified directory before apps loading", uwsgi_opt_set_str, &uwsgi.chdir, 0},
	{"chdir2", required_argument, 0, "chdir to specified directory after apps loading", uwsgi_opt_set_str, &uwsgi.chdir2, 0},
	{"lazy", no_argument, 0, "set lazy mode (load apps in workers instead of master)", uwsgi_opt_true, &uwsgi.lazy, 0},
	{"lazy-apps", no_argument, 0, "load apps in each worker instead of the master", uwsgi_opt_true, &uwsgi.lazy_apps, 0},
	{"cheap", no_argument, 0, "set cheap mode (spawn workers only after the first request)", uwsgi_opt_true, &uwsgi.status.is_cheap, UWSGI_OPT_MASTER},
	{"cheaper", required_argument, 0, "set cheaper mode (adaptive process spawning)", uwsgi_opt_set_int, &uwsgi.cheaper_count, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"cheaper-initial", required_argument, 0, "set the initial number of processes to spawn in cheaper mode", uwsgi_opt_set_int, &uwsgi.cheaper_initial, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"cheaper-algo", required_argument, 0, "choose to algorithm used for adaptive process spawning", uwsgi_opt_set_str, &uwsgi.requested_cheaper_algo, UWSGI_OPT_MASTER},
	{"cheaper-step", required_argument, 0, "number of additional processes to spawn at each overload", uwsgi_opt_set_int, &uwsgi.cheaper_step, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"cheaper-overload", required_argument, 0, "increase workers after specified overload", uwsgi_opt_set_64bit, &uwsgi.cheaper_overload, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"cheaper-algo-list", no_argument, 0, "list enabled cheapers algorithms", uwsgi_opt_true, &uwsgi.cheaper_algo_list, 0},
	{"cheaper-algos-list", no_argument, 0, "list enabled cheapers algorithms", uwsgi_opt_true, &uwsgi.cheaper_algo_list, 0},
	{"cheaper-list", no_argument, 0, "list enabled cheapers algorithms", uwsgi_opt_true, &uwsgi.cheaper_algo_list, 0},
	{"cheaper-rss-limit-soft", required_argument, 0, "don't spawn new workers if total resident memory usage of all workers is higher than this limit", uwsgi_opt_set_64bit, &uwsgi.cheaper_rss_limit_soft, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"cheaper-rss-limit-hard", required_argument, 0, "if total workers resident memory usage is higher try to stop workers", uwsgi_opt_set_64bit, &uwsgi.cheaper_rss_limit_hard, UWSGI_OPT_MASTER | UWSGI_OPT_CHEAPER},
	{"idle", required_argument, 0, "set idle mode (put uWSGI in cheap mode after inactivity)", uwsgi_opt_set_int, &uwsgi.idle, UWSGI_OPT_MASTER},
	{"die-on-idle", no_argument, 0, "shutdown uWSGI when idle", uwsgi_opt_true, &uwsgi.die_on_idle, 0},
	{"mount", required_argument, 0, "load application under mountpoint", uwsgi_opt_add_string_list, &uwsgi.mounts, 0},
	{"worker-mount", required_argument, 0, "load application under mountpoint in the specified worker or after workers spawn", uwsgi_opt_add_string_list, &uwsgi.mounts, 0},

	{"threads", required_argument, 0, "run each worker in prethreaded mode with the specified number of threads", uwsgi_opt_set_int, &uwsgi.threads, UWSGI_OPT_THREADS},
	{"thread-stacksize", required_argument, 0, "set threads stacksize", uwsgi_opt_set_int, &uwsgi.threads_stacksize, UWSGI_OPT_THREADS},
	{"threads-stacksize", required_argument, 0, "set threads stacksize", uwsgi_opt_set_int, &uwsgi.threads_stacksize, UWSGI_OPT_THREADS},
	{"thread-stack-size", required_argument, 0, "set threads stacksize", uwsgi_opt_set_int, &uwsgi.threads_stacksize, UWSGI_OPT_THREADS},
	{"threads-stack-size", required_argument, 0, "set threads stacksize", uwsgi_opt_set_int, &uwsgi.threads_stacksize, UWSGI_OPT_THREADS},

	{"vhost", no_argument, 0, "enable virtualhosting mode (based on SERVER_NAME variable)", uwsgi_opt_true, &uwsgi.vhost, 0},
	{"vhost-host", no_argument, 0, "enable virtualhosting mode (based on HTTP_HOST variable)", uwsgi_opt_true, &uwsgi.vhost_host, UWSGI_OPT_VHOST},
#ifdef UWSGI_ROUTING
	{"route", required_argument, 0, "add a route", uwsgi_opt_add_route, "path_info", 0},
	{"route-host", required_argument, 0, "add a route based on Host header", uwsgi_opt_add_route, "http_host", 0},
	{"route-uri", required_argument, 0, "add a route based on REQUEST_URI", uwsgi_opt_add_route, "request_uri", 0},
	{"route-qs", required_argument, 0, "add a route based on QUERY_STRING", uwsgi_opt_add_route, "query_string", 0},
	{"route-remote-addr", required_argument, 0, "add a route based on REMOTE_ADDR", uwsgi_opt_add_route, "remote_addr", 0},
	{"route-user-agent", required_argument, 0, "add a route based on HTTP_USER_AGENT", uwsgi_opt_add_route, "user_agent", 0},
	{"route-remote-user", required_argument, 0, "add a route based on REMOTE_USER", uwsgi_opt_add_route, "remote_user", 0},
	{"route-referer", required_argument, 0, "add a route based on HTTP_REFERER", uwsgi_opt_add_route, "referer", 0},
	{"route-label", required_argument, 0, "add a routing label (for use with goto)", uwsgi_opt_add_route, NULL, 0},
	{"route-if", required_argument, 0, "add a route based on condition", uwsgi_opt_add_route, "if", 0},
	{"route-if-not", required_argument, 0, "add a route based on condition (negate version)", uwsgi_opt_add_route, "if-not", 0},
	{"route-run", required_argument, 0, "always run the specified route action", uwsgi_opt_add_route, "run", 0},



	{"final-route", required_argument, 0, "add a final route", uwsgi_opt_add_route, "path_info", 0},
	{"final-route-status", required_argument, 0, "add a final route for the specified status", uwsgi_opt_add_route, "status", 0},
        {"final-route-host", required_argument, 0, "add a final route based on Host header", uwsgi_opt_add_route, "http_host", 0},
        {"final-route-uri", required_argument, 0, "add a final route based on REQUEST_URI", uwsgi_opt_add_route, "request_uri", 0},
        {"final-route-qs", required_argument, 0, "add a final route based on QUERY_STRING", uwsgi_opt_add_route, "query_string", 0},
        {"final-route-remote-addr", required_argument, 0, "add a final route based on REMOTE_ADDR", uwsgi_opt_add_route, "remote_addr", 0},
        {"final-route-user-agent", required_argument, 0, "add a final route based on HTTP_USER_AGENT", uwsgi_opt_add_route, "user_agent", 0},
        {"final-route-remote-user", required_argument, 0, "add a final route based on REMOTE_USER", uwsgi_opt_add_route, "remote_user", 0},
        {"final-route-referer", required_argument, 0, "add a final route based on HTTP_REFERER", uwsgi_opt_add_route, "referer", 0},
        {"final-route-label", required_argument, 0, "add a final routing label (for use with goto)", uwsgi_opt_add_route, NULL, 0},
        {"final-route-if", required_argument, 0, "add a final route based on condition", uwsgi_opt_add_route, "if", 0},
        {"final-route-if-not", required_argument, 0, "add a final route based on condition (negate version)", uwsgi_opt_add_route, "if-not", 0},
        {"final-route-run", required_argument, 0, "always run the specified final route action", uwsgi_opt_add_route, "run", 0},

	{"error-route", required_argument, 0, "add an error route", uwsgi_opt_add_route, "path_info", 0},
	{"error-route-status", required_argument, 0, "add an error route for the specified status", uwsgi_opt_add_route, "status", 0},
        {"error-route-host", required_argument, 0, "add an error route based on Host header", uwsgi_opt_add_route, "http_host", 0},
        {"error-route-uri", required_argument, 0, "add an error route based on REQUEST_URI", uwsgi_opt_add_route, "request_uri", 0},
        {"error-route-qs", required_argument, 0, "add an error route based on QUERY_STRING", uwsgi_opt_add_route, "query_string", 0},
        {"error-route-remote-addr", required_argument, 0, "add an error route based on REMOTE_ADDR", uwsgi_opt_add_route, "remote_addr", 0},
        {"error-route-user-agent", required_argument, 0, "add an error route based on HTTP_USER_AGENT", uwsgi_opt_add_route, "user_agent", 0},
        {"error-route-remote-user", required_argument, 0, "add an error route based on REMOTE_USER", uwsgi_opt_add_route, "remote_user", 0},
        {"error-route-referer", required_argument, 0, "add an error route based on HTTP_REFERER", uwsgi_opt_add_route, "referer", 0},
        {"error-route-label", required_argument, 0, "add an error routing label (for use with goto)", uwsgi_opt_add_route, NULL, 0},
        {"error-route-if", required_argument, 0, "add an error route based on condition", uwsgi_opt_add_route, "if", 0},
        {"error-route-if-not", required_argument, 0, "add an error route based on condition (negate version)", uwsgi_opt_add_route, "if-not", 0},
        {"error-route-run", required_argument, 0, "always run the specified error route action", uwsgi_opt_add_route, "run", 0},

	{"response-route", required_argument, 0, "add a response route", uwsgi_opt_add_route, "path_info", 0},
        {"response-route-status", required_argument, 0, "add a response route for the specified status", uwsgi_opt_add_route, "status", 0},
        {"response-route-host", required_argument, 0, "add a response route based on Host header", uwsgi_opt_add_route, "http_host", 0},
        {"response-route-uri", required_argument, 0, "add a response route based on REQUEST_URI", uwsgi_opt_add_route, "request_uri", 0},
        {"response-route-qs", required_argument, 0, "add a response route based on QUERY_STRING", uwsgi_opt_add_route, "query_string", 0},
        {"response-route-remote-addr", required_argument, 0, "add a response route based on REMOTE_ADDR", uwsgi_opt_add_route, "remote_addr", 0},
        {"response-route-user-agent", required_argument, 0, "add a response route based on HTTP_USER_AGENT", uwsgi_opt_add_route, "user_agent", 0},
        {"response-route-remote-user", required_argument, 0, "add a response route based on REMOTE_USER", uwsgi_opt_add_route, "remote_user", 0},
        {"response-route-referer", required_argument, 0, "add a response route based on HTTP_REFERER", uwsgi_opt_add_route, "referer", 0},
        {"response-route-label", required_argument, 0, "add a response routing label (for use with goto)", uwsgi_opt_add_route, NULL, 0},
        {"response-route-if", required_argument, 0, "add a response route based on condition", uwsgi_opt_add_route, "if", 0},
        {"response-route-if-not", required_argument, 0, "add a response route based on condition (negate version)", uwsgi_opt_add_route, "if-not", 0},
        {"response-route-run", required_argument, 0, "always run the specified response route action", uwsgi_opt_add_route, "run", 0},

	{"router-list", no_argument, 0, "list enabled routers", uwsgi_opt_true, &uwsgi.router_list, 0},
	{"routers-list", no_argument, 0, "list enabled routers", uwsgi_opt_true, &uwsgi.router_list, 0},
#endif


	{"error-page-403", required_argument, 0, "add an error page (html) for managed 403 response", uwsgi_opt_add_string_list, &uwsgi.error_page_403, 0},
	{"error-page-404", required_argument, 0, "add an error page (html) for managed 404 response", uwsgi_opt_add_string_list, &uwsgi.error_page_404, 0},
	{"error-page-500", required_argument, 0, "add an error page (html) for managed 500 response", uwsgi_opt_add_string_list, &uwsgi.error_page_500, 0},

	{"websockets-ping-freq", required_argument, 0, "set the frequency (in seconds) of websockets automatic ping packets", uwsgi_opt_set_int, &uwsgi.websockets_ping_freq, 0},
	{"websocket-ping-freq", required_argument, 0, "set the frequency (in seconds) of websockets automatic ping packets", uwsgi_opt_set_int, &uwsgi.websockets_ping_freq, 0},

	{"websockets-pong-tolerance", required_argument, 0, "set the tolerance (in seconds) of websockets ping/pong subsystem", uwsgi_opt_set_int, &uwsgi.websockets_pong_tolerance, 0},
	{"websocket-pong-tolerance", required_argument, 0, "set the tolerance (in seconds) of websockets ping/pong subsystem", uwsgi_opt_set_int, &uwsgi.websockets_pong_tolerance, 0},

	{"websockets-max-size", required_argument, 0, "set the max allowed size of websocket messages (in Kbytes, default 1024)", uwsgi_opt_set_64bit, &uwsgi.websockets_max_size, 0},
	{"websocket-max-size", required_argument, 0, "set the max allowed size of websocket messages (in Kbytes, default 1024)", uwsgi_opt_set_64bit, &uwsgi.websockets_max_size, 0},

	{"chunked-input-limit", required_argument, 0, "set the max size of a chunked input part (default 1MB, in bytes)", uwsgi_opt_set_64bit, &uwsgi.chunked_input_limit, 0},
	{"chunked-input-timeout", required_argument, 0, "set default timeout for chunked input", uwsgi_opt_set_int, &uwsgi.chunked_input_timeout, 0},

	{"clock", required_argument, 0, "set a clock source", uwsgi_opt_set_str, &uwsgi.requested_clock, 0},

	{"clock-list", no_argument, 0, "list enabled clocks", uwsgi_opt_true, &uwsgi.clock_list, 0},
	{"clocks-list", no_argument, 0, "list enabled clocks", uwsgi_opt_true, &uwsgi.clock_list, 0},

	{"add-header", required_argument, 0, "automatically add HTTP headers to response", uwsgi_opt_add_string_list, &uwsgi.additional_headers, 0},
	{"rem-header", required_argument, 0, "automatically remove specified HTTP header from the response", uwsgi_opt_add_string_list, &uwsgi.remove_headers, 0},
	{"del-header", required_argument, 0, "automatically remove specified HTTP header from the response", uwsgi_opt_add_string_list, &uwsgi.remove_headers, 0},
	{"collect-header", required_argument, 0, "store the specified response header in a request var (syntax: header var)", uwsgi_opt_add_string_list, &uwsgi.collect_headers, 0},
	{"response-header-collect", required_argument, 0, "store the specified response header in a request var (syntax: header var)", uwsgi_opt_add_string_list, &uwsgi.collect_headers, 0},

	{"pull-header", required_argument, 0, "store the specified response header in a request var and remove it from the response (syntax: header var)", uwsgi_opt_add_string_list, &uwsgi.pull_headers, 0},

	{"check-static", required_argument, 0, "check for static files in the specified directory", uwsgi_opt_check_static, NULL, UWSGI_OPT_MIME},
	{"check-static-docroot", no_argument, 0, "check for static files in the requested DOCUMENT_ROOT", uwsgi_opt_true, &uwsgi.check_static_docroot, UWSGI_OPT_MIME},
	{"static-check", required_argument, 0, "check for static files in the specified directory", uwsgi_opt_check_static, NULL, UWSGI_OPT_MIME},
	{"static-map", required_argument, 0, "map mountpoint to static directory (or file)", uwsgi_opt_static_map, &uwsgi.static_maps, UWSGI_OPT_MIME},
	{"static-map2", required_argument, 0, "like static-map but completely appending the requested resource to the docroot", uwsgi_opt_static_map, &uwsgi.static_maps2, UWSGI_OPT_MIME},
	{"static-skip-ext", required_argument, 0, "skip specified extension from staticfile checks", uwsgi_opt_add_string_list, &uwsgi.static_skip_ext, UWSGI_OPT_MIME},
	{"static-index", required_argument, 0, "search for specified file if a directory is requested", uwsgi_opt_add_string_list, &uwsgi.static_index, UWSGI_OPT_MIME},
	{"static-safe", required_argument, 0, "skip security checks if the file is under the specified path", uwsgi_opt_add_string_list, &uwsgi.static_safe, UWSGI_OPT_MIME},
	{"static-cache-paths", required_argument, 0, "put resolved paths in the uWSGI cache for the specified amount of seconds", uwsgi_opt_set_int, &uwsgi.use_static_cache_paths, UWSGI_OPT_MIME|UWSGI_OPT_MASTER},
	{"static-cache-paths-name", required_argument, 0, "use the specified cache for static paths", uwsgi_opt_set_str, &uwsgi.static_cache_paths_name, UWSGI_OPT_MIME|UWSGI_OPT_MASTER},
#ifdef __APPLE__
	{"mimefile", required_argument, 0, "set mime types file path (default /etc/apache2/mime.types)", uwsgi_opt_add_string_list, &uwsgi.mime_file, UWSGI_OPT_MIME},
	{"mime-file", required_argument, 0, "set mime types file path (default /etc/apache2/mime.types)", uwsgi_opt_add_string_list, &uwsgi.mime_file, UWSGI_OPT_MIME},
#else
	{"mimefile", required_argument, 0, "set mime types file path (default /etc/mime.types)", uwsgi_opt_add_string_list, &uwsgi.mime_file, UWSGI_OPT_MIME},
	{"mime-file", required_argument, 0, "set mime types file path (default /etc/mime.types)", uwsgi_opt_add_string_list, &uwsgi.mime_file, UWSGI_OPT_MIME},
#endif

	{"static-expires-type", required_argument, 0, "set the Expires header based on content type", uwsgi_opt_add_dyn_dict, &uwsgi.static_expires_type, UWSGI_OPT_MIME},
	{"static-expires-type-mtime", required_argument, 0, "set the Expires header based on content type and file mtime", uwsgi_opt_add_dyn_dict, &uwsgi.static_expires_type_mtime, UWSGI_OPT_MIME},

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	{"static-expires", required_argument, 0, "set the Expires header based on filename regexp", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires, UWSGI_OPT_MIME},
	{"static-expires-mtime", required_argument, 0, "set the Expires header based on filename regexp and file mtime", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires_mtime, UWSGI_OPT_MIME},

	{"static-expires-uri", required_argument, 0, "set the Expires header based on REQUEST_URI regexp", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires_uri, UWSGI_OPT_MIME},
	{"static-expires-uri-mtime", required_argument, 0, "set the Expires header based on REQUEST_URI regexp and file mtime", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires_uri_mtime, UWSGI_OPT_MIME},

	{"static-expires-path-info", required_argument, 0, "set the Expires header based on PATH_INFO regexp", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires_path_info, UWSGI_OPT_MIME},
	{"static-expires-path-info-mtime", required_argument, 0, "set the Expires header based on PATH_INFO regexp and file mtime", uwsgi_opt_add_regexp_dyn_dict, &uwsgi.static_expires_path_info_mtime, UWSGI_OPT_MIME},
	{"static-gzip", required_argument, 0, "if the supplied regexp matches the static file translation it will search for a gzip version", uwsgi_opt_add_regexp_list, &uwsgi.static_gzip, UWSGI_OPT_MIME},
#endif
	{"static-gzip-all", no_argument, 0, "check for a gzip version of all requested static files", uwsgi_opt_true, &uwsgi.static_gzip_all, UWSGI_OPT_MIME},
	{"static-gzip-dir", required_argument, 0, "check for a gzip version of all requested static files in the specified dir/prefix", uwsgi_opt_add_string_list, &uwsgi.static_gzip_dir, UWSGI_OPT_MIME},
	{"static-gzip-prefix", required_argument, 0, "check for a gzip version of all requested static files in the specified dir/prefix", uwsgi_opt_add_string_list, &uwsgi.static_gzip_dir, UWSGI_OPT_MIME},
	{"static-gzip-ext", required_argument, 0, "check for a gzip version of all requested static files with the specified ext/suffix", uwsgi_opt_add_string_list, &uwsgi.static_gzip_ext, UWSGI_OPT_MIME},
	{"static-gzip-suffix", required_argument, 0, "check for a gzip version of all requested static files with the specified ext/suffix", uwsgi_opt_add_string_list, &uwsgi.static_gzip_ext, UWSGI_OPT_MIME},

	{"honour-range", no_argument, 0, "enable support for the HTTP Range header", uwsgi_opt_true, &uwsgi.honour_range, 0},

	{"offload-threads", required_argument, 0, "set the number of offload threads to spawn (per-worker, default 0)", uwsgi_opt_set_int, &uwsgi.offload_threads, 0},
	{"offload-thread", required_argument, 0, "set the number of offload threads to spawn (per-worker, default 0)", uwsgi_opt_set_int, &uwsgi.offload_threads, 0},

	{"file-serve-mode", required_argument, 0, "set static file serving mode", uwsgi_opt_fileserve_mode, NULL, UWSGI_OPT_MIME},
	{"fileserve-mode", required_argument, 0, "set static file serving mode", uwsgi_opt_fileserve_mode, NULL, UWSGI_OPT_MIME},

	{"disable-sendfile", no_argument, 0, "disable sendfile() and rely on boring read()/write()", uwsgi_opt_true, &uwsgi.disable_sendfile, 0},

	{"check-cache", optional_argument, 0, "check for response data in the specified cache (empty for default cache)", uwsgi_opt_set_str, &uwsgi.use_check_cache, 0},
	{"close-on-exec", no_argument, 0, "set close-on-exec on connection sockets (could be required for spawning processes in requests)", uwsgi_opt_true, &uwsgi.close_on_exec, 0},
	{"close-on-exec2", no_argument, 0, "set close-on-exec on server sockets (could be required for spawning processes in requests)", uwsgi_opt_true, &uwsgi.close_on_exec2, 0},
	{"mode", required_argument, 0, "set uWSGI custom mode", uwsgi_opt_set_str, &uwsgi.mode, 0},
	{"env", required_argument, 0, "set environment variable", uwsgi_opt_set_env, NULL, 0},
	{"envdir", required_argument, 0, "load a daemontools compatible envdir", uwsgi_opt_add_string_list, &uwsgi.envdirs, 0},
	{"early-envdir", required_argument, 0, "load a daemontools compatible envdir ASAP", uwsgi_opt_envdir, NULL, UWSGI_OPT_IMMEDIATE},
	{"unenv", required_argument, 0, "unset environment variable", uwsgi_opt_unset_env, NULL, 0},
	{"vacuum", no_argument, 0, "try to remove all of the generated file/sockets", uwsgi_opt_true, &uwsgi.vacuum, 0},
	{"file-write", required_argument, 0, "write the specified content to the specified file (syntax: file=value) before privileges drop", uwsgi_opt_add_string_list, &uwsgi.file_write_list, 0},
#ifdef __linux__
	{"cgroup", required_argument, 0, "put the processes in the specified cgroup", uwsgi_opt_add_string_list, &uwsgi.cgroup, 0},
	{"cgroup-opt", required_argument, 0, "set value in specified cgroup option", uwsgi_opt_add_string_list, &uwsgi.cgroup_opt, 0},
	{"cgroup-dir-mode", required_argument, 0, "set permission for cgroup directory (default is 700)", uwsgi_opt_set_str, &uwsgi.cgroup_dir_mode, 0},
	{"namespace", required_argument, 0, "run in a new namespace under the specified rootfs", uwsgi_opt_set_str, &uwsgi.ns, 0},
	{"namespace-keep-mount", required_argument, 0, "keep the specified mountpoint in your namespace", uwsgi_opt_add_string_list, &uwsgi.ns_keep_mount, 0},
	{"ns", required_argument, 0, "run in a new namespace under the specified rootfs", uwsgi_opt_set_str, &uwsgi.ns, 0},
	{"namespace-net", required_argument, 0, "add network namespace", uwsgi_opt_set_str, &uwsgi.ns_net, 0},
	{"ns-net", required_argument, 0, "add network namespace", uwsgi_opt_set_str, &uwsgi.ns_net, 0},
#endif
	{"enable-proxy-protocol", no_argument, 0, "enable PROXY1 protocol support (only for http parsers)", uwsgi_opt_true, &uwsgi.enable_proxy_protocol, 0},
	{"reuse-port", no_argument, 0, "enable REUSE_PORT flag on socket (BSD only)", uwsgi_opt_true, &uwsgi.reuse_port, 0},
	{"tcp-fast-open", required_argument, 0, "enable TCP_FASTOPEN flag on TCP sockets with the specified qlen value", uwsgi_opt_set_int, &uwsgi.tcp_fast_open, 0},
	{"tcp-fastopen", required_argument, 0, "enable TCP_FASTOPEN flag on TCP sockets with the specified qlen value", uwsgi_opt_set_int, &uwsgi.tcp_fast_open, 0},
	{"tcp-fast-open-client", no_argument, 0, "use sendto(..., MSG_FASTOPEN, ...) instead of connect() if supported", uwsgi_opt_true, &uwsgi.tcp_fast_open_client, 0},
	{"tcp-fastopen-client", no_argument, 0, "use sendto(..., MSG_FASTOPEN, ...) instead of connect() if supported", uwsgi_opt_true, &uwsgi.tcp_fast_open_client, 0},
	{"zerg", required_argument, 0, "attach to a zerg server", uwsgi_opt_add_string_list, &uwsgi.zerg_node, 0},
	{"zerg-fallback", no_argument, 0, "fallback to normal sockets if the zerg server is not available", uwsgi_opt_true, &uwsgi.zerg_fallback, 0},
	{"zerg-server", required_argument, 0, "enable the zerg server on the specified UNIX socket", uwsgi_opt_set_str, &uwsgi.zerg_server, UWSGI_OPT_MASTER},

	{"cron", required_argument, 0, "add a cron task", uwsgi_opt_add_cron, NULL, UWSGI_OPT_MASTER},
	{"cron2", required_argument, 0, "add a cron task (key=val syntax)", uwsgi_opt_add_cron2, NULL, UWSGI_OPT_MASTER},
	{"unique-cron", required_argument, 0, "add a unique cron task", uwsgi_opt_add_unique_cron, NULL, UWSGI_OPT_MASTER},
	{"cron-harakiri", required_argument, 0, "set the maximum time (in seconds) we wait for cron command to complete", uwsgi_opt_set_int, &uwsgi.cron_harakiri, 0},
#ifdef UWSGI_SSL
	{"legion-cron", required_argument, 0, "add a cron task runnable only when the instance is a lord of the specified legion", uwsgi_opt_add_legion_cron, NULL, UWSGI_OPT_MASTER},
	{"cron-legion", required_argument, 0, "add a cron task runnable only when the instance is a lord of the specified legion", uwsgi_opt_add_legion_cron, NULL, UWSGI_OPT_MASTER},
	{"unique-legion-cron", required_argument, 0, "add a unique cron task runnable only when the instance is a lord of the specified legion", uwsgi_opt_add_unique_legion_cron, NULL, UWSGI_OPT_MASTER},
	{"unique-cron-legion", required_argument, 0, "add a unique cron task runnable only when the instance is a lord of the specified legion", uwsgi_opt_add_unique_legion_cron, NULL, UWSGI_OPT_MASTER},
#endif
	{"loop", required_argument, 0, "select the uWSGI loop engine", uwsgi_opt_set_str, &uwsgi.loop, 0},
	{"loop-list", no_argument, 0, "list enabled loop engines", uwsgi_opt_true, &uwsgi.loop_list, 0},
	{"loops-list", no_argument, 0, "list enabled loop engines", uwsgi_opt_true, &uwsgi.loop_list, 0},
	{"worker-exec", required_argument, 0, "run the specified command as worker", uwsgi_opt_set_str, &uwsgi.worker_exec, 0},
	{"worker-exec2", required_argument, 0, "run the specified command as worker (after post_fork hook)", uwsgi_opt_set_str, &uwsgi.worker_exec2, 0},
	{"attach-daemon", required_argument, 0, "attach a command/daemon to the master process (the command has to not go in background)", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
	{"attach-control-daemon", required_argument, 0, "attach a command/daemon to the master process (the command has to not go in background), when the daemon dies, the master dies too", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
	{"smart-attach-daemon", required_argument, 0, "attach a command/daemon to the master process managed by a pidfile (the command has to daemonize)", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
	{"smart-attach-daemon2", required_argument, 0, "attach a command/daemon to the master process managed by a pidfile (the command has to NOT daemonize)", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
#ifdef UWSGI_SSL
	{"legion-attach-daemon", required_argument, 0, "same as --attach-daemon but daemon runs only on legion lord node", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
	{"legion-smart-attach-daemon", required_argument, 0, "same as --smart-attach-daemon but daemon runs only on legion lord node", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
	{"legion-smart-attach-daemon2", required_argument, 0, "same as --smart-attach-daemon2 but daemon runs only on legion lord node", uwsgi_opt_add_daemon, NULL, UWSGI_OPT_MASTER},
#endif
	{"daemons-honour-stdin", no_argument, 0, "do not change the stdin of external daemons to /dev/null", uwsgi_opt_true, &uwsgi.daemons_honour_stdin, UWSGI_OPT_MASTER},
	{"attach-daemon2", required_argument, 0, "attach-daemon keyval variant (supports smart modes too)", uwsgi_opt_add_daemon2, NULL, UWSGI_OPT_MASTER},
	{"plugins", required_argument, 0, "load uWSGI plugins", uwsgi_opt_load_plugin, NULL, UWSGI_OPT_IMMEDIATE},
	{"plugin", required_argument, 0, "load uWSGI plugins", uwsgi_opt_load_plugin, NULL, UWSGI_OPT_IMMEDIATE},
	{"need-plugins", required_argument, 0, "load uWSGI plugins (exit on error)", uwsgi_opt_load_plugin, NULL, UWSGI_OPT_IMMEDIATE},
	{"need-plugin", required_argument, 0, "load uWSGI plugins (exit on error)", uwsgi_opt_load_plugin, NULL, UWSGI_OPT_IMMEDIATE},
	{"plugins-dir", required_argument, 0, "add a directory to uWSGI plugin search path", uwsgi_opt_add_string_list, &uwsgi.plugins_dir, UWSGI_OPT_IMMEDIATE},
	{"plugin-dir", required_argument, 0, "add a directory to uWSGI plugin search path", uwsgi_opt_add_string_list, &uwsgi.plugins_dir, UWSGI_OPT_IMMEDIATE},
	{"plugins-list", no_argument, 0, "list enabled plugins", uwsgi_opt_true, &uwsgi.plugins_list, 0},
	{"plugin-list", no_argument, 0, "list enabled plugins", uwsgi_opt_true, &uwsgi.plugins_list, 0},
	{"autoload", no_argument, 0, "try to automatically load plugins when unknown options are found", uwsgi_opt_true, &uwsgi.autoload, UWSGI_OPT_IMMEDIATE},
	{"dlopen", required_argument, 0, "blindly load a shared library", uwsgi_opt_load_dl, NULL, UWSGI_OPT_IMMEDIATE},
	{"allowed-modifiers", required_argument, 0, "comma separated list of allowed modifiers", uwsgi_opt_set_str, &uwsgi.allowed_modifiers, 0},
	{"remap-modifier", required_argument, 0, "remap request modifier from one id to another", uwsgi_opt_set_str, &uwsgi.remap_modifier, 0},

	{"dump-options", no_argument, 0, "dump the full list of available options", uwsgi_opt_true, &uwsgi.dump_options, 0},
	{"show-config", no_argument, 0, "show the current config reformatted as ini", uwsgi_opt_true, &uwsgi.show_config, 0},
	{"binary-append-data", required_argument, 0, "return the content of a resource to stdout for appending to a uwsgi binary (for data:// usage)", uwsgi_opt_binary_append_data, NULL, UWSGI_OPT_IMMEDIATE},
	{"print", required_argument, 0, "simple print", uwsgi_opt_print, NULL, 0},
	{"iprint", required_argument, 0, "simple print (immediate version)", uwsgi_opt_print, NULL, UWSGI_OPT_IMMEDIATE},
	{"exit", optional_argument, 0, "force exit() of the instance", uwsgi_opt_exit, NULL, UWSGI_OPT_IMMEDIATE},
	{"cflags", no_argument, 0, "report uWSGI CFLAGS (useful for building external plugins)", uwsgi_opt_cflags, NULL, UWSGI_OPT_IMMEDIATE},
	{"dot-h", no_argument, 0, "dump the uwsgi.h used for building the core  (useful for building external plugins)", uwsgi_opt_dot_h, NULL, UWSGI_OPT_IMMEDIATE},
	{"config-py", no_argument, 0, "dump the uwsgiconfig.py used for building the core  (useful for building external plugins)", uwsgi_opt_config_py, NULL, UWSGI_OPT_IMMEDIATE},
	{"build-plugin", required_argument, 0, "build a uWSGI plugin for the current binary", uwsgi_opt_build_plugin, NULL, UWSGI_OPT_IMMEDIATE},
	{"version", no_argument, 0, "print uWSGI version", uwsgi_opt_print, UWSGI_VERSION, 0},
	{"response-headers-limit", required_argument, 0, "set response header maximum size (default: 64k)", uwsgi_opt_set_int, &uwsgi.response_header_limit, 0},
	{0, 0, 0, 0, 0, 0, 0}
};

void show_config(void) {
	int i;
	uwsgi_log("\n;uWSGI instance configuration\n[uwsgi]\n");
	for (i = 0; i < uwsgi.exported_opts_cnt; i++) {
		if (uwsgi.exported_opts[i]->value) {
			uwsgi_log("%s = %s\n", uwsgi.exported_opts[i]->key, uwsgi.exported_opts[i]->value);
		}
		else {
			uwsgi_log("%s = true\n", uwsgi.exported_opts[i]->key);
		}
	}
	uwsgi_log(";end of configuration\n\n");

}

void config_magic_table_fill(char *filename, char **magic_table) {

	char *tmp = NULL;
	char *fullname = filename;

	magic_table['o'] = filename;

	if (uwsgi_check_scheme(filename) || !strcmp(filename, "-")) {
		return;
	}

        char *section = uwsgi_get_last_char(filename, ':');
        if (section) {
                *section = 0;
		if (section == filename) {
			goto reuse;
		}
	}


	// we have a special case for symlinks
	if (uwsgi_is_link(filename)) {
		if (filename[0] != '/') {
			fullname = uwsgi_concat3(uwsgi.cwd, "/", filename);
		}
	}
	else {

		fullname = uwsgi_expand_path(filename, strlen(filename), NULL);
		if (!fullname) {
			exit(1);
		}
		char *minimal_name = uwsgi_malloc(strlen(fullname) + 1);
		memcpy(minimal_name, fullname, strlen(fullname));
		minimal_name[strlen(fullname)] = 0;
		free(fullname);
		fullname = minimal_name;
	}

	magic_table['b'] = uwsgi.binary_path;
	magic_table['p'] = fullname;

	// compute filename hash
	uint32_t hash = djb33x_hash(magic_table['p'], strlen(magic_table['p']));
	char *hex = uwsgi_str_to_hex((char *)&hash, 4);
	magic_table['j'] = uwsgi_concat2n(hex, 8, "", 0);
	free(hex);

	struct stat st;
	if (!lstat(fullname, &st)) {
		magic_table['i'] = uwsgi_num2str(st.st_ino);
	}

	magic_table['s'] = uwsgi_get_last_char(fullname, '/') + 1;

	magic_table['d'] = uwsgi_concat2n(magic_table['p'], magic_table['s'] - magic_table['p'], "", 0);
	if (magic_table['d'][strlen(magic_table['d']) - 1] == '/') {
		tmp = magic_table['d'] + (strlen(magic_table['d']) - 1);
#ifdef UWSGI_DEBUG
		uwsgi_log("tmp = %c\n", *tmp);
#endif
		*tmp = 0;
	}

	// clear optional vars
	magic_table['c'] = "";
	magic_table['e'] = "";
	magic_table['n'] = magic_table['s'];

	magic_table['0'] = "";
	magic_table['1'] = "";
	magic_table['2'] = "";
	magic_table['3'] = "";
	magic_table['4'] = "";
	magic_table['5'] = "";
	magic_table['6'] = "";
	magic_table['7'] = "";
	magic_table['8'] = "";
	magic_table['9'] = "";

	if (uwsgi_get_last_char(magic_table['d'], '/')) {
		magic_table['c'] = uwsgi_str(uwsgi_get_last_char(magic_table['d'], '/') + 1);
		if (magic_table['c'][strlen(magic_table['c']) - 1] == '/') {
			magic_table['c'][strlen(magic_table['c']) - 1] = 0;
		}
	}

	int base = '0';
	char *to_split = uwsgi_str(magic_table['d']);
	char *p, *ctx = NULL;
	uwsgi_foreach_token(to_split, "/", p, ctx) {
		if (base <= '9') {
			magic_table[base] = p;
			base++;
		}
		else {
			break;
		}
	}

	if (tmp)
		*tmp = '/';

	if (uwsgi_get_last_char(magic_table['s'], '.'))
		magic_table['e'] = uwsgi_get_last_char(magic_table['s'], '.') + 1;
	if (uwsgi_get_last_char(magic_table['s'], '.'))
		magic_table['n'] = uwsgi_concat2n(magic_table['s'], uwsgi_get_last_char(magic_table['s'], '.') - magic_table['s'], "", 0);

reuse:
	magic_table['x'] = "";
	if (section) {
		magic_table['x'] = section+1;
		*section = ':';
	}

	// first round ?
	if (!uwsgi.magic_table_first_round) { 
		magic_table['O'] = magic_table['o'];
                magic_table['D'] = magic_table['d'];
                magic_table['S'] = magic_table['s'];
                magic_table['P'] = magic_table['p'];
                magic_table['C'] = magic_table['c'];
                magic_table['E'] = magic_table['e'];
                magic_table['N'] = magic_table['n'];
                magic_table['X'] = magic_table['x'];
                magic_table['I'] = magic_table['i'];
                magic_table['J'] = magic_table['j'];
		uwsgi.magic_table_first_round = 1;
        }

}

int find_worker_id(pid_t pid) {
	int i;
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].pid == pid)
			return i;
	}

	return -1;
}


void warn_pipe() {
	struct wsgi_request *wsgi_req = current_wsgi_req();

	if (uwsgi.threads < 2 && wsgi_req->uri_len > 0) {
		uwsgi_log_verbose("SIGPIPE: writing to a closed pipe/socket/fd (probably the client disconnected) on request %.*s (ip %.*s) !!!\n", wsgi_req->uri_len, wsgi_req->uri, wsgi_req->remote_addr_len, wsgi_req->remote_addr);
	}
	else {
		uwsgi_log_verbose("SIGPIPE: writing to a closed pipe/socket/fd (probably the client disconnected) !!!\n");
	}
}

// This function is called from signal handler or main thread to wait worker threads.
// `uwsgi.workers[uwsgi.mywid].manage_next_request` should be set to 0 to stop worker threads.
static void wait_for_threads() {
	int i, ret;

	// This option was added because we used pthread_cancel().
	// thread cancellation is REALLY flaky
	if (uwsgi.no_threads_wait) return;

	// wait for thread termination
	for (i = 0; i < uwsgi.threads; i++) {
		if (!pthread_equal(uwsgi.workers[uwsgi.mywid].cores[i].thread_id, pthread_self())) {
			ret = pthread_join(uwsgi.workers[uwsgi.mywid].cores[i].thread_id, NULL);
			if (ret) {
				uwsgi_log("pthread_join() = %d\n", ret);
			}
			else {
				// uwsgi_worker_is_busy() should not consider this thread as busy.
				uwsgi.workers[uwsgi.mywid].cores[i].in_request = 0;
			}
		}
	}
}


void gracefully_kill(int signum) {

	uwsgi_log("Gracefully killing worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
	uwsgi.workers[uwsgi.mywid].manage_next_request = 0;

	if (uwsgi.threads > 1) {
		// Stop event_queue_wait() in other threads.
		// We use loop_stop_pipe only in threaded workers to avoid
		// unintensional behavior changes in single threaded workers.
		int fd;
		if ((fd = uwsgi.loop_stop_pipe[1]) > 0) {
			close(fd);
			uwsgi.loop_stop_pipe[1] = 0;
		}
		return;
	}

	// still not found a way to gracefully reload in async mode
	if (uwsgi.async > 1) {
		if (uwsgi.workers[uwsgi.mywid].shutdown_sockets)
			uwsgi_shutdown_all_sockets();
		exit(UWSGI_RELOAD_CODE);
	}

	if (!uwsgi.workers[uwsgi.mywid].cores[0].in_request) {
		if (uwsgi.workers[uwsgi.mywid].shutdown_sockets)
			uwsgi_shutdown_all_sockets();
		exit(UWSGI_RELOAD_CODE);
	}
}

void end_me(int signum) {
	if (getpid() != masterpid && uwsgi.skip_atexit) {
		_exit(UWSGI_END_CODE);
		// never here
	}
	exit(UWSGI_END_CODE);
}

static void simple_goodbye_cruel_world() {
	int prev = uwsgi.workers[uwsgi.mywid].manage_next_request;
	uwsgi.workers[uwsgi.mywid].manage_next_request = 0;
	if (prev) {
		// Avoid showing same message from all threads.
		uwsgi_log("...The work of process %d is done. Seeya!\n", getpid());
	}

	if (uwsgi.threads > 1) {
		// Stop event_queue_wait() in other threads.
		// We use loop_stop_pipe only in threaded workers to avoid
		// unintensional behavior changes in single threaded workers.
		int fd;
		if ((fd = uwsgi.loop_stop_pipe[1]) > 0) {
			close(fd);
			uwsgi.loop_stop_pipe[1] = 0;
		}
	}
}

void goodbye_cruel_world() {
	uwsgi_curse(uwsgi.mywid, 0);

	if (!uwsgi.gbcw_hook) {
		simple_goodbye_cruel_world();
	}
	else {
		uwsgi.gbcw_hook();
	}
}

// brutally destroy
void kill_them_all(int signum) {

	if (uwsgi_instance_is_dying) return;
	uwsgi.status.brutally_destroying = 1;

	// unsubscribe if needed
	uwsgi_unsubscribe_all();

	uwsgi_log("SIGINT/SIGTERM received...killing workers...\n");

	int i;
	for (i = 1; i <= uwsgi.numproc; i++) {
                if (uwsgi.workers[i].pid > 0) {
                        uwsgi_curse(i, SIGINT);
                }
        }
	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].pid > 0) {
			uwsgi_curse_mule(i, SIGINT);
		}
	}

	uwsgi_destroy_processes();
}

// gracefully destroy
void gracefully_kill_them_all(int signum) {
	if (uwsgi_instance_is_dying) return;
	uwsgi.status.gracefully_destroying = 1;

	// unsubscribe if needed
	uwsgi_unsubscribe_all();

	uwsgi_log_verbose("graceful shutdown triggered...\n");

	int i;
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].pid > 0) {
			if (uwsgi.shutdown_sockets)
				uwsgi.workers[i].shutdown_sockets = 1;
			uwsgi_curse(i, SIGHUP);
		}
	}

	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].pid > 0) {
			uwsgi_curse_mule(i, SIGHUP);
		}
	}

	// avoid breaking other child process signal handling logic by doing nohang checks on the workers
	// until they are all done.
	int keep_waiting = 1;
	while (keep_waiting == 1) {
		int still_running = 0;
		int errors = 0;
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].pid > 0) {
				pid_t rval = waitpid(uwsgi.workers[i].pid, NULL, WNOHANG);
				if (rval == uwsgi.workers[i].pid) {
					uwsgi.workers[i].pid = 0;
				} else if (rval == 0) {
					still_running++;
				} else if (rval < 0) {
					errors++;
				}
			}
		}

		// exit out if everything is done or we got errors as we can't do much about the errors at this point
		if (still_running == 0 || errors > 0) {
			keep_waiting = 0;
			break;
		}
		sleep(1);
	}

	uwsgi_destroy_processes();
}


// graceful reload
void grace_them_all(int signum) {
	if (uwsgi_instance_is_reloading || uwsgi_instance_is_dying)
		return;

	int i;

	if (uwsgi.lazy) {
		for (i = 1; i <= uwsgi.numproc; i++) {
			if (uwsgi.workers[i].pid > 0) {
				uwsgi_curse(i, SIGHUP);
			}
		}
		return;
	}
	

	uwsgi.status.gracefully_reloading = 1;

	uwsgi_destroy_processes();

	uwsgi_log("...gracefully killing workers...\n");

#ifdef UWSGI_SSL
	uwsgi_legion_announce_death();
#endif

	if (uwsgi.unsubscribe_on_graceful_reload) {
		uwsgi_unsubscribe_all();
	}

	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].pid > 0) {
			uwsgi_curse(i, SIGHUP);
		}
	}

	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].pid > 0) {
			uwsgi_curse_mule(i, SIGHUP);
		}
	}
}

void uwsgi_nuclear_blast() {

	// the Emperor (as an example) cannot nuke itself
	if (uwsgi.disable_nuclear_blast) return;

	if (!uwsgi.workers) {
		reap_them_all(0);
	}
	else if (uwsgi.master_process) {
		if (getpid() == uwsgi.workers[0].pid) {
			reap_them_all(0);
		}
	}

	exit(1);
}

// brutally reload
void reap_them_all(int signum) {

	// avoid reace condition in lazy mode
	if (uwsgi_instance_is_reloading)
		return;

	uwsgi.status.brutally_reloading = 1;

	if (!uwsgi.workers) return;

	uwsgi_destroy_processes();

	uwsgi_log("...brutally killing workers...\n");

#ifdef UWSGI_SSL
        uwsgi_legion_announce_death();
#endif

	// unsubscribe if needed
	uwsgi_unsubscribe_all();

	int i;
	for (i = 1; i <= uwsgi.numproc; i++) {
		if (uwsgi.workers[i].pid > 0)
			uwsgi_curse(i, SIGTERM);
	}
	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].pid > 0) {
			uwsgi_curse_mule(i, SIGTERM);
		}
	}
}

void harakiri() {

	uwsgi_log("\nKilling the current process (pid: %d app_id: %d)...\n", uwsgi.mypid, uwsgi.wsgi_req->app_id);

	if (!uwsgi.master_process) {
		uwsgi_log("*** if you want your workers to be automatically respawned consider enabling the uWSGI master process ***\n");
	}
	exit(0);
}

void stats(int signum) {
	//fix this for better logging(this cause races)
	struct uwsgi_app *ua = NULL;
	int i, j;

	if (uwsgi.mywid == 0) {
		show_config();
		uwsgi_log("\tworkers total requests: %lu\n", uwsgi.workers[0].requests);
		uwsgi_log("-----------------\n");
		for (j = 1; j <= uwsgi.numproc; j++) {
			for (i = 0; i < uwsgi.workers[j].apps_cnt; i++) {
				ua = &uwsgi.workers[j].apps[i];
				if (ua) {
					uwsgi_log("\tworker %d app %d [%.*s] requests: %lu exceptions: %lu\n", j, i, ua->mountpoint_len, ua->mountpoint, ua->requests, ua->exceptions);
				}
			}
			uwsgi_log("-----------------\n");
		}
	}
	else {
		uwsgi_log("worker %d total requests: %lu\n", uwsgi.mywid, uwsgi.workers[0].requests);
		for (i = 0; i < uwsgi.workers[uwsgi.mywid].apps_cnt; i++) {
			ua = &uwsgi.workers[uwsgi.mywid].apps[i];
			if (ua) {
				uwsgi_log("\tapp %d [%.*s] requests: %lu exceptions: %lu\n", i, ua->mountpoint_len, ua->mountpoint, ua->requests, ua->exceptions);
			}
		}
		uwsgi_log("-----------------\n");
	}
	uwsgi_log("\n");
}

void what_i_am_doing() {

	struct wsgi_request *wsgi_req;
	int i;
	char ctime_storage[26];

	uwsgi_backtrace(uwsgi.backtrace_depth);

	if (uwsgi.cores > 1) {
		for (i = 0; i < uwsgi.cores; i++) {
			wsgi_req = &uwsgi.workers[uwsgi.mywid].cores[i].req;
			if (wsgi_req->uri_len > 0) {
#if defined(__sun__) && !defined(__clang__)
				ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, ctime_storage, 26);
#else
				ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, ctime_storage);
#endif
				if (uwsgi.harakiri_options.workers > 0 && uwsgi.workers[uwsgi.mywid].harakiri < uwsgi_now()) {
					uwsgi_log("HARAKIRI: --- uWSGI worker %d core %d (pid: %d) WAS managing request %.*s since %.*s ---\n", (int) uwsgi.mywid, i, (int) uwsgi.mypid, wsgi_req->uri_len, wsgi_req->uri, 24, ctime_storage);
				}
				else {
					uwsgi_log("SIGUSR2: --- uWSGI worker %d core %d (pid: %d) is managing request %.*s since %.*s ---\n", (int) uwsgi.mywid, i, (int) uwsgi.mypid, wsgi_req->uri_len, wsgi_req->uri, 24, ctime_storage);
				}
			}
		}
	}
	else {
		wsgi_req = &uwsgi.workers[uwsgi.mywid].cores[0].req;
		if (wsgi_req->uri_len > 0) {
#if defined(__sun__) && !defined(__clang__)
			ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, ctime_storage, 26);
#else
			ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, ctime_storage);
#endif
			if (uwsgi.harakiri_options.workers > 0 && uwsgi.workers[uwsgi.mywid].harakiri < uwsgi_now()) {
				uwsgi_log("HARAKIRI: --- uWSGI worker %d (pid: %d) WAS managing request %.*s since %.*s ---\n", (int) uwsgi.mywid, (int) uwsgi.mypid, wsgi_req->uri_len, wsgi_req->uri, 24, ctime_storage);
			}
			else {
				uwsgi_log("SIGUSR2: --- uWSGI worker %d (pid: %d) is managing request %.*s since %.*s ---\n", (int) uwsgi.mywid, (int) uwsgi.mypid, wsgi_req->uri_len, wsgi_req->uri, 24, ctime_storage);
			}
		}
		else if (uwsgi.harakiri_options.workers > 0 && uwsgi.workers[uwsgi.mywid].harakiri < uwsgi_now() && uwsgi.workers[uwsgi.mywid].sig) {
			uwsgi_log("HARAKIRI: --- uWSGI worker %d (pid: %d) WAS handling signal %d ---\n", (int) uwsgi.mywid, (int) uwsgi.mypid, uwsgi.workers[uwsgi.mywid].signum);
		}
	}
}



int unconfigured_hook(struct wsgi_request *wsgi_req) {
	if (wsgi_req->uh->modifier1 == 0 && !uwsgi.no_default_app) {
		if (uwsgi_apps_cnt > 0 && uwsgi.default_app > -1) {
			struct uwsgi_app *ua = &uwsgi_apps[uwsgi.default_app];
			if (uwsgi.p[ua->modifier1]->request != unconfigured_hook) {
				wsgi_req->uh->modifier1 = ua->modifier1;
				return uwsgi.p[ua->modifier1]->request(wsgi_req);
			}
		}
	}
	uwsgi_log("-- unavailable modifier requested: %d --\n", wsgi_req->uh->modifier1);
	return -1;
}

static void unconfigured_after_hook(struct wsgi_request *wsgi_req) {
	return;
}

struct uwsgi_plugin unconfigured_plugin = {

	.name = "unconfigured",
	.request = unconfigured_hook,
	.after_request = unconfigured_after_hook,
};

void uwsgi_exec_atexit(void) {
	if (getpid() == masterpid) {
	
		uwsgi_hooks_run(uwsgi.hook_as_user_atexit, "atexit", 0);
		// now run exit scripts needed by the user
		struct uwsgi_string_list *usl;

		uwsgi_foreach(usl, uwsgi.exec_as_user_atexit) {
			uwsgi_log("running \"%s\" (as uid: %d gid: %d) ...\n", usl->value, (int) getuid(), (int) getgid());
			int ret = uwsgi_run_command_and_wait(NULL, usl->value);
			if (ret != 0) {
				uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
			}
		}

		uwsgi_foreach(usl, uwsgi.call_as_user_atexit) {
                	if (uwsgi_call_symbol(usl->value)) {
                        	uwsgi_log("unable to call function \"%s\"\n", usl->value);
                	}
        	}
	}
}

static void vacuum(void) {

	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;

	if (uwsgi.restore_tc) {
		if (getpid() == masterpid) {
			if (tcsetattr(0, TCSANOW, &uwsgi.termios)) {
				uwsgi_error("vacuum()/tcsetattr()");
			}
		}
	}

	if (uwsgi.vacuum) {
		if (getpid() == masterpid) {
			if (chdir(uwsgi.cwd)) {
				uwsgi_error("chdir()");
			}
			if (uwsgi.pidfile && !uwsgi.uid) {
				if (unlink(uwsgi.pidfile)) {
					uwsgi_error("unlink()");
				}
				else {
					uwsgi_log("VACUUM: pidfile removed.\n");
				}
			}
			if (uwsgi.pidfile2) {
				if (unlink(uwsgi.pidfile2)) {
					uwsgi_error("unlink()");
				}
				else {
					uwsgi_log("VACUUM: pidfile2 removed.\n");
				}
			}
			if (uwsgi.safe_pidfile && !uwsgi.uid) {
				if (unlink(uwsgi.safe_pidfile)) {
					uwsgi_error("unlink()");
				}
				else {
					uwsgi_log("VACUUM: safe pidfile removed.\n");
				}
			}
			if (uwsgi.safe_pidfile2) {
				if (unlink(uwsgi.safe_pidfile2)) {
					uwsgi_error("unlink()");
				}
				else {
					uwsgi_log("VACUUM: safe pidfile2 removed.\n");
				}
			}
			if (uwsgi.chdir) {
				if (chdir(uwsgi.chdir)) {
					uwsgi_error("chdir()");
				}
			}
			while (uwsgi_sock) {
				if (uwsgi_sock->family == AF_UNIX && uwsgi_sock->name[0] != '@') {
					struct stat st;
					if (!stat(uwsgi_sock->name, &st)) {
						if (st.st_ino != uwsgi_sock->inode) {
							uwsgi_log("VACUUM WARNING: unix socket %s changed inode. Skip removal\n", uwsgi_sock->name);
							goto next;
						}
					}
					if (unlink(uwsgi_sock->name)) {
						uwsgi_error("unlink()");
					}
					else {
						uwsgi_log("VACUUM: unix socket %s removed.\n", uwsgi_sock->name);
					}
				}
next:
				uwsgi_sock = uwsgi_sock->next;
			}
			if (uwsgi.stats) {
				// is a unix socket ?
				if (!strchr(uwsgi.stats, ':') && uwsgi.stats[0] != '@') {
					if (unlink(uwsgi.stats)) {
                                                uwsgi_error("unlink()");
                                        }
                                        else {
                                                uwsgi_log("VACUUM: unix socket %s (stats) removed.\n", uwsgi.stats);
                                        }
				}
			}
		}
	}
}

int signal_pidfile(int sig, char *filename) {

	int ret = 0;
	size_t size = 0;

	char *buffer = uwsgi_open_and_read(filename, &size, 1, NULL);

	if (size > 0) {
		if (kill((pid_t) atoi(buffer), sig)) {
			uwsgi_error("signal_pidfile()/kill()");
			ret = -1;
		}
	}
	else {
		uwsgi_log("error: invalid pidfile\n");
		ret = -1;
	}
	free(buffer);
	return ret;
}

/*static*/ void uwsgi_command_signal(char *opt) {

	int tmp_signal;
	char *colon = strchr(opt, ',');
	if (!colon) {
		uwsgi_log("invalid syntax for signal, must be addr,signal\n");
		exit(1);
	}

	colon[0] = 0;
	tmp_signal = atoi(colon + 1);

	if (tmp_signal < 0 || tmp_signal > 255) {
		uwsgi_log("invalid signal number\n");
		exit(3);
	}

	uint8_t uwsgi_signal = tmp_signal;
	int ret = uwsgi_remote_signal_send(opt, uwsgi_signal);

	if (ret < 0) {
		uwsgi_log("unable to deliver signal %d to node %s\n", uwsgi_signal, opt);
		exit(1);
	}

	if (ret == 0) {
		uwsgi_log("node %s rejected signal %d\n", opt, uwsgi_signal);
		exit(2);
	}

	uwsgi_log("signal %d delivered to node %s\n", uwsgi_signal, opt);
	exit(0);
}

static void fixup_argv_and_environ(int argc, char **argv, char **environ, char **envp) {

	uwsgi.orig_argv = argv;
	uwsgi.argv = argv;
	uwsgi.argc = argc;
	uwsgi.environ = UWSGI_ENVIRON;

	// avoid messing with fake environ
	if (envp && *environ != *envp) return;
	

#if defined(__linux__) || defined(__sun__)

	int i;
	int env_count = 0;

	uwsgi.argv = uwsgi_malloc(sizeof(char *) * (argc + 1));

	for (i = 0; i < argc; i++) {
		if (i == 0 || argv[0] + uwsgi.max_procname + 1 == argv[i]) {
			uwsgi.max_procname += strlen(argv[i]) + 1;
		}
		uwsgi.argv[i] = strdup(argv[i]);
	}

	// required by execve
	uwsgi.argv[i] = NULL;

	uwsgi.max_procname++;

	for (i = 0; environ[i] != NULL; i++) {
		// useless
		//if ((environ[0] + uwsgi.max_procname + 1) == environ[i]) {
		uwsgi.max_procname += strlen(environ[i]) + 1;
		//}
		env_count++;
	}

	uwsgi.environ = uwsgi_malloc(sizeof(char *) * (env_count+1));
	for (i = 0; i < env_count; i++) {
		uwsgi.environ[i] = strdup(environ[i]);
#ifdef UWSGI_DEBUG
		uwsgi_log("ENVIRON: %s\n", uwsgi.environ[i]);
#endif
		environ[i] = uwsgi.environ[i];
	}
	uwsgi.environ[env_count] = NULL;

#ifdef UWSGI_DEBUG
	uwsgi_log("max space for custom process name = %d\n", uwsgi.max_procname);
#endif
	//environ = uwsgi.environ;

#endif
}


void uwsgi_plugins_atexit(void) {

	int j;

	if (!uwsgi.workers)
		return;

	// the master cannot run atexit handlers...
	if (uwsgi.master_process && uwsgi.workers[0].pid == getpid())
		return;

	for (j = 0; j < uwsgi.gp_cnt; j++) {
		if (uwsgi.gp[j]->atexit) {
			uwsgi.gp[j]->atexit();
		}
	}

	for (j = 0; j < 256; j++) {
		if (uwsgi.p[j]->atexit) {
			uwsgi.p[j]->atexit();
		}
	}

}

void uwsgi_backtrace(int depth) {

#if defined(__GLIBC__) || (defined(__APPLE__) && !defined(NO_EXECINFO)) || defined(UWSGI_HAS_EXECINFO)

#include <execinfo.h>

	void **btrace = uwsgi_malloc(sizeof(void *) * depth);
	size_t bt_size, i;
	char **bt_strings;

	bt_size = backtrace(btrace, depth);

	bt_strings = backtrace_symbols(btrace, bt_size);

	struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
	uwsgi_buffer_append(ub, "*** backtrace of ",17);
	uwsgi_buffer_num64(ub, (int64_t) getpid());
	uwsgi_buffer_append(ub, " ***\n", 5);
	for (i = 0; i < bt_size; i++) {
		uwsgi_buffer_append(ub, bt_strings[i], strlen(bt_strings[i]));
		uwsgi_buffer_append(ub, "\n", 1);
	}

	free(btrace);

	uwsgi_buffer_append(ub, "*** end of backtrace ***\n", 25);

	uwsgi_log("%.*s", ub->pos, ub->buf);

	struct uwsgi_string_list *usl = uwsgi.alarm_segfault;
	while(usl) {
		uwsgi_alarm_trigger(usl->value, ub->buf, ub->pos);	
		usl = usl->next;
	}	

	uwsgi_buffer_destroy(ub);
#endif

}

void uwsgi_segfault(int signum) {

	uwsgi_log("!!! uWSGI process %d got Segmentation Fault !!!\n", (int) getpid());
	uwsgi_backtrace(uwsgi.backtrace_depth);

	if (uwsgi.use_abort) abort();

	// restore default handler to generate core
	signal(signum, SIG_DFL);
	kill(getpid(), signum);

	// never here...
	exit(1);
}

void uwsgi_fpe(int signum) {

	uwsgi_log("!!! uWSGI process %d got Floating Point Exception !!!\n", (int) getpid());
	uwsgi_backtrace(uwsgi.backtrace_depth);

	if (uwsgi.use_abort) abort();

	// restore default handler to generate core
	signal(signum, SIG_DFL);
	kill(getpid(), signum);

	// never here...
	exit(1);
}

void uwsgi_flush_logs() {

	struct pollfd pfd;

	if (!uwsgi.master_process)
		return;
	if (!uwsgi.log_master)
		return;

	if (uwsgi.workers) {
		if (uwsgi.workers[0].pid == getpid()) {
			goto check;
		}
	}


	if (uwsgi.mywid == 0)
		goto check;

	return;

check:
	// this buffer could not be initialized !!!
	if (uwsgi.log_master) {
		uwsgi.log_master_buf = uwsgi_malloc(uwsgi.log_master_bufsize);
	}

	// check for data in logpipe
	pfd.events = POLLIN;
	pfd.fd = uwsgi.shared->worker_log_pipe[0];
	if (pfd.fd == -1)
		pfd.fd = 2;

	while (poll(&pfd, 1, 0) > 0) {
		if (uwsgi_master_log()) {
			break;
		}
	}
}

static void plugins_list(void) {
	int i;
	uwsgi_log("\n*** uWSGI loaded generic plugins ***\n");
	for (i = 0; i < uwsgi.gp_cnt; i++) {
		uwsgi_log("%s\n", uwsgi.gp[i]->name);
	}

	uwsgi_log("\n*** uWSGI loaded request plugins ***\n");
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i] == &unconfigured_plugin)
			continue;
		uwsgi_log("%d: %s\n", i, uwsgi.p[i]->name);
	}

	uwsgi_log("--- end of plugins list ---\n\n");
}

static void loggers_list(void) {
	struct uwsgi_logger *ul = uwsgi.loggers;
	uwsgi_log("\n*** uWSGI loaded loggers ***\n");
	while (ul) {
		uwsgi_log("%s\n", ul->name);
		ul = ul->next;
	}
	uwsgi_log("--- end of loggers list ---\n\n");
}

static void cheaper_algo_list(void) {
	struct uwsgi_cheaper_algo *uca = uwsgi.cheaper_algos;
	uwsgi_log("\n*** uWSGI loaded cheaper algorithms ***\n");
	while (uca) {
		uwsgi_log("%s\n", uca->name);
		uca = uca->next;
	}
	uwsgi_log("--- end of cheaper algorithms list ---\n\n");
}

#ifdef UWSGI_ROUTING
static void router_list(void) {
	struct uwsgi_router *ur = uwsgi.routers;
	uwsgi_log("\n*** uWSGI loaded routers ***\n");
	while (ur) {
		uwsgi_log("%s\n", ur->name);
		ur = ur->next;
	}
	uwsgi_log("--- end of routers list ---\n\n");
}
#endif

static void loop_list(void) {
	struct uwsgi_loop *loop = uwsgi.loops;
	uwsgi_log("\n*** uWSGI loaded loop engines ***\n");
	while (loop) {
		uwsgi_log("%s\n", loop->name);
		loop = loop->next;
	}
	uwsgi_log("--- end of loop engines list ---\n\n");
}

static void imperial_monitor_list(void) {
	struct uwsgi_imperial_monitor *uim = uwsgi.emperor_monitors;
	uwsgi_log("\n*** uWSGI loaded imperial monitors ***\n");
	while (uim) {
		uwsgi_log("%s\n", uim->scheme);
		uim = uim->next;
	}
	uwsgi_log("--- end of imperial monitors list ---\n\n");
}

static void clocks_list(void) {
	struct uwsgi_clock *clocks = uwsgi.clocks;
	uwsgi_log("\n*** uWSGI loaded clocks ***\n");
	while (clocks) {
		uwsgi_log("%s\n", clocks->name);
		clocks = clocks->next;
	}
	uwsgi_log("--- end of clocks list ---\n\n");
}

static void alarms_list(void) {
	struct uwsgi_alarm *alarms = uwsgi.alarms;
	uwsgi_log("\n*** uWSGI loaded alarms ***\n");
	while (alarms) {
		uwsgi_log("%s\n", alarms->name);
		alarms = alarms->next;
	}
	uwsgi_log("--- end of alarms list ---\n\n");
}

static time_t uwsgi_unix_seconds() {
	return time(NULL);
}

static uint64_t uwsgi_unix_microseconds() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return ((uint64_t) tv.tv_sec * 1000000) + tv.tv_usec;
}

static struct uwsgi_clock uwsgi_unix_clock = {
	.name = "unix",
	.seconds = uwsgi_unix_seconds,
	.microseconds = uwsgi_unix_microseconds,
};

void uwsgi_init_random() {
        srand((unsigned int) (uwsgi.start_tv.tv_usec * uwsgi.start_tv.tv_sec));
}

#ifdef UWSGI_AS_SHARED_LIBRARY
int uwsgi_init(int argc, char *argv[], char *envp[]) {
#else
int main(int argc, char *argv[], char *envp[]) {
#endif
	uwsgi_setup(argc, argv, envp);
	return uwsgi_run();
}

static char *uwsgi_at_file_read(char *filename) {
	size_t size = 0;
	char *buffer = uwsgi_open_and_read(filename, &size, 1, NULL);
	if (size > 1) {
		if (buffer[size-2] == '\n' || buffer[size-2] == '\r') {
			buffer[size-2] = 0;
		}
	}
	return buffer;
}

void uwsgi_setup(int argc, char *argv[], char *envp[]) {
	int i;

	struct utsname uuts;

	// signal mask is inherited, and sme process manager could make a real mess...
	sigset_t smask;
        sigfillset(&smask);
        if (sigprocmask(SIG_UNBLOCK, &smask, NULL)) {
                uwsgi_error("sigprocmask()");
        }

	signal(SIGCHLD, SIG_DFL);
	signal(SIGSEGV, uwsgi_segfault);
	signal(SIGFPE, uwsgi_fpe);
	signal(SIGHUP, SIG_IGN);
	signal(SIGTERM, SIG_IGN);
	signal(SIGPIPE, SIG_IGN);

	//initialize masterpid with a default value
	masterpid = getpid();

	memset(&uwsgi, 0, sizeof(struct uwsgi_server));
	uwsgi_proto_hooks_setup();
	uwsgi.cwd = uwsgi_get_cwd();

	init_magic_table(uwsgi.magic_table);

	// initialize schemes
	uwsgi_setup_schemes();

	// initialize the clock
	uwsgi_register_clock(&uwsgi_unix_clock);
	uwsgi_set_clock("unix");

	// fallback config
	atexit(uwsgi_fallback_config);
	// manage/flush logs
	atexit(uwsgi_flush_logs);
	// clear sockets, pidfiles...
	atexit(vacuum);
	// call user scripts
	atexit(uwsgi_exec_atexit);
#ifdef UWSGI_SSL
	// call legions death hooks
	atexit(uwsgi_legion_atexit);
#endif

	// allocate main shared memory
	uwsgi.shared = (struct uwsgi_shared *) uwsgi_calloc_shared(sizeof(struct uwsgi_shared));

	// initialize request plugin to void
	for (i = 0; i < 256; i++) {
		uwsgi.p[i] = &unconfigured_plugin;
	}

	// set default values
	uwsgi_init_default();

	// detect cpu cores
#if defined(_SC_NPROCESSORS_ONLN)
	uwsgi.cpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(_SC_NPROCESSORS_CONF)
	uwsgi.cpus = sysconf(_SC_NPROCESSORS_CONF);
#endif
	// set default logit hook
	uwsgi.logit = uwsgi_logit_simple;

#ifdef UWSGI_BLACKLIST
	if (!uwsgi_file_to_string_list(UWSGI_BLACKLIST, &uwsgi.blacklist)) {
		uwsgi_log("you cannot run this build of uWSGI without a blacklist file\n");
		exit(1);
	}
#endif

#ifdef UWSGI_WHITELIST
	if (!uwsgi_file_to_string_list(UWSGI_WHITELIST, &uwsgi.whitelist)) {
		uwsgi_log("you cannot run this build of uWSGI without a whitelist file\n");
		exit(1);
	}
#endif

	// get startup time
	gettimeofday(&uwsgi.start_tv, NULL);

	// initialize random engine
	uwsgi_init_random();

	setlinebuf(stdout);

	uwsgi.rl.rlim_cur = 0;
	uwsgi.rl.rlim_max = 0;

	// are we under systemd ?
	char *notify_socket = getenv("NOTIFY_SOCKET");
	if (notify_socket) {
		uwsgi_systemd_init(notify_socket);
	}

	uwsgi_notify("initializing uWSGI");

	// check if we are under the Emperor
	uwsgi_check_emperor();

	char *screen_env = getenv("TERM");
	if (screen_env) {
		if (!strcmp(screen_env, "screen")) {
			uwsgi.screen_session = getenv("STY");
		}
	}


	// count/set the current reload status
	uwsgi_setup_reload();

#ifdef __CYGWIN__
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	uwsgi.page_size = si.dwPageSize;
#else
	uwsgi.page_size = getpagesize();
#endif
	uwsgi.binary_path = uwsgi_get_binary_path(argv[0]);

	if(uwsgi.response_header_limit == 0)
		uwsgi.response_header_limit = UMAX16;

	// ok we can now safely play with argv and environ
	fixup_argv_and_environ(argc, argv, UWSGI_ENVIRON, envp);

	if (gethostname(uwsgi.hostname, 255)) {
		uwsgi_error("gethostname()");
	}
	uwsgi.hostname_len = strlen(uwsgi.hostname);

#ifdef UWSGI_ROUTING
	uwsgi_register_embedded_routers();
#endif

	// call here to allows plugin to override hooks
	uwsgi_register_base_hooks();
	uwsgi_register_logchunks();
	uwsgi_log_encoders_register_embedded();

	// register base metrics (so plugins can override them)
	uwsgi_metrics_collectors_setup();

	//initialize embedded plugins
	UWSGI_LOAD_EMBEDDED_PLUGINS
		// now a bit of magic, if the executable basename contains a 'uwsgi_' string,
		// try to automatically load a plugin
#ifdef UWSGI_DEBUG
		uwsgi_log("executable name: %s\n", uwsgi.binary_path);
#endif
	uwsgi_autoload_plugins_by_name(argv[0]);


	// build the options structure
	build_options();

	// set a couple of 'static' magic vars
	uwsgi.magic_table['v'] = uwsgi.cwd;
	uwsgi.magic_table['h'] = uwsgi.hostname;
	uwsgi.magic_table['t'] = uwsgi_64bit2str(uwsgi_now());
	uwsgi.magic_table['T'] = uwsgi_64bit2str(uwsgi_micros());
	uwsgi.magic_table['V'] = UWSGI_VERSION;
	uwsgi.magic_table['k'] = uwsgi_num2str(uwsgi.cpus);
	uwsgi.magic_table['['] = "\033";
	uwsgi.magic_table['u'] = uwsgi_num2str((int)getuid());
	struct passwd *pw = getpwuid(getuid());
	uwsgi.magic_table['U'] = pw ? pw->pw_name : uwsgi.magic_table['u'];
	uwsgi.magic_table['g'] = uwsgi_num2str((int)getgid());
	struct group *gr = getgrgid(getgid());
	uwsgi.magic_table['G'] = gr ? gr->gr_name : uwsgi.magic_table['g'];

	// you can embed a ini file in the uWSGi binary with default options
#ifdef UWSGI_EMBED_CONFIG
	uwsgi_ini_config("", uwsgi.magic_table);
	// rebuild options if a custom ini is set
	build_options();
#endif
	//parse environ
	parse_sys_envs(UWSGI_ENVIRON);

	// parse commandline options
	uwsgi_commandline_config();

	// second pass: ENVs
	uwsgi_apply_config_pass('$', (char *(*)(char *)) getenv);

	// third pass: FILEs
	uwsgi_apply_config_pass('@', uwsgi_at_file_read);

	// last pass: REFERENCEs
	uwsgi_apply_config_pass('%', uwsgi_manage_placeholder);

	// ok, the options dictionary is available, lets manage it
	uwsgi_configure();

	// fixup cwd
	if (uwsgi.force_cwd) uwsgi.cwd = uwsgi.force_cwd;

	// run "asap" hooks
	uwsgi_hooks_run(uwsgi.hook_asap, "asap", 1);
        struct uwsgi_string_list *usl = NULL;
        uwsgi_foreach(usl, uwsgi.mount_asap) {
        	uwsgi_log("mounting \"%s\" (asap)...\n", usl->value);
                if (uwsgi_mount_hook(usl->value)) exit(1);
	}
        uwsgi_foreach(usl, uwsgi.umount_asap) {
        	uwsgi_log("un-mounting \"%s\" (asap)...\n", usl->value);
                if (uwsgi_umount_hook(usl->value)) exit(1);
	}
        uwsgi_foreach(usl, uwsgi.exec_asap) {
        	uwsgi_log("running \"%s\" (asap)...\n", usl->value);
                int ret = uwsgi_run_command_and_wait(NULL, usl->value);
                if (ret != 0) {
                	uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
                        exit(1);
                }
	}
        uwsgi_foreach(usl, uwsgi.call_asap) {
        	if (uwsgi_call_symbol(usl->value)) {
                	uwsgi_log("unable to call function \"%s\"\n", usl->value);
                        exit(1);
                }
	}

	// manage envdirs ASAP
	uwsgi_envdirs(uwsgi.envdirs);

	// --get management
	struct uwsgi_string_list *get_list = uwsgi.get_list;
	while(get_list) {
		char *v = uwsgi_get_exported_opt(get_list->value);
		if (v) {
			fprintf(stdout, "%s\n", v);
		}
		get_list = get_list->next;
	}

	if (uwsgi.get_list) {
		exit(0);
	}


	// initial log setup (files and daemonization)
	uwsgi_setup_log();

#ifndef __CYGWIN__
	// enable never-swap mode
	if (uwsgi.never_swap) {
		if (mlockall(MCL_CURRENT | MCL_FUTURE)) {
			uwsgi_error("mlockall()");
		}
	}
#endif

	if (uwsgi.flock2)
		uwsgi_opt_flock(NULL, uwsgi.flock2, NULL);

	if (uwsgi.flock_wait2)
		uwsgi_opt_flock(NULL, uwsgi.flock_wait2, NULL);

	// setup master logging
	if (uwsgi.log_master && !uwsgi.daemonize2)
		uwsgi_setup_log_master();

	// setup offload engines
	uwsgi_offload_engines_register_all();

	// setup main loops
	uwsgi_register_loop("simple", simple_loop);
	uwsgi_register_loop("async", async_loop);

	// setup cheaper algos
	uwsgi_register_cheaper_algo("spare", uwsgi_cheaper_algo_spare);
	uwsgi_register_cheaper_algo("backlog", uwsgi_cheaper_algo_backlog);
	uwsgi_register_cheaper_algo("manual", uwsgi_cheaper_algo_manual);

	// setup imperial monitors
	uwsgi_register_imperial_monitor("dir", uwsgi_imperial_monitor_directory_init, uwsgi_imperial_monitor_directory);
	uwsgi_register_imperial_monitor("glob", uwsgi_imperial_monitor_glob_init, uwsgi_imperial_monitor_glob);

	// setup stats pushers
	uwsgi_stats_pusher_setup();

	// register embedded alarms
	uwsgi_register_embedded_alarms();

	/* uWSGI IS CONFIGURED !!! */

	if (uwsgi.dump_options) {
		struct option *lopt = uwsgi.long_options;
		while (lopt && lopt->name) {
			fprintf(stdout, "%s\n", lopt->name);
			lopt++;
		}
		exit(0);
	}

	if (uwsgi.show_config)
		show_config();

	if (uwsgi.plugins_list)
		plugins_list();

	if (uwsgi.loggers_list)
		loggers_list();

	if (uwsgi.cheaper_algo_list)
		cheaper_algo_list();


#ifdef UWSGI_ROUTING
	if (uwsgi.router_list)
		router_list();
#endif


	if (uwsgi.loop_list)
		loop_list();

	if (uwsgi.imperial_monitor_list)
		imperial_monitor_list();

	if (uwsgi.clock_list)
		clocks_list();

	if (uwsgi.alarms_list)
		alarms_list();

	// set the clock
	if (uwsgi.requested_clock)
		uwsgi_set_clock(uwsgi.requested_clock);

	if (uwsgi.binary_path == uwsgi.argv[0]) {
		uwsgi.binary_path = uwsgi_str(uwsgi.argv[0]);
	}

	uwsgi_log_initial("*** Starting uWSGI %s (%dbit) on [%.*s] ***\n", UWSGI_VERSION, (int) (sizeof(void *)) * 8, 24, ctime((const time_t *) &uwsgi.start_tv.tv_sec));

#ifdef UWSGI_DEBUG
	uwsgi_log("***\n*** You are running a DEBUG version of uWSGI, please disable debug in your build profile and recompile it ***\n***\n");
#endif

	uwsgi_log_initial("compiled with version: %s on %s\n", __VERSION__, UWSGI_BUILD_DATE);

#ifdef __sun__
	if (uname(&uuts) < 0) {
#else
	if (uname(&uuts)) {
#endif
		uwsgi_error("uname()");
	}
	else {
		uwsgi_log_initial("os: %s-%s %s\n", uuts.sysname, uuts.release, uuts.version);
		uwsgi_log_initial("nodename: %s\n", uuts.nodename);
		uwsgi_log_initial("machine: %s\n", uuts.machine);
	}

	uwsgi_log_initial("clock source: %s\n", uwsgi.clock->name);
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	if (uwsgi.pcre_jit) {
		uwsgi_log_initial("pcre jit enabled\n");
	}
	else {
		uwsgi_log_initial("pcre jit disabled\n");
	}
#endif

#ifdef __BIG_ENDIAN__
	uwsgi_log_initial("*** big endian arch detected ***\n");
#endif

	uwsgi_log_initial("detected number of CPU cores: %d\n", uwsgi.cpus);


	uwsgi_log_initial("current working directory: %s\n", uwsgi.cwd);

	if (uwsgi.screen_session) {
		uwsgi_log("*** running under screen session %s ***\n", uwsgi.screen_session);
	}

	if (uwsgi.pidfile && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile(uwsgi.pidfile);
	}

	uwsgi_log_initial("detected binary path: %s\n", uwsgi.binary_path);

	if (uwsgi.is_a_reload) {
		struct rlimit rl;
		if (!getrlimit(RLIMIT_NOFILE, &rl)) {
			uwsgi.max_fd = rl.rlim_cur;
		}
	}

#ifdef UWSGI_ROUTING
	uwsgi_routing_dump();
#else
	uwsgi_log("!!! no internal routing support, rebuild with pcre support !!!\n");
#endif

	// initialize shared sockets
	uwsgi_setup_shared_sockets();

#ifdef __linux__
	if (uwsgi.setns_preopen) {
		uwsgi_setns_preopen();
	}
	// eventually join a linux namespace
	if (uwsgi.setns) {
		uwsgi_setns(uwsgi.setns);
	}
#endif

	// start the Emperor if needed
	if (uwsgi.early_emperor && uwsgi.emperor) {
		uwsgi_emperor_start();
	}

	if (!uwsgi.reloads) {
		uwsgi_hooks_run(uwsgi.hook_pre_jail, "pre-jail", 1);
		struct uwsgi_string_list *usl = NULL;
		uwsgi_foreach(usl, uwsgi.mount_pre_jail) {
                                uwsgi_log("mounting \"%s\" (pre-jail)...\n", usl->value);
                                if (uwsgi_mount_hook(usl->value)) {
                                        exit(1);
                                }
                        }

                        uwsgi_foreach(usl, uwsgi.umount_pre_jail) {
                                uwsgi_log("un-mounting \"%s\" (pre-jail)...\n", usl->value);
                                if (uwsgi_umount_hook(usl->value)) {
                                        exit(1);
                                }
                        }
		// run the pre-jail scripts
		uwsgi_foreach(usl, uwsgi.exec_pre_jail) {
			uwsgi_log("running \"%s\" (pre-jail)...\n", usl->value);
			int ret = uwsgi_run_command_and_wait(NULL, usl->value);
			if (ret != 0) {
				uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
				exit(1);
			}
		}

		uwsgi_foreach(usl, uwsgi.call_pre_jail) {
			if (uwsgi_call_symbol(usl->value)) {
				uwsgi_log("unable to call function \"%s\"\n", usl->value);
				exit(1);
			}
		}
	}

	// we could now patch the binary
	if (uwsgi.privileged_binary_patch) {
		uwsgi.argv[0] = uwsgi.privileged_binary_patch;
		execvp(uwsgi.privileged_binary_patch, uwsgi.argv);
		uwsgi_error("execvp()");
		exit(1);
	}

	if (uwsgi.privileged_binary_patch_arg) {
		uwsgi_exec_command_with_args(uwsgi.privileged_binary_patch_arg);
	}


	// call jail systems
	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->jail) {
			uwsgi.gp[i]->jail(uwsgi_start, uwsgi.argv);
		}
	}

	// TODO pluginize basic Linux namespace support
#if defined(__linux__) && !defined(__ia64__)
	if (uwsgi.ns) {
		linux_namespace_start((void *) uwsgi.argv);
		// never here
	}
	else {
#endif
		uwsgi_start((void *) uwsgi.argv);
#if defined(__linux__) && !defined(__ia64__)
	}
#endif

	if (uwsgi.safe_pidfile && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile_explicit(uwsgi.safe_pidfile, masterpid);
	}
}


int uwsgi_start(void *v_argv) {

	int i, j;

#ifdef __linux__
	uwsgi_set_cgroup();

#if !defined(__ia64__)
	if (uwsgi.ns) {
		linux_namespace_jail();
	}
#endif
#endif

	uwsgi_hooks_run(uwsgi.hook_in_jail, "in-jail", 1);

	struct uwsgi_string_list *usl;

	uwsgi_foreach(usl, uwsgi.mount_in_jail) {
                                uwsgi_log("mounting \"%s\" (in-jail)...\n", usl->value);
                                if (uwsgi_mount_hook(usl->value)) {
                                        exit(1);
                                }
                        }

                        uwsgi_foreach(usl, uwsgi.umount_in_jail) {
                                uwsgi_log("un-mounting \"%s\" (in-jail)...\n", usl->value);
                                if (uwsgi_umount_hook(usl->value)) {
                                        exit(1);
                                }
                        }

	uwsgi_foreach(usl, uwsgi.exec_in_jail) {
                uwsgi_log("running \"%s\" (in-jail)...\n", usl->value);
                int ret = uwsgi_run_command_and_wait(NULL, usl->value);
                if (ret != 0) {
                        uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
                        exit(1);
                }
        }

        uwsgi_foreach(usl, uwsgi.call_in_jail) {
                if (uwsgi_call_symbol(usl->value)) {
                        uwsgi_log("unable to call function \"%s\"\n", usl->value);
			exit(1);
                }
        }


	uwsgi_file_write_do(uwsgi.file_write_list);

	if (!uwsgi.master_as_root && !uwsgi.chown_socket && !uwsgi.drop_after_init && !uwsgi.drop_after_apps) {
		uwsgi_as_root();
	}

	// wait for socket
	uwsgi_foreach(usl, uwsgi.wait_for_socket) {
		if (uwsgi_wait_for_socket(usl->value)) exit(1);
	}

	if (uwsgi.logto2) {
		if (!uwsgi.is_a_reload || uwsgi.log_reopen) {
			logto(uwsgi.logto2);
		}
	}

	if (uwsgi.chdir) {
		uwsgi_log("chdir() to %s\n", uwsgi.chdir);
		if (chdir(uwsgi.chdir)) {
			uwsgi_error("chdir()");
			exit(1);
		}
	}

	if (uwsgi.pidfile2 && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile(uwsgi.pidfile2);
	}

	if (!uwsgi.master_process && !uwsgi.command_mode) {
		uwsgi_log_initial("*** WARNING: you are running uWSGI without its master process manager ***\n");
	}

#ifdef RLIMIT_NPROC
	if (uwsgi.rl_nproc.rlim_max > 0) {
		uwsgi.rl_nproc.rlim_cur = uwsgi.rl_nproc.rlim_max;
		uwsgi_log_initial("limiting number of processes to %d...\n", (int) uwsgi.rl_nproc.rlim_max);
		if (setrlimit(RLIMIT_NPROC, &uwsgi.rl_nproc)) {
			uwsgi_error("setrlimit()");
		}
	}

	if (!getrlimit(RLIMIT_NPROC, &uwsgi.rl_nproc)) {
		if (uwsgi.rl_nproc.rlim_cur != RLIM_INFINITY) {
			uwsgi_log_initial("your processes number limit is %d\n", (int) uwsgi.rl_nproc.rlim_cur);
			if ((int) uwsgi.rl_nproc.rlim_cur < uwsgi.numproc + uwsgi.master_process) {
				uwsgi.numproc = uwsgi.rl_nproc.rlim_cur - 1;
				uwsgi_log_initial("!!! number of workers adjusted to %d due to system limits !!!\n", uwsgi.numproc);
			}
		}
	}
#endif
#ifndef __OpenBSD__

	if (uwsgi.rl.rlim_max > 0) {
		uwsgi.rl.rlim_cur = uwsgi.rl.rlim_max;
		uwsgi_log_initial("limiting address space of processes...\n");
		if (setrlimit(RLIMIT_AS, &uwsgi.rl)) {
			uwsgi_error("setrlimit()");
		}
	}
	if (uwsgi.prio != 0) {
#ifdef __HAIKU__
		if (set_thread_priority(find_thread(NULL), uwsgi.prio) == B_BAD_THREAD_ID) {
			uwsgi_error("set_thread_priority()");
#else
		if (setpriority(PRIO_PROCESS, 0, uwsgi.prio)) {
			uwsgi_error("setpriority()");
#endif

		}
		else {
			uwsgi_log_initial("scheduler priority set to %d\n", uwsgi.prio);
		}
	}
	if (!getrlimit(RLIMIT_AS, &uwsgi.rl)) {
		//check for overflow
		if (uwsgi.rl.rlim_max != (rlim_t) RLIM_INFINITY) {
			uwsgi_log_initial("your process address space limit is %lld bytes (%lld MB)\n", (long long) uwsgi.rl.rlim_max, (long long) uwsgi.rl.rlim_max / 1024 / 1024);
		}
	}
#endif

	uwsgi_log_initial("your memory page size is %d bytes\n", uwsgi.page_size);

	// automatically fix options
	sanitize_args();


	if (uwsgi.requested_max_fd) {
		uwsgi.rl.rlim_cur = uwsgi.requested_max_fd;
		uwsgi.rl.rlim_max = uwsgi.requested_max_fd;
		if (setrlimit(RLIMIT_NOFILE, &uwsgi.rl)) {
			uwsgi_error("setrlimit()");
		}
	}

	if (!getrlimit(RLIMIT_NOFILE, &uwsgi.rl)) {
		uwsgi.max_fd = uwsgi.rl.rlim_cur;
		uwsgi_log_initial("detected max file descriptor number: %lu\n", (unsigned long) uwsgi.max_fd);
	}

	// start the Emperor if needed
	if (!uwsgi.early_emperor && uwsgi.emperor) {
		uwsgi_emperor_start();
	}

	// end of generic initialization


	// build mime.types dictionary
	if (uwsgi.build_mime_dict) {
		if (!uwsgi.mime_file)
#ifdef __APPLE__
			uwsgi_string_new_list(&uwsgi.mime_file, "/etc/apache2/mime.types");
#else
			uwsgi_string_new_list(&uwsgi.mime_file, "/etc/mime.types");
#endif
		struct uwsgi_string_list *umd = uwsgi.mime_file;
		while (umd) {
			if (!access(umd->value, R_OK)) {
				uwsgi_build_mime_dict(umd->value);
			}
			else {
				uwsgi_log("!!! no %s file found !!!\n", umd->value);
			}
			umd = umd->next;
		}
	}

	if (uwsgi.async > 1) {
		if ((unsigned long) uwsgi.max_fd < (unsigned long) uwsgi.async) {
			uwsgi_log_initial("- your current max open files limit is %lu, this is lower than requested async cores !!! -\n", (unsigned long) uwsgi.max_fd);
			uwsgi.rl.rlim_cur = uwsgi.async;
			uwsgi.rl.rlim_max = uwsgi.async;
			if (!setrlimit(RLIMIT_NOFILE, &uwsgi.rl)) {
				uwsgi_log("max open files limit raised to %lu\n", (unsigned long) uwsgi.rl.rlim_cur);
				uwsgi.async = uwsgi.rl.rlim_cur;
				uwsgi.max_fd = uwsgi.rl.rlim_cur;
			}
			else {
				uwsgi.async = (int) uwsgi.max_fd;
			}
		}
		uwsgi_log_initial("- async cores set to %d - fd table size: %d\n", uwsgi.async, (int) uwsgi.max_fd);
	}

#ifdef UWSGI_DEBUG
	uwsgi_log("cores allocated...\n");
#endif

	if (uwsgi.vhost) {
		uwsgi_log_initial("VirtualHosting mode enabled.\n");
	}

	// setup locking
	uwsgi_setup_locking();
	if (uwsgi.use_thunder_lock) {
		uwsgi_log_initial("thunder lock: enabled\n");
	}
	else {
		uwsgi_log_initial("thunder lock: disabled (you can enable it with --thunder-lock)\n");
	}

	// allocate rpc structures
        uwsgi_rpc_init();

	// initialize sharedareas
	uwsgi_sharedareas_init();

	uwsgi.snmp_lock = uwsgi_lock_init("snmp");

	// setup queue
	if (uwsgi.queue_size > 0) {
		uwsgi_init_queue();
	}

	uwsgi_cache_create_all();

	if (uwsgi.use_check_cache) {
		uwsgi.check_cache = uwsgi_cache_by_name(uwsgi.use_check_cache);
		if (!uwsgi.check_cache) {
			uwsgi_log("unable to find cache \"%s\"\n", uwsgi.use_check_cache);
			exit(1);
		}
	}

	if (uwsgi.use_static_cache_paths) {
		if (uwsgi.static_cache_paths_name) {
			uwsgi.static_cache_paths = uwsgi_cache_by_name(uwsgi.static_cache_paths_name);
			if (!uwsgi.static_cache_paths) {
				uwsgi_log("unable to find cache \"%s\"\n", uwsgi.static_cache_paths_name);
				exit(1);
			}
		}
		else {
			if (!uwsgi.caches) {
                		uwsgi_log("caching of static paths requires uWSGI caching !!!\n");
                		exit(1);
			}
			uwsgi.static_cache_paths = uwsgi.caches;
		}
        }

        // initialize the alarm subsystem
        uwsgi_alarms_init();

	// initialize the exception handlers
	uwsgi_exception_setup_handlers();

	// initialize socket protocols (do it after caching !!!)
	uwsgi_protocols_register();

	/* plugin initialization */
	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->init) {
			uwsgi.gp[i]->init();
		}
	}

	if (!uwsgi.no_server) {

		// systemd/upstart/zerg socket activation
		if (!uwsgi.is_a_reload) {
			uwsgi_setup_systemd();
			uwsgi_setup_upstart();
			uwsgi_setup_zerg();
			uwsgi_setup_emperor();
		}


		//check for inherited sockets
		if (uwsgi.is_a_reload) {
			uwsgi_setup_inherited_sockets();
		}


		//now bind all the unbound sockets
		uwsgi_bind_sockets();

		if (!uwsgi.master_as_root && !uwsgi.drop_after_init && !uwsgi.drop_after_apps) {
			uwsgi_as_root();
		}

		// put listening socket in non-blocking state and set the protocol
		uwsgi_set_sockets_protocols();

	}


	// initialize request plugin only if workers or master are available
	if (uwsgi.sockets || uwsgi.master_process || uwsgi.no_server || uwsgi.command_mode || uwsgi.loop) {
		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->init) {
				uwsgi.p[i]->init();
			}
		}
	}

	if (!uwsgi.master_as_root && !uwsgi.drop_after_apps) {
		uwsgi_as_root();
	}


	/* gp/plugin initialization */
	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->post_init) {
			uwsgi.gp[i]->post_init();
		}
	}

	// again check for workers/sockets...
	if (uwsgi.sockets || uwsgi.master_process || uwsgi.no_server || uwsgi.command_mode || uwsgi.loop) {
		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->post_init) {
				uwsgi.p[i]->post_init();
			}
		}
	}

	uwsgi.current_wsgi_req = simple_current_wsgi_req;


	if (uwsgi.has_threads) {
		if (uwsgi.threads > 1)
			uwsgi.current_wsgi_req = threaded_current_wsgi_req;
		(void) pthread_attr_init(&uwsgi.threads_attr);
		if (uwsgi.threads_stacksize) {
			if (pthread_attr_setstacksize(&uwsgi.threads_attr, uwsgi.threads_stacksize * 1024) == 0) {
				uwsgi_log("threads stack size set to %luk\n", (unsigned long) uwsgi.threads_stacksize);
			}
			else {
				uwsgi_log("!!! unable to set requested threads stacksize !!!\n");
			}
		}

		pthread_mutex_init(&uwsgi.lock_static, NULL);

		// again check for workers/sockets...
		if (uwsgi.sockets || uwsgi.master_process || uwsgi.no_server || uwsgi.command_mode || uwsgi.loop) {
			for (i = 0; i < 256; i++) {
				if (uwsgi.p[i]->enable_threads)
					uwsgi.p[i]->enable_threads();
			}
		}
	}

	// users of the --loop option should know what they are doing... really...
#ifndef UWSGI_DEBUG
	if (uwsgi.loop)
		goto unsafe;
#endif

	if (!uwsgi.sockets &&
		!ushared->gateways_cnt &&
		!uwsgi.no_server &&
		!uwsgi.udp_socket &&
		!uwsgi.emperor &&
		!uwsgi.command_mode &&
		!uwsgi.daemons_cnt &&
		!uwsgi.crons &&
		!uwsgi.spoolers &&
		!uwsgi.emperor_proxy
#ifdef __linux__
		&& !uwsgi.setns_socket
#endif
#ifdef UWSGI_SSL
&& !uwsgi.legions
#endif
		) {
		uwsgi_log("The -s/--socket option is missing and stdin is not a socket.\n");
		exit(1);
	}
	else if (!uwsgi.sockets && ushared->gateways_cnt && !uwsgi.no_server && !uwsgi.master_process) {
		// here we will have a zombie... sorry
		uwsgi_log("...you should enable the master process... really...\n");
		if (uwsgi.force_gateway) {
			struct uwsgi_gateway *ug = &ushared->gateways[0];
			ug->loop(0, ug->data);
			// when we are here the gateway is dead :(
		}
		exit(0);
	}

	if (!uwsgi.sockets)
		uwsgi.numproc = 0;

	if (uwsgi.command_mode) {
		uwsgi.sockets = NULL;
		uwsgi.numproc = 1;
		// hack to destroy the instance after command exit
		uwsgi.status.brutally_destroying = 1;
	}

#ifndef UWSGI_DEBUG
unsafe:
#endif

#ifdef UWSGI_DEBUG
	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	int so_bufsize;
	socklen_t so_bufsize_len;
	while (uwsgi_sock) {
		so_bufsize_len = sizeof(int);
		if (getsockopt(uwsgi_sock->fd, SOL_SOCKET, SO_RCVBUF, &so_bufsize, &so_bufsize_len)) {
			uwsgi_error("getsockopt()");
		}
		else {
			uwsgi_debug("uwsgi socket %d SO_RCVBUF size: %d\n", i, so_bufsize);
		}

		so_bufsize_len = sizeof(int);
		if (getsockopt(uwsgi_sock->fd, SOL_SOCKET, SO_SNDBUF, &so_bufsize, &so_bufsize_len)) {
			uwsgi_error("getsockopt()");
		}
		else {
			uwsgi_debug("uwsgi socket %d SO_SNDBUF size: %d\n", i, so_bufsize);
		}
		uwsgi_sock = uwsgi_sock->next;
	}
#endif


#ifndef UNBIT
	if (uwsgi.sockets)
		uwsgi_log("your server socket listen backlog is limited to %d connections\n", uwsgi.listen_queue);
#endif

	uwsgi_log("your mercy for graceful operations on workers is %d seconds\n", uwsgi.worker_reload_mercy);

	if (uwsgi.crons) {
		struct uwsgi_cron *ucron = uwsgi.crons;
		while (ucron) {
#ifdef UWSGI_SSL
			if (ucron->legion) {
				uwsgi_log("[uwsgi-cron] command \"%s\" registered as cron task for legion \"%s\"\n", ucron->command, ucron->legion);
				ucron = ucron->next;
				continue;
			}
#endif
			uwsgi_log("[uwsgi-cron] command \"%s\" registered as cron task\n", ucron->command);
			ucron = ucron->next;
		}
	}


	// initialize post buffering values
	if (uwsgi.post_buffering > 0)
		uwsgi_setup_post_buffering();

	// initialize workers/master shared memory segments
	uwsgi_setup_workers();

	// create signal pipes if master is enabled
	if (uwsgi.master_process) {
		for (i = 1; i <= uwsgi.numproc; i++) {
			create_signal_pipe(uwsgi.workers[i].signal_pipe);
		}
	}

	// set masterpid
	uwsgi.mypid = getpid();
	masterpid = uwsgi.mypid;
	uwsgi.workers[0].pid = masterpid;

	// initialize mules and farms
	uwsgi_setup_mules_and_farms();

	if (uwsgi.command_mode) {
		uwsgi_log("*** Operational MODE: command ***\n");
	}
	else if (!uwsgi.numproc) {
		uwsgi_log("*** Operational MODE: no-workers ***\n");
	}
	else if (uwsgi.threads > 1) {
		if (uwsgi.numproc > 1) {
			uwsgi_log("*** Operational MODE: preforking+threaded ***\n");
		}
		else {
			uwsgi_log("*** Operational MODE: threaded ***\n");
		}
	}
	else if (uwsgi.async > 1) {
		if (uwsgi.numproc > 1) {
			uwsgi_log("*** Operational MODE: preforking+async ***\n");
		}
		else {
			uwsgi_log("*** Operational MODE: async ***\n");
		}
	}
	else if (uwsgi.numproc > 1) {
		uwsgi_log("*** Operational MODE: preforking ***\n");
	}
	else {
		uwsgi_log("*** Operational MODE: single process ***\n");
	}

	// set a default request structure (for loading apps...)
	uwsgi.wsgi_req = &uwsgi.workers[0].cores[0].req;

	// ok, let's initialize the metrics subsystem
	uwsgi_setup_metrics();

	// cores are allocated, lets allocate logformat (if required)
	if (uwsgi.logformat) {
		uwsgi_build_log_format(uwsgi.logformat);
		uwsgi.logit = uwsgi_logit_lf;
		// TODO check it
		//if (uwsgi.logformat_strftime) {
			//uwsgi.logit = uwsgi_logit_lf_strftime;
		//}
		uwsgi.logvectors = uwsgi_malloc(sizeof(struct iovec *) * uwsgi.cores);
		for (j = 0; j < uwsgi.cores; j++) {
			uwsgi.logvectors[j] = uwsgi_malloc(sizeof(struct iovec) * uwsgi.logformat_vectors);
			uwsgi.logvectors[j][uwsgi.logformat_vectors - 1].iov_base = "\n";
			uwsgi.logvectors[j][uwsgi.logformat_vectors - 1].iov_len = 1;
		}
	}

	// initialize locks and socket as soon as possible, as the master could enqueue tasks
	if (uwsgi.spoolers != NULL) {
		create_signal_pipe(uwsgi.shared->spooler_signal_pipe);
		struct uwsgi_spooler *uspool = uwsgi.spoolers;
		while (uspool) {
			// lock is required even in EXTERNAL mode
			uspool->lock = uwsgi_lock_init(uwsgi_concat2("spooler on ", uspool->dir));
			if (uspool->mode == UWSGI_SPOOLER_EXTERNAL)
				goto next;
			create_signal_pipe(uspool->signal_pipe);
next:
			uspool = uspool->next;
		}
	}

	// preinit apps (create the language environment)
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->preinit_apps) {
			uwsgi.p[i]->preinit_apps();
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->preinit_apps) {
			uwsgi.gp[i]->preinit_apps();
		}
	}

	//init apps hook (if not lazy)
	if (!uwsgi.lazy && !uwsgi.lazy_apps) {
		uwsgi_init_all_apps();
		// Register uwsgi atexit plugin callbacks after all applications have
		// been loaded. This ensures plugin atexit callbacks are called prior
		// to application registered atexit callbacks.
		atexit(uwsgi_plugins_atexit);
	}

	if (!uwsgi.master_as_root) {
		uwsgi_as_root();
	}

	// postinit apps (setup specific features after app initialization)
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->postinit_apps) {
			uwsgi.p[i]->postinit_apps();
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->postinit_apps) {
			uwsgi.gp[i]->postinit_apps();
		}
	}

	// initialize after_request hooks
	uwsgi_foreach(usl, uwsgi.after_request_hooks) {
		usl->custom_ptr =  dlsym(RTLD_DEFAULT, usl->value);
		if (!usl->custom_ptr) {
			uwsgi_log("unable to find symbol/function \"%s\"\n", usl->value);
			exit(1);
		}
		uwsgi_log("added \"%s(struct wsgi_request *)\" to the after-request chain\n", usl->value);
	}

	if (uwsgi.daemonize2) {
		masterpid = uwsgi_daemonize2();
	}

	if (uwsgi.no_server) {
		uwsgi_log("no-server mode requested. Goodbye.\n");
		exit(0);
	}


	if (!uwsgi.master_process && uwsgi.numproc == 0) {
		exit(0);
	}

	if (!uwsgi.single_interpreter && uwsgi.numproc > 0) {
		uwsgi_log("*** uWSGI is running in multiple interpreter mode ***\n");
	}

	// check for request plugins, and eventually print a warning
	int rp_available = 0;
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i] != &unconfigured_plugin) {
			rp_available = 1;
			break;
		}
	}
	if (!rp_available && !ushared->gateways_cnt) {
		uwsgi_log("!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!\n");
		uwsgi_log("no request plugin is loaded, you will not be able to manage requests.\n");
		uwsgi_log("you may need to install the package for your language of choice, or simply load it with --plugin.\n");
		uwsgi_log("!!!!!!!!!!! END OF WARNING !!!!!!!!!!\n");
	}

#ifdef __linux__
#ifdef MADV_MERGEABLE
	if (uwsgi.linux_ksm > 0) {
		uwsgi_log("[uwsgi-KSM] enabled with frequency: %d\n", uwsgi.linux_ksm);
	}
#endif
#endif

	if (uwsgi.master_process) {
		// initialize threads with shared state
		uwsgi_alarm_thread_start();
        	uwsgi_exceptions_handler_thread_start();
		// initialize a mutex to avoid glibc problem with pthread+fork()
		if (uwsgi.threaded_logger) {
			pthread_mutex_init(&uwsgi.threaded_logger_lock, NULL);
		}

		if (uwsgi.is_a_reload) {
			uwsgi_log("gracefully (RE)spawned uWSGI master process (pid: %d)\n", uwsgi.mypid);
		}
		else {
			uwsgi_log("spawned uWSGI master process (pid: %d)\n", uwsgi.mypid);
		}
	}



	// security in multiuser environment: allow only a subset of modifiers
	if (uwsgi.allowed_modifiers) {
		for (i = 0; i < 256; i++) {
			if (!uwsgi_list_has_num(uwsgi.allowed_modifiers, i)) {
				uwsgi.p[i]->request = unconfigured_hook;
				uwsgi.p[i]->after_request = unconfigured_after_hook;
			}
		}
	}

	// master fixup
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->master_fixup) {
			uwsgi.p[i]->master_fixup(0);
		}
	}



	struct uwsgi_spooler *uspool = uwsgi.spoolers;
	while (uspool) {
		if (uspool->mode == UWSGI_SPOOLER_EXTERNAL)
			goto next2;
		uspool->pid = spooler_start(uspool);
next2:
		uspool = uspool->next;
	}

	if (!uwsgi.master_process) {
		if (uwsgi.numproc == 1) {
			uwsgi_log("spawned uWSGI worker 1 (and the only) (pid: %d, cores: %d)\n", masterpid, uwsgi.cores);
		}
		else {
			uwsgi_log("spawned uWSGI worker 1 (pid: %d, cores: %d)\n", masterpid, uwsgi.cores);
		}
		uwsgi.workers[1].pid = masterpid;
		uwsgi.workers[1].id = 1;
		uwsgi.workers[1].last_spawn = uwsgi_now();
		uwsgi.workers[1].manage_next_request = 1;
		uwsgi.mywid = 1;
		uwsgi.respawn_delta = uwsgi_now();
	}
	else {
		// setup internal signalling system
		create_signal_pipe(uwsgi.shared->worker_signal_pipe);
		uwsgi.signal_socket = uwsgi.shared->worker_signal_pipe[1];
	}

	// uWSGI is ready
	uwsgi_notify_ready();
	uwsgi.current_time = uwsgi_now();

	// here we spawn the workers...
	if (!uwsgi.status.is_cheap) {
		if (uwsgi.cheaper && uwsgi.cheaper_count) {
			int nproc = uwsgi.cheaper_initial;
			if (!nproc)
				nproc = uwsgi.cheaper_count;
			for (i = 1; i <= uwsgi.numproc; i++) {
				if (i <= nproc) {
					if (uwsgi_respawn_worker(i))
						break;
					uwsgi.respawn_delta = uwsgi_now();
				}
				else {
					uwsgi.workers[i].cheaped = 1;
				}
			}
		}
		else {
			for (i = 2 - uwsgi.master_process; i < uwsgi.numproc + 1; i++) {
				if (uwsgi_respawn_worker(i))
					break;
				uwsgi.respawn_delta = uwsgi_now();
			}
		}
	}

	if (uwsgi.safe_pidfile2 && !uwsgi.is_a_reload) {
		uwsgi_write_pidfile_explicit(uwsgi.safe_pidfile2, masterpid);
	}

	// END OF INITIALIZATION
	return 0;

}

// this lives in a worker thread and periodically scans for memory usage
// when evil reloaders are in place
void *mem_collector(void *foobar) {
	// block all signals
        sigset_t smask;
        sigfillset(&smask);
        pthread_sigmask(SIG_BLOCK, &smask, NULL);
	uwsgi_log_verbose("mem-collector thread started for worker %d\n", uwsgi.mywid);
	for(;;) {
		sleep(uwsgi.mem_collector_freq);
		uint64_t rss = 0, vsz = 0;
		get_memusage(&rss, &vsz);
		uwsgi.workers[uwsgi.mywid].rss_size = rss;
		uwsgi.workers[uwsgi.mywid].vsz_size = vsz;
	}
	return NULL;
}

int uwsgi_run() {

	// !!! from now on, we could be in the master or in a worker !!!
	int i;

	if (getpid() == masterpid && uwsgi.master_process == 1) {
#ifdef UWSGI_AS_SHARED_LIBRARY
		int ml_ret = master_loop(uwsgi.argv, uwsgi.environ);
		if (ml_ret == -1) {
			return 0;
		}
#else
		(void) master_loop(uwsgi.argv, uwsgi.environ);
#endif
		//from now on the process is a real worker
	}

#if defined(__linux__) && defined(PR_SET_PDEATHSIG)
	// avoid workers running without master at all costs !!! (dangerous)
	if (uwsgi.master_process && uwsgi.no_orphans) {
		if (prctl(PR_SET_PDEATHSIG, SIGKILL)) {
			uwsgi_error("uwsgi_run()/prctl()");
		}
	}
#endif

	if (uwsgi.evil_reload_on_rss || uwsgi.evil_reload_on_as) {
		pthread_t t;
		pthread_create(&t, NULL, mem_collector, NULL);
	}


	// eventually maps (or disable) sockets for the  worker
	uwsgi_map_sockets();

	// eventually set cpu affinity poilicies (OS-dependent)
	uwsgi_set_cpu_affinity();

	if (uwsgi.worker_exec) {
		char *w_argv[2];
		w_argv[0] = uwsgi.worker_exec;
		w_argv[1] = NULL;

		uwsgi.sockets->arg &= (~O_NONBLOCK);
		if (fcntl(uwsgi.sockets->fd, F_SETFL, uwsgi.sockets->arg) < 0) {
			uwsgi_error("fcntl()");
			exit(1);
		}

		if (uwsgi.sockets->fd != 0 && !uwsgi.honour_stdin) {
			if (dup2(uwsgi.sockets->fd, 0) < 0) {
				uwsgi_error("dup2()");
			}
		}
		execvp(w_argv[0], w_argv);
		// never here
		uwsgi_error("execvp()");
		exit(1);
	}

	if (uwsgi.master_as_root) {
		uwsgi_as_root();
	}

	// set default wsgi_req (for loading apps);
	uwsgi.wsgi_req = &uwsgi.workers[uwsgi.mywid].cores[0].req;

	if (uwsgi.offload_threads > 0) {
		uwsgi.offload_thread = uwsgi_malloc(sizeof(struct uwsgi_thread *) * uwsgi.offload_threads);
		for(i=0;i<uwsgi.offload_threads;i++) {
			uwsgi.offload_thread[i] = uwsgi_offload_thread_start();
			if (!uwsgi.offload_thread[i]) {
				uwsgi_log("unable to start offload thread %d for worker %d !!!\n", i, uwsgi.mywid);
				uwsgi.offload_threads = i;
				break;
			}
		}
		uwsgi_log("spawned %d offload threads for uWSGI worker %d\n", uwsgi.offload_threads, uwsgi.mywid);
	}

	// must be run before running apps
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->post_fork) {
			uwsgi.p[i]->post_fork();
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
                if (uwsgi.gp[i]->post_fork) {
                        uwsgi.gp[i]->post_fork();
                }
        }

	uwsgi_hooks_run(uwsgi.hook_post_fork, "post-fork", 1);

	if (uwsgi.worker_exec2) {
                char *w_argv[2];
                w_argv[0] = uwsgi.worker_exec2;
                w_argv[1] = NULL;

                uwsgi.sockets->arg &= (~O_NONBLOCK);
                if (fcntl(uwsgi.sockets->fd, F_SETFL, uwsgi.sockets->arg) < 0) {
                        uwsgi_error("fcntl()");
                        exit(1);
                }

                if (uwsgi.sockets->fd != 0 && !uwsgi.honour_stdin) {
                        if (dup2(uwsgi.sockets->fd, 0) < 0) {
                                uwsgi_error("dup2()");
                        }
                }
                execvp(w_argv[0], w_argv);
                // never here
                uwsgi_error("execvp()");
                exit(1);
        }

	// must be run before running apps

	// check for worker override
        for (i = 0; i < 256; i++) {
                if (uwsgi.p[i]->worker) {
                        if (uwsgi.p[i]->worker()) {
				_exit(0);
			}
                }
        }

        for (i = 0; i < uwsgi.gp_cnt; i++) {
                if (uwsgi.gp[i]->worker) {
                        if (uwsgi.gp[i]->worker()) {
				_exit(0);
			}
                }
        }

	uwsgi_worker_run();
	// never here
	_exit(0);

}

void uwsgi_worker_run() {

	int i;

	if (uwsgi.lazy || uwsgi.lazy_apps) {
		uwsgi_init_all_apps();

		// Register uwsgi atexit plugin callbacks after all applications have
		// been loaded. This ensures plugin atexit callbacks are called prior
		// to application registered atexit callbacks.
		atexit(uwsgi_plugins_atexit);
	}

	// some apps could be mounted only on specific workers
	uwsgi_init_worker_mount_apps();

	if (uwsgi.async > 1) {
		// a stack of unused cores
        	uwsgi.async_queue_unused = uwsgi_malloc(sizeof(struct wsgi_request *) * uwsgi.async);

        	// fill it with default values
               for (i = 0; i < uwsgi.async; i++) {
               	uwsgi.async_queue_unused[i] = &uwsgi.workers[uwsgi.mywid].cores[i].req;
               }

                // the first available core is the last one
                uwsgi.async_queue_unused_ptr = uwsgi.async - 1;
	}

	// setup UNIX signals for the worker
	if (uwsgi.harakiri_options.workers > 0 && !uwsgi.master_process) {
		signal(SIGALRM, (void *) &harakiri);
	}
	uwsgi_unix_signal(SIGHUP, gracefully_kill);
	uwsgi_unix_signal(SIGINT, end_me);
	uwsgi_unix_signal(SIGTERM, end_me);

	uwsgi_unix_signal(SIGUSR1, stats);
	signal(SIGUSR2, (void *) &what_i_am_doing);
	if (!uwsgi.ignore_sigpipe) {
		signal(SIGPIPE, (void *) &warn_pipe);
	}

	// worker initialization done

	// run fixup handler
	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->fixup) {
			uwsgi.p[i]->fixup();
		}
	}

	if (uwsgi.chdir2) {
		uwsgi_log("chdir() to %s\n", uwsgi.chdir2);
		if (chdir(uwsgi.chdir2)) {
			uwsgi_error("chdir()");
			exit(1);
		}
	}


	//re - initialize wsgi_req(can be full of init_uwsgi_app data)
	for (i = 0; i < uwsgi.cores; i++) {
		memset(&uwsgi.workers[uwsgi.mywid].cores[i].req, 0, sizeof(struct wsgi_request));
		uwsgi.workers[uwsgi.mywid].cores[i].req.async_id = i;
	}


	// eventually remap plugins
	if (uwsgi.remap_modifier) {
		char *map, *ctx = NULL;
		uwsgi_foreach_token(uwsgi.remap_modifier, ",", map, ctx) {
			char *colon = strchr(map, ':');
			if (colon) {
				colon[0] = 0;
				int rm_src = atoi(map);
				int rm_dst = atoi(colon + 1);
				uwsgi.p[rm_dst]->request = uwsgi.p[rm_src]->request;
				uwsgi.p[rm_dst]->after_request = uwsgi.p[rm_src]->after_request;
			}
		}
	}


	if (uwsgi.cores > 1) {
		uwsgi.workers[uwsgi.mywid].cores[0].thread_id = pthread_self();
	}

	uwsgi_ignition();

	// never here
	exit(0);

}


void uwsgi_ignition() {

	int i;

	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->hijack_worker) {
			uwsgi.p[i]->hijack_worker();
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->hijack_worker) {
			uwsgi.gp[i]->hijack_worker();
		}
	}

	// create a pthread key, storing per-thread wsgi_request structure
	if (uwsgi.threads > 1) {
		if (pthread_key_create(&uwsgi.tur_key, NULL)) {
			uwsgi_error("pthread_key_create()");
			exit(1);
		}
	}
	if (pipe(&uwsgi.loop_stop_pipe[0])) {
		uwsgi_error("pipe()")
		exit(1);
	}

	// mark the worker as "accepting" (this is a mark used by chain reloading)
	uwsgi.workers[uwsgi.mywid].accepting = 1;
	// ready to accept request, if i am a vassal signal Emperor about it
        if (uwsgi.has_emperor && uwsgi.mywid == 1) {
                char byte = 5;
                if (write(uwsgi.emperor_fd, &byte, 1) != 1) {
                        uwsgi_error("emperor-i-am-ready-to-accept/write()");
			uwsgi_log_verbose("lost communication with the Emperor, goodbye...\n");
			gracefully_kill_them_all(0);
			exit(1);
                }
        }

	// run accepting hooks
	uwsgi_hooks_run(uwsgi.hook_accepting, "accepting", 1);
	if (uwsgi.workers[uwsgi.mywid].respawn_count == 1) {
		uwsgi_hooks_run(uwsgi.hook_accepting_once, "accepting-once", 1);
	}

	if (uwsgi.mywid == 1) {
		uwsgi_hooks_run(uwsgi.hook_accepting1, "accepting1", 1);
		if (uwsgi.workers[uwsgi.mywid].respawn_count == 1) {
			uwsgi_hooks_run(uwsgi.hook_accepting1_once, "accepting1-once", 1);
		}
	}

	if (uwsgi.loop) {
		void (*u_loop) (void) = uwsgi_get_loop(uwsgi.loop);
		if (!u_loop) {
			uwsgi_log("unavailable loop engine !!!\n");
			exit(1);
		}
		if (uwsgi.mywid == 1) {
			uwsgi_log("*** running %s loop engine [addr:%p] ***\n", uwsgi.loop, u_loop);
		}
		u_loop();
		uwsgi_log("your loop engine died. R.I.P.\n");
	}
	else {
		if (uwsgi.async < 2) {
			simple_loop();
		}
		else {
			async_loop();
		}
	}

	// main thread waits other threads.
	if (uwsgi.threads > 1) {
		wait_for_threads();
	}

	// end of the process...
	end_me(0);
}

/*

what happens here ?

we transform the uwsgi_option structure to a struct option
for passing it to getopt_long
A short options string is built.

This function could be called multiple times, so it will free previous areas

*/

void build_options() {

	int options_count = 0;
	int pos = 0;
	int i;
	// first count the base options

	struct uwsgi_option *op = uwsgi_base_options;
	while (op->name) {
		options_count++;
		op++;
	}

	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->options) {
			options_count += uwsgi_count_options(uwsgi.p[i]->options);
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->options) {
			options_count += uwsgi_count_options(uwsgi.gp[i]->options);
		}
	}

	// add custom options
	struct uwsgi_custom_option *uco = uwsgi.custom_options;
	while (uco) {
		options_count++;
		uco = uco->next;
	}

	if (uwsgi.options)
		free(uwsgi.options);


	// rebuild uwsgi.options area
	uwsgi.options = uwsgi_calloc(sizeof(struct uwsgi_option) * (options_count + 1));

	op = uwsgi_base_options;
	while (op->name) {
		memcpy(&uwsgi.options[pos], op, sizeof(struct uwsgi_option));
		pos++;
		op++;
	}

	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->options) {
			int c = uwsgi_count_options(uwsgi.p[i]->options);
			memcpy(&uwsgi.options[pos], uwsgi.p[i]->options, sizeof(struct uwsgi_option) * c);
			pos += c;
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->options) {
			int c = uwsgi_count_options(uwsgi.gp[i]->options);
			memcpy(&uwsgi.options[pos], uwsgi.gp[i]->options, sizeof(struct uwsgi_option) * c);
			pos += c;
		}
	}

	uco = uwsgi.custom_options;
        while (uco) {
                uwsgi.options[pos].name = uco->name;
                if (uco->has_args) {
                        uwsgi.options[pos].type = required_argument;
                }
                else {
                        uwsgi.options[pos].type = no_argument;
                }
                // custom options should be immediate
                uwsgi.options[pos].flags = UWSGI_OPT_IMMEDIATE;
                // help shows the option definition
                uwsgi.options[pos].help = uco->value;
                uwsgi.options[pos].data = uco;
                uwsgi.options[pos].func = uwsgi_opt_custom;

                pos++;
                uco = uco->next;
        }


	pos = 0;

	if (uwsgi.long_options)
		free(uwsgi.long_options);

	uwsgi.long_options = uwsgi_calloc(sizeof(struct option) * (options_count + 1));

	if (uwsgi.short_options)
		free(uwsgi.short_options);

	uwsgi.short_options = uwsgi_calloc((options_count * 3) + 1);

	// build long_options (this time with custom_options)
	op = uwsgi.options;
	while (op->name) {
		uwsgi.long_options[pos].name = op->name;
		uwsgi.long_options[pos].has_arg = op->type;
		uwsgi.long_options[pos].flag = 0;
		// add 1000 to avoid short_options collision
		uwsgi.long_options[pos].val = 1000 + pos;
		if (op->shortcut) {
			char shortcut = (char) op->shortcut;
			// avoid duplicates in short_options
			if (!strchr(uwsgi.short_options, shortcut)) {
				strncat(uwsgi.short_options, &shortcut, 1);
				if (op->type == optional_argument) {
					strcat(uwsgi.short_options, "::");
				}
				else if (op->type == required_argument) {
					strcat(uwsgi.short_options, ":");
				}
			}
		}
		op++;
		pos++;
	}
}

/*

this function builds the help output from the uwsgi.options structure

*/
void uwsgi_help(char *opt, char *val, void *none) {

	size_t max_size = 0;

	fprintf(stdout, "Usage: %s [options...]\n", uwsgi.binary_path);

	struct uwsgi_option *op = uwsgi.options;
	while (op && op->name) {
		if (strlen(op->name) > max_size) {
			max_size = strlen(op->name);
		}
		op++;
	}

	max_size++;

	op = uwsgi.options;
	while (op && op->name) {
		if (op->shortcut) {
			fprintf(stdout, "    -%c|--%-*s %s\n", op->shortcut, (int) max_size - 3, op->name, op->help);
		}
		else {
			fprintf(stdout, "    --%-*s %s\n", (int) max_size, op->name, op->help);
		}
		op++;
	}

	exit(0);
}

/*

initialize all apps

*/
void uwsgi_init_all_apps() {

	int i, j;

	uwsgi_hooks_run(uwsgi.hook_pre_app, "pre app", 1);

	// now run the pre-app scripts
	struct uwsgi_string_list *usl = uwsgi.exec_pre_app;
	while (usl) {
		uwsgi_log("running \"%s\" (pre app)...\n", usl->value);
		int ret = uwsgi_run_command_and_wait(NULL, usl->value);
		if (ret != 0) {
			uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
			exit(1);
		}
		usl = usl->next;
	}

	uwsgi_foreach(usl, uwsgi.call_pre_app) {
                if (uwsgi_call_symbol(usl->value)) {
                        uwsgi_log("unable to call function \"%s\"\n", usl->value);
			exit(1);
                }
        }


	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->init_apps) {
			uwsgi.p[i]->init_apps();
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {
		if (uwsgi.gp[i]->init_apps) {
			uwsgi.gp[i]->init_apps();
		}
	}

	struct uwsgi_string_list *app_mps = uwsgi.mounts;
	while (app_mps) {
		char *what = strchr(app_mps->value, '=');
		if (what) {
			what[0] = 0;
			what++;
			for (j = 0; j < 256; j++) {
				if (uwsgi.p[j]->mount_app) {
					uwsgi_log("mounting %s on %s\n", what, app_mps->value);
					if (uwsgi.p[j]->mount_app(app_mps->value, what) != -1)
						break;
				}
			}
			what--;
			what[0] = '=';
		}
		else {
			uwsgi_log("invalid mountpoint: %s\n", app_mps->value);
			exit(1);
		}
		app_mps = app_mps->next;
	}

	// no app initialized and virtualhosting enabled
	if (uwsgi_apps_cnt == 0 && uwsgi.numproc > 0 && !uwsgi.command_mode) {
		if (uwsgi.need_app) {
			if (!uwsgi.lazy)
				uwsgi_log("*** no app loaded. GAME OVER ***\n");
			exit(UWSGI_FAILED_APP_CODE);
		}
		else {
			uwsgi_log("*** no app loaded. going in full dynamic mode ***\n");
		}
	}

	uwsgi_hooks_run(uwsgi.hook_post_app, "post app", 1);

	usl = uwsgi.exec_post_app;
        while (usl) {
                uwsgi_log("running \"%s\" (post app)...\n", usl->value);
                int ret = uwsgi_run_command_and_wait(NULL, usl->value);
                if (ret != 0) {
                        uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
                        exit(1);
                }
                usl = usl->next;
        }

	uwsgi_foreach(usl, uwsgi.call_post_app) {
                if (uwsgi_call_symbol(usl->value)) {
                        uwsgi_log("unable to call function \"%s\"\n", usl->value);
                }
        }

}

void uwsgi_init_worker_mount_apps() {
/*
	int i,j;
	for (i = 0; i < uwsgi.mounts_cnt; i++) {
                char *what = strchr(uwsgi.mounts[i], '=');
                if (what) {
                        what[0] = 0;
                        what++;
                        for (j = 0; j < 256; j++) {
                                if (uwsgi.p[j]->mount_app) {
                                        if (!uwsgi_startswith(uwsgi.mounts[i], "worker://", 9)) {
                        			uwsgi_log("mounting %s on %s\n", what, uwsgi.mounts[i]+9);
                                                if (uwsgi.p[j]->mount_app(uwsgi.mounts[i] + 9, what, 1) != -1)
                                                        break;
                                        }
                                }
                        }
                        what--;
                        what[0] = '=';
                }
                else {
                        uwsgi_log("invalid mountpoint: %s\n", uwsgi.mounts[i]);
                        exit(1);
                }
        }
*/

}

void uwsgi_opt_true(char *opt, char *value, void *key) {

	int *ptr = (int *) key;
	*ptr = 1;
	if (value) {
		if (!strcasecmp("false", value) || !strcasecmp("off", value) || !strcasecmp("no", value) || !strcmp("0", value)) {
			*ptr = 0;
		}
	}
}

void uwsgi_opt_false(char *opt, char *value, void *key) {

        int *ptr = (int *) key;
        *ptr = 0;
        if (value) {
                if (!strcasecmp("false", value) || !strcasecmp("off", value) || !strcasecmp("no", value) || !strcmp("0", value)) {
                        *ptr = 1;
                }
        }
}

void uwsgi_opt_set_immediate_gid(char *opt, char *value, void *none) {
        gid_t gid = 0;
	if (is_a_number(value)) gid = atoi(value);
	if (gid == 0) {
		struct group *ugroup = getgrnam(value);
                if (ugroup)
                	gid = ugroup->gr_gid;
	}
        if (gid <= 0) {
                uwsgi_log("uwsgi_opt_set_immediate_gid(): invalid gid %d\n", (int) gid);
                exit(1);
        }
        if (setgid(gid)) {
                uwsgi_error("uwsgi_opt_set_immediate_gid()/setgid()");
                exit(1);
        }

	if (setgroups(0, NULL)) {
        	uwsgi_error("uwsgi_opt_set_immediate_gid()/setgroups()");
                exit(1);
        }

	gid = getgid();
	if (!gid) {
		exit(1);
	}
	uwsgi_log("immediate gid: %d\n", (int) gid);
}


void uwsgi_opt_set_immediate_uid(char *opt, char *value, void *none) {
	uid_t uid = 0;
	if (is_a_number(value)) uid = atoi(value);
	if (uid == 0) {
		struct passwd *upasswd = getpwnam(value);
                if (upasswd)
                        uid = upasswd->pw_uid;
	}
	if (uid <= 0) {
		uwsgi_log("uwsgi_opt_set_immediate_uid(): invalid uid %d\n", uid);
		exit(1);
	}
	if (setuid(uid)) {
		uwsgi_error("uwsgi_opt_set_immediate_uid()/setuid()");
		exit(1);
	}

	uid = getuid();
	if (!uid) {
		exit(1);
	}
	uwsgi_log("immediate uid: %d\n", (int) uid);
}

void uwsgi_opt_safe_fd(char *opt, char *value, void *foobar) {
	int fd = atoi(value);
	if (fd < 0) {
		uwsgi_log("invalid file descriptor: %d\n", fd);
		exit(1);
	}
	uwsgi_add_safe_fd(fd);
}

void uwsgi_opt_set_int(char *opt, char *value, void *key) {
	int *ptr = (int *) key;
	if (value) {
		*ptr = atoi((char *) value);
	}
	else {
		*ptr = 1;
	}

	if (*ptr < 0) {
		uwsgi_log("invalid value for option \"%s\": must be > 0\n", opt);
		exit(1);
	}
}

void uwsgi_opt_uid(char *opt, char *value, void *key) {
	uid_t uid = 0;
	if (is_a_number(value)) uid = atoi(value);
	if (!uid) {
		struct passwd *p = getpwnam(value);
		if (p) {
			uid = p->pw_uid;	
		}
		else {
			uwsgi_log("unable to find user %s\n", value);
			exit(1);
		}
	}
	if (key)  {
        	uid_t *ptr = (uid_t *) key;
        	*ptr = uid;
        }
}

void uwsgi_opt_gid(char *opt, char *value, void *key) {
        gid_t gid = 0;
	if (is_a_number(value)) gid = atoi(value);
        if (!gid) {
                struct group *g = getgrnam(value);
                if (g) {
                        gid = g->gr_gid;
                }
                else {
                        uwsgi_log("unable to find group %s\n", value);
			exit(1);
                }
        }       
        if (key)  {
                gid_t *ptr = (gid_t *) key;
                *ptr = gid;
        }       
}     

void uwsgi_opt_set_rawint(char *opt, char *value, void *key) {
	int *ptr = (int *) key;
	if (value) {
		*ptr = atoi((char *) value);
	}
	else {
		*ptr = 1;
	}
}


void uwsgi_opt_set_64bit(char *opt, char *value, void *key) {
	uint64_t *ptr = (uint64_t *) key;

	if (value) {
		*ptr = (strtoul(value, NULL, 10));
	}
	else {
		*ptr = 1;
	}
}

void uwsgi_opt_set_16bit(char *opt, char *value, void *key) {
        uint16_t *ptr = (uint16_t *) key;

        if (value) {
		unsigned long n = strtoul(value, NULL, 10);
		if (n > 65535) n = 65535;
                *ptr = n;
        }
        else {
                *ptr = 1;
        }
}


void uwsgi_opt_set_megabytes(char *opt, char *value, void *key) {
	uint64_t *ptr = (uint64_t *) key;
	*ptr = (uint64_t)strtoul(value, NULL, 10) * 1024 * 1024;
}

void uwsgi_opt_set_str(char *opt, char *value, void *key) {
	char **ptr = (char **) key;
	if (!value) {
		*ptr = "";
		return;	
	}
	*ptr = (char *) value;
}

void uwsgi_opt_set_null(char *opt, char *value, void *key) {
        char **ptr = (char **) key;
        *ptr = NULL;
}


void uwsgi_opt_set_logger(char *opt, char *value, void *prefix) {

	if (!value)
		value = "";

	if (prefix) {
		uwsgi_string_new_list(&uwsgi.requested_logger, uwsgi_concat3((char *) prefix, ":", value));
	}
	else {
		uwsgi_string_new_list(&uwsgi.requested_logger, uwsgi_str(value));
	}
}

void uwsgi_opt_set_req_logger(char *opt, char *value, void *prefix) {

	if (!value)
		value = "";

	if (prefix) {
		uwsgi_string_new_list(&uwsgi.requested_req_logger, uwsgi_concat3((char *) prefix, ":", value));
	}
	else {
		uwsgi_string_new_list(&uwsgi.requested_req_logger, uwsgi_str(value));
	}
}

void uwsgi_opt_set_str_spaced(char *opt, char *value, void *key) {
	char **ptr = (char **) key;
	*ptr = uwsgi_concat2((char *) value, " ");
}

void uwsgi_opt_add_string_list(char *opt, char *value, void *list) {
	struct uwsgi_string_list **ptr = (struct uwsgi_string_list **) list;
	uwsgi_string_new_list(ptr, value);
}

void uwsgi_opt_add_addr_list(char *opt, char *value, void *list) {
        struct uwsgi_string_list **ptr = (struct uwsgi_string_list **) list;
	int af = AF_INET;
#ifdef AF_INET6
	void *ip = uwsgi_malloc(16);
	if (strchr(value, ':')) {
		af = AF_INET6;
	}
#else
	void *ip = uwsgi_malloc(4);
#endif
	
	if (inet_pton(af, value, ip) <= 0) {
		uwsgi_log("%s: invalid address\n", opt);
		uwsgi_error("uwsgi_opt_add_addr_list()");
		exit(1);
	}

        struct uwsgi_string_list *usl = uwsgi_string_new_list(ptr, ip);
	usl->custom = af;
	usl->custom_ptr = value;
}


void uwsgi_opt_add_string_list_custom(char *opt, char *value, void *list) {
	struct uwsgi_string_list **ptr = (struct uwsgi_string_list **) list;
	struct uwsgi_string_list *usl = uwsgi_string_new_list(ptr, value);
	usl->custom = 1;
}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
void uwsgi_opt_add_regexp_list(char *opt, char *value, void *list) {
	struct uwsgi_regexp_list **ptr = (struct uwsgi_regexp_list **) list;
	uwsgi_regexp_new_list(ptr, value);
}

void uwsgi_opt_add_regexp_custom_list(char *opt, char *value, void *list) {
	char *space = strchr(value, ' ');
	if (!space) {
		uwsgi_log("invalid custom regexp syntax: must be <custom> <regexp>\n");
		exit(1);
	}
	char *custom = uwsgi_concat2n(value, space - value, "", 0);
	struct uwsgi_regexp_list **ptr = (struct uwsgi_regexp_list **) list;
	uwsgi_regexp_custom_new_list(ptr, space + 1, custom);
}
#endif

void uwsgi_opt_add_shared_socket(char *opt, char *value, void *protocol) {
	struct uwsgi_socket *us = uwsgi_new_shared_socket(generate_socket_name(value));
	if (!strcmp(opt, "undeferred-shared-socket")) {
		us->no_defer = 1;
	}
}

void uwsgi_opt_add_socket(char *opt, char *value, void *protocol) {
	struct uwsgi_socket *uwsgi_sock = uwsgi_new_socket(generate_socket_name(value));
	uwsgi_sock->name_len = strlen(uwsgi_sock->name);
	uwsgi_sock->proto_name = protocol;
}

#ifdef UWSGI_SSL
void uwsgi_opt_add_ssl_socket(char *opt, char *value, void *protocol) {
	char *client_ca = NULL;

        // build socket, certificate and key file
        char *sock = uwsgi_str(value);
        char *crt = strchr(sock, ',');
        if (!crt) {
                uwsgi_log("invalid https-socket syntax must be socket,crt,key\n");
                exit(1);
        }
        *crt = '\0'; crt++;
        char *key = strchr(crt, ',');
        if (!key) {
                uwsgi_log("invalid https-socket syntax must be socket,crt,key\n");
                exit(1);
        }
        *key = '\0'; key++;

        char *ciphers = strchr(key, ',');
        if (ciphers) {
                *ciphers = '\0'; ciphers++;
                client_ca = strchr(ciphers, ',');
                if (client_ca) {
                        *client_ca = '\0'; client_ca++;
                }
        }

	struct uwsgi_socket *uwsgi_sock = uwsgi_new_socket(generate_socket_name(sock));
	uwsgi_sock->name_len = strlen(uwsgi_sock->name);
        uwsgi_sock->proto_name = protocol;

        // ok we have the socket, initialize ssl if required
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        // initialize ssl context
        uwsgi_sock->ssl_ctx = uwsgi_ssl_new_server_context(uwsgi_sock->name, crt, key, ciphers, client_ca);
        if (!uwsgi_sock->ssl_ctx) {
                exit(1);
        }
}
#endif

void uwsgi_opt_add_socket_no_defer(char *opt, char *value, void *protocol) {
        struct uwsgi_socket *uwsgi_sock = uwsgi_new_socket(generate_socket_name(value));
        uwsgi_sock->name_len = strlen(uwsgi_sock->name);
        uwsgi_sock->proto_name = protocol;
	uwsgi_sock->no_defer = 1;
}

void uwsgi_opt_add_lazy_socket(char *opt, char *value, void *protocol) {
	struct uwsgi_socket *uwsgi_sock = uwsgi_new_socket(generate_socket_name(value));
	uwsgi_sock->proto_name = protocol;
	uwsgi_sock->bound = 1;
	uwsgi_sock->lazy = 1;
}


void uwsgi_opt_set_placeholder(char *opt, char *value, void *ph) {

	char *p = strchr(value, '=');
	if (!p) {
		uwsgi_log("invalid placeholder/--set value\n");
		exit(1);
	}

	p[0] = 0;
	add_exported_option_do(uwsgi_str(value), p + 1, 0, ph ? 1 : 0);
	p[0] = '=';

}

void uwsgi_opt_ssa(char *opt, char *value, void *foobar) {
	uwsgi_subscription_set_algo(value);
}

#ifdef UWSGI_SSL
void uwsgi_opt_scd(char *opt, char *value, void *foobar) {
	// openssl could not be initialized
	if (!uwsgi.ssl_initialized) {
		uwsgi_ssl_init();
	}

	char *colon = strchr(value, ':');
	if (!colon) {
		uwsgi_log("invalid syntax for '%s', must be: <digest>:<directory>\n", opt);
		exit(1);
	}

	char *algo = uwsgi_concat2n(value, (colon - value), "", 0);
	uwsgi.subscriptions_sign_check_md = EVP_get_digestbyname(algo);
	if (!uwsgi.subscriptions_sign_check_md) {
		uwsgi_log("unable to find digest algorithm: %s\n", algo);
		exit(1);
	}
	free(algo);

	uwsgi.subscriptions_sign_check_dir = colon + 1;
}
#endif

void uwsgi_opt_set_umask(char *opt, char *value, void *mode) {
	int error = 0;
	mode_t mask = uwsgi_mode_t(value, &error);
	if (error) {
		uwsgi_log("invalid umask: %s\n", value);
	}
	umask(mask);

	uwsgi.do_not_change_umask = 1;
}

void uwsgi_opt_exit(char *opt, char *value, void *none) {
	int exit_code = 1;
	if (value) {
		exit_code = atoi(value);
	}
	exit(exit_code);
}

void uwsgi_opt_print(char *opt, char *value, void *str) {
	if (str) {
		fprintf(stdout, "%s\n", (char *) str);
		exit(0);
	}
	fprintf(stdout, "%s\n", value);
}

void uwsgi_opt_set_uid(char *opt, char *value, void *none) {
	if (is_a_number(value)) uwsgi.uid = atoi(value);
	if (!uwsgi.uid)
		uwsgi.uidname = value;
}

void uwsgi_opt_set_gid(char *opt, char *value, void *none) {
	if (is_a_number(value)) uwsgi.gid = atoi(value);
	if (!uwsgi.gid)
		uwsgi.gidname = value;
}

#ifdef UWSGI_CAP
void uwsgi_opt_set_cap(char *opt, char *value, void *none) {
	uwsgi.cap_count = uwsgi_build_cap(value, &uwsgi.cap);
	if (uwsgi.cap_count == 0) {
		uwsgi_log("[security] empty capabilities mask !!!\n");
		exit(1);
	}
}
void uwsgi_opt_set_emperor_cap(char *opt, char *value, void *none) {
	uwsgi.emperor_cap_count = uwsgi_build_cap(value, &uwsgi.emperor_cap);
	if (uwsgi.emperor_cap_count == 0) {
		uwsgi_log("[security] empty capabilities mask !!!\n");
		exit(1);
	}
}
#endif
#ifdef __linux__
void uwsgi_opt_set_unshare(char *opt, char *value, void *mask) {
	uwsgi_build_unshare(value, (int *) mask);
}
#endif

void uwsgi_opt_set_env(char *opt, char *value, void *none) {
	if (putenv(value)) {
		uwsgi_error("putenv()");
	}
}

void uwsgi_opt_unset_env(char *opt, char *value, void *none) {
#ifdef UNSETENV_VOID
	unsetenv(value);
#else
	if (unsetenv(value)) {
		uwsgi_error("unsetenv()");
	}
#endif
}

void uwsgi_opt_pidfile_signal(char *opt, char *pidfile, void *sig) {

	long *signum_fake_ptr = (long *) sig;
	int signum = (long) signum_fake_ptr;
	exit(signal_pidfile(signum, pidfile));
}

void uwsgi_opt_load_dl(char *opt, char *value, void *none) {
	if (!dlopen(value, RTLD_NOW | RTLD_GLOBAL)) {
		uwsgi_log("%s\n", dlerror());
	}
}

void uwsgi_opt_load_plugin(char *opt, char *value, void *none) {

	char *plugins_list = uwsgi_concat2(value, "");
	char *p, *ctx = NULL;
	uwsgi_foreach_token(plugins_list, ",", p, ctx) {
#ifdef UWSGI_DEBUG
		uwsgi_debug("loading plugin %s\n", p);
#endif
		if (uwsgi_load_plugin(-1, p, NULL)) {
			build_options();
		}
		else if (!uwsgi_startswith(opt, "need-", 5)) {
			uwsgi_log("unable to load plugin \"%s\"\n", p);
			exit(1);
		}
	}
	free(p);
	free(plugins_list);
}

void uwsgi_opt_check_static(char *opt, char *value, void *foobar) {

	uwsgi_dyn_dict_new(&uwsgi.check_static, value, strlen(value), NULL, 0);
	uwsgi_log("[uwsgi-static] added check for %s\n", value);
	uwsgi.build_mime_dict = 1;

}

void uwsgi_opt_add_dyn_dict(char *opt, char *value, void *dict) {

	char *equal = strchr(value, '=');
	if (!equal) {
		uwsgi_log("invalid dictionary syntax for %s\n", opt);
		exit(1);
	}

	struct uwsgi_dyn_dict **udd = (struct uwsgi_dyn_dict **) dict;

	uwsgi_dyn_dict_new(udd, value, equal - value, equal + 1, strlen(equal + 1));

}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
void uwsgi_opt_add_regexp_dyn_dict(char *opt, char *value, void *dict) {

	char *space = strchr(value, ' ');
	if (!space) {
		uwsgi_log("invalid dictionary syntax for %s\n", opt);
		exit(1);
	}

	struct uwsgi_dyn_dict **udd = (struct uwsgi_dyn_dict **) dict;

	struct uwsgi_dyn_dict *new_udd = uwsgi_dyn_dict_new(udd, value, space - value, space + 1, strlen(space + 1));

	char *regexp = uwsgi_concat2n(value, space - value, "", 0);

	if (uwsgi_regexp_build(regexp, &new_udd->pattern)) {
		exit(1);
	}

	free(regexp);
}
#endif


void uwsgi_opt_fileserve_mode(char *opt, char *value, void *foobar) {

	if (!strcasecmp("x-sendfile", value)) {
		uwsgi.file_serve_mode = 2;
	}
	else if (!strcasecmp("xsendfile", value)) {
		uwsgi.file_serve_mode = 2;
	}
	else if (!strcasecmp("x-accel-redirect", value)) {
		uwsgi.file_serve_mode = 1;
	}
	else if (!strcasecmp("xaccelredirect", value)) {
		uwsgi.file_serve_mode = 1;
	}
	else if (!strcasecmp("nginx", value)) {
		uwsgi.file_serve_mode = 1;
	}

}

void uwsgi_opt_static_map(char *opt, char *value, void *static_maps) {

	struct uwsgi_dyn_dict **maps = (struct uwsgi_dyn_dict **) static_maps;
	char *mountpoint = uwsgi_str(value);

	char *docroot = strchr(mountpoint, '=');

	if (!docroot) {
		uwsgi_log("invalid document root in static map, syntax mountpoint=docroot\n");
		exit(1);
	}
	docroot[0] = 0;
	docroot++;
	uwsgi_dyn_dict_new(maps, mountpoint, strlen(mountpoint), docroot, strlen(docroot));
	uwsgi_log_initial("[uwsgi-static] added mapping for %s => %s\n", mountpoint, docroot);
	uwsgi.build_mime_dict = 1;
}


int uwsgi_zerg_attach(char *value) {

	int count = 8;
	int zerg_fd = uwsgi_connect(value, 30, 0);
	if (zerg_fd < 0) {
		uwsgi_log("--- unable to connect to zerg server %s ---\n", value);
		return -1;
	}

	int last_count = count;

	int *zerg = uwsgi_attach_fd(zerg_fd, &count, "uwsgi-zerg", 10);
	if (zerg == NULL) {
		if (last_count != count) {
			close(zerg_fd);
			zerg_fd = uwsgi_connect(value, 30, 0);
			if (zerg_fd < 0) {
				uwsgi_log("--- unable to connect to zerg server %s ---\n", value);
				return -1;
			}
			zerg = uwsgi_attach_fd(zerg_fd, &count, "uwsgi-zerg", 10);
		}
	}

	if (zerg == NULL) {
		uwsgi_log("--- invalid data received from zerg-server ---\n");
		close(zerg_fd);
		return -1;
	}

	if (!uwsgi.zerg) {
		uwsgi.zerg = zerg;
	}
	else {
		int pos = 0;
		for (;;) {
			if (uwsgi.zerg[pos] == -1) {
				uwsgi.zerg = realloc(uwsgi.zerg, (sizeof(int) * (pos)) + (sizeof(int) * count + 1));
				if (!uwsgi.zerg) {
					uwsgi_error("realloc()");
					exit(1);
				}
				memcpy(&uwsgi.zerg[pos], zerg, (sizeof(int) * count + 1));
				break;
			}
			pos++;
		}
		free(zerg);
	}

	close(zerg_fd);
	return 0;
}

void uwsgi_opt_signal(char *opt, char *value, void *foobar) {
	uwsgi_command_signal(value);
}

void uwsgi_opt_log_date(char *opt, char *value, void *foobar) {

	uwsgi.logdate = 1;
	if (value) {
		if (strcasecmp("true", value) && strcasecmp("1", value) && strcasecmp("on", value) && strcasecmp("yes", value)) {
			uwsgi.log_strftime = value;
		}
	}
}

void uwsgi_opt_chmod_socket(char *opt, char *value, void *foobar) {

	int i;

	uwsgi.chmod_socket = 1;
	if (value) {
		if (strlen(value) == 1 && *value == '1') {
			return;
		}
		if (strlen(value) != 3) {
			uwsgi_log("invalid chmod value: %s\n", value);
			exit(1);
		}
		for (i = 0; i < 3; i++) {
			if (value[i] < '0' || value[i] > '7') {
				uwsgi_log("invalid chmod value: %s\n", value);
				exit(1);
			}
		}

		uwsgi.chmod_socket_value = (uwsgi.chmod_socket_value << 3) + (value[0] - '0');
		uwsgi.chmod_socket_value = (uwsgi.chmod_socket_value << 3) + (value[1] - '0');
		uwsgi.chmod_socket_value = (uwsgi.chmod_socket_value << 3) + (value[2] - '0');
	}

}

void uwsgi_opt_logfile_chmod(char *opt, char *value, void *foobar) {

	int i;

	if (strlen(value) != 3) {
		uwsgi_log("invalid chmod value: %s\n", value);
		exit(1);
	}
	for (i = 0; i < 3; i++) {
		if (value[i] < '0' || value[i] > '7') {
			uwsgi_log("invalid chmod value: %s\n", value);
			exit(1);
		}
	}

	uwsgi.chmod_logfile_value = (uwsgi.chmod_logfile_value << 3) + (value[0] - '0');
	uwsgi.chmod_logfile_value = (uwsgi.chmod_logfile_value << 3) + (value[1] - '0');
	uwsgi.chmod_logfile_value = (uwsgi.chmod_logfile_value << 3) + (value[2] - '0');

}

void uwsgi_opt_max_vars(char *opt, char *value, void *foobar) {

	uwsgi.max_vars = atoi(value);
	uwsgi.vec_size = 4 + 1 + (4 * uwsgi.max_vars);
}

void uwsgi_opt_deprecated(char *opt, char *value, void *message) {
	uwsgi_log("[WARNING] option \"%s\" is deprecated: %s\n", opt, (char *) message);
}

void uwsgi_opt_load(char *opt, char *filename, void *none) {

	// here we need to avoid setting upper magic vars
	int orig_magic = uwsgi.magic_table_first_round;
	uwsgi.magic_table_first_round = 1;

	if (uwsgi_endswith(filename, ".ini")) {
		uwsgi_opt_load_ini(opt, filename, none);
		goto end;
	}
#ifdef UWSGI_XML
	if (uwsgi_endswith(filename, ".xml")) {
		uwsgi_opt_load_xml(opt, filename, none);
		goto end;
	}
#endif
#ifdef UWSGI_YAML
	if (uwsgi_endswith(filename, ".yaml")) {
		uwsgi_opt_load_yml(opt, filename, none);
		goto end;
	}
	if (uwsgi_endswith(filename, ".yml")) {
		uwsgi_opt_load_yml(opt, filename, none);
		goto end;
	}
#endif
#ifdef UWSGI_JSON
	if (uwsgi_endswith(filename, ".json")) {
		uwsgi_opt_load_json(opt, filename, none);
		goto end;
	}
	if (uwsgi_endswith(filename, ".js")) {
		uwsgi_opt_load_json(opt, filename, none);
		goto end;
	}
#endif

	// fallback to pluggable system
	uwsgi_opt_load_config(opt, filename, none);
end:
	uwsgi.magic_table_first_round = orig_magic;
}

void uwsgi_opt_logic(char *opt, char *arg, void *func) {

	if (uwsgi.logic_opt) {
		uwsgi_log("recursive logic in options is not supported (option = %s)\n", opt);
		exit(1);
	}
	uwsgi.logic_opt = (int (*)(char *, char *)) func;
	uwsgi.logic_opt_cycles = 0;
	if (arg) {
		uwsgi.logic_opt_arg = uwsgi_str(arg);
	}
	else {
		uwsgi.logic_opt_arg = NULL;
	}
}

void uwsgi_opt_noop(char *opt, char *foo, void *bar) {
}

void uwsgi_opt_load_ini(char *opt, char *filename, void *none) {
	config_magic_table_fill(filename, uwsgi.magic_table);
	uwsgi_ini_config(filename, uwsgi.magic_table);
}

void uwsgi_opt_load_config(char *opt, char *filename, void *none) {
        struct uwsgi_configurator *uc = uwsgi.configurators;
        while(uc) {
                if (uwsgi_endswith(filename, uc->name)) {
                        config_magic_table_fill(filename, uwsgi.magic_table);
                        uc->func(filename, uwsgi.magic_table);
                        return;
                }
                uc = uc->next;
        }

	uwsgi_log("unable to load configuration from %s\n", filename);
	exit(1);
}

#ifdef UWSGI_XML
void uwsgi_opt_load_xml(char *opt, char *filename, void *none) {
	config_magic_table_fill(filename, uwsgi.magic_table);
	uwsgi_xml_config(filename, uwsgi.wsgi_req, uwsgi.magic_table);
}
#endif

#ifdef UWSGI_YAML
void uwsgi_opt_load_yml(char *opt, char *filename, void *none) {
	config_magic_table_fill(filename, uwsgi.magic_table);
	uwsgi_yaml_config(filename, uwsgi.magic_table);
}
#endif

#ifdef UWSGI_JSON
void uwsgi_opt_load_json(char *opt, char *filename, void *none) {
	config_magic_table_fill(filename, uwsgi.magic_table);
	uwsgi_json_config(filename, uwsgi.magic_table);
}
#endif

void uwsgi_opt_add_custom_option(char *opt, char *value, void *none) {

	struct uwsgi_custom_option *uco = uwsgi.custom_options, *old_uco;

	if (!uco) {
		uwsgi.custom_options = uwsgi_malloc(sizeof(struct uwsgi_custom_option));
		uco = uwsgi.custom_options;
	}
	else {
		while (uco) {
			old_uco = uco;
			uco = uco->next;
		}

		uco = uwsgi_malloc(sizeof(struct uwsgi_custom_option));
		old_uco->next = uco;
	}

	char *copy = uwsgi_str(value);
	char *equal = strchr(copy, '=');
	if (!equal) {
		uwsgi_log("invalid %s syntax, must be newoption=template\n", value);
		exit(1);
	}
	*equal = 0;

	uco->name = copy;
	uco->value = equal + 1;
	uco->has_args = 0;
	// a little hack, we allow the user to skip the first 2 arguments (yes.. it is silly...but users tend to make silly things...)
	if (strstr(uco->value, "$1") || strstr(uco->value, "$2") || strstr(uco->value, "$3")) {
		uco->has_args = 1;
	}
	uco->next = NULL;
	build_options();
}


void uwsgi_opt_flock(char *opt, char *filename, void *none) {

	int fd = open(filename, O_RDWR);
	if (fd < 0) {
		uwsgi_error_open(filename);
		exit(1);
	}

	if (uwsgi_fcntl_is_locked(fd)) {
		uwsgi_log("uWSGI ERROR: %s is locked by another instance\n", filename);
		exit(1);
	}
}

void uwsgi_opt_flock_wait(char *opt, char *filename, void *none) {

	int fd = open(filename, O_RDWR);
	if (fd < 0) {
		uwsgi_error_open(filename);
		exit(1);
	}

	if (uwsgi_fcntl_lock(fd)) {
		exit(1);
	}
}

// report CFLAGS used for compiling the server
// use that values to build external plugins
void uwsgi_opt_cflags(char *opt, char *filename, void *foobar) {
	fprintf(stdout, "%s\n", uwsgi_get_cflags());
	exit(0);
}

char *uwsgi_get_cflags() {
	size_t len = sizeof(UWSGI_CFLAGS) -1;
        char *src = UWSGI_CFLAGS;
        char *ptr = uwsgi_malloc((len / 2) + 1);
        char *base = ptr;
        size_t i;
        unsigned int u;
        for (i = 0; i < len; i += 2) {
                sscanf(src + i, "%2x", &u);
                *ptr++ = (char) u;
        }
	*ptr ++= 0;
	return base;
}

// report uwsgi.h used for compiling the server
// use that values to build external plugins
extern char *uwsgi_dot_h;
char *uwsgi_get_dot_h() {
	char *src = uwsgi_dot_h;
	size_t len = strlen(src);
        char *ptr = uwsgi_malloc((len / 2) + 1);
        char *base = ptr;
        size_t i;
        unsigned int u;
        for (i = 0; i < len; i += 2) {
                sscanf(src + i, "%2x", &u);
                *ptr++ = (char) u;
        }
#ifdef UWSGI_ZLIB
	struct uwsgi_buffer *ub = uwsgi_zlib_decompress(base, ptr-base);
	if (!ub) {
		free(base);
		return "";
	}
	// add final null byte
	uwsgi_buffer_append(ub, "\0", 1);
	free(base);
	// base is the final blob
	base = ub->buf;
	ub->buf = NULL;
	uwsgi_buffer_destroy(ub);
#else
        // add final null byte
        *ptr = '\0';
#endif
	return base;
}
void uwsgi_opt_dot_h(char *opt, char *filename, void *foobar) {
        fprintf(stdout, "%s\n", uwsgi_get_dot_h());
        exit(0);
}

extern char *uwsgi_config_py;
char *uwsgi_get_config_py() {
        char *src = uwsgi_config_py;
        size_t len = strlen(src);
        char *ptr = uwsgi_malloc((len / 2) + 1);
        char *base = ptr;
        size_t i;
        unsigned int u;
        for (i = 0; i < len; i += 2) {
                sscanf(src + i, "%2x", &u);
                *ptr++ = (char) u;
        }
#ifdef UWSGI_ZLIB
        struct uwsgi_buffer *ub = uwsgi_zlib_decompress(base, ptr-base);
        if (!ub) {
                free(base);
                return "";
        }
        // add final null byte
        uwsgi_buffer_append(ub, "\0", 1);
        free(base);
        // base is the final blob
        base = ub->buf;
        ub->buf = NULL;
        uwsgi_buffer_destroy(ub);
#else
        // add final null byte
        *ptr = '\0';
#endif
        return base;
}

void uwsgi_opt_config_py(char *opt, char *filename, void *foobar) {
        fprintf(stdout, "%s\n", uwsgi_get_config_py());
        exit(0);
}


void uwsgi_opt_build_plugin(char *opt, char *directory, void *foobar) {
	uwsgi_build_plugin(directory);
	exit(1);
}

void uwsgi_opt_connect_and_read(char *opt, char *address, void *foobar) {

	char buf[8192];

	int fd = uwsgi_connect(address, -1, 0);
	while (fd >= 0) {
		int ret = uwsgi_waitfd(fd, -1);
		if (ret <= 0) {
			exit(0);
		}
		ssize_t len = read(fd, buf, 8192);
		if (len <= 0) {
			exit(0);
		}
		uwsgi_log("%.*s", (int) len, buf);
	}
	uwsgi_error("uwsgi_connect()");
	exit(1);
}

void uwsgi_opt_extract(char *opt, char *address, void *foobar) {

	size_t len = 0;
	char *buf;

	buf = uwsgi_open_and_read(address, &len, 0, NULL);
	if (len > 0) {
		if (write(1, buf, len) != (ssize_t) len) {
			uwsgi_error("write()");
			exit(1);
		};
	};
	exit(0);
}

void uwsgi_print_sym(char *opt, char *symbol, void *foobar) {
	char **sym = dlsym(RTLD_DEFAULT, symbol);
	if (sym) {
		uwsgi_log("%s", *sym);
		exit(0);
	}
	
	char *symbol_start = uwsgi_concat2(symbol, "_start");
	char *symbol_end = uwsgi_concat2(symbol, "_end");

	char *sym_s = dlsym(RTLD_DEFAULT, symbol_start);
	char *sym_e = dlsym(RTLD_DEFAULT, symbol_end);

	if (sym_s && sym_e) {
		uwsgi_log("%.*s", sym_e - sym_s, sym_s);
	}

	exit(0);
}

void uwsgi_update_pidfiles() {
	if (uwsgi.pidfile) {
		uwsgi_write_pidfile(uwsgi.pidfile);
	}
	if (uwsgi.pidfile2) {
		uwsgi_write_pidfile(uwsgi.pidfile2);
	}
	if (uwsgi.safe_pidfile) {
		uwsgi_write_pidfile(uwsgi.safe_pidfile);
	}
	if (uwsgi.safe_pidfile2) {
		uwsgi_write_pidfile(uwsgi.safe_pidfile2);
	}
}

void uwsgi_opt_binary_append_data(char *opt, char *value, void *none) {

	size_t size;
	char *buf = uwsgi_open_and_read(value, &size, 0, NULL);

	uint64_t file_len = size;

	if (write(1, buf, size) != (ssize_t) size) {
		uwsgi_error("uwsgi_opt_binary_append_data()/write()");
		exit(1);
	}

	if (write(1, &file_len, 8) != 8) {
		uwsgi_error("uwsgi_opt_binary_append_data()/write()");
		exit(1);
	}

	exit(0);
}
