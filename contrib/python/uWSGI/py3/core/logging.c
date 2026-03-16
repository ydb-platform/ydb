#include <contrib/python/uWSGI/py3/config.h>
#ifndef __DragonFly__
#include "uwsgi.h"
#endif
#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__DragonFly__) || defined(__OpenBSD__)
#include <sys/user.h>
#include <sys/sysctl.h>
#include <kvm.h>
#elif defined(__sun__)
/* Terrible Hack !!! */
#ifndef _LP64
#undef _FILE_OFFSET_BITS
#endif
#error #include <procfs.h>
#define _FILE_OFFSET_BITS 64
#endif

#ifdef __DragonFly__
#include "uwsgi.h"
#endif

extern struct uwsgi_server uwsgi;

//use this instead of fprintf to avoid buffering mess with udp logging
void uwsgi_log(const char *fmt, ...) {
	va_list ap;
	char logpkt[4096];
	int rlen = 0;
	int ret;

	struct timeval tv;
	char sftime[64];
	char ctime_storage[26];
	time_t now;

	if (uwsgi.logdate) {
		if (uwsgi.log_strftime) {
			now = uwsgi_now();
			rlen = strftime(sftime, 64, uwsgi.log_strftime, localtime(&now));
			memcpy(logpkt, sftime, rlen);
			memcpy(logpkt + rlen, " - ", 3);
			rlen += 3;
		}
		else {
			gettimeofday(&tv, NULL);
#if defined(__sun__) && !defined(__clang__)
			ctime_r((const time_t *) &tv.tv_sec, ctime_storage, 26);
#else
			ctime_r((const time_t *) &tv.tv_sec, ctime_storage);
#endif
			memcpy(logpkt, ctime_storage, 24);
			memcpy(logpkt + 24, " - ", 3);

			rlen = 24 + 3;
		}
	}

	va_start(ap, fmt);
	ret = vsnprintf(logpkt + rlen, 4096 - rlen, fmt, ap);
	va_end(ap);

	if (ret >= 4096) {
		char *tmp_buf = uwsgi_malloc(rlen + ret + 1);
		memcpy(tmp_buf, logpkt, rlen);
		va_start(ap, fmt);
		ret = vsnprintf(tmp_buf + rlen, ret + 1, fmt, ap);
		va_end(ap);
		rlen = write(2, tmp_buf, rlen + ret);
		free(tmp_buf);
		return;
	}

	rlen += ret;
	// do not check for errors
	rlen = write(2, logpkt, rlen);
}

void uwsgi_log_verbose(const char *fmt, ...) {

	va_list ap;
	char logpkt[4096];
	int rlen = 0;

	struct timeval tv;
	char sftime[64];
	time_t now;
	char ctime_storage[26];

	if (uwsgi.log_strftime) {
		now = uwsgi_now();
		rlen = strftime(sftime, 64, uwsgi.log_strftime, localtime(&now));
		memcpy(logpkt, sftime, rlen);
		memcpy(logpkt + rlen, " - ", 3);
		rlen += 3;
	}
	else {
		gettimeofday(&tv, NULL);
#if defined(__sun__) && !defined(__clang__)
		ctime_r((const time_t *) &tv.tv_sec, ctime_storage, 26);
#else
		ctime_r((const time_t *) &tv.tv_sec, ctime_storage);
#endif
		memcpy(logpkt, ctime_storage, 24);
		memcpy(logpkt + 24, " - ", 3);

		rlen = 24 + 3;
	}



	va_start(ap, fmt);
	rlen += vsnprintf(logpkt + rlen, 4096 - rlen, fmt, ap);
	va_end(ap);

	// do not check for errors
	rlen = write(2, logpkt, rlen);
}


/*
	commodity function mainly useful in log rotation
*/
void uwsgi_logfile_write(const char *fmt, ...) {
	va_list ap;
        char logpkt[4096];

        struct timeval tv;
        char ctime_storage[26];

	gettimeofday(&tv, NULL);
#if defined(__sun__) && !defined(__clang__)
        ctime_r((const time_t *) &tv.tv_sec, ctime_storage, 26);
#else
        ctime_r((const time_t *) &tv.tv_sec, ctime_storage);
#endif
        memcpy(logpkt, ctime_storage, 24);
        memcpy(logpkt + 24, " - ", 3);

        int rlen = 24 + 3;

        va_start(ap, fmt);
        rlen += vsnprintf(logpkt + rlen, 4096 - rlen, fmt, ap);
        va_end(ap);

        // do not check for errors
        rlen = write(uwsgi.original_log_fd, logpkt, rlen);
}



static void fix_logpipe_buf(int *fd) {
	int so_bufsize;
        socklen_t so_bufsize_len = sizeof(int);

        if (getsockopt(fd[0], SOL_SOCKET, SO_RCVBUF, &so_bufsize, &so_bufsize_len)) {
                uwsgi_error("fix_logpipe_buf()/getsockopt()");
		return;
        }

	if ((size_t)so_bufsize < uwsgi.log_master_bufsize) {
		so_bufsize = uwsgi.log_master_bufsize;
		if (setsockopt(fd[0], SOL_SOCKET, SO_RCVBUF, &so_bufsize, so_bufsize_len)) {
                        uwsgi_error("fix_logpipe_buf()/setsockopt()");
        	}
	}

	if (getsockopt(fd[1], SOL_SOCKET, SO_SNDBUF, &so_bufsize, &so_bufsize_len)) {
                uwsgi_error("fix_logpipe_buf()/getsockopt()");
                return;
        }

        if ((size_t)so_bufsize < uwsgi.log_master_bufsize) {
                so_bufsize = uwsgi.log_master_bufsize;
                if (setsockopt(fd[1], SOL_SOCKET, SO_SNDBUF, &so_bufsize, so_bufsize_len)) {
                        uwsgi_error("fix_logpipe_buf()/setsockopt()");
                }
        }
}

// create the logpipe
void create_logpipe(void) {

	if (uwsgi.log_master_stream) {
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, uwsgi.shared->worker_log_pipe)) {
			uwsgi_error("create_logpipe()/socketpair()\n");
			exit(1);
		}
	}
	else {
#if defined(SOCK_SEQPACKET) && defined(__linux__)
		if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, uwsgi.shared->worker_log_pipe)) {
#else
		if (socketpair(AF_UNIX, SOCK_DGRAM, 0, uwsgi.shared->worker_log_pipe)) {
#endif
			uwsgi_error("create_logpipe()/socketpair()\n");
			exit(1);
		}
		fix_logpipe_buf(uwsgi.shared->worker_log_pipe);
	}

	uwsgi_socket_nb(uwsgi.shared->worker_log_pipe[0]);
	uwsgi_socket_nb(uwsgi.shared->worker_log_pipe[1]);

	if (uwsgi.shared->worker_log_pipe[1] != 1) {
		if (dup2(uwsgi.shared->worker_log_pipe[1], 1) < 0) {
			uwsgi_error("dup2()");
			exit(1);
		}
	}

	if (dup2(1, 2) < 0) {
		uwsgi_error("dup2()");
		exit(1);
	}

	if (uwsgi.req_log_master) {
		if (uwsgi.log_master_req_stream) {
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, uwsgi.shared->worker_req_log_pipe)) {
				uwsgi_error("create_logpipe()/socketpair()\n");
				exit(1);
			}
		}
		else {
#if defined(SOCK_SEQPACKET) && defined(__linux__)
			if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, uwsgi.shared->worker_req_log_pipe)) {
#else
			if (socketpair(AF_UNIX, SOCK_DGRAM, 0, uwsgi.shared->worker_req_log_pipe)) {
#endif
				uwsgi_error("create_logpipe()/socketpair()\n");
				exit(1);
			}
			fix_logpipe_buf(uwsgi.shared->worker_req_log_pipe);
		}

		uwsgi_socket_nb(uwsgi.shared->worker_req_log_pipe[0]);
		uwsgi_socket_nb(uwsgi.shared->worker_req_log_pipe[1]);
		uwsgi.req_log_fd = uwsgi.shared->worker_req_log_pipe[1];
	}

}

// log to the specified file or udp address
void logto(char *logfile) {

	int fd;

	char *udp_port;
	struct sockaddr_in udp_addr;

	udp_port = strchr(logfile, ':');
	if (udp_port) {
		udp_port[0] = 0;
		if (!udp_port[1] || !logfile[0]) {
			uwsgi_log("invalid udp address\n");
			exit(1);
		}

		fd = socket(AF_INET, SOCK_DGRAM, 0);
		if (fd < 0) {
			uwsgi_error("socket()");
			exit(1);
		}

		memset(&udp_addr, 0, sizeof(struct sockaddr_in));

		udp_addr.sin_family = AF_INET;
		udp_addr.sin_port = htons(atoi(udp_port + 1));
		char *resolved = uwsgi_resolve_ip(logfile);
		if (resolved) {
			udp_addr.sin_addr.s_addr = inet_addr(resolved);
		}
		else {
			udp_addr.sin_addr.s_addr = inet_addr(logfile);
		}

		if (connect(fd, (const struct sockaddr *) &udp_addr, sizeof(struct sockaddr_in)) < 0) {
			uwsgi_error("connect()");
			exit(1);
		}
	}
	else {
		if (uwsgi.log_truncate) {
			fd = open(logfile, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP);
		}
		else {
			fd = open(logfile, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP);
		}
		if (fd < 0) {
			uwsgi_error_open(logfile);
			exit(1);
		}
		uwsgi.logfile = logfile;

		if (uwsgi.chmod_logfile_value) {
			if (chmod(uwsgi.logfile, uwsgi.chmod_logfile_value)) {
				uwsgi_error("chmod()");
			}
		}
	}


	// if the log-master is already active, just re-set the original_log_fd
	if (uwsgi.shared->worker_log_pipe[0] == -1) {
		/* stdout */
		if (fd != 1) {
			if (dup2(fd, 1) < 0) {
				uwsgi_error("dup2()");
				exit(1);
			}
			close(fd);
		}

		/* stderr */
		if (dup2(1, 2) < 0) {
			uwsgi_error("dup2()");
			exit(1);
		}

		 uwsgi.original_log_fd = 2;
	}
	else {
		uwsgi.original_log_fd = fd;
	}
}



void uwsgi_setup_log() {

	uwsgi_setup_log_encoders();

	if (uwsgi.daemonize) {
		if (uwsgi.has_emperor) {
			logto(uwsgi.daemonize);
		}
		else {
			if (!uwsgi.is_a_reload) {
				daemonize(uwsgi.daemonize);
			}
			else if (uwsgi.log_reopen) {
				logto(uwsgi.daemonize);
			}
		}
	}
	else if (uwsgi.logfile) {
		if (!uwsgi.is_a_reload || uwsgi.log_reopen) {
			logto(uwsgi.logfile);
		}
	}

}

static struct uwsgi_logger *setup_choosen_logger(struct uwsgi_string_list *usl) {
	char *id = NULL;
	char *name = usl->value;

	char *space = strchr(name, ' ');
	if (space) {
		int is_id = 1;
		int i;
		for (i = 0; i < (space - name); i++) {
			if (!isalnum((int)name[i])) {
				is_id = 0;
				break;
			}
		}
		if (is_id) {
			id = uwsgi_concat2n(name, space - name, "", 0);
			name = space + 1;
		}
	}

	char *colon = strchr(name, ':');
	if (colon) {
		*colon = 0;
	}

	struct uwsgi_logger *choosen_logger = uwsgi_get_logger(name);
	if (!choosen_logger) {
		uwsgi_log("unable to find logger %s\n", name);
		exit(1);
	}

	// make a copy of the logger
	struct uwsgi_logger *copy_of_choosen_logger = uwsgi_malloc(sizeof(struct uwsgi_logger));
	memcpy(copy_of_choosen_logger, choosen_logger, sizeof(struct uwsgi_logger));
	choosen_logger = copy_of_choosen_logger;
	choosen_logger->id = id;
	choosen_logger->next = NULL;

	if (colon) {
		choosen_logger->arg = colon + 1;
		// check for empty string
		if (*choosen_logger->arg == 0) {
			choosen_logger->arg = NULL;
		}
		*colon = ':';
	}
	return choosen_logger;
}

void uwsgi_setup_log_master(void) {

	struct uwsgi_string_list *usl = uwsgi.requested_logger;
	while (usl) {
		struct uwsgi_logger *choosen_logger = setup_choosen_logger(usl);
		uwsgi_append_logger(choosen_logger);
		usl = usl->next;
	}

	usl = uwsgi.requested_req_logger;
	while (usl) {
                struct uwsgi_logger *choosen_logger = setup_choosen_logger(usl);
                uwsgi_append_req_logger(choosen_logger);
                usl = usl->next;
        }

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	// set logger by its id
	struct uwsgi_regexp_list *url = uwsgi.log_route;
	while (url) {
		url->custom_ptr = uwsgi_get_logger_from_id(url->custom_str);
		url = url->next;
	}
	url = uwsgi.log_req_route;
	while (url) {
		url->custom_ptr = uwsgi_get_logger_from_id(url->custom_str);
		url = url->next;
	}
#endif

	uwsgi.original_log_fd = dup(1);
	create_logpipe();
}

struct uwsgi_logvar *uwsgi_logvar_get(struct wsgi_request *wsgi_req, char *key, uint8_t keylen) {
	struct uwsgi_logvar *lv = wsgi_req->logvars;
	while (lv) {
		if (!uwsgi_strncmp(key, keylen, lv->key, lv->keylen)) {
			return lv;
		}
		lv = lv->next;
	}
	return NULL;
}

void uwsgi_logvar_add(struct wsgi_request *wsgi_req, char *key, uint8_t keylen, char *val, uint8_t vallen) {

	struct uwsgi_logvar *lv = uwsgi_logvar_get(wsgi_req, key, keylen);
	if (lv) {
		memcpy(lv->val, val, vallen);
		lv->vallen = vallen;
		return;
	}

	// add a new log object

	lv = wsgi_req->logvars;
	if (lv) {
		while (lv) {
			if (!lv->next) {
				lv->next = uwsgi_malloc(sizeof(struct uwsgi_logvar));
				lv = lv->next;
				break;
			}
			lv = lv->next;
		}
	}
	else {
		lv = uwsgi_malloc(sizeof(struct uwsgi_logvar));
		wsgi_req->logvars = lv;
	}

	memcpy(lv->key, key, keylen);
	lv->keylen = keylen;
	memcpy(lv->val, val, vallen);
	lv->vallen = vallen;
	lv->next = NULL;

}

void uwsgi_check_logrotate(void) {

	int need_rotation = 0;
	int need_reopen = 0;
	off_t logsize;

	if (uwsgi.log_master) {
		logsize = lseek(uwsgi.original_log_fd, 0, SEEK_CUR);
	}
	else {
		logsize = lseek(2, 0, SEEK_CUR);
	}
	if (logsize < 0) {
		uwsgi_error("uwsgi_check_logrotate()/lseek()");
		return;
	}
	uwsgi.shared->logsize = logsize;

	if (uwsgi.log_maxsize > 0 && (uint64_t) uwsgi.shared->logsize > uwsgi.log_maxsize) {
		need_rotation = 1;
	}

	if (uwsgi_check_touches(uwsgi.touch_logrotate)) {
		need_rotation = 1;
	}

	if (uwsgi_check_touches(uwsgi.touch_logreopen)) {
		need_reopen = 1;
	}

	if (need_rotation) {
		uwsgi_log_rotate();
	}
	else if (need_reopen) {
		uwsgi_log_reopen();
	}
}

void uwsgi_log_do_rotate(char *logfile, char *rotatedfile, off_t logsize, int log_fd) {
	int need_free = 0;
	char *rot_name = rotatedfile;

	if (rot_name == NULL) {
		char *ts_str = uwsgi_num2str((int) uwsgi_now());
		rot_name = uwsgi_concat3(logfile, ".", ts_str);
		free(ts_str);
		need_free = 1;
	}
	// this will be rawly written to the logfile
	uwsgi_logfile_write("logsize: %llu, triggering rotation to %s...\n", (unsigned long long) logsize, rot_name);
	if (rename(logfile, rot_name) == 0) {
		// reopen logfile and dup'it, on dup2 error, exit(1)
		int fd = open(logfile, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP);
		if (fd < 0) {
			// this will be written to the original file
			uwsgi_error_open(logfile);
			exit(1);
		}
		else {
			if (dup2(fd, log_fd) < 0) {
				// this could be lost :(
				uwsgi_error("uwsgi_log_do_rotate()/dup2()");
				exit(1);
			}
			close(fd);
		}
	}
	else {
		uwsgi_error("unable to rotate log: rename()");
	}
	if (need_free)
		free(rot_name);
}

void uwsgi_log_rotate() {
	if (!uwsgi.logfile)
		return;
	uwsgi_log_do_rotate(uwsgi.logfile, uwsgi.log_backupname, uwsgi.shared->logsize, uwsgi.original_log_fd);
}

void uwsgi_log_reopen() {
	char message[1024];
	if (!uwsgi.logfile) return;
	int ret = snprintf(message, 1024, "[%d] logsize: %llu, triggering log-reopen...\n", (int) uwsgi_now(), (unsigned long long) uwsgi.shared->logsize);
        if (ret > 0 && ret < 1024) {
                        if (write(uwsgi.original_log_fd, message, ret) != ret) {
                                // very probably this will never be printed
                                uwsgi_error("write()");
                        }
                }

                // reopen logfile;
                close(uwsgi.original_log_fd);
                uwsgi.original_log_fd = open(uwsgi.logfile, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP);
                if (uwsgi.original_log_fd < 0) {
                        uwsgi_error_open(uwsgi.logfile);
                        grace_them_all(0);
			return;
                }
                ret = snprintf(message, 1024, "[%d] %s reopened.\n", (int) uwsgi_now(), uwsgi.logfile);
                if (ret > 0 && ret < 1024) {
                        if (write(uwsgi.original_log_fd, message, ret) != ret) {
                                // very probably this will never be printed
                                uwsgi_error("write()");
                        }
                }
                uwsgi.shared->logsize = lseek(uwsgi.original_log_fd, 0, SEEK_CUR);
}


void log_request(struct wsgi_request *wsgi_req) {

	int log_it = uwsgi.logging_options.enabled;

	if (wsgi_req->do_not_log)
		return;

	if (wsgi_req->log_this) {
		goto logit;
	}

/* conditional logging */
	if (uwsgi.logging_options.zero && wsgi_req->response_size == 0) {
		goto logit;
	}
	if (uwsgi.logging_options.slow && (uint32_t) wsgi_req_time >= uwsgi.logging_options.slow) {
		goto logit;
	}
	if (uwsgi.logging_options._4xx && (wsgi_req->status >= 400 && wsgi_req->status <= 499)) {
		goto logit;
	}
	if (uwsgi.logging_options._5xx && (wsgi_req->status >= 500 && wsgi_req->status <= 599)) {
		goto logit;
	}
	if (uwsgi.logging_options.big && (wsgi_req->response_size >= uwsgi.logging_options.big)) {
		goto logit;
	}
	if (uwsgi.logging_options.sendfile && wsgi_req->via == UWSGI_VIA_SENDFILE) {
		goto logit;
	}
	if (uwsgi.logging_options.ioerror && wsgi_req->read_errors > 0 && wsgi_req->write_errors > 0) {
		goto logit;
	}

	if (!log_it)
		return;

logit:

	uwsgi.logit(wsgi_req);
}

void uwsgi_logit_simple(struct wsgi_request *wsgi_req) {

	// optimize this (please)
	char time_request[26];
	int rlen;
	int app_req = -1;
	char *msg2 = " ";
	char *via = msg2;

	char mempkt[4096];
	char logpkt[4096];

	struct iovec logvec[4];
	int logvecpos = 0;

	const char *msecs = "msecs";
	const char *micros = "micros";

	char *tsize = (char *) msecs;

	char *msg1 = " via sendfile() ";
	char *msg3 = " via route() ";
	char *msg4 = " via offload() ";

	struct uwsgi_app *wi;

	if (wsgi_req->app_id >= 0) {
		wi = &uwsgi_apps[wsgi_req->app_id];
		if (wi->requests > 0) {
			app_req = wi->requests;
		}
	}

	// mark requests via (sendfile, route, offload...)
	switch(wsgi_req->via) {
		case UWSGI_VIA_SENDFILE:
			via = msg1;
			break;
		case UWSGI_VIA_ROUTE:
			via = msg3;
			break;
		case UWSGI_VIA_OFFLOAD:
			via = msg4;
			break;
		default:
			break;
	}

#if defined(__sun__) && !defined(__clang__)
	ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, time_request, 26);
#else
	ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, time_request);
#endif

	uint64_t rt = 0;
	// avoid overflow on clock instability (#1489)
	if (wsgi_req->end_of_request > wsgi_req->start_of_request)
		rt = wsgi_req->end_of_request - wsgi_req->start_of_request;

	if (uwsgi.log_micros) {
		tsize = (char *) micros;
	}
	else {
		rt /= 1000;
	}

	if (uwsgi.vhost) {
		logvec[logvecpos].iov_base = wsgi_req->host;
		logvec[logvecpos].iov_len = wsgi_req->host_len;
		logvecpos++;

		logvec[logvecpos].iov_base = " ";
		logvec[logvecpos].iov_len = 1;
		logvecpos++;
	}

	if (uwsgi.logging_options.memory_report == 1) {
		rlen = snprintf(mempkt, 4096, "{address space usage: %llu bytes/%lluMB} {rss usage: %llu bytes/%lluMB} ", (unsigned long long) uwsgi.workers[uwsgi.mywid].vsz_size, (unsigned long long) uwsgi.workers[uwsgi.mywid].vsz_size / 1024 / 1024,
			(unsigned long long) uwsgi.workers[uwsgi.mywid].rss_size, (unsigned long long) uwsgi.workers[uwsgi.mywid].rss_size / 1024 / 1024);
		logvec[logvecpos].iov_base = mempkt;
		logvec[logvecpos].iov_len = rlen;
		logvecpos++;

	}

	char *remote_user = wsgi_req->remote_user == NULL ? "" : wsgi_req->remote_user;
	rlen = snprintf(logpkt, 4096, "[pid: %d|app: %d|req: %d/%llu] %.*s (%.*s) {%d vars in %d bytes} [%.*s] %.*s %.*s => generated %llu bytes in %llu %s%s(%.*s %d) %d headers in %llu bytes (%d switches on core %d)\n", (int) uwsgi.mypid, wsgi_req->app_id, app_req, (unsigned long long) uwsgi.workers[0].requests, wsgi_req->remote_addr_len, wsgi_req->remote_addr, wsgi_req->remote_user_len, remote_user, wsgi_req->var_cnt, wsgi_req->uh->pktsize,
			24, time_request, wsgi_req->method_len, wsgi_req->method, wsgi_req->uri_len, wsgi_req->uri, (unsigned long long) wsgi_req->response_size, (unsigned long long) rt, tsize, via, wsgi_req->protocol_len, wsgi_req->protocol, wsgi_req->status, wsgi_req->header_cnt, (unsigned long long) wsgi_req->headers_size, wsgi_req->switches, wsgi_req->async_id);

	// not enough space for logging the request, just log a (safe) minimal message
	if (rlen > 4096) {
		rlen = snprintf(logpkt, 4096, "[pid: %d|app: %d|req: %d/%llu] 0.0.0.0 () {%d vars in %d bytes} [%.*s] - - => generated %llu bytes in %llu %s%s(- %d) %d headers in %llu bytes (%d switches on core %d)\n", (int) uwsgi.mypid, wsgi_req->app_id, app_req, (unsigned long long) uwsgi.workers[0].requests, wsgi_req->var_cnt, wsgi_req->uh->pktsize,
		24, time_request, (unsigned long long) wsgi_req->response_size, (unsigned long long) rt, tsize, via, wsgi_req->status, wsgi_req->header_cnt, (unsigned long long) wsgi_req->headers_size, wsgi_req->switches, wsgi_req->async_id);
		// argh, last resort, truncate it
		if (rlen > 4096) {
			rlen = 4096;
		}
	}

	logvec[logvecpos].iov_base = logpkt;
	logvec[logvecpos].iov_len = rlen;

	// do not check for errors
	rlen = writev(uwsgi.req_log_fd, logvec, logvecpos + 1);
}

void get_memusage(uint64_t * rss, uint64_t * vsz) {

#ifdef UNBIT
	uint64_t ret[2];
	ret[0] = 0; ret[1] = 0;
	syscall(358, ret);
	*vsz = ret[0];
	*rss = ret[1] * uwsgi.page_size;
#elif defined(__linux__)
	FILE *procfile;
	int i;
	procfile = fopen("/proc/self/stat", "r");
	if (procfile) {
		i = fscanf(procfile, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %llu %llu", (unsigned long long *) vsz, (unsigned long long *) rss);
		if (i != 2) {
			uwsgi_log("warning: invalid record in /proc/self/stat\n");
		} else {
			*rss = *rss * uwsgi.page_size;
		}
		fclose(procfile);
	}
#elif defined(__CYGWIN__)
	// same as Linux but rss is not in pages...
	FILE *procfile;
        int i;
        procfile = fopen("/proc/self/stat", "r");
        if (procfile) {
                i = fscanf(procfile, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %llu %llu", (unsigned long long *) vsz, (unsigned long long *) rss);
                if (i != 2) {
                        uwsgi_log("warning: invalid record in /proc/self/stat\n");
                }
                fclose(procfile);
        }
#elif defined (__sun__)
	psinfo_t info;
	int procfd;

	procfd = open("/proc/self/psinfo", O_RDONLY);
	if (procfd >= 0) {
		if (read(procfd, (char *) &info, sizeof(info)) > 0) {
			*rss = (uint64_t) info.pr_rssize * 1024;
			*vsz = (uint64_t) info.pr_size * 1024;
		}
		close(procfd);
	}

#elif defined(__APPLE__)
	/* darwin documentation says that the value are in pages, but they are bytes !!! */
	struct task_basic_info t_info;
	mach_msg_type_number_t t_size = sizeof(struct task_basic_info);

	if (task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t) & t_info, &t_size) == KERN_SUCCESS) {
		*rss = t_info.resident_size;
		*vsz = t_info.virtual_size;
	}
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__DragonFly__) || defined(__OpenBSD__)
	kvm_t *kv;
	int cnt;

#if defined(__FreeBSD__)
	kv = kvm_open(NULL, "/dev/null", NULL, O_RDONLY, NULL);
#elif defined(__NetBSD__) || defined(__OpenBSD__)
	kv = kvm_open(NULL, NULL, NULL, KVM_NO_FILES, NULL);
#else
	kv = kvm_open(NULL, NULL, NULL, O_RDONLY, NULL);
#endif
	if (kv) {
#if defined(__FreeBSD__) || defined(__DragonFly__)

		struct kinfo_proc *kproc;
		kproc = kvm_getprocs(kv, KERN_PROC_PID, uwsgi.mypid, &cnt);
		if (kproc && cnt > 0) {
#if defined(__FreeBSD__)
			*vsz = kproc->ki_size;
			*rss = kproc->ki_rssize * uwsgi.page_size;
#elif defined(__DragonFly__)
			*vsz = kproc->kp_vm_map_size;
			*rss = kproc->kp_vm_rssize * uwsgi.page_size;
#endif
		}
#elif defined(UWSGI_NEW_OPENBSD)
		struct kinfo_proc *kproc;
		kproc = kvm_getprocs(kv, KERN_PROC_PID, uwsgi.mypid, sizeof(struct kinfo_proc), &cnt);
		if (kproc && cnt > 0) {
			*vsz = (kproc->p_vm_dsize + kproc->p_vm_ssize + kproc->p_vm_tsize) * uwsgi.page_size;
			*rss = kproc->p_vm_rssize * uwsgi.page_size;
		}
#elif defined(__NetBSD__) || defined(__OpenBSD__)
		struct kinfo_proc2 *kproc2;

		kproc2 = kvm_getproc2(kv, KERN_PROC_PID, uwsgi.mypid, sizeof(struct kinfo_proc2), &cnt);
		if (kproc2 && cnt > 0) {
#ifdef __OpenBSD__
			*vsz = (kproc2->p_vm_dsize + kproc2->p_vm_ssize + kproc2->p_vm_tsize) * uwsgi.page_size;
#else
			*vsz = kproc2->p_vm_msize * uwsgi.page_size;
#endif
			*rss = kproc2->p_vm_rssize * uwsgi.page_size;
		}
#endif

		kvm_close(kv);
	}
#elif defined(__HAIKU__)
	area_info ai;
	int32 cookie;

	*vsz = 0;
	*rss = 0;
	while (get_next_area_info(0, &cookie, &ai) == B_OK) {
		*vsz += ai.ram_size;
		if ((ai.protection & B_WRITE_AREA) != 0) {
			*rss += ai.ram_size;
		}
	}
#endif

}

void uwsgi_register_logger(char *name, ssize_t(*func) (struct uwsgi_logger *, char *, size_t)) {

	struct uwsgi_logger *ul = uwsgi.loggers, *old_ul;

	if (!ul) {
		uwsgi.loggers = uwsgi_malloc(sizeof(struct uwsgi_logger));
		ul = uwsgi.loggers;
	}
	else {
		while (ul) {
			old_ul = ul;
			ul = ul->next;
		}

		ul = uwsgi_malloc(sizeof(struct uwsgi_logger));
		old_ul->next = ul;
	}

	ul->name = name;
	ul->func = func;
	ul->next = NULL;
	ul->configured = 0;
	ul->fd = -1;
	ul->data = NULL;
	ul->buf = NULL;


#ifdef UWSGI_DEBUG
	uwsgi_log("[uwsgi-logger] registered \"%s\"\n", ul->name);
#endif
}

void uwsgi_append_logger(struct uwsgi_logger *ul) {

	if (!uwsgi.choosen_logger) {
		uwsgi.choosen_logger = ul;
		return;
	}

	struct uwsgi_logger *ucl = uwsgi.choosen_logger;
	while (ucl) {
		if (!ucl->next) {
			ucl->next = ul;
			return;
		}
		ucl = ucl->next;
	}
}

void uwsgi_append_req_logger(struct uwsgi_logger *ul) {

        if (!uwsgi.choosen_req_logger) {
                uwsgi.choosen_req_logger = ul;
                return;
        }

        struct uwsgi_logger *ucl = uwsgi.choosen_req_logger;
        while (ucl) {
                if (!ucl->next) {
                        ucl->next = ul;
                        return;
                }
                ucl = ucl->next;
        }
}


struct uwsgi_logger *uwsgi_get_logger(char *name) {
	struct uwsgi_logger *ul = uwsgi.loggers;

	while (ul) {
		if (!strcmp(ul->name, name)) {
			return ul;
		}
		ul = ul->next;
	}

	return NULL;
}

struct uwsgi_logger *uwsgi_get_logger_from_id(char *id) {
	struct uwsgi_logger *ul = uwsgi.choosen_logger;

	while (ul) {
		if (ul->id && !strcmp(ul->id, id)) {
			return ul;
		}
		ul = ul->next;
	}

	return NULL;
}


void uwsgi_logit_lf(struct wsgi_request *wsgi_req) {
	struct uwsgi_logchunk *logchunk = uwsgi.logchunks;
	ssize_t rlen = 0;
	const char *empty_var = "-";
	while (logchunk) {
		int pos = logchunk->vec;
		// raw string
		if (logchunk->type == 0) {
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = logchunk->ptr;
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = logchunk->len;
		}
		// offsetof
		else if (logchunk->type == 1) {
			char **var = (char **) (((char *) wsgi_req) + logchunk->pos);
			uint16_t *varlen = (uint16_t *) (((char *) wsgi_req) + logchunk->pos_len);
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = *var;
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = *varlen;
		}
		// logvar
		else if (logchunk->type == 2) {
			struct uwsgi_logvar *lv = uwsgi_logvar_get(wsgi_req, logchunk->ptr, logchunk->len);
			if (lv) {
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = lv->val;
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = lv->vallen;
			}
			else {
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = NULL;
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = 0;
			}
		}
		// func
		else if (logchunk->type == 3) {
			rlen = logchunk->func(wsgi_req, (char **) &uwsgi.logvectors[wsgi_req->async_id][pos].iov_base);
			if (rlen > 0) {
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = rlen;
			}
			else {
				uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = 0;
			}
		}
		// var
		else if (logchunk->type == 5) {
			uint16_t value_len = 0;
			char *value = uwsgi_get_var(wsgi_req, logchunk->ptr, logchunk->len, &value_len);
			// could be NULL
                        uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = value;
                        uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = (size_t) value_len;
                }
		// metric
		else if (logchunk->type == 4) {
			int64_t metric = uwsgi_metric_get(logchunk->ptr, NULL);
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = uwsgi_64bit2str(metric);
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = strlen(uwsgi.logvectors[wsgi_req->async_id][pos].iov_base);
		}

		if (uwsgi.logvectors[wsgi_req->async_id][pos].iov_len == 0 && logchunk->type != 0) {
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_base = (char *) empty_var;
			uwsgi.logvectors[wsgi_req->async_id][pos].iov_len = 1;
		}
		logchunk = logchunk->next;
	}

	// do not check for errors
	rlen = writev(uwsgi.req_log_fd, uwsgi.logvectors[wsgi_req->async_id], uwsgi.logformat_vectors);

	// free allocated memory
	logchunk = uwsgi.logchunks;
	while (logchunk) {
		if (logchunk->free) {
			if (uwsgi.logvectors[wsgi_req->async_id][logchunk->vec].iov_len > 0) {
				if (uwsgi.logvectors[wsgi_req->async_id][logchunk->vec].iov_base != empty_var) {
					free(uwsgi.logvectors[wsgi_req->async_id][logchunk->vec].iov_base);
				}
			}
		}
		logchunk = logchunk->next;
	}
}

void uwsgi_build_log_format(char *format) {
	int state = 0;
	char *ptr = format;
	char *current = ptr;
	char *logvar = NULL;
	// get the number of required iovec
	while (*ptr) {
		if (*ptr == '%') {
			if (state == 0) {
				state = 1;
			}
		}
		// start of the variable
		else if (*ptr == '(') {
			if (state == 1) {
				state = 2;
			}
		}
		// end of the variable
		else if (*ptr == ')') {
			if (logvar) {
				uwsgi_add_logchunk(1, uwsgi.logformat_vectors, logvar, ptr - logvar);
				uwsgi.logformat_vectors++;
				state = 0;
				logvar = NULL;
				current = ptr + 1;
			}
		}
		else {
			if (state == 2) {
				uwsgi_add_logchunk(0, uwsgi.logformat_vectors, current, (ptr - current) - 2);
				uwsgi.logformat_vectors++;
				logvar = ptr;
			}
			state = 0;
		}
		ptr++;
	}

	if (ptr - current > 0) {
		uwsgi_add_logchunk(0, uwsgi.logformat_vectors, current, ptr - current);
		uwsgi.logformat_vectors++;
	}

	// +1 for "\n"

	uwsgi.logformat_vectors++;

}

static ssize_t uwsgi_lf_status(struct wsgi_request *wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->status);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_rsize(struct wsgi_request *wsgi_req, char **buf) {
	*buf = uwsgi_size2str(wsgi_req->response_size);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_hsize(struct wsgi_request *wsgi_req, char **buf) {
	*buf = uwsgi_size2str(wsgi_req->headers_size);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_size(struct wsgi_request *wsgi_req, char **buf) {
	*buf = uwsgi_size2str(wsgi_req->headers_size+wsgi_req->response_size);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_cl(struct wsgi_request *wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->post_cl);
	return strlen(*buf);
}


static ssize_t uwsgi_lf_epoch(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi_now());
	return strlen(*buf);
}

static ssize_t uwsgi_lf_ctime(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_malloc(26);
#if defined(__sun__) && !defined(__clang__)
	ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, *buf, 26);
#else
	ctime_r((const time_t *) &wsgi_req->start_of_request_in_sec, *buf);
#endif
	return 24;
}

static ssize_t uwsgi_lf_time(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->start_of_request / 1000000);
	return strlen(*buf);
}


static ssize_t uwsgi_lf_ltime(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_malloc(64);
	time_t now = wsgi_req->start_of_request / 1000000;
	size_t ret = strftime(*buf, 64, "%d/%b/%Y:%H:%M:%S %z", localtime(&now));
	if (ret == 0) {
		*buf[0] = 0;
		return 0;
	}
	return ret;
}

static ssize_t uwsgi_lf_ftime(struct wsgi_request * wsgi_req, char **buf) {
	if (!uwsgi.logformat_strftime || !uwsgi.log_strftime) {
		return uwsgi_lf_ltime(wsgi_req, buf);
	}
	*buf = uwsgi_malloc(64);
	time_t now = wsgi_req->start_of_request / 1000000;
	size_t ret = strftime(*buf, 64, uwsgi.log_strftime, localtime(&now));
	if (ret == 0) {
		*buf[0] = 0;
		return 0;
	}
	return ret;
}

static ssize_t uwsgi_lf_tmsecs(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_64bit2str(wsgi_req->start_of_request / (int64_t) 1000);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_tmicros(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_64bit2str(wsgi_req->start_of_request);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_micros(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->end_of_request - wsgi_req->start_of_request);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_msecs(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str((wsgi_req->end_of_request - wsgi_req->start_of_request) / 1000);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_secs(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_float2str((wsgi_req->end_of_request - wsgi_req->start_of_request) / 1000000.0);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_pid(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.mypid);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_wid(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.mywid);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_switches(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->switches);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_vars(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->var_cnt);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_core(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->async_id);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_vsz(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.workers[uwsgi.mywid].vsz_size);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_rss(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.workers[uwsgi.mywid].rss_size);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_vszM(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.workers[uwsgi.mywid].vsz_size / 1024 / 1024);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_rssM(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(uwsgi.workers[uwsgi.mywid].rss_size / 1024 / 1024);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_pktsize(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->uh->pktsize);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_modifier1(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->uh->modifier1);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_modifier2(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->uh->modifier2);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_headers(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str(wsgi_req->header_cnt);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_werr(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str((int) wsgi_req->write_errors);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_rerr(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str((int) wsgi_req->read_errors);
	return strlen(*buf);
}

static ssize_t uwsgi_lf_ioerr(struct wsgi_request * wsgi_req, char **buf) {
	*buf = uwsgi_num2str((int) (wsgi_req->write_errors + wsgi_req->read_errors));
	return strlen(*buf);
}

struct uwsgi_logchunk *uwsgi_register_logchunk(char *name, ssize_t (*func)(struct wsgi_request *, char **), int need_free) {
	struct uwsgi_logchunk *old_logchunk = NULL, *logchunk = uwsgi.registered_logchunks;
	while(logchunk) {
		if (!strcmp(logchunk->name, name)) goto found;
		old_logchunk = logchunk;
		logchunk = logchunk->next;
	}
	logchunk = uwsgi_calloc(sizeof(struct uwsgi_logchunk));
	logchunk->name = name;
	if (old_logchunk) {
		old_logchunk->next = logchunk;
	}
	else {
		uwsgi.registered_logchunks = logchunk;
	}
found:
	logchunk->func = func;
	logchunk->free = need_free;
	logchunk->type = 3;
	return logchunk;
}

struct uwsgi_logchunk *uwsgi_get_logchunk_by_name(char *name, size_t name_len) {
	struct uwsgi_logchunk *logchunk = uwsgi.registered_logchunks;
	while(logchunk) {
		if (!uwsgi_strncmp(name, name_len, logchunk->name, strlen(logchunk->name))) {
			return logchunk;
		}
		logchunk = logchunk->next;
	}
	return NULL;
}

void uwsgi_add_logchunk(int variable, int pos, char *ptr, size_t len) {

	struct uwsgi_logchunk *logchunk = uwsgi.logchunks;

	if (logchunk) {
		while (logchunk) {
			if (!logchunk->next) {
				logchunk->next = uwsgi_calloc(sizeof(struct uwsgi_logchunk));
				logchunk = logchunk->next;
				break;
			}
			logchunk = logchunk->next;
		}
	}
	else {
		uwsgi.logchunks = uwsgi_calloc(sizeof(struct uwsgi_logchunk));
		logchunk = uwsgi.logchunks;
	}

	/*
	   0 -> raw text
	   1 -> offsetof variable
	   2 -> logvar
	   3 -> func
	   4 -> metric
	   5 -> request variable
	 */

	logchunk->type = variable;
	logchunk->vec = pos;
	// normal text
	logchunk->ptr = ptr;
	logchunk->len = len;
	// variable
	if (variable) {
		struct uwsgi_logchunk *rlc = uwsgi_get_logchunk_by_name(ptr, len);
		if (rlc) {
			if (rlc->type == 1) {
				logchunk->pos = rlc->pos;
				logchunk->pos_len = rlc->pos_len;
			}
			else if (rlc->type == 3) {
				logchunk->type = 3;
				logchunk->func = rlc->func;
				logchunk->free = rlc->free;
			}
		}
		// var
		else if (!uwsgi_starts_with(ptr, len, "var.", 4)) {
			logchunk->type = 5;
			logchunk->ptr = ptr+4;
			logchunk->len = len-4;
			logchunk->free = 0;
		}
		// metric
		else if (!uwsgi_starts_with(ptr, len, "metric.", 7)) {
			logchunk->type = 4;
			logchunk->ptr = uwsgi_concat2n(ptr+7, len - 7, "", 0);
			logchunk->free = 1;
		}
		// logvar
		else {
			logchunk->type = 2;
		}
	}
}

static void uwsgi_log_func_do(struct uwsgi_string_list *encoders, struct uwsgi_logger *ul, char *msg, size_t len) {
	struct uwsgi_string_list *usl = encoders;
	// note: msg must not be freed !!!
	char *new_msg = msg;
	size_t new_msg_len = len;
	while(usl) {
		struct uwsgi_log_encoder *ule = (struct uwsgi_log_encoder *) usl->custom_ptr;
		if (ule->use_for) {
			if (ul && ul->id) {
				if (strcmp(ule->use_for, ul->id)) {
					goto next;
				}
			}
			else {
				goto next;
			}
		}
		size_t rlen = 0;
		char *buf = ule->func(ule, new_msg, new_msg_len, &rlen);
		if (new_msg != msg) {
                	free(new_msg);
        	}
		new_msg = buf;
		new_msg_len = rlen;
next:
		usl = usl->next;
	}
	if (ul) {
		ul->func(ul, new_msg, new_msg_len);
	}
	else {
		new_msg_len = (size_t) write(uwsgi.original_log_fd, new_msg, new_msg_len);
	}
	if (new_msg != msg) {
		free(new_msg);
	}

}

int uwsgi_master_log(void) {

        ssize_t rlen = read(uwsgi.shared->worker_log_pipe[0], uwsgi.log_master_buf, uwsgi.log_master_bufsize);
        if (rlen > 0) {
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
                uwsgi_alarm_log_check(uwsgi.log_master_buf, rlen);
                struct uwsgi_regexp_list *url = uwsgi.log_drain_rules;
                while (url) {
                        if (uwsgi_regexp_match(url->pattern, uwsgi.log_master_buf, rlen) >= 0) {
                                return 0;
                        }
                        url = url->next;
                }
                if (uwsgi.log_filter_rules) {
                        int show = 0;
                        url = uwsgi.log_filter_rules;
                        while (url) {
                                if (uwsgi_regexp_match(url->pattern, uwsgi.log_master_buf, rlen) >= 0) {
                                        show = 1;
                                        break;
                                }
                                url = url->next;
                        }
                        if (!show)
                                return 0;
                }

                url = uwsgi.log_route;
                int finish = 0;
                while (url) {
                        if (uwsgi_regexp_match(url->pattern, uwsgi.log_master_buf, rlen) >= 0) {
                                struct uwsgi_logger *ul_route = (struct uwsgi_logger *) url->custom_ptr;
                                if (ul_route) {
					uwsgi_log_func_do(uwsgi.requested_log_encoders, ul_route, uwsgi.log_master_buf, rlen);
                                        finish = 1;
                                }
                        }
                        url = url->next;
                }
                if (finish)
                        return 0;
#endif

                int raw_log = 1;

                struct uwsgi_logger *ul = uwsgi.choosen_logger;
                while (ul) {
                        // check for named logger
                        if (ul->id) {
                                goto next;
                        }
                        uwsgi_log_func_do(uwsgi.requested_log_encoders, ul, uwsgi.log_master_buf, rlen);
                        raw_log = 0;
next:
                        ul = ul->next;
                }

                if (raw_log) {
			uwsgi_log_func_do(uwsgi.requested_log_encoders, NULL, uwsgi.log_master_buf, rlen);
                }
                return 0;
        }

        return -1;
}

int uwsgi_master_req_log(void) {

        ssize_t rlen = read(uwsgi.shared->worker_req_log_pipe[0], uwsgi.log_master_buf, uwsgi.log_master_bufsize);
        if (rlen > 0) {
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
                struct uwsgi_regexp_list *url = uwsgi.log_req_route;
                int finish = 0;
                while (url) {
                        if (uwsgi_regexp_match(url->pattern, uwsgi.log_master_buf, rlen) >= 0) {
                                struct uwsgi_logger *ul_route = (struct uwsgi_logger *) url->custom_ptr;
                                if (ul_route) {
                                        uwsgi_log_func_do(uwsgi.requested_log_req_encoders, ul_route, uwsgi.log_master_buf, rlen);
                                        finish = 1;
                                }
                        }
                        url = url->next;
                }
                if (finish)
                        return 0;
#endif

                int raw_log = 1;

                struct uwsgi_logger *ul = uwsgi.choosen_req_logger;
                while (ul) {
                        // check for named logger
                        if (ul->id) {
                                goto next;
                        }
                        uwsgi_log_func_do(uwsgi.requested_log_req_encoders, ul, uwsgi.log_master_buf, rlen);
                        raw_log = 0;
next:
                        ul = ul->next;
                }

                if (raw_log) {
			uwsgi_log_func_do(uwsgi.requested_log_req_encoders, NULL, uwsgi.log_master_buf, rlen);
                }
                return 0;
        }

        return -1;
}

static void *logger_thread_loop(void *noarg) {
        struct pollfd logpoll[2];

        // block all signals
        sigset_t smask;
        sigfillset(&smask);
        pthread_sigmask(SIG_BLOCK, &smask, NULL);

        logpoll[0].events = POLLIN;
        logpoll[0].fd = uwsgi.shared->worker_log_pipe[0];

        int logpolls = 1;

        if (uwsgi.req_log_master) {
                logpoll[1].events = POLLIN;
                logpoll[1].fd = uwsgi.shared->worker_req_log_pipe[0];
		logpolls++;
        }


        for (;;) {
                int ret = poll(logpoll, logpolls, -1);
                if (ret > 0) {
                        if (logpoll[0].revents & POLLIN) {
                                pthread_mutex_lock(&uwsgi.threaded_logger_lock);
                                uwsgi_master_log();
                                pthread_mutex_unlock(&uwsgi.threaded_logger_lock);
                        }
                        else if (logpolls > 1 && logpoll[1].revents & POLLIN) {
                                pthread_mutex_lock(&uwsgi.threaded_logger_lock);
                                uwsgi_master_req_log();
                                pthread_mutex_unlock(&uwsgi.threaded_logger_lock);
                        }

                }
        }

        return NULL;
}



void uwsgi_threaded_logger_spawn() {
	pthread_t logger_thread;

	if (pthread_create(&logger_thread, NULL, logger_thread_loop, NULL)) {
        	uwsgi_error("pthread_create()");
                uwsgi_log("falling back to non-threaded logger...\n");
                event_queue_add_fd_read(uwsgi.master_queue, uwsgi.shared->worker_log_pipe[0]);
                if (uwsgi.req_log_master) {
                	event_queue_add_fd_read(uwsgi.master_queue, uwsgi.shared->worker_req_log_pipe[0]);
                }
                uwsgi.threaded_logger = 0;
	}
}

void uwsgi_register_log_encoder(char *name, char *(*func)(struct uwsgi_log_encoder *, char *, size_t, size_t *)) {
	struct uwsgi_log_encoder *old_ule = NULL, *ule = uwsgi.log_encoders;

	while(ule) {
		if (!strcmp(ule->name, name)) {
			ule->func = func;
			return;
		}
		old_ule = ule;
		ule = ule->next;
	}

	ule = uwsgi_calloc(sizeof(struct uwsgi_log_encoder));
	ule->name = name;
	ule->func = func;

	if (old_ule) {
		old_ule->next = ule;
	}
	else {
		uwsgi.log_encoders = ule;
	}
}

struct uwsgi_log_encoder *uwsgi_log_encoder_by_name(char *name) {
	struct uwsgi_log_encoder *ule = uwsgi.log_encoders;
	while(ule) {
		if (!strcmp(name, ule->name)) return ule;
		ule = ule->next;
	}
	return NULL;
}

void uwsgi_setup_log_encoders() {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, uwsgi.requested_log_encoders) {
		char *space = strchr(usl->value, ' ');
		if (space) *space = 0;
		char *use_for = strchr(usl->value, ':');
		if (use_for) *use_for = 0;
		struct uwsgi_log_encoder *ule = uwsgi_log_encoder_by_name(usl->value);
		if (!ule) {
			uwsgi_log("log encoder \"%s\" not found\n", usl->value);
			exit(1);
		}
		struct uwsgi_log_encoder *ule2 = uwsgi_malloc(sizeof(struct uwsgi_log_encoder));
		memcpy(ule2, ule, sizeof(struct uwsgi_log_encoder));
		if (use_for) {
			ule2->use_for = uwsgi_str(use_for+1);
			*use_for = ':';
		}
		// we use a copy
		if (space) {
			*space = ' ';
			ule2->args = uwsgi_str(space+1);
		}
		else {
			ule2->args = uwsgi_str("");
		}

		usl->custom_ptr = ule2;
		uwsgi_log("[log-encoder] registered %s\n", usl->value);
	}

	uwsgi_foreach(usl, uwsgi.requested_log_req_encoders) {
                char *space = strchr(usl->value, ' ');
                if (space) *space = 0;
		char *use_for = strchr(usl->value, ':');
		if (use_for) *use_for = 0;
                struct uwsgi_log_encoder *ule = uwsgi_log_encoder_by_name(usl->value);
                if (!ule) {
                        uwsgi_log("log encoder \"%s\" not found\n", usl->value);
                        exit(1);
                }
		struct uwsgi_log_encoder *ule2 = uwsgi_malloc(sizeof(struct uwsgi_log_encoder));
                memcpy(ule2, ule, sizeof(struct uwsgi_log_encoder));
                if (use_for) {
			ule2->use_for = uwsgi_str(use_for+1);
                        *use_for = ':';
                }
                // we use a copy
                if (space) {
                        *space = ' ';
                        ule2->args = uwsgi_str(space+1);
                }
                else {
                        ule2->args = uwsgi_str("");
                }
                usl->custom_ptr = ule2;
		uwsgi_log("[log-req-encoder] registered %s\n", usl->value);
        }
}

#ifdef UWSGI_ZLIB
static char *uwsgi_log_encoder_gzip(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {
	struct uwsgi_buffer *ub = uwsgi_gzip(msg, len);
	if (!ub) return NULL;
	*rlen = ub->pos;
	// avoid destruction
	char *buf = ub->buf;
	ub->buf = NULL;
	uwsgi_buffer_destroy(ub);
	return buf;
}

static char *uwsgi_log_encoder_compress(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {
	size_t c_len = (size_t) compressBound(len);
	uLongf destLen = c_len;
	char *buf = uwsgi_malloc(c_len);
	if (compress((Bytef *) buf, &destLen, (Bytef *)msg, (uLong) len) == Z_OK) {
		*rlen = destLen;
		return buf;
	}
	free(buf);
	return NULL;
}
#endif

/*

really fast encoder adding only a prefix

*/
static char *uwsgi_log_encoder_prefix(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {
	char *buf = NULL;
	struct uwsgi_buffer *ub = uwsgi_buffer_new(len + strlen(ule->args));
	if (uwsgi_buffer_append(ub, ule->args, strlen(ule->args))) goto end;
	if (uwsgi_buffer_append(ub, msg, len)) goto end;
	*rlen = ub->pos;
	buf = ub->buf;
	ub->buf = NULL;
end:
	uwsgi_buffer_destroy(ub);
	return buf;
}

/*

really fast encoder adding only a newline

*/
static char *uwsgi_log_encoder_nl(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {
        char *buf = NULL;
        struct uwsgi_buffer *ub = uwsgi_buffer_new(len + 1);
        if (uwsgi_buffer_append(ub, msg, len)) goto end;
        if (uwsgi_buffer_byte(ub, '\n')) goto end;
        *rlen = ub->pos;
        buf = ub->buf;
        ub->buf = NULL;
end:
        uwsgi_buffer_destroy(ub);
        return buf;
}

/*

really fast encoder adding only a suffix

*/
static char *uwsgi_log_encoder_suffix(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {
        char *buf = NULL;
        struct uwsgi_buffer *ub = uwsgi_buffer_new(len + strlen(ule->args));
        if (uwsgi_buffer_append(ub, msg, len)) goto end;
        if (uwsgi_buffer_append(ub, ule->args, strlen(ule->args))) goto end;
        *rlen = ub->pos;
        buf = ub->buf;
        ub->buf = NULL;
end:
        uwsgi_buffer_destroy(ub);
        return buf;
}



void uwsgi_log_encoder_parse_vars(struct uwsgi_log_encoder *ule) {
		char *ptr = ule->args;
		size_t remains = strlen(ptr);
		char *base = ptr;
		size_t base_len = 0;
		char *var = NULL;
		size_t var_len = 0;
		int status = 0; // 1 -> $ 2-> { end -> }
		while(remains--) {
			char b = *ptr++;
			if (status == 1) {
				if (b == '{') {
					status = 2;
					continue;
				}
				base_len+=2;
				status = 0;
				continue;
			}
			else if (status == 2) {
				if (b == '}') {
					status = 0;
					uwsgi_string_new_list((struct uwsgi_string_list **) &ule->data, uwsgi_concat2n(base, base_len, "", 0));
					struct uwsgi_string_list *usl = uwsgi_string_new_list((struct uwsgi_string_list **) &ule->data, uwsgi_concat2n(var, var_len, "", 0));
					usl->custom = 1;
					var = NULL;
					var_len = 0;
					base = NULL;
					base_len = 0;
					continue;
				}
				if (!var) var = (ptr-1);
				var_len++;
				continue;
			}
			// status == 0
			if (b == '$') {
				status = 1;
			}
			else {
				if (!base) base = (ptr-1);
				base_len++;
			}
		}

		if (base) {
			if (status == 1) {
				base_len+=2;
			}
			else if (status == 2) {
				base_len+=3;
			}
			uwsgi_string_new_list((struct uwsgi_string_list **) &ule->data, uwsgi_concat2n(base, base_len, "", 0));
		}
}

/*
        // format: foo ${var} bar
        msg (the logline)
        msgnl (the logline with newline)
        unix (the time_t value)
        micros (current microseconds)
        strftime (strftime)
*/
static char *uwsgi_log_encoder_format(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {

	if (!ule->configured) {
		uwsgi_log_encoder_parse_vars(ule);
		ule->configured = 1;
	}

	struct uwsgi_buffer *ub = uwsgi_buffer_new(strlen(ule->args) + len);
	struct uwsgi_string_list *usl = (struct uwsgi_string_list *) ule->data;
	char *buf = NULL;
	while(usl) {
		if (usl->custom) {
			if (!uwsgi_strncmp(usl->value, usl->len, "msg", 3)) {
				if (msg[len-1] == '\n') {
					if (uwsgi_buffer_append(ub, msg, len-1)) goto end;
				}
				else {
					if (uwsgi_buffer_append(ub, msg, len)) goto end;
				}
			}
			else if (!uwsgi_strncmp(usl->value, usl->len, "msgnl", 5)) {
				if (uwsgi_buffer_append(ub, msg, len)) goto end;
			}
			else if (!uwsgi_strncmp(usl->value, usl->len, "unix", 4)) {
				if (uwsgi_buffer_num64(ub, uwsgi_now())) goto end;
			}
			else if (!uwsgi_strncmp(usl->value, usl->len, "micros", 6)) {
				if (uwsgi_buffer_num64(ub, uwsgi_micros())) goto end;
			}
			else if (!uwsgi_strncmp(usl->value, usl->len, "millis", 6)) {
				if (uwsgi_buffer_num64(ub, uwsgi_millis())) goto end;
			}
			else if (!uwsgi_starts_with(usl->value, usl->len, "strftime:", 9)) {
				char sftime[64];
                                time_t now = uwsgi_now();
				char *buf = uwsgi_concat2n(usl->value+9, usl->len-9,"", 0);
                                int strftime_len = strftime(sftime, 64, buf, localtime(&now));
				free(buf);
				if (strftime_len > 0) {
					if (uwsgi_buffer_append(ub, sftime, strftime_len)) goto end;
				}
			}
		}
		else {
			if (uwsgi_buffer_append(ub, usl->value, usl->len)) goto end;
		}
		usl = usl->next;
	}
	buf = ub->buf;
	*rlen = ub->pos;
	ub->buf = NULL;
end:
	uwsgi_buffer_destroy(ub);
	return buf;
}

static char *uwsgi_log_encoder_json(struct uwsgi_log_encoder *ule, char *msg, size_t len, size_t *rlen) {

        if (!ule->configured) {
                uwsgi_log_encoder_parse_vars(ule);
                ule->configured = 1;
        }

        struct uwsgi_buffer *ub = uwsgi_buffer_new(strlen(ule->args) + len);
        struct uwsgi_string_list *usl = (struct uwsgi_string_list *) ule->data;
        char *buf = NULL;
        while(usl) {
                if (usl->custom) {
                        if (!uwsgi_strncmp(usl->value, usl->len, "msg", 3)) {
				size_t msg_len = len;
                                if (msg[len-1] == '\n') msg_len--;
				char *e_json = uwsgi_malloc((msg_len * 2)+1);
				escape_json(msg, msg_len, e_json);
				if (uwsgi_buffer_append(ub, e_json, strlen(e_json))){
                                	free(e_json);
                                        goto end;
                                }
                                free(e_json);
                        }
                        else if (!uwsgi_strncmp(usl->value, usl->len, "msgnl", 5)) {
				char *e_json = uwsgi_malloc((len * 2)+1);
                                escape_json(msg, len, e_json);
                                if (uwsgi_buffer_append(ub, e_json, strlen(e_json))){
                                        free(e_json);
                                        goto end;
                                }
                                free(e_json);
                        }
                        else if (!uwsgi_strncmp(usl->value, usl->len, "unix", 4)) {
                                if (uwsgi_buffer_num64(ub, uwsgi_now())) goto end;
                        }
                        else if (!uwsgi_strncmp(usl->value, usl->len, "micros", 6)) {
                                if (uwsgi_buffer_num64(ub, uwsgi_micros())) goto end;
                        }
                        else if (!uwsgi_strncmp(usl->value, usl->len, "millis", 6)) {
                                if (uwsgi_buffer_num64(ub, uwsgi_millis())) goto end;
                        }
                        else if (!uwsgi_starts_with(usl->value, usl->len, "strftime:", 9)) {
                                char sftime[64];
                                time_t now = uwsgi_now();
                                char *buf = uwsgi_concat2n(usl->value+9, usl->len-9, "", 0);
                                int strftime_len = strftime(sftime, 64, buf, localtime(&now));
                                free(buf);
                                if (strftime_len > 0) {
					char *e_json = uwsgi_malloc((strftime_len * 2)+1);
					escape_json(sftime, strftime_len, e_json);
                                        if (uwsgi_buffer_append(ub, e_json, strlen(e_json))){
						free(e_json);
						goto end;
					}
					free(e_json);
                                }
                        }
                }
                else {
                        if (uwsgi_buffer_append(ub, usl->value, usl->len)) goto end;
                }
                usl = usl->next;
        }
        buf = ub->buf;
        *rlen = ub->pos;
        ub->buf = NULL;
end:
        uwsgi_buffer_destroy(ub);
        return buf;
}

#define r_logchunk(x) uwsgi_register_logchunk(#x, uwsgi_lf_ ## x, 1)
#define r_logchunk_offset(x, y) { struct uwsgi_logchunk *lc = uwsgi_register_logchunk(#x, NULL, 0); lc->pos = offsetof(struct wsgi_request, y); lc->pos_len = offsetof(struct wsgi_request, y ## _len); lc->type = 1; lc->free=0;}
void uwsgi_register_logchunks() {
	// offsets
	r_logchunk_offset(uri, uri);
	r_logchunk_offset(method, method);
	r_logchunk_offset(user, remote_user);
	r_logchunk_offset(addr, remote_addr);
	r_logchunk_offset(host, host);
	r_logchunk_offset(proto, protocol);
	r_logchunk_offset(uagent, user_agent);
	r_logchunk_offset(referer, referer);

	// funcs
	r_logchunk(status);
	r_logchunk(rsize);
	r_logchunk(hsize);
	r_logchunk(size);
	r_logchunk(cl);
	r_logchunk(micros);
	r_logchunk(msecs);
	r_logchunk(secs);
	r_logchunk(tmsecs);
	r_logchunk(tmicros);
	r_logchunk(time);
	r_logchunk(ltime);
	r_logchunk(ftime);
	r_logchunk(ctime);
	r_logchunk(epoch);
	r_logchunk(pid);
	r_logchunk(wid);
	r_logchunk(switches);
	r_logchunk(vars);
	r_logchunk(core);
	r_logchunk(vsz);
	r_logchunk(rss);
	r_logchunk(vszM);
	r_logchunk(rssM);
	r_logchunk(pktsize);
	r_logchunk(modifier1);
	r_logchunk(modifier2);
	r_logchunk(headers);
	r_logchunk(werr);
	r_logchunk(rerr);
	r_logchunk(ioerr);
}

void uwsgi_log_encoders_register_embedded() {
	uwsgi_register_log_encoder("prefix", uwsgi_log_encoder_prefix);
	uwsgi_register_log_encoder("suffix", uwsgi_log_encoder_suffix);
	uwsgi_register_log_encoder("nl", uwsgi_log_encoder_nl);
	uwsgi_register_log_encoder("format", uwsgi_log_encoder_format);
	uwsgi_register_log_encoder("json", uwsgi_log_encoder_json);
#ifdef UWSGI_ZLIB
	uwsgi_register_log_encoder("gzip", uwsgi_log_encoder_gzip);
	uwsgi_register_log_encoder("compress", uwsgi_log_encoder_compress);
#endif
}
