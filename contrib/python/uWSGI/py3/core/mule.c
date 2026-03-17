#include <contrib/python/uWSGI/py3/config.h>
/*

uWSGI mules are very simple workers only managing signals or running custom code in background.

By default they born in signal-only mode, but if you patch them (passing the script/code to run) they will became fully customized daemons.

*/

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

void uwsgi_mule_handler(void);

int mule_send_msg(int fd, char *message, size_t len) {

	socklen_t so_bufsize_len = sizeof(int);
	int so_bufsize = 0;

	if (write(fd, message, len) != (ssize_t) len) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &so_bufsize, &so_bufsize_len)) {
				uwsgi_error("getsockopt()");
			}
			uwsgi_log("*** MULE MSG QUEUE IS FULL: buffer size %d bytes (you can tune it with --mule-msg-size) ***\n", so_bufsize);
		}
		else {
			uwsgi_error("mule_send_msg()");
		}
		return -1;
	}
	return 0;
}

void uwsgi_mule(int id) {

	int i;

	pid_t pid = uwsgi_fork(uwsgi.mules[id - 1].name);
	if (pid == 0) {
#ifdef __linux__
		if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0)) {
			uwsgi_error("prctl()");
		}
#endif

		signal(SIGALRM, SIG_IGN);
                signal(SIGHUP, end_me);
                signal(SIGINT, end_me);
                signal(SIGTERM, end_me);
                signal(SIGUSR1, SIG_IGN);
                signal(SIGUSR2, SIG_IGN);
                signal(SIGPIPE, SIG_IGN);
                signal(SIGSTOP, SIG_IGN);
                signal(SIGTSTP, SIG_IGN);

		uwsgi.muleid = id;
		// avoid race conditions
		uwsgi.mules[id - 1].id = id;
		uwsgi.mules[id - 1].pid = getpid();
		uwsgi.mypid = uwsgi.mules[id - 1].pid;

		uwsgi_fixup_fds(0, id, NULL);

		uwsgi.my_signal_socket = uwsgi.mules[id - 1].signal_pipe[1];
		uwsgi.signal_socket = uwsgi.shared->mule_signal_pipe[1];

		uwsgi_close_all_sockets();

		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->master_fixup) {
				uwsgi.p[i]->master_fixup(1);
			}
		}

		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->post_fork) {
				uwsgi.p[i]->post_fork();
			}
		}

		uwsgi_hooks_run(uwsgi.hook_as_mule, "as-mule", 1);
		uwsgi_mule_run();

	}
	else if (pid > 0) {
		uwsgi.mules[id - 1].id = id;
		uwsgi.mules[id - 1].pid = pid;
		uwsgi_log("spawned uWSGI mule %d (pid: %d)\n", id, (int) pid);
	}
}

void uwsgi_mule_run() {
	int id = uwsgi.muleid;
	int i;
	if (uwsgi.mules[id - 1].patch) {
                        for (i = 0; i < 256; i++) {
                                if (uwsgi.p[i]->mule) {
                                        if (uwsgi.p[i]->mule(uwsgi.mules[id - 1].patch) == 1) {
                                                // never here ?
                                                end_me(1);
                                        }
                                }
                        }
                }

                uwsgi_mule_handler();
}

int uwsgi_farm_has_mule(struct uwsgi_farm *farm, int muleid) {

	struct uwsgi_mule_farm *umf = farm->mules;

	while (umf) {
		if (umf->mule->id == muleid) {
			return 1;
		}
		umf = umf->next;
	}

	return 0;
}

int farm_has_signaled(int fd) {

	int i;
	for (i = 0; i < uwsgi.farms_cnt; i++) {
		struct uwsgi_mule_farm *umf = uwsgi.farms[i].mules;
		while (umf) {
			if (umf->mule->id == uwsgi.muleid && uwsgi.farms[i].signal_pipe[1] == fd) {
				return 1;
			}
			umf = umf->next;
		}
	}

	return 0;
}

int farm_has_msg(int fd) {

	int i;
	for (i = 0; i < uwsgi.farms_cnt; i++) {
		struct uwsgi_mule_farm *umf = uwsgi.farms[i].mules;
		while (umf) {
			if (umf->mule->id == uwsgi.muleid && uwsgi.farms[i].queue_pipe[1] == fd) {
				return 1;
			}
			umf = umf->next;
		}
	}

	return 0;
}


void uwsgi_mule_add_farm_to_queue(int queue) {

	int i;
	for (i = 0; i < uwsgi.farms_cnt; i++) {
		if (uwsgi_farm_has_mule(&uwsgi.farms[i], uwsgi.muleid)) {
			event_queue_add_fd_read(queue, uwsgi.farms[i].signal_pipe[1]);
			event_queue_add_fd_read(queue, uwsgi.farms[i].queue_pipe[1]);
		}
	}
}

void uwsgi_mule_handler() {

	ssize_t len;
	uint8_t uwsgi_signal;
	int rlen;
	int interesting_fd;

	// this must be configurable
	char message[65536];

	int mule_queue = event_queue_init();

	event_queue_add_fd_read(mule_queue, uwsgi.signal_socket);
	event_queue_add_fd_read(mule_queue, uwsgi.my_signal_socket);
	event_queue_add_fd_read(mule_queue, uwsgi.mules[uwsgi.muleid - 1].queue_pipe[1]);
	event_queue_add_fd_read(mule_queue, uwsgi.shared->mule_queue_pipe[1]);

	uwsgi_mule_add_farm_to_queue(mule_queue);

	for (;;) {
		rlen = event_queue_wait(mule_queue, -1, &interesting_fd);
		if (rlen <= 0) {
			continue;
		}

		if (interesting_fd == uwsgi.signal_socket || interesting_fd == uwsgi.my_signal_socket || farm_has_signaled(interesting_fd)) {
			len = read(interesting_fd, &uwsgi_signal, 1);
			if (len <= 0) {
				if (len < 0 && (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK)) continue;
				uwsgi_log_verbose("uWSGI mule %d braying: my master died, i will follow him...\n", uwsgi.muleid);
				end_me(0);
			}
#ifdef UWSGI_DEBUG
			uwsgi_log_verbose("master sent signal %d to mule %d\n", uwsgi_signal, uwsgi.muleid);
#endif
			if (uwsgi_signal_handler(uwsgi_signal)) {
				uwsgi_log_verbose("error managing signal %d on mule %d\n", uwsgi_signal, uwsgi.muleid);
			}
		}
		else if (interesting_fd == uwsgi.mules[uwsgi.muleid - 1].queue_pipe[1] || interesting_fd == uwsgi.shared->mule_queue_pipe[1] || farm_has_msg(interesting_fd)) {
			len = read(interesting_fd, message, 65536);
			if (len < 0) {
				if (errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK) {
					uwsgi_error("uwsgi_mule_handler/read()");
				}
			}
			else {
				int i, found = 0;
				for (i = 0; i < 256; i++) {
					if (uwsgi.p[i]->mule_msg) {
						if (uwsgi.p[i]->mule_msg(message, len)) {
							found = 1;
							break;
						}
					}
				}
				if (!found)
					uwsgi_log("*** mule %d received a %ld bytes message ***\n", uwsgi.muleid, (long) len);
			}
		}
	}

}

struct uwsgi_mule *get_mule_by_id(int id) {

	int i;

	for (i = 0; i < uwsgi.mules_cnt; i++) {
		if (uwsgi.mules[i].id == id) {
			return &uwsgi.mules[i];
		}
	}

	return NULL;
}

struct uwsgi_farm *get_farm_by_name(char *name) {

	int i;

	for (i = 0; i < uwsgi.farms_cnt; i++) {
		if (!strcmp(uwsgi.farms[i].name, name)) {
			return &uwsgi.farms[i];
		}
	}

	return NULL;
}


struct uwsgi_mule_farm *uwsgi_mule_farm_new(struct uwsgi_mule_farm **umf, struct uwsgi_mule *um) {

	struct uwsgi_mule_farm *uwsgi_mf = *umf, *old_umf;

	if (!uwsgi_mf) {
		*umf = uwsgi_malloc(sizeof(struct uwsgi_mule_farm));
		uwsgi_mf = *umf;
	}
	else {
		while (uwsgi_mf) {
			old_umf = uwsgi_mf;
			uwsgi_mf = uwsgi_mf->next;
		}

		uwsgi_mf = uwsgi_malloc(sizeof(struct uwsgi_mule_farm));
		old_umf->next = uwsgi_mf;
	}

	uwsgi_mf->mule = um;
	uwsgi_mf->next = NULL;

	return uwsgi_mf;
}

ssize_t uwsgi_mule_get_msg(int manage_signals, int manage_farms, char *message, size_t buffer_size, int timeout) {

	ssize_t len = 0;
	struct pollfd *mulepoll;
	int count = 4;
	int farms_count = 0;
	uint8_t uwsgi_signal;
	int i;

	if (uwsgi.muleid == 0)
		return -1;

	if (manage_signals)
		count = 2;

	if (!manage_farms)
		goto next;

	for (i = 0; i < uwsgi.farms_cnt; i++) {
		if (uwsgi_farm_has_mule(&uwsgi.farms[i], uwsgi.muleid))
			farms_count++;
	}
next:

	if (timeout > -1)
		timeout = timeout * 1000;

	mulepoll = uwsgi_malloc(sizeof(struct pollfd) * (count + farms_count));

	mulepoll[0].fd = uwsgi.mules[uwsgi.muleid - 1].queue_pipe[1];
	mulepoll[0].events = POLLIN;
	mulepoll[1].fd = uwsgi.shared->mule_queue_pipe[1];
	mulepoll[1].events = POLLIN;
	if (count > 2) {
		mulepoll[2].fd = uwsgi.signal_socket;
		mulepoll[2].events = POLLIN;
		mulepoll[3].fd = uwsgi.my_signal_socket;
		mulepoll[3].events = POLLIN;
	}

	if (farms_count > 0) {
		int tmp_cnt = 0;
		for (i = 0; i < uwsgi.farms_cnt; i++) {
			if (uwsgi_farm_has_mule(&uwsgi.farms[i], uwsgi.muleid)) {
				mulepoll[count + tmp_cnt].fd = uwsgi.farms[i].queue_pipe[1];
				mulepoll[count + tmp_cnt].events = POLLIN;
				tmp_cnt++;
			}
		}
	}

	int ret = -1;
retry:
	ret = poll(mulepoll, count + farms_count, timeout);
	if (ret < 0) {
		uwsgi_error("uwsgi_mule_get_msg()/poll()");
	}
	else if (ret > 0 ) {
		if (mulepoll[0].revents & POLLIN) {
			len = read(uwsgi.mules[uwsgi.muleid - 1].queue_pipe[1], message, buffer_size);
		}
		else if (mulepoll[1].revents & POLLIN) {
			len = read(uwsgi.shared->mule_queue_pipe[1], message, buffer_size);
		}
		else {
			if (count > 2) {
				int interesting_fd = -1;
				if (mulepoll[2].revents & POLLIN) {
					interesting_fd = mulepoll[2].fd;
				}
				else if (mulepoll[3].revents & POLLIN) {
					interesting_fd = mulepoll[3].fd;
				}

				if (interesting_fd > -1) {
					len = read(interesting_fd, &uwsgi_signal, 1);
					if (len <= 0) {
						if (uwsgi_is_again()) goto retry;
						uwsgi_log_verbose("uWSGI mule %d braying: my master died, i will follow him...\n", uwsgi.muleid);
						end_me(0);
					}
#ifdef UWSGI_DEBUG
					uwsgi_log_verbose("master sent signal %d to mule %d\n", uwsgi_signal, uwsgi.muleid);
#endif
					if (uwsgi_signal_handler(uwsgi_signal)) {
						uwsgi_log_verbose("error managing signal %d on mule %d\n", uwsgi_signal, uwsgi.muleid);
					}
					// set the error condition
					len = -1;
					goto clear;
				}
			}

			// read messages in the farm
			for (i = 0; i < farms_count; i++) {
				if (mulepoll[count + i].revents & POLLIN) {
					len = read(mulepoll[count + i].fd, message, buffer_size);
					break;
				}
			}
		}
	}

	if (len < 0) {
		if (uwsgi_is_again()) goto retry;
		uwsgi_error("read()");
		goto clear;
	}

clear:
	free(mulepoll);
	return len;
}


void uwsgi_opt_add_mule(char *opt, char *value, void *foobar) {

	uwsgi.mules_cnt++;
	uwsgi_string_new_list(&uwsgi.mules_patches, value);
}

void uwsgi_opt_add_mules(char *opt, char *value, void *foobar) {
	int i;

	for (i = 0; i < atoi(value); i++) {
		uwsgi.mules_cnt++;
		uwsgi_string_new_list(&uwsgi.mules_patches, NULL);
	}
}

void uwsgi_opt_add_farm(char *opt, char *value, void *foobar) {
	uwsgi.farms_cnt++;
	uwsgi_string_new_list(&uwsgi.farms_list, value);

}

void uwsgi_setup_mules_and_farms() {
	int i;
	if (uwsgi.mules_cnt > 0) {
		uwsgi.mules = (struct uwsgi_mule *) uwsgi_calloc_shared(sizeof(struct uwsgi_mule) * uwsgi.mules_cnt);

		create_signal_pipe(uwsgi.shared->mule_signal_pipe);
		create_msg_pipe(uwsgi.shared->mule_queue_pipe, uwsgi.mule_msg_size);

		for (i = 0; i < uwsgi.mules_cnt; i++) {
			// create the socket pipe
			create_signal_pipe(uwsgi.mules[i].signal_pipe);
			create_msg_pipe(uwsgi.mules[i].queue_pipe, uwsgi.mule_msg_size);

			uwsgi.mules[i].id = i + 1;

			snprintf(uwsgi.mules[i].name, 0xff, "uWSGI mule %d", i + 1);
		}
	}

	if (uwsgi.farms_cnt > 0) {
		uwsgi.farms = (struct uwsgi_farm *) uwsgi_calloc_shared(sizeof(struct uwsgi_farm) * uwsgi.farms_cnt);

		struct uwsgi_string_list *farm_name = uwsgi.farms_list;
		for (i = 0; i < uwsgi.farms_cnt; i++) {

			char *farm_value = uwsgi_str(farm_name->value);

			char *mules_list = strchr(farm_value, ':');
			if (!mules_list) {
				uwsgi_log("invalid farm value (%s) must be in the form name:mule[,muleN].\n", farm_value);
				exit(1);
			}

			mules_list[0] = 0;
			mules_list++;

			snprintf(uwsgi.farms[i].name, 0xff, "%s", farm_value);

			// create the socket pipe
			create_signal_pipe(uwsgi.farms[i].signal_pipe);
			create_msg_pipe(uwsgi.farms[i].queue_pipe, uwsgi.mule_msg_size);

			char *p, *ctx = NULL;
			uwsgi_foreach_token(mules_list, ",", p, ctx) {
				struct uwsgi_mule *um = get_mule_by_id(atoi(p));
				if (!um) {
					uwsgi_log("invalid mule id: %s\n", p);
					exit(1);
				}

				uwsgi_mule_farm_new(&uwsgi.farms[i].mules, um);
			}
			uwsgi_log("created farm %d name: %s mules:%s\n", i + 1, uwsgi.farms[i].name, strchr(farm_name->value, ':') + 1);

			farm_name = farm_name->next;
			free(farm_value);
		}

	}

}
