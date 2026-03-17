#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;


void uwsgi_setup_systemd() {
	struct uwsgi_socket *uwsgi_sock = NULL;
	int i;

	char *listen_pid = getenv("LISTEN_PID");
	if (listen_pid) {
		if (atoi(listen_pid) == (int) getpid()) {
			char *listen_fds = getenv("LISTEN_FDS");
			if (listen_fds) {
				int systemd_fds = atoi(listen_fds);
				if (systemd_fds > 0) {
					uwsgi_log("- SystemD socket activation detected -\n");
					for (i = 3; i < 3 + systemd_fds; i++) {
						uwsgi_sock = uwsgi_new_socket(NULL);
						uwsgi_add_socket_from_fd(uwsgi_sock, i);
					}
					uwsgi.skip_zero = 1;
				}
				unsetenv("LISTEN_PID");
				unsetenv("LISTEN_FDS");
			}
		}
	}

}

void uwsgi_setup_upstart() {

	struct uwsgi_socket *uwsgi_sock = NULL;

	char *upstart_events = getenv("UPSTART_EVENTS");
	if (upstart_events && !strcmp(upstart_events, "socket")) {
		char *upstart_fds = getenv("UPSTART_FDS");
		if (upstart_fds) {
			uwsgi_log("- Upstart socket bridge detected (job: %s) -\n", getenv("UPSTART_JOB"));
			uwsgi_sock = uwsgi_new_socket(NULL);
			uwsgi_add_socket_from_fd(uwsgi_sock, atoi(upstart_fds));
			uwsgi.skip_zero = 1;
		}
		unsetenv("UPSTART_EVENTS");
		unsetenv("UPSTART_FDS");
	}

}


void uwsgi_setup_zerg() {

	struct uwsgi_socket *uwsgi_sock = NULL;
	int i;

	struct uwsgi_string_list *zn = uwsgi.zerg_node;
	while (zn) {
		if (uwsgi_zerg_attach(zn->value)) {
			if (!uwsgi.zerg_fallback) {
				exit(1);
			}
		}
		zn = zn->next;
	}



	if (uwsgi.zerg) {
#ifdef UWSGI_DEBUG
		uwsgi_log("attaching zerg sockets...\n");
#endif
		int zerg_fd;
		i = 0;
		for (;;) {
			zerg_fd = uwsgi.zerg[i];
			if (zerg_fd == -1) {
				break;
			}
			uwsgi_sock = uwsgi_new_socket(NULL);
			uwsgi_add_socket_from_fd(uwsgi_sock, zerg_fd);
			i++;
		}

		uwsgi_log("zerg sockets attached\n");
	}
}

void uwsgi_setup_inherited_sockets() {

	int j;
	union uwsgi_sockaddr usa;
	union uwsgi_sockaddr_ptr gsa;

	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	while (uwsgi_sock) {
		//a bit overengineering
		if (uwsgi_sock->name[0] != 0 && !uwsgi_sock->bound) {
			for (j = 3; j < (int) uwsgi.max_fd; j++) {
				uwsgi_add_socket_from_fd(uwsgi_sock, j);
			}
		}
		uwsgi_sock = uwsgi_sock->next;
	}

	//now close all the unbound fd
	for (j = 3; j < (int) uwsgi.max_fd; j++) {
		int useless = 1;

		if (uwsgi_fd_is_safe(j)) continue;

		if (uwsgi.has_emperor) {
			if (j == uwsgi.emperor_fd)
				continue;
			if (j == uwsgi.emperor_fd_config)
				continue;
		}

		if (uwsgi.shared->worker_log_pipe[0] > -1) {
			if (j == uwsgi.shared->worker_log_pipe[0])
				continue;
		}

		if (uwsgi.shared->worker_log_pipe[1] > -1) {
			if (j == uwsgi.shared->worker_log_pipe[1])
				continue;
		}

		if (uwsgi.shared->worker_req_log_pipe[0] > -1) {
			if (j == uwsgi.shared->worker_req_log_pipe[0])
				continue;
		}

		if (uwsgi.shared->worker_req_log_pipe[1] > -1) {
			if (j == uwsgi.shared->worker_req_log_pipe[1])
				continue;
		}

		if (uwsgi.original_log_fd > -1) {
			if (j == uwsgi.original_log_fd)
				continue;
		}

		struct uwsgi_gateway_socket *ugs = uwsgi.gateway_sockets;
		int found = 0;
		while (ugs) {
			if (ugs->fd == j) {
				found = 1;
				break;
			}
			ugs = ugs->next;
		}
		if (found)
			continue;


		int y;
		found = 0;
		for (y = 0; y < ushared->gateways_cnt; y++) {
			if (ushared->gateways[y].internal_subscription_pipe[0] == j) {
				found = 1;
				break;
			}
			if (ushared->gateways[y].internal_subscription_pipe[1] == j) {
				found = 1;
				break;
			}
		}

		if (found)
			continue;


		socklen_t socket_type_len = sizeof(struct sockaddr_un);
		gsa.sa = (struct sockaddr *) &usa;
		if (!getsockname(j, gsa.sa, &socket_type_len)) {
			uwsgi_sock = uwsgi.sockets;
			while (uwsgi_sock) {
				if (uwsgi_sock->fd == j && uwsgi_sock->bound) {
					useless = 0;
					break;
				}
				uwsgi_sock = uwsgi_sock->next;
			}

			if (useless) {
				uwsgi_sock = uwsgi.shared_sockets;
				while (uwsgi_sock) {
					if (uwsgi_sock->fd == j && uwsgi_sock->bound) {
						useless = 0;
						break;
					}
					uwsgi_sock = uwsgi_sock->next;
				}
			}
		}

		if (useless) {
			close(j);
		}
	}

}
