#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

struct uwsgi_gateway *register_gateway(char *name, void (*loop) (int, void *), void *data) {

	struct uwsgi_gateway *ug;
	int num = 1, i;

	if (ushared->gateways_cnt >= MAX_GATEWAYS) {
		uwsgi_log("you can register max %d gateways\n", MAX_GATEWAYS);
		return NULL;
	}

	for (i = 0; i < ushared->gateways_cnt; i++) {
		if (!strcmp(name, ushared->gateways[i].name)) {
			num++;
		}
	}

	char *str = uwsgi_num2str(num);
	char *fullname = uwsgi_concat3(name, " ", str);
	free(str);

	ug = &ushared->gateways[ushared->gateways_cnt];
	ug->pid = 0;
	ug->name = name;
	ug->loop = loop;
	ug->num = num;
	ug->fullname = fullname;
	ug->data = data;
	ug->uid = 0;
	ug->gid = 0;

#if defined(SOCK_SEQPACKET) && defined(__linux__)
	if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, ug->internal_subscription_pipe)) {
#else
	if (socketpair(AF_UNIX, SOCK_DGRAM, 0, ug->internal_subscription_pipe)) {
#endif
		uwsgi_error("socketpair()");
	}

	uwsgi_socket_nb(ug->internal_subscription_pipe[0]);
	uwsgi_socket_nb(ug->internal_subscription_pipe[1]);

	if (!uwsgi.master_process && !uwsgi.force_gateway)
		gateway_respawn(ushared->gateways_cnt);

	ushared->gateways_cnt++;


	return ug;
}

static void gateway_brutal_end() {
        _exit(UWSGI_END_CODE);
}

void gateway_respawn(int id) {

	pid_t gw_pid;
	struct uwsgi_gateway *ug = &ushared->gateways[id];

	if (uwsgi.master_process)
		uwsgi.shared->gateways_harakiri[id] = 0;

	gw_pid = uwsgi_fork(ug->fullname);
	if (gw_pid < 0) {
		uwsgi_error("fork()");
		return;
	}

	if (gw_pid == 0) {
		uwsgi_fixup_fds(0, 0, ug);
		uwsgi_close_all_unshared_sockets();
		if (uwsgi.master_as_root)
			uwsgi_as_root();
#ifdef __linux__
		if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0)) {
			uwsgi_error("prctl()");
		}
#endif
		uwsgi.mypid = getpid();
		atexit(gateway_brutal_end);
		signal(SIGALRM, SIG_IGN);
		signal(SIGHUP, SIG_IGN);
		signal(SIGINT, end_me);
		signal(SIGTERM, end_me);
		signal(SIGUSR1, SIG_IGN);
		signal(SIGUSR2, SIG_IGN);
		signal(SIGPIPE, SIG_IGN);
		signal(SIGSTOP, SIG_IGN);
		signal(SIGTSTP, SIG_IGN);

		uwsgi_hooks_run(uwsgi.hook_as_gateway, "as-gateway", 1);

		if (ug->gid) {
			uwsgi_log("%s %d setgid() to %d\n", ug->name, ug->num, (int) ug->gid);
			if (setgid(ug->gid)) {
				uwsgi_error("gateway_respawn()/setgid()");
				exit(1);
			}
		}

		if (ug->uid) {
			uwsgi_log("%s %d setuid() to %d\n", ug->name, ug->num, (int) ug->uid);
			if (setuid(ug->uid)) {
				uwsgi_error("gateway_respawn()/setuid()");
				exit(1);
			}
		}

		ug->loop(id, ug->data);
		// never here !!! (i hope)
		exit(1);
	}

	ug->pid = gw_pid;
	ug->respawns++;
	if (ug->respawns == 1) {
		uwsgi_log("spawned %s %d (pid: %d)\n", ug->name, ug->num, (int) gw_pid);
	}
	else {
		uwsgi_log("respawned %s %d (pid: %d)\n", ug->name, ug->num, (int) gw_pid);
	}

}

struct uwsgi_gateway_socket *uwsgi_new_gateway_socket(char *name, char *owner) {

	struct uwsgi_gateway_socket *uwsgi_sock = uwsgi.gateway_sockets, *old_uwsgi_sock;

	if (!uwsgi_sock) {
		uwsgi.gateway_sockets = uwsgi_malloc(sizeof(struct uwsgi_gateway_socket));
		uwsgi_sock = uwsgi.gateway_sockets;
	}
	else {
		while (uwsgi_sock) {
			old_uwsgi_sock = uwsgi_sock;
			uwsgi_sock = uwsgi_sock->next;
		}

		uwsgi_sock = uwsgi_malloc(sizeof(struct uwsgi_gateway_socket));
		old_uwsgi_sock->next = uwsgi_sock;
	}

	memset(uwsgi_sock, 0, sizeof(struct uwsgi_gateway_socket));
	uwsgi_sock->fd = -1;
	uwsgi_sock->shared = 0;
	uwsgi_sock->name = name;
	uwsgi_sock->name_len = strlen(name);
	uwsgi_sock->owner = owner;

	return uwsgi_sock;
}

struct uwsgi_gateway_socket *uwsgi_new_gateway_socket_from_fd(int fd, char *owner) {

	struct uwsgi_gateway_socket *uwsgi_sock = uwsgi.gateway_sockets, *old_uwsgi_sock;

	if (!uwsgi_sock) {
		uwsgi.gateway_sockets = uwsgi_malloc(sizeof(struct uwsgi_gateway_socket));
		uwsgi_sock = uwsgi.gateway_sockets;
	}
	else {
		while (uwsgi_sock) {
			old_uwsgi_sock = uwsgi_sock;
			uwsgi_sock = uwsgi_sock->next;
		}

		uwsgi_sock = uwsgi_malloc(sizeof(struct uwsgi_gateway_socket));
		old_uwsgi_sock->next = uwsgi_sock;
	}

	memset(uwsgi_sock, 0, sizeof(struct uwsgi_gateway_socket));
	uwsgi_sock->fd = fd;
	uwsgi_sock->name = uwsgi_getsockname(fd);
	uwsgi_sock->name_len = strlen(uwsgi_sock->name);
	uwsgi_sock->owner = owner;

	return uwsgi_sock;
}


void uwsgi_gateway_go_cheap(char *gw_id, int queue, int *i_am_cheap) {

	uwsgi_log("[%s pid %d] no more nodes available. Going cheap...\n", gw_id, (int) uwsgi.mypid);
	struct uwsgi_gateway_socket *ugs = uwsgi.gateway_sockets;
	while (ugs) {
		if (!strcmp(ugs->owner, gw_id) && !ugs->subscription) {
			event_queue_del_fd(queue, ugs->fd, event_queue_read());
		}
		ugs = ugs->next;
	}
	*i_am_cheap = 1;
}
