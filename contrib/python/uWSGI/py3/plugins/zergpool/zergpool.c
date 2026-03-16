#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI zergpool

*/

#include "../../uwsgi.h"

extern struct uwsgi_server uwsgi;

struct uwsgi_string_list *zergpool_socket_names;

#define ZERGPOOL_EVENTS 64

struct uwsgi_option zergpool_options[] = {
	{ "zergpool", required_argument, 0, "start a zergpool on specified address for specified address", uwsgi_opt_add_string_list, &zergpool_socket_names, 0},
	{ "zerg-pool", required_argument, 0, "start a zergpool on specified address for specified address", uwsgi_opt_add_string_list, &zergpool_socket_names, 0},
	{0, 0, 0, 0, 0, 0, 0},
};

struct zergpool_socket {
	int fd;
	int *sockets;
	int num_sockets;

	struct zergpool_socket *next;
};

struct zergpool_socket *zergpool_sockets;

void zergpool_loop(int id, void *foobar) {

	int i;

	int zergpool_queue = event_queue_init();

	void *events = event_queue_alloc(ZERGPOOL_EVENTS);

	struct zergpool_socket *zps = zergpool_sockets;
	while(zps) {
		event_queue_add_fd_read(zergpool_queue, zps->fd);
		zps = zps->next;
	}

	for(;;) {
		int nevents = event_queue_wait_multi(zergpool_queue, -1, events, ZERGPOOL_EVENTS);

		for(i=0;i<nevents;i++) {

			int interesting_fd = event_queue_interesting_fd(events, i);

			zps = zergpool_sockets;
			while(zps) {
				if (zps->fd == interesting_fd) {
					uwsgi_manage_zerg(zps->fd, zps->num_sockets, zps->sockets);
				}
				zps = zps->next;
			}
		}
	}
}

struct zergpool_socket *add_zergpool_socket(char *name, char *sockets) {

	struct zergpool_socket *z_sock,*zps = zergpool_sockets;

	if (!zps) {
		z_sock = uwsgi_calloc(sizeof(struct zergpool_socket));
		zergpool_sockets = z_sock;
	}
	else {
		while(zps) {
			if (!zps->next) {
				z_sock= uwsgi_calloc(sizeof(struct zergpool_socket));
				zps->next = z_sock;
				break;
			}
			zps = zps->next;
		}
	}


	// do not defer accept for zergpools
	if (uwsgi.no_defer_accept) {
		uwsgi.no_defer_accept = 0;
		z_sock->fd = bind_to_unix(name, uwsgi.listen_queue, uwsgi.chmod_socket, 0);
		uwsgi.no_defer_accept = 1;
	}
	else {
		z_sock->fd = bind_to_unix(name, uwsgi.listen_queue, uwsgi.chmod_socket, 0);
	}

	char *sock_list = uwsgi_str(sockets);
	char *p, *ctx = NULL;
	uwsgi_foreach_token(sock_list, ",", p, ctx) {
		z_sock->num_sockets++;
	}
	free(sock_list);
	z_sock->sockets = uwsgi_calloc(sizeof(int) * (z_sock->num_sockets + 1));

	sock_list = uwsgi_str(sockets);
	int pos = 0;
	ctx = NULL;
	uwsgi_foreach_token(sock_list, ",", p, ctx) {
		char *port = strchr(p, ':');
		char *sockname;
		if (!port) {
			z_sock->sockets[pos] = bind_to_unix(p, uwsgi.listen_queue, uwsgi.chmod_socket, uwsgi.abstract_socket);
			sockname = uwsgi_getsockname(z_sock->sockets[pos]);
			uwsgi_log("zergpool %s bound to UNIX socket %s (fd: %d)\n", name, sockname, z_sock->sockets[pos]);
		}
		else {
			char *gsn = generate_socket_name(p);
			z_sock->sockets[pos] = bind_to_tcp(gsn, uwsgi.listen_queue, strchr(gsn, ':'));
			sockname = uwsgi_getsockname(z_sock->sockets[pos]);
			uwsgi_log("zergpool %s bound to TCP socket %s (fd: %d)\n", name, sockname, z_sock->sockets[pos]);
		}
		pos++;
		free(sockname);
	}
	free(sock_list);

	return z_sock;
}

int zergpool_init() {

	if (!zergpool_socket_names) return 0;

	struct uwsgi_string_list *zpsn = zergpool_socket_names;
	while(zpsn) {
		char *colon = strchr(zpsn->value, ':');
		if (!colon) {
			uwsgi_log("invalid zergpool syntax: %s\n", zpsn->value);
			exit(1);
		}
		*colon = 0;
		add_zergpool_socket(zpsn->value, colon+1);
		*colon = ':';
		zpsn = zpsn->next;		
	}

	if (register_gateway("uWSGI zergpool", zergpool_loop, NULL) == NULL) {
		uwsgi_log("unable to register the zergpool gateway\n");
		exit(1);
	}

	return 0;
}


struct uwsgi_plugin zergpool_plugin = {

	.name = "zergpool",
	.options = zergpool_options,
	.init = zergpool_init,
};
