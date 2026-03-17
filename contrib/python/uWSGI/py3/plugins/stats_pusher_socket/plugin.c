#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

/*

this is a stats pusher plugin for sendign metrics over a udp socket

--stats-push socket:address[,prefix]

example:

--stats-push socket:127.0.0.1:8125,myinstance

it exports values exposed by the metric subsystem

(it is based on the statsd plugin)

*/

extern struct uwsgi_server uwsgi;

// configuration of a socket node
struct socket_node {
	int fd;
	union uwsgi_sockaddr addr;
	socklen_t addr_len;
	char *prefix;
	uint16_t prefix_len;
};

static int socket_send_metric(struct uwsgi_buffer *ub, struct uwsgi_stats_pusher_instance *uspi, struct uwsgi_metric *um) {
	struct socket_node *sn = (struct socket_node *) uspi->data;
	// reset the buffer
        ub->pos = 0;
	if (uwsgi_buffer_append(ub, sn->prefix, sn->prefix_len)) return -1;	
	if (uwsgi_buffer_append(ub, ".", 1)) return -1;
	if (uwsgi_buffer_append(ub, um->name, um->name_len)) return -1;
	if (uwsgi_buffer_append(ub, " ", 1)) return -1;
        if (uwsgi_buffer_num64(ub, (int64_t) um->type)) return -1;
	if (uwsgi_buffer_append(ub, " ", 1)) return -1;
        if (uwsgi_buffer_num64(ub, *um->value)) return -1;

        if (sendto(sn->fd, ub->buf, ub->pos, 0, (struct sockaddr *) &sn->addr.sa_in, sn->addr_len) < 0) {
                uwsgi_error("socket_send_metric()/sendto()");
        }

        return 0;

}


static void stats_pusher_socket(struct uwsgi_stats_pusher_instance *uspi, time_t now, char *json, size_t json_len) {

	if (!uspi->configured) {
		struct socket_node *sn = uwsgi_calloc(sizeof(struct socket_node));
		char *comma = strchr(uspi->arg, ',');
		if (comma) {
			sn->prefix = comma+1;
			sn->prefix_len = strlen(sn->prefix);
			*comma = 0;
		}
		else {
			sn->prefix = "uwsgi";
			sn->prefix_len = 5;
		}

		char *colon = strchr(uspi->arg, ':');
		if (!colon) {
			uwsgi_log("invalid socket address %s\n", uspi->arg);
			if (comma) *comma = ',';
			free(sn);
			return;
		}
		sn->addr_len = socket_to_in_addr(uspi->arg, colon, 0, &sn->addr.sa_in);

		sn->fd = socket(AF_INET, SOCK_DGRAM, 0);
		if (sn->fd < 0) {
			uwsgi_error("stats_pusher_socket()/socket()");
			if (comma) *comma = ',';
                        free(sn);
                        return;
		}
		uwsgi_socket_nb(sn->fd);
		if (comma) *comma = ',';
		uspi->data = sn;
		uspi->configured = 1;
	}

	// we use the same buffer for all of the packets
	struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
	struct uwsgi_metric *um = uwsgi.metrics;
	while(um) {
		uwsgi_rlock(uwsgi.metrics_lock);
		socket_send_metric(ub, uspi, um);
		uwsgi_rwunlock(uwsgi.metrics_lock);
		if (um->reset_after_push){
			uwsgi_wlock(uwsgi.metrics_lock);
			*um->value = um->initial_value;
			uwsgi_rwunlock(uwsgi.metrics_lock);
		}
		um = um->next;
	}
	uwsgi_buffer_destroy(ub);
}

static void stats_pusher_socket_init(void) {
        struct uwsgi_stats_pusher *usp = uwsgi_register_stats_pusher("socket", stats_pusher_socket);
	// we use a custom format not the JSON one
	usp->raw = 1;
}

struct uwsgi_plugin stats_pusher_socket_plugin = {

        .name = "stats_pusher_socket",
        .on_load = stats_pusher_socket_init,
};

