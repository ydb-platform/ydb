#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI rawrouter

*/

#include "../../uwsgi.h"
#include "../corerouter/cr.h"

static struct uwsgi_rawrouter {
	struct uwsgi_corerouter cr;
	int xclient;
} urr;

extern struct uwsgi_server uwsgi;

struct rawrouter_session {
	struct corerouter_session session;

	// XCLIENT ADDR=xxx\r\n
	struct uwsgi_buffer *xclient;
	size_t xclient_pos;
	// placeholder for \r\n
	size_t xclient_rn;
};

static struct uwsgi_option rawrouter_options[] = {
	{"rawrouter", required_argument, 0, "run the rawrouter on the specified port", uwsgi_opt_undeferred_corerouter, &urr, 0},
	{"rawrouter-processes", required_argument, 0, "prefork the specified number of rawrouter processes", uwsgi_opt_set_int, &urr.cr.processes, 0},
	{"rawrouter-workers", required_argument, 0, "prefork the specified number of rawrouter processes", uwsgi_opt_set_int, &urr.cr.processes, 0},
	{"rawrouter-zerg", required_argument, 0, "attach the rawrouter to a zerg server", uwsgi_opt_corerouter_zerg, &urr, 0},
	{"rawrouter-use-cache", optional_argument, 0, "use uWSGI cache as hostname->server mapper for the rawrouter", uwsgi_opt_set_str, &urr.cr.use_cache, 0},

	{"rawrouter-use-pattern", required_argument, 0, "use a pattern for rawrouter hostname->server mapping", uwsgi_opt_corerouter_use_pattern, &urr, 0},
	{"rawrouter-use-base", required_argument, 0, "use a base dir for rawrouter hostname->server mapping", uwsgi_opt_corerouter_use_base, &urr, 0},

	{"rawrouter-fallback", required_argument, 0, "fallback to the specified node in case of error", uwsgi_opt_add_string_list, &urr.cr.fallback, 0},

	{"rawrouter-use-code-string", required_argument, 0, "use code string as hostname->server mapper for the rawrouter", uwsgi_opt_corerouter_cs, &urr, 0},
	{"rawrouter-use-socket", optional_argument, 0, "forward request to the specified uwsgi socket", uwsgi_opt_corerouter_use_socket, &urr, 0},
	{"rawrouter-to", required_argument, 0, "forward requests to the specified uwsgi server (you can specify it multiple times for load balancing)", uwsgi_opt_add_string_list, &urr.cr.static_nodes, 0},
	{"rawrouter-gracetime", required_argument, 0, "retry connections to dead static nodes after the specified amount of seconds", uwsgi_opt_set_int, &urr.cr.static_node_gracetime, 0},
	{"rawrouter-events", required_argument, 0, "set the maximum number of concurrent events", uwsgi_opt_set_int, &urr.cr.nevents, 0},
	{"rawrouter-max-retries", required_argument, 0, "set the maximum number of retries/fallbacks to other nodes", uwsgi_opt_set_int, &urr.cr.max_retries, 0},
	{"rawrouter-quiet", required_argument, 0, "do not report failed connections to instances", uwsgi_opt_true, &urr.cr.quiet, 0},
	{"rawrouter-cheap", no_argument, 0, "run the rawrouter in cheap mode", uwsgi_opt_true, &urr.cr.cheap, 0},
	{"rawrouter-subscription-server", required_argument, 0, "run the rawrouter subscription server on the spcified address", uwsgi_opt_corerouter_ss, &urr, 0},
	{"rawrouter-subscription-slot", required_argument, 0, "*** deprecated ***", uwsgi_opt_deprecated, (void *) "useless thanks to the new implementation", 0},

	{"rawrouter-timeout", required_argument, 0, "set rawrouter timeout", uwsgi_opt_set_int, &urr.cr.socket_timeout, 0},

	{"rawrouter-stats", required_argument, 0, "run the rawrouter stats server", uwsgi_opt_set_str, &urr.cr.stats_server, 0},
	{"rawrouter-stats-server", required_argument, 0, "run the rawrouter stats server", uwsgi_opt_set_str, &urr.cr.stats_server, 0},
	{"rawrouter-ss", required_argument, 0, "run the rawrouter stats server", uwsgi_opt_set_str, &urr.cr.stats_server, 0},
	{"rawrouter-harakiri", required_argument, 0, "enable rawrouter harakiri", uwsgi_opt_set_int, &urr.cr.harakiri, 0},

	{"rawrouter-xclient", no_argument, 0, "use the xclient protocol to pass the client address", uwsgi_opt_true, &urr.xclient, 0},

	{"rawrouter-buffer-size", required_argument, 0, "set internal buffer size (default: page size)", uwsgi_opt_set_64bit, &urr.cr.buffer_size, 0},

	{0, 0, 0, 0, 0, 0, 0},
};

// write to backend
static ssize_t rr_instance_write(struct corerouter_peer *peer) {
	ssize_t len = cr_write(peer, "rr_instance_write()");
	// end on empty write
	if (!len) return 0;

	// the chunk has been sent, start (again) reading from client and instances
	if (cr_write_complete(peer)) {
		// reset the buffer
		peer->out->pos = 0;
		cr_reset_hooks(peer);
	}

	return len;
}

// write to client
static ssize_t rr_write(struct corerouter_peer *main_peer) {
	ssize_t len = cr_write(main_peer, "rr_write()");
	// end on empty write
	if (!len) return 0;

	// ok this response chunk is sent, let's start reading again
	if (cr_write_complete(main_peer)) {
		// reset the buffer
		main_peer->out->pos = 0;
                cr_reset_hooks(main_peer);
        } 

	return len;
}

// read from backend
static ssize_t rr_instance_read(struct corerouter_peer *peer) {
	ssize_t len = cr_read(peer, "rr_instance_read()");
	if (!len) return 0;

	// set the input buffer as the main output one
	peer->session->main_peer->out = peer->in;
	peer->session->main_peer->out_pos = 0;

	cr_write_to_main(peer, rr_write);
	return len;
}

// write the xclient banner
static ssize_t rr_xclient_write(struct corerouter_peer *peer) {
        struct corerouter_session *cs = peer->session;
        struct rawrouter_session *rr = (struct rawrouter_session *) cs;
        ssize_t len = cr_write_buf(peer, rr->xclient, "rr_xclient_write()");
        if (!len) return 0;

        if (cr_write_complete_buf(peer, rr->xclient)) {
                if (peer->session->main_peer->out_pos > 0) {
                        // (eventually) send previous data
			peer->last_hook_read = rr_instance_read;
                        cr_write_to_main(peer, rr_write);
                }
                else {
                        // reset to standard behaviour
			peer->in->pos = 0;
			cr_reset_hooks_and_read(peer, rr_instance_read);
                }
        }

        return len;
}

// read the first line from the backend and skip it
static ssize_t rr_xclient_read(struct corerouter_peer *peer) {
	struct corerouter_session *cs = peer->session;
        struct rawrouter_session *rr = (struct rawrouter_session *) cs;
        ssize_t len = cr_read(peer, "rr_xclient_read()");
	if (!len) return 0;

	char *ptr = (peer->in->buf + peer->in->pos) - len;
	ssize_t i;
	for(i=0;i<len;i++) {
		if (rr->xclient_rn == 1) {
			if (ptr[i] != '\n') {
				return -1;
			}
			// banner received (skip it, will be sent later)
			size_t remains = len - (i+1);
			if (remains > 0) {
				peer->session->main_peer->out = peer->in;
				peer->session->main_peer->out_pos = (peer->in->pos - remains) ;
			}
			cr_write_to_backend(peer, rr_xclient_write);
			return len;
		}
		else if (ptr[i] == '\r') {
			rr->xclient_rn = 1;
		}
	}

	return len;
}

// the instance is connected now we cannot retry connections
static ssize_t rr_instance_connected(struct corerouter_peer *peer) {

	struct corerouter_session *cs = peer->session;
        struct rawrouter_session *rr = (struct rawrouter_session *) cs;
	cr_peer_connected(peer, "rr_instance_connected()");

	peer->can_retry = 0;

	if (rr->xclient) {
		cr_reset_hooks_and_read(peer, rr_xclient_read);
		return 1;
	}
	cr_reset_hooks_and_read(peer, rr_instance_read);
	return 1;
}

// read from client
static ssize_t rr_read(struct corerouter_peer *main_peer) {
	ssize_t len = cr_read(main_peer, "rr_read()");
	if (!len) return 0;

	main_peer->session->peers->out = main_peer->in;
	main_peer->session->peers->out_pos = 0;

	cr_write_to_backend(main_peer->session->peers, rr_instance_write);
	return len;
}

// retry the connection
static int rr_retry(struct corerouter_peer *peer) {

	struct corerouter_session *cs = peer->session;
	struct uwsgi_corerouter *ucr = cs->corerouter;

	if (peer->instance_address_len > 0) goto retry;

	if (ucr->mapper(ucr, peer)) {
                return -1;
        }

        if (peer->instance_address_len == 0) {
                return -1;
	}

retry:
	// start async connect (again)
	cr_connect(peer, rr_instance_connected);
	return 0;
}

static void rr_session_close(struct corerouter_session *cs) {
	struct rawrouter_session *rr = (struct rawrouter_session *) cs;
	if (rr->xclient) {
		uwsgi_buffer_destroy(rr->xclient);
	}
}

// allocate a new session
static int rawrouter_alloc_session(struct uwsgi_corerouter *ucr, struct uwsgi_gateway_socket *ugs, struct corerouter_session *cs, struct sockaddr *sa, socklen_t s_len) {

	// set default read hook
	cs->main_peer->last_hook_read = rr_read;
	// set close hook
	cs->close = rr_session_close;
	// set retry hook
	cs->retry = rr_retry;

	if (sa && sa->sa_family == AF_INET) {
		if (urr.xclient) {
			struct rawrouter_session *rr = (struct rawrouter_session *) cs;
			rr->xclient = uwsgi_buffer_new(13+sizeof(cs->client_address)+2);
			if (uwsgi_buffer_append(rr->xclient, "XCLIENT ADDR=", 13)) return -1;
			if (uwsgi_buffer_append(rr->xclient, cs->client_address, strlen(cs->client_address))) return -1;
			if (uwsgi_buffer_append(rr->xclient, "\r\n", 2)) return -1;
		}
        }

	// add a new peer
	struct corerouter_peer *peer = uwsgi_cr_peer_add(cs);

	// set default peer hook
	peer->last_hook_read = rr_instance_read;

	// use the address as hostname
        memcpy(peer->key, cs->ugs->name, cs->ugs->name_len);
        peer->key_len = cs->ugs->name_len;

        // the mapper hook
        if (ucr->mapper(ucr, peer)) {
		return -1;
	}

        if (peer->instance_address_len == 0) {
		return -1;
        }

	peer->can_retry = 1;
	cr_connect(peer, rr_instance_connected);
	return 0;
}

static int rawrouter_init() {

	urr.cr.session_size = sizeof(struct rawrouter_session);
	urr.cr.alloc_session = rawrouter_alloc_session;
	uwsgi_corerouter_init((struct uwsgi_corerouter *) &urr);

	return 0;
}

static void rawrouter_setup() {
	urr.cr.name = uwsgi_str("uWSGI rawrouter");
	urr.cr.short_name = uwsgi_str("rawrouter");
}

struct uwsgi_plugin rawrouter_plugin = {

	.name = "rawrouter",
	.options = rawrouter_options,
	.init = rawrouter_init,
	.on_load = rawrouter_setup
};
