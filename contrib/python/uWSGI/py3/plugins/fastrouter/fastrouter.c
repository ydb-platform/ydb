#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI fastrouter

*/

#include <uwsgi.h>
#include "../corerouter/cr.h"

static struct uwsgi_fastrouter {
	struct uwsgi_corerouter cr;
} ufr;

extern struct uwsgi_server uwsgi;

struct fastrouter_session {
	struct corerouter_session session;
	int has_key;
	uint64_t content_length;
	uint64_t buffered;
};

static struct uwsgi_option fastrouter_options[] = {
	{"fastrouter", required_argument, 0, "run the fastrouter on the specified port", uwsgi_opt_corerouter, &ufr, 0},
	{"fastrouter-processes", required_argument, 0, "prefork the specified number of fastrouter processes", uwsgi_opt_set_int, &ufr.cr.processes, 0},
	{"fastrouter-workers", required_argument, 0, "prefork the specified number of fastrouter processes", uwsgi_opt_set_int, &ufr.cr.processes, 0},
	{"fastrouter-zerg", required_argument, 0, "attach the fastrouter to a zerg server", uwsgi_opt_corerouter_zerg, &ufr, 0},
	{"fastrouter-use-cache", optional_argument, 0, "use uWSGI cache as hostname->server mapper for the fastrouter", uwsgi_opt_set_str, &ufr.cr.use_cache, 0},

	{"fastrouter-use-pattern", required_argument, 0, "use a pattern for fastrouter hostname->server mapping", uwsgi_opt_corerouter_use_pattern, &ufr, 0},
	{"fastrouter-use-base", required_argument, 0, "use a base dir for fastrouter hostname->server mapping", uwsgi_opt_corerouter_use_base, &ufr, 0},

	{"fastrouter-fallback", required_argument, 0, "fallback to the specified node in case of error", uwsgi_opt_add_string_list, &ufr.cr.fallback, 0},

	{"fastrouter-use-code-string", required_argument, 0, "use code string as hostname->server mapper for the fastrouter", uwsgi_opt_corerouter_cs, &ufr, 0},
	{"fastrouter-use-socket", optional_argument, 0, "forward request to the specified uwsgi socket", uwsgi_opt_corerouter_use_socket, &ufr, 0},
	{"fastrouter-to", required_argument, 0, "forward requests to the specified uwsgi server (you can specify it multiple times for load balancing)", uwsgi_opt_add_string_list, &ufr.cr.static_nodes, 0},
	{"fastrouter-gracetime", required_argument, 0, "retry connections to dead static nodes after the specified amount of seconds", uwsgi_opt_set_int, &ufr.cr.static_node_gracetime, 0},
	{"fastrouter-events", required_argument, 0, "set the maximum number of concurrent events", uwsgi_opt_set_int, &ufr.cr.nevents, 0},
	{"fastrouter-quiet", required_argument, 0, "do not report failed connections to instances", uwsgi_opt_true, &ufr.cr.quiet, 0},
	{"fastrouter-cheap", no_argument, 0, "run the fastrouter in cheap mode", uwsgi_opt_true, &ufr.cr.cheap, 0},
	{"fastrouter-subscription-server", required_argument, 0, "run the fastrouter subscription server on the specified address", uwsgi_opt_corerouter_ss, &ufr, 0},
	{"fastrouter-subscription-slot", required_argument, 0, "*** deprecated ***", uwsgi_opt_deprecated, (void *) "useless thanks to the new implementation", 0},

	{"fastrouter-timeout", required_argument, 0, "set fastrouter timeout", uwsgi_opt_set_int, &ufr.cr.socket_timeout, 0},
	{"fastrouter-post-buffering", required_argument, 0, "enable fastrouter post buffering", uwsgi_opt_set_64bit, &ufr.cr.post_buffering, 0},
	{"fastrouter-post-buffering-dir", required_argument, 0, "put fastrouter buffered files to the specified directory (noop, use TMPDIR env)", uwsgi_opt_set_str, &ufr.cr.pb_base_dir, 0},

	{"fastrouter-stats", required_argument, 0, "run the fastrouter stats server", uwsgi_opt_set_str, &ufr.cr.stats_server, 0},
	{"fastrouter-stats-server", required_argument, 0, "run the fastrouter stats server", uwsgi_opt_set_str, &ufr.cr.stats_server, 0},
	{"fastrouter-ss", required_argument, 0, "run the fastrouter stats server", uwsgi_opt_set_str, &ufr.cr.stats_server, 0},
	{"fastrouter-harakiri", required_argument, 0, "enable fastrouter harakiri", uwsgi_opt_set_int, &ufr.cr.harakiri, 0},

	{"fastrouter-uid", required_argument, 0, "drop fastrouter privileges to the specified uid", uwsgi_opt_uid, &ufr.cr.uid, 0 },
        {"fastrouter-gid", required_argument, 0, "drop fastrouter privileges to the specified gid", uwsgi_opt_gid, &ufr.cr.gid, 0 },
	{"fastrouter-resubscribe", required_argument, 0, "forward subscriptions to the specified subscription server", uwsgi_opt_add_string_list, &ufr.cr.resubscribe, 0},
	{"fastrouter-resubscribe-bind", required_argument, 0, "bind to the specified address when re-subscribing", uwsgi_opt_set_str, &ufr.cr.resubscribe_bind, 0},

	{"fastrouter-buffer-size", required_argument, 0, "set internal buffer size (default: page size)", uwsgi_opt_set_64bit, &ufr.cr.buffer_size, 0},
	{"fastrouter-fallback-on-no-key", no_argument, 0, "move to fallback node even if a subscription key is not found", uwsgi_opt_true, &ufr.cr.fallback_on_no_key, 0},
	{"fastrouter-subscription-fallback-key", required_argument, 0, "key to use for fallback fastrouter", uwsgi_opt_corerouter_fallback_key, &ufr.cr, 0},

	UWSGI_END_OF_OPTIONS
};

static void fr_get_hostname(char *key, uint16_t keylen, char *val, uint16_t vallen, void *data) {

	struct corerouter_peer *peer = (struct corerouter_peer *) data;
	struct fastrouter_session *fr = (struct fastrouter_session *) peer->session;

	//uwsgi_log("%.*s = %.*s\n", keylen, key, vallen, val);
	if (!uwsgi_strncmp("SERVER_NAME", 11, key, keylen) && !peer->key_len) {
		if (vallen <= 0xff) {
			memcpy(peer->key, val, vallen);
			peer->key_len = vallen;
		}
		return;
	}

	if (!uwsgi_strncmp("HTTP_HOST", 9, key, keylen) && !fr->has_key) {
		if (vallen <= 0xff) {
                        memcpy(peer->key, val, vallen);
                        peer->key_len = vallen;
                }
		return;
	}

	if (!uwsgi_strncmp("UWSGI_FASTROUTER_KEY", 20, key, keylen)) {
		if (vallen <= 0xff) {
			fr->has_key = 1;
                        memcpy(peer->key, val, vallen);
                        peer->key_len = vallen;
		}
		return;
	}

	if (!uwsgi_strncmp("REMOTE_ADDR", 11, key, keylen)) {
		if (vallen < sizeof(peer->session->client_address)) {
			strncpy(peer->session->client_address, val, vallen);
		}
                return;
        }

	if (!uwsgi_strncmp("REMOTE_PORT", 11, key, keylen)) {
		if (vallen < sizeof(peer->session->client_port)) {
			strncpy(peer->session->client_port, val, vallen);
		}
                return;
        }

	if (ufr.cr.post_buffering > 0) {
		if (!uwsgi_strncmp("CONTENT_LENGTH", 14, key, keylen)) {
			fr->content_length = uwsgi_str_num(val, vallen);
		}
	}
}

// writing client body to the instance
static ssize_t fr_instance_write_body(struct corerouter_peer *peer) {
	ssize_t len = cr_write(peer, "fr_instance_write_body()");
        // end on empty write
        if (!len) return 0;

        // the chunk has been sent, start (again) reading from client and instances
        if (cr_write_complete(peer)) {
                // reset the original read buffer
                peer->out->pos = 0;
                cr_reset_hooks(peer);
        }

        return len;
}


// read client body
static ssize_t fr_read_body(struct corerouter_peer *main_peer) {
	ssize_t len = cr_read(main_peer, "fr_read_body()");
        if (!len) return 0;

        main_peer->session->peers->out = main_peer->in;
        main_peer->session->peers->out_pos = 0;

        cr_write_to_backend(main_peer->session->peers, fr_instance_write_body);
        return len;	
}

// write to the client
static ssize_t fr_write(struct corerouter_peer *main_peer) {
	ssize_t len = cr_write(main_peer, "fr_write()");
        // end on empty write
        if (!len) return 0;

        // ok this response chunk is sent, let's start reading again
        if (cr_write_complete(main_peer)) {
                // reset the original read buffer
                main_peer->out->pos = 0;
                cr_reset_hooks(main_peer);
        }

        return len;
}

// data from instance
static ssize_t fr_instance_read(struct corerouter_peer *peer) {
	ssize_t len = cr_read(peer, "fr_instance_read()");
        if (!len) return 0;

        // set the input buffer as the main output one
        peer->session->main_peer->out = peer->in;
        peer->session->main_peer->out_pos = 0;

        cr_write_to_main(peer, fr_write);
        return len;
}

static ssize_t fr_instance_sendfile(struct corerouter_peer *peer) {
	struct fastrouter_session *fr = (struct fastrouter_session *) peer->session;
	ssize_t len = uwsgi_sendfile_do(peer->fd, peer->session->main_peer->buffering_fd, fr->buffered, fr->content_length - fr->buffered);
	if (len < 0) {
		cr_try_again;
		uwsgi_cr_error(peer, "fr_instance_sendfile()/sendfile()");
		return -1;
	}
	if (len == 0) return 0;
	fr->buffered += len;
	if (peer != peer->session->main_peer && peer->un) peer->un->rx+=len;
	if (fr->buffered >= fr->content_length) {
		cr_reset_hooks(peer);	
	}
	return len;
}

// send the uwsgi request header and vars
static ssize_t fr_instance_send_request(struct corerouter_peer *peer) {
	ssize_t len = cr_write(peer, "fr_instance_send_request()");
        // end on empty write
        if (!len) return 0;

        // the chunk has been sent, start (again) reading from client and instances
        if (cr_write_complete(peer)) {
                // reset the original read buffer
                peer->out->pos = 0;
		if (!peer->session->main_peer->is_buffering) {
			// start waiting for body
			peer->session->main_peer->last_hook_read = fr_read_body;
                	cr_reset_hooks(peer);
		}
		else {
			peer->hook_write = fr_instance_sendfile;
			// stop reading from the client
			peer->session->main_peer->last_hook_read = NULL;
		}
        }

	return len;
}

// instance is connected
static ssize_t fr_instance_connected(struct corerouter_peer *peer) {

	cr_peer_connected(peer, "fr_instance_connected()");

	// we are connected, we cannot retry anymore
	peer->can_retry = 0;

	// fix modifiers
	peer->session->main_peer->in->buf[0] = peer->modifier1;
	peer->session->main_peer->in->buf[3] = peer->modifier2;

	// prepare to write the uwsgi packet
	peer->out = peer->session->main_peer->in;
	peer->out_pos = 0;	

	peer->last_hook_write = fr_instance_send_request;
	return fr_instance_send_request(peer);
}

// called after receaving the uwsgi header (read vars)
static ssize_t fr_recv_uwsgi_vars(struct corerouter_peer *main_peer) {
	struct fastrouter_session *fr = (struct fastrouter_session *) main_peer->session;

	struct corerouter_peer *new_peer = NULL;
	ssize_t len = 0;

	struct uwsgi_header *uh = (struct uwsgi_header *) main_peer->in->buf;
	// better to store it as the original buf address could change
	uint16_t pktsize = uh->pktsize;

	// are we buffering ?
	if (main_peer->is_buffering) {
		// memory or disk ?
		if (fr->content_length <= ufr.cr.post_buffering) {
			// increase buffer if needed
        		if (uwsgi_buffer_fix(main_peer->in, pktsize+4+fr->content_length))
                		return -1;
        		len = cr_read_exact(main_peer, pktsize+4+fr->content_length, "fr_recv_uwsgi_vars()");
        		if (!len) return 0;
			// whole body read ?
			if (main_peer->in->pos == (size_t)(pktsize+4+fr->content_length)) {
				main_peer->is_buffering = 0;
				goto done;
			}
			return len;
		}
		// first round ?
		if (main_peer->buffering_fd == -1) {
			main_peer->buffering_fd = uwsgi_tmpfd();
			if (main_peer->buffering_fd < 0) return -1;
		}
		char buf[32768];
		size_t remains = fr->content_length - fr->buffered;
		ssize_t rlen = read(main_peer->fd, buf, UMIN(32768, remains));
		if (rlen < 0) {
			cr_try_again;
			uwsgi_cr_error(main_peer, "fr_recv_uwsgi_vars()/read()");
			return -1;
		}
		if (rlen == 0) return 0;
		fr->buffered += rlen;
		if (write(main_peer->buffering_fd, buf, rlen) != rlen) {
			uwsgi_cr_error(main_peer, "fr_recv_uwsgi_vars()/write()");
                        return -1;
		}

		// have we done ?
		if (fr->buffered >= fr->content_length) {
			fr->buffered = 0;
			len = rlen;
			goto done;
		}

		return rlen;
	}

	// increase buffer if needed
	if (uwsgi_buffer_fix(main_peer->in, pktsize+4))
		return -1;
	len = cr_read_exact(main_peer, pktsize+4, "fr_recv_uwsgi_vars()");
	if (!len) return 0;

	// headers received, ready to choose the instance
	if (main_peer->in->pos == (size_t)(pktsize+4)) {

		struct uwsgi_corerouter *ucr = main_peer->session->corerouter;

		new_peer = uwsgi_cr_peer_add(main_peer->session);
		new_peer->last_hook_read = fr_instance_read;

		// find the hostname
		if (uwsgi_hooked_parse(main_peer->in->buf+4, pktsize, fr_get_hostname, (void *) new_peer)) {
			return -1;
		}

		// check the hostname;
		if (new_peer->key_len == 0)
			return -1;

		// find an instance using the key
		if (ucr->mapper(ucr, new_peer))
			return -1;

		// check instance
		if (new_peer->instance_address_len == 0) {
			if (ufr.cr.fallback_on_no_key) {
				new_peer->failed = 1;
				new_peer->can_retry = 1;
				corerouter_close_peer(&ufr.cr, new_peer);
				return len;
			}
			return -1;
		}

		// buffering ?
		if (ufr.cr.post_buffering > 0 && fr->content_length > 0) {
			main_peer->is_buffering = 1;
			main_peer->buffering_fd = -1;
			return len;
		}

done:
		if (!new_peer) {
			new_peer = main_peer->session->peers;
		}

		new_peer->can_retry = 1;

		cr_connect(new_peer, fr_instance_connected);
	}

	return len;
}

// called soon after accept()
static ssize_t fr_recv_uwsgi_header(struct corerouter_peer *main_peer) {
	ssize_t len = cr_read_exact(main_peer, 4, "fr_recv_uwsgi_header()");
	if (!len) return 0;

	// header ready
	if (main_peer->in->pos == 4) {
		// change the reading default and current hook (simulate a reset hook but without syscall)
		// this is a special case for the fastrouter as it changes its hook without changing the event mapping
		main_peer->last_hook_read = fr_recv_uwsgi_vars;
		main_peer->hook_read = fr_recv_uwsgi_vars;
		return fr_recv_uwsgi_vars(main_peer);
	}

	return len;
}

// retry connection to the backend
static int fr_retry(struct corerouter_peer *peer) {

        struct uwsgi_corerouter *ucr = peer->session->corerouter;

        if (peer->instance_address_len > 0) goto retry;

        if (ucr->mapper(ucr, peer)) {
                return -1;
        }

        if (peer->instance_address_len == 0) {
                return -1;
        }

retry:
        // start async connect (again)
        cr_connect(peer, fr_instance_connected);
        return 0;
}


// called when a new session is created
static int fastrouter_alloc_session(struct uwsgi_corerouter *ucr, struct uwsgi_gateway_socket *ugs, struct corerouter_session *cs, struct sockaddr *sa, socklen_t s_len) {
	// set the retry hook
	cs->retry = fr_retry;
	// wait for requests...
	if (uwsgi_cr_set_hooks(cs->main_peer, fr_recv_uwsgi_header, NULL)) return -1;
	return 0;
}

static int fastrouter_init() {

	ufr.cr.session_size = sizeof(struct fastrouter_session);
	ufr.cr.alloc_session = fastrouter_alloc_session;
	uwsgi_corerouter_init((struct uwsgi_corerouter *) &ufr);

	return 0;
}

static void fastrouter_setup() {
	ufr.cr.name = uwsgi_str("uWSGI fastrouter");
	ufr.cr.short_name = uwsgi_str("fastrouter");
}


struct uwsgi_plugin fastrouter_plugin = {

	.name = "fastrouter",
	.options = fastrouter_options,
	.init = fastrouter_init,
	.on_load = fastrouter_setup
};
