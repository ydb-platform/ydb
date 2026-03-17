#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_SSL

/*

   uWSGI sslrouter

*/

#include "../corerouter/cr.h"

extern struct uwsgi_server uwsgi;

static struct uwsgi_sslrouter {
	struct uwsgi_corerouter cr;
	char *ssl_session_context;
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
	int sni;
#endif
} usr;

struct sslrouter_session {
	struct corerouter_session session;
	SSL *ssl;
};

static void uwsgi_opt_sslrouter(char *opt, char *value, void *cr) {
        struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        char *client_ca = NULL;

        // build socket, certificate and key file
        char *sock = uwsgi_str(value);
        char *crt = strchr(sock, ',');
        if (!crt) {
                uwsgi_log("invalid sslrouter syntax must be socket,crt,key\n");
                exit(1);
        }
        *crt = '\0'; crt++;
        char *key = strchr(crt, ',');
        if (!key) {
                uwsgi_log("invalid sslrouter syntax must be socket,crt,key\n");
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

        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(sock, ucr->name);
        // ok we have the socket, initialize ssl if required
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        // initialize ssl context
        char *name = usr.ssl_session_context;
        if (!name) {
                name = uwsgi_concat3(ucr->short_name, "-", ugs->name);
        }

        ugs->ctx = uwsgi_ssl_new_server_context(name, crt, key, ciphers, client_ca);
        if (!ugs->ctx) {
                exit(1);
        }

        ucr->has_sockets++;
}


static void uwsgi_opt_sslrouter2(char *opt, char *value, void *cr) {
        struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;

        char *s2_addr = NULL;
        char *s2_cert = NULL;
        char *s2_key = NULL;
        char *s2_ciphers = NULL;
        char *s2_clientca = NULL;

        if (uwsgi_kvlist_parse(value, strlen(value), ',', '=',
                        "addr", &s2_addr,
                        "cert", &s2_cert,
                        "crt", &s2_cert,
                        "key", &s2_key,
                        "ciphers", &s2_ciphers,
                        "clientca", &s2_clientca,
                        "client_ca", &s2_clientca,
                        NULL)) {
                uwsgi_log("error parsing --sslrouter option\n");
                exit(1);
        }

        if (!s2_addr || !s2_cert || !s2_key) {
                uwsgi_log("--sslrouter option needs addr, cert and key items\n");
                exit(1);
        }

        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(s2_addr, ucr->name);
        // ok we have the socket, initialize ssl if required
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }
        
        // initialize ssl context
        char *name = usr.ssl_session_context;
        if (!name) {
                name = uwsgi_concat3(ucr->short_name, "-", ugs->name);
        }

	ugs->ctx = uwsgi_ssl_new_server_context(name, s2_cert, s2_key, s2_ciphers, s2_clientca);
        if (!ugs->ctx) {
                exit(1);
        }
        ucr->has_sockets++;
}

        
static struct uwsgi_option sslrouter_options[] = {
	{"sslrouter", required_argument, 0, "run the sslrouter on the specified port", uwsgi_opt_sslrouter, &usr, 0},
	{"sslrouter2", required_argument, 0, "run the sslrouter on the specified port (key-value based)", uwsgi_opt_sslrouter2, &usr, 0},
	{"sslrouter-session-context", required_argument, 0, "set the session id context to the specified value", uwsgi_opt_set_str, &usr.ssl_session_context, 0},
	{"sslrouter-processes", required_argument, 0, "prefork the specified number of sslrouter processes", uwsgi_opt_set_int, &usr.cr.processes, 0},
	{"sslrouter-workers", required_argument, 0, "prefork the specified number of sslrouter processes", uwsgi_opt_set_int, &usr.cr.processes, 0},
	{"sslrouter-zerg", required_argument, 0, "attach the sslrouter to a zerg server", uwsgi_opt_corerouter_zerg, &usr, 0},
	{"sslrouter-use-cache", optional_argument, 0, "use uWSGI cache as hostname->server mapper for the sslrouter", uwsgi_opt_set_str, &usr.cr.use_cache, 0},

	{"sslrouter-use-pattern", required_argument, 0, "use a pattern for sslrouter hostname->server mapping", uwsgi_opt_corerouter_use_pattern, &usr, 0},
	{"sslrouter-use-base", required_argument, 0, "use a base dir for sslrouter hostname->server mapping", uwsgi_opt_corerouter_use_base, &usr, 0},

	{"sslrouter-fallback", required_argument, 0, "fallback to the specified node in case of error", uwsgi_opt_add_string_list, &usr.cr.fallback, 0},

	{"sslrouter-use-code-string", required_argument, 0, "use code string as hostname->server mapper for the sslrouter", uwsgi_opt_corerouter_cs, &usr, 0},
	{"sslrouter-use-socket", optional_argument, 0, "forward request to the specified uwsgi socket", uwsgi_opt_corerouter_use_socket, &usr, 0},
	{"sslrouter-to", required_argument, 0, "forward requests to the specified uwsgi server (you can specify it multiple times for load balancing)", uwsgi_opt_add_string_list, &usr.cr.static_nodes, 0},
	{"sslrouter-gracetime", required_argument, 0, "retry connections to dead static nodes after the specified amount of seconds", uwsgi_opt_set_int, &usr.cr.static_node_gracetime, 0},
	{"sslrouter-events", required_argument, 0, "set the maximum number of concurrent events", uwsgi_opt_set_int, &usr.cr.nevents, 0},
	{"sslrouter-max-retries", required_argument, 0, "set the maximum number of retries/fallbacks to other nodes", uwsgi_opt_set_int, &usr.cr.max_retries, 0},
	{"sslrouter-quiet", required_argument, 0, "do not report failed connections to instances", uwsgi_opt_true, &usr.cr.quiet, 0},
	{"sslrouter-cheap", no_argument, 0, "run the sslrouter in cheap mode", uwsgi_opt_true, &usr.cr.cheap, 0},
	{"sslrouter-subscription-server", required_argument, 0, "run the sslrouter subscription server on the spcified address", uwsgi_opt_corerouter_ss, &usr, 0},

	{"sslrouter-timeout", required_argument, 0, "set sslrouter timeout", uwsgi_opt_set_int, &usr.cr.socket_timeout, 0},

	{"sslrouter-stats", required_argument, 0, "run the sslrouter stats server", uwsgi_opt_set_str, &usr.cr.stats_server, 0},
	{"sslrouter-stats-server", required_argument, 0, "run the sslrouter stats server", uwsgi_opt_set_str, &usr.cr.stats_server, 0},
	{"sslrouter-ss", required_argument, 0, "run the sslrouter stats server", uwsgi_opt_set_str, &usr.cr.stats_server, 0},
	{"sslrouter-harakiri", required_argument, 0, "enable sslrouter harakiri", uwsgi_opt_set_int, &usr.cr.harakiri, 0},

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
	{"sslrouter-sni", no_argument, 0, "use SNI to route requests", uwsgi_opt_true, &usr.sni, 0},
#endif
	{"sslrouter-buffer-size", required_argument, 0, "set internal buffer size (default: page size)", uwsgi_opt_set_64bit, &usr.cr.buffer_size, 0},

	{0, 0, 0, 0, 0, 0, 0},
};

static ssize_t sr_write(struct corerouter_peer *);

// write to backend
static ssize_t sr_instance_write(struct corerouter_peer *peer) {
	ssize_t len = cr_write(peer, "sr_instance_write()");
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

// read from backend
static ssize_t sr_instance_read(struct corerouter_peer *peer) {
	ssize_t len = cr_read(peer, "sr_instance_read()");
	if (!len) return 0;

	// set the input buffer as the main output one
	peer->session->main_peer->out = peer->in;
	peer->session->main_peer->out_pos = 0;

	cr_write_to_main(peer, sr_write);
	return len;
}

// the instance is connected now we cannot retry connections
static ssize_t sr_instance_connected(struct corerouter_peer *peer) {
	cr_peer_connected(peer, "sr_instance_connected()");
	peer->can_retry = 0;
	// set the output buffer as the main_peer output one
	peer->out = peer->session->main_peer->in;
        peer->out_pos = 0;
	return sr_instance_write(peer);
}

// retry the connection
static int sr_retry(struct corerouter_peer *peer) {

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
	cr_connect(peer, sr_instance_connected);
	return 0;
}

static ssize_t sr_write(struct corerouter_peer *main_peer) {
        struct corerouter_session *cs = main_peer->session;
        struct sslrouter_session *sr = (struct sslrouter_session *) cs;

        int ret = SSL_write(sr->ssl, main_peer->out->buf + main_peer->out_pos, main_peer->out->pos - main_peer->out_pos);
        if (ret > 0) {
                main_peer->out_pos += ret;
                if (main_peer->out->pos == main_peer->out_pos) {
                        main_peer->out->pos = 0;
                        cr_reset_hooks(main_peer);
                }
                return ret;
        }
        if (ret == 0) return 0;
        int err = SSL_get_error(sr->ssl, ret);

        if (err == SSL_ERROR_WANT_READ) {
                cr_reset_hooks_and_read(main_peer, sr_write);
                return 1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
                cr_write_to_main(main_peer, sr_write);
                return 1;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_cr_error(main_peer, "sr_write()");
        }

        else if (err == SSL_ERROR_SSL && uwsgi.ssl_verbose) {
                ERR_print_errors_fp(stderr);
        }

        return -1;
}

static ssize_t sr_read(struct corerouter_peer *main_peer) {
        struct corerouter_session *cs = main_peer->session;
        struct sslrouter_session *sr = (struct sslrouter_session *) cs;

        int ret = SSL_read(sr->ssl, main_peer->in->buf + main_peer->in->pos, main_peer->in->len - main_peer->in->pos);
        if (ret > 0) {
                // fix the buffer
                main_peer->in->pos += ret;
                // check for pending data
                int ret2 = SSL_pending(sr->ssl);
                if (ret2 > 0) {
                        if (uwsgi_buffer_fix(main_peer->in, main_peer->in->len + ret2 )) {
                                uwsgi_cr_log(main_peer, "cannot fix the buffer to %d\n", main_peer->in->len + ret2);
                                return -1;
                        }
                        if (SSL_read(sr->ssl, main_peer->in->buf + main_peer->in->pos, ret2) != ret2) {
                                uwsgi_cr_log(main_peer, "SSL_read() on %d bytes of pending data failed\n", ret2);
                                return -1;
                        }
                        // fix the buffer
                        main_peer->in->pos += ret2;
                }
		if (!main_peer->session->peers) {
			// add a new peer
        		struct corerouter_peer *peer = uwsgi_cr_peer_add(cs);
        		// set default peer hook
        		peer->last_hook_read = sr_instance_read;
        		// use the address as hostname
        		memcpy(peer->key, cs->ugs->name, cs->ugs->name_len);
        		peer->key_len = cs->ugs->name_len;

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
			if (usr.sni) {
				const char *servername = SSL_get_servername(sr->ssl, TLSEXT_NAMETYPE_host_name);
				if (servername && strlen(servername) <= 0xff) {
        				peer->key_len = strlen(servername);
        				memcpy(peer->key, servername, peer->key_len);
				}
			}
#endif
        		// the mapper hook
        		if (cs->corerouter->mapper(cs->corerouter, peer)) {
                		return -1;
        		}

        		if (peer->instance_address_len == 0) {
                		return -1;
        		}

			peer->can_retry = 1;
        		cr_connect(peer, sr_instance_connected);
			return 1;
		}
		main_peer->session->peers->out = main_peer->in;
        	main_peer->session->peers->out_pos = 0;
		cr_write_to_backend(main_peer, sr_instance_write);
		return ret;
        }
        if (ret == 0) return 0;
        int err = SSL_get_error(sr->ssl, ret);

        if (err == SSL_ERROR_WANT_READ) {
                cr_reset_hooks_and_read(main_peer, sr_read);
                return 1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
                cr_write_to_main(main_peer, sr_read);
                return 1;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_cr_error(main_peer, "sr_ssl_read()");
        }

        else if (err == SSL_ERROR_SSL && uwsgi.ssl_verbose) {
                ERR_print_errors_fp(stderr);
        }

        return -1;
}

static void sr_session_close(struct corerouter_session *cs) {
	struct sslrouter_session *sr = (struct sslrouter_session *) cs;
	// clear the errors (otherwise they could be propagated)
        ERR_clear_error();
        SSL_free(sr->ssl);
}

// allocate a new session
static int sslrouter_alloc_session(struct uwsgi_corerouter *ucr, struct uwsgi_gateway_socket *ugs, struct corerouter_session *cs, struct sockaddr *sa, socklen_t s_len) {

	// set close hook
	cs->close = sr_session_close;
	// set retry hook
	cs->retry = sr_retry;

	struct sslrouter_session *sr = (struct sslrouter_session *) cs;

	sr->ssl = SSL_new(ugs->ctx);
        SSL_set_fd(sr->ssl, cs->main_peer->fd);
        SSL_set_accept_state(sr->ssl);

	if (uwsgi_cr_set_hooks(cs->main_peer, sr_read, NULL))
		return -1;

	return 0;
}

static int sslrouter_init() {

	usr.cr.session_size = sizeof(struct sslrouter_session);
	usr.cr.alloc_session = sslrouter_alloc_session;
	uwsgi_corerouter_init((struct uwsgi_corerouter *) &usr);

	return 0;
}

static void sslrouter_setup() {
	usr.cr.name = uwsgi_str("uWSGI sslrouter");
	usr.cr.short_name = uwsgi_str("sslrouter");
}

struct uwsgi_plugin sslrouter_plugin = {

	.name = "sslrouter",
	.options = sslrouter_options,
	.init = sslrouter_init,
	.on_load = sslrouter_setup
};

#else
struct uwsgi_plugin sslrouter_plugin = {
	.name = "sslrouter",
};
#endif
