#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI corerouter

   requires:

   - async
   - caching
   - pcre (optional)

*/

#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

#include "cr.h"

struct corerouter_peer *uwsgi_cr_peer_find_by_sid(struct corerouter_session *cs, uint32_t sid) {

	struct corerouter_peer *peers = cs->peers;
	while(peers) {
		if (peers->sid == sid) {
			return peers;
		}
		peers = peers->next;
	}
	return NULL;
}

// add a new peer to the session
struct corerouter_peer *uwsgi_cr_peer_add(struct corerouter_session *cs) {
	struct corerouter_peer *old_peers = NULL, *peers = cs->peers; 
	
	while(peers) {
		old_peers = peers;
		peers = peers->next;
	}

	peers = uwsgi_calloc(sizeof(struct corerouter_peer));
	peers->session = cs;
	peers->fd = -1;
	// create input buffer
	size_t bufsize = cs->corerouter->buffer_size;
	if (!bufsize) bufsize = uwsgi.page_size;
	peers->in = uwsgi_buffer_new(bufsize);
	// add timeout
	peers->current_timeout = cs->corerouter->socket_timeout;
        peers->timeout = cr_add_timeout(cs->corerouter, peers);
	peers->prev = old_peers;

	if (old_peers) {
		old_peers->next = peers;
	}
	else {
		cs->peers = peers;
	}

	return peers;
}

// reset a peer (allows it to connect to another backend)
void uwsgi_cr_peer_reset(struct corerouter_peer *peer) {
	if (peer->tmp_socket_name) {
		free(peer->tmp_socket_name);
		peer->tmp_socket_name = NULL;
	}
	cr_del_timeout(peer->session->corerouter, peer);
	
	if (peer->fd != -1) {
		close(peer->fd);
		peer->session->corerouter->cr_table[peer->fd] = NULL;
		peer->fd = -1;
		peer->hook_read = NULL;
		peer->hook_write = NULL;
	}

	if (peer->is_buffering) {
		if (peer->buffering_fd != -1) {
			close(peer->buffering_fd);
		}
	}

	peer->failed = 0;
	peer->soopt = 0;
	peer->timed_out = 0;

	peer->un = NULL;
	peer->static_node = NULL;
}

// destroy a peer
int uwsgi_cr_peer_del(struct corerouter_peer *peer) {
	// first of all check if we need to run a flush procedure
	if (peer->flush && !peer->is_flushing) {
		peer->is_flushing = 1;
		// on success, suspend the execution
		if (peer->flush(peer) > 0) return -1;
	}
	struct corerouter_peer *prev = peer->prev;
	struct corerouter_peer *next = peer->next;

	if (prev) {
		prev->next = peer->next;
	}

	if (next) {
		next->prev = peer->prev;
	}

	if (peer == peer->session->peers) {
		peer->session->peers = peer->next;
	}

	uwsgi_cr_peer_reset(peer);

	if (peer->in) {
		uwsgi_buffer_destroy(peer->in);
	}

	// main_peer bring the output buffer from backend peers
	if (peer->out && peer->out_need_free) {
		uwsgi_buffer_destroy(peer->out);
	}
	free(peer);
	return 0;
}

void uwsgi_opt_corerouter(char *opt, char *value, void *cr) {
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        uwsgi_new_gateway_socket(value, ucr->name);
        ucr->has_sockets++;
}

void uwsgi_opt_undeferred_corerouter(char *opt, char *value, void *cr) {
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(value, ucr->name);
	ugs->no_defer = 1;
        ucr->has_sockets++;
}

void uwsgi_opt_corerouter_use_socket(char *opt, char *value, void *cr) {
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        ucr->use_socket = 1;
	ucr->has_backends++;

        if (value) {
                ucr->socket_num = atoi(value);
        }
}

void uwsgi_opt_corerouter_use_base(char *opt, char *value, void *cr) {
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        ucr->base = value;
        ucr->base_len = strlen(ucr->base);
	ucr->has_backends++;
}

void uwsgi_opt_corerouter_use_pattern(char *opt, char *value, void *cr) {
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        ucr->pattern = value;
        ucr->pattern_len = strlen(ucr->pattern);
	ucr->has_backends++;
}


void uwsgi_opt_corerouter_zerg(char *opt, char *value, void *cr) {

        int j;
        int count = 8;
        struct uwsgi_gateway_socket *ugs;
	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;

        int zerg_fd = uwsgi_connect(value, 30, 0);
        if (zerg_fd < 0) {
                uwsgi_log("--- unable to connect to zerg server ---\n");
                exit(1);
        }

        int last_count = count;
        int *zerg = uwsgi_attach_fd(zerg_fd, &count, "uwsgi-zerg", 10);
        if (zerg == NULL) {
                if (last_count != count) {
                        close(zerg_fd);
                        zerg_fd = uwsgi_connect(value, 30, 0);
                        if (zerg_fd < 0) {
                                uwsgi_log("--- unable to connect to zerg server ---\n");
                                exit(1);
                        }
                        zerg = uwsgi_attach_fd(zerg_fd, &count, "uwsgi-zerg", 10);
                }
                else {
                        uwsgi_log("--- invalid data received from zerg-server ---\n");
                        exit(1);
                }
        }

        if (zerg == NULL) {
                uwsgi_log("--- invalid data received from zerg-server ---\n");
                exit(1);
        }


        close(zerg_fd);

        for(j=0;j<count;j++) {
                if (zerg[j] == -1) break;
                ugs = uwsgi_new_gateway_socket_from_fd(zerg[j], ucr->name);
                ugs->zerg = optarg;
        }
        free(zerg);
}


void uwsgi_opt_corerouter_cs(char *opt, char *value, void *cr) {

	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;

        char *cs = uwsgi_str(value);
               char *cs_code = strchr(cs, ':');
                if (!cs_code) {
                        uwsgi_log("invalid code_string option\n");
                        exit(1);
                }
                cs_code[0] = 0;
                char *cs_func = strchr(cs_code + 1, ':');
                if (!cs_func) {
                        uwsgi_log("invalid code_string option\n");
                        exit(1);
                }
                cs_func[0] = 0;
                ucr->code_string_modifier1 = atoi(cs);
                ucr->code_string_code = cs_code + 1;
                ucr->code_string_function = cs_func + 1;

	ucr->has_backends++;

}

void uwsgi_opt_corerouter_ss(char *opt, char *value, void *cr) {

	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(value, ucr->name);
        ugs->subscription = 1;
        ucr->has_subscription_sockets++;

	// this is the subscription hash table
	ucr->subscriptions = uwsgi_subscription_init_ht();

	ucr->has_backends++;

}


void uwsgi_opt_corerouter_fallback_key(char *opt, char *value, void *key) {
    struct uwsgi_corerouter *ptr = (struct uwsgi_corerouter *) key;
    if (!value) {
        ptr->fallback_key = NULL;
        ptr->fallback_key_len = 0;
        return;
    }
    ptr->fallback_key = value;
    ptr->fallback_key_len = strlen(value);
}


void corerouter_send_stats(struct uwsgi_corerouter *);

void corerouter_manage_subscription(char *key, uint16_t keylen, char *val, uint16_t vallen, void *data) {

	struct uwsgi_subscribe_req *usr = (struct uwsgi_subscribe_req *) data;

	if (!uwsgi_strncmp("key", 3, key, keylen)) {
		usr->key = val;
		usr->keylen = vallen;
	}
	else if (!uwsgi_strncmp("address", 7, key, keylen)) {
		usr->address = val;
		usr->address_len = vallen;
	}
	else if (!uwsgi_strncmp("modifier1", 9, key, keylen)) {
		usr->modifier1 = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("modifier2", 9, key, keylen)) {
		usr->modifier2 = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("cores", 5, key, keylen)) {
		usr->cores = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("load", 4, key, keylen)) {
		usr->load = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("weight", 6, key, keylen)) {
		usr->weight = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("unix", 4, key, keylen)) {
		usr->unix_check = uwsgi_str_num(val, vallen);
	}
	else if (!uwsgi_strncmp("sign", 4, key, keylen)) {
		usr->sign = val;
                usr->sign_len = vallen;
	}
	else if (!uwsgi_strncmp("sni_key", 7, key, keylen)) {
		usr->sni_key = val;
                usr->sni_key_len = vallen;
	}
	else if (!uwsgi_strncmp("sni_crt", 7, key, keylen)) {
		usr->sni_crt = val;
                usr->sni_crt_len = vallen;
	}
	else if (!uwsgi_strncmp("sni_ca", 6, key, keylen)) {
		usr->sni_ca = val;
                usr->sni_ca_len = vallen;
	}
	else if (!uwsgi_strncmp("notify", 6, key, keylen)) {
		usr->notify = val;
                usr->notify_len = vallen;
	}
}

void corerouter_close_peer(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	struct corerouter_session *cs = peer->session;

	
	// manage subscription reference count
	if (ucr->subscriptions && peer->un && peer->un->len > 0) {
                // decrease reference count
#ifdef UWSGI_DEBUG
               uwsgi_log("[1] node %.*s refcnt: %llu\n", peer->un->len, peer->un->name, peer->un->reference);
#endif
               peer->un->reference--;
#ifdef UWSGI_DEBUG
               uwsgi_log("[2] node %.*s refcnt: %llu\n", peer->un->len, peer->un->name, peer->un->reference);
#endif
        }

	if (peer->failed) {
		if (peer->soopt) {
                        if (!ucr->quiet)
                                uwsgi_log("[uwsgi-%s] unable to connect() to node \"%.*s\" (%d retries): %s\n", ucr->short_name, (int) peer->instance_address_len, peer->instance_address, peer->retries, strerror(peer->soopt));
                }
                else if (peer->timed_out) {
                        if (peer->instance_address_len > 0) {
                                if (peer->connecting) {
                                        if (!ucr->quiet)
                                                uwsgi_log("[uwsgi-%s] unable to connect() to node \"%.*s\" (%d retries): timeout\n", ucr->short_name, (int) peer->instance_address_len, peer->instance_address, peer->retries);
                                }
                        }
                }

                // now check for dead nodes
                if (ucr->subscriptions && peer->un && peer->un->len > 0) {

                        if (peer->un->death_mark == 0)
                                uwsgi_log("[uwsgi-%s] %.*s => marking %.*s as failed\n", ucr->short_name, (int) peer->key_len, peer->key, (int) peer->instance_address_len, peer->instance_address);

                        peer->un->failcnt++;
                        peer->un->death_mark = 1;
                        // check if i can remove the node
                        if (peer->un->reference == 0) {
                                uwsgi_remove_subscribe_node(ucr->subscriptions, peer->un);
                        }
                        if (ucr->cheap && !ucr->i_am_cheap && !ucr->fallback && uwsgi_no_subscriptions(ucr->subscriptions)) {
                                uwsgi_gateway_go_cheap(ucr->name, ucr->queue, &ucr->i_am_cheap);
                        }

                }
		else if (peer->static_node) {
			peer->static_node->custom = uwsgi_now();
			uwsgi_log("[uwsgi-%s] %.*s => marking %.*s as failed\n", ucr->short_name, (int) peer->key_len, peer->key, (int) peer->instance_address_len, peer->instance_address);
		}

		// check if the router supports the retry hook
		if (!peer->can_retry) goto end;
		if (peer->retries >= (size_t) ucr->max_retries) goto end;

		peer->retries++;	
		// reset the peer
		uwsgi_cr_peer_reset(peer);
		// set new timeout
		peer->timeout = cr_add_timeout(ucr, peer);

		if (ucr->fallback) {
			// ok let's try with the fallback nodes
                        if (!cs->fallback) {
                                cs->fallback = ucr->fallback;
                        }
                        else {
                                cs->fallback = cs->fallback->next;
                                if (!cs->fallback) goto end;
                        }

                        peer->instance_address = cs->fallback->value;
                        peer->instance_address_len = cs->fallback->len;

                        if (cs->retry(peer)) {
                                if (!peer->failed) goto end;
                        }
                        return;
                }

		peer->instance_address = NULL;
                peer->instance_address_len = 0;
                if (cs->retry(peer)) {
                        if (!peer->failed) goto end;
                }
		return;
	}

end:
	if (uwsgi_cr_peer_del(peer) < 0) return;

	if (peer == cs->main_peer) {
		cs->main_peer = NULL;
		corerouter_close_session(ucr, cs);
	}
	else {
		if (cs->can_keepalive == 0 && cs->wait_full_write == 0) {
			corerouter_close_session(ucr, cs);
		}
	}
}

// destroy a session
void corerouter_close_session(struct uwsgi_corerouter *ucr, struct corerouter_session *cr_session) {

	struct corerouter_peer *main_peer = cr_session->main_peer;
	if (main_peer) {
		if (uwsgi_cr_peer_del(main_peer) < 0) return;
	}

	// free peers
	struct corerouter_peer *peers = cr_session->peers;
	while(peers) {
		struct corerouter_peer *tmp_peer = peers;
		peers = peers->next;
		// special case here for subscription system
		if (ucr->subscriptions && tmp_peer->un && tmp_peer->un->len) {
			tmp_peer->un->reference--;
		}
		if (uwsgi_cr_peer_del(tmp_peer) < 0) return;
	}

	// could be used to free additional resources
	if (cr_session->close)
		cr_session->close(cr_session);

	free(cr_session);

	if (ucr->active_sessions == 0) {
		uwsgi_log("[BUG] number of active sessions already 0 !!!\n");
		return;
	}
	ucr->active_sessions--;
}

struct uwsgi_rb_timer *corerouter_reset_timeout(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	cr_del_timeout(ucr, peer);
	return cr_add_timeout(ucr, peer);
}

struct uwsgi_rb_timer *corerouter_reset_timeout_fast(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer, time_t now) {
        cr_del_timeout(ucr, peer);
        return cr_add_timeout_fast(ucr, peer, now);
}


static void corerouter_expire_timeouts(struct uwsgi_corerouter *ucr, time_t now) {

	uint64_t current = (uint64_t) now;
	struct uwsgi_rb_timer *urbt;
	struct corerouter_peer *peer;

	for (;;) {
		urbt = uwsgi_min_rb_timer(ucr->timeouts, NULL);
		if (urbt == NULL)
			return;

		if (urbt->value <= current) {
			peer = (struct corerouter_peer *) urbt->data;
			peer->timed_out = 1;
			if (peer->connecting) {
				peer->failed = 1;
			}
			corerouter_close_peer(ucr, peer);
			continue;
		}

		break;
	}
}

int uwsgi_cr_set_hooks(struct corerouter_peer *peer, ssize_t (*read_hook)(struct corerouter_peer *), ssize_t (*write_hook)(struct corerouter_peer *)) {
	struct corerouter_session *cs = peer->session;
	struct uwsgi_corerouter *ucr = cs->corerouter;

	//uwsgi_log("uwsgi_cr_set_hooks(%d, %p, %p)\n", peer->fd, read_hook, write_hook);

	if (read_hook) {
		peer->last_hook_read = read_hook;
	}

	if (write_hook) {
		peer->last_hook_write = write_hook;
	}

	int read_changed = 1;
	int write_changed = 1;

	if (read_hook && peer->hook_read) {
		read_changed = 0;
	}
	else if (!read_hook && !peer->hook_read) {
		read_changed = 0;
	}

	if (write_hook && peer->hook_write) {
		write_changed = 0;
	}
	else if (!write_hook && !peer->hook_write) {
		write_changed = 0;
	}

	if (!read_changed && !write_changed) {
		goto unchanged;
	}

	int has_read = 0;
	int has_write = 0;

	if (peer->hook_read) {
		has_read = 1;	
	}

	if (peer->hook_write) {
		has_write = 1;
	}

	if (!read_hook && !write_hook) {
		if (has_read) {
			if (event_queue_del_fd(ucr->queue, peer->fd, event_queue_read())) return -1;
		}
		if (has_write) {
			if (event_queue_del_fd(ucr->queue, peer->fd, event_queue_write())) return -1;
		}
	}
	else if (read_hook && write_hook) {
		if (has_read) {
			if (event_queue_fd_read_to_readwrite(ucr->queue, peer->fd)) return -1;
		}
		else if (has_write) {
			if (event_queue_fd_write_to_readwrite(ucr->queue, peer->fd)) return -1;
		}
	}
	else if (read_hook) {
		if (has_write) {
			if (write_changed) {
				if (event_queue_fd_write_to_read(ucr->queue, peer->fd)) return -1;
			}
			else {
				if (event_queue_fd_write_to_readwrite(ucr->queue, peer->fd)) return -1;
			}
		}
		else {
			if (event_queue_add_fd_read(ucr->queue, peer->fd)) return -1;
		}
	}
	else if (write_hook) {
		if (has_read) {
			if (read_changed) {
				if (event_queue_fd_read_to_write(ucr->queue, peer->fd)) return -1;
			}
			else {
				if (event_queue_fd_read_to_readwrite(ucr->queue, peer->fd)) return -1;
			}
		}
		else {
			if (event_queue_add_fd_write(ucr->queue, peer->fd)) return -1;
		}
	}

unchanged:

	peer->hook_read = read_hook;
	peer->hook_write = write_hook;
	return 0;

}

struct corerouter_session *corerouter_alloc_session(struct uwsgi_corerouter *ucr, struct uwsgi_gateway_socket *ugs, int new_connection, struct sockaddr *cr_addr, socklen_t cr_addr_len) {

	struct corerouter_session *cs = uwsgi_calloc(ucr->session_size);

	struct corerouter_peer *peer = uwsgi_calloc(sizeof(struct corerouter_peer));
	// main_peer has only input buffer as output buffer is taken from backend peers
	size_t bufsize = ucr->buffer_size;
	if (!bufsize) bufsize = uwsgi.page_size;
	peer->in = uwsgi_buffer_new(bufsize);

	ucr->cr_table[new_connection] = peer;
	cs->main_peer = peer;

	peer->fd = new_connection;
	peer->session = cs;

	// map corerouter and socket
	cs->corerouter = ucr;
	cs->ugs = ugs;

	// set initial timeout (could be overridden)
	peer->current_timeout = ucr->socket_timeout;

	ucr->active_sessions++;

	// build the client address
	memcpy(&cs->client_sockaddr, cr_addr, cr_addr_len);
	switch(cr_addr->sa_family) {
		case AF_INET:
			if (inet_ntop(AF_INET, &cs->client_sockaddr.sa_in.sin_addr, cs->client_address, sizeof(cs->client_address)) == NULL) {
				uwsgi_error("corerouter_alloc_session/inet_ntop()");
				memcpy(cs->client_address, "0.0.0.0", 7);
				cs->client_port[0] = '0';
				cs->client_port[1] = 0;
			}
			uwsgi_num2str2(cs->client_sockaddr.sa_in.sin_port, cs->client_port);
			break;
#ifdef AF_INET6
		case AF_INET6:
			if (inet_ntop(AF_INET6, &cs->client_sockaddr.sa_in6.sin6_addr, cs->client_address, sizeof(cs->client_address)) == NULL) {
				uwsgi_error("corerouter_alloc_session/inet_ntop()");
				memcpy(cs->client_address, "0.0.0.0", 7);
				cs->client_port[0] = '0';
				cs->client_port[1] = 0;
			}
			uwsgi_num2str2(cs->client_sockaddr.sa_in6.sin6_port, cs->client_port);
			break;
#endif
		default:
			memcpy(cs->client_address, "0.0.0.0", 7);
			cs->client_port[0] = '0';
			cs->client_port[1] = 0;
			break;
	}

	// here we prepare the real session and set the hooks
	if (ucr->alloc_session(ucr, ugs, cs, cr_addr, cr_addr_len)) {
		corerouter_close_session(ucr, cs);
		cs = NULL;
	}
	else {
		// truly set the timeout
        	peer->timeout = cr_add_timeout(ucr, ucr->cr_table[new_connection]);
	}

	return cs;
}

void uwsgi_corerouter_loop(int id, void *data) {

	int i;

	struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) data;

	ucr->cr_stats_server = -1;

	ucr->cr_table = uwsgi_malloc(sizeof(struct corerouter_session *) * uwsgi.max_fd);

	for (i = 0; i < (int) uwsgi.max_fd; i++) {
		ucr->cr_table[i] = NULL;
	}

	ucr->i_am_cheap = ucr->cheap;

	void *events = uwsgi_corerouter_setup_event_queue(ucr, id);

	if (ucr->has_subscription_sockets)
		event_queue_add_fd_read(ucr->queue, ushared->gateways[id].internal_subscription_pipe[1]);


	if (!ucr->socket_timeout)
		ucr->socket_timeout = 60;

	if (!ucr->static_node_gracetime)
		ucr->static_node_gracetime = 30;

	int i_am_the_first = 1;
	for(i=0;i<id;i++) {
		if (!strcmp(ushared->gateways[i].name, ucr->name)) {
			i_am_the_first = 0;
			break;
		}
	}

	if (ucr->stats_server && i_am_the_first) {
		char *tcp_port = strchr(ucr->stats_server, ':');
		if (tcp_port) {
			// disable deferred accept for this socket
			int current_defer_accept = uwsgi.no_defer_accept;
			uwsgi.no_defer_accept = 1;
			ucr->cr_stats_server = bind_to_tcp(ucr->stats_server, uwsgi.listen_queue, tcp_port);
			uwsgi.no_defer_accept = current_defer_accept;
		}
		else {
			ucr->cr_stats_server = bind_to_unix(ucr->stats_server, uwsgi.listen_queue, uwsgi.chmod_socket, uwsgi.abstract_socket);
		}

		event_queue_add_fd_read(ucr->queue, ucr->cr_stats_server);
		uwsgi_log("*** %s stats server enabled on %s fd: %d ***\n", ucr->short_name, ucr->stats_server, ucr->cr_stats_server);
	}


	if (ucr->use_socket) {
		ucr->to_socket = uwsgi_get_socket_by_num(ucr->socket_num);
		if (ucr->to_socket) {
			// fix socket name_len
			if (ucr->to_socket->name_len == 0 && ucr->to_socket->name) {
				ucr->to_socket->name_len = strlen(ucr->to_socket->name);
			}
		}
	}

	if (!ucr->pb_base_dir) {
		ucr->pb_base_dir = getenv("TMPDIR");
		if (!ucr->pb_base_dir)
			ucr->pb_base_dir = "/tmp";
	}

	int nevents;

	time_t delta;

	struct uwsgi_rb_timer *min_timeout;

	int new_connection;


	if (ucr->pattern) {
		init_magic_table(ucr->magic_table);
	}

	union uwsgi_sockaddr cr_addr;
	socklen_t cr_addr_len = sizeof(struct sockaddr_un);

	ucr->mapper = uwsgi_cr_map_use_void;

			if (ucr->use_cache) {
				ucr->cache = uwsgi_cache_by_name(ucr->use_cache);
				if (!ucr->cache) {
					uwsgi_log("!!! unable to find cache \"%s\" !!!\n", ucr->use_cache);
					exit(1);
				}
                        	ucr->mapper = uwsgi_cr_map_use_cache;
                        }
                        else if (ucr->pattern) {
                                ucr->mapper = uwsgi_cr_map_use_pattern;
                        }
                        else if (ucr->has_subscription_sockets) {
                                ucr->mapper = uwsgi_cr_map_use_subscription;
				if (uwsgi.subscription_dotsplit) {
                                	ucr->mapper = uwsgi_cr_map_use_subscription_dotsplit;
				}
                        }
                        else if (ucr->base) {
                                ucr->mapper = uwsgi_cr_map_use_base;
                        }
                        else if (ucr->code_string_code && ucr->code_string_function) {
                                ucr->mapper = uwsgi_cr_map_use_cs;
			}
                        else if (ucr->to_socket) {
                                ucr->mapper = uwsgi_cr_map_use_to;
                        }
                        else if (ucr->static_nodes) {
                                ucr->mapper = uwsgi_cr_map_use_static_nodes;
                        }

	ucr->timeouts = uwsgi_init_rb_timer();

	for (;;) {

		time_t now = uwsgi_now();

		// set timeouts and harakiri
		min_timeout = uwsgi_min_rb_timer(ucr->timeouts, NULL);
		if (min_timeout == NULL) {
			delta = -1;
		}
		else {
			delta = min_timeout->value - now;
			if (delta <= 0) {
				corerouter_expire_timeouts(ucr, now);
				delta = 0;
			}
		}

		if (uwsgi.master_process && ucr->harakiri > 0) {
			ushared->gateways_harakiri[id] = 0;
		}

		// wait for events
		nevents = event_queue_wait_multi(ucr->queue, delta, events, ucr->nevents);

		now = uwsgi_now();

		if (uwsgi.master_process && ucr->harakiri > 0) {
			ushared->gateways_harakiri[id] = now + ucr->harakiri;
		}

		if (nevents == 0) {
			corerouter_expire_timeouts(ucr, now);
		}

		for (i = 0; i < nevents; i++) {

			// get the interesting fd
			ucr->interesting_fd = event_queue_interesting_fd(events, i);
			// something bad happened
			if (ucr->interesting_fd < 0) continue;

			// check if the ucr->interesting_fd matches a gateway socket
			struct uwsgi_gateway_socket *ugs = uwsgi.gateway_sockets;
			int taken = 0;
			while (ugs) {
				if (ugs->gateway == &ushared->gateways[id] && ucr->interesting_fd == ugs->fd) {
					if (!ugs->subscription) {
#if defined(__linux__) && defined(SOCK_NONBLOCK) && !defined(OBSOLETE_LINUX_KERNEL)
						new_connection = accept4(ucr->interesting_fd, (struct sockaddr *) &cr_addr, &cr_addr_len, SOCK_NONBLOCK);
						if (new_connection < 0) {
							taken = 1;
							break;
						}
#else
						new_connection = accept(ucr->interesting_fd, (struct sockaddr *) &cr_addr, &cr_addr_len);
						if (new_connection < 0) {
							taken = 1;
							break;
						}
						// set socket in non-blocking mode, on non-linux platforms, clients get the server mode
#ifdef __linux__
                                                uwsgi_socket_nb(new_connection);
#endif
#endif
						struct corerouter_session *cr = corerouter_alloc_session(ucr, ugs, new_connection, (struct sockaddr *) &cr_addr, cr_addr_len);
						//something wrong in the allocation
						if (!cr) break;
					}
					else if (ugs->subscription) {
						uwsgi_corerouter_manage_subscription(ucr, id, ugs);
					}

					taken = 1;
					break;
				}


				ugs = ugs->next;
			}

			if (taken) {
				continue;
			}

			// manage internal subscription
			if (ucr->interesting_fd == ushared->gateways[id].internal_subscription_pipe[1]) {
				uwsgi_corerouter_manage_internal_subscription(ucr, ucr->interesting_fd);
			}
			// manage a stats request
			else if (ucr->interesting_fd == ucr->cr_stats_server) {
				corerouter_send_stats(ucr);
			}
			else {
				struct corerouter_peer *peer = ucr->cr_table[ucr->interesting_fd];

				// something is going wrong...
				if (peer == NULL)
					continue;

				// on error, destroy the session
				if (event_queue_interesting_fd_has_error(events, i)) {
					peer->failed = 1;
					corerouter_close_peer(ucr, peer);
					continue;
				}

				// set timeout (in main_peer too)
				peer->timeout = corerouter_reset_timeout_fast(ucr, peer, now);
				peer->session->main_peer->timeout = corerouter_reset_timeout_fast(ucr, peer->session->main_peer, now);

				ssize_t (*hook)(struct corerouter_peer *) = NULL;

				// call event hook
				if (event_queue_interesting_fd_is_read(events, i)) {
					hook = peer->hook_read;	
				}
				else if (event_queue_interesting_fd_is_write(events, i)) {
					hook = peer->hook_write;	
				}

				if (!hook) continue;
				// reset errno (as we use it for internal signalling)
				errno = 0;
				ssize_t ret = hook(peer);
				// connection closed
				if (ret == 0) {
					corerouter_close_peer(ucr, peer);
					continue;
				}
				else if (ret < 0) {
					if (errno == EINPROGRESS) continue;
					// remove keepalive on error
					peer->session->can_keepalive = 0;
					corerouter_close_peer(ucr, peer);
					continue;
				}
				
			}
		}
	}

}

int uwsgi_corerouter_has_backends(struct uwsgi_corerouter *ucr) {

	if (ucr->has_backends) return 1;

	// check if the router has configured backends
                if (ucr->use_cache ||
                        ucr->pattern ||
                        ucr->has_subscription_sockets ||
                        ucr->base ||
                        (ucr->code_string_code && ucr->code_string_function) ||
                        ucr->to_socket ||
                        ucr->static_nodes
                ) {
                        return 1;
                }

	
	return 0;

}

int uwsgi_corerouter_init(struct uwsgi_corerouter *ucr) {

	int i;

	if (ucr->has_sockets) {

		if (ucr->use_cache && !uwsgi.caches) {
			uwsgi_log("you need to create a uwsgi cache to use the %s (add --cache <n>)\n", ucr->name);
			exit(1);
		}

		if (!ucr->nevents)
			ucr->nevents = 64;

		if (!ucr->max_retries)
			ucr->max_retries = 3;
	

		ucr->has_backends = uwsgi_corerouter_has_backends(ucr);


		uwsgi_corerouter_setup_sockets(ucr);

		if (ucr->processes < 1)
			ucr->processes = 1;
		if (ucr->cheap) {
			uwsgi_log("starting %s in cheap mode\n", ucr->name);
		}
		for (i = 0; i < ucr->processes; i++) {
			struct uwsgi_gateway *ug = register_gateway(ucr->name, uwsgi_corerouter_loop, ucr);
			if (ug == NULL) {
				uwsgi_log("unable to register the %s gateway\n", ucr->name);
				exit(1);
			}
			ug->uid = ucr->uid;
			ug->gid = ucr->gid;
		}
	}

	return 0;
}


struct uwsgi_plugin corerouter_plugin = {

	.name = "corerouter",
};

void corerouter_send_stats(struct uwsgi_corerouter *ucr) {

	struct sockaddr_un client_src;
	socklen_t client_src_len = 0;

	int client_fd = accept(ucr->cr_stats_server, (struct sockaddr *) &client_src, &client_src_len);
	if (client_fd < 0) {
		uwsgi_error("corerouter_send_stats()/accept()");
		return;
	}

	if (uwsgi.stats_http) {
                if (uwsgi_send_http_stats(client_fd)) {
                        close(client_fd);
                        return;
                }
        }

	struct uwsgi_stats *us = uwsgi_stats_new(8192);

        if (uwsgi_stats_keyval_comma(us, "version", UWSGI_VERSION)) goto end;
        if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) getpid())) goto end;
        if (uwsgi_stats_keylong_comma(us, "uid", (unsigned long long) getuid())) goto end;
        if (uwsgi_stats_keylong_comma(us, "gid", (unsigned long long) getgid())) goto end;

        char *cwd = uwsgi_get_cwd();
        if (uwsgi_stats_keyval_comma(us, "cwd", cwd)) goto end0;

        if (uwsgi_stats_keylong_comma(us, "active_sessions", (unsigned long long) ucr->active_sessions)) goto end0;

	if (uwsgi_stats_key(us , ucr->short_name)) goto end0;
        if (uwsgi_stats_list_open(us)) goto end0;

	struct uwsgi_gateway_socket *ugs = uwsgi.gateway_sockets;
	while (ugs) {
		if (!strcmp(ugs->owner, ucr->name)) {
			if (uwsgi_stats_str(us, ugs->name)) goto end0;
			if (ugs->next) {
				if (uwsgi_stats_comma(us)) goto end0;
			}
		}
		ugs = ugs->next;
	}
	if (uwsgi_stats_list_close(us)) goto end0;
	if (uwsgi_stats_comma(us)) goto end0;

	if (ucr->static_nodes) {
		if (uwsgi_stats_key(us , "static_nodes")) goto end0;
                if (uwsgi_stats_list_open(us)) goto end0;

		struct uwsgi_string_list *usl = ucr->static_nodes;
		while(usl) {
			if (uwsgi_stats_object_open(us)) goto end0;
			if (uwsgi_stats_keyvaln_comma(us, "name", usl->value, usl->len)) goto end0;

			if (uwsgi_stats_keylong_comma(us, "hits", (unsigned long long) usl->custom2)) goto end0;
			if (uwsgi_stats_keylong(us, "grace", (unsigned long long) usl->custom)) goto end0;

			if (uwsgi_stats_object_close(us)) goto end0;
			usl = usl->next;
			if (usl) {
				if (uwsgi_stats_comma(us)) goto end0;
			}
		}

		if (uwsgi_stats_list_close(us)) goto end0;
                if (uwsgi_stats_comma(us)) goto end0;
        }

	if (ucr->has_subscription_sockets) {
		if (uwsgi_stats_key(us , "subscriptions")) goto end0;
		if (uwsgi_stats_list_open(us)) goto end0;

		int i;
		int first_processed = 0;
		for(i=0;i<UMAX16;i++) {
			struct uwsgi_subscribe_slot *s_slot = ucr->subscriptions[i];
			if (s_slot && first_processed) {
				if (uwsgi_stats_comma(us)) goto end0;
			}
			while (s_slot) {
				first_processed = 1;
				if (uwsgi_stats_object_open(us)) goto end0;
				if (uwsgi_stats_keyvaln_comma(us, "key", s_slot->key, s_slot->keylen)) goto end0;
				if (uwsgi_stats_keylong_comma(us, "hash", (unsigned long long) s_slot->hash)) goto end0;
				if (uwsgi_stats_keylong_comma(us, "hits", (unsigned long long) s_slot->hits)) goto end0;
#ifdef UWSGI_SSL
				if (uwsgi_stats_keylong_comma(us, "sni_enabled", (unsigned long long) s_slot->sni_enabled)) goto end0;
#endif

				if (uwsgi_stats_key(us , "nodes")) goto end0;
				if (uwsgi_stats_list_open(us)) goto end0;

				struct uwsgi_subscribe_node *s_node = s_slot->nodes;
				while (s_node) {
					if (uwsgi_stats_object_open(us)) goto end0;

					if (uwsgi_stats_keyvaln_comma(us, "name", s_node->name, s_node->len)) goto end0;

					if (uwsgi_stats_keylong_comma(us, "modifier1", (unsigned long long) s_node->modifier1)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "modifier2", (unsigned long long) s_node->modifier2)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "last_check", (unsigned long long) s_node->last_check)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) s_node->pid)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "uid", (unsigned long long) s_node->uid)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "gid", (unsigned long long) s_node->gid)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "requests", (unsigned long long) s_node->requests)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "last_requests", (unsigned long long) s_node->last_requests)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "tx", (unsigned long long) s_node->tx)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "rx", (unsigned long long) s_node->rx)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "cores", (unsigned long long) s_node->cores)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "load", (unsigned long long) s_node->load)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "weight", (unsigned long long) s_node->weight)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "wrr", (unsigned long long) s_node->wrr)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "ref", (unsigned long long) s_node->reference)) goto end0;
					if (uwsgi_stats_keylong_comma(us, "failcnt", (unsigned long long) s_node->failcnt)) goto end0;
					if (uwsgi_stats_keylong(us, "death_mark", (unsigned long long) s_node->death_mark)) goto end0;

					if (uwsgi_stats_object_close(us)) goto end0;
					if (s_node->next) {
						if (uwsgi_stats_comma(us)) goto end0;
					}
					s_node = s_node->next;
				}

				if (uwsgi_stats_list_close(us)) goto end0;
				if (uwsgi_stats_object_close(us)) goto end0;
				if (s_slot->next) {
					if (uwsgi_stats_comma(us)) goto end0;
				}

				s_slot = s_slot->next;
				// check for loopy optimization
				if (s_slot == ucr->subscriptions[i])
					break;
			}
		}

			if (uwsgi_stats_list_close(us)) goto end0;
			if (uwsgi_stats_comma(us)) goto end0;
	}

	if (uwsgi_stats_keylong(us, "cheap", (unsigned long long) ucr->i_am_cheap)) goto end0;	

	if (uwsgi_stats_object_close(us)) goto end0;

        size_t remains = us->pos;
        off_t pos = 0;
        while(remains > 0) {
		int ret = uwsgi_waitfd_write(client_fd, uwsgi.socket_timeout);
                if (ret <= 0) {
                        goto end0;
                }
                ssize_t res = write(client_fd, us->base + pos, remains);
                if (res <= 0) {
                        if (res < 0) {
                                uwsgi_error("write()");
                        }
                        goto end0;
                }
                pos += res;
                remains -= res;
        }

end0:
        free(cwd);
end:
        free(us->base);
        free(us);
        close(client_fd);


}
