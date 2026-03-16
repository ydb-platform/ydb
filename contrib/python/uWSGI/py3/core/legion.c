#include <contrib/python/uWSGI/py3/config.h>
#include "../uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	uWSGI Legions subsystem

	A Legion is a group of uWSGI instances sharing a single object. This single
	object can be owned only by the instance with the higher valor. Such an instance is the
	Lord of the Legion. There can only be one (and only one) Lord for each Legion.
	If a member of a Legion spawns with an higher valor than the current Lord, it became the new Lord.


*/

struct uwsgi_legion *uwsgi_legion_get_by_socket(int fd) {
	struct uwsgi_legion *ul = uwsgi.legions;
	while (ul) {
		if (ul->socket == fd) {
			return ul;
		}
		ul = ul->next;
	}

	return NULL;
}

struct uwsgi_legion *uwsgi_legion_get_by_name(char *name) {
	struct uwsgi_legion *ul = uwsgi.legions;
	while (ul) {
		if (!strcmp(name, ul->legion)) {
			return ul;
		}
		ul = ul->next;
	}

	return NULL;
}


void uwsgi_parse_legion(char *key, uint16_t keylen, char *value, uint16_t vallen, void *data) {
	struct uwsgi_legion *ul = (struct uwsgi_legion *) data;

	if (!uwsgi_strncmp(key, keylen, "legion", 6)) {
		ul->legion = value;
		ul->legion_len = vallen;
	}
	else if (!uwsgi_strncmp(key, keylen, "valor", 5)) {
		ul->valor = uwsgi_str_num(value, vallen);
	}
	else if (!uwsgi_strncmp(key, keylen, "name", 4)) {
		ul->name = value;
		ul->name_len = vallen;
	}
	else if (!uwsgi_strncmp(key, keylen, "pid", 3)) {
		ul->pid = uwsgi_str_num(value, vallen);
	}
	else if (!uwsgi_strncmp(key, keylen, "unix", 4)) {
		ul->unix_check = uwsgi_str_num(value, vallen);
	}
	else if (!uwsgi_strncmp(key, keylen, "checksum", 8)) {
		ul->checksum = uwsgi_str_num(value, vallen);
	}
	else if (!uwsgi_strncmp(key, keylen, "uuid", 4)) {
		if (vallen == 36) {
			memcpy(ul->uuid, value, 36);
		}
	}
	else if (!uwsgi_strncmp(key, keylen, "lord_valor", 10)) {
		ul->lord_valor = uwsgi_str_num(value, vallen);
	}
	else if (!uwsgi_strncmp(key, keylen, "lord_uuid", 9)) {
		if (vallen == 36) {
			memcpy(ul->lord_uuid, value, 36);
		}
	}
	else if (!uwsgi_strncmp(key, keylen, "scroll", 6)) {
		ul->scroll = value;
		ul->scroll_len = vallen;
	}
	else if (!uwsgi_strncmp(key, keylen, "dead", 4)) {
		ul->dead = 1;
	}
}

// this function is called when a node is added or removed (heavy locking is needed)
static void legion_rebuild_scrolls(struct uwsgi_legion *ul) {
	uint64_t max_size = ul->scrolls_max_size;

	// first, try to add myself
	if (ul->scroll_len + (uint64_t) 2 > max_size) {
		uwsgi_log("[DANGER] you have configured a too much tiny buffer for the scrolls list !!! tune it with --legion-scroll-list-max-size\n");
		ul->scroll_len = 0;
		return;
	}

	char *ptr = ul->scrolls;
	*ptr ++= (uint8_t) (ul->scroll_len & 0xff);
	*ptr ++= (uint8_t) ((ul->scroll_len >> 8) &0xff);
	memcpy(ptr, ul->scroll, ul->scroll_len); ptr += ul->scroll_len;
	ul->scrolls_len = 2 + ul->scroll_len;
	// ok start adding nodes;
	struct uwsgi_legion_node *uln = ul->nodes_head;
	while(uln) {
		if (ul->scrolls_len + 2 + uln->scroll_len > max_size) {
			uwsgi_log("[DANGER] you have configured a too much tiny buffer for the scrolls list !!! tune it with --legion-scroll-list-max-size\n");
			return;
		}
		*ptr ++= (uint8_t) (uln->scroll_len & 0xff);
        	*ptr ++= (uint8_t) ((uln->scroll_len >> 8) &0xff);
        	memcpy(ptr, uln->scroll, uln->scroll_len); ptr += uln->scroll_len;
        	ul->scrolls_len += 2 + uln->scroll_len;
		uln = uln->next;
	}
}

// critical section (remember to lock when you use it)
struct uwsgi_legion_node *uwsgi_legion_add_node(struct uwsgi_legion *ul, uint16_t valor, char *name, uint16_t name_len, char *uuid) {

	struct uwsgi_legion_node *node = uwsgi_calloc(sizeof(struct uwsgi_legion_node));
	if (!name_len)
		goto error;
	node->name = uwsgi_calloc(name_len);
	node->name_len = name_len;
	memcpy(node->name, name, name_len);
	node->valor = valor;
	memcpy(node->uuid, uuid, 36);

	if (ul->nodes_tail) {
		node->prev = ul->nodes_tail;
		ul->nodes_tail->next = node;
	}

	ul->nodes_tail = node;

	if (!ul->nodes_head) {
		ul->nodes_head = node;
	}


	return node;


error:
	free(node);
	return NULL;
}

// critical section (remember to lock when you use it)
void uwsgi_legion_remove_node(struct uwsgi_legion *ul, struct uwsgi_legion_node *node) {
	// check if the node is the first one
	if (node == ul->nodes_head) {
		ul->nodes_head = node->next;
	}

	// check if the node is the last one
	if (node == ul->nodes_tail) {
		ul->nodes_tail = node->prev;
	}

	if (node->prev) {
		node->prev->next = node->next;
	}

	if (node->next) {
		node->next->prev = node->prev;
	}

	if (node->name_len) {
		free(node->name);
	}

	if (node->scroll_len) {
		free(node->scroll);
	}

	free(node);

	legion_rebuild_scrolls(ul);
}

struct uwsgi_legion_node *uwsgi_legion_get_node(struct uwsgi_legion *ul, uint64_t valor, char *name, uint16_t name_len, char *uuid) {
	struct uwsgi_legion_node *nodes = ul->nodes_head;
	while (nodes) {
		if (valor != nodes->valor)
			goto next;
		if (name_len != nodes->name_len)
			goto next;
		if (memcmp(nodes->name, name, name_len))
			goto next;
		if (memcmp(nodes->uuid, uuid, 36))
			goto next;
		return nodes;
next:
		nodes = nodes->next;
	}
	return NULL;
}

static void legions_check_nodes() {

	struct uwsgi_legion *legion = uwsgi.legions;
	while (legion) {
		time_t now = uwsgi_now();

		struct uwsgi_legion_node *node = legion->nodes_head;
		while (node) {
			if (now - node->last_seen > uwsgi.legion_tolerance) {
				struct uwsgi_legion_node *tmp_node = node;
				node = node->next;
				uwsgi_log("[uwsgi-legion] %s: %.*s valor: %llu uuid: %.*s left Legion %s\n", tmp_node->valor > 0 ? "node" : "arbiter", tmp_node->name_len, tmp_node->name, tmp_node->valor, 36, tmp_node->uuid, legion->legion);
				uwsgi_wlock(legion->lock);
				uwsgi_legion_remove_node(legion, tmp_node);
				uwsgi_rwunlock(legion->lock);
				// trigger node_left hooks
				struct uwsgi_string_list *usl = legion->node_left_hooks;
				while (usl) {
					int ret = uwsgi_legion_action_call("node_left", legion, usl);
					if (ret) {
						uwsgi_log("[uwsgi-legion] ERROR, node_left hook returned: %d\n", ret);
					}
					usl = usl->next;
				}
				continue;
			}
			node = node->next;
		}

		legion = legion->next;
	}
}

struct uwsgi_legion_node *uwsgi_legion_get_lord(struct uwsgi_legion *);

static void legions_report_quorum(struct uwsgi_legion *ul, uint64_t best_valor, char *best_uuid, int votes) {
	struct uwsgi_legion_node *nodes = ul->nodes_head;
	uwsgi_log("[uwsgi-legion] --- WE HAVE QUORUM FOR LEGION %s !!! (valor: %llu uuid: %.*s checksum: %llu votes: %d) ---\n", ul->legion, best_valor, 36, best_uuid, ul->checksum, votes);
	while (nodes) {
		uwsgi_log("[uwsgi-legion-node] %s: %.*s valor: %llu uuid: %.*s last_seen: %d vote_valor: %llu vote_uuid: %.*s\n", nodes->valor > 0 ? "node" : "arbiter", nodes->name_len, nodes->name, nodes->valor, 36, nodes->uuid, nodes->last_seen, nodes->lord_valor, 36, nodes->lord_uuid);
		nodes = nodes->next;
	}
	uwsgi_log("[uwsgi-legion] --- END OF QUORUM REPORT ---\n");
}

uint64_t uwsgi_legion_checksum(struct uwsgi_legion *ul) {
	uint16_t i;
	uint64_t checksum = ul->valor;
	for(i=0;i<36;i++) {
		checksum += ul->uuid[i];
	}

	struct uwsgi_legion_node *nodes = ul->nodes_head;
	while (nodes) {
		checksum += nodes->valor;
		for(i=0;i<36;i++) {
			checksum += nodes->uuid[i];
		}
		nodes = nodes->next;
	}

	return checksum;	
	
}

static void legions_check_nodes_step2() {
	struct uwsgi_legion *ul = uwsgi.legions;
	while (ul) {
		// ok now we can check the status of the lord
		int i_am_the_best = 0;
		uint64_t best_valor = 0;
		char best_uuid[36];
		struct uwsgi_legion_node *node = uwsgi_legion_get_lord(ul);
		if (node) {
			// a node is the best candidate
			best_valor = node->valor;
			memcpy(best_uuid, node->uuid, 36);
		}
		// go on if i am not an arbiter
		// no potential Lord is available, i will propose myself
		// but only if i am not suspended...
		else if (ul->valor > 0 && uwsgi_now() > ul->suspended_til) {
			best_valor = ul->valor;
			memcpy(best_uuid, ul->uuid, 36);
			i_am_the_best = 1;
		}
		else {
			// empty lord
			memset(best_uuid, 0, 36);
		}

		// calculate the checksum
		uint64_t new_checksum = uwsgi_legion_checksum(ul);
		if (new_checksum != ul->checksum) {
			ul->changed = 1;
		}
		ul->checksum = new_checksum;

		// ... ok let's see if all of the nodes agree on the lord
		// ... but first check if i am not alone...
		int votes = 1;
		struct uwsgi_legion_node *nodes = ul->nodes_head;
		while (nodes) {
			if (nodes->checksum != ul->checksum) {
				votes = 0;
				break;
			}
			if (nodes->lord_valor != best_valor) {
				votes = 0;
				break;
			}
			if (memcmp(nodes->lord_uuid, best_uuid, 36)) {
				votes = 0;
				break;
			}
			votes++;
			nodes = nodes->next;
		}

		// we have quorum !!!
		if (votes > 0 && votes >= ul->quorum) {
			if (!ul->joined) {
				// triggering join hooks
				struct uwsgi_string_list *usl = ul->join_hooks;
				while (usl) {
					int ret = uwsgi_legion_action_call("join", ul, usl);
					if (ret) {
						uwsgi_log("[uwsgi-legion] ERROR, join hook returned: %d\n", ret);
					}
					usl = usl->next;
				}
				ul->joined = 1;
			}
			// something changed ???
			if (ul->changed) {
				legions_report_quorum(ul, best_valor, best_uuid, votes);
				ul->changed = 0;
			}
			if (i_am_the_best) {
				if (!ul->i_am_the_lord) {
					// triggering lord hooks
					uwsgi_log("[uwsgi-legion] attempting to become the Lord of the Legion %s\n", ul->legion);
					struct uwsgi_string_list *usl = ul->lord_hooks;
					while (usl) {
						int ret = uwsgi_legion_action_call("lord", ul, usl);
						if (ret) {
							uwsgi_log("[uwsgi-legion] ERROR, lord hook returned: %d\n", ret);
							if (uwsgi.legion_death_on_lord_error) {
								ul->dead = 1;
                						uwsgi_legion_announce(ul);
								ul->suspended_til = uwsgi_now() + uwsgi.legion_death_on_lord_error;
								uwsgi_log("[uwsgi-legion] suspending myself from Legion \"%s\" for %d seconds\n", ul->legion, uwsgi.legion_death_on_lord_error);
								goto next;
							}
						}
						usl = usl->next;
					}
					if (ul->scroll_len > 0 && ul->scroll_len <= ul->lord_scroll_size) {
                				uwsgi_wlock(ul->lock);
                				ul->lord_scroll_len = ul->scroll_len;
                				memcpy(ul->lord_scroll, ul->scroll, ul->lord_scroll_len);
                				uwsgi_rwunlock(ul->lock);
        				}
        				else {
                				ul->lord_scroll_len = 0;
        				}
					uwsgi_log("[uwsgi-legion] i am now the Lord of the Legion %s\n", ul->legion);
					ul->i_am_the_lord = uwsgi_now();
					// trick: reduce the time needed by the old lord to unlord itself
					uwsgi_legion_announce(ul);
				}
			}
			else {
				if (ul->i_am_the_lord) {
					uwsgi_log("[uwsgi-legion] a new Lord (valor: %llu uuid: %.*s) raised for Legion %s...\n", ul->lord_valor, 36, ul->lord_uuid, ul->legion);
					if (ul->lord_scroll_len > 0) {
						uwsgi_log("*********** The New Lord Scroll ***********\n");
						uwsgi_log("%.*s\n", ul->lord_scroll_len, ul->lord_scroll);
						uwsgi_log("*********** End of the New Lord Scroll ***********\n");
					}
					// no more lord, trigger unlord hooks
					struct uwsgi_string_list *usl = ul->unlord_hooks;
					while (usl) {
						int ret = uwsgi_legion_action_call("unlord", ul, usl);
						if (ret) {
							uwsgi_log("[uwsgi-legion] ERROR, unlord hook returned: %d\n", ret);
						}
						usl = usl->next;
					}
					ul->i_am_the_lord = 0;
				}
			}
		}
		else if (votes > 0 && votes < ul->quorum && (uwsgi_now() - ul->last_warning >= 60)) {
			uwsgi_log("[uwsgi-legion] no quorum: only %d vote(s) for Legion %s, %d needed to elect a Lord\n", votes, ul->legion, ul->quorum);
			// no more quorum, leave the Lord state
			if (ul->i_am_the_lord) {
				uwsgi_log("[uwsgi-legion] i cannot be The Lord of The Legion %s without a quorum ...\n", ul->legion);
				// no more lord, trigger unlord hooks
                                struct uwsgi_string_list *usl = ul->unlord_hooks;
                                while (usl) {
                                	int ret = uwsgi_legion_action_call("unlord", ul, usl);
                                        if (ret) {
                                        	uwsgi_log("[uwsgi-legion] ERROR, unlord hook returned: %d\n", ret);
                                        }
                                        usl = usl->next;
                                }
                                ul->i_am_the_lord = 0;
			}
			ul->last_warning = uwsgi_now();
		}
next:
		ul = ul->next;
	}
}

// check who should be the lord of the legion
struct uwsgi_legion_node *uwsgi_legion_get_lord(struct uwsgi_legion *ul) {

	char best_uuid[36];

	memcpy(best_uuid, ul->uuid, 36);
	uint64_t best_valor = ul->valor;
	
	struct uwsgi_legion_node *best_node = NULL;

	struct uwsgi_legion_node *nodes = ul->nodes_head;
	while (nodes) {
		// skip arbiters
		if (nodes->valor == 0) goto next;
		if (nodes->valor > best_valor) {
			best_node = nodes;
			best_valor = nodes->valor;
			memcpy(best_uuid, nodes->uuid, 36);
		}
		else if (nodes->valor == best_valor) {
			if (uwsgi_uuid_cmp(nodes->uuid, best_uuid) > 0) {
				best_node = nodes;
				best_valor = nodes->valor;
				memcpy(best_uuid, nodes->uuid, 36);
			}
		}
next:
		nodes = nodes->next;
	}

	// first round ? (skip first round if arbiter)
	if (ul->valor > 0 && ul->lord_valor == 0) {
		ul->changed = 1;
	}
	else if (best_valor != ul->lord_valor) {
		ul->changed = 1;
	}
	else {
		if (memcmp(best_uuid, ul->lord_uuid, 36)) {
			ul->changed = 1;
		}
	}

	ul->lord_valor = best_valor;
	memcpy(ul->lord_uuid, best_uuid, 36);

	if (!best_node) return NULL;

	if (best_node->scroll_len > 0 && best_node->scroll_len <= ul->lord_scroll_size) {
		uwsgi_wlock(ul->lock);
		ul->lord_scroll_len = best_node->scroll_len;
		memcpy(ul->lord_scroll, best_node->scroll, ul->lord_scroll_len);
		uwsgi_rwunlock(ul->lock);
	}
	else {
		ul->lord_scroll_len = 0;
	}

	return best_node;
}


static void *legion_loop(void *foobar) {

	time_t last_round = uwsgi_now();

	unsigned char *crypted_buf = uwsgi_malloc(UMAX16 - EVP_MAX_BLOCK_LENGTH - 4);
	unsigned char *clear_buf = uwsgi_malloc(UMAX16);

	struct uwsgi_legion legion_msg;

	if (!uwsgi.legion_freq)
		uwsgi.legion_freq = 3;
	if (!uwsgi.legion_tolerance)
		uwsgi.legion_tolerance = 15;
	if (!uwsgi.legion_skew_tolerance)
		uwsgi.legion_skew_tolerance = 60;

	int first_round = 1;
	for (;;) {
		int timeout = uwsgi.legion_freq;
		time_t now = uwsgi_now();
		if (now > last_round) {
			timeout -= (now - last_round);
			if (timeout < 0) {
				timeout = 0;
			}
		}
		last_round = now;
		// wait for event
		int interesting_fd = -1;
		if (uwsgi_instance_is_reloading || uwsgi_instance_is_dying) return NULL;
		int rlen = event_queue_wait(uwsgi.legion_queue, timeout, &interesting_fd);

		if (rlen < 0 && errno != EINTR) {
			if (uwsgi_instance_is_reloading || uwsgi_instance_is_dying) return NULL;
			uwsgi_nuclear_blast();
			return NULL;	
		}

		now = uwsgi_now();
		if (timeout == 0 || rlen == 0 || (now - last_round) >= timeout) {
			struct uwsgi_legion *legions = uwsgi.legions;
			while (legions) {
				uwsgi_legion_announce(legions);
				legions = legions->next;
			}
			last_round = now;
		}

		// check the nodes
		legions_check_nodes();

		if (rlen > 0) {
			struct uwsgi_legion *ul = uwsgi_legion_get_by_socket(interesting_fd);
			if (!ul)
				continue;
			// ensure the first 4 bytes are valid
			ssize_t len = read(ul->socket, crypted_buf, (UMAX16 - EVP_MAX_BLOCK_LENGTH - 4));
			if (len < 0) {
				uwsgi_error("[uwsgi-legion] read()");
				continue;
			}
			else if (len < 4) {
				uwsgi_log("[uwsgi-legion] invalid packet size: %d\n", (int) len);
				continue;
			}

			struct uwsgi_header *uh = (struct uwsgi_header *) crypted_buf;

			if (uh->modifier1 != 109) {
				uwsgi_log("[uwsgi-legion] invalid modifier1");
				continue;
			}

			int d_len = 0;
			int d2_len = 0;
			// decrypt packet using the secret
			if (EVP_DecryptInit_ex(ul->decrypt_ctx, NULL, NULL, NULL, NULL) <= 0) {
				uwsgi_error("[uwsgi-legion] EVP_DecryptInit_ex()");
				continue;
			}

			if (EVP_DecryptUpdate(ul->decrypt_ctx, clear_buf, &d_len, crypted_buf + 4, len - 4) <= 0) {
				uwsgi_error("[uwsgi-legion] EVP_DecryptUpdate()");
				continue;
			}

			if (EVP_DecryptFinal_ex(ul->decrypt_ctx, clear_buf + d_len, &d2_len) <= 0) {
				ERR_print_errors_fp(stderr);
				uwsgi_log("[uwsgi-legion] EVP_DecryptFinal_ex()\n");
				continue;
			}

			d_len += d2_len;

			if (d_len != uh->pktsize) {
				uwsgi_log("[uwsgi-legion] invalid packet size\n");
				continue;
			}

			// parse packet
			memset(&legion_msg, 0, sizeof(struct uwsgi_legion));
			if (uwsgi_hooked_parse((char *) clear_buf, d_len, uwsgi_parse_legion, &legion_msg)) {
				uwsgi_log("[uwsgi-legion] invalid packet\n");
				continue;
			}

			if (uwsgi_strncmp(ul->legion, ul->legion_len, legion_msg.legion, legion_msg.legion_len)) {
				uwsgi_log("[uwsgi-legion] invalid legion name\n");
				continue;
			}

			// check for loop packets... (expecially when in multicast mode)
			if (!uwsgi_strncmp(uwsgi.hostname, uwsgi.hostname_len, legion_msg.name, legion_msg.name_len)) {
				if (legion_msg.pid == ul->pid) {
					if (legion_msg.valor == ul->valor) {
						if (!memcmp(legion_msg.uuid, ul->uuid, 36)) {
							continue;
						}
					}
				}
			}

			// check for "tolerable" unix time
			if (legion_msg.unix_check < (uwsgi_now() - uwsgi.legion_skew_tolerance)) {
				uwsgi_log("[uwsgi-legion] untolerable packet received for Legion %s , check your clock !!!\n", ul->legion);
				continue;
			}

			// check if the node is already accounted
			struct uwsgi_legion_node *node = uwsgi_legion_get_node(ul, legion_msg.valor, legion_msg.name, legion_msg.name_len, legion_msg.uuid);
			if (!node) {
				// if a lord hook election fails, a node can announce itself as dead for long time...
				if (legion_msg.dead) continue;
				// add the new node
				uwsgi_wlock(ul->lock);
				node = uwsgi_legion_add_node(ul, legion_msg.valor, legion_msg.name, legion_msg.name_len, legion_msg.uuid);
				if (!node) continue;
				if (legion_msg.scroll_len > 0) {
					node->scroll = uwsgi_malloc(legion_msg.scroll_len);
					node->scroll_len = legion_msg.scroll_len;
					memcpy(node->scroll, legion_msg.scroll, node->scroll_len);
				}
				// we are still locked (and safe), let's rebuild the scrolls list
				legion_rebuild_scrolls(ul);
				uwsgi_rwunlock(ul->lock);
				uwsgi_log("[uwsgi-legion] %s: %.*s valor: %llu uuid: %.*s joined Legion %s\n", node->valor > 0 ? "node" : "arbiter", node->name_len, node->name, node->valor, 36, node->uuid, ul->legion);
				// trigger node_joined hooks
				struct uwsgi_string_list *usl = ul->node_joined_hooks;
				while (usl) {
					int ret = uwsgi_legion_action_call("node_joined", ul, usl);
					if (ret) {
						uwsgi_log("[uwsgi-legion] ERROR, node_joined hook returned: %d\n", ret);
					}
					usl = usl->next;
				}
			}
			// remove node announcing death
			else if (legion_msg.dead) {
				uwsgi_log("[uwsgi-legion] %s: %.*s valor: %llu uuid: %.*s announced its death to Legion %s\n", node->valor > 0 ? "node" : "arbiter", node->name_len, node->name, node->valor, 36, node->uuid, ul->legion);
                                uwsgi_wlock(ul->lock);
                                uwsgi_legion_remove_node(ul, node);
                                uwsgi_rwunlock(ul->lock);
				continue;
			}

			node->last_seen = uwsgi_now();
			node->lord_valor = legion_msg.lord_valor;
			node->checksum = legion_msg.checksum;
			memcpy(node->lord_uuid, legion_msg.lord_uuid, 36);

		}

		// skip the first round if i no packet is received
		if (first_round) {
			first_round = 0;
			continue;
		}
		legions_check_nodes_step2();
	}

	return NULL;
}

int uwsgi_legion_action_call(char *phase, struct uwsgi_legion *ul, struct uwsgi_string_list *usl) {
	struct uwsgi_legion_action *ula = uwsgi_legion_action_get(usl->custom_ptr);
	if (!ula) {
		uwsgi_log("[uwsgi-legion] ERROR unable to find legion_action \"%s\"\n", (char *) usl->custom_ptr);
		return -1;
	}

	if (ula->log_msg) {
		uwsgi_log("[uwsgi-legion] (phase: %s legion: %s) %s\n", phase, ul->legion, ula->log_msg);
	}
	else {
		uwsgi_log("[uwsgi-legion] (phase: %s legion: %s) calling %s\n", phase, ul->legion, usl->value);
	}
	return ula->func(ul, usl->value + usl->custom);
}

static int legion_action_cmd(struct uwsgi_legion *ul, char *arg) {
	return uwsgi_run_command_and_wait(NULL, arg);
}

static int legion_action_signal(struct uwsgi_legion *ul, char *arg) {
	return uwsgi_signal_send(uwsgi.signal_socket, atoi(arg));
}

static int legion_action_log(struct uwsgi_legion *ul, char *arg) {
	char *logline = uwsgi_concat2(arg, "\n");
	uwsgi_log(logline);
	free(logline);
	return 0;
}

static int legion_action_alarm(struct uwsgi_legion *ul, char *arg) {
	char *space = strchr(arg,' ');
        if (!space) {
                uwsgi_log("invalid alarm action syntax, must be: <alarm> <msg>\n");
                return -1;
        }
        *space = 0;
        uwsgi_alarm_trigger(arg, space+1,  strlen(space+1));
        *space = ' ';
        return 0;
}

void uwsgi_start_legions() {
	pthread_t legion_loop_t;

	if (!uwsgi.legions)
		return;

	// register embedded actions
	uwsgi_legion_action_register("cmd", legion_action_cmd);
	uwsgi_legion_action_register("exec", legion_action_cmd);
	uwsgi_legion_action_register("signal", legion_action_signal);
	uwsgi_legion_action_register("log", legion_action_log);
	uwsgi_legion_action_register("alarm", legion_action_alarm);

	uwsgi.legion_queue = event_queue_init();
	struct uwsgi_legion *legion = uwsgi.legions;
	while (legion) {
		char *colon = strchr(legion->addr, ':');
		if (colon) {
			legion->socket = bind_to_udp(legion->addr, 0, 0);
		}
		else {
			legion->socket = bind_to_unix_dgram(legion->addr);
		}
		if (legion->socket < 0 || event_queue_add_fd_read(uwsgi.legion_queue, legion->socket)) {
			uwsgi_log("[uwsgi-legion] unable to activate legion %s\n", legion->legion);
			exit(1);
		}
		uwsgi_socket_nb(legion->socket);
		legion->pid = uwsgi.mypid;
		uwsgi_uuid(legion->uuid);
		struct uwsgi_string_list *usl = legion->setup_hooks;
		while (usl) {
			int ret = uwsgi_legion_action_call("setup", legion, usl);
			if (ret) {
				uwsgi_log("[uwsgi-legion] ERROR, setup hook returned: %d\n", ret);
			}
			usl = usl->next;
		}
		legion = legion->next;
	}

#ifndef UWSGI_UUID
	uwsgi_log("WARNING: you are not using libuuid to generate Legions UUID\n");
#endif

	if (pthread_create(&legion_loop_t, NULL, legion_loop, NULL)) {
		uwsgi_error("pthread_create()");
		uwsgi_log("unable to run the legion server !!!\n");
	}
	else {
		uwsgi_log("legion manager thread enabled\n");
	}

}

void uwsgi_legion_add(struct uwsgi_legion *ul) {
	struct uwsgi_legion *old_legion = NULL, *legion = uwsgi.legions;
	while (legion) {
		old_legion = legion;
		legion = legion->next;
	}

	if (old_legion) {
		old_legion->next = ul;
	}
	else {
		uwsgi.legions = ul;
	}
}

int uwsgi_legion_announce(struct uwsgi_legion *ul) {
	time_t now = uwsgi_now();

	if (now <= ul->suspended_til) return 0;
	ul->suspended_til = 0;

	struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);
	unsigned char *encrypted = NULL;

	if (uwsgi_buffer_append_keyval(ub, "legion", 6, ul->legion, ul->legion_len))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "valor", 5, ul->valor))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "unix", 4, now))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "lord", 4, ul->i_am_the_lord ? ul->i_am_the_lord : 0))
		goto err;
	if (uwsgi_buffer_append_keyval(ub, "name", 4, uwsgi.hostname, uwsgi.hostname_len))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "pid", 3, ul->pid))
		goto err;
	if (uwsgi_buffer_append_keyval(ub, "uuid", 4, ul->uuid, 36))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "checksum", 8, ul->checksum))
		goto err;
	if (uwsgi_buffer_append_keynum(ub, "lord_valor", 10, ul->lord_valor))
		goto err;
	if (uwsgi_buffer_append_keyval(ub, "lord_uuid", 9, ul->lord_uuid, 36))
		goto err;

	if (ul->scroll_len > 0) {
		if (uwsgi_buffer_append_keyval(ub, "scroll", 6, ul->scroll, ul->scroll_len))
                	goto err;
	}

	if (ul->dead) {
		if (uwsgi_buffer_append_keyval(ub, "dead", 4, "1", 1))
                	goto err;
	}

	encrypted = uwsgi_malloc(ub->pos + 4 + EVP_MAX_BLOCK_LENGTH);
	if (EVP_EncryptInit_ex(ul->encrypt_ctx, NULL, NULL, NULL, NULL) <= 0) {
		uwsgi_error("[uwsgi-legion] EVP_EncryptInit_ex()");
		goto err;
	}

	int e_len = 0;

	if (EVP_EncryptUpdate(ul->encrypt_ctx, encrypted + 4, &e_len, (unsigned char *) ub->buf, ub->pos) <= 0) {
		uwsgi_error("[uwsgi-legion] EVP_EncryptUpdate()");
		goto err;
	}

	int tmplen = 0;
	if (EVP_EncryptFinal_ex(ul->encrypt_ctx, encrypted + 4 + e_len, &tmplen) <= 0) {
		uwsgi_error("[uwsgi-legion] EVP_EncryptFinal_ex()");
		goto err;
	}

	e_len += tmplen;
	uint16_t pktsize = ub->pos;
	encrypted[0] = 109;
	encrypted[1] = (unsigned char) (pktsize & 0xff);
	encrypted[2] = (unsigned char) ((pktsize >> 8) & 0xff);
	encrypted[3] = 0;

	struct uwsgi_string_list *usl = ul->nodes;
	while (usl) {
		if (sendto(ul->socket, encrypted, e_len + 4, 0, usl->custom_ptr, usl->custom) != e_len + 4) {
			uwsgi_error("[uwsgi-legion] sendto()");
		}
		usl = usl->next;
	}

	uwsgi_buffer_destroy(ub);
	free(encrypted);
	return 0;
err:
	uwsgi_buffer_destroy(ub);
	free(encrypted);
	return -1;
}

void uwsgi_opt_legion_mcast(char *opt, char *value, void *foobar) {
	uwsgi_opt_legion(opt, value, foobar);
	char *legion = uwsgi_str(value);
	char *space = strchr(legion, ' ');	
	// over engineering
	if (!space) exit(1);
	*space = 0;
	struct uwsgi_legion *ul = uwsgi_legion_get_by_name(legion);
        if (!ul) {
                uwsgi_log("unknown legion: %s\n", legion);
                exit(1);
        }
	uwsgi_legion_register_node(ul, uwsgi_str(ul->addr));
	free(legion);
}

void uwsgi_opt_legion_node(char *opt, char *value, void *foobar) {

	char *legion = uwsgi_str(value);

	char *space = strchr(legion, ' ');
	if (!space) {
		uwsgi_log("invalid legion-node syntax, must be <legion> <addr>\n");
		exit(1);
	}
	*space = 0;

	struct uwsgi_legion *ul = uwsgi_legion_get_by_name(legion);
	if (!ul) {
		uwsgi_log("unknown legion: %s\n", legion);
		exit(1);
	}

	uwsgi_legion_register_node(ul, space + 1);
	
}

void uwsgi_legion_register_node(struct uwsgi_legion *ul, char *addr) {
	struct uwsgi_string_list *usl = uwsgi_string_new_list(&ul->nodes, addr);
	char *port = strchr(addr, ':');
	if (!port) {
		uwsgi_log("[uwsgi-legion] invalid udp address: %s\n", addr);
		exit(1);
	}
	// no need to zero the memory, socket_to_in_addr will do that
	struct sockaddr_in *sin = uwsgi_malloc(sizeof(struct sockaddr_in));
	usl->custom = socket_to_in_addr(addr, port, 0, sin);
	usl->custom_ptr = sin;
}

void uwsgi_opt_legion_quorum(char *opt, char *value, void *foobar) {

        char *legion = uwsgi_str(value);

        char *space = strchr(legion, ' ');
        if (!space) {
                uwsgi_log("invalid legion-quorum syntax, must be <legion> <quorum>\n");
                exit(1);
        }
        *space = 0;

        struct uwsgi_legion *ul = uwsgi_legion_get_by_name(legion);
        if (!ul) {
                uwsgi_log("unknown legion: %s\n", legion);
                exit(1);
        }

	ul->quorum = atoi(space+1);
	free(legion);
}

void uwsgi_opt_legion_scroll(char *opt, char *value, void *foobar) {

        char *legion = uwsgi_str(value);

        char *space = strchr(legion, ' ');
        if (!space) {
                uwsgi_log("invalid legion-scroll syntax, must be <legion> <scroll>\n");
                exit(1);
        }
        *space = 0;

        struct uwsgi_legion *ul = uwsgi_legion_get_by_name(legion);
        if (!ul) {
                uwsgi_log("unknown legion: %s\n", legion);
                exit(1);
        }

        ul->scroll = space+1;
	ul->scroll_len = strlen(ul->scroll);
	// DO NOT FREE IT !!!
        //free(legion);
}



void uwsgi_opt_legion_hook(char *opt, char *value, void *foobar) {

	char *event = strchr(opt, '-');
	if (!event) {
		uwsgi_log("[uwsgi-legion] invalid option name (%s), this should not happen (possible bug)\n", opt);
		exit(1);
	}
  
	char *legion = uwsgi_str(value);
	
	char *space = strchr(legion, ' ');
	if (!space) {
		uwsgi_log("[uwsgi-legion] invalid %s syntax, must be <legion> <action>\n", opt);
		exit(1);
	}
	*space = 0;

	struct uwsgi_legion *ul = uwsgi_legion_get_by_name(legion);
	if (!ul) {
		uwsgi_log("[uwsgi-legion] unknown legion: %s\n", legion);
		exit(1);
	}

	uwsgi_legion_register_hook(ul, event + 1, space + 1);
}

void uwsgi_legion_register_hook(struct uwsgi_legion *ul, char *event, char *action) {

	struct uwsgi_string_list *usl = NULL;

	if (!strcmp(event, "lord")) {
		usl = uwsgi_string_new_list(&ul->lord_hooks, action);
	}
	else if (!strcmp(event, "unlord")) {
		usl = uwsgi_string_new_list(&ul->unlord_hooks, action);
	}
	else if (!strcmp(event, "setup")) {
		usl = uwsgi_string_new_list(&ul->setup_hooks, action);
	}
	else if (!strcmp(event, "death")) {
		usl = uwsgi_string_new_list(&ul->death_hooks, action);
	}
	else if (!strcmp(event, "join")) {
		usl = uwsgi_string_new_list(&ul->join_hooks, action);
	}
	else if (!strcmp(event, "node-joined")) {
		usl = uwsgi_string_new_list(&ul->node_joined_hooks, action);
	}
	else if (!strcmp(event, "node-left")) {
		usl = uwsgi_string_new_list(&ul->node_left_hooks, action);
	}

	else {
		uwsgi_log("[uwsgi-legion] invalid event: %s\n", event);
		exit(1);
	}

	if (!usl)
		return;

	char *hook = strchr(action, ':');
	if (!hook) {
		uwsgi_log("[uwsgi-legion] invalid %s action: %s\n", event, action);
		exit(1);
	}

	// pointer to action plugin
	usl->custom_ptr = uwsgi_concat2n(action, hook - action, "", 0);;
	// add that to check the plugin value
	usl->custom = hook - action + 1;

}

void uwsgi_opt_legion(char *opt, char *value, void *foobar) {

	// legion addr valor algo:secret
	char *legion = uwsgi_str(value);
	char *space = strchr(legion, ' ');
	if (!space) {
		uwsgi_log("invalid legion syntax, must be <legion> <addr> <valor> <algo:secret>\n");
		exit(1);
	}
	*space = 0;
	char *addr = space + 1;

	space = strchr(addr, ' ');
	if (!space) {
		uwsgi_log("invalid legion syntax, must be <legion> <addr> <valor> <algo:secret>\n");
		exit(1);
	}
	*space = 0;
	char *valor = space + 1;

	space = strchr(valor, ' ');
	if (!space) {
		uwsgi_log("invalid legion syntax, must be <legion> <addr> <valor> <algo:secret>\n");
		exit(1);
	}
	*space = 0;
	char *algo_secret = space + 1;

	char *colon = strchr(algo_secret, ':');
	if (!colon) {
		uwsgi_log("invalid legion syntax, must be <legion> <addr> <valor> <algo:secret>\n");
		exit(1);
	}
	*colon = 0;
	char *secret = colon + 1;

	uwsgi_legion_register(legion, addr, valor, algo_secret, secret);
}

struct uwsgi_legion *uwsgi_legion_register(char *legion, char *addr, char *valor, char *algo, char *secret) {
	char *iv = strchr(secret, ' ');
	if (iv) {
		*iv = 0;
		iv++;
	}

	if (!uwsgi.ssl_initialized) {
		uwsgi_ssl_init();
	}

#if OPENSSL_VERSION_NUMBER < 0x10100000L
	EVP_CIPHER_CTX *ctx = uwsgi_malloc(sizeof(EVP_CIPHER_CTX));
	EVP_CIPHER_CTX_init(ctx);
#else
	EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
#endif

	const EVP_CIPHER *cipher = EVP_get_cipherbyname(algo);
	if (!cipher) {
		uwsgi_log("[uwsgi-legion] unable to find algorithm/cipher %s\n", algo);
		exit(1);
	}

	int cipher_len = EVP_CIPHER_key_length(cipher);
	size_t s_len = strlen(secret);
	if ((unsigned int) cipher_len > s_len) {
		char *secret_tmp = uwsgi_malloc(cipher_len);
		memcpy(secret_tmp, secret, s_len);
		memset(secret_tmp + s_len, 0, cipher_len - s_len);
		secret = secret_tmp;
	}

	int iv_len = EVP_CIPHER_iv_length(cipher);
	size_t s_iv_len = 0;
	if (iv) {
		s_iv_len = strlen(iv);
	}
	if ((unsigned int) iv_len > s_iv_len) {
                char *secret_tmp = uwsgi_malloc(iv_len);
                memcpy(secret_tmp, iv, s_iv_len);
                memset(secret_tmp + s_iv_len, '0', iv_len - s_iv_len);
                iv = secret_tmp;
        }

	if (EVP_EncryptInit_ex(ctx, cipher, NULL, (const unsigned char *) secret, (const unsigned char *) iv) <= 0) {
		uwsgi_error("EVP_EncryptInit_ex()");
		exit(1);
	}

#if OPENSSL_VERSION_NUMBER < 0x10100000L
	EVP_CIPHER_CTX *ctx2 = uwsgi_malloc(sizeof(EVP_CIPHER_CTX));
	EVP_CIPHER_CTX_init(ctx2);
#else
	EVP_CIPHER_CTX *ctx2 = EVP_CIPHER_CTX_new();
#endif

	if (EVP_DecryptInit_ex(ctx2, cipher, NULL, (const unsigned char *) secret, (const unsigned char *) iv) <= 0) {
		uwsgi_error("EVP_DecryptInit_ex()");
		exit(1);
	}

	// we use shared memory, as we want to export legion status to the api
	struct uwsgi_legion *ul = uwsgi_calloc_shared(sizeof(struct uwsgi_legion));
	ul->legion = legion;
	ul->legion_len = strlen(ul->legion);

	ul->valor = strtol(valor, (char **) NULL, 10);
	ul->addr = addr;

	ul->encrypt_ctx = ctx;
	ul->decrypt_ctx = ctx2;

	if (!uwsgi.legion_scroll_max_size) {
		uwsgi.legion_scroll_max_size = 4096;
	}

	if (!uwsgi.legion_scroll_list_max_size) {
		uwsgi.legion_scroll_list_max_size = 32768;
	}

	ul->lord_scroll_size = uwsgi.legion_scroll_max_size;
	ul->lord_scroll = uwsgi_calloc_shared(ul->lord_scroll_size);
	ul->scrolls_max_size = uwsgi.legion_scroll_list_max_size;
	ul->scrolls = uwsgi_calloc_shared(ul->scrolls_max_size);

	uwsgi_legion_add(ul);

	return ul;
}

struct uwsgi_legion_action *uwsgi_legion_action_get(char *name) {
	struct uwsgi_legion_action *ula = uwsgi.legion_actions;
	while (ula) {
		if (!strcmp(name, ula->name)) {
			return ula;
		}
		ula = ula->next;
	}
	return NULL;
}

struct uwsgi_legion_action *uwsgi_legion_action_register(char *name, int (*func) (struct uwsgi_legion *, char *)) {
	struct uwsgi_legion_action *found_ula = uwsgi_legion_action_get(name);
	if (found_ula) {
		uwsgi_log("[uwsgi-legion] action \"%s\" is already registered !!!\n", name);
		return found_ula;
	}

	struct uwsgi_legion_action *old_ula = NULL, *ula = uwsgi.legion_actions;
	while (ula) {
		old_ula = ula;
		ula = ula->next;
	}

	ula = uwsgi_calloc(sizeof(struct uwsgi_legion_action));
	ula->name = name;
	ula->func = func;

	if (old_ula) {
		old_ula->next = ula;
	}
	else {
		uwsgi.legion_actions = ula;
	}

	return ula;
}

void uwsgi_legion_announce_death(void) {
	struct uwsgi_legion *legion = uwsgi.legions;
        while (legion) {
                legion->dead = 1;
                uwsgi_legion_announce(legion);
                legion = legion->next;
        }
}

void uwsgi_legion_atexit(void) {
	struct uwsgi_legion *legion = uwsgi.legions;
	while (legion) {
		if (getpid() != legion->pid)
			goto next;
		struct uwsgi_string_list *usl = legion->death_hooks;
		while (usl) {
			int ret = uwsgi_legion_action_call("death", legion, usl);
			if (ret) {
				uwsgi_log("[uwsgi-legion] ERROR, death hook returned: %d\n", ret);
			}
			usl = usl->next;
		}
next:
		legion = legion->next;
	}

	// this must be called only by the master !!!
	if (!uwsgi.workers) return;
	if (uwsgi.workers[0].pid != getpid()) return;
	uwsgi_legion_announce_death();
}

int uwsgi_legion_i_am_the_lord(char *name) {
	struct uwsgi_legion *legion = uwsgi_legion_get_by_name(name);
	if (!legion) return 0;
	if (legion->i_am_the_lord) {
		return 1;
	}
	return 0;
}

char *uwsgi_legion_lord_scroll(char *name, uint16_t *rlen) {
	char *buf = NULL;
	struct uwsgi_legion *legion = uwsgi_legion_get_by_name(name);
        if (!legion) return 0;
	uwsgi_rlock(legion->lock);
	if (legion->lord_scroll_len > 0) {
		buf = uwsgi_malloc(legion->lord_scroll_len);
		memcpy(buf, legion->lord_scroll, legion->lord_scroll_len);
		*rlen = legion->lord_scroll_len;
	}
	uwsgi_rwunlock(legion->lock);
	return buf;
}

char *uwsgi_legion_scrolls(char *name, uint64_t *rlen) {
	char *buf = NULL;
        struct uwsgi_legion *legion = uwsgi_legion_get_by_name(name);
        if (!legion) return NULL;
	uwsgi_rlock(legion->lock);
	buf = uwsgi_malloc(legion->scrolls_len);
	memcpy(buf, legion->scrolls, legion->scrolls_len);
	*rlen = legion->scrolls_len;
	uwsgi_rwunlock(legion->lock);
	return buf;
}
