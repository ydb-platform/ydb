#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	subscription subsystem

	each subscription slot is as an hashed item in a dictionary

	each slot has a circular linked list containing the nodes names

	the structure and system is very similar to uwsgi_dyn_dict already used by the mime type parser

	This system is not mean to run on shared memory. If you have multiple processes for the same app, you have to create
	a new subscriptions slot list.

	To avoid removal of already using nodes, a reference count system has been implemented

*/


extern struct uwsgi_server uwsgi;

#ifdef UWSGI_SSL
static void uwsgi_subscription_sni_check(struct uwsgi_subscribe_slot *current_slot, struct uwsgi_subscribe_req *usr) {
	if (usr->sni_key_len > 0 && usr->sni_crt_len > 0) {
		if (!current_slot->sni_enabled) {
			char *sni_key = uwsgi_concat2n(usr->sni_key, usr->sni_key_len, "", 0);
			char *sni_crt = uwsgi_concat2n(usr->sni_crt, usr->sni_crt_len, "", 0);
			char *sni_ca = NULL;
			if (usr->sni_ca_len > 0) {
				sni_ca = uwsgi_concat2n(usr->sni_ca, usr->sni_ca_len, "", 0);
			}
			char *servername = NULL;
			char *colon = memchr(current_slot->key, ':', current_slot->keylen);
			if (colon) {
				servername = uwsgi_concat2n(current_slot->key, colon - current_slot->key, "", 0);
			}
			else {
				servername = uwsgi_concat2n(current_slot->key, current_slot->keylen, "", 0);
			}
			if (uwsgi_ssl_add_sni_item(servername, sni_crt, sni_key, uwsgi.sni_dir_ciphers, sni_ca)) {
				current_slot->sni_enabled = 1;
			}
			if (sni_key)
				free(sni_key);
			if (sni_crt)
				free(sni_crt);
			if (sni_ca)
				free(sni_ca);
		}
	}
}
#endif

int uwsgi_subscription_credentials_check(struct uwsgi_subscribe_slot *slot, struct uwsgi_subscribe_req *usr) {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, uwsgi.subscriptions_credentials_check_dir) {
		char *filename = uwsgi_concat2n(usl->value, usl->len, slot->key, slot->keylen);
		struct stat st;
		int ret = stat(filename, &st);
		free(filename);
		if (ret != 0)
			continue;
		if (st.st_uid != usr->uid)
			continue;
		if (st.st_gid != usr->gid)
			continue;
		// accepted...
		return 1;
	}
	return 0;
}

struct uwsgi_subscribe_slot *uwsgi_get_subscribe_slot(struct uwsgi_subscribe_slot **slot, char *key, uint16_t keylen) {

	if (keylen > 0xff)
		return NULL;

	uint32_t hash = djb33x_hash(key, keylen);
	int hash_key = hash % 0xffff;

	struct uwsgi_subscribe_slot *current_slot = slot[hash_key];


#ifdef UWSGI_DEBUG
	uwsgi_log("****************************\n");
	while (current_slot) {
		uwsgi_log("slot %.*s %d\n", current_slot->keylen, current_slot->key, current_slot->hits);
		current_slot = current_slot->next;
	}
	uwsgi_log("****************************\n");
	current_slot = slot[hash_key];
#endif

	while (current_slot) {
		if (!uwsgi_strncmp(key, keylen, current_slot->key, current_slot->keylen)) {
			// auto optimization
			if (current_slot->prev) {
				if (current_slot->hits > current_slot->prev->hits) {
					struct uwsgi_subscribe_slot *slot_parent = current_slot->prev->prev, *slot_prev = current_slot->prev;
					if (slot_parent) {
						slot_parent->next = current_slot;
					}
					else {
						slot[hash_key] = current_slot;
					}

					if (current_slot->next) {
						current_slot->next->prev = slot_prev;
					}

					slot_prev->prev = current_slot;
					slot_prev->next = current_slot->next;

					current_slot->next = slot_prev;
					current_slot->prev = slot_parent;

				}
			}
			return current_slot;
		}
		current_slot = current_slot->next;
		// check for loopy optimization
		if (current_slot == slot[hash_key])
			break;
	}

	return NULL;
}

// least reference count
static struct uwsgi_subscribe_node *uwsgi_subscription_algo_lrc(struct uwsgi_subscribe_slot *current_slot, struct uwsgi_subscribe_node *node) {
	// if node is NULL we are in the second step (in lrc mode we do not use the first step)
	if (node)
		return NULL;

	struct uwsgi_subscribe_node *choosen_node = NULL;
	node = current_slot->nodes;
	uint64_t min_rc = 0;
	while (node) {
		if (!node->death_mark) {
			if (min_rc == 0 || node->reference < min_rc) {
				min_rc = node->reference;
				choosen_node = node;
				if (min_rc == 0 && !(node->next && node->next->reference <= node->reference && node->next->last_requests <= node->last_requests))
					break;
			}
		}
		node = node->next;
	}

	if (choosen_node) {
		choosen_node->reference++;
	}

	return choosen_node;
}

// weighted least reference count
static struct uwsgi_subscribe_node *uwsgi_subscription_algo_wlrc(struct uwsgi_subscribe_slot *current_slot, struct uwsgi_subscribe_node *node) {
	// if node is NULL we are in the second step (in wlrc mode we do not use the first step)
	if (node)
		return NULL;

	struct uwsgi_subscribe_node *choosen_node = NULL;
	node = current_slot->nodes;
	double min_rc = 0;
	while (node) {
		if (!node->death_mark) {
			// node->weight is always >= 1, we can safely use it as divider
			double ref = (double) node->reference / (double) node->weight;
			double next_node_ref = 0;
			if (node->next)
				next_node_ref = (double) node->next->reference / (double) node->next->weight;

			if (min_rc == 0 || ref < min_rc) {
				min_rc = ref;
				choosen_node = node;
				if (min_rc == 0 && !(node->next && next_node_ref <= ref && node->next->last_requests <= node->last_requests))
					break;
			}
		}
		node = node->next;
	}

	if (choosen_node) {
		choosen_node->reference++;
	}

	return choosen_node;
}

// weighted round robin algo
static struct uwsgi_subscribe_node *uwsgi_subscription_algo_wrr(struct uwsgi_subscribe_slot *current_slot, struct uwsgi_subscribe_node *node) {
	// if node is NULL we are in the second step
	if (node) {
		if (node->death_mark == 0 && node->wrr > 0) {
			node->wrr--;
			node->reference++;
			return node;
		}
		return NULL;
	}

	// no wrr > 0 node found, reset them
	node = current_slot->nodes;
	uint64_t min_weight = 0;
	while (node) {
		if (!node->death_mark) {
			if (min_weight == 0 || node->weight < min_weight)
				min_weight = node->weight;
		}
		node = node->next;
	}

	// now set wrr
	node = current_slot->nodes;
	struct uwsgi_subscribe_node *choosen_node = NULL;
	while (node) {
		if (!node->death_mark) {
			node->wrr = node->weight / min_weight;
			choosen_node = node;
		}
		node = node->next;
	}
	if (choosen_node) {
		choosen_node->wrr--;
		choosen_node->reference++;
	}
	return choosen_node;
}

void uwsgi_subscription_set_algo(char *algo) {

	if (!algo)
		goto wrr;

	if (!strcmp(algo, "wrr")) {
		uwsgi.subscription_algo = uwsgi_subscription_algo_wrr;
		return;
	}

	if (!strcmp(algo, "lrc")) {
		uwsgi.subscription_algo = uwsgi_subscription_algo_lrc;
		return;
	}

	if (!strcmp(algo, "wlrc")) {
		uwsgi.subscription_algo = uwsgi_subscription_algo_wlrc;
		return;
	}

wrr:
	uwsgi.subscription_algo = uwsgi_subscription_algo_wrr;
}

struct uwsgi_subscribe_node *uwsgi_get_subscribe_node(struct uwsgi_subscribe_slot **slot, char *key, uint16_t keylen) {

	if (keylen > 0xff)
		return NULL;

	struct uwsgi_subscribe_slot *current_slot = uwsgi_get_subscribe_slot(slot, key, keylen);
	if (!current_slot)
		return NULL;

	// slot found, move up in the list increasing hits
	current_slot->hits++;
	time_t now = uwsgi_now();
	struct uwsgi_subscribe_node *node = current_slot->nodes;
	while (node) {
		// is the node alive ?
		if (now - node->last_check > uwsgi.subscription_tolerance) {
			if (node->death_mark == 0)
				uwsgi_log("[uwsgi-subscription for pid %d] %.*s => marking %.*s as failed (no announce received in %d seconds)\n", (int) uwsgi.mypid, (int) keylen, key, (int) node->len, node->name, uwsgi.subscription_tolerance);
			node->failcnt++;
			node->death_mark = 1;
		}
		// do i need to remove the node ?
		if (node->death_mark && node->reference == 0) {
			// remove the node and move to next
			struct uwsgi_subscribe_node *dead_node = node;
			node = node->next;
			// if the slot has been removed, return NULL;
			if (uwsgi_remove_subscribe_node(slot, dead_node) == 1) {
				return NULL;
			}
			continue;
		}

		struct uwsgi_subscribe_node *choosen_node = uwsgi.subscription_algo(current_slot, node);
		if (choosen_node)
			return choosen_node;

		node = node->next;
	}

	return uwsgi.subscription_algo(current_slot, node);
}

struct uwsgi_subscribe_node *uwsgi_get_subscribe_node_by_name(struct uwsgi_subscribe_slot **slot, char *key, uint16_t keylen, char *val, uint16_t vallen) {

	if (keylen > 0xff)
		return NULL;
	struct uwsgi_subscribe_slot *current_slot = uwsgi_get_subscribe_slot(slot, key, keylen);
	if (current_slot) {
		struct uwsgi_subscribe_node *node = current_slot->nodes;
		while (node) {
			if (!uwsgi_strncmp(val, vallen, node->name, node->len)) {
				return node;
			}
			node = node->next;
		}
	}

	return NULL;
}

int uwsgi_remove_subscribe_node(struct uwsgi_subscribe_slot **slot, struct uwsgi_subscribe_node *node) {

	int ret = 0;

	struct uwsgi_subscribe_node *a_node;
	struct uwsgi_subscribe_slot *node_slot = node->slot;
	struct uwsgi_subscribe_slot *prev_slot = node_slot->prev;
	struct uwsgi_subscribe_slot *next_slot = node_slot->next;

	int hash_key = node_slot->hash;

	// over-engineering to avoid race conditions
	node->len = 0;

	if (node == node_slot->nodes) {
		node_slot->nodes = node->next;
	}
	else {
		a_node = node_slot->nodes;
		while (a_node) {
			if (a_node->next == node) {
				a_node->next = node->next;
				break;
			}
			a_node = a_node->next;
		}
	}

	free(node);
	// no more nodes, remove the slot too
	if (node_slot->nodes == NULL) {

		ret = 1;

		// first check if i am the only node
		if ((!prev_slot && !next_slot) || next_slot == node_slot) {
#ifdef UWSGI_SSL
			if (node_slot->sign_ctx) {
				EVP_PKEY_free(node_slot->sign_public_key);
				EVP_MD_CTX_destroy(node_slot->sign_ctx);
			}
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
			// if there is a SNI context active, destroy it
			if (node_slot->sni_enabled) {
				uwsgi_ssl_del_sni_item(node_slot->key, node_slot->keylen);
			}
#endif
#endif
			free(node_slot);
			slot[hash_key] = NULL;
			goto end;
		}

		// if i am the main entry point, set the next value
		if (node_slot == slot[hash_key]) {
			slot[hash_key] = next_slot;
		}

		if (prev_slot) {
			prev_slot->next = next_slot;
		}
		if (next_slot) {
			next_slot->prev = prev_slot;
		}

#ifdef UWSGI_SSL
		if (node_slot->sign_ctx) {
			EVP_PKEY_free(node_slot->sign_public_key);
			EVP_MD_CTX_destroy(node_slot->sign_ctx);
		}
#endif
		free(node_slot);
	}

end:

	return ret;
}

#ifdef UWSGI_SSL
static int subscription_new_sign_ctx(struct uwsgi_subscribe_slot *, struct uwsgi_subscribe_req *);
static int subscription_is_safe(struct uwsgi_subscribe_req *);
#endif

struct uwsgi_subscribe_node *uwsgi_add_subscribe_node(struct uwsgi_subscribe_slot **slot, struct uwsgi_subscribe_req *usr) {

	struct uwsgi_subscribe_slot *current_slot = uwsgi_get_subscribe_slot(slot, usr->key, usr->keylen), *old_slot = NULL, *a_slot;
	struct uwsgi_subscribe_node *node, *old_node = NULL;

	if (usr->address_len > 0xff || usr->address_len == 0)
		return NULL;

	if (current_slot) {
#ifdef UWSGI_SSL
		if (uwsgi.subscriptions_sign_check_dir && !uwsgi_subscription_sign_check(current_slot, usr)) {
			return NULL;
		}
#endif

		if (uwsgi.subscriptions_credentials_check_dir && !uwsgi_subscription_credentials_check(current_slot, usr)) {
			return NULL;
		}

		node = current_slot->nodes;
		while (node) {
			if (!uwsgi_strncmp(node->name, node->len, usr->address, usr->address_len)) {
#ifdef UWSGI_SSL
				// this should avoid sending sniffed packets...
				if (current_slot->sign_ctx && !subscription_is_safe(usr) && usr->unix_check <= node->unix_check) {
					uwsgi_log("[uwsgi-subscription for pid %d] invalid (sniffed ?) packet sent for slot: %.*s node: %.*s unix_check: %lu\n", (int) uwsgi.mypid, usr->keylen, usr->key, usr->address_len, usr->address, (unsigned long) usr->unix_check);
					return NULL;
				}
				// eventually the packet could be upgraded to sni...
				uwsgi_subscription_sni_check(current_slot, usr);
#endif
				// remove death mark and update cores and load
				node->death_mark = 0;
				node->last_check = uwsgi_now();
				node->cores = usr->cores;
				node->load = usr->load;
				node->weight = usr->weight;
				if (!node->weight)
					node->weight = 1;
				node->last_requests = 0;
				return node;
			}
			old_node = node;
			node = node->next;
		}

#ifdef UWSGI_SSL
		if (current_slot->sign_ctx && !subscription_is_safe(usr) && usr->unix_check < (uwsgi_now() - (time_t) uwsgi.subscriptions_sign_check_tolerance)) {
			uwsgi_log("[uwsgi-subscription for pid %d] invalid (sniffed ?) packet sent for slot: %.*s node: %.*s unix_check: %lu\n", (int) uwsgi.mypid, usr->keylen, usr->key, usr->address_len, usr->address, (unsigned long) usr->unix_check);
                        return NULL;
		}
		// check here as we are sure the node will be added
		uwsgi_subscription_sni_check(current_slot, usr);
#endif

		node = uwsgi_malloc(sizeof(struct uwsgi_subscribe_node));
		node->len = usr->address_len;
		node->modifier1 = usr->modifier1;
		node->modifier2 = usr->modifier2;
		node->requests = 0;
		node->last_requests = 0;
		node->tx = 0;
		node->rx = 0;
		node->reference = 0;
		node->death_mark = 0;
		node->failcnt = 0;
		node->cores = usr->cores;
		node->load = usr->load;
		node->weight = usr->weight;
		node->unix_check = usr->unix_check;
		if (!node->weight)
			node->weight = 1;
		node->wrr = 0;
		node->pid = usr->pid;
		node->uid = usr->uid;
		node->gid = usr->gid;
		node->notify[0] = 0;
		if (usr->notify_len > 0 && usr->notify_len < 102) {
			memcpy(node->notify, usr->notify, usr->notify_len);
			node->notify[usr->notify_len] = 0;
		}
		node->last_check = uwsgi_now();
		node->slot = current_slot;
		memcpy(node->name, usr->address, usr->address_len);
		if (old_node) {
			old_node->next = node;
		}
		node->next = NULL;

		uwsgi_log("[uwsgi-subscription for pid %d] %.*s => new node: %.*s\n", (int) uwsgi.mypid, usr->keylen, usr->key, usr->address_len, usr->address);
		if (node->notify[0]) {
			char buf[1024];
			int ret = snprintf(buf, 1024, "[subscription ack] %.*s => new node: %.*s", usr->keylen, usr->key, usr->address_len, usr->address);
			if (ret > 0 && ret < 1024)
				uwsgi_notify_msg(node->notify, buf, ret);
		}
		return node;
	}
	else {
		current_slot = uwsgi_malloc(sizeof(struct uwsgi_subscribe_slot));
#ifdef UWSGI_SSL
		current_slot->sign_ctx = NULL;
		if (uwsgi.subscriptions_sign_check_dir && !subscription_new_sign_ctx(current_slot, usr)) {
			free(current_slot);
			return NULL;
		}
#endif
		uint32_t hash = djb33x_hash(usr->key, usr->keylen);
		int hash_key = hash % 0xffff;
		current_slot->hash = hash_key;
		current_slot->keylen = usr->keylen;
		memcpy(current_slot->key, usr->key, usr->keylen);
		if (uwsgi.subscriptions_credentials_check_dir) {
			if (!uwsgi_subscription_credentials_check(current_slot, usr)) {
				free(current_slot);
				return NULL;
			}
		}

		current_slot->key[usr->keylen] = 0;
		current_slot->hits = 0;
#ifdef UWSGI_SSL
		current_slot->sni_enabled = 0;
		uwsgi_subscription_sni_check(current_slot, usr);
#endif
		current_slot->nodes = uwsgi_malloc(sizeof(struct uwsgi_subscribe_node));
		current_slot->nodes->slot = current_slot;
		current_slot->nodes->len = usr->address_len;
		current_slot->nodes->reference = 0;
		current_slot->nodes->requests = 0;
		current_slot->nodes->last_requests = 0;
		current_slot->nodes->tx = 0;
		current_slot->nodes->rx = 0;
		current_slot->nodes->death_mark = 0;
		current_slot->nodes->failcnt = 0;
		current_slot->nodes->modifier1 = usr->modifier1;
		current_slot->nodes->modifier2 = usr->modifier2;
		current_slot->nodes->cores = usr->cores;
		current_slot->nodes->load = usr->load;
		current_slot->nodes->weight = usr->weight;
		current_slot->nodes->unix_check = usr->unix_check;
		if (!current_slot->nodes->weight)
			current_slot->nodes->weight = 1;
		current_slot->nodes->wrr = 0;
		current_slot->nodes->pid = usr->pid;
		current_slot->nodes->uid = usr->uid;
		current_slot->nodes->gid = usr->gid;
		current_slot->nodes->notify[0] = 0;
		if (usr->notify_len > 0 && usr->notify_len < 102) {
			memcpy(current_slot->nodes->notify, usr->notify, usr->notify_len);
			current_slot->nodes->notify[usr->notify_len] = 0;
		}
		memcpy(current_slot->nodes->name, usr->address, usr->address_len);
		current_slot->nodes->last_check = uwsgi_now();

		current_slot->nodes->next = NULL;

		a_slot = slot[hash_key];
		while (a_slot) {
			old_slot = a_slot;
			a_slot = a_slot->next;
		}


		if (old_slot) {
			old_slot->next = current_slot;
		}

		current_slot->prev = old_slot;
		current_slot->next = NULL;


		if (!slot[hash_key] || current_slot->prev == NULL) {
			slot[hash_key] = current_slot;
		}

		uwsgi_log("[uwsgi-subscription for pid %d] new pool: %.*s (hash key: %d)\n", (int) uwsgi.mypid, usr->keylen, usr->key, current_slot->hash);
		uwsgi_log("[uwsgi-subscription for pid %d] %.*s => new node: %.*s\n", (int) uwsgi.mypid, usr->keylen, usr->key, usr->address_len, usr->address);

		if (current_slot->nodes->notify[0]) {
			char buf[1024];
			int ret = snprintf(buf, 1024, "[subscription ack] %.*s => new node: %.*s", usr->keylen, usr->key, usr->address_len, usr->address);
			if (ret > 0 && ret < 1024)
				uwsgi_notify_msg(current_slot->nodes->notify, buf, ret);
		}
		return current_slot->nodes;
	}

}

static void send_subscription(int sfd, char *host, char *message, uint16_t message_size) {

	int fd = sfd;
	struct sockaddr_in udp_addr;
	struct sockaddr_un un_addr;
	ssize_t ret;

	char *udp_port = strchr(host, ':');

	if (fd == -1) {
		if (udp_port) {
			fd = socket(AF_INET, SOCK_DGRAM, 0);
		}
		else {
			fd = socket(AF_UNIX, SOCK_DGRAM, 0);
		}
		if (fd < 0) {
			uwsgi_error("send_subscription()/socket()");
			return;
		}
		uwsgi_socket_nb(fd);
	}
	else if (fd == -2) {
		static int unix_fd = -1;
		static int inet_fd = -1;
		if (udp_port) {
			if (inet_fd == -1) {
				inet_fd = socket(AF_INET, SOCK_DGRAM, 0);
				if (inet_fd < 0) {
					uwsgi_error("send_subscription()/socket()");
					return;
				}
				uwsgi_socket_nb(inet_fd);
			}
			fd = inet_fd;
		}
		else {
			if (unix_fd == -1) {
				unix_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
				if (unix_fd < 0) {
					uwsgi_error("send_subscription()/socket()");
					return;
				}
				uwsgi_socket_nb(unix_fd);
			}
			fd = unix_fd;
		}
	}

	if (udp_port) {
		udp_port[0] = 0;
		memset(&udp_addr, 0, sizeof(struct sockaddr_in));
		udp_addr.sin_family = AF_INET;
		udp_addr.sin_port = htons(atoi(udp_port + 1));
		udp_addr.sin_addr.s_addr = inet_addr(host);
		ret = sendto(fd, message, message_size, 0, (struct sockaddr *) &udp_addr, sizeof(udp_addr));
		udp_port[0] = ':';
	}
	else {
		memset(&un_addr, 0, sizeof(struct sockaddr_un));
		un_addr.sun_family = AF_UNIX;
		// use 102 as the magic number
		strncat(un_addr.sun_path, host, 102);
		if (uwsgi.subscriptions_use_credentials) {
			// could be useless as internally the socket could add them automagically
			ret = uwsgi_pass_cred2(fd, message, message_size, (struct sockaddr *) &un_addr, sizeof(un_addr));
		}
		else {
			ret = sendto(fd, message, message_size, 0, (struct sockaddr *) &un_addr, sizeof(un_addr));
		}
	}

	if (ret < 0) {
		uwsgi_error("send_subscription()/sendto()");
	}

	if (sfd == -1)
		close(fd);
}

static struct uwsgi_buffer *uwsgi_subscription_ub(char *key, size_t keysize, uint8_t modifier1, uint8_t modifier2, uint8_t cmd, char *socket_name, char *sign, char *sni_key, char *sni_crt, char *sni_ca) {
	struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);

	// make space for uwsgi header
	ub->pos = 4;

	if (uwsgi_buffer_append_keyval(ub, "key", 3, key, keysize))
		goto end;
	if (uwsgi_buffer_append_keyval(ub, "address", 7, socket_name, strlen(socket_name)))
		goto end;

	if (uwsgi.subscribe_with_modifier1) {
		modifier1 = atoi(uwsgi.subscribe_with_modifier1);
	}
	if (uwsgi_buffer_append_keynum(ub, "modifier1", 9, modifier1))
		goto end;
	if (uwsgi_buffer_append_keynum(ub, "modifier2", 9, modifier2))
		goto end;
	if (uwsgi_buffer_append_keynum(ub, "cores", 5, uwsgi.numproc * uwsgi.cores))
		goto end;
	if (uwsgi_buffer_append_keynum(ub, "load", 4, uwsgi.shared->load))
		goto end;
	if (uwsgi.auto_weight) {
		if (uwsgi_buffer_append_keynum(ub, "weight", 6, uwsgi.numproc * uwsgi.cores))
			goto end;
	}
	else {
		if (uwsgi_buffer_append_keynum(ub, "weight", 6, uwsgi.weight))
			goto end;
	}

	if (sni_key) {
		if (uwsgi_buffer_append_keyval(ub, "sni_key", 7, sni_key, strlen(sni_key)))
			goto end;
	}

	if (sni_crt) {
		if (uwsgi_buffer_append_keyval(ub, "sni_crt", 7, sni_crt, strlen(sni_crt)))
			goto end;
	}

	if (sni_ca) {
		if (uwsgi_buffer_append_keyval(ub, "sni_ca", 6, sni_ca, strlen(sni_ca)))
			goto end;
	}

	if (uwsgi.subscription_notify_socket) {
		if (uwsgi_buffer_append_keyval(ub, "notify", 6, uwsgi.subscription_notify_socket, strlen(uwsgi.subscription_notify_socket)))
			goto end;
	}
	else if (uwsgi.notify_socket_fd > -1 && uwsgi.notify_socket) {
		if (uwsgi_buffer_append_keyval(ub, "notify", 6, uwsgi.notify_socket, strlen(uwsgi.notify_socket)))
			goto end;
	}

#ifdef UWSGI_SSL
	if (sign) {
		if (uwsgi_buffer_append_keynum(ub, "unix", 4, (uwsgi_now() + (time_t) cmd)))
			goto end;

		unsigned int signature_len = 0;
		char *signature = uwsgi_rsa_sign(sign, ub->buf + 4, ub->pos - 4, &signature_len);
		if (signature && signature_len > 0) {
			if (uwsgi_buffer_append_keyval(ub, "sign", 4, signature, signature_len)) {
				free(signature);
				goto end;
			}
			free(signature);
		}
	}
#endif

	// add uwsgi header
	if (uwsgi_buffer_set_uh(ub, 224, cmd))
		goto end;

	return ub;

end:
	uwsgi_buffer_destroy(ub);
	return NULL;
}

void uwsgi_send_subscription_from_fd(int fd, char *udp_address, char *key, size_t keysize, uint8_t modifier1, uint8_t modifier2, uint8_t cmd, char *socket_name, char *sign, char *sni_key, char *sni_crt, char *sni_ca) {

	if (socket_name == NULL && !uwsgi.sockets)
		return;

	if (!socket_name) {
		socket_name = uwsgi.sockets->name;
	}

	struct uwsgi_buffer *ub = uwsgi_subscription_ub(key, keysize, modifier1, modifier2, cmd, socket_name, sign, sni_key, sni_crt, sni_ca);

	if (!ub)
		return;

	send_subscription(fd, udp_address, ub->buf, ub->pos);
	uwsgi_buffer_destroy(ub);
}


void uwsgi_send_subscription(char *udp_address, char *key, size_t keysize, uint8_t modifier1, uint8_t modifier2, uint8_t cmd, char *socket_name, char *sign, char *sni_key, char *sni_crt, char *sni_ca) {
	uwsgi_send_subscription_from_fd(-1, udp_address, key, keysize, modifier1, modifier2, cmd, socket_name, sign, sni_key, sni_crt, sni_ca);
}

#ifdef UWSGI_SSL
static int subscription_is_safe(struct uwsgi_subscribe_req *usr) {
	struct uwsgi_string_list *usl = NULL;
        uwsgi_foreach(usl, uwsgi.subscriptions_sign_skip_uid) {
                if (usl->custom == 0) {
                        usl->custom = atoi(usl->value);
                }
                if (usr->uid > 0 && usr->uid == (uid_t) usl->custom) {
                        return 1;
                }
        }
	return 0;
}
static int subscription_new_sign_ctx(struct uwsgi_subscribe_slot *slot, struct uwsgi_subscribe_req *usr) {
	if (subscription_is_safe(usr)) return 1;

	if (usr->sign_len == 0 || usr->base_len == 0)
		return 0;

	if (usr->unix_check < (uwsgi_now() - (time_t) uwsgi.subscriptions_sign_check_tolerance)) {
        	uwsgi_log("[uwsgi-subscription for pid %d] invalid (sniffed ?) packet sent for slot: %.*s node: %.*s unix_check: %lu\n", (int) uwsgi.mypid, usr->keylen, usr->key, usr->address_len, usr->address, (unsigned long) usr->unix_check);
		return 0;
        }

	char *keyfile = uwsgi_sanitize_cert_filename(uwsgi.subscriptions_sign_check_dir, usr->key, usr->keylen);
	FILE *kf = fopen(keyfile, "r");
	free(keyfile);
	if (!kf) return 0;
	slot->sign_public_key = PEM_read_PUBKEY(kf, NULL, NULL, NULL);
	fclose(kf);
	if (!slot->sign_public_key) {
        	uwsgi_log("unable to load public key for %.*s\n", usr->keylen, usr->key);
		return 0;
	}
	slot->sign_ctx = EVP_MD_CTX_create();
	if (!slot->sign_ctx) {
        	uwsgi_log("unable to initialize EVP context for %.*s\n", usr->keylen, usr->key);
                EVP_PKEY_free(slot->sign_public_key);
		return 0;
	}

	if (!uwsgi_subscription_sign_check(slot, usr)) {
		EVP_PKEY_free(slot->sign_public_key);
		EVP_MD_CTX_destroy(slot->sign_ctx);
		return 0;
	}

	return 1;
}
int uwsgi_subscription_sign_check(struct uwsgi_subscribe_slot *slot, struct uwsgi_subscribe_req *usr) {
	if (subscription_is_safe(usr)) return 1;

	if (usr->sign_len == 0 || usr->base_len == 0)
		return 0;

	if (!slot->sign_ctx) {
		if (!subscription_new_sign_ctx(slot, usr)) return 0;
	}

	if (EVP_VerifyInit_ex(slot->sign_ctx, uwsgi.subscriptions_sign_check_md, NULL) == 0) {
		ERR_print_errors_fp(stderr);
		return 0;
	}

	if (EVP_VerifyUpdate(slot->sign_ctx, usr->base, usr->base_len) == 0) {
		ERR_print_errors_fp(stderr);
		return 0;
	}

	if (EVP_VerifyFinal(slot->sign_ctx, (unsigned char *) usr->sign, usr->sign_len, slot->sign_public_key) != 1) {
#ifdef UWSGI_DEBUG
		ERR_print_errors_fp(stderr);
#endif
		return 0;
	}


	return 1;
}
#endif

int uwsgi_no_subscriptions(struct uwsgi_subscribe_slot **slot) {
	int i;
	for (i = 0; i < UMAX16; i++) {
		if (slot[i])
			return 0;
	}
	return 1;
}

struct uwsgi_subscribe_slot **uwsgi_subscription_init_ht() {
	if (!uwsgi.subscription_algo) {
		uwsgi_subscription_set_algo(NULL);
	}
	return uwsgi_calloc(sizeof(struct uwsgi_subscription_slot *) * UMAX16);
}

void uwsgi_subscribe(char *subscription, uint8_t cmd) {

	size_t subfile_size;
	size_t i;
	char *key = NULL;
	int keysize = 0;
	char *modifier1 = NULL;
	int modifier1_len = 0;
	char *socket_name = NULL;
	char *udp_address = subscription;
	char *udp_port = NULL;
	char *subscription_key = NULL;
	char *sign = NULL;

	// check for explicit socket_name
	char *equal = strchr(subscription, '=');
	if (equal) {
		socket_name = subscription;
		if (socket_name[0] == '=') {
			equal = strchr(socket_name + 1, '=');
			if (!equal)
				return;
			*equal = '\0';
			struct uwsgi_socket *us = uwsgi_get_shared_socket_by_num(atoi(socket_name + 1));
			if (!us)
				return;
			socket_name = us->name;
		}
		*equal = '\0';
		udp_address = equal + 1;
	}

	// check for unix socket
	if (udp_address[0] != '/') {
		udp_port = strchr(udp_address, ':');
		if (!udp_port) {
			if (equal)
				*equal = '=';
			return;
		}
		subscription_key = strchr(udp_port + 1, ':');
	}
	else {
		subscription_key = strchr(udp_address + 1, ':');
	}

	if (!subscription_key) {
		if (equal)
			*equal = '=';
		return;
	}

	udp_address = uwsgi_concat2n(udp_address, subscription_key - udp_address, "", 0);

	if (subscription_key[1] == '@') {
		if (!uwsgi_file_exists(subscription_key + 2))
			goto clear;
		char *lines = uwsgi_open_and_read(subscription_key + 2, &subfile_size, 1, NULL);
		if (subfile_size > 0) {
			key = lines;
			for (i = 0; i < subfile_size; i++) {
				if (lines[i] == 0) {
					if (keysize > 0) {
						if (key[0] != '#' && key[0] != '\n') {
							modifier1 = strchr(key, ',');
							if (modifier1) {
								modifier1[0] = 0;
								modifier1++;
								modifier1_len = strlen(modifier1);
								keysize = strlen(key);
							}
							uwsgi_send_subscription(udp_address, key, keysize, uwsgi_str_num(modifier1, modifier1_len), 0, cmd, socket_name, sign, NULL, NULL, NULL);
							modifier1 = NULL;
							modifier1_len = 0;
						}
					}
					break;
				}
				else if (lines[i] == '\n') {
					if (keysize > 0) {
						if (key[0] != '#' && key[0] != '\n') {
							lines[i] = 0;
							modifier1 = strchr(key, ',');
							if (modifier1) {
								modifier1[0] = 0;
								modifier1++;
								modifier1_len = strlen(modifier1);
								keysize = strlen(key);
							}
							uwsgi_send_subscription(udp_address, key, keysize, uwsgi_str_num(modifier1, modifier1_len), 0, cmd, socket_name, sign, NULL, NULL, NULL);
							modifier1 = NULL;
							modifier1_len = 0;
							lines[i] = '\n';
						}
					}
					key = lines + i + 1;
					keysize = 0;
					continue;
				}
				keysize++;
			}
		}
		free(lines);
	}
	else {
		modifier1 = strchr(subscription_key + 1, ',');
		if (modifier1) {
			modifier1[0] = 0;
			modifier1++;

			sign = strchr(modifier1 + 1, ',');
			if (sign) {
				*sign = 0;
				sign++;
			}
			modifier1_len = strlen(modifier1);
		}

		uwsgi_send_subscription(udp_address, subscription_key + 1, strlen(subscription_key + 1), uwsgi_str_num(modifier1, modifier1_len), 0, cmd, socket_name, sign, NULL, NULL, NULL);
		if (modifier1)
			modifier1[-1] = ',';
		if (sign)
			sign[-1] = ',';
	}

clear:
	if (equal)
		*equal = '=';
	free(udp_address);

}

void uwsgi_subscribe2(char *arg, uint8_t cmd) {

	char *s2_server = NULL;
	char *s2_key = NULL;
	char *s2_socket = NULL;
	char *s2_addr = NULL;
	char *s2_weight = NULL;
	char *s2_sign = NULL;
	char *s2_modifier1 = NULL;
	char *s2_modifier2 = NULL;
	char *s2_check = NULL;
	char *s2_sni_key = NULL;
	char *s2_sni_crt = NULL;
	char *s2_sni_ca = NULL;

	if (uwsgi_kvlist_parse(arg, strlen(arg), ',', '=', "server", &s2_server, "key", &s2_key, "socket", &s2_socket, "addr", &s2_addr, "weight", &s2_weight, "modifier1", &s2_modifier1, "modifier2", &s2_modifier2, "sign", &s2_sign, "check", &s2_check, "sni_key", &s2_sni_key, "sni_crt", &s2_sni_crt, "sni_ca", &s2_sni_ca, NULL)) {
		return;
	}

	if (!s2_server || !s2_key)
		goto end;

	if (s2_check) {
		if (uwsgi_file_exists(s2_check))
			goto end;
	}

	if (s2_weight) {
		uwsgi.weight = atoi(s2_weight);
	}

	if (s2_socket) {
		struct uwsgi_socket *us = uwsgi_get_socket_by_num(atoi(s2_socket));
		if (us) {
			if (s2_addr) {
				free(s2_addr);
			}
			s2_addr = uwsgi_str(us->name);
		}
	}

	uint8_t modifier1 = 0;
	uint8_t modifier2 = 0;

	if (s2_modifier1) {
		modifier1 = atoi(s2_modifier1);
	}

	if (s2_modifier2) {
		modifier2 = atoi(s2_modifier2);
	}

	uwsgi_send_subscription(s2_server, s2_key, strlen(s2_key), modifier1, modifier2, cmd, s2_addr, s2_sign, s2_sni_key, s2_sni_crt, s2_sni_ca);
end:
	if (s2_server)
		free(s2_server);
	if (s2_key)
		free(s2_key);
	if (s2_socket)
		free(s2_socket);
	if (s2_addr)
		free(s2_addr);
	if (s2_weight)
		free(s2_weight);
	if (s2_modifier1)
		free(s2_modifier1);
	if (s2_modifier2)
		free(s2_modifier2);
	if (s2_sign)
		free(s2_sign);
	if (s2_check)
		free(s2_check);
	if (s2_sni_crt)
		free(s2_sni_crt);
	if (s2_sni_key)
		free(s2_sni_key);
	if (s2_sni_ca)
		free(s2_sni_ca);
}

void uwsgi_subscribe_all(uint8_t cmd, int verbose) {

	if (uwsgi.subscriptions_blocked)
		return;
	// -- subscribe
	struct uwsgi_string_list *subscriptions = uwsgi.subscriptions;
	while (subscriptions) {
		if (verbose) {
			uwsgi_log("%s %s\n", cmd ? "unsubscribing from" : "subscribing to", subscriptions->value);
		}
		uwsgi_subscribe(subscriptions->value, cmd);
		subscriptions = subscriptions->next;
	}

	// --subscribe2
	subscriptions = uwsgi.subscriptions2;
	while (subscriptions) {
		if (verbose) {
			uwsgi_log("%s %s\n", cmd ? "unsubscribing from" : "subscribing to", subscriptions->value);
		}
		uwsgi_subscribe2(subscriptions->value, cmd);
		subscriptions = subscriptions->next;
	}

}
