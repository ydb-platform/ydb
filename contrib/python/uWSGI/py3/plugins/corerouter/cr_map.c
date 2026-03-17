#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

#include "cr.h"

extern struct uwsgi_server uwsgi;

int uwsgi_cr_map_use_void(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	return 0;
}

int uwsgi_cr_map_use_cache(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	uint64_t hits = 0;
	uwsgi_rlock(ucr->cache->lock);
	char *value = uwsgi_cache_get4(ucr->cache, peer->key, peer->key_len, &peer->instance_address_len, &hits);
	if (!value)
		goto end;
	peer->tmp_socket_name = uwsgi_concat2n(value, peer->instance_address_len, "", 0);
	size_t nodes = uwsgi_str_occurence(peer->tmp_socket_name, peer->instance_address_len, '|');
	if (nodes > 0) {
		size_t choosen_node = hits % (nodes + 1);
		size_t choosen_node_len = 0;
		peer->instance_address = uwsgi_str_split_nget(peer->tmp_socket_name, peer->instance_address_len, '|', choosen_node, &choosen_node_len);
		if (!peer->instance_address)
			goto end;
		peer->instance_address_len = choosen_node_len;
	}
	else {
		peer->instance_address = peer->tmp_socket_name;
	}
	char *cs_mod = uwsgi_str_contains(peer->instance_address, peer->instance_address_len, ',');
	if (cs_mod) {
		peer->modifier1 = uwsgi_str_num(cs_mod + 1, (peer->instance_address_len - (cs_mod - peer->instance_address)) - 1);
		peer->instance_address_len = (cs_mod - peer->instance_address);
	}
end:
	uwsgi_rwunlock(ucr->cache->lock);
	return 0;
}

int uwsgi_cr_map_use_pattern(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	size_t tmp_socket_name_len = 0;
	ucr->magic_table['s'] = uwsgi_concat2n(peer->key, peer->key_len, "", 0);
	peer->tmp_socket_name = magic_sub(ucr->pattern, ucr->pattern_len, &tmp_socket_name_len, ucr->magic_table);
	free(ucr->magic_table['s']);
	peer->instance_address_len = tmp_socket_name_len;
	peer->instance_address = peer->tmp_socket_name;
	return 0;
}


int uwsgi_cr_map_use_subscription(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {

	peer->un = uwsgi_get_subscribe_node(ucr->subscriptions, peer->key, peer->key_len);
	if((peer->un == NULL) && (ucr->fallback_key != NULL)) {
		peer->un = uwsgi_get_subscribe_node(ucr->subscriptions, ucr->fallback_key, ucr->fallback_key_len);
	}
	if (peer->un && peer->un->len) {
		peer->instance_address = peer->un->name;
		peer->instance_address_len = peer->un->len;
		peer->modifier1 = peer->un->modifier1;
		peer->modifier2 = peer->un->modifier2;
	}
	else if (ucr->cheap && !ucr->i_am_cheap && uwsgi_no_subscriptions(ucr->subscriptions)) {
		uwsgi_gateway_go_cheap(ucr->name, ucr->queue, &ucr->i_am_cheap);
	}

	return 0;
}

int uwsgi_cr_map_use_subscription_dotsplit(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {

	char *name = peer->key;
	uint16_t name_len = peer->key_len;
	// max 5 split, reduce DOS attempts
	int count = 5;

split:
	if (!count)
		return 0;
#ifdef UWSGI_DEBUG
	uwsgi_log("trying with %.*s\n", name_len, name);
#endif
	peer->un = uwsgi_get_subscribe_node(ucr->subscriptions, name, name_len);
	if (!peer->un) {
		char *next = memchr(name + 1, '.', name_len - 1);
		if (next) {
			name_len -= next - name;
			name = next;
			count--;
			goto split;
		}
	}

	if (peer->un && peer->un->len) {
		peer->instance_address = peer->un->name;
		peer->instance_address_len = peer->un->len;
		peer->modifier1 = peer->un->modifier1;
		peer->modifier2 = peer->un->modifier2;
	}
	else if (ucr->cheap && !ucr->i_am_cheap && uwsgi_no_subscriptions(ucr->subscriptions)) {
		uwsgi_gateway_go_cheap(ucr->name, ucr->queue, &ucr->i_am_cheap);
	}

	return 0;
}


int uwsgi_cr_map_use_base(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {

	int tmp_socket_name_len = 0;

	peer->tmp_socket_name = uwsgi_concat2nn(ucr->base, ucr->base_len, peer->key, peer->key_len, &tmp_socket_name_len);
	peer->instance_address_len = tmp_socket_name_len;
	peer->instance_address = peer->tmp_socket_name;

	return 0;
}


int uwsgi_cr_map_use_cs(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	if (uwsgi.p[ucr->code_string_modifier1]->code_string) {
		char *name = uwsgi_concat2("uwsgi_", ucr->short_name);
		peer->instance_address = uwsgi.p[ucr->code_string_modifier1]->code_string(name, ucr->code_string_code, ucr->code_string_function, peer->key, peer->key_len);
		free(name);
		if (peer->instance_address) {
			peer->instance_address_len = strlen(peer->instance_address);
			char *cs_mod = uwsgi_str_contains(peer->instance_address, peer->instance_address_len, ',');
			if (cs_mod) {
				peer->modifier1 = uwsgi_str_num(cs_mod + 1, (peer->instance_address_len - (cs_mod - peer->instance_address)) - 1);
				peer->instance_address_len = (cs_mod - peer->instance_address);
			}
		}
	}
	return 0;
}

int uwsgi_cr_map_use_to(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	peer->instance_address = ucr->to_socket->name;
	peer->instance_address_len = ucr->to_socket->name_len;
	return 0;
}

int uwsgi_cr_map_use_static_nodes(struct uwsgi_corerouter *ucr, struct corerouter_peer *peer) {
	if (!ucr->current_static_node) {
		ucr->current_static_node = ucr->static_nodes;
	}

	peer->static_node = ucr->current_static_node;

	// is it a dead node ?
	if (peer->static_node->custom > 0) {

		// gracetime passed ?
		if (peer->static_node->custom + ucr->static_node_gracetime <= (uint64_t) uwsgi_now()) {
			peer->static_node->custom = 0;
		}
		else {
			struct uwsgi_string_list *tmp_node = peer->static_node;
			struct uwsgi_string_list *next_node = peer->static_node->next;
			peer->static_node = NULL;
			// needed for 1-node only setups
			if (!next_node)
				next_node = ucr->static_nodes;

			while (tmp_node != next_node) {
				if (!next_node) {
					next_node = ucr->static_nodes;
				}

				if (tmp_node == next_node)
					break;

				if (next_node->custom == 0) {
					peer->static_node = next_node;
					break;
				}
				next_node = next_node->next;
			}
		}
	}

	if (peer->static_node) {

		peer->instance_address = peer->static_node->value;
		peer->instance_address_len = peer->static_node->len;
		// set the next one
		ucr->current_static_node = peer->static_node->next;
	}
	else {
		// set the next one
		ucr->current_static_node = ucr->current_static_node->next;
	}

	return 0;

}
