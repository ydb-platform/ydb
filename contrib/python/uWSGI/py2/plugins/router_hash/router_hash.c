#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

/*

	by Unbit

	syntax:

	route = /^foobar1(.*)/ hash:key=foo$1poo,algo=murmur2,var=MYHASH,items=node1;node2;node3

*/

struct uwsgi_router_hash_conf {

	char *key;
	size_t key_len;

	char *var;
	size_t var_len;

	char *algo;
	char *items;
	size_t items_len;
};

static int uwsgi_routing_func_hash(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_hash_conf *urhc = (struct uwsgi_router_hash_conf *) ur->data2;

	struct uwsgi_hash_algo *uha = uwsgi_hash_algo_get(urhc->algo);
	if (!uha) {
		uwsgi_log("[uwsgi-hash-router] unable to find hash algo \"%s\"\n", urhc->algo);
		return UWSGI_ROUTE_BREAK;
	}

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urhc->key, urhc->key_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	uint32_t h = uha->func(ub->buf, ub->pos);
        uwsgi_buffer_destroy(ub);

	// now count the number of items
	uint32_t items = 1;
	size_t i, ilen = urhc->items_len;
	for(i=0;i<ilen;i++) {
		if (urhc->items[i] == ';') items++;
	}

	// skip last semicolon
	if (urhc->items[ilen-1] == ';') items--;

	if (items < 1) return UWSGI_ROUTE_BREAK;

	uint32_t hashed_result = h % items;
	uint32_t found = 0;
	char *value = urhc->items;
	uint16_t vallen = 0;
	for(i=0;i<ilen;i++) {
		if (!value) {
			value = urhc->items + i;
		}
                if (urhc->items[i] == ';') {
			if (found == hashed_result) {
				vallen = (urhc->items+i) - value;
				break;
			}
			value = NULL;
			found++;
		}
        }

	if (vallen == 0) {
		// first item
		if (hashed_result == 0) {
			value = urhc->items;
			vallen = urhc->items_len;
		}
		// last item
		else if (value != NULL) {
			vallen = (urhc->items + urhc->items_len) - value;
		}
	}

	if (!vallen) {
		uwsgi_log("[uwsgi-hash-router] BUG !!! unable to hash items\n");
		return UWSGI_ROUTE_BREAK;
	}

	if (!uwsgi_req_append(wsgi_req, urhc->var, urhc->var_len, value, vallen)) {
		uwsgi_log("[uwsgi-hash-router] unable to append hash var to the request\n");
		return UWSGI_ROUTE_BREAK;
        }

        return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_hash(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_hash;
        ur->data = args;
        ur->data_len = strlen(args);
        struct uwsgi_router_hash_conf *urhc = uwsgi_calloc(sizeof(struct uwsgi_router_hash_conf));
                if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urhc->key,
                        "var", &urhc->var,
                        "algo", &urhc->algo,
                        "items", &urhc->items,
                        NULL)) {
                        uwsgi_log("invalid route syntax: %s\n", args);
                        exit(1);
                }

                if (!urhc->key || !urhc->var || !urhc->items) {
                        uwsgi_log("invalid route syntax: you need to specify a hash key, a var and a set of items\n");
                        exit(1);
                }

                urhc->key_len = strlen(urhc->key);
                urhc->var_len = strlen(urhc->var);
                urhc->items_len = strlen(urhc->items);


		if (!urhc->algo) urhc->algo = "djb33x";

                ur->data2 = urhc;
        return 0;
}

static void router_hash_register() {
	uwsgi_register_router("hash", uwsgi_router_hash);
}

struct uwsgi_plugin router_hash_plugin = {
	.name = "router_hash",
	.on_load = router_hash_register,
};

#else
struct uwsgi_plugin router_hash_plugin = {
	.name = "router_hash",
};
#endif
