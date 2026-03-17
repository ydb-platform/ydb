#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

/*

	by Unbit

	syntax:

	route = /^foobar1(.*)/ cache:key=foo$1poo,content_type=text/html,name=foobar

*/

struct uwsgi_router_cache_conf {

	// the name of the cache
	char *name;
	size_t name_len;

	char *key;
	size_t key_len;

	char *var;
	size_t var_len;

	char *value;
	size_t value_len;

	// use mime types ?
	char *mime;

	char *as_num;

#ifdef UWSGI_ZLIB
	char *gzip;
	size_t gzip_len;
#endif

	char *content_type;
	size_t content_type_len;

	char *content_encoding;
	size_t content_encoding_len;

	struct uwsgi_cache *cache;

	char *expires_str;
	uint64_t expires;

	int64_t default_num;

	uint64_t flags;

	char *status_str;
	int status;
	char *no_offload;

	char *no_cl;
};

// this is allocated for each transformation
struct uwsgi_transformation_cache_conf {
	struct uwsgi_buffer *cache_it;
#ifdef UWSGI_ZLIB
        struct uwsgi_buffer *cache_it_gzip;
#endif

	int status;
	struct uwsgi_buffer *value;

        struct uwsgi_buffer *cache_it_to;
        uint64_t cache_it_expires;
};

static int transform_cache(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {
	struct uwsgi_transformation_cache_conf *utcc = (struct uwsgi_transformation_cache_conf *) ut->data;
	struct uwsgi_buffer *ub = ut->chunk;
	// force value
	if (utcc->value) {
		ub = utcc->value;
	}
	// store only successfull response
	if (wsgi_req->write_errors == 0 && (wsgi_req->status == 200 || (utcc->status && wsgi_req->status == utcc->status))  && ub->pos > 0) {
		if (utcc->cache_it) {
			uwsgi_cache_magic_set(utcc->cache_it->buf, utcc->cache_it->pos, ub->buf, ub->pos, utcc->cache_it_expires,
				UWSGI_CACHE_FLAG_UPDATE, utcc->cache_it_to ? utcc->cache_it_to->buf : NULL);
#ifdef UWSGI_ZLIB
			if (utcc->cache_it_gzip) {
				struct uwsgi_buffer *gzipped = uwsgi_gzip(ub->buf, ub->pos);
				if (gzipped) {
					uwsgi_cache_magic_set(utcc->cache_it_gzip->buf, utcc->cache_it_gzip->pos, gzipped->buf, gzipped->pos, utcc->cache_it_expires,
                                		UWSGI_CACHE_FLAG_UPDATE, utcc->cache_it_to ? utcc->cache_it_to->buf : NULL);
					uwsgi_buffer_destroy(gzipped);
				}
			}
#endif
		}
	}

	// free resources
	if (utcc->cache_it) uwsgi_buffer_destroy(utcc->cache_it);
#ifdef UWSGI_ZLIB
	if (utcc->cache_it_gzip) uwsgi_buffer_destroy(utcc->cache_it_gzip);
#endif
	if (utcc->cache_it_to) uwsgi_buffer_destroy(utcc->cache_it_to);
	if (utcc->value) uwsgi_buffer_destroy(utcc->value);
	free(utcc);
        return 0;
}


// be tolerant on errors
static int uwsgi_routing_func_cache_store(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){
	struct uwsgi_router_cache_conf *urcc = (struct uwsgi_router_cache_conf *) ur->data2;

	struct uwsgi_transformation_cache_conf *utcc = uwsgi_calloc(sizeof(struct uwsgi_transformation_cache_conf));

	// build key and name
        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        utcc->cache_it = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->key, urcc->key_len);
        if (!utcc->cache_it) goto error;
	
	if (urcc->name) {
		utcc->cache_it_to = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->name, urcc->name_len);
		if (!utcc->cache_it_to) goto error;
	}

	if (urcc->value) {
		utcc->value = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->value, urcc->value_len);
                if (!utcc->value) goto error;
	}

	utcc->status = urcc->status;

#ifdef UWSGI_ZLIB
	if (urcc->gzip) {
		utcc->cache_it_gzip = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->gzip, urcc->gzip_len);
        	if (!utcc->cache_it_gzip) goto error;
	}
#endif
	utcc->cache_it_expires = urcc->expires;

	uwsgi_add_transformation(wsgi_req, transform_cache, utcc);

	return UWSGI_ROUTE_NEXT;

error:
	if (utcc->cache_it) uwsgi_buffer_destroy(utcc->cache_it);
#ifdef UWSGI_ZLIB
	if (utcc->cache_it_gzip) uwsgi_buffer_destroy(utcc->cache_it_gzip);
#endif
	if (utcc->cache_it_to) uwsgi_buffer_destroy(utcc->cache_it_to);
	free(utcc);
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_routing_func_cache(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

	char *mime_type = NULL;
	size_t mime_type_len = 0;
	struct uwsgi_router_cache_conf *urcc = (struct uwsgi_router_cache_conf *) ur->data2;

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);
	struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->key, urcc->key_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	uint64_t valsize = 0;
	uint64_t expires = 0;
	char *value = uwsgi_cache_magic_get(ub->buf, ub->pos, &valsize, &expires, urcc->name);
	if (urcc->mime && value) {
		mime_type = uwsgi_get_mime_type(ub->buf, ub->pos, &mime_type_len);	
	}
	uwsgi_buffer_destroy(ub);
	if (value) {
		if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) goto error;
		if (mime_type) {
                        uwsgi_response_add_content_type(wsgi_req, mime_type, mime_type_len);
		}
		else {
			if (uwsgi_response_add_content_type(wsgi_req, urcc->content_type, urcc->content_type_len)) goto error;
		}
		if (urcc->content_encoding_len) {
			if (uwsgi_response_add_header(wsgi_req, "Content-Encoding", 16, urcc->content_encoding, urcc->content_encoding_len)) goto error;	
		}
		if (expires) {
			if (uwsgi_response_add_expires(wsgi_req, expires)) goto error;	
		}
		if (!urcc->no_cl) {
			if (uwsgi_response_add_content_length(wsgi_req, valsize)) goto error;
		}
		if (wsgi_req->socket->can_offload && !ur->custom && !urcc->no_offload) {
                	if (!uwsgi_offload_request_memory_do(wsgi_req, value, valsize)) {
                        	wsgi_req->via = UWSGI_VIA_OFFLOAD;
                        	return UWSGI_ROUTE_BREAK;
                	}
		}

		uwsgi_response_write_body_do(wsgi_req, value, valsize);
		free(value);
		if (ur->custom)
			return UWSGI_ROUTE_NEXT;
		return UWSGI_ROUTE_BREAK;
	}
	
	return UWSGI_ROUTE_NEXT;
error:
	free(value);
	return UWSGI_ROUTE_BREAK;
}

// place a cache value in a request var
static int uwsgi_routing_func_cachevar(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_cache_conf *urcc = (struct uwsgi_router_cache_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->key, urcc->key_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        uint64_t valsize = 0;
        char *value = uwsgi_cache_magic_get(ub->buf, ub->pos, &valsize, NULL, urcc->name);
        uwsgi_buffer_destroy(ub);
        if (value) {
		if (urcc->as_num) {
			char *tmp = value;
			if (valsize == 8) {
				int64_t *num = (int64_t *) value;
				value = uwsgi_64bit2str(*num);
			}
			else {
				value = uwsgi_64bit2str(0);
			}
			free(tmp);
		}
		if (!uwsgi_req_append(wsgi_req, urcc->var, urcc->var_len, value, valsize)) {
        		free(value);
			return UWSGI_ROUTE_BREAK;
        	}
		free(value);
        }

        return UWSGI_ROUTE_NEXT;
}

// set a cache item
static int uwsgi_routing_func_cacheset(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_cache_conf *urcc = (struct uwsgi_router_cache_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->key, urcc->key_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        struct uwsgi_buffer *ub_val = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->value, urcc->value_len);
        if (!ub_val) {
		uwsgi_buffer_destroy(ub);
		return UWSGI_ROUTE_BREAK;
	}

        if (uwsgi_cache_magic_set(ub->buf, ub->pos, ub_val->buf, ub_val->pos, urcc->expires, 0, urcc->name)) {
		uwsgi_buffer_destroy(ub);
		uwsgi_buffer_destroy(ub_val);
		return UWSGI_ROUTE_BREAK;
	}
        uwsgi_buffer_destroy(ub);
	uwsgi_buffer_destroy(ub_val);
        return UWSGI_ROUTE_NEXT;
}

// cacheinc/cachedec/cachemul/cachediv
static int uwsgi_routing_func_cachemath(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_cache_conf *urcc = (struct uwsgi_router_cache_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->key, urcc->key_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	int64_t num = urcc->default_num;

	if (urcc->value) {
        	struct uwsgi_buffer *ub_val = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urcc->value, urcc->value_len);
        	if (!ub_val) {
                	uwsgi_buffer_destroy(ub);
                	return UWSGI_ROUTE_BREAK;
        	}
		num = strtol(ub_val->buf, NULL, 10);
                uwsgi_buffer_destroy(ub_val);
	}

        if (uwsgi_cache_magic_set(ub->buf, ub->pos, (char *) &num, 8, urcc->expires, urcc->flags, urcc->name)) {
                uwsgi_buffer_destroy(ub);
                return UWSGI_ROUTE_BREAK;
        }
        uwsgi_buffer_destroy(ub);
        return UWSGI_ROUTE_NEXT;
}



static int uwsgi_router_cache_store(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_cache_store;
	ur->data = args;
	ur->data_len = strlen(args);
	struct uwsgi_router_cache_conf *urcc = uwsgi_calloc(sizeof(struct uwsgi_router_cache_conf));
	if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urcc->key,
#ifdef UWSGI_ZLIB
                        "gzip", &urcc->gzip,
#endif
                        "name", &urcc->name,
                        "value", &urcc->value,
			"status", &urcc->status_str,
			"code", &urcc->status_str,
                        "expires", &urcc->expires_str, NULL)) {
                        uwsgi_log("invalid cachestore route syntax: %s\n", args);
			goto error;
                }

                if (urcc->key) {
                        urcc->key_len = strlen(urcc->key);
                }

#ifdef UWSGI_ZLIB
		if (urcc->gzip) {
			urcc->gzip_len = strlen(urcc->gzip);
		}
#endif

		if (urcc->name) {
                        urcc->name_len = strlen(urcc->name);
		}

                if (!urcc->key) {
                        uwsgi_log("invalid cachestore route syntax: you need to specify a cache key\n");
			goto error;
                }

		if (urcc->expires_str) {
			urcc->expires = strtoul(urcc->expires_str, NULL, 10);
		}

		if (urcc->value) {
			urcc->value_len = strlen(urcc->value);
		}

		if (urcc->status_str) {
                        urcc->status = atoi(urcc->status_str);
                }

	ur->data2 = urcc;
        return 0;
error:
	if (urcc->key) free(urcc->key);
	if (urcc->name) free(urcc->name);
	if (urcc->expires_str) free(urcc->expires_str);
	free(urcc);
	return -1;
}

static int uwsgi_router_cache(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_cache;
        ur->data = args;
        ur->data_len = strlen(args);
	struct uwsgi_router_cache_conf *urcc = uwsgi_calloc(sizeof(struct uwsgi_router_cache_conf));
                if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urcc->key,
                        "content_type", &urcc->content_type,
                        "content_encoding", &urcc->content_encoding,
                        "mime", &urcc->mime,
                        "name", &urcc->name,
                        "no_offload", &urcc->no_offload,
                        "no_content_length", &urcc->no_cl,
                        "no_cl", &urcc->no_cl,
                        "nocl", &urcc->no_cl,
                        NULL)) {
			uwsgi_log("invalid route syntax: %s\n", args);
			exit(1);
                }

		if (urcc->key) {
			urcc->key_len = strlen(urcc->key);
		}

		if (!urcc->key) {
			uwsgi_log("invalid route syntax: you need to specify a cache key\n");
			exit(1);
		}

                if (!urcc->content_type) urcc->content_type = "text/html";

                urcc->content_type_len = strlen(urcc->content_type);

		if (urcc->content_encoding) {
			urcc->content_encoding_len = strlen(urcc->content_encoding);
		}

                ur->data2 = urcc;
	return 0;
}

static int uwsgi_router_cache_continue(struct uwsgi_route *ur, char *args) {
	uwsgi_router_cache(ur, args);
	ur->custom = 1;
	return 0;
}

static struct uwsgi_router_cache_conf *uwsgi_router_cachemath(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_cachemath;
	ur->data = args;
        ur->data_len = strlen(args);

	struct uwsgi_router_cache_conf *urcc = uwsgi_calloc(sizeof(struct uwsgi_router_cache_conf));
	if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urcc->key,
                        "value", &urcc->value,
                        "name", &urcc->name,
                        "expires", &urcc->expires_str, NULL)) {
                        uwsgi_log("invalid cachemath route syntax: %s\n", args);
                        goto error;
                }

                if (urcc->key) {
                        urcc->key_len = strlen(urcc->key);
                }

                if (urcc->value) {
                        urcc->value_len = strlen(urcc->value);
                }

                if (urcc->name) {
                        urcc->name_len = strlen(urcc->name);
                }

                if (!urcc->key) {
                        uwsgi_log("invalid cachemath route syntax: you need to specify a cache key\n");
                        goto error;
                }

                if (urcc->expires_str) {
                        urcc->expires = strtoul(urcc->expires_str, NULL, 10);
                }

	urcc->flags = UWSGI_CACHE_FLAG_UPDATE|UWSGI_CACHE_FLAG_MATH|UWSGI_CACHE_FLAG_FIXEXPIRE;
        ur->data2 = urcc;
        return urcc;
error:
        if (urcc->key) free(urcc->key);
        if (urcc->name) free(urcc->name);
        if (urcc->value) free(urcc->value);
        if (urcc->expires_str) free(urcc->expires_str);
        free(urcc);
        return NULL;	
}

static int uwsgi_router_cacheinc(struct uwsgi_route *ur, char *args) {
	struct uwsgi_router_cache_conf *urcc = uwsgi_router_cachemath(ur, args);
	if (!urcc) return -1;
	urcc->default_num = 1;
	urcc->flags |= UWSGI_CACHE_FLAG_INC;
	return 0;
}

static int uwsgi_router_cachedec(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_cache_conf *urcc = uwsgi_router_cachemath(ur, args);
        if (!urcc) return -1;
        urcc->default_num = 1;
        urcc->flags |= UWSGI_CACHE_FLAG_DEC;
        return 0;
}

static int uwsgi_router_cachemul(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_cache_conf *urcc = uwsgi_router_cachemath(ur, args);
        if (!urcc) return -1;
        urcc->default_num = 2;
        urcc->flags |= UWSGI_CACHE_FLAG_MUL;
        return 0;
}

static int uwsgi_router_cachediv(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_cache_conf *urcc = uwsgi_router_cachemath(ur, args);
        if (!urcc) return -1;
        urcc->default_num = 2;
        urcc->flags |= UWSGI_CACHE_FLAG_DIV;
        return 0;
}

static int uwsgi_router_cacheset(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_cacheset;
        ur->data = args;
        ur->data_len = strlen(args);
        struct uwsgi_router_cache_conf *urcc = uwsgi_calloc(sizeof(struct uwsgi_router_cache_conf));
        if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urcc->key,
                        "value", &urcc->value,
                        "name", &urcc->name,
                        "expires", &urcc->expires_str, NULL)) {
                        uwsgi_log("invalid cacheset route syntax: %s\n", args);
                        goto error;
                }

                if (urcc->key) {
                        urcc->key_len = strlen(urcc->key);
                }

                if (urcc->value) {
                        urcc->value_len = strlen(urcc->value);
                }

                if (urcc->name) {
                        urcc->name_len = strlen(urcc->name);
                }

                if (!urcc->key || !urcc->value) {
                        uwsgi_log("invalid cacheset route syntax: you need to specify a cache key and a value\n");
                        goto error;
                }

                if (urcc->expires_str) {
                        urcc->expires = strtoul(urcc->expires_str, NULL, 10);
                }

        ur->data2 = urcc;
        return 0;
error:
        if (urcc->key) free(urcc->key);
        if (urcc->name) free(urcc->name);
        if (urcc->value) free(urcc->value);
        if (urcc->expires_str) free(urcc->expires_str);
        free(urcc);
        return -1;
}


static int uwsgi_router_cachevar(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_cachevar;
        ur->data = args;
        ur->data_len = strlen(args);
        struct uwsgi_router_cache_conf *urcc = uwsgi_calloc(sizeof(struct uwsgi_router_cache_conf));
                if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "key", &urcc->key,
                        "var", &urcc->var,
                        "name", &urcc->name,
                        "num", &urcc->as_num,
                        "as_num", &urcc->as_num,
                        NULL)) {
                        uwsgi_log("invalid route syntax: %s\n", args);
                        exit(1);
                }

                if (urcc->key) {
                        urcc->key_len = strlen(urcc->key);
                }

                if (urcc->var) {
                        urcc->var_len = strlen(urcc->var);
                }

                if (!urcc->key || !urcc->var) {
                        uwsgi_log("invalid route syntax: you need to specify a cache key and a request var\n");
                        exit(1);
                }

                ur->data2 = urcc;
        return 0;
}

static int uwsgi_route_condition_incache(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	int ret = 0;
	char *key = NULL;
	size_t key_len = 0;
	char *name = NULL;

	if (uwsgi_kvlist_parse(ur->subject_str, ur->subject_str_len, ',', '=',
        	"key", &key,
                "name", &name,
                NULL)) {
		return 0;
	}

	if (!key) goto end;

	key_len = strlen(key);
	struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, NULL, 0, key, key_len);
        if (!ub) goto end;

        ret = uwsgi_cache_magic_exists(ub->buf, ub->pos, name);
	uwsgi_buffer_destroy(ub);

end:
	if (key) free(key);
	if (name) free(name);
	return ret;
}

static void router_cache_register() {
	uwsgi_register_router("cache", uwsgi_router_cache);
	uwsgi_register_router("cache-continue", uwsgi_router_cache_continue);
	uwsgi_register_router("cachevar", uwsgi_router_cachevar);
	uwsgi_register_router("cacheset", uwsgi_router_cacheset);
	uwsgi_register_router("cachestore", uwsgi_router_cache_store);
	uwsgi_register_router("cache-store", uwsgi_router_cache_store);
	uwsgi_register_route_condition("incache", uwsgi_route_condition_incache);

	uwsgi_register_router("cacheinc", uwsgi_router_cacheinc);
	uwsgi_register_router("cachedec", uwsgi_router_cachedec);
	uwsgi_register_router("cachemul", uwsgi_router_cachemul);
	uwsgi_register_router("cachediv", uwsgi_router_cachediv);
}

struct uwsgi_plugin router_cache_plugin = {
	.name = "router_cache",
	.on_load = router_cache_register,
};

#else
struct uwsgi_plugin router_cache_plugin = {
	.name = "router_cache",
};
#endif
