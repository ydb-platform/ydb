#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

/*

	Plugin for remote access to the uWSGI cache

	For request generating a response containing cache data, we need to make
	a copy to avoid holding the lock too much in case of blocking/slow writes

	we use 1.9 cache magic functions to be DRY

	the modifier2 is the command to run. Some command is extremely raw, and generally it is mean
	for internal uWSGI usage.

	commands:

		0 -> simple cache get for values not bigger than 64k, the request is [111, keysize, 0] + key / response is [111, vallen, 0] + value

		1 -> simple cache set for values not bigger than 64k, the request is [111, pktsize, 1] + (key, value) / response is connection closed

		2 -> simple cache del, the request is [111, keysize, 2] + key / response is connection closed

		3/4 -> simple dict based get command -> [111, pktsize ,3/4] + (key|get) / response is value for "key and get", but if "get" and no key is found an HTTP 404 is returned

		5 -> get and stream -> [111, keysize, 5] + key response is a stream of the value til the whole object is transferred

		6 -> dump the whole cache

		17 -> magic interface for plugins remote access { "cmd": "get|set|update|del|exists", "key": "cache key", "expires": "seconds", "cache": "the cache name"}
			returns: {"status":"ok|notfound|error", "size": "size of the following body, if present"} + stream

*/

extern struct uwsgi_server uwsgi;

static void cache_simple_command(char *key, uint16_t keylen, char *val, uint16_t vallen, void *data) {

        struct wsgi_request *wsgi_req = (struct wsgi_request *) data;

        if (vallen > 0) {
                if (!uwsgi_strncmp(key, keylen, "key", 3)) {
        		uint64_t vallen = 0;
                        char *value = uwsgi_cache_magic_get(val, vallen, &vallen, NULL, NULL);
                        if (value) {
				uwsgi_response_write_body_do(wsgi_req, value, vallen);
				free(value);
                        }

                }
                else if (!uwsgi_strncmp(key, keylen, "get", 3)) {
        		uint64_t vallen = 0;
                        char *value = uwsgi_cache_magic_get(val, vallen, &vallen, NULL, NULL);
                        if (value) {
				uwsgi_response_write_body_do(wsgi_req, value, vallen);
				free(value);
                        }
                        else {
				uwsgi_404(wsgi_req);
                        }
                }
        }
}

// this function does not use the magic api internally to avoid too much copy
static void manage_magic_context(struct wsgi_request *wsgi_req, struct uwsgi_cache_magic_context *ucmc) {

	struct uwsgi_buffer *ub = NULL;
	struct uwsgi_cache *uc = uwsgi.caches;

	if (ucmc->cache_len > 0) {
		uc = uwsgi_cache_by_namelen(ucmc->cache, ucmc->cache_len);
		if (!uc) return;
	}

	if (!uc) return;

	// cache get
	if (!uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "get", 3)) {
		uint64_t vallen = 0;
		uint64_t expires = 0;
		uwsgi_rlock(uc->lock);
		char *value = uwsgi_cache_get3(uc, ucmc->key, ucmc->key_len, &vallen, &expires);
		if (!value) {
			uwsgi_rwunlock(uc->lock);
			return;
		}
		// we are still locked !!!
		ub = uwsgi_buffer_new(uwsgi.page_size);
		ub->pos = 4;
		if (uwsgi_buffer_append_keyval(ub, "status", 6, "ok", 2)) goto error;
		if (uwsgi_buffer_append_keynum(ub, "size", 4, vallen)) goto error;
		if (expires) {
			if (uwsgi_buffer_append_keynum(ub, "expires", 7, expires)) goto error;
		}
		if (uwsgi_buffer_set_uh(ub, 111, 17)) goto error;
		if (uwsgi_buffer_append(ub, value, vallen)) goto error;
		// unlock !!!
		uwsgi_rwunlock(uc->lock);
		uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos);
		uwsgi_buffer_destroy(ub);
		return;	
	}

	// cache exists
	if (!uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "exists", 6)) {
                uwsgi_rlock(uc->lock);
                if (!uwsgi_cache_exists2(uc, ucmc->key, ucmc->key_len)) {
                        uwsgi_rwunlock(uc->lock);
                        return;
                }
                // we are still locked !!!
                ub = uwsgi_buffer_new(uwsgi.page_size);
                ub->pos = 4;
                if (uwsgi_buffer_append_keyval(ub, "status", 6, "ok", 2)) goto error;
                if (uwsgi_buffer_set_uh(ub, 111, 17)) goto error;
                // unlock !!!
                uwsgi_rwunlock(uc->lock);
                uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos);
                uwsgi_buffer_destroy(ub);
                return;
        }

	// cache del
        if (!uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "del", 3)) {
                uwsgi_wlock(uc->lock);
                if (uwsgi_cache_del2(uc, ucmc->key, ucmc->key_len, 0, 0)) {
                        uwsgi_rwunlock(uc->lock);
                        return;
                }
                // we are still locked !!!
                ub = uwsgi_buffer_new(uwsgi.page_size);
                ub->pos = 4;
                if (uwsgi_buffer_append_keyval(ub, "status", 6, "ok", 2)) goto error;
                if (uwsgi_buffer_set_uh(ub, 111, 17)) goto error;
                // unlock !!!
                uwsgi_rwunlock(uc->lock);
                uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos);
                uwsgi_buffer_destroy(ub);
                return;
        }

	// cache clear
        if (!uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "clear", 5)) {
		uint64_t i;
		uwsgi_wlock(uc->lock);
		for (i = 1; i < uwsgi.caches->max_items; i++) {
			if (uwsgi_cache_del2(uc, NULL, 0, i, 0)) {
                                uwsgi_rwunlock(uc->lock);
                                return;
                        }	
		}
                // we are still locked !!!
                ub = uwsgi_buffer_new(uwsgi.page_size);
                ub->pos = 4;
                if (uwsgi_buffer_append_keyval(ub, "status", 6, "ok", 2)) goto error;
                if (uwsgi_buffer_set_uh(ub, 111, 17)) goto error;
                // unlock !!!
                uwsgi_rwunlock(uc->lock);
                uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos);
                uwsgi_buffer_destroy(ub);
                return;
        }

	// cache set
	if (!uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "set", 3) || !uwsgi_strncmp(ucmc->cmd, ucmc->cmd_len, "update", 6)) {
		if (ucmc->size == 0 || ucmc->size > uc->max_item_size) return;
		wsgi_req->post_cl = ucmc->size;
		// read the value
		ssize_t rlen = 0;
		char *value = uwsgi_request_body_read(wsgi_req, ucmc->size, &rlen);
		if (rlen != (ssize_t) ucmc->size) return;
		// ok let's lock
		uwsgi_wlock(uc->lock);
		if (uwsgi_cache_set2(uc, ucmc->key, ucmc->key_len, value, ucmc->size, ucmc->expires, ucmc->cmd_len > 3 ? UWSGI_CACHE_FLAG_UPDATE : 0)) {
			uwsgi_rwunlock(uc->lock);
			return;
		}
		// we are still locked !!!
		ub = uwsgi_buffer_new(uwsgi.page_size);
		ub->pos = 4;
                if (uwsgi_buffer_append_keyval(ub, "status", 6, "ok", 2)) goto error;
		if (uwsgi_buffer_set_uh(ub, 111, 17)) goto error;
		// unlock !!!
		uwsgi_rwunlock(uc->lock);
		uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos);
                uwsgi_buffer_destroy(ub);
                return;
	}

	return;
error:
	uwsgi_rwunlock(uc->lock);
	uwsgi_buffer_destroy(ub);
}

static int uwsgi_cache_request(struct wsgi_request *wsgi_req) {

        uint64_t vallen = 0;
        char *value;
        char *argv[3];
        uint16_t argvs[3];
        uint8_t argc = 0;

	// used for modifier2 17
	struct uwsgi_cache_magic_context ucmc;
	struct uwsgi_cache *uc = NULL;

        switch(wsgi_req->uh->modifier2) {
                case 0:
                        // get
                        if (wsgi_req->uh->pktsize > 0) {
                                value = uwsgi_cache_magic_get(wsgi_req->buffer, wsgi_req->uh->pktsize, &vallen, NULL, NULL);
                                if (value) {
                                        wsgi_req->uh->pktsize = vallen;
					if (uwsgi_response_write_body_do(wsgi_req, (char *)&wsgi_req->uh, 4)) { free(value) ; return -1;}
					uwsgi_response_write_body_do(wsgi_req, value, vallen);
					free(value);
                                }
                        }
                        break;
                case 1:
                        // set
                        if (wsgi_req->uh->pktsize > 0) {
				// max 3 items
                                argc = 3;
                                if (!uwsgi_parse_array(wsgi_req->buffer, wsgi_req->uh->pktsize, argv, argvs, &argc)) {
                                        if (argc > 1) {
						uwsgi_cache_magic_set(argv[0], argvs[0], argv[1], argvs[1], 0, 0, NULL);
                                        }
                                }
                        }
                        break;
                case 2:
                        // del
                        if (wsgi_req->uh->pktsize > 0) {
                                uwsgi_cache_magic_del(wsgi_req->buffer, wsgi_req->uh->pktsize, NULL);
                        }
                        break;
                case 3:
                case 4:
                        // dict
                        if (wsgi_req->uh->pktsize > 0) {
                                uwsgi_hooked_parse(wsgi_req->buffer, wsgi_req->uh->pktsize, cache_simple_command, (void *) wsgi_req);
                        }
                        break;
                case 5:
                        // get (uwsgi + stream)
                        if (wsgi_req->uh->pktsize > 0) {
                                value = uwsgi_cache_magic_get(wsgi_req->buffer, wsgi_req->uh->pktsize, &vallen, NULL, NULL);
                                if (value) {
                                        wsgi_req->uh->pktsize = 0;
                                        wsgi_req->uh->modifier2 = 1;
					if (uwsgi_response_write_body_do(wsgi_req, (char *)&wsgi_req->uh, 4)) { free(value) ;return -1;}
					uwsgi_response_write_body_do(wsgi_req, value, vallen);
					free(value);
                                }
                                else {
                                        wsgi_req->uh->pktsize = 0;
                                        wsgi_req->uh->modifier2 = 0;
					uwsgi_response_write_body_do(wsgi_req, (char *)&wsgi_req->uh, 4);
					free(value);
                                }
                        }
                        break;
		case 6:
			// dump
			uc = uwsgi.caches;
			if (wsgi_req->uh->pktsize > 0) {
				uc = uwsgi_cache_by_namelen(wsgi_req->buffer, wsgi_req->uh->pktsize);
			}

			if (!uc) break;

			uwsgi_wlock(uc->lock);
			struct uwsgi_buffer *cache_dump = uwsgi_buffer_new(uwsgi.page_size + uc->filesize);
			cache_dump->pos = 4;
			if (uwsgi_buffer_append_keynum(cache_dump, "items", 5, uc->max_items)) {
				uwsgi_buffer_destroy(cache_dump);
				break;
			}
			if (uwsgi_buffer_append_keynum(cache_dump, "blocksize", 9, uc->blocksize)) {
				uwsgi_buffer_destroy(cache_dump);
				break;
			}

			if (uwsgi_buffer_set_uh(cache_dump, 111, 7)) {
				uwsgi_buffer_destroy(cache_dump);
				break;
			}

			if (uwsgi_buffer_append(cache_dump, (char *)uc->items, uc->filesize)) {
				uwsgi_buffer_destroy(cache_dump);
				break;
			}

			uwsgi_rwunlock(uc->lock);

			uwsgi_response_write_body_do(wsgi_req, cache_dump->buf, cache_dump->pos);
			uwsgi_buffer_destroy(cache_dump);
			break;
		case 17:
			if (wsgi_req->uh->pktsize == 0) break;
			memset(&ucmc, 0, sizeof(struct uwsgi_cache_magic_context));
			if (uwsgi_hooked_parse(wsgi_req->buffer, wsgi_req->uh->pktsize, uwsgi_cache_magic_context_hook, &ucmc)) {
				break;
			}
			manage_magic_context(wsgi_req, &ucmc);
			break;
		default:
			break;
        }

        return UWSGI_OK;
}

struct uwsgi_plugin cache_plugin = {

        .name = "cache",
        .modifier1 = 111,
        .request = uwsgi_cache_request,

};

