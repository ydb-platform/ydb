#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

// this is like uwsgi_str_num but with security checks
static size_t get_content_length(char *buf, uint16_t size) {
        int i;
        size_t val = 0;
        for (i = 0; i < size; i++) {
                if (buf[i] >= '0' && buf[i] <= '9') {
                        val = (val * 10) + (buf[i] - '0');
                        continue;
                }
                break;
        }

        return val;
}


int uwsgi_read_response(int fd, struct uwsgi_header *uh, int timeout, char **buf) {

	char *ptr = (char *) uh;
	size_t remains = 4;
	int ret = -1;
	int rlen;
	ssize_t len;

	while (remains > 0) {
		rlen = uwsgi_waitfd(fd, timeout);
		if (rlen > 0) {
			len = read(fd, ptr, remains);
			if (len <= 0)
				break;
			remains -= len;
			ptr += len;
			if (remains == 0) {
				ret = uh->modifier2;
				break;
			}
			continue;
		}
		// timed out ?
		else if (ret == 0)
			ret = -2;
		break;
	}

	if (buf && uh->pktsize > 0) {
		if (*buf == NULL)
			*buf = uwsgi_malloc(uh->pktsize);
		remains = uh->pktsize;
		ptr = *buf;
		ret = -1;
		while (remains > 0) {
			rlen = uwsgi_waitfd(fd, timeout);
			if (rlen > 0) {
				len = read(fd, ptr, remains);
				if (len <= 0)
					break;
				remains -= len;
				ptr += len;
				if (remains == 0) {
					ret = uh->modifier2;
					break;
				}
				continue;
			}
			// timed out ?
			else if (ret == 0)
				ret = -2;
			break;
		}
	}

	return ret;
}

int uwsgi_parse_array(char *buffer, uint16_t size, char **argv, uint16_t argvs[], uint8_t * argc) {

	char *ptrbuf, *bufferend;
	uint16_t strsize = 0;

	uint8_t max = *argc;
	*argc = 0;

	ptrbuf = buffer;
	bufferend = ptrbuf + size;

	while (ptrbuf < bufferend && *argc < max) {
		if (ptrbuf + 2 < bufferend) {
			memcpy(&strsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
			strsize = uwsgi_swap16(strsize);
#endif

			ptrbuf += 2;
			/* item cannot be null */
			if (!strsize)
				continue;

			if (ptrbuf + strsize <= bufferend) {
				// item
				argv[*argc] = uwsgi_cheap_string(ptrbuf, strsize);
				argvs[*argc] = strsize;
#ifdef UWSGI_DEBUG
				uwsgi_log("arg %s\n", argv[*argc]);
#endif
				ptrbuf += strsize;
				*argc = *argc + 1;
			}
			else {
				uwsgi_log("invalid uwsgi array. skip this var.\n");
				return -1;
			}
		}
		else {
			uwsgi_log("invalid uwsgi array. skip this request.\n");
			return -1;
		}
	}


	return 0;
}

int uwsgi_simple_parse_vars(struct wsgi_request *wsgi_req, char *ptrbuf, char *bufferend) {

	uint16_t strsize;

	while (ptrbuf < bufferend) {
		if (ptrbuf + 2 < bufferend) {
			memcpy(&strsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
			strsize = uwsgi_swap16(strsize);
#endif
			/* key cannot be null */
			if (!strsize) {
				uwsgi_log("uwsgi key cannot be null. skip this request.\n");
				return -1;
			}

			ptrbuf += 2;
			if (ptrbuf + strsize < bufferend) {
				// var key
				wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptrbuf;
				wsgi_req->hvec[wsgi_req->var_cnt].iov_len = strsize;
				ptrbuf += strsize;
				// value can be null (even at the end) so use <=
				if (ptrbuf + 2 <= bufferend) {
					memcpy(&strsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
					strsize = uwsgi_swap16(strsize);
#endif
					ptrbuf += 2;
					if (ptrbuf + strsize <= bufferend) {

						if (wsgi_req->var_cnt < uwsgi.vec_size - (4 + 1)) {
							wsgi_req->var_cnt++;
						}
						else {
							uwsgi_log("max vec size reached. skip this header.\n");
							return -1;
						}
						// var value
						wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptrbuf;
						wsgi_req->hvec[wsgi_req->var_cnt].iov_len = strsize;

						if (wsgi_req->var_cnt < uwsgi.vec_size - (4 + 1)) {
							wsgi_req->var_cnt++;
						}
						else {
							uwsgi_log("max vec size reached. skip this var.\n");
							return -1;
						}
						ptrbuf += strsize;
					}
					else {
						uwsgi_log("invalid uwsgi request (current strsize: %d). skip.\n", strsize);
						return -1;
					}
				}
				else {
					uwsgi_log("invalid uwsgi request (current strsize: %d). skip.\n", strsize);
					return -1;
				}
			}
		}
	}

	return 0;
}

#define uwsgi_proto_key(x, y) memcmp(x, key, y)

static int uwsgi_proto_check_5(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {

	if (!uwsgi_proto_key("HTTPS", 5)) {
		wsgi_req->https = buf;
		wsgi_req->https_len = len;
		return 0;
	}

	return 0;
}

static int uwsgi_proto_check_9(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {

	if (!uwsgi_proto_key("PATH_INFO", 9)) {
		wsgi_req->path_info = buf;
		wsgi_req->path_info_len = len;
		wsgi_req->path_info_pos = wsgi_req->var_cnt + 1;
#ifdef UWSGI_DEBUG
		uwsgi_debug("PATH_INFO=%.*s\n", wsgi_req->path_info_len, wsgi_req->path_info);
#endif
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_HOST", 9)) {
		wsgi_req->host = buf;
		wsgi_req->host_len = len;
#ifdef UWSGI_DEBUG
		uwsgi_debug("HTTP_HOST=%.*s\n", wsgi_req->host_len, wsgi_req->host);
#endif
		return 0;
	}

	return 0;
}

static void uwsgi_parse_http_range(char *buf, uint16_t len, enum uwsgi_range *parsed, int64_t *from, int64_t *to) {
	*parsed = UWSGI_RANGE_INVALID;
	*from = 0;
	*to = 0;
	uint16_t rlen = 0;
	uint16_t i;
	for(i=0;i<len;i++) {
		if (buf[i] == ',') break;
		rlen++;
	}

	// bytes=X-
	if (rlen < 8) return;
	char *equal = memchr(buf, '=', rlen);
	if (!equal) return;
	if (equal-buf != 5) return;
	if (memcmp(buf, "bytes", 5)) return;
	char *range = equal+1;
	rlen -= 6;
	char *dash = memchr(range, '-', rlen);
	if (!dash) return;
	if (dash != range) {
		*from = uwsgi_str_num(range, dash-range);
		if (dash == range+(rlen-1)) {
			/* RFC7233 prefix range
			 * `bytes=start-` is a same as `byte=start-0x7ffffffffffffff`
			 */
			*to = INT64_MAX;
		} else {
			*to = uwsgi_str_num(dash+1, rlen - ((dash+1)-range));
		}
		if (*to >= *from) {
			*parsed = UWSGI_RANGE_PARSED;
		} else {
			*from = 0;
			*to = 0;
		}
	} else {
		/* RFC7233 suffix-byte-range-spec: `bytes=-500` */
		*from = -(int64_t)uwsgi_str_num(dash+1, rlen - ((dash+1)-range));
		if (*from < 0) {
			*to = INT64_MAX;
			*parsed = UWSGI_RANGE_PARSED;
		}
	}
}

static int uwsgi_proto_check_10(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {

	if (uwsgi.honour_range && !uwsgi_proto_key("HTTP_IF_RANGE", 13)) {
		wsgi_req->if_range = buf;
		wsgi_req->if_range_len = len;
	}

	if (uwsgi.honour_range && !uwsgi_proto_key("HTTP_RANGE", 10)) {
		uwsgi_parse_http_range(buf, len, &wsgi_req->range_parsed,
				&wsgi_req->range_from, &wsgi_req->range_to);
		// set deprecated fields for binary compatibility
		wsgi_req->__range_from = (size_t)wsgi_req->range_from;
		wsgi_req->__range_to = (size_t)wsgi_req->range_to;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_FILE", 10)) {
		wsgi_req->file = buf;
		wsgi_req->file_len = len;
		wsgi_req->dynamic = 1;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_HOME", 10)) {
		wsgi_req->home = buf;
		wsgi_req->home_len = len;
		return 0;
	}

	return 0;
}

static int uwsgi_proto_check_11(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {

	if (!uwsgi_proto_key("SCRIPT_NAME", 11)) {
		wsgi_req->script_name = buf;
		wsgi_req->script_name_len = len;
		wsgi_req->script_name_pos = wsgi_req->var_cnt + 1;
#ifdef UWSGI_DEBUG
		uwsgi_debug("SCRIPT_NAME=%.*s\n", wsgi_req->script_name_len, wsgi_req->script_name);
#endif
		return 0;
	}

	if (!uwsgi_proto_key("REQUEST_URI", 11)) {
		wsgi_req->uri = buf;
		wsgi_req->uri_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("REMOTE_USER", 11)) {
		wsgi_req->remote_user = buf;
		wsgi_req->remote_user_len = len;
		return 0;
	}

	if (wsgi_req->host_len == 0 && !uwsgi_proto_key("SERVER_NAME", 11)) {
		wsgi_req->host = buf;
		wsgi_req->host_len = len;
#ifdef UWSGI_DEBUG
		uwsgi_debug("SERVER_NAME=%.*s\n", wsgi_req->host_len, wsgi_req->host);
#endif
		return 0;
	}

	if (wsgi_req->remote_addr_len == 0 && !uwsgi_proto_key("REMOTE_ADDR", 11)) {
		wsgi_req->remote_addr = buf;
                wsgi_req->remote_addr_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_COOKIE", 11)) {
		wsgi_req->cookie = buf;
		wsgi_req->cookie_len = len;
		return 0;
	}


	if (!uwsgi_proto_key("UWSGI_APPID", 11)) {
		wsgi_req->appid = buf;
		wsgi_req->appid_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_CHDIR", 11)) {
		wsgi_req->chdir = buf;
		wsgi_req->chdir_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_ORIGIN", 11)) {
                wsgi_req->http_origin = buf;
                wsgi_req->http_origin_len = len;
                return 0;
        }

	return 0;
}

static int uwsgi_proto_check_12(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("QUERY_STRING", 12)) {
		wsgi_req->query_string = buf;
		wsgi_req->query_string_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("CONTENT_TYPE", 12)) {
		wsgi_req->content_type = buf;
		wsgi_req->content_type_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_REFERER", 12)) {
                wsgi_req->referer = buf;
                wsgi_req->referer_len = len;
                return 0;
        }

	if (!uwsgi_proto_key("UWSGI_SCHEME", 12)) {
		wsgi_req->scheme = buf;
		wsgi_req->scheme_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_SCRIPT", 12)) {
		wsgi_req->script = buf;
		wsgi_req->script_len = len;
		wsgi_req->dynamic = 1;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_MODULE", 12)) {
		wsgi_req->module = buf;
		wsgi_req->module_len = len;
		wsgi_req->dynamic = 1;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_PYHOME", 12)) {
		wsgi_req->home = buf;
		wsgi_req->home_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_SETENV", 12)) {
		char *env_value = memchr(buf, '=', len);
		if (env_value) {
			env_value[0] = 0;
			env_value = uwsgi_concat2n(env_value + 1, len - ((env_value + 1) - buf), "", 0);
			if (setenv(buf, env_value, 1)) {
				uwsgi_error("setenv()");
			}
			free(env_value);
		}
		return 0;
	}
	return 0;
}

static int uwsgi_proto_check_13(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("DOCUMENT_ROOT", 13)) {
		wsgi_req->document_root = buf;
		wsgi_req->document_root_len = len;
		return 0;
	}
	return 0;
}

static int uwsgi_proto_check_14(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("REQUEST_METHOD", 14)) {
		wsgi_req->method = buf;
		wsgi_req->method_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("CONTENT_LENGTH", 14)) {
		wsgi_req->post_cl = get_content_length(buf, len);
		if (uwsgi.limit_post) {
			if (wsgi_req->post_cl > uwsgi.limit_post) {
				uwsgi_log("Invalid (too big) CONTENT_LENGTH. skip.\n");
				return -1;
			}
		}
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_POSTFILE", 14)) {
		char *postfile = uwsgi_concat2n(buf, len, "", 0);
		wsgi_req->post_file = fopen(postfile, "r");
		if (!wsgi_req->post_file) {
			uwsgi_error_open(postfile);
		}
		free(postfile);
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_CALLABLE", 14)) {
		wsgi_req->callable = buf;
		wsgi_req->callable_len = len;
		wsgi_req->dynamic = 1;
		return 0;
	}

	return 0;
}


static int uwsgi_proto_check_15(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("SERVER_PROTOCOL", 15)) {
		wsgi_req->protocol = buf;
		wsgi_req->protocol_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_USER_AGENT", 15)) {
		wsgi_req->user_agent = buf;
		wsgi_req->user_agent_len = len;
		return 0;
	}

	if (uwsgi.caches && !uwsgi_proto_key("UWSGI_CACHE_GET", 15)) {
		wsgi_req->cache_get = buf;
		wsgi_req->cache_get_len = len;
		return 0;
	}

	return 0;
}

static int uwsgi_proto_check_18(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("HTTP_AUTHORIZATION", 18)) {
		wsgi_req->authorization = buf;
		wsgi_req->authorization_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("UWSGI_TOUCH_RELOAD", 18)) {
		wsgi_req->touch_reload = buf;
		wsgi_req->touch_reload_len = len;
		return 0;
	}

	return 0;
}


static int uwsgi_proto_check_20(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (uwsgi.logging_options.log_x_forwarded_for && !uwsgi_proto_key("HTTP_X_FORWARDED_FOR", 20)) {
		wsgi_req->remote_addr = buf;
		wsgi_req->remote_addr_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_X_FORWARDED_SSL", 20)) {
		wsgi_req->https = buf;
                wsgi_req->https_len = len;
	}

	if (!uwsgi_proto_key("HTTP_ACCEPT_ENCODING", 20)) {
		wsgi_req->encoding = buf;
		wsgi_req->encoding_len = len;
		return 0;
	}

	return 0;
}

static int uwsgi_proto_check_22(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {
	if (!uwsgi_proto_key("HTTP_IF_MODIFIED_SINCE", 22)) {
		wsgi_req->if_modified_since = buf;
		wsgi_req->if_modified_since_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_SEC_WEBSOCKET_KEY", 22)) {
		wsgi_req->http_sec_websocket_key = buf;
		wsgi_req->http_sec_websocket_key_len = len;
		return 0;
	}

	if (!uwsgi_proto_key("HTTP_X_FORWARDED_PROTO", 22)) {
                wsgi_req->scheme = buf;
                wsgi_req->scheme_len = len;
        }

	return 0;
}

static int uwsgi_proto_check_27(struct wsgi_request *wsgi_req, char *key, char *buf, uint16_t len) {

        if (!uwsgi_proto_key("HTTP_SEC_WEBSOCKET_PROTOCOL", 27)) {
                wsgi_req->http_sec_websocket_protocol = buf;
                wsgi_req->http_sec_websocket_protocol_len = len;
                return 0;
        }

        return 0;
}


void uwsgi_proto_hooks_setup() {
	int i = 0;
	for(i=0;i<UWSGI_PROTO_MAX_CHECK;i++) {
		uwsgi.proto_hooks[i] = NULL;
	}

	uwsgi.proto_hooks[5] = uwsgi_proto_check_5;
	uwsgi.proto_hooks[9] = uwsgi_proto_check_9;
	uwsgi.proto_hooks[10] = uwsgi_proto_check_10;
	uwsgi.proto_hooks[11] = uwsgi_proto_check_11;
	uwsgi.proto_hooks[12] = uwsgi_proto_check_12;
	uwsgi.proto_hooks[13] = uwsgi_proto_check_13;
	uwsgi.proto_hooks[14] = uwsgi_proto_check_14;
	uwsgi.proto_hooks[15] = uwsgi_proto_check_15;
	uwsgi.proto_hooks[18] = uwsgi_proto_check_18;
	uwsgi.proto_hooks[20] = uwsgi_proto_check_20;
	uwsgi.proto_hooks[22] = uwsgi_proto_check_22;
	uwsgi.proto_hooks[27] = uwsgi_proto_check_27;
}


int uwsgi_parse_vars(struct wsgi_request *wsgi_req) {

	char *buffer = wsgi_req->buffer;

	char *ptrbuf, *bufferend;

	uint16_t strsize = 0;
	struct uwsgi_dyn_dict *udd;

	ptrbuf = buffer;
	bufferend = ptrbuf + wsgi_req->uh->pktsize;
	int i;

	/* set an HTTP 500 status as default */
	wsgi_req->status = 500;

	// skip if already parsed
	if (wsgi_req->parsed)
		return 0;

	// has the protocol already parsed the request ?
	if (wsgi_req->uri_len > 0) {
		wsgi_req->parsed = 1;
		i = uwsgi_simple_parse_vars(wsgi_req, ptrbuf, bufferend);
		if (i == 0)
			goto next;
		return i;
	}

	wsgi_req->parsed = 1;
	wsgi_req->script_name_pos = -1;
	wsgi_req->path_info_pos = -1;

	while (ptrbuf < bufferend) {
		if (ptrbuf + 2 < bufferend) {
			memcpy(&strsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
			strsize = uwsgi_swap16(strsize);
#endif
			/* key cannot be null */
			if (!strsize) {
				uwsgi_log("uwsgi key cannot be null. skip this var.\n");
				return -1;
			}

			ptrbuf += 2;
			if (ptrbuf + strsize < bufferend) {
				// var key
				wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptrbuf;
				wsgi_req->hvec[wsgi_req->var_cnt].iov_len = strsize;
				ptrbuf += strsize;
				// value can be null (even at the end) so use <=
				if (ptrbuf + 2 <= bufferend) {
					memcpy(&strsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
					strsize = uwsgi_swap16(strsize);
#endif
					ptrbuf += 2;
					if (ptrbuf + strsize <= bufferend) {
						if (wsgi_req->hvec[wsgi_req->var_cnt].iov_len > UWSGI_PROTO_MIN_CHECK &&
							wsgi_req->hvec[wsgi_req->var_cnt].iov_len < UWSGI_PROTO_MAX_CHECK &&
								uwsgi.proto_hooks[wsgi_req->hvec[wsgi_req->var_cnt].iov_len]) {
							if (uwsgi.proto_hooks[wsgi_req->hvec[wsgi_req->var_cnt].iov_len](wsgi_req, wsgi_req->hvec[wsgi_req->var_cnt].iov_base, ptrbuf, strsize)) {
								return -1;
							}
						}
						//uwsgi_log("uwsgi %.*s = %.*s\n", wsgi_req->hvec[wsgi_req->var_cnt].iov_len, wsgi_req->hvec[wsgi_req->var_cnt].iov_base, strsize, ptrbuf);

						if (wsgi_req->var_cnt < uwsgi.vec_size - (4 + 1)) {
							wsgi_req->var_cnt++;
						}
						else {
							uwsgi_log("max vec size reached. skip this var.\n");
							return -1;
						}
						// var value
						wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptrbuf;
						wsgi_req->hvec[wsgi_req->var_cnt].iov_len = strsize;
						//uwsgi_log("%.*s = %.*s\n", wsgi_req->hvec[wsgi_req->var_cnt-1].iov_len, wsgi_req->hvec[wsgi_req->var_cnt-1].iov_base, wsgi_req->hvec[wsgi_req->var_cnt].iov_len, wsgi_req->hvec[wsgi_req->var_cnt].iov_base);
						if (wsgi_req->var_cnt < uwsgi.vec_size - (4 + 1)) {
							wsgi_req->var_cnt++;
						}
						else {
							uwsgi_log("max vec size reached. skip this var.\n");
							return -1;
						}
						ptrbuf += strsize;
					}
					else {
						uwsgi_log("invalid uwsgi request (current strsize: %d). skip.\n", strsize);
						return -1;
					}
				}
				else {
					uwsgi_log("invalid uwsgi request (current strsize: %d). skip.\n", strsize);
					return -1;
				}
			}
		}
		else {
			uwsgi_log("invalid uwsgi request (current strsize: %d). skip.\n", strsize);
			return -1;
		}
	}

next:

	// manage post buffering (if needed as post_file could be created before)
	if (uwsgi.post_buffering > 0 && !wsgi_req->post_file) {
		// read to disk if post_cl > post_buffering (it will eventually do upload progress...)
		if (wsgi_req->post_cl >= uwsgi.post_buffering) {
			if (uwsgi_postbuffer_do_in_disk(wsgi_req)) {
				return -1;
			}
		}
		// on tiny post use memory
		else {
			if (uwsgi_postbuffer_do_in_mem(wsgi_req)) {
				return -1;
			}
		}
	}


	// check if data are available in the local cache
	if (wsgi_req->cache_get_len > 0) {
		uint64_t cache_value_size;
		char *cache_value = uwsgi_cache_magic_get(wsgi_req->cache_get, wsgi_req->cache_get_len, &cache_value_size, NULL, NULL);
		if (cache_value && cache_value_size > 0) {
			uwsgi_response_write_body_do(wsgi_req, cache_value, cache_value_size);
			free(cache_value);
			return -1;
		}
	}

	if (uwsgi.check_cache && wsgi_req->uri_len && wsgi_req->method_len == 3 && wsgi_req->method[0] == 'G' && wsgi_req->method[1] == 'E' && wsgi_req->method[2] == 'T') {

		uint64_t cache_value_size;
		char *cache_value = uwsgi_cache_magic_get(wsgi_req->uri, wsgi_req->uri_len, &cache_value_size, NULL, NULL);
		if (cache_value && cache_value_size > 0) {
			uwsgi_response_write_body_do(wsgi_req, cache_value, cache_value_size);
			free(cache_value);
			return -1;
		}
	}

	if (uwsgi.manage_script_name) {
		if (uwsgi_apps_cnt > 0 && wsgi_req->path_info_len > 1 && wsgi_req->path_info_pos != -1) {
			// starts with 1 as the 0 app is the default (/) one
			int best_found = 0;
			char *orig_path_info = wsgi_req->path_info;
			int orig_path_info_len = wsgi_req->path_info_len;
			// if SCRIPT_NAME is not allocated, add a slot for it
			if (wsgi_req->script_name_pos == -1) {
				if (wsgi_req->var_cnt >= uwsgi.vec_size - (4 + 2)) {
					uwsgi_log("max vec size reached. skip this var.\n");
					return -1;
				}
				wsgi_req->hvec[wsgi_req->var_cnt].iov_base = "SCRIPT_NAME";
				wsgi_req->hvec[wsgi_req->var_cnt].iov_len = 11;
				wsgi_req->var_cnt++;
				wsgi_req->script_name_pos = wsgi_req->var_cnt;
				wsgi_req->hvec[wsgi_req->script_name_pos].iov_base = "";
				wsgi_req->hvec[wsgi_req->script_name_pos].iov_len = 0;
				wsgi_req->var_cnt++;
			}

			for (i = 0; i < uwsgi_apps_cnt; i++) {
				char* mountpoint = uwsgi_apps[i].mountpoint;
				int mountpoint_len = uwsgi_apps[i].mountpoint_len;

				// Ignore trailing mountpoint slashes
				if (mountpoint_len > 0 && mountpoint[mountpoint_len - 1] == '/') {
					mountpoint_len -= 1;
				}

				//uwsgi_log("app mountpoint = %.*s\n", uwsgi_apps[i].mountpoint_len, uwsgi_apps[i].mountpoint);

				// Check if mountpoint could be a possible candidate
				if (orig_path_info_len < mountpoint_len || // it should be shorter than or equal to path_info
					mountpoint_len <= best_found || // it should be better than the previous found
					// should have the same prefix of path_info
					uwsgi_startswith(orig_path_info, mountpoint, mountpoint_len) ||
					// and should not be "misleading"
					(orig_path_info_len > mountpoint_len && orig_path_info[mountpoint_len] != '/' )) {
					continue;
				}

				best_found = mountpoint_len;
				wsgi_req->script_name = uwsgi_apps[i].mountpoint;
				wsgi_req->script_name_len = uwsgi_apps[i].mountpoint_len;
				wsgi_req->path_info = orig_path_info + wsgi_req->script_name_len;
				wsgi_req->path_info_len = orig_path_info_len - wsgi_req->script_name_len;

				wsgi_req->hvec[wsgi_req->script_name_pos].iov_base = wsgi_req->script_name;
				wsgi_req->hvec[wsgi_req->script_name_pos].iov_len = wsgi_req->script_name_len;

				wsgi_req->hvec[wsgi_req->path_info_pos].iov_base = wsgi_req->path_info;
				wsgi_req->hvec[wsgi_req->path_info_pos].iov_len = wsgi_req->path_info_len;
#ifdef UWSGI_DEBUG
				uwsgi_log("managed SCRIPT_NAME = %.*s PATH_INFO = %.*s\n", wsgi_req->script_name_len, wsgi_req->script_name, wsgi_req->path_info_len, wsgi_req->path_info);
#endif
			}
		}
	}


	/* CHECK FOR STATIC FILES */

	// skip extensions
	struct uwsgi_string_list *sse = uwsgi.static_skip_ext;
	while (sse) {
		if (wsgi_req->path_info_len >= sse->len) {
			if (!uwsgi_strncmp(wsgi_req->path_info + (wsgi_req->path_info_len - sse->len), sse->len, sse->value, sse->len)) {
				return 0;
			}
		}
		sse = sse->next;
	}

	// check if a file named uwsgi.check_static+env['PATH_INFO'] exists
	udd = uwsgi.check_static;
	while (udd) {
		// need to build the path ?
		if (udd->value == NULL) {
			if (uwsgi.threads > 1)
				pthread_mutex_lock(&uwsgi.lock_static);
			udd->value = uwsgi_malloc(PATH_MAX + 1);
			if (!realpath(udd->key, udd->value)) {
				free(udd->value);
				udd->value = NULL;
			}
			if (uwsgi.threads > 1)
				pthread_mutex_unlock(&uwsgi.lock_static);
			if (!udd->value)
				goto nextcs;
			udd->vallen = strlen(udd->value);
		}

		if (!uwsgi_file_serve(wsgi_req, udd->value, udd->vallen, wsgi_req->path_info, wsgi_req->path_info_len, 0)) {
			return -1;
		}
nextcs:
		udd = udd->next;
	}

	// check static-map
	udd = uwsgi.static_maps;
	while (udd) {
#ifdef UWSGI_DEBUG
		uwsgi_log("checking for %.*s <-> %.*s %.*s\n", (int)wsgi_req->path_info_len, wsgi_req->path_info, (int)udd->keylen, udd->key, (int) udd->vallen, udd->value);
#endif
		if (udd->status == 0) {
			if (uwsgi.threads > 1)
				pthread_mutex_lock(&uwsgi.lock_static);
			char *real_docroot = uwsgi_malloc(PATH_MAX + 1);
			if (!realpath(udd->value, real_docroot)) {
				free(real_docroot);
				real_docroot = NULL;
				udd->value = NULL;
			}
			if (uwsgi.threads > 1)
				pthread_mutex_unlock(&uwsgi.lock_static);
			if (!real_docroot)
				goto nextsm;
			udd->value = real_docroot;
			udd->vallen = strlen(udd->value);
			udd->status = 1 + uwsgi_is_file(real_docroot);
		}

		if (!uwsgi_starts_with(wsgi_req->path_info, wsgi_req->path_info_len, udd->key, udd->keylen)) {
			if (!uwsgi_file_serve(wsgi_req, udd->value, udd->vallen, wsgi_req->path_info + udd->keylen, wsgi_req->path_info_len - udd->keylen, udd->status - 1)) {
				return -1;
			}
		}
nextsm:
		udd = udd->next;
	}

	// check for static_maps in append mode
	udd = uwsgi.static_maps2;
	while (udd) {
#ifdef UWSGI_DEBUG
		uwsgi_log("checking for %.*s <-> %.*s\n", wsgi_req->path_info_len, wsgi_req->path_info, udd->keylen, udd->key);
#endif
		if (udd->status == 0) {
			if (uwsgi.threads > 1)
				pthread_mutex_lock(&uwsgi.lock_static);
			char *real_docroot = uwsgi_malloc(PATH_MAX + 1);
			if (!realpath(udd->value, real_docroot)) {
				free(real_docroot);
				real_docroot = NULL;
				udd->value = NULL;
			}
			if (uwsgi.threads > 1)
				pthread_mutex_unlock(&uwsgi.lock_static);
			if (!real_docroot)
				goto nextsm2;
			udd->value = real_docroot;
			udd->vallen = strlen(udd->value);
			udd->status = 1 + uwsgi_is_file(real_docroot);
		}

		if (!uwsgi_starts_with(wsgi_req->path_info, wsgi_req->path_info_len, udd->key, udd->keylen)) {
			if (!uwsgi_file_serve(wsgi_req, udd->value, udd->vallen, wsgi_req->path_info, wsgi_req->path_info_len, udd->status - 1)) {
				return -1;
			}
		}
nextsm2:
		udd = udd->next;
	}


	// finally check for docroot
	if (uwsgi.check_static_docroot && wsgi_req->document_root_len > 0) {
		char *real_docroot = uwsgi_expand_path(wsgi_req->document_root, wsgi_req->document_root_len, NULL);
		if (!real_docroot) {
			return -1;
		}
		if (!uwsgi_file_serve(wsgi_req, real_docroot, strlen(real_docroot), wsgi_req->path_info, wsgi_req->path_info_len, 0)) {
			free(real_docroot);
			return -1;
		}
		free(real_docroot);
	}

	return 0;
}

int uwsgi_hooked_parse(char *buffer, size_t len, void (*hook) (char *, uint16_t, char *, uint16_t, void *), void *data) {

	char *ptrbuf, *bufferend;
	uint16_t keysize = 0, valsize = 0;
	char *key;

	ptrbuf = buffer;
	bufferend = buffer + len;

	while (ptrbuf < bufferend) {
		if (ptrbuf + 2 >= bufferend)
			return -1;
		memcpy(&keysize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
		keysize = uwsgi_swap16(keysize);
#endif
		/* key cannot be null */
		if (!keysize)
			return -1;

		ptrbuf += 2;
		if (ptrbuf + keysize > bufferend)
			return -1;

		// key
		key = ptrbuf;
		ptrbuf += keysize;
		// value can be null
		if (ptrbuf + 2 > bufferend)
			return -1;

		memcpy(&valsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
		valsize = uwsgi_swap16(valsize);
#endif
		ptrbuf += 2;
		if (ptrbuf + valsize > bufferend)
			return -1;

		// now call the hook
		hook(key, keysize, ptrbuf, valsize, data);
		ptrbuf += valsize;
	}

	return 0;

}

int uwsgi_hooked_parse_array(char *buffer, size_t len, void (*hook) (uint16_t, char *, uint16_t, void *), void *data) {

        char *ptrbuf, *bufferend;
        uint16_t valsize = 0;
        char *value;
	uint16_t pos = 0;

        ptrbuf = buffer;
        bufferend = buffer + len;

        while (ptrbuf < bufferend) {
                if (ptrbuf + 2 > bufferend)
                        return -1;
                memcpy(&valsize, ptrbuf, 2);
#ifdef __BIG_ENDIAN__
                valsize = uwsgi_swap16(valsize);
#endif
                ptrbuf += 2;
                if (ptrbuf + valsize > bufferend)
                        return -1;

                // key
                value = ptrbuf;
                // now call the hook
                hook(pos, value, valsize, data);
                ptrbuf += valsize;
		pos++;
        }

        return 0;

}


// this functions transform a raw HTTP response to a uWSGI-managed response
int uwsgi_blob_to_response(struct wsgi_request *wsgi_req, char *body, size_t len) {
	char *line = body;
	size_t line_len = 0;
	size_t i;
	int status_managed = 0;
	for(i=0;i<len;i++) {
		if (body[i] == '\n') {
			// invalid line
			if (line_len < 1) {
				return -1;
			}
			if (line[line_len-1] != '\r') {
				return -1;
			}
			// end of the headers
			if (line_len == 1) {
				break;
			}

			if (status_managed) {
				char *colon = memchr(line, ':', line_len-1);
				if (!colon) return -1;
				if (colon[1] != ' ') return -1;
				if (uwsgi_response_add_header(wsgi_req, line, colon-line, colon+2, (line_len-1) - ((colon+2)-line))) return -1;
			}
			else {
				char *space = memchr(line, ' ', line_len-1);
				if (!space) return -1;
				if ((line_len-1) - ((space+1)-line) < 3) return -1;
				if (uwsgi_response_prepare_headers(wsgi_req, space+1, (line_len-1) - ((space+1)-line))) return -1;
				status_managed = 1;
			}
			line = NULL;
			line_len = 0;
		}
		else {
			if (!line) {
				line = body + i;
			}
			line_len++;
		}
	}

	if ((i+1) < len) {
		if (uwsgi_response_write_body_do(wsgi_req, body + (i + 1), len-(i+1))) {
			return -1;
		}
	}

	return 0;
}

/*

the following functions need to take in account that POST data could be already available in wsgi_req->buffer (generally when uwsgi protocol is in use)

In such a case, allocate a proto_parser_buf and move data there

*/

char *uwsgi_req_append(struct wsgi_request *wsgi_req, char *key, uint16_t keylen, char *val, uint16_t vallen) {

	if (!wsgi_req->proto_parser_buf) {
		if (wsgi_req->proto_parser_remains > 0) {
			wsgi_req->proto_parser_buf = uwsgi_malloc(wsgi_req->proto_parser_remains);
			memcpy(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_remains_buf, wsgi_req->proto_parser_remains);
			wsgi_req->proto_parser_remains_buf = wsgi_req->proto_parser_buf;
		}
	}

	if ((wsgi_req->uh->pktsize + (2 + keylen + 2 + vallen)) > uwsgi.buffer_size) {
		uwsgi_log("not enough buffer space to add %.*s variable, consider increasing it with the --buffer-size option\n", keylen, key);
		return NULL;
	}

	if (wsgi_req->var_cnt >= uwsgi.vec_size - (4 + 2)) {
        	uwsgi_log("max vec size reached. skip this header.\n");
		return NULL;
	}

	char *ptr = wsgi_req->buffer + wsgi_req->uh->pktsize;

	*ptr++ = (uint8_t) (keylen & 0xff);
	*ptr++ = (uint8_t) ((keylen >> 8) & 0xff);

	memcpy(ptr, key, keylen);
	wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptr;
        wsgi_req->hvec[wsgi_req->var_cnt].iov_len = keylen;
	wsgi_req->var_cnt++;
	ptr += keylen;



	*ptr++ = (uint8_t) (vallen & 0xff);
	*ptr++ = (uint8_t) ((vallen >> 8) & 0xff);

	memcpy(ptr, val, vallen);
	wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptr;
        wsgi_req->hvec[wsgi_req->var_cnt].iov_len = vallen;
	wsgi_req->var_cnt++;

	wsgi_req->uh->pktsize += (2 + keylen + 2 + vallen);

	return ptr;
}

int uwsgi_req_append_path_info_with_index(struct wsgi_request *wsgi_req, char *index, uint16_t index_len) {

	if (!wsgi_req->proto_parser_buf) {
                if (wsgi_req->proto_parser_remains > 0) {
                        wsgi_req->proto_parser_buf = uwsgi_malloc(wsgi_req->proto_parser_remains);
                        memcpy(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_remains_buf, wsgi_req->proto_parser_remains);
                        wsgi_req->proto_parser_remains_buf = wsgi_req->proto_parser_buf;
                }
        }

	uint8_t need_slash = 0;
	if (wsgi_req->path_info_len > 0) {
		if (wsgi_req->path_info[wsgi_req->path_info_len-1] != '/') {
			need_slash = 1;
		}
	}

	wsgi_req->path_info_len += need_slash + index_len;

	// 2 + 9 + 2
	if ((wsgi_req->uh->pktsize + (13 + wsgi_req->path_info_len)) > uwsgi.buffer_size) {
                uwsgi_log("not enough buffer space to transform the PATH_INFO variable, consider increasing it with the --buffer-size option\n");
                return -1;
        }

	if (wsgi_req->var_cnt >= uwsgi.vec_size - (4 + 2)) {
                uwsgi_log("max vec size reached for PATH_INFO + index. skip this request.\n");
                return -1;
        }

	uint16_t keylen = 9;
	char *ptr = wsgi_req->buffer + wsgi_req->uh->pktsize;
	*ptr++ = (uint8_t) (keylen & 0xff);
        *ptr++ = (uint8_t) ((keylen >> 8) & 0xff);

	memcpy(ptr, "PATH_INFO", keylen);
	wsgi_req->hvec[wsgi_req->var_cnt].iov_base = ptr;
	wsgi_req->hvec[wsgi_req->var_cnt].iov_len = keylen;
	wsgi_req->var_cnt++;
        ptr += keylen;

	*ptr++ = (uint8_t) (wsgi_req->path_info_len & 0xff);
        *ptr++ = (uint8_t) ((wsgi_req->path_info_len >> 8) & 0xff);

	char *new_path_info = ptr;

	memcpy(ptr, wsgi_req->path_info, wsgi_req->path_info_len - (need_slash + index_len));
	ptr+=wsgi_req->path_info_len - (need_slash + index_len);
	if (need_slash) {
		*ptr ++= '/';
	}
	memcpy(ptr, index, index_len);

	wsgi_req->hvec[wsgi_req->var_cnt].iov_base = new_path_info;
        wsgi_req->hvec[wsgi_req->var_cnt].iov_len = wsgi_req->path_info_len;
        wsgi_req->var_cnt++;

	wsgi_req->uh->pktsize += 13 + wsgi_req->path_info_len;
	wsgi_req->path_info = new_path_info;

	return 0;
}
