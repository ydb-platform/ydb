#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI HTTP router

*/

#include "common.h"

struct uwsgi_http uhttp;

struct uwsgi_option http_options[] = {
	{"http", required_argument, 0, "add an http router/server on the specified address", uwsgi_opt_corerouter, &uhttp, 0},
	{"httprouter", required_argument, 0, "add an http router/server on the specified address", uwsgi_opt_corerouter, &uhttp, 0},
#ifdef UWSGI_SSL
	{"https", required_argument, 0, "add an https router/server on the specified address with specified certificate and key", uwsgi_opt_https, &uhttp, 0},
	{"https2", required_argument, 0, "add an https/spdy router/server using keyval options", uwsgi_opt_https2, &uhttp, 0},
	{"https-export-cert", no_argument, 0, "export uwsgi variable HTTPS_CC containing the raw client certificate", uwsgi_opt_true, &uhttp.https_export_cert, 0},
	{"https-session-context", required_argument, 0, "set the session id context to the specified value", uwsgi_opt_set_str, &uhttp.https_session_context, 0},
	{"http-to-https", required_argument, 0, "add an http router/server on the specified address and redirect all of the requests to https", uwsgi_opt_http_to_https, &uhttp, 0},
#endif
	{"http-processes", required_argument, 0, "set the number of http processes to spawn", uwsgi_opt_set_int, &uhttp.cr.processes, 0},
	{"http-workers", required_argument, 0, "set the number of http processes to spawn", uwsgi_opt_set_int, &uhttp.cr.processes, 0},
	{"http-var", required_argument, 0, "add a key=value item to the generated uwsgi packet", uwsgi_opt_add_string_list, &uhttp.http_vars, 0},
	{"http-to", required_argument, 0, "forward requests to the specified node (you can specify it multiple time for lb)", uwsgi_opt_add_string_list, &uhttp.cr.static_nodes, 0 },
	{"http-zerg", required_argument, 0, "attach the http router to a zerg server", uwsgi_opt_corerouter_zerg, &uhttp, 0 },
	{"http-fallback", required_argument, 0, "fallback to the specified node in case of error", uwsgi_opt_add_string_list, &uhttp.cr.fallback, 0},
	{"http-modifier1", required_argument, 0, "set uwsgi protocol modifier1", uwsgi_opt_set_int, &uhttp.modifier1, 0},
	{"http-modifier2", required_argument, 0, "set uwsgi protocol modifier2", uwsgi_opt_set_int, &uhttp.modifier2, 0},
	{"http-use-cache", optional_argument, 0, "use uWSGI cache as key->value virtualhost mapper", uwsgi_opt_set_str, &uhttp.cr.use_cache, 0},
	{"http-use-pattern", required_argument, 0, "use the specified pattern for mapping requests to unix sockets", uwsgi_opt_corerouter_use_pattern, &uhttp, 0},
	{"http-use-base", required_argument, 0, "use the specified base for mapping requests to unix sockets", uwsgi_opt_corerouter_use_base, &uhttp, 0},
	{"http-events", required_argument, 0, "set the number of concurrent http async events", uwsgi_opt_set_int, &uhttp.cr.nevents, 0},
	{"http-subscription-server", required_argument, 0, "enable the subscription server", uwsgi_opt_corerouter_ss, &uhttp, 0},
	{"http-subscription-fallback-key", required_argument, 0, "key to use for fallback http handler", uwsgi_opt_corerouter_fallback_key, &uhttp.cr, 0},
	{"http-timeout", required_argument, 0, "set internal http socket timeout (default: 60 seconds)", uwsgi_opt_set_int, &uhttp.cr.socket_timeout, 0},
	{"http-manage-expect", optional_argument, 0, "manage the Expect HTTP request header (optionally checking for Content-Length)", uwsgi_opt_set_64bit, &uhttp.manage_expect, 0},
	{"http-keepalive", optional_argument, 0, "HTTP 1.1 keepalive support (non-pipelined) requests", uwsgi_opt_set_int, &uhttp.keepalive, 0},
	{"http-auto-chunked", no_argument, 0, "automatically transform output to chunked encoding during HTTP 1.1 keepalive (if needed)", uwsgi_opt_true, &uhttp.auto_chunked, 0},
#ifdef UWSGI_ZLIB
	{"http-auto-gzip", no_argument, 0, "automatically gzip content if uWSGI-Encoding header is set to gzip, but content size (Content-Length/Transfer-Encoding) and Content-Encoding are not specified", uwsgi_opt_true, &uhttp.auto_gzip, 0},
#endif

	{"http-raw-body", no_argument, 0, "blindly send HTTP body to backends (required for WebSockets and Icecast support in backends)", uwsgi_opt_true, &uhttp.raw_body, 0},
	{"http-websockets", no_argument, 0, "automatically detect websockets connections and put the session in raw mode", uwsgi_opt_true, &uhttp.websockets, 0},

	{"http-chunked-input", no_argument, 0, "automatically detect chunked input requests and put the session in raw mode", uwsgi_opt_true, &uhttp.chunked_input, 0},

	{"http-use-code-string", required_argument, 0, "use code string as hostname->server mapper for the http router", uwsgi_opt_corerouter_cs, &uhttp, 0},
        {"http-use-socket", optional_argument, 0, "forward request to the specified uwsgi socket", uwsgi_opt_corerouter_use_socket, &uhttp, 0},
        {"http-gracetime", required_argument, 0, "retry connections to dead static nodes after the specified amount of seconds", uwsgi_opt_set_int, &uhttp.cr.static_node_gracetime, 0},

	{"http-quiet", required_argument, 0, "do not report failed connections to instances", uwsgi_opt_true, &uhttp.cr.quiet, 0},
        {"http-cheap", no_argument, 0, "run the http router in cheap mode", uwsgi_opt_true, &uhttp.cr.cheap, 0},

	{"http-stats", required_argument, 0, "run the http router stats server", uwsgi_opt_set_str, &uhttp.cr.stats_server, 0},
	{"http-stats-server", required_argument, 0, "run the http router stats server", uwsgi_opt_set_str, &uhttp.cr.stats_server, 0},
	{"http-ss", required_argument, 0, "run the http router stats server", uwsgi_opt_set_str, &uhttp.cr.stats_server, 0},
	{"http-harakiri", required_argument, 0, "enable http router harakiri", uwsgi_opt_set_int, &uhttp.cr.harakiri, 0},
	{"http-stud-prefix", required_argument, 0, "expect a stud prefix (1byte family + 4/16 bytes address) on connections from the specified address", uwsgi_opt_add_addr_list, &uhttp.stud_prefix, 0},
	{"http-uid", required_argument, 0, "drop http router privileges to the specified uid", uwsgi_opt_uid, &uhttp.cr.uid, 0 },
	{"http-gid", required_argument, 0, "drop http router privileges to the specified gid", uwsgi_opt_gid, &uhttp.cr.gid, 0 },
	{"http-resubscribe", required_argument, 0, "forward subscriptions to the specified subscription server", uwsgi_opt_add_string_list, &uhttp.cr.resubscribe, 0},
	{"http-buffer-size", required_argument, 0, "set internal buffer size (default: page size)", uwsgi_opt_set_64bit, &uhttp.cr.buffer_size, 0},

	{"http-server-name-as-http-host", required_argument, 0, "force SERVER_NAME to HTTP_HOST", uwsgi_opt_true, &uhttp.server_name_as_http_host, 0},
	{"http-headers-timeout", required_argument, 0, "set internal http socket timeout for headers", uwsgi_opt_set_int, &uhttp.headers_timeout, 0},
	{"http-connect-timeout", required_argument, 0, "set internal http socket timeout for backend connections", uwsgi_opt_set_int, &uhttp.connect_timeout, 0},

	{"http-manage-source", no_argument, 0, "manage the SOURCE HTTP method placing the session in raw mode", uwsgi_opt_true, &uhttp.manage_source, 0},
	{"http-manage-rtsp", no_argument, 0, "manage RTSP sessions", uwsgi_opt_true, &uhttp.manage_rtsp, 0},
	{"http-enable-proxy-protocol", optional_argument, 0, "manage PROXY protocol requests", uwsgi_opt_true, &uhttp.enable_proxy_protocol, 0},
	{0, 0, 0, 0, 0, 0, 0},
};

static void http_set_timeout(struct corerouter_peer *peer, int timeout) {
	if (peer->current_timeout == timeout) return;
	peer->current_timeout = timeout;
	peer->timeout = corerouter_reset_timeout(peer->session->corerouter, peer);
}

static char * http_header_to_cgi(char *hh, size_t hhlen, size_t *keylen, size_t *vallen, int *has_prefix) {
	size_t i;
	char *val = hh;
	int status = 0;
	for (i = 0; i < hhlen; i++) {
                if (!status) {
                        hh[i] = toupper((int) hh[i]);
                        if (hh[i] == '-')
                                hh[i] = '_';
                        if (hh[i] == ':') {
                                status = 1;
                                *keylen = i;
                        }
                }
                else if (status == 1 && hh[i] != ' ') {
                        status = 2;
                        val += i;
                        *vallen+=1;
                }
                else if (status == 2) {
                        *vallen+=1;
                }
        }

	if (!(*keylen))
                return NULL;

	if (uwsgi_strncmp("CONTENT_LENGTH", 14, hh, *keylen) && uwsgi_strncmp("CONTENT_TYPE", 12, hh, *keylen)) {
		*has_prefix = 0x02;
        }

	return val;
}

static int http_add_uwsgi_header(struct corerouter_peer *peer, char *hh, size_t keylen, char *val, size_t vallen, int prefix) {

	struct uwsgi_buffer *out = peer->out;
	struct http_session *hr = (struct http_session *) peer->session;

	if (hr->websockets) {
		if (!uwsgi_strncmp("UPGRADE", 7, hh, keylen)) {
			if (!uwsgi_strnicmp(val, vallen, "websocket", 9)) {
				hr->websockets++;
			}
			goto done;
		}
		else if (!uwsgi_strncmp("CONNECTION", 10, hh, keylen)) {
			if (!uwsgi_strnicmp(val, vallen, "Upgrade", 7)) {
				hr->websockets++;
			}
			goto done;
		}
		else if (!uwsgi_strncmp("SEC_WEBSOCKET_VERSION", 21, hh, keylen)) {
			hr->websockets++;
			goto done;
		}
		else if (!uwsgi_strncmp("SEC_WEBSOCKET_KEY", 17, hh, keylen)) {
			hr->websocket_key = val;
			hr->websocket_key_len = vallen;
			goto done;
		}
	}	

	if (!uwsgi_strncmp("HOST", 4, hh, keylen)) {
		if (vallen <= 0xff) {
			memcpy(peer->key, val, vallen);
			peer->key_len = vallen;
			if (uhttp.server_name_as_http_host && uwsgi_buffer_append_keyval(out, "SERVER_NAME", 11, peer->key, peer->key_len)) return -1;
		}
		else {
			return -1;
		}
	}

	else if (!uwsgi_strncmp("CONTENT_LENGTH", 14, hh, keylen)) {
		hr->content_length = uwsgi_str_num(val, vallen);
	}

	// in the future we could support chunked requests...
	else if (!uwsgi_strncmp("TRANSFER_ENCODING", 17, hh, keylen)) {
		hr->session.can_keepalive = 0;
		if (uhttp.chunked_input) {
			if (!uwsgi_strnicmp(val, vallen, "chunked", 7)) {
				hr->raw_body = 1;
			} 
		}
	}

	else if (!uwsgi_strncmp("CONNECTION", 10, hh, keylen)) {
		if (!uwsgi_strnicmp(val, vallen, "close", 5) || !uwsgi_strnicmp(val, vallen, "upgrade", 7)) {
			hr->session.can_keepalive = 0;
		}
	}
	else if (peer->key == uwsgi.hostname && hr->raw_body && !uwsgi_strncmp("ICE_URL", 7, hh, keylen)) {
		if (vallen <= 0xff) {
                        memcpy(peer->key, val, vallen);
                        peer->key_len = vallen;
		}
	}

#ifdef UWSGI_ZLIB
	else if (uhttp.auto_gzip && !uwsgi_strncmp("ACCEPT_ENCODING", 15, hh, keylen)) {
		if ( uwsgi_contains_n(val, vallen, "gzip", 4) ) {
			hr->can_gzip = 1;
		}
	}
#endif

done:
	if (prefix) keylen += 5;

	if (uwsgi_buffer_u16le(out, keylen)) return -1;

	if (prefix) {
		if (uwsgi_buffer_append(out, "HTTP_", 5)) return -1;
	}

	if (uwsgi_buffer_append(out, hh, keylen - (prefix ? 5 : 0))) return -1;

	if (uwsgi_buffer_u16le(out, vallen)) return -1;
	if (uwsgi_buffer_append(out, val, vallen)) return -1;

	return 0;
}


int http_headers_parse(struct corerouter_peer *peer) {

	struct http_session *hr = (struct http_session *) peer->session;

	char *ptr = peer->session->main_peer->in->buf;
	char *watermark = ptr + hr->headers_size;
	char *base = ptr;
	char *query_string = NULL;
	char *proxy_src = NULL;
	char *proxy_dst = NULL;
	char *proxy_src_port = NULL;
	char *proxy_dst_port = NULL;
	uint16_t proxy_src_len = 0;
	uint16_t proxy_dst_len = 0;
	uint16_t proxy_src_port_len = 0;
	uint16_t proxy_dst_port_len = 0;

	peer->out = uwsgi_buffer_new(uwsgi.page_size);
	// force this buffer to be destroyed as soon as possibile
	peer->out_need_free = 1;
	peer->out->limit = UMAX16;
	// leave space for the uwsgi header
	peer->out->pos = 4;
	peer->out_pos = 0;

	struct uwsgi_buffer *out = peer->out;
	int found = 0;

        if (uwsgi.enable_proxy_protocol || uhttp.enable_proxy_protocol) {
		ptr = proxy1_parse(ptr, watermark, &proxy_src, &proxy_src_len, &proxy_dst, &proxy_dst_len, &proxy_src_port, &proxy_src_port_len, &proxy_dst_port, &proxy_dst_port_len);
		base = ptr;
        }

	// REQUEST_METHOD 
	while (ptr < watermark) {
		if (*ptr == ' ') {
			if (uwsgi_buffer_append_keyval(out, "REQUEST_METHOD", 14, base, ptr - base)) return -1;
			// on SOURCE METHOD, force raw body
			if (uhttp.manage_source && !uwsgi_strncmp(base, ptr - base, "SOURCE", 6)) {
				hr->raw_body = 1;
			}
			ptr++;
			found = 1;
			break;
		}
		else if (*ptr == '\r' || *ptr == '\n') break;
		ptr++;
	}

        // ensure we have a method
        if (!found) {
          return -1;
        }

	// REQUEST_URI / PATH_INFO / QUERY_STRING
	base = ptr;
	found = 0;
	while (ptr < watermark) {
		if (*ptr == '?' && !query_string) {
			// PATH_INFO must be url-decoded !!!
			if (!hr->path_info) {
				hr->path_info_len = ptr - base;
				hr->path_info = uwsgi_malloc(hr->path_info_len);
			}
			else {
				size_t new_path_info = ptr - base;
				if (new_path_info > hr->path_info_len) {
					char *tmp_buf = realloc(hr->path_info, new_path_info);
					if (!tmp_buf) return -1;
					hr->path_info = tmp_buf;
				}
				hr->path_info_len = new_path_info;
			}
			http_url_decode(base, &hr->path_info_len, hr->path_info);
			if (uwsgi_buffer_append_keyval(out, "PATH_INFO", 9, hr->path_info, hr->path_info_len)) return -1;
			query_string = ptr + 1;
		}
		else if (*ptr == ' ') {
			hr->request_uri = base;
			hr->request_uri_len = ptr - base;
			if (uwsgi_buffer_append_keyval(out, "REQUEST_URI", 11, base, ptr - base)) return -1;
			if (!query_string) {
				// PATH_INFO must be url-decoded !!!
				if (!hr->path_info) {
                                	hr->path_info_len = ptr - base;
                                	hr->path_info = uwsgi_malloc(hr->path_info_len);
                        	}
                        	else {
                                	size_t new_path_info = ptr - base;
                                	if (new_path_info > hr->path_info_len) {
                                        	char *tmp_buf = realloc(hr->path_info, new_path_info);
                                        	if (!tmp_buf) return -1;
                                        	hr->path_info = tmp_buf;
                                	}
                                	hr->path_info_len = new_path_info;
                        	}
				http_url_decode(base, &hr->path_info_len, hr->path_info);
				if (uwsgi_buffer_append_keyval(out, "PATH_INFO", 9, hr->path_info, hr->path_info_len)) return -1;
				if (uwsgi_buffer_append_keyval(out, "QUERY_STRING", 12, "", 0)) return -1;
			}
			else {
				if (uwsgi_buffer_append_keyval(out, "QUERY_STRING", 12, query_string, ptr - query_string)) return -1;
			}
			ptr++;
			found = 1;
			break;
		}
		ptr++;
	}

        // ensure we have a URI
        if (!found) {
          return -1;
        }

	// SERVER_PROTOCOL
	base = ptr;
	found = 0;
	while (ptr < watermark) {
		if (*ptr == '\r') {
			if (ptr + 1 >= watermark)
				return 0;
			if (*(ptr + 1) != '\n')
				return 0;
			if (uwsgi_buffer_append_keyval(out, "SERVER_PROTOCOL", 15, base, ptr - base)) return -1;
			if (uhttp.keepalive && !uwsgi_strncmp("HTTP/1.1", 8, base, ptr-base)) {
				hr->session.can_keepalive = 1;
			}
			if (uhttp.manage_rtsp && !uwsgi_strncmp("RTSP/1.0", 8, base, ptr-base)) {
				hr->raw_body = 1;
				hr->is_rtsp = 1;
			}
			ptr += 2;
			found = 1;
			break;
		}
		ptr++;
	}

        // ensure we have a protocol
        if (!found) {
          return -1;
        }

	// SCRIPT_NAME
	if (uwsgi_buffer_append_keyval(out, "SCRIPT_NAME", 11, "", 0)) return -1;

	// SERVER_NAME
	if (!uhttp.server_name_as_http_host && uwsgi_buffer_append_keyval(out, "SERVER_NAME", 11, uwsgi.hostname, uwsgi.hostname_len)) return -1;
	memcpy(peer->key, uwsgi.hostname, uwsgi.hostname_len);
	peer->key_len = uwsgi.hostname_len;

	// SERVER_PORT
	if (uwsgi_buffer_append_keyval(out, "SERVER_PORT", 11, hr->port, hr->port_len)) return -1;

	// UWSGI_ROUTER
	if (uwsgi_buffer_append_keyval(out, "UWSGI_ROUTER", 12, "http", 4)) return -1;

	// stud HTTPS
	if (hr->stud_prefix_pos > 0) {
		if (uwsgi_buffer_append_keyval(out, "HTTPS", 5, "on", 2)) return -1;
	}

#ifdef UWSGI_SSL
	if (hr_https_add_vars(hr, peer, out)) return -1;
#endif

	// REMOTE_ADDR
        if (proxy_src) {
		if (uwsgi_buffer_append_keyval(out, "REMOTE_ADDR", 11, proxy_src, proxy_src_len)) return -1;
		if (proxy_src_port) {
			if (uwsgi_buffer_append_keyval(out, "REMOTE_PORT", 11, proxy_src_port, proxy_src_port_len)) return -1;
		}
	}
	else
	{
		if (uwsgi_buffer_append_keyval(out, "REMOTE_ADDR", 11, peer->session->client_address, strlen(peer->session->client_address))) return -1;
		if (uwsgi_buffer_append_keyval(out, "REMOTE_PORT", 11, peer->session->client_port, strlen(peer->session->client_port))) return -1;
	}

	//HEADERS
	base = ptr;

	struct uwsgi_string_list *headers = NULL, *usl = NULL;

	while (ptr < watermark) {
		if (*ptr == '\r') {
			if (ptr + 1 >= watermark)
				break;
			if (*(ptr + 1) != '\n')
				break;
			// multiline header ?
			if (ptr + 2 < watermark) {
				if (*(ptr + 2) == ' ' || *(ptr + 2) == '\t') {
					ptr += 2;
					continue;
				}
			}

			// this is an hack with dumb/wrong/useless error checking
			if (uhttp.manage_expect) {
				if (!uwsgi_strncmp("Expect: 100-continue", 20, base, ptr - base)) {
					hr->send_expect_100 = 1;
				}
			}

			size_t key_len = 0, value_len = 0;
			int has_prefix = 0;
			// last line, do not waste time
			if (ptr - base == 0) break;
			char *value = http_header_to_cgi(base, ptr - base, &key_len, &value_len, &has_prefix);
			if (!value) goto clear;
			usl = uwsgi_string_list_has_item(headers, base, key_len);
			// there is already a HTTP header with the same name, let's merge them
			if (usl) {	
				char *old_value = usl->custom_ptr;
				usl->custom_ptr = uwsgi_concat3n(old_value, (size_t) usl->custom, ", ", 2, value, value_len);
				usl->custom += 2 + value_len;
				if (usl->custom2 & 0x01) free(old_value);
				usl->custom2 |= 0x01;
			}
			else {
			// add an entry
				usl = uwsgi_string_new_list(&headers, NULL);
				usl->value = base;
				usl->len = key_len;
				usl->custom_ptr = value;
				usl->custom = value_len;
				usl->custom2 = has_prefix;
			}	
			ptr++;
			base = ptr + 1;
		}
		ptr++;
	}

	usl = headers;
	int broken = 0;
	while(usl) {
		if (!broken) {
			if (http_add_uwsgi_header(peer, usl->value, usl->len, usl->custom_ptr, (size_t) usl->custom, usl->custom2 & 0x02)) broken = 1;
		}
		if (usl->custom2 & 0x01) {
			free(usl->custom_ptr);
		}
		struct uwsgi_string_list *tmp_usl = usl;
		usl = usl->next;
		free(tmp_usl);
	}	

	if (broken) return -1;

	struct uwsgi_string_list *hv = uhttp.http_vars;
	while (hv) {
		char *equal = strchr(hv->value, '=');
		if (equal) {
			if (uwsgi_buffer_append_keyval(out, hv->value, equal - hv->value, equal + 1, strlen(equal + 1))) return -1;
		}
		hv = hv->next;
	}

	if (hr->is_rtsp) {
		if (uwsgi_starts_with("rtsp://", 7, hr->path_info, hr->path_info_len)) {
			char *slash = memchr(hr->path_info + 7, '/', hr->path_info_len - 7);
			if (!slash) return -1;
			if (slash - (hr->path_info + 7) <= 0xff) {
				peer->key_len = slash - (hr->path_info + 7);
				memcpy(peer->key, hr->path_info + 7, peer->key_len);
				// override PATH_INFO
				if (uwsgi_buffer_append_keyval(out, "PATH_INFO", 9, slash, hr->path_info_len - (7 + peer->key_len))) return -1;
			}
		}
	}

	return 0;

clear:
	usl = headers;
        while(usl) {
		if (usl->custom2 & 0x01) {
			free(usl->custom_ptr);
		}
		struct uwsgi_string_list *tmp_usl = usl;
                usl = usl->next;
                free(tmp_usl);
	}
	return -1;

}


int hr_manage_expect_continue(struct corerouter_peer *peer) {
	struct corerouter_session *cs = peer->session;
	struct http_session *hr = (struct http_session *) cs;

	if (uhttp.manage_expect > 1) {
		if (hr->content_length > uhttp.manage_expect) {
			if (uwsgi_buffer_append(peer->in, "HTTP/1.1 413 Request Entity Too Large\r\n\r\n", 41)) return -1;
			hr->session.wait_full_write = 1;
			goto ready;	
		}
	}

	if (uwsgi_buffer_append(peer->in, "HTTP/1.1 100 Continue\r\n\r\n", 25)) return -1;
	hr->session.connect_peer_after_write = peer;

ready:
	peer->session->main_peer->out = peer->in;
        peer->session->main_peer->out_pos = 0;
	cr_write_to_main(peer, hr->func_write);
	return 0;
}


ssize_t hr_instance_write(struct corerouter_peer *peer) {
	ssize_t len = cr_write(peer, "hr_instance_write()");
        // end on empty write
        if (!len) { peer->session->can_keepalive = 0; return 0; }

        // the chunk has been sent, start (again) reading from client and instances
        if (cr_write_complete(peer)) {
		// destroy the buffer used for the uwsgi packet
		if (peer->out_need_free == 1) {
			uwsgi_buffer_destroy(peer->out);
			peer->out_need_free = 0;
			peer->out = NULL;
			// reset the main_peer input stream
			peer->session->main_peer->in->pos = 0;
		}
		// reset the stream (main_peer->in = peer->out)
		else {
			peer->out->pos = 0;
		}
                cr_reset_hooks(peer);
#ifdef UWSGI_SPDY
		struct http_session *hr = (struct http_session *) peer->session;
		if (hr->spdy) {
			if (hr->spdy_update_window) {
				if (uwsgi_buffer_fix(peer->in, 16)) return -1;
				peer->in->pos = 16;
				spdy_window_update(peer->in->buf, hr->spdy_update_window, 8192);
				peer->session->main_peer->out = peer->in;
                        	peer->session->main_peer->out_pos = 0;
				hr->spdy_update_window = 0;
                        	cr_write_to_main(peer, hr->func_write);	
				return 1;
			}
			return spdy_parse(peer->session->main_peer);
		}
#endif
		
        }

        return len;
}

// write to the client
ssize_t hr_write(struct corerouter_peer *main_peer) {
        ssize_t len = cr_write(main_peer, "hr_write()");
        // end on empty write
        if (!len) return 0;

        // ok this response chunk is sent, let's start reading again
        if (cr_write_complete(main_peer)) {
                // reset the original read buffer
                main_peer->out->pos = 0;
		if (main_peer->session->wait_full_write) {
			main_peer->session->wait_full_write = 0;
			return 0;
		}
		if (main_peer->session->connect_peer_after_write) {
			http_set_timeout(main_peer->session->connect_peer_after_write, uhttp.connect_timeout);
			cr_connect(main_peer->session->connect_peer_after_write, hr_instance_connected);
			main_peer->session->connect_peer_after_write = NULL;
			return len;
		}
                cr_reset_hooks(main_peer);
        }

        return len;
}

ssize_t hr_instance_connected(struct corerouter_peer* peer) {

	cr_peer_connected(peer, "hr_instance_connected()");
	
	// set the default timeout
	http_set_timeout(peer, uhttp.cr.socket_timeout);

	// we are connected, we cannot retry anymore
        peer->can_retry = 0;

	// prepare for write
	peer->out_pos = 0;

	// change the write hook (we are already monitoring for write)
	peer->hook_write = hr_instance_write;
	// and directly call it (optimistic approach...)
        return hr_instance_write(peer);
}

// check if the response allows for keepalive
int hr_check_response_keepalive(struct corerouter_peer *peer) {
	struct http_session *hr = (struct http_session *) peer->session;
	struct uwsgi_buffer *ub = peer->in;
	size_t i;
	for(i=0;i<ub->pos;i++) {
                char c = ub->buf[i];
                if (c == '\r' && (peer->r_parser_status == 0 || peer->r_parser_status == 2)) {
                        peer->r_parser_status++;
                }
                else if (c == '\r') {
                        peer->r_parser_status = 1;
                }
                else if (c == '\n' && peer->r_parser_status == 1) {
                        peer->r_parser_status = 2;
                }
                // parsing done
                else if (c == '\n' && peer->r_parser_status == 3) {
			// end of headers
			peer->r_parser_status = 4;
			if (http_response_parse(hr, ub, i+1)) {
				return -1;
			}
			return 0;
		}
                else {
                        peer->r_parser_status = 0;
                }
        }

	return 1;

}

// data from instance
ssize_t hr_instance_read(struct corerouter_peer *peer) {
        peer->in->limit = UMAX16;
	if (uwsgi_buffer_ensure(peer->in, uwsgi.page_size)) return -1;
	struct http_session *hr = (struct http_session *) peer->session;
        ssize_t len = cr_read(peer, "hr_instance_read()");
        if (!len) {
		// disable keepalive on unread body
		if (hr->content_length) hr->session.can_keepalive = 0;
		if (hr->session.can_keepalive) {
			peer->session->main_peer->disabled = 0;
			hr->rnrn = 0;
#ifdef UWSGI_ZLIB
			hr->can_gzip = 0;
			hr->has_gzip = 0;
#endif
			if (uhttp.keepalive > 1) {
				http_set_timeout(peer->session->main_peer, uhttp.keepalive);
			}
		}
#ifdef UWSGI_ZLIB
		if (hr->force_chunked || hr->force_gzip) {
#else
		if (hr->force_chunked) {
#endif
			hr->force_chunked = 0;
			if (!hr->last_chunked) {
				hr->last_chunked = uwsgi_buffer_new(5);
			}
#ifdef UWSGI_ZLIB
			if (hr->force_gzip) {
				hr->force_gzip = 0;
				size_t zlen = 0;
				char *gzipped = uwsgi_deflate(&hr->z, NULL, 0, &zlen);
				if (!gzipped) return -1;
				if (uwsgi_buffer_append_chunked(hr->last_chunked, zlen)) {free(gzipped) ; return -1;}
				if (uwsgi_buffer_append(hr->last_chunked, gzipped, zlen)) {free(gzipped) ; return -1;}
				free(gzipped);
				if (uwsgi_buffer_append(hr->last_chunked, "\r\n", 2)) return -1;
				if (uwsgi_buffer_append_chunked(hr->last_chunked, 8)) return -1;
				if (uwsgi_buffer_u32le(hr->last_chunked, hr->gzip_crc32)) return -1;
				if (uwsgi_buffer_u32le(hr->last_chunked, hr->gzip_size)) return -1;
				if (uwsgi_buffer_append(hr->last_chunked, "\r\n", 2)) return -1;
			}
#endif
			if (uwsgi_buffer_append(hr->last_chunked, "0\r\n\r\n", 5)) return -1;
			peer->session->main_peer->out = hr->last_chunked;
			peer->session->main_peer->out_pos = 0;
			cr_write_to_main(peer, hr->func_write);
			if (!hr->session.can_keepalive) {
				hr->session.wait_full_write = 1;
			}
		}
		else {
			cr_reset_hooks(peer);
		}
		return 0;
	}

	// need to parse response headers
#ifdef UWSGI_ZLIB
	if (hr->session.can_keepalive || hr->can_gzip) {
#else
	if (hr->session.can_keepalive) {
#endif
		if (peer->r_parser_status != 4) {
			int ret = hr_check_response_keepalive(peer);
			if (ret < 0) return -1;
			if (ret > 0) {
				return 1;
			}
		}
#ifdef UWSGI_ZLIB
		else if (hr->force_gzip) {
			size_t zlen = 0;
			char *gzipped = uwsgi_deflate(&hr->z, peer->in->buf, peer->in->pos, &zlen);
			if (!gzipped) return -1;
			hr->gzip_size += peer->in->pos;
			uwsgi_crc32(&hr->gzip_crc32, peer->in->buf, peer->in->pos);
			peer->in->pos = 0;
			if (uwsgi_buffer_insert_chunked(peer->in, 0, zlen)) {free(gzipped); return -1;}
			if (uwsgi_buffer_append(peer->in, gzipped, zlen)) {
				free(gzipped);
				return -1;
			}
			free(gzipped);
			if (uwsgi_buffer_append(peer->in, "\r\n", 2)) return -1;
		}
#endif
		else if (hr->force_chunked) {
			peer->in->limit = 0;
			if (uwsgi_buffer_insert_chunked(peer->in, 0, len)) return -1;
			if (uwsgi_buffer_append(peer->in, "\r\n", 2)) return -1;
			peer->in->len = UMIN(peer->in->len, UMAX16);
		}
	}

        // set the input buffer as the main output one
        peer->session->main_peer->out = peer->in;
        peer->session->main_peer->out_pos = 0;

	// set the default hook in case of blocking writes (optimistic approach)
        cr_write_to_main(peer, hr->func_write);
	return 1;
}



ssize_t http_parse(struct corerouter_peer *main_peer) {
	struct corerouter_session *cs = main_peer->session;
	struct http_session *hr = (struct http_session *) cs;

	// is it http body ?
	if (hr->rnrn == 4) {
		// something bad happened in keepalive mode...
		if (!main_peer->session->peers) return -1;

		if (hr->content_length == 0 && !hr->raw_body) {
			// ignore data...
			main_peer->in->pos = 0;
			return 1;
		}
		else {
			if (hr->content_length) {
				if (main_peer->in->pos > hr->content_length) {
					main_peer->in->pos = hr->content_length;
					hr->content_length = 0;
					// on pipeline attempt, disable keepalive
					hr->session.can_keepalive = 0;
				}		
				else {
					hr->content_length -= main_peer->in->pos;
					if (hr->content_length == 0) {
						main_peer->disabled = 1;
                                		// stop reading from the client
                                		if (uwsgi_cr_set_hooks(main_peer, NULL, NULL)) return -1;
					}
				}
			}
		}
		main_peer->session->peers->out = main_peer->in;
		main_peer->session->peers->out_pos = 0;
		cr_write_to_backend(main_peer->session->peers, hr_instance_write);
		return 1;
	}

	// ensure the headers timeout is honoured
	http_set_timeout(main_peer, uhttp.headers_timeout);

	// read until \r\n\r\n is found
	size_t j;
	size_t len = main_peer->in->pos;
	char *ptr = main_peer->in->buf;

	hr->rnrn = 0;
	
	for (j = 0; j < len; j++) {
		if (*ptr == '\r' && (hr->rnrn == 0 || hr->rnrn == 2)) {
			hr->rnrn++;
		}
		else if (*ptr == '\r') {
			hr->rnrn = 1;
		}
		else if (*ptr == '\n' && hr->rnrn == 1) {
			hr->rnrn = 2;
		}
		else if (*ptr == '\n' && hr->rnrn == 3) {
			hr->rnrn = 4;
			hr->headers_size = j;

			// for security
			if ((j+1) <= len) {
				hr->remains = len - (j+1);
			}

			struct uwsgi_corerouter *ucr = main_peer->session->corerouter;

			// create a new peer
                	struct corerouter_peer *new_peer = uwsgi_cr_peer_add(main_peer->session);
			// default hook
			new_peer->last_hook_read = hr_instance_read;
		
			// parse HTTP request
			if (http_headers_parse(new_peer)) return -1;

			// check for a valid hostname
			if (new_peer->key_len == 0) return -1;

#ifdef UWSGI_SSL
			if (hr->force_https) {
				if (hr_force_https(new_peer)) return -1;
				break;
			}
#endif
			// find an instance using the key
                	if (ucr->mapper(ucr, new_peer))
                        	return -1;

                	// check instance
                	if (new_peer->instance_address_len == 0)
                        	return -1;

			// fix modifiers
			if (uhttp.modifier1)
				new_peer->modifier1 = uhttp.modifier1;
			if (uhttp.modifier2)
				new_peer->modifier2 = uhttp.modifier2;

			uint16_t pktsize = new_peer->out->pos-4;
        		// fix modifiers
        		new_peer->out->buf[0] = new_peer->modifier1;
        		new_peer->out->buf[3] = new_peer->modifier2;
        		// fix pktsize
        		new_peer->out->buf[1] = (uint8_t) (pktsize & 0xff);
        		new_peer->out->buf[2] = (uint8_t) ((pktsize >> 8) & 0xff);

			if (hr->remains > 0) {
				if (hr->content_length < hr->remains) {
					if (hr->content_length > 0 || !hr->raw_body)
						hr->remains = hr->content_length;
					hr->content_length = 0;
					// we need to avoid problems with pipelined requests
					hr->session.can_keepalive = 0;
				}
				else {
					hr->content_length -= hr->remains;
				}
				if (uwsgi_buffer_append(new_peer->out, main_peer->in->buf + hr->headers_size + 1, hr->remains)) return -1;
			}

			if (new_peer->modifier1 == 123) {
				// reset modifier1 to 0
				new_peer->out->buf[0] = 0;
				hr->raw_body = 1;
			}

			if (hr->websockets > 2 && hr->websocket_key_len > 0) {
				hr->raw_body = 1;
			}

			// on raw body, ensure keepalive is disabled
			if (hr->raw_body) hr->session.can_keepalive = 0;

			if (hr->session.can_keepalive && hr->content_length == 0) {
				main_peer->disabled = 1;
				// stop reading from the client
				if (uwsgi_cr_set_hooks(main_peer, NULL, NULL)) return -1;
			}

			if (hr->send_expect_100) {
				if (hr_manage_expect_continue(new_peer)) return -1;	
				break;
        		}


			new_peer->can_retry = 1;
			// reset main timeout
			http_set_timeout(main_peer, uhttp.cr.socket_timeout);
			// set peer timeout
			http_set_timeout(new_peer, uhttp.connect_timeout);
                	cr_connect(new_peer, hr_instance_connected);
			break;
		}
		else {
			hr->rnrn = 0;
		}
		ptr++;
	}
	
	return 1;
}

// read from client
ssize_t hr_read(struct corerouter_peer *main_peer) {
        // try to always leave 4k available (this will dinamically increase the buffer...)
        if (uwsgi_buffer_ensure(main_peer->in, uwsgi.page_size)) return -1;
        ssize_t len = cr_read(main_peer, "hr_read()");
        if (!len) return 0;

        return http_parse(main_peer);
}



void hr_session_close(struct corerouter_session *cs) {
	struct http_session *hr = (struct http_session *) cs;
	if (hr->path_info) {
		free(hr->path_info);
	}

	if (hr->last_chunked) {
		uwsgi_buffer_destroy(hr->last_chunked);
	}

#ifdef UWSGI_ZLIB
	if (hr->z.next_in) {
		deflateEnd(&hr->z);
	}
#endif
}

ssize_t hr_recv_stud4(struct corerouter_peer * main_peer) {
	struct http_session *hr = (struct http_session *) main_peer->session;
	ssize_t len = read(main_peer->fd, hr->stud_prefix + hr->stud_prefix_pos, hr->stud_prefix_remains - hr->stud_prefix_pos);
	if (len < 0) {
                cr_try_again;
                uwsgi_cr_error(main_peer, "hr_recv_stud4()");
                return -1;
        }

	hr->stud_prefix_pos += len;

        if (hr->stud_prefix_pos == hr->stud_prefix_remains) {
		if (hr->stud_prefix[0] != AF_INET) {
			uwsgi_cr_log(main_peer, "invalid stud prefix for address family %d\n", hr->stud_prefix[0]);
			return -1;
		}
		// set the passed ip address
		memcpy(&main_peer->session->client_sockaddr.sa_in.sin_addr, hr->stud_prefix + 1, 4);
		
		// optimistic approach
		main_peer->hook_read = hr_read;
		return hr_read(main_peer);
        }

        return len;

}

// retry connection to the backend
static int hr_retry(struct corerouter_peer *peer) {

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
	http_set_timeout(peer, uhttp.connect_timeout);
        cr_connect(peer, hr_instance_connected);
        return 0;
}


int http_alloc_session(struct uwsgi_corerouter *ucr, struct uwsgi_gateway_socket *ugs, struct corerouter_session *cs, struct sockaddr *sa, socklen_t s_len) {

	if (!uhttp.headers_timeout) uhttp.headers_timeout = uhttp.cr.socket_timeout;
	if (!uhttp.connect_timeout) uhttp.connect_timeout = uhttp.cr.socket_timeout;

	// set the retry hook
        cs->retry = hr_retry;
	struct http_session *hr = (struct http_session *) cs;
	// default hook
	cs->main_peer->last_hook_read = hr_read;

	// headers timeout
	cs->main_peer->current_timeout = uhttp.headers_timeout;

	if (uhttp.raw_body) {
		hr->raw_body = 1;
	}

	if (uhttp.websockets) {
		hr->websockets = 1;	
	}
	hr->func_write = hr_write;

	// be sure buffer does not grow over 64k
        cs->main_peer->in->limit = UMAX16;

	if (sa && sa->sa_family == AF_INET) {
		struct uwsgi_string_list *usl = uhttp.stud_prefix;
		while(usl) {
			if (!memcmp(&cs->client_sockaddr.sa_in.sin_addr, usl->value, 4)) {
				hr->stud_prefix_remains = 5;
				cs->main_peer->last_hook_read = hr_recv_stud4;
				break;
			}
			usl = usl->next;
		}

	}

	hr->port = ugs->port;
	hr->port_len = ugs->port_len;
	switch(ugs->mode) {
#ifdef UWSGI_SSL
		case UWSGI_HTTP_SSL:
			hr_setup_ssl(hr, ugs);
			break;
#endif
		default:
			if (uwsgi_cr_set_hooks(cs->main_peer, cs->main_peer->last_hook_read, NULL))
				return -1;
			cs->close = hr_session_close;
			break;
	}

	return 0;
}

void http_setup() {
	uhttp.cr.name = uwsgi_str("uWSGI http");
	uhttp.cr.short_name = uwsgi_str("http");
}


int http_init() {

	uhttp.cr.session_size = sizeof(struct http_session);
	uhttp.cr.alloc_session = http_alloc_session;
	if (uhttp.cr.has_sockets && !uwsgi_corerouter_has_backends(&uhttp.cr)) {
		if (!uwsgi.sockets) {
			uwsgi_new_socket(uwsgi_concat2("127.0.0.1:0", ""));
		}
		uhttp.cr.use_socket = 1;
		uhttp.cr.socket_num = 0;
	}
	uwsgi_corerouter_init((struct uwsgi_corerouter *) &uhttp);
	return 0;
}

struct uwsgi_plugin http_plugin = {

	.name = "http",
	.options = http_options,
	.init = http_init,
	.on_load = http_setup,
};
