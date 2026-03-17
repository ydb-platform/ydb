#include <contrib/python/uWSGI/py3/config.h>
/* async http protocol parser */

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

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

static uint16_t http_add_uwsgi_header(struct wsgi_request *wsgi_req, char *hh, size_t hhlen, char *hv, size_t hvlen, int has_prefix) {

	char *buffer = wsgi_req->buffer + wsgi_req->uh->pktsize;
	char *watermark = wsgi_req->buffer + uwsgi.buffer_size;
	char *ptr = buffer;
	size_t keylen = hhlen;

	if (has_prefix) keylen += 5;

	if (buffer + keylen + hvlen + 2 + 2 >= watermark) {
		if (has_prefix) {
			uwsgi_log("[WARNING] unable to add HTTP_%.*s=%.*s to uwsgi packet, consider increasing buffer size\n", keylen-5, hh, hvlen, hv);
		}
		else {
			uwsgi_log("[WARNING] unable to add %.*s=%.*s to uwsgi packet, consider increasing buffer size\n", keylen, hh, hvlen, hv);
		}
		return 0;
	}


	*ptr++ = (uint8_t) (keylen & 0xff);
	*ptr++ = (uint8_t) ((keylen >> 8) & 0xff);

	if (has_prefix) {
		memcpy(ptr, "HTTP_", 5);
		ptr += 5;
		memcpy(ptr, hh, keylen - 5);
		ptr += (keylen - 5);
	}
	else {
		memcpy(ptr, hh, keylen);
		ptr += keylen;
	}

	*ptr++ = (uint8_t) (hvlen & 0xff);
	*ptr++ = (uint8_t) ((hvlen >> 8) & 0xff);
	memcpy(ptr, hv, hvlen);

	return 2 + keylen + 2 + hvlen;
}

char *proxy1_parse(char *ptr, char *watermark, char **src, uint16_t *src_len, char **dst, uint16_t *dst_len,  char **src_port, uint16_t *src_port_len, char **dst_port, uint16_t *dst_port_len) {
	// check for PROXY header
	if (watermark - ptr > 6) {
		if (memcmp(ptr, "PROXY ", 6)) return ptr;
	}
	else {
		return ptr;
	}

	ptr+= 6;
	char *base = ptr;
	while (ptr < watermark) {
		if (*ptr == ' ') {
                        ptr++;
                        break;
                }
		else if (*ptr == '\n') {
			return ptr+1;
		}
                ptr++;
	}

	// SRC address
	base = ptr;
	while (ptr < watermark) {
		if (*ptr == ' ') {
			*src = base;
			*src_len = ptr - base;
                        ptr++;
                        break;
                }
                else if (*ptr == '\n') {
                        return ptr+1;
                }
                ptr++;
	}

	// DST address
        base = ptr;
        while (ptr < watermark) {
                if (*ptr == ' ') {
                        *dst = base;
                        *dst_len = ptr - base;
                        ptr++;
                        break;
                }
                else if (*ptr == '\n') {
                        return ptr+1;
                }
                ptr++;
        }

	// SRC port
        base = ptr;
        while (ptr < watermark) {
                if (*ptr == ' ') {
                        *src_port = base;
                        *src_port_len = ptr - base;
                        ptr++;
                        break;
                }
                else if (*ptr == '\n') {
                        return ptr+1;
                }
                ptr++;
        }

        // DST port
        base = ptr;
        while (ptr < watermark) {
                if (*ptr == '\r') {
                        *dst_port = base;
                        *dst_port_len = ptr - base;
                        ptr++;
                        break;
                }
                else if (*ptr == '\n') {
                        return ptr+1;
                }
                ptr++;
        }

	// check for \n
	while (ptr < watermark) {
		if (*ptr == '\n') return ptr+1;
		ptr++;
	}

	return ptr;
}

static int http_parse(struct wsgi_request *wsgi_req, char *watermark) {

	char *ptr = wsgi_req->proto_parser_buf;
	char *base = ptr;
	char *query_string = NULL;
	char ip[INET6_ADDRSTRLEN+1];
	struct sockaddr *http_sa = (struct sockaddr *) &wsgi_req->client_addr;
	char *proxy_src = NULL;
	char *proxy_dst = NULL;
	char *proxy_src_port = NULL;
	char *proxy_dst_port = NULL;
	uint16_t proxy_src_len = 0;
	uint16_t proxy_dst_len = 0;
	uint16_t proxy_src_port_len = 0;
	uint16_t proxy_dst_port_len = 0;

	if (uwsgi.enable_proxy_protocol) {
		ptr = proxy1_parse(ptr, watermark, &proxy_src, &proxy_src_len, &proxy_dst, &proxy_dst_len, &proxy_src_port, &proxy_src_port_len, &proxy_dst_port, &proxy_dst_port_len);
		base = ptr;
	}

	// REQUEST_METHOD 
	while (ptr < watermark) {
		if (*ptr == ' ') {
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REQUEST_METHOD", 14, base, ptr - base);
			ptr++;
			break;
		}
		ptr++;
	}

	// REQUEST_URI / PATH_INFO / QUERY_STRING
	base = ptr;
	while (ptr < watermark) {
		if (*ptr == '?' && !query_string) {
			if (watermark + (ptr - base) < (char *)(wsgi_req->proto_parser_buf + uwsgi.buffer_size)) {
				uint16_t path_info_len = ptr - base;
				char *path_info = uwsgi_malloc(path_info_len);
				http_url_decode(base, &path_info_len, path_info);
				wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "PATH_INFO", 9, path_info, path_info_len);
				free(path_info);
			}
			else {
				uwsgi_log("not enough space in wsgi_req http proto_parser_buf to decode PATH_INFO, consider tuning it with --buffer-size\n");
				return -1;
			}
			query_string = ptr + 1;
		}
		else if (*ptr == ' ') {
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REQUEST_URI", 11, base, ptr - base);
			
			if (!query_string) {
				if (watermark + (ptr - base) < (char *)(wsgi_req->proto_parser_buf + uwsgi.buffer_size)) {
                                	uint16_t path_info_len = ptr - base;
					char *path_info = uwsgi_malloc(path_info_len);
                                	http_url_decode(base, &path_info_len, path_info);
                                	wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "PATH_INFO", 9, path_info, path_info_len);
					free(path_info);
                        	}
				else {
					uwsgi_log("not enough space in wsgi_req http proto_parser_buf to decode PATH_INFO, consider tuning it with --buffer-size\n");
					return -1;
				}
				wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "QUERY_STRING", 12, "", 0);
			}
			else {
				wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "QUERY_STRING", 12, query_string, ptr - query_string);
			}
			ptr++;
			break;
		}
		ptr++;
	}

	// SERVER_PROTOCOL
	base = ptr;
	while (ptr < watermark) {
		if (*ptr == '\r') {
			if (ptr + 1 >= watermark)
				return -1 ;
			if (*(ptr + 1) != '\n')
				return -1;
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_PROTOCOL", 15, base, ptr - base);
			ptr += 2;
			break;
		}
		ptr++;
	}

	// SCRIPT_NAME
	if (!uwsgi.manage_script_name) {
		wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SCRIPT_NAME", 11, "", 0);
	}
	

	// SERVER_NAME
	wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_NAME", 11, uwsgi.hostname, uwsgi.hostname_len);

	// SERVER_PORT / SERVER_ADDR
	if (proxy_dst) {
		wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_ADDR", 11, proxy_dst, proxy_dst_len);
                if (proxy_dst_port) {
                        wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_PORT", 11, proxy_dst_port, proxy_dst_port_len);
                }
	}
	else {
		char *server_port = strrchr(wsgi_req->socket->name, ':');
		if (server_port) {
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_PORT", 11, server_port+1, strlen(server_port+1));
		}
		else {
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "SERVER_PORT", 11, "80", 2);
		}
	}

	// REMOTE_ADDR
	if (proxy_src) {
		wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REMOTE_ADDR", 11, proxy_src, proxy_src_len);
		if (proxy_src_port) {
			wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REMOTE_PORT", 11, proxy_src_port, proxy_src_port_len);
		}
	}
	else {
		// TODO log something useful for AF_UNIX sockets
		switch(http_sa->sa_family) {
		case AF_INET6: {
			struct sockaddr_in6* http_sin = &wsgi_req->client_addr.sin6;
			memset(ip, 0, sizeof(ip));
			/* check if it's an IPv6-mapped-IPv4 address and, if so,
			 * represent it as an IPv4 address
			 *
			 * these IPv6 macros are defined in POSIX.1-2001.
			 */
			if (IN6_IS_ADDR_V4MAPPED(&http_sin->sin6_addr)) {
				/* just grab the last 4 bytes and pretend they're
				 * IPv4. None of the word/half-word convenience
				 * functions are in POSIX, so just stick to .s6_addr
				 */
				union {
					unsigned char s6[4];
					uint32_t s4;
				} addr_parts;
				memcpy(addr_parts.s6, &http_sin->sin6_addr.s6_addr[12], 4);
				uint32_t in4_addr = addr_parts.s4;
				if (inet_ntop(AF_INET, (void*)&in4_addr, ip, INET_ADDRSTRLEN)) {
					wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REMOTE_ADDR", 11, ip, strlen(ip));
				} else {
					uwsgi_error("inet_ntop()");
					return -1;
				}
			} else {
				if (inet_ntop(AF_INET6, (void *) &http_sin->sin6_addr, ip, INET6_ADDRSTRLEN)) {
					wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REMOTE_ADDR", 11, ip, strlen(ip));
				} else {
					uwsgi_error("inet_ntop()");
					return -1;
				}
			}
			break;
		}
		case AF_INET:
		default: {
			struct sockaddr_in* http_sin = &wsgi_req->client_addr.sin;
			memset(ip, 0, sizeof(ip));
			if (inet_ntop(AF_INET, (void *) &http_sin->sin_addr, ip, INET_ADDRSTRLEN)) {
				wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "REMOTE_ADDR", 11, ip, strlen(ip));
			}
			else {
				uwsgi_error("inet_ntop()");
				return -1;
			}
			}
			break;
		}
	}

	if (wsgi_req->https_len > 0) {
		wsgi_req->uh->pktsize += proto_base_add_uwsgi_var(wsgi_req, "HTTPS", 5, wsgi_req->https, wsgi_req->https_len);
	}

	//HEADERS
	base = ptr;

	struct uwsgi_string_list *headers = NULL, *usl = NULL;

	while (ptr < watermark) {
		if (*ptr == '\r') {
			if (ptr + 1 >= watermark)
				return -1;
			if (*(ptr + 1) != '\n')
				return -1;
			// multiline header ?
			if (ptr + 2 < watermark) {
				if (*(ptr + 2) == ' ' || *(ptr + 2) == '\t') {
					ptr += 2;
					continue;
				}
			}
			size_t key_len = 0, value_len = 0;
			int has_prefix = 0;
			// last line, do not waste time
			if (ptr - base == 0) break;
			char *value = http_header_to_cgi(base, ptr - base, &key_len, &value_len, &has_prefix);
			if (!value) {
				uwsgi_log_verbose("invalid HTTP request\n");
				goto clear;
			}
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
			uint16_t old_pktsize = wsgi_req->uh->pktsize;
			wsgi_req->uh->pktsize += http_add_uwsgi_header(wsgi_req, usl->value, usl->len, usl->custom_ptr, (size_t) usl->custom, usl->custom2 & 0x02);
			// if the packet remains unchanged, the buffer is full, mark the request as broken
			if (old_pktsize == wsgi_req->uh->pktsize) {
				broken = 1;
			}
		}
		if (usl->custom2 & 0x01) {
			free(usl->custom_ptr);
		}
		struct uwsgi_string_list *tmp_usl = usl;
		usl = usl->next;
		free(tmp_usl);
	}

	return broken;

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



static int uwsgi_proto_http_parser(struct wsgi_request *wsgi_req) {

	ssize_t j;
	char *ptr;

	// first round ? (wsgi_req->proto_parser_buf is freed at the end of the request)
	if (!wsgi_req->proto_parser_buf) {
		wsgi_req->proto_parser_buf = uwsgi_malloc(uwsgi.buffer_size);
	}

	if (uwsgi.buffer_size - wsgi_req->proto_parser_pos == 0) {
		uwsgi_log("invalid HTTP request size (max %u)...skip\n", uwsgi.buffer_size);
		return -1;
	}

	ssize_t len = read(wsgi_req->fd, wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos, uwsgi.buffer_size - wsgi_req->proto_parser_pos);
	if (len > 0) {
		goto parse;
	}
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
                	return UWSGI_AGAIN;
                }
		uwsgi_error("uwsgi_proto_http_parser()");
		return -1;
	}
	// mute on 0 len...
	if (wsgi_req->proto_parser_pos > 0) {
		uwsgi_log("uwsgi_proto_http_parser() -> client closed connection\n");
	}
	return -1;

parse:
	ptr = wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos;
	wsgi_req->proto_parser_pos += len;

	for (j = 0; j < len; j++) {
		if (*ptr == '\r' && (wsgi_req->proto_parser_status == 0 || wsgi_req->proto_parser_status == 2)) {
			wsgi_req->proto_parser_status++;
		}
		else if (*ptr == '\r') {
			wsgi_req->proto_parser_status = 1;
		}
		else if (*ptr == '\n' && wsgi_req->proto_parser_status == 1) {
			wsgi_req->proto_parser_status = 2;
		}
		else if (*ptr == '\n' && wsgi_req->proto_parser_status == 3) {
			ptr++;
			wsgi_req->proto_parser_remains = len - (j + 1);
			if (wsgi_req->proto_parser_remains > 0) {
				wsgi_req->proto_parser_remains_buf = (wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos) - wsgi_req->proto_parser_remains;
			}
			if (http_parse(wsgi_req, ptr)) return -1;
			wsgi_req->uh->modifier1 = uwsgi.http_modifier1;
			wsgi_req->uh->modifier2 = uwsgi.http_modifier2;
			return UWSGI_OK;
		}
		else {
			wsgi_req->proto_parser_status = 0;
		}
		ptr++;
	}

	return UWSGI_AGAIN;
}

static void uwsgi_httpize_var(char *buf, size_t len) {
	size_t i;
	int upper = 1;
	for(i=0;i<len;i++) {
		if (upper) {
			upper = 0;
			continue;
		}

		if (buf[i] == '_') {
			buf[i] = '-';
			upper = 1;
			continue;
		}

		buf[i] = tolower( (int) buf[i]);
	}
}

struct uwsgi_buffer *uwsgi_to_http_dumb(struct wsgi_request *wsgi_req, char *host, uint16_t host_len, char *uri, uint16_t uri_len) {

        struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);

        if (uwsgi_buffer_append(ub, wsgi_req->method, wsgi_req->method_len)) goto clear;
        if (uwsgi_buffer_append(ub, " ", 1)) goto clear;

        if (uri_len && uri) {
                if (uwsgi_buffer_append(ub, uri, uri_len)) goto clear;
        }
        else {
                if (uwsgi_buffer_append(ub, wsgi_req->uri, wsgi_req->uri_len)) goto clear;
        }

        if (uwsgi_buffer_append(ub, " ", 1)) goto clear;
        if (uwsgi_buffer_append(ub, wsgi_req->protocol, wsgi_req->protocol_len)) goto clear;
        if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;

        int i;
        char *x_forwarded_for = NULL;
        size_t x_forwarded_for_len = 0;

        // start adding headers
        for(i=0;i<wsgi_req->var_cnt;i++) {
                if (!uwsgi_starts_with(wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i].iov_len, "HTTP_", 5)) {

                        char *header = wsgi_req->hvec[i].iov_base+5;
                        size_t header_len = wsgi_req->hvec[i].iov_len-5;

                        if (host && !uwsgi_strncmp(header, header_len, "HOST", 4)) goto next;

                        if (!uwsgi_strncmp(header, header_len, "X_FORWARDED_FOR", 15)) {
                                x_forwarded_for = wsgi_req->hvec[i+1].iov_base;
                                x_forwarded_for_len = wsgi_req->hvec[i+1].iov_len;
                                goto next;
                        }

                        if (uwsgi_buffer_append(ub, header, header_len)) goto clear;

                        // transofmr uwsgi var to http header
                        uwsgi_httpize_var((ub->buf+ub->pos) - header_len, header_len);

                        if (uwsgi_buffer_append(ub, ": ", 2)) goto clear;
                        if (uwsgi_buffer_append(ub, wsgi_req->hvec[i+1].iov_base, wsgi_req->hvec[i+1].iov_len)) goto clear;
                        if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;

                }
next:
                i++;
        }


        // append custom Host (if needed)
        if (host) {
                if (uwsgi_buffer_append(ub, "Host: ", 6)) goto clear;
                if (uwsgi_buffer_append(ub, host, host_len)) goto clear;
                if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
        }

        if (wsgi_req->content_type_len > 0) {
                if (uwsgi_buffer_append(ub, "Content-Type: ", 14)) goto clear;
                if (uwsgi_buffer_append(ub, wsgi_req->content_type, wsgi_req->content_type_len)) goto clear;
                if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
        }

        if (wsgi_req->post_cl > 0) {
                if (uwsgi_buffer_append(ub, "Content-Length: ", 16)) goto clear;
                if (uwsgi_buffer_num64(ub, (int64_t) wsgi_req->post_cl)) goto clear;
                if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
        }

        // append required headers
        if (uwsgi_buffer_append(ub, "X-Forwarded-For: ", 17)) goto clear;

        if (x_forwarded_for_len > 0) {
                if (uwsgi_buffer_append(ub, x_forwarded_for, x_forwarded_for_len)) goto clear;
                if (uwsgi_buffer_append(ub, ", ", 2)) goto clear;
        }

        if (uwsgi_buffer_append(ub, wsgi_req->remote_addr, wsgi_req->remote_addr_len)) goto clear;

        if (uwsgi_buffer_append(ub, "\r\n\r\n", 4)) goto clear;

        return ub;
clear:
        uwsgi_buffer_destroy(ub);
        return NULL;
}


struct uwsgi_buffer *uwsgi_to_http(struct wsgi_request *wsgi_req, char *host, uint16_t host_len, char *uri, uint16_t uri_len) {

        struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);

        if (uwsgi_buffer_append(ub, wsgi_req->method, wsgi_req->method_len)) goto clear;
        if (uwsgi_buffer_append(ub, " ", 1)) goto clear;

	if (uri_len && uri) {
        	if (uwsgi_buffer_append(ub, uri, uri_len)) goto clear;
	}
	else {
        	if (uwsgi_buffer_append(ub, wsgi_req->uri, wsgi_req->uri_len)) goto clear;
	}

	// force HTTP/1.0
        if (uwsgi_buffer_append(ub, " HTTP/1.0\r\n", 11)) goto clear;

        int i;
	char *x_forwarded_for = NULL;
	size_t x_forwarded_for_len = 0;

        // start adding headers
        for(i=0;i<wsgi_req->var_cnt;i++) {
		if (!uwsgi_starts_with(wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i].iov_len, "HTTP_", 5)) {

			char *header = wsgi_req->hvec[i].iov_base+5;
			size_t header_len = wsgi_req->hvec[i].iov_len-5;

			if (host && !uwsgi_strncmp(header, header_len, "HOST", 4)) goto next;

			// remove dangerous headers
			if (!uwsgi_strncmp(header, header_len, "CONNECTION", 10)) goto next;
			if (!uwsgi_strncmp(header, header_len, "KEEP_ALIVE", 10)) goto next;
			if (!uwsgi_strncmp(header, header_len, "TE", 2)) goto next;
			if (!uwsgi_strncmp(header, header_len, "TRAILER", 7)) goto next;
			if (!uwsgi_strncmp(header, header_len, "X_FORWARDED_FOR", 15)) {
				x_forwarded_for = wsgi_req->hvec[i+1].iov_base;
				x_forwarded_for_len = wsgi_req->hvec[i+1].iov_len;
				goto next;
			}

			if (uwsgi_buffer_append(ub, header, header_len)) goto clear;

			// transofmr uwsgi var to http header
			uwsgi_httpize_var((ub->buf+ub->pos) - header_len, header_len);

			if (uwsgi_buffer_append(ub, ": ", 2)) goto clear;
			if (uwsgi_buffer_append(ub, wsgi_req->hvec[i+1].iov_base, wsgi_req->hvec[i+1].iov_len)) goto clear;
			if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;

		}
next:
		i++;
        }


	// append custom Host (if needed)
	if (host) {
		if (uwsgi_buffer_append(ub, "Host: ", 6)) goto clear;
		if (uwsgi_buffer_append(ub, host, host_len)) goto clear;
		if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
	}

	if (wsgi_req->content_type_len > 0) {
		if (uwsgi_buffer_append(ub, "Content-Type: ", 14)) goto clear;
		if (uwsgi_buffer_append(ub, wsgi_req->content_type, wsgi_req->content_type_len)) goto clear;
		if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
	}

	if (wsgi_req->post_cl > 0) {
		if (uwsgi_buffer_append(ub, "Content-Length: ", 16)) goto clear;
		if (uwsgi_buffer_num64(ub, (int64_t) wsgi_req->post_cl)) goto clear;
		if (uwsgi_buffer_append(ub, "\r\n", 2)) goto clear;
	}

	// append required headers
	if (uwsgi_buffer_append(ub, "Connection: close\r\n", 19)) goto clear;
	if (uwsgi_buffer_append(ub, "X-Forwarded-For: ", 17)) goto clear;

	if (x_forwarded_for_len > 0) {
		if (uwsgi_buffer_append(ub, x_forwarded_for, x_forwarded_for_len)) goto clear;
		if (uwsgi_buffer_append(ub, ", ", 2)) goto clear;
	}

	if (uwsgi_buffer_append(ub, wsgi_req->remote_addr, wsgi_req->remote_addr_len)) goto clear;

	if (uwsgi_buffer_append(ub, "\r\n\r\n", 4)) goto clear;

	return ub;
clear:
        uwsgi_buffer_destroy(ub);
        return NULL;
}

int uwsgi_is_full_http(struct uwsgi_buffer *ub) {
	size_t i;
	int status = 0;
	for(i=0;i<ub->pos;i++) {
		switch(status) {
			// \r
			case 0:
				if (ub->buf[i] == '\r') status = 1;
				break;
			// \r\n
			case 1:
				if (ub->buf[i] == '\n') {
					status = 2;
					break;
				}
				status = 0;
				break;
			// \r\n\r
			case 2:
				if (ub->buf[i] == '\r') {
					status = 3;
					break;
				}
				status = 0;
				break;
			// \r\n\r\n
			case 3:
				if (ub->buf[i] == '\n') {
					return 1;
				}
				status = 0;
				break;
			default:
				status = 0;
				break;
			
		}
	}

	return 0;
}

void uwsgi_proto_http_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_http_parser;
	uwsgi_sock->proto_accept = uwsgi_proto_base_accept;
	uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
	uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
	uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
	uwsgi_sock->proto_read_body = uwsgi_proto_base_read_body;
	uwsgi_sock->proto_write = uwsgi_proto_base_write;
	uwsgi_sock->proto_writev = uwsgi_proto_base_writev;
	uwsgi_sock->proto_write_headers = uwsgi_proto_base_write;
	uwsgi_sock->proto_sendfile = uwsgi_proto_base_sendfile;
	uwsgi_sock->proto_close = uwsgi_proto_base_close;
	if (uwsgi.offload_threads > 0)
		uwsgi_sock->can_offload = 1;

}

/*
close the connection on errors, incomplete parsing, HTTP/1.0,  pipelined or offloaded requests
NOTE: Connection: close is not honoured
*/
void uwsgi_proto_http11_close(struct wsgi_request *wsgi_req) {
	// check for errors or incomplete packets
	if (wsgi_req->write_errors || wsgi_req->proto_parser_status != 3 || wsgi_req->proto_parser_remains > 0
		|| wsgi_req->post_pos < wsgi_req->post_cl || wsgi_req->via == UWSGI_VIA_OFFLOAD
		|| !uwsgi_strncmp("HTTP/1.0", 8, wsgi_req->protocol, wsgi_req->protocol_len)) {
		close(wsgi_req->fd);
		wsgi_req->socket->retry[wsgi_req->async_id] = 0;
		wsgi_req->socket->fd_threads[wsgi_req->async_id] = -1;
	}
	else {
		wsgi_req->socket->retry[wsgi_req->async_id] = 1;
		wsgi_req->socket->fd_threads[wsgi_req->async_id] = wsgi_req->fd;
	}
}

int uwsgi_proto_http11_accept(struct wsgi_request *wsgi_req, int fd) {
	if (wsgi_req->socket->retry[wsgi_req->async_id]) {
		wsgi_req->fd = wsgi_req->socket->fd_threads[wsgi_req->async_id];
		int ret = uwsgi_wait_read_req(wsgi_req);
		if (ret <= 0) {
			close(wsgi_req->fd);
			wsgi_req->socket->retry[wsgi_req->async_id] = 0;
			wsgi_req->socket->fd_threads[wsgi_req->async_id] = -1;
			return -1;
		}
		return wsgi_req->socket->fd_threads[wsgi_req->async_id];
	}
	return uwsgi_proto_base_accept(wsgi_req, fd);
}

void uwsgi_proto_http11_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_http_parser;
	uwsgi_sock->proto_accept = uwsgi_proto_http11_accept;
	uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
	uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
	uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
	uwsgi_sock->proto_read_body = uwsgi_proto_base_read_body;
	uwsgi_sock->proto_write = uwsgi_proto_base_write;
	uwsgi_sock->proto_writev = uwsgi_proto_base_writev;
	uwsgi_sock->proto_write_headers = uwsgi_proto_base_write;
	uwsgi_sock->proto_sendfile = uwsgi_proto_base_sendfile;
	uwsgi_sock->proto_close = uwsgi_proto_http11_close;
	if (uwsgi.offload_threads > 0)
		uwsgi_sock->can_offload = 1;
	uwsgi_sock->fd_threads = uwsgi_malloc(sizeof(int) * uwsgi.cores);
	memset(uwsgi_sock->fd_threads, -1, sizeof(int) * uwsgi.cores);
	uwsgi_sock->retry = uwsgi_calloc(sizeof(int) * uwsgi.cores);
	uwsgi.is_et = 1;
}

#ifdef UWSGI_SSL
static int uwsgi_proto_https_parser(struct wsgi_request *wsgi_req) {

        ssize_t j;
        char *ptr;
	int len = -1;

        // first round ? (wsgi_req->proto_parser_buf is freed at the end of the request)
        if (!wsgi_req->proto_parser_buf) {
                wsgi_req->proto_parser_buf = uwsgi_malloc(uwsgi.buffer_size);
        }

        if (uwsgi.buffer_size - wsgi_req->proto_parser_pos == 0) {
                uwsgi_log("invalid HTTPS request size (max %u)...skip\n", uwsgi.buffer_size);
                return -1;
        }

retry:
        len = SSL_read(wsgi_req->ssl, wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos, uwsgi.buffer_size - wsgi_req->proto_parser_pos);
        if (len > 0) {
                goto parse;
        }
	if (len == 0) goto empty;

        int err = SSL_get_error(wsgi_req->ssl, len);

        if (err == SSL_ERROR_WANT_READ) {
                return UWSGI_AGAIN;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
                int ret = uwsgi_wait_write_req(wsgi_req);
                if (ret <= 0) return -1;
                goto retry;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_error("uwsgi_proto_https_parser()/SSL_read()");
        }
	return -1;
empty:
        // mute on 0 len...
        if (wsgi_req->proto_parser_pos > 0) {
                uwsgi_log("uwsgi_proto_https_parser() -> client closed connection");
        }
        return -1;

parse:
        ptr = wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos;
        wsgi_req->proto_parser_pos += len;

        for (j = 0; j < len; j++) {
                if (*ptr == '\r' && (wsgi_req->proto_parser_status == 0 || wsgi_req->proto_parser_status == 2)) {
                        wsgi_req->proto_parser_status++;
                }
                else if (*ptr == '\r') {
                        wsgi_req->proto_parser_status = 1;
                }
                else if (*ptr == '\n' && wsgi_req->proto_parser_status == 1) {
                        wsgi_req->proto_parser_status = 2;
                }
                else if (*ptr == '\n' && wsgi_req->proto_parser_status == 3) {
                        ptr++;
                        wsgi_req->proto_parser_remains = len - (j + 1);
                        if (wsgi_req->proto_parser_remains > 0) {
                                wsgi_req->proto_parser_remains_buf = (wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos) - wsgi_req->proto_parser_remains;
                        }
			wsgi_req->https = "on";
			wsgi_req->https_len = 2;
                        if (http_parse(wsgi_req, ptr)) return -1;
                        wsgi_req->uh->modifier1 = uwsgi.https_modifier1;
                        wsgi_req->uh->modifier2 = uwsgi.https_modifier2;
                        return UWSGI_OK;
                }
                else {
                        wsgi_req->proto_parser_status = 0;
                }
                ptr++;
        }

        return UWSGI_AGAIN;
}

void uwsgi_proto_https_setup(struct uwsgi_socket *uwsgi_sock) {
        uwsgi_sock->proto = uwsgi_proto_https_parser;
                        uwsgi_sock->proto_accept = uwsgi_proto_ssl_accept;
                        uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
                        uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
                        uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
                        uwsgi_sock->proto_read_body = uwsgi_proto_ssl_read_body;
                        uwsgi_sock->proto_write = uwsgi_proto_ssl_write;
                        uwsgi_sock->proto_write_headers = uwsgi_proto_ssl_write;
                        uwsgi_sock->proto_sendfile = uwsgi_proto_ssl_sendfile;
                        uwsgi_sock->proto_close = uwsgi_proto_ssl_close;

}
#endif
