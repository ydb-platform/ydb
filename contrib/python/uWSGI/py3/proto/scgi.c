#include <contrib/python/uWSGI/py3/config.h>
/* async SCGI protocol parser */

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

static int scgi_parse(struct wsgi_request *wsgi_req) {
	char *buf = wsgi_req->proto_parser_buf;
	size_t len = wsgi_req->proto_parser_pos;
	size_t i;
	size_t scgi_len = 0;
	for(i=0;i<len;i++) {
		if (buf[i] == ':') {
			scgi_len = uwsgi_str_num(buf, i);
			if (scgi_len == 0) return -1;
			goto keyval;
		}
	}
	return 0;

keyval:

	if (i + scgi_len + 1 > len) {
		return 0;
	}

	i++;

	size_t vars = i;
	char *key = buf + i;
	size_t keylen = 0;
	char *value = NULL;
	size_t vallen = 0;
	for(i=vars;i<vars+scgi_len;i++) {
		if (key == NULL) {
			key = buf + i;
		}
		else if (keylen > 0 && value == NULL) {
			value = buf + i;
		}
		if (buf[i] == 0) {
			if (value) {
				vallen = (buf+i) - value;
				uint16_t pktsize = proto_base_add_uwsgi_var(wsgi_req, key, keylen, value, vallen);
                		if (pktsize == 0) return -1;
                		wsgi_req->uh->pktsize += pktsize;
				key = NULL;
				value = NULL;
				keylen = 0;
				vallen = 0;
			}
			else {
				keylen = (buf+i) - key;
				value = NULL;
			}
		}
	}


	if (buf[i] == ',') {
		if (len > i+1) {
			wsgi_req->proto_parser_remains = len-(i+1);
                        wsgi_req->proto_parser_remains_buf = buf + i + 1;			
		}
		return 1;
	}
	return -1;
}

int uwsgi_proto_scgi_parser(struct wsgi_request *wsgi_req) {

	// first round ? (wsgi_req->proto_parser_buf is freed at the end of the request)
        if (!wsgi_req->proto_parser_buf) {
                wsgi_req->proto_parser_buf = uwsgi_malloc(uwsgi.buffer_size);
        }

	if (uwsgi.buffer_size - wsgi_req->proto_parser_pos == 0) {
                uwsgi_log("invalid SCGI request size (max %u)...skip\n", uwsgi.buffer_size);
                return -1;
        }

	char *ptr = wsgi_req->proto_parser_buf;

	ssize_t len = read(wsgi_req->fd, ptr + wsgi_req->proto_parser_pos, uwsgi.buffer_size - wsgi_req->proto_parser_pos);
	if (len > 0) {
		wsgi_req->proto_parser_pos += len;
		int ret = scgi_parse(wsgi_req);
		if (ret > 0) {
			wsgi_req->uh->modifier1 = uwsgi.scgi_modifier1;
                        wsgi_req->uh->modifier2 = uwsgi.scgi_modifier2;
			return UWSGI_OK;
		}
		if (ret == 0) return UWSGI_AGAIN;
		return -1;
	}
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
		uwsgi_error("uwsgi_proto_scgi_parser()");	
		return -1;
	}
	// 0 len
	if (wsgi_req->proto_parser_pos > 0) {
		uwsgi_error("uwsgi_proto_scgi_parser()");	
	}
	return -1;
}

void uwsgi_proto_scgi_setup(struct uwsgi_socket *uwsgi_sock) {
                        uwsgi_sock->proto = uwsgi_proto_scgi_parser;
                        uwsgi_sock->proto_accept = uwsgi_proto_base_accept;
                        uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_cgi_prepare_headers;
                        uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
                        uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
                        uwsgi_sock->proto_read_body = uwsgi_proto_base_read_body;
                        uwsgi_sock->proto_write = uwsgi_proto_base_write;
                        uwsgi_sock->proto_writev = uwsgi_proto_base_writev;
                        uwsgi_sock->proto_write_headers = uwsgi_proto_base_write;
                        uwsgi_sock->proto_sendfile = uwsgi_proto_base_sendfile;
                        uwsgi_sock->proto_close = uwsgi_proto_base_close;
}

void uwsgi_proto_scgi_nph_setup(struct uwsgi_socket *uwsgi_sock) {
                        uwsgi_sock->proto = uwsgi_proto_scgi_parser;
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
}

