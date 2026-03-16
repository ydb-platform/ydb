#include <contrib/python/uWSGI/py2/config.h>
/* async fastcgi protocol parser */

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

#define FCGI_END_REQUEST "\1\x06\0\1\0\0\0\0\1\3\0\1\0\x08\0\0\0\0\0\0\0\0\0\0"

struct fcgi_record {
	uint8_t version;
	uint8_t type;
	uint8_t req1;
	uint8_t req0;
	uint8_t cl1;
	uint8_t cl0;
	uint8_t pad;
	uint8_t reserved;
} __attribute__ ((__packed__));


// convert fastcgi params to uwsgi key/val
int fastcgi_to_uwsgi(struct wsgi_request *wsgi_req, char *buf, size_t len) {
	size_t j;
	uint8_t octet;
	uint32_t keylen, vallen;
	for (j = 0; j < len; j++) {
		octet = (uint8_t) buf[j];
		if (octet > 127) {
			if (j + 4 >= len)
				return -1;
			keylen = uwsgi_be32(&buf[j]) ^ 0x80000000;
			j += 4;
		}
		else {
			if (j + 1 >= len)
				return -1;
			keylen = octet;
			j++;
		}
		octet = (uint8_t) buf[j];
		if (octet > 127) {
			if (j + 4 >= len)
				return -1;
			vallen = uwsgi_be32(&buf[j]) ^ 0x80000000;
			j += 4;
		}
		else {
			if (j + 1 >= len)
				return -1;
			vallen = octet;
			j++;
		}

		if (j + (keylen + vallen) > len) {
			return -1;
		}

		if (keylen > 0xffff || vallen > 0xffff)
			return -1;
		uint16_t pktsize = proto_base_add_uwsgi_var(wsgi_req, buf + j, keylen, buf + j + keylen, vallen);
		if (pktsize == 0)
			return -1;
		wsgi_req->uh->pktsize += pktsize;
		// -1 here as the for() will increment j again
		j += (keylen + vallen) - 1;
	}

	return 0;
}


/*

	each fastcgi packet is composed by a header and a body
	the parser rebuild a whole packet until it finds a 0 STDIN

*/

int uwsgi_proto_fastcgi_parser(struct wsgi_request *wsgi_req) {

	// allocate space for a fastcgi record
	if (!wsgi_req->proto_parser_buf) {
		wsgi_req->proto_parser_buf = uwsgi_malloc(uwsgi.buffer_size);
		wsgi_req->proto_parser_buf_size = uwsgi.buffer_size;
	}

	ssize_t len = read(wsgi_req->fd, wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos, wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos);
	if (len > 0) {
		goto parse;
	}
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
		uwsgi_error("uwsgi_proto_fastcgi_parser()");
		return -1;
	}
	// mute on 0 len...
	if (wsgi_req->proto_parser_pos > 0) {
		uwsgi_error("uwsgi_proto_fastcgi_parser()");
	}
	return -1;
parse:
	wsgi_req->proto_parser_pos += len;
	// ok let's see what we need to do
	for (;;) {
		if (wsgi_req->proto_parser_pos >= sizeof(struct fcgi_record)) {
			struct fcgi_record *fr = (struct fcgi_record *) wsgi_req->proto_parser_buf;
			uint16_t fcgi_len = uwsgi_be16((char *) &fr->cl1);
			uint32_t fcgi_all_len = sizeof(struct fcgi_record) + fcgi_len + fr->pad;
			uint8_t fcgi_type = fr->type;
			uint8_t *sid = (uint8_t *) & wsgi_req->stream_id;
			sid[0] = fr->req0;
			sid[1] = fr->req1;
			// if STDIN, end of the loop
			if (fcgi_type == 5) {
				wsgi_req->uh->modifier1 = uwsgi.fastcgi_modifier1;
				wsgi_req->uh->modifier2 = uwsgi.fastcgi_modifier2;
				// does the request stream ended ?
				if (fcgi_len == 0) wsgi_req->proto_parser_eof = 1;
				return UWSGI_OK;
			}
			// if we have a full packet, parse it and reset the memory
			if (wsgi_req->proto_parser_pos >= fcgi_all_len) {
				// PARAMS ? (ignore other types)
				if (fcgi_type == 4) {
					if (fastcgi_to_uwsgi(wsgi_req, wsgi_req->proto_parser_buf + sizeof(struct fcgi_record), fcgi_len)) {
						return -1;
					}
				}
				memmove(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf + fcgi_all_len, wsgi_req->proto_parser_pos - fcgi_all_len);
				wsgi_req->proto_parser_pos -= fcgi_all_len;
			}
			else if (fcgi_all_len > wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos) {
				char *tmp_buf = realloc(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf_size + fcgi_all_len - (wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos));
				if (!tmp_buf) {
					uwsgi_error("uwsgi_proto_fastcgi_parser()/realloc()");
					return -1;
				}
				wsgi_req->proto_parser_buf = tmp_buf;
				wsgi_req->proto_parser_buf_size += (fcgi_all_len - (wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos));
				break;
			}
			else {
				break;
			}
		}
		else {
			break;
		}
	}
	return UWSGI_AGAIN;

}


ssize_t uwsgi_proto_fastcgi_read_body(struct wsgi_request * wsgi_req, char *buf, size_t len) {
	int has_read = 0;
	if (wsgi_req->proto_parser_remains > 0) {
		size_t remains = UMIN(wsgi_req->proto_parser_remains, len);
		memcpy(buf, wsgi_req->proto_parser_remains_buf, remains);
		wsgi_req->proto_parser_remains -= remains;
		wsgi_req->proto_parser_remains_buf += remains;
		// we consumed all of the body, we can safely move the memory
		if (wsgi_req->proto_parser_remains == 0 && wsgi_req->proto_parser_move) {
			memmove(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf + wsgi_req->proto_parser_move, wsgi_req->proto_parser_pos);
			wsgi_req->proto_parser_move = 0;
		}
		return remains;
	}

	// if we already have seen eof, return 0
	if (wsgi_req->proto_parser_eof) return 0;

	ssize_t rlen;

	for (;;) {
		if (wsgi_req->proto_parser_pos >= sizeof(struct fcgi_record)) {
			struct fcgi_record *fr = (struct fcgi_record *) wsgi_req->proto_parser_buf;
			uint16_t fcgi_len = uwsgi_be16((char *) &fr->cl1);
			uint32_t fcgi_all_len = sizeof(struct fcgi_record) + fcgi_len + fr->pad;
			uint8_t fcgi_type = fr->type;
			// if we have a full packet, parse it and reset the memory
			if (wsgi_req->proto_parser_pos >= fcgi_all_len) {
				// STDIN ? (ignore other types)
				if (fcgi_type == 5) {
					// EOF ?
					if (fcgi_len == 0) {
						wsgi_req->proto_parser_eof = 1;
						return 0;	
					}
					// copy data to the buf
					size_t remains = UMIN(fcgi_len, len);
					memcpy(buf, wsgi_req->proto_parser_buf + sizeof(struct fcgi_record), remains);
					// copy remaining
					wsgi_req->proto_parser_remains = fcgi_len - remains;
					wsgi_req->proto_parser_remains_buf = wsgi_req->proto_parser_buf + sizeof(struct fcgi_record) + remains;
					// we consumed all of the body, we can safely move the memory
					if (wsgi_req->proto_parser_remains == 0) {
						memmove(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf + fcgi_all_len, wsgi_req->proto_parser_pos - fcgi_all_len);
					}
					else {
						// postpone memory move
						wsgi_req->proto_parser_move = fcgi_all_len;
					}
					wsgi_req->proto_parser_pos -= fcgi_all_len;
					return remains;
				}
				memmove(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf + fcgi_all_len, wsgi_req->proto_parser_pos - fcgi_all_len);
				wsgi_req->proto_parser_pos -= fcgi_all_len;
			}
			else if (fcgi_all_len > wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos) {
				char *tmp_buf = realloc(wsgi_req->proto_parser_buf, wsgi_req->proto_parser_buf_size + fcgi_all_len - (wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos));
				if (!tmp_buf) {
					uwsgi_error("uwsgi_proto_fastcgi_read_body()/realloc()");
					return -1;
				}
				wsgi_req->proto_parser_buf = tmp_buf;
				wsgi_req->proto_parser_buf_size += fcgi_all_len - (wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos);
			}
			if (!has_read)
				goto gather;
			errno = EAGAIN;
			return -1;
		}
		else {
gather:
			rlen = read(wsgi_req->fd, wsgi_req->proto_parser_buf + wsgi_req->proto_parser_pos, wsgi_req->proto_parser_buf_size - wsgi_req->proto_parser_pos);
			if (rlen > 0) {
				has_read = 1;
				wsgi_req->proto_parser_pos += rlen;
				continue;
			}
			return rlen;
		}
	}

	return -1;

}

// write a STDOUT packet
int uwsgi_proto_fastcgi_write(struct wsgi_request *wsgi_req, char *buf, size_t len) {

	// fastcgi packets are limited to 64k
	if (wsgi_req->proto_parser_status == 0) {
		uint16_t fcgi_len = UMIN(len - wsgi_req->write_pos, 0xffff);
		wsgi_req->proto_parser_status = fcgi_len;
		struct fcgi_record fr;
		fr.version = 1;
		fr.type = 6;
		uint8_t *sid = (uint8_t *) & wsgi_req->stream_id;
		fr.req1 = sid[1];
		fr.req0 = sid[0];
		fr.pad = 0;
		fr.reserved = 0;
		fr.cl0 = (uint8_t) (fcgi_len & 0xff);
		fr.cl1 = (uint8_t) ((fcgi_len >> 8) & 0xff);
		if (uwsgi_write_true_nb(wsgi_req->fd, (char *) &fr, sizeof(struct fcgi_record), uwsgi.socket_timeout)) {
			return -1;
		}
	}

	ssize_t wlen = write(wsgi_req->fd, buf + wsgi_req->write_pos, wsgi_req->proto_parser_status);
	if (wlen > 0) {
		wsgi_req->write_pos += wlen;
		wsgi_req->proto_parser_status -= wlen;
		if (wsgi_req->write_pos == len) {
			return UWSGI_OK;
		}
		return UWSGI_AGAIN;
	}
	if (wlen < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
	}
	return -1;

}

void uwsgi_proto_fastcgi_close(struct wsgi_request *wsgi_req) {
	// before sending the END_REQUEST and closing the connection we need to check for EOF
	if (!wsgi_req->proto_parser_eof) {
		// we use a custom tiny buffer, all the data will be discarded...
		char buf[4096];
		for(;;) {
			ssize_t rlen = uwsgi_proto_fastcgi_read_body(wsgi_req, buf, 4096);
			if (rlen < 0) {
				if (uwsgi_is_again()) {
					int ret = uwsgi.wait_read_hook(wsgi_req->fd, uwsgi.socket_timeout);
					if (ret <= 0) goto end;
					continue;
				}
				goto end;
			}
			if (rlen == 0) break;
		}
	}
	// special case here, we run in void context, so we need to wait directly here
	char end_request[24];
	memcpy(end_request, FCGI_END_REQUEST, 24);
	char *sid = (char *) &wsgi_req->stream_id;
	// update with request id
	end_request[2] = sid[1];
	end_request[3] = sid[0];
	end_request[10] = sid[1];
	end_request[11] = sid[0];
	(void) uwsgi_write_true_nb(wsgi_req->fd, end_request, 24, uwsgi.socket_timeout);
end:
	uwsgi_proto_base_close(wsgi_req);
}

int uwsgi_proto_fastcgi_sendfile(struct wsgi_request *wsgi_req, int fd, size_t pos, size_t len) {

	// fastcgi packets are limited to 64k
	if (wsgi_req->proto_parser_status == 0) {
		uint16_t fcgi_len = (uint16_t) UMIN(len - wsgi_req->write_pos, 0xffff);
		wsgi_req->proto_parser_status = fcgi_len;
		struct fcgi_record fr;
		fr.version = 1;
		fr.type = 6;
		uint8_t *sid = (uint8_t *) & wsgi_req->stream_id;
		fr.req1 = sid[1];
		fr.req0 = sid[0];
		fr.pad = 0;
		fr.reserved = 0;
		fr.cl0 = (uint8_t) (fcgi_len & 0xff);
		fr.cl1 = (uint8_t) ((fcgi_len >> 8) & 0xff);
		if (uwsgi_write_true_nb(wsgi_req->fd, (char *) &fr, sizeof(struct fcgi_record), uwsgi.socket_timeout)) {
			return -1;
		}
	}

	ssize_t wlen = uwsgi_sendfile_do(wsgi_req->fd, fd, pos + wsgi_req->write_pos, wsgi_req->proto_parser_status);
	if (wlen > 0) {
		wsgi_req->write_pos += wlen;
		wsgi_req->proto_parser_status -= wlen;
		if (wsgi_req->write_pos == len) {
			return UWSGI_OK;
		}
		return UWSGI_AGAIN;
	}
	if (wlen < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
	}
	return -1;
}

void uwsgi_proto_fastcgi_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_fastcgi_parser;
	uwsgi_sock->proto_accept = uwsgi_proto_base_accept;
	uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_cgi_prepare_headers;
	uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
	uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
	uwsgi_sock->proto_read_body = uwsgi_proto_fastcgi_read_body;
	uwsgi_sock->proto_write = uwsgi_proto_fastcgi_write;
	uwsgi_sock->proto_write_headers = uwsgi_proto_fastcgi_write;
	uwsgi_sock->proto_sendfile = uwsgi_proto_fastcgi_sendfile;
	uwsgi_sock->proto_close = uwsgi_proto_fastcgi_close;
}

void uwsgi_proto_fastcgi_nph_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_fastcgi_parser;
	uwsgi_sock->proto_accept = uwsgi_proto_base_accept;
	uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
	uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
	uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
	uwsgi_sock->proto_read_body = uwsgi_proto_fastcgi_read_body;
	uwsgi_sock->proto_write = uwsgi_proto_fastcgi_write;
	uwsgi_sock->proto_write_headers = uwsgi_proto_fastcgi_write;
	uwsgi_sock->proto_sendfile = uwsgi_proto_fastcgi_sendfile;
	uwsgi_sock->proto_close = uwsgi_proto_fastcgi_close;
}
