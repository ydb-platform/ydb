#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

int uwsgi_proto_raw_parser(struct wsgi_request *wsgi_req) {
	wsgi_req->is_raw = 1;
	wsgi_req->uh->modifier1 = uwsgi.raw_modifier1;
	wsgi_req->uh->modifier2 = uwsgi.raw_modifier2;
	return UWSGI_OK;
}

uint16_t proto_base_add_uwsgi_header(struct wsgi_request *wsgi_req, char *key, uint16_t keylen, char *val, uint16_t vallen) {


	int i;
	char *buffer = wsgi_req->buffer + wsgi_req->uh->pktsize;
	char *watermark = wsgi_req->buffer + uwsgi.buffer_size;
	char *ptr = buffer;


	for (i = 0; i < keylen; i++) {
		if (key[i] == '-') {
			key[i] = '_';
		}
		else {
			key[i] = toupper((int)key[i]);
		}
	}

	if (uwsgi_strncmp("CONTENT_TYPE", 12, key, keylen) && uwsgi_strncmp("CONTENT_LENGTH", 14, key, keylen)) {
		if (buffer + keylen + vallen + 2 + 2 + 5 >= watermark) {
			uwsgi_log("[WARNING] unable to add %.*s=%.*s to uwsgi packet, consider increasing buffer size\n", keylen, key, vallen, val);
			return 0;
		}
		*ptr++ = (uint8_t) ((keylen + 5) & 0xff);
		*ptr++ = (uint8_t) (((keylen + 5) >> 8) & 0xff);
		memcpy(ptr, "HTTP_", 5);
		ptr += 5;
		memcpy(ptr, key, keylen);
		ptr += keylen;
		keylen += 5;
	}
	else {
		if (buffer + keylen + vallen + 2 + 2 >= watermark) {
			uwsgi_log("[WARNING] unable to add %.*s=%.*s to uwsgi packet, consider increasing buffer size\n", keylen, key, vallen, val);
			return 0;
		}
		*ptr++ = (uint8_t) (keylen & 0xff);
		*ptr++ = (uint8_t) ((keylen >> 8) & 0xff);
		memcpy(ptr, key, keylen);
		ptr += keylen;
	}

	*ptr++ = (uint8_t) (vallen & 0xff);
	*ptr++ = (uint8_t) ((vallen >> 8) & 0xff);
	memcpy(ptr, val, vallen);

#ifdef UWSGI_DEBUG
	uwsgi_log("add uwsgi var: %.*s = %.*s\n", keylen, key, vallen, val);
#endif

	return keylen + vallen + 2 + 2;
}



uint16_t proto_base_add_uwsgi_var(struct wsgi_request * wsgi_req, char *key, uint16_t keylen, char *val, uint16_t vallen) {


	char *buffer = wsgi_req->buffer + wsgi_req->uh->pktsize;
	char *watermark = wsgi_req->buffer + uwsgi.buffer_size;
	char *ptr = buffer;

	if (buffer + keylen + vallen + 2 + 2 >= watermark) {
		uwsgi_log("[WARNING] unable to add %.*s=%.*s to uwsgi packet, consider increasing buffer size\n", keylen, key, vallen, val);
		return 0;
	}


	*ptr++ = (uint8_t) (keylen & 0xff);
	*ptr++ = (uint8_t) ((keylen >> 8) & 0xff);
	memcpy(ptr, key, keylen);
	ptr += keylen;

	*ptr++ = (uint8_t) (vallen & 0xff);
	*ptr++ = (uint8_t) ((vallen >> 8) & 0xff);
	memcpy(ptr, val, vallen);

#ifdef UWSGI_DEBUG
	uwsgi_log("add uwsgi var: %.*s = %.*s\n", keylen, key, vallen, val);
#endif

	return keylen + vallen + 2 + 2;
}


int uwsgi_proto_base_accept(struct wsgi_request *wsgi_req, int fd) {

	wsgi_req->c_len = sizeof(struct sockaddr_un);
#if defined(__linux__) && defined(SOCK_NONBLOCK) && !defined(OBSOLETE_LINUX_KERNEL)
	return accept4(fd, (struct sockaddr *) &wsgi_req->client_addr, (socklen_t *) & wsgi_req->c_len, SOCK_NONBLOCK);
#elif defined(__linux__)
	int client_fd = accept(fd, (struct sockaddr *) &wsgi_req->client_addr, (socklen_t *) & wsgi_req->c_len);
	if (client_fd >= 0) {
		uwsgi_socket_nb(client_fd);
	}
	return client_fd;
#else
	return accept(fd, (struct sockaddr *) &wsgi_req->client_addr, (socklen_t *) & wsgi_req->c_len);
#endif
}

void uwsgi_proto_base_close(struct wsgi_request *wsgi_req) {
	close(wsgi_req->fd);
}

#ifdef UWSGI_SSL
int uwsgi_proto_ssl_accept(struct wsgi_request *wsgi_req, int server_fd) {

	int fd = uwsgi_proto_base_accept(wsgi_req, server_fd);
	if (fd >= 0) {
		wsgi_req->ssl = SSL_new(wsgi_req->socket->ssl_ctx);
        	SSL_set_fd(wsgi_req->ssl, fd);
        	SSL_set_accept_state(wsgi_req->ssl);
	}
	return fd;
}

void uwsgi_proto_ssl_close(struct wsgi_request *wsgi_req) {
	uwsgi_proto_base_close(wsgi_req);
	// clear the errors (otherwise they could be propagated)
        ERR_clear_error();
        SSL_free(wsgi_req->ssl);
}
#endif

struct uwsgi_buffer *uwsgi_proto_base_add_header(struct wsgi_request *wsgi_req, char *k, uint16_t kl, char *v, uint16_t vl) {
	struct uwsgi_buffer *ub = NULL;
	if (kl > 0) {
		ub = uwsgi_buffer_new(kl + 2 + vl + 2);
		if (uwsgi_buffer_append(ub, k, kl)) goto end;
		if (uwsgi_buffer_append(ub, ": ", 2)) goto end;
		if (uwsgi_buffer_append(ub, v, vl)) goto end;
		if (uwsgi_buffer_append(ub, "\r\n", 2)) goto end;
	}
	else {
		ub = uwsgi_buffer_new(vl + 2);
		if (uwsgi_buffer_append(ub, v, vl)) goto end;
                if (uwsgi_buffer_append(ub, "\r\n", 2)) goto end;
	}
	return ub;
end:
	uwsgi_buffer_destroy(ub);
	return NULL;
}

struct uwsgi_buffer *uwsgi_proto_base_prepare_headers(struct wsgi_request *wsgi_req, char *s, uint16_t sl) {
        struct uwsgi_buffer *ub = NULL;
	if (uwsgi.cgi_mode == 0) {
		if (wsgi_req->protocol_len) {
			ub = uwsgi_buffer_new(wsgi_req->protocol_len + 1 + sl + 2);
			if (uwsgi_buffer_append(ub, wsgi_req->protocol, wsgi_req->protocol_len)) goto end;
			if (uwsgi_buffer_append(ub, " ", 1)) goto end;
		}
		else {
			ub = uwsgi_buffer_new(9 + sl + 2);
			if (uwsgi_buffer_append(ub, "HTTP/1.0 ", 9)) goto end;
		}
	}
	else {
		ub = uwsgi_buffer_new(8 + sl + 2);
		if (uwsgi_buffer_append(ub, "Status: ", 8)) goto end;
	}
        if (uwsgi_buffer_append(ub, s, sl)) goto end;
	if (uwsgi_buffer_append(ub, "\r\n", 2)) goto end;
        return ub;
end:
        uwsgi_buffer_destroy(ub);
        return NULL;
}

struct uwsgi_buffer *uwsgi_proto_base_cgi_prepare_headers(struct wsgi_request *wsgi_req, char *s, uint16_t sl) {
	struct uwsgi_buffer *ub = uwsgi_buffer_new(8 + sl + 2);
	if (uwsgi_buffer_append(ub, "Status: ", 8)) goto end;
        if (uwsgi_buffer_append(ub, s, sl)) goto end;
	if (uwsgi_buffer_append(ub, "\r\n", 2)) goto end;
	return ub;	
end:
	uwsgi_buffer_destroy(ub);
	return NULL;
}


int uwsgi_proto_base_write(struct wsgi_request * wsgi_req, char *buf, size_t len) {
        ssize_t wlen = write(wsgi_req->fd, buf+wsgi_req->write_pos, len-wsgi_req->write_pos);
        if (wlen > 0) {
                wsgi_req->write_pos += wlen;
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

/*
	NOTE: len is a pointer as it could be changed on the fly
*/
int uwsgi_proto_base_writev(struct wsgi_request * wsgi_req, struct iovec *iov, size_t *len) {
	size_t i,needed = 0;
	// count the number of bytes to write
	for(i=0;i<*len;i++) needed += iov[i].iov_len;
	ssize_t wlen = writev(wsgi_req->fd, iov, *len);
        if (wlen > 0) {
		wsgi_req->write_pos += wlen;
                if ((size_t)wlen == needed) {
                        return UWSGI_OK;
                }
		// now the complex part, we need to rebuild iovec and len...
		size_t orig_len = *len;
		size_t new_len = orig_len;
		// first remove the consumed items
		size_t first_iov = 0;
		size_t skip_bytes = 0;
		for(i=0;i<orig_len;i++) {
			if (iov[i].iov_len <= (size_t)wlen) {
				wlen -= iov[i].iov_len;
				new_len--;
			}
			else {
				first_iov = i;
				skip_bytes = wlen;
				break;
			}
		}
		*len = new_len;
		// now moves remaining iovec's to top
		size_t pos = 0;
		for(i=first_iov;i<orig_len;i++) {
			if (pos == 0) {
				iov[i].iov_base += skip_bytes;
				iov[i].iov_len -= skip_bytes;
			}
			iov[pos].iov_base = iov[i].iov_base;
			iov[pos].iov_len = iov[i].iov_len;
			pos++;
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


#ifdef UWSGI_SSL
int uwsgi_proto_ssl_write(struct wsgi_request * wsgi_req, char *buf, size_t len) {
	int ret = -1;
retry:
        ret = SSL_write(wsgi_req->ssl, buf, len);
        if (ret > 0) {
		wsgi_req->write_pos += ret;
		if (wsgi_req->write_pos == len) {
                        return UWSGI_OK;
                }
                return UWSGI_AGAIN;
	}

        int err = SSL_get_error(wsgi_req->ssl, ret);

        if (err == SSL_ERROR_WANT_WRITE) {
                ret = uwsgi_wait_write_req(wsgi_req);
                if (ret <= 0) return -1;
                goto retry;
        }

        else if (err == SSL_ERROR_WANT_READ) {
                ret = uwsgi_wait_read_req(wsgi_req);
                if (ret <= 0) return -1;
                goto retry;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_error("uwsgi_proto_ssl_write()/SSL_write()");
        }

        return -1;
}

#endif
int uwsgi_proto_base_sendfile(struct wsgi_request * wsgi_req, int fd, size_t pos, size_t len) {
        ssize_t wlen = uwsgi_sendfile_do(wsgi_req->fd, fd, pos+wsgi_req->write_pos, len-wsgi_req->write_pos);
        if (wlen > 0) {
                wsgi_req->write_pos += wlen;
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

#ifdef UWSGI_SSL
int uwsgi_proto_ssl_sendfile(struct wsgi_request *wsgi_req, int fd, size_t pos, size_t len) {
	char buf[32768];

	if (lseek(fd, pos+wsgi_req->write_pos, SEEK_SET) < 0) {
		uwsgi_error("lseek()");
		return -1;
	}
	ssize_t rlen = read(fd, buf, UMIN(len-wsgi_req->write_pos, 32768));
	if (rlen <= 0) return -1;

	char *rbuf = buf;

	while(rlen > 0) {
		size_t current_write_pos = wsgi_req->write_pos;
		int ret = uwsgi_proto_ssl_write(wsgi_req, rbuf, rlen);
		if (ret == UWSGI_OK) {
			break;
                }
		if (ret == UWSGI_AGAIN) {
			rbuf += (wsgi_req->write_pos - current_write_pos);
			rlen -= (wsgi_req->write_pos - current_write_pos);
			continue;
		}
		return -1;
	}

	if (wsgi_req->write_pos == len) {
		return UWSGI_OK;
	}
	return UWSGI_AGAIN;
}

#endif

int uwsgi_proto_base_fix_headers(struct wsgi_request * wsgi_req) {
        return uwsgi_buffer_append(wsgi_req->headers, "\r\n", 2);
}

ssize_t uwsgi_proto_base_read_body(struct wsgi_request *wsgi_req, char *buf, size_t len) {
	if (wsgi_req->proto_parser_remains > 0) {
		size_t remains = UMIN(wsgi_req->proto_parser_remains, len);
		memcpy(buf, wsgi_req->proto_parser_remains_buf, remains);
		wsgi_req->proto_parser_remains -= remains;
		wsgi_req->proto_parser_remains_buf += remains;
		return remains;
	}
	return read(wsgi_req->fd, buf, len);
}

#ifdef UWSGI_SSL
ssize_t uwsgi_proto_ssl_read_body(struct wsgi_request *wsgi_req, char *buf, size_t len) {
	int ret = -1;
        if (wsgi_req->proto_parser_remains > 0) {
                size_t remains = UMIN(wsgi_req->proto_parser_remains, len);
                memcpy(buf, wsgi_req->proto_parser_remains_buf, remains);
                wsgi_req->proto_parser_remains -= remains;
                wsgi_req->proto_parser_remains_buf += remains;
                return remains;
        }

retry:
	ret = SSL_read(wsgi_req->ssl, buf, len);
        if (ret > 0) return ret;

        int err = SSL_get_error(wsgi_req->ssl, ret);

        if (err == SSL_ERROR_WANT_READ) {
		errno = EAGAIN;
                return -1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
		ret = uwsgi_wait_write_req(wsgi_req);
                if (ret <= 0) return -1;
		goto retry;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_error("uwsgi_proto_ssl_read_body()/SSL_read()");
        }

	return -1;
}
#endif

ssize_t uwsgi_proto_noop_read_body(struct wsgi_request *wsgi_req, char *buf, size_t len) {
	uwsgi_log_verbose("!!! the current protocol does not support request body !!!\n");
	return -1;
}

void uwsgi_proto_raw_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_raw_parser;
                        uwsgi_sock->proto_accept = uwsgi_proto_base_accept;
                        uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
                        uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
                        uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
                        uwsgi_sock->proto_read_body = uwsgi_proto_base_read_body;
                        uwsgi_sock->proto_write = uwsgi_proto_base_write;
                        uwsgi_sock->proto_write_headers = uwsgi_proto_base_write;
                        uwsgi_sock->proto_sendfile = uwsgi_proto_base_sendfile;
                        uwsgi_sock->proto_close = uwsgi_proto_base_close;
                        if (uwsgi.offload_threads > 0)
                                uwsgi_sock->can_offload = 1;
}
