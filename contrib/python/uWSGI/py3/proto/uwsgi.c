#include <contrib/python/uWSGI/py3/config.h>
/* async uwsgi protocol parser */

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

static int uwsgi_proto_uwsgi_parser(struct wsgi_request *wsgi_req) {
	char *ptr = (char *) wsgi_req->uh;
	ssize_t len = read(wsgi_req->fd, ptr + wsgi_req->proto_parser_pos, (uwsgi.buffer_size + 4) - wsgi_req->proto_parser_pos);
	if (len > 0) {
		wsgi_req->proto_parser_pos += len;
		if (wsgi_req->proto_parser_pos >= 4) {
#ifdef __BIG_ENDIAN__
			wsgi_req->uh->pktsize = uwsgi_swap16(wsgi_req->uh->pktsize);
#endif
			if ((wsgi_req->proto_parser_pos - 4) == wsgi_req->uh->pktsize) {
				return UWSGI_OK;
			}
			if ((wsgi_req->proto_parser_pos - 4) > wsgi_req->uh->pktsize) {
				wsgi_req->proto_parser_remains = wsgi_req->proto_parser_pos - (4 + wsgi_req->uh->pktsize);
				wsgi_req->proto_parser_remains_buf = wsgi_req->buffer + wsgi_req->uh->pktsize;
				return UWSGI_OK;
			}
			if (wsgi_req->uh->pktsize > uwsgi.buffer_size) {
				uwsgi_log("invalid request block size: %u (max %u)...skip\n", wsgi_req->uh->pktsize, uwsgi.buffer_size);
				return -1;
			}
		}
		return UWSGI_AGAIN;
	}
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
		uwsgi_error("uwsgi_proto_uwsgi_parser()");
		return -1;
	}
	// 0 len
	if (wsgi_req->proto_parser_pos > 0) {
		uwsgi_error("uwsgi_proto_uwsgi_parser()");
	}
	return -1;
}

#ifdef UWSGI_SSL
static int uwsgi_proto_suwsgi_parser(struct wsgi_request *wsgi_req) {
        char *ptr = (char *) wsgi_req->uh;
	int len = -1;
retry:
        len = SSL_read(wsgi_req->ssl, ptr + wsgi_req->proto_parser_pos, (uwsgi.buffer_size + 4) - wsgi_req->proto_parser_pos);
        if (len > 0) {
                wsgi_req->proto_parser_pos += len;
                if (wsgi_req->proto_parser_pos >= 4) {
#ifdef __BIG_ENDIAN__
                        wsgi_req->uh->pktsize = uwsgi_swap16(wsgi_req->uh->pktsize);
#endif
                        if ((wsgi_req->proto_parser_pos - 4) == wsgi_req->uh->pktsize) {
                                return UWSGI_OK;
                        }
                        if ((wsgi_req->proto_parser_pos - 4) > wsgi_req->uh->pktsize) {
                                wsgi_req->proto_parser_remains = wsgi_req->proto_parser_pos - (4 + wsgi_req->uh->pktsize);
                                wsgi_req->proto_parser_remains_buf = wsgi_req->buffer + wsgi_req->uh->pktsize;
                                return UWSGI_OK;
                        }
                        if (wsgi_req->uh->pktsize > uwsgi.buffer_size) {
                                uwsgi_log("invalid request block size: %u (max %u)...skip\n", wsgi_req->uh->pktsize, uwsgi.buffer_size);
                                return -1;
                        }
                }
                return UWSGI_AGAIN;
        }
	else if (len == 0) goto empty;
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
                	uwsgi_error("uwsgi_proto_suwsgi_parser()/SSL_read()");
        }

        return -1;
empty:
        // 0 len
        if (wsgi_req->proto_parser_pos > 0) {
                uwsgi_error("uwsgi_proto_uwsgi_parser()");
        }
        return -1;
}

#endif

/*
int uwsgi_proto_uwsgi_parser_unix(struct wsgi_request *wsgi_req) {

	uint8_t *hdr_buf = (uint8_t *) & wsgi_req->uh;
	ssize_t len;

	struct iovec iov[1];
	struct cmsghdr *cmsg;

	if (wsgi_req->proto_parser_status == PROTO_STATUS_RECV_HDR) {


		if (wsgi_req->proto_parser_pos > 0) {
			len = read(wsgi_req->fd, hdr_buf + wsgi_req->proto_parser_pos, 4 - wsgi_req->proto_parser_pos);
		}
		else {
			iov[0].iov_base = hdr_buf;
			iov[0].iov_len = 4;

			wsgi_req->msg.msg_name = NULL;
			wsgi_req->msg.msg_namelen = 0;
			wsgi_req->msg.msg_iov = iov;
			wsgi_req->msg.msg_iovlen = 1;
			wsgi_req->msg.msg_control = &wsgi_req->msg_control;
			wsgi_req->msg.msg_controllen = sizeof(wsgi_req->msg_control);
			wsgi_req->msg.msg_flags = 0;

			len = recvmsg(wsgi_req->fd, &wsgi_req->msg, 0);
		}

		if (len <= 0) {
			if (len < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
					return UWSGI_AGAIN;
				}
			}
			// ignore empty packets
			if (len == 0 && wsgi_req->proto_parser_pos == 0) return -3;
			uwsgi_error(wsgi_req->proto_parser_pos > 0 ? "read()" : "recvmsg()");
			return -1;
		}
		wsgi_req->proto_parser_pos += len;
		// header ready ?
		if (wsgi_req->proto_parser_pos == 4) {
			wsgi_req->proto_parser_status = PROTO_STATUS_RECV_VARS;
			wsgi_req->proto_parser_pos = 0;
#ifdef __BIG_ENDIAN__
			wsgi_req->uh->pktsize = uwsgi_swap16(wsgi_req->uh->pktsize);
#endif

#ifdef UWSGI_DEBUG
			uwsgi_debug("uwsgi payload size: %d (0x%X) modifier1: %d modifier2: %d\n", wsgi_req->uh.pktsize, wsgi_req->uh.pktsize, wsgi_req->uh.modifier1, wsgi_req->uh.modifier2);
#endif

			if (wsgi_req->uh->pktsize > uwsgi.buffer_size) {
				return -1;
			}

			if (!wsgi_req->uh->pktsize)
				return UWSGI_OK;

		}
		return UWSGI_AGAIN;
	}

	else if (wsgi_req->proto_parser_status == PROTO_STATUS_RECV_VARS) {
		len = read(wsgi_req->fd, wsgi_req->buffer + wsgi_req->proto_parser_pos, wsgi_req->uh->pktsize - wsgi_req->proto_parser_pos);
		if (len <= 0) {
			uwsgi_error("read()");
			return -1;
		}
		wsgi_req->proto_parser_pos += len;

		// body ready ?
		if (wsgi_req->proto_parser_pos >= wsgi_req->uh->pktsize) {

			// older OSX versions make mess with CMSG_FIRSTHDR
#ifdef __APPLE__
			if (!wsgi_req->msg.msg_controllen)
				return UWSGI_OK;
#endif

			if (uwsgi.no_fd_passing)
				return UWSGI_OK;

			cmsg = CMSG_FIRSTHDR(&wsgi_req->msg);
			while (cmsg != NULL) {
				if (cmsg->cmsg_len == CMSG_LEN(sizeof(int)) && cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type && SCM_RIGHTS) {

					// upgrade connection to the new socket
#ifdef UWSGI_DEBUG
					uwsgi_log("upgrading fd %d to ", wsgi_req->fd);
#endif
					close(wsgi_req->fd);
					memcpy(&wsgi_req->fd, CMSG_DATA(cmsg), sizeof(int));
#ifdef UWSGI_DEBUG
					uwsgi_log("%d\n", wsgi_req->fd);
#endif
				}
				cmsg = CMSG_NXTHDR(&wsgi_req->msg, cmsg);
			}

			return UWSGI_OK;
		}
		return UWSGI_AGAIN;
	}

	// never here

	return -1;
}

*/

void uwsgi_proto_uwsgi_setup(struct uwsgi_socket *uwsgi_sock) {
	uwsgi_sock->proto = uwsgi_proto_uwsgi_parser;
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

#ifdef UWSGI_SSL
void uwsgi_proto_suwsgi_setup(struct uwsgi_socket *uwsgi_sock) {
        uwsgi_sock->proto = uwsgi_proto_suwsgi_parser;
        uwsgi_sock->proto_accept = uwsgi_proto_ssl_accept;
        uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
        uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
        uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
        uwsgi_sock->proto_read_body = uwsgi_proto_ssl_read_body;
        uwsgi_sock->proto_write = uwsgi_proto_ssl_write;
        uwsgi_sock->proto_write_headers = uwsgi_proto_ssl_write;
        uwsgi_sock->proto_sendfile = uwsgi_proto_ssl_sendfile;
        uwsgi_sock->proto_close = uwsgi_proto_ssl_close;
        if (uwsgi.offload_threads > 0)
                uwsgi_sock->can_offload = 1;
}
#endif
