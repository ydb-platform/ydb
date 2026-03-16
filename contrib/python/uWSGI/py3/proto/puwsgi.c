#include <contrib/python/uWSGI/py3/config.h>
/* async uwsgi protocol parser */

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

perfect framing is required in persistent mode, so we need at least 2 syscall to assemble
a uwsgi header: 4 bytes header + payload

increase write_errors on error to force socket close

*/

int uwsgi_proto_puwsgi_parser(struct wsgi_request *wsgi_req) {
	ssize_t len;
	char *ptr = (char *) wsgi_req->uh;
	if (wsgi_req->proto_parser_pos < 4) {
		len = read(wsgi_req->fd, ptr + wsgi_req->proto_parser_pos, 4 - wsgi_req->proto_parser_pos);
		if (len > 0) {
			wsgi_req->proto_parser_pos += len;
			if (wsgi_req->proto_parser_pos == 4) {
#ifdef __BIG_ENDIAN__
                        	wsgi_req->uh->pktsize = uwsgi_swap16(wsgi_req->uh->pktsize);
#endif
				if (wsgi_req->uh->pktsize > uwsgi.buffer_size) {
                                	uwsgi_log("invalid request block size: %u (max %u)...skip\n", wsgi_req->uh->pktsize, uwsgi.buffer_size);
					wsgi_req->write_errors++;		
                                	return -1;
                        	}
			}
			return UWSGI_AGAIN;
		}
		goto negative;
	}
	len = read(wsgi_req->fd, ptr + wsgi_req->proto_parser_pos, wsgi_req->uh->pktsize - (wsgi_req->proto_parser_pos-4));
	if (len > 0) {
		wsgi_req->proto_parser_pos += len;
		if ((wsgi_req->proto_parser_pos-4) == wsgi_req->uh->pktsize) {
			return UWSGI_OK;	
		}
		return UWSGI_AGAIN;
	}
negative:
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			return UWSGI_AGAIN;
		}
		uwsgi_error("uwsgi_proto_uwsgi_parser()");	
		wsgi_req->write_errors++;		
		return -1;
	}
	// 0 len
	if (wsgi_req->proto_parser_pos > 0) {
		uwsgi_error("uwsgi_proto_uwsgi_parser()");	
	}
	wsgi_req->write_errors++;		
	return -1;
}

/*
close the connection on errors, otherwise force edge triggering
*/
void uwsgi_proto_puwsgi_close(struct wsgi_request *wsgi_req) {
	// check for errors or incomplete packets
	if (wsgi_req->write_errors || (size_t) (wsgi_req->uh->pktsize + 4) != wsgi_req->proto_parser_pos) {
		close(wsgi_req->fd);
		wsgi_req->socket->retry[wsgi_req->async_id] = 0;
		wsgi_req->socket->fd_threads[wsgi_req->async_id] = -1;
	}
	else {
		wsgi_req->socket->retry[wsgi_req->async_id] = 1;
		wsgi_req->socket->fd_threads[wsgi_req->async_id] = wsgi_req->fd;
	}
}

int uwsgi_proto_puwsgi_accept(struct wsgi_request *wsgi_req, int fd) {
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

void uwsgi_proto_puwsgi_setup(struct uwsgi_socket *uwsgi_sock) {
                        uwsgi_sock->proto = uwsgi_proto_puwsgi_parser;
                        uwsgi_sock->proto_accept = uwsgi_proto_puwsgi_accept;
                        uwsgi_sock->proto_prepare_headers = uwsgi_proto_base_prepare_headers;
                        uwsgi_sock->proto_add_header = uwsgi_proto_base_add_header;
                        uwsgi_sock->proto_fix_headers = uwsgi_proto_base_fix_headers;
                        uwsgi_sock->proto_read_body = uwsgi_proto_noop_read_body;
                        uwsgi_sock->proto_write = uwsgi_proto_base_write;
                        uwsgi_sock->proto_writev = uwsgi_proto_base_writev;
                        uwsgi_sock->proto_write_headers = uwsgi_proto_base_write;
                        uwsgi_sock->proto_sendfile = uwsgi_proto_base_sendfile;
                        uwsgi_sock->proto_close = uwsgi_proto_puwsgi_close;
                        uwsgi_sock->fd_threads = uwsgi_malloc(sizeof(int) * uwsgi.cores);
                        memset(uwsgi_sock->fd_threads, -1, sizeof(int) * uwsgi.cores);
                        uwsgi_sock->retry = uwsgi_calloc(sizeof(int) * uwsgi.cores);
                        uwsgi.is_et = 1;
                }
