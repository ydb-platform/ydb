#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

int uwsgi_simple_wait_read_hook(int fd, int timeout) {
	struct pollfd upoll;
	timeout = timeout * 1000;
	int ret;

        upoll.fd = fd;
        upoll.events = POLLIN;
        upoll.revents = 0;
	for (;;) {
		ret = poll(&upoll, 1, timeout);
		if ((ret < 0) && (errno == EINTR))
			continue;
		break;
        }

        if (ret > 0) {
                if (upoll.revents & POLLIN) {
                        return 1;
                }
		return -1;
        }
        if (ret < 0) {
                uwsgi_error("uwsgi_simple_wait_read_hook()/poll()");
        }

        return ret;
}

int uwsgi_simple_wait_read2_hook(int fd0, int fd1, int timeout, int *fd) {
        struct pollfd upoll[2];
        timeout = timeout * 1000;

        upoll[0].fd = fd0;
        upoll[0].events = POLLIN;
        upoll[0].revents = 0;

        upoll[1].fd = fd1;
        upoll[1].events = POLLIN;
        upoll[1].revents = 0;

        int ret = poll(upoll, 2, timeout);

        if (ret > 0) {
                if (upoll[0].revents & POLLIN) {
			*fd = fd0;
                        return 1;
                }
                if (upoll[1].revents & POLLIN) {
			*fd = fd1;
                        return 1;
                }
                return -1;
        }
        if (ret < 0) {
                uwsgi_error("uwsgi_simple_wait_read_hook2()/poll()");
        }

        return ret;
}


/*
	seek()/rewind() language-independent implementations.
*/

void uwsgi_request_body_seek(struct wsgi_request *wsgi_req, off_t pos) {
	if (wsgi_req->post_file) {
		if (pos < 0) {
			if (fseek(wsgi_req->post_file, pos, SEEK_CUR)) {
                        	uwsgi_req_error("uwsgi_request_body_seek()/fseek()");
				wsgi_req->read_errors++;
                	}
			wsgi_req->post_pos = ftell(wsgi_req->post_file);
			return;
		}

		if (fseek(wsgi_req->post_file, pos, SEEK_SET)) {
			uwsgi_req_error("uwsgi_request_body_seek()/fseek()");
			wsgi_req->read_errors++;
		}
		wsgi_req->post_pos = ftell(wsgi_req->post_file);
		return;
	}

	if (uwsgi.post_buffering) {
		if (pos < 0) {
			if ((off_t) wsgi_req->post_pos - pos < 0) {
				wsgi_req->post_pos = 0;
				return;
			}
			wsgi_req->post_pos -= pos;
			return;
		}
		if (pos >= (off_t) uwsgi.post_buffering) {
			pos = uwsgi.post_buffering - 1;	
		}
		wsgi_req->post_pos = pos;
	}
}

/*

	read() and readline() language-independent implementations.

*/

#define uwsgi_read_error0(x) uwsgi_log("[uwsgi-body-read] Error reading %llu bytes. Content-Length: %llu consumed: %llu left: %llu message: Client closed connection\n",\
                        (unsigned long long) x,\
                        (unsigned long long) wsgi_req->post_cl, (unsigned long long) wsgi_req->post_pos, (unsigned long long) wsgi_req->post_cl-wsgi_req->post_pos);

#define uwsgi_read_error(x) uwsgi_log("[uwsgi-body-read] Error reading %llu bytes. Content-Length: %llu consumed: %llu left: %llu message: %s\n",\
			(unsigned long long) x,\
			(unsigned long long) wsgi_req->post_cl, (unsigned long long) wsgi_req->post_pos, (unsigned long long) wsgi_req->post_cl-wsgi_req->post_pos,\
			strerror(errno));

#define uwsgi_read_timeout(x) uwsgi_log("[uwsgi-body-read] Timeout reading %llu bytes. Content-Length: %llu consumed: %llu left: %llu\n",\
                        (unsigned long long) x,\
                        (unsigned long long) wsgi_req->post_cl, (unsigned long long) wsgi_req->post_pos, (unsigned long long) wsgi_req->post_cl-wsgi_req->post_pos);

static int consume_body_for_readline(struct wsgi_request *wsgi_req) {

	size_t remains = UMIN(uwsgi.buffer_size, wsgi_req->post_cl - wsgi_req->post_pos);

	int ret;

	// allocate more memory if needed
	if (wsgi_req->post_readline_size - wsgi_req->post_readline_watermark == 0) {
		memmove(wsgi_req->post_readline_buf, wsgi_req->post_readline_buf + wsgi_req->post_readline_pos, wsgi_req->post_readline_watermark - wsgi_req->post_readline_pos);
		wsgi_req->post_readline_watermark -= wsgi_req->post_readline_pos;
		wsgi_req->post_readline_pos = 0;
		// still something to use ?
		if (wsgi_req->post_readline_size - wsgi_req->post_readline_watermark < remains) {
        		char *tmp_buf = realloc(wsgi_req->post_readline_buf, wsgi_req->post_readline_size + remains);
                	if (!tmp_buf) {
                		uwsgi_req_error("consume_body_for_readline()/realloc()");
                        	return -1;
                	}
                	wsgi_req->post_readline_buf = tmp_buf;
                	wsgi_req->post_readline_size += remains;
			// INFORM THE USER HIS readline() USAGE IS FOOLISH
			if (!wsgi_req->post_warning && wsgi_req->post_readline_size > (uwsgi.body_read_warning * 1024*1024)) {
				uwsgi_log("[uwsgi-warning] you are using readline() on request body allocating over than %llu MB, that is really bad and can be avoided...\n", (unsigned long long) (wsgi_req->post_readline_size/(1024*1024)));
				wsgi_req->post_warning = 1;
			}
		}
        }

	remains = UMIN(wsgi_req->post_readline_size - wsgi_req->post_readline_watermark, wsgi_req->post_cl - wsgi_req->post_pos);

	// read from a file
	if (wsgi_req->post_file) {
		size_t ret = fread(wsgi_req->post_readline_buf + wsgi_req->post_readline_watermark, remains, 1, wsgi_req->post_file);	
		if (ret == 0) {
			uwsgi_req_error("consume_body_for_readline()/fread()");
			return -1;
		}
		wsgi_req->post_pos += remains;
		wsgi_req->post_readline_watermark += remains;
		return 0;
	}


	// read from post_buffering memory
	if (uwsgi.post_buffering) {
		memcpy(wsgi_req->post_readline_buf + wsgi_req->post_readline_watermark, wsgi_req->post_buffering_buf + wsgi_req->post_pos, remains);
		wsgi_req->post_pos += remains;
		wsgi_req->post_readline_watermark += remains;
		return 0;
	}

	// read from socket
	ssize_t len = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_readline_buf + wsgi_req->post_readline_watermark , remains);
	if (len > 0) {
		wsgi_req->post_pos += len;
		wsgi_req->post_readline_watermark += len;
		return 0;
	}
	if (len == 0) {
		uwsgi_read_error(remains);
		wsgi_req->read_errors++;
		return -1;	
	}
	if (len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
			goto wait;
		}
		uwsgi_read_error(remains);
		wsgi_req->read_errors++;
		return -1;
	}
wait:
	ret = uwsgi_wait_read_req(wsgi_req);
        if (ret > 0) {
        	len = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_readline_buf + wsgi_req->post_readline_watermark , remains);
                if (len > 0) {
			wsgi_req->post_pos += len;
			wsgi_req->post_readline_watermark += len;
			return 0;
		}
		uwsgi_read_error(remains);
		wsgi_req->read_errors++;
                return -1;
	}
        // 0 means timeout
        else if (ret == 0) {
		uwsgi_read_timeout(remains);
		return -1;
        }
	uwsgi_read_error(remains);
	wsgi_req->read_errors++;
        return -1;
}

// TODO take hint into account
// readline_buf is allocated when needed and freed at the end of the request
char *uwsgi_request_body_readline(struct wsgi_request *wsgi_req, ssize_t hint, ssize_t *rlen) {

	// return 0 if no post_cl or pos >= post_cl and no residual data
        if ((!wsgi_req->post_cl || wsgi_req->post_pos >= wsgi_req->post_cl ) && !wsgi_req->post_readline_pos) {
		return uwsgi.empty;
        }

	// some residual data ?
	if (wsgi_req->post_readline_pos > 0) {
		size_t i;
		for(i=wsgi_req->post_readline_pos;i<wsgi_req->post_readline_watermark;i++) {
			// found a newline
			if (wsgi_req->post_readline_buf[i] == '\n') {
				*rlen = (i+1)-wsgi_req->post_readline_pos;
				char *buf = wsgi_req->post_readline_buf + wsgi_req->post_readline_pos;
				wsgi_req->post_readline_pos += *rlen;
				// all the readline buffer has been consumed
				if (wsgi_req->post_readline_pos >= wsgi_req->post_readline_watermark) {
					wsgi_req->post_readline_pos = 0;
					wsgi_req->post_readline_watermark = 0;
				}
				return buf;
			}
		}
		// ok, no newline found, continue below
	}

	// allocate memory on the first round
	if (!wsgi_req->post_readline_buf) {
		size_t amount = UMIN(uwsgi.buffer_size, wsgi_req->post_cl);
		wsgi_req->post_readline_buf = malloc(amount);
		if (!wsgi_req->post_readline_buf) {
			uwsgi_req_error("uwsgi_request_body_readline()/malloc()");
			wsgi_req->read_errors++;
			*rlen = -1;
			return NULL;
		}
		wsgi_req->post_readline_size = amount;
	}

	// ok, no newline found, consume a bit more of memory and retry
        for(;;) {
		// no more data to consume
		if (wsgi_req->post_pos >= wsgi_req->post_cl) break;

                        if (consume_body_for_readline(wsgi_req)) {
				wsgi_req->read_errors++;
                                *rlen = -1;
                                return NULL;
                        }
			size_t i;
                        for(i=wsgi_req->post_readline_pos;i<wsgi_req->post_readline_watermark;i++) {
                                if (wsgi_req->post_readline_buf[i] == '\n') {
                                        *rlen = (i+1)-wsgi_req->post_readline_pos;
                                        char *buf = wsgi_req->post_readline_buf + wsgi_req->post_readline_pos;
                                        wsgi_req->post_readline_pos += *rlen;
                                        if (wsgi_req->post_readline_pos >= wsgi_req->post_readline_watermark) {
                                                wsgi_req->post_readline_pos = 0;
						wsgi_req->post_readline_watermark = 0;
                                        }
                                        return buf;
                                }
                        }
                }

	// no line found, let's return all
        *rlen = wsgi_req->post_readline_watermark - wsgi_req->post_readline_pos;
        char *buf = wsgi_req->post_readline_buf + wsgi_req->post_readline_pos;
	wsgi_req->post_readline_pos = 0;
	return buf;

}

char *uwsgi_request_body_read(struct wsgi_request *wsgi_req, ssize_t hint, ssize_t *rlen) {

	int ret = -1;
	size_t remains = hint;

	// return empty if no post_cl or pos >= post_cl and no residual data
        if ((!wsgi_req->post_cl || wsgi_req->post_pos >= wsgi_req->post_cl ) && !wsgi_req->post_readline_pos) {
		return uwsgi.empty;
        }

        // return the whole input
        if (remains <= 0) {
                remains = wsgi_req->post_cl;
        }

        // some residual data ?
        if (wsgi_req->post_readline_pos > 0) {
                       if (remains <= (wsgi_req->post_readline_watermark - wsgi_req->post_readline_pos)) {
				*rlen = remains;
				char *buf = wsgi_req->post_readline_buf + wsgi_req->post_readline_pos;
				wsgi_req->post_readline_pos += remains;
				return buf;
                        }
			// the hint is higher than residual data, let's copy it to read() memory and go on	
			size_t avail = wsgi_req->post_readline_watermark - wsgi_req->post_readline_pos;
			// check if we have enough memory...
			if (avail > wsgi_req->post_read_buf_size) {
				char *tmp_buf = realloc(wsgi_req->post_read_buf, avail);
				if (!tmp_buf) {
                                	uwsgi_req_error("uwsgi_request_body_read()/realloc()");
                                	*rlen = -1;
					wsgi_req->read_errors++;
                                	return NULL;
                        	}
                        	wsgi_req->post_read_buf = tmp_buf;
                        	wsgi_req->post_read_buf_size = avail;
                        	if (!wsgi_req->post_warning && wsgi_req->post_read_buf_size > (uwsgi.body_read_warning * 1024*1024)) {
                                	uwsgi_log("[uwsgi-warning] you are using read() on request body allocating over than %llu MB, that is really bad and can be avoided...\n", (unsigned long long) (wsgi_req->post_read_buf_size/(1024*1024)));
                                	wsgi_req->post_warning = 1;
                        	}
			}
			// fix remains...
			if (remains > 0) {
				remains -= avail;
			}
			*rlen += avail;
			memcpy(wsgi_req->post_read_buf, wsgi_req->post_readline_buf + wsgi_req->post_readline_pos, avail);
                	wsgi_req->post_readline_pos = 0;
			wsgi_req->post_readline_watermark = 0;
        }

        if (remains + wsgi_req->post_pos > wsgi_req->post_cl) {
                remains = wsgi_req->post_cl - wsgi_req->post_pos;
        }



        if (remains == 0) {
		if (*rlen > 0) {
			return wsgi_req->post_read_buf;
		}
		else {
                	return uwsgi.empty;
		}
        }

	// read from post buffering memory
        if (uwsgi.post_buffering > 0 && !wsgi_req->post_file) {
		*rlen += remains;
                char *buf = wsgi_req->post_buffering_buf+wsgi_req->post_pos;
		wsgi_req->post_pos += remains;
		return buf;
        }

	// ok we need to check if we need to allocate memory
	if (!wsgi_req->post_read_buf) {
		wsgi_req->post_read_buf = malloc(remains);
		if (!wsgi_req->post_read_buf) {
			uwsgi_req_error("uwsgi_request_body_read()/malloc()");
			wsgi_req->read_errors++;
			*rlen = -1;
			return NULL;
		}
		wsgi_req->post_read_buf_size = remains;
	}
	// need to realloc ?
	else {
		if ((remains+*rlen) > wsgi_req->post_read_buf_size) {
			char *tmp_buf = realloc(wsgi_req->post_read_buf, (remains+*rlen));
			if (!tmp_buf) {
				uwsgi_req_error("uwsgi_request_body_read()/realloc()");
				wsgi_req->read_errors++;
				*rlen = -1;
				return NULL;
			}
			wsgi_req->post_read_buf = tmp_buf;
			wsgi_req->post_read_buf_size = (remains+*rlen);
			if (!wsgi_req->post_warning && wsgi_req->post_read_buf_size > (uwsgi.body_read_warning * 1024*1024)) {
                                uwsgi_log("[uwsgi-warning] you are using read() on request body allocating over than %llu MB, that is really bad and can be avoided...\n", (unsigned long long) (wsgi_req->post_read_buf_size/(1024*1024)));
                                wsgi_req->post_warning = 1;
                        }
		}
	}

	// check for disk buffered body first (they are all read in one shot)
	if (wsgi_req->post_file) {
		if (fread(wsgi_req->post_read_buf + *rlen, remains, 1, wsgi_req->post_file) != 1) {
			*rlen = -1;
			uwsgi_req_error("uwsgi_request_body_read()/fread()");
			wsgi_req->read_errors++;
			return NULL;
		}
		*rlen += remains;
		wsgi_req->post_pos+= remains;
		return wsgi_req->post_read_buf;
	}

	// ok read all the required bytes...
	while(remains > 0) {
		// here we first try to read (as data could be already available)
		ssize_t len = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_read_buf + *rlen , remains);
		if (len > 0) {
			wsgi_req->post_pos+=len;
			remains -= len;
			*rlen += len;
			continue;
		}
		// client closed connection...
		if (len == 0) {
			*rlen = -1;
			uwsgi_read_error0(remains);
			return NULL;
		}
		if (len < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
				goto wait;
			}
			*rlen = -1;
			uwsgi_read_error(remains);
			wsgi_req->read_errors++;
			return NULL;
		}
wait:
		ret = uwsgi_wait_read_req(wsgi_req);
        	if (ret > 0) {
			len = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_read_buf + *rlen, remains);
			if (len > 0) {
				wsgi_req->post_pos+=len;
				remains -= len;
                        	*rlen += len;
                        	continue;
			}else if (len < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS)) {
				goto wait;
			}
			*rlen = -1;
			if (len == 0) {
				uwsgi_read_error0(remains);
			}
			else {
				uwsgi_read_error(remains);
				wsgi_req->read_errors++;
			}
			return NULL;
		}
		// 0 means timeout
		else if (ret == 0) {
			*rlen = 0;
			uwsgi_read_timeout(remains);
			return NULL;
		}
		*rlen = -1;
		uwsgi_read_error(remains);
		wsgi_req->read_errors++;
		return NULL;
	}

	return wsgi_req->post_read_buf;
}

/*

	post buffering

*/

int uwsgi_postbuffer_do_in_mem(struct wsgi_request *wsgi_req) {

        size_t remains = wsgi_req->post_cl;
        int ret;
        char *ptr = wsgi_req->post_buffering_buf;

        while (remains > 0) {
                if (uwsgi.harakiri_options.workers > 0) {
                        inc_harakiri(uwsgi.harakiri_options.workers);
                }

                ssize_t rlen = wsgi_req->socket->proto_read_body(wsgi_req, ptr, remains);
                if (rlen > 0) {
			remains -= rlen;
			ptr += rlen;
			continue;
		}
                if (rlen == 0) {
			uwsgi_read_error0(remains);
			return -1; 
		}
                if (rlen < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
                                goto wait;
                        }
			uwsgi_read_error(remains);
			wsgi_req->read_errors++;
                        return -1;
                }

wait:
                ret = uwsgi_wait_read_req(wsgi_req);
                if (ret > 0) {
			rlen = wsgi_req->socket->proto_read_body(wsgi_req, ptr, remains);
			if (rlen > 0) {
				remains -= rlen;
				ptr += rlen;
				continue;
			}
		}
                if (ret < 0) {
			uwsgi_read_error(remains);
			wsgi_req->read_errors++;
                        return -1;
                }
		uwsgi_read_timeout(remains);
                return -1;
	}

        return 0;

}


int uwsgi_postbuffer_do_in_disk(struct wsgi_request *wsgi_req) {

        size_t post_remains = wsgi_req->post_cl;
        int ret;
        int upload_progress_fd = -1;
        char *upload_progress_filename = NULL;

        wsgi_req->post_file = uwsgi_tmpfile();
        if (!wsgi_req->post_file) {
                uwsgi_req_error("uwsgi_postbuffer_do_in_disk()/uwsgi_tmpfile()");
		wsgi_req->read_errors++;
                return -1;
        }

        if (uwsgi.upload_progress) {
                // first check for X-Progress-ID size
                // separator + 'X-Progress-ID' + '=' + uuid     
                upload_progress_filename = uwsgi_upload_progress_create(wsgi_req, &upload_progress_fd);
                if (!upload_progress_filename) {
                        uwsgi_log("invalid X-Progress-ID value: must be a UUID\n");
                }
        }

        // manage buffered data and upload progress
        while (post_remains > 0) {

                // during post buffering we need to constantly reset the harakiri
                if (uwsgi.harakiri_options.workers > 0) {
                        inc_harakiri(uwsgi.harakiri_options.workers);
                }

                // we use the already available post buffering buffer to read chunks....
                size_t remains = UMIN(post_remains, uwsgi.post_buffering);

                // first try to read data (there could be something already available
                ssize_t rlen = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_buffering_buf, remains);
                if (rlen > 0) goto write;
                if (rlen == 0) {
			uwsgi_read_error0(remains);
			goto end;
		}
                if (rlen < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
                                goto wait;
                        }
			uwsgi_read_error(remains);
			wsgi_req->read_errors++;
                        goto end;
                }

wait:
                ret = uwsgi_wait_read_req(wsgi_req);
                if (ret > 0) {
			rlen = wsgi_req->socket->proto_read_body(wsgi_req, wsgi_req->post_buffering_buf, remains);
			if (rlen > 0) goto write;
			if (rlen == 0) {
				uwsgi_read_error0(remains);
			}
			else {
				uwsgi_read_error(remains);
				wsgi_req->read_errors++;
			}
                        goto end;
		}
                if (ret < 0) {
			uwsgi_read_error(remains);
			wsgi_req->read_errors++;
                        goto end;
                }
		uwsgi_read_timeout(remains);
                goto end;

write:
                if (fwrite(wsgi_req->post_buffering_buf, rlen, 1, wsgi_req->post_file) != 1) {
                        uwsgi_req_error("uwsgi_postbuffer_do_in_disk()/fwrite()");
			wsgi_req->read_errors++;
                        goto end;
                }

                post_remains -= rlen;

		if (upload_progress_filename) {
                        // stop updating it on errors
                        if (uwsgi_upload_progress_update(wsgi_req, upload_progress_fd, post_remains)) {
                                uwsgi_upload_progress_destroy(upload_progress_filename, upload_progress_fd);
                                upload_progress_filename = NULL;
                        }
                }
        }
        rewind(wsgi_req->post_file);

        if (upload_progress_filename) {
                uwsgi_upload_progress_destroy(upload_progress_filename, upload_progress_fd);
        }

        return 0;

end:
        if (upload_progress_filename) {
                uwsgi_upload_progress_destroy(upload_progress_filename, upload_progress_fd);
        }
        return -1;
}

