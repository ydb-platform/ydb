#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

// sendfile() abstraction
ssize_t uwsgi_sendfile_do(int sockfd, int filefd, size_t pos, size_t len) {
	// for platform not supporting sendfile we need to rely on boring read/write
	// generally that platforms have very low memory, so use a 8k buffer
	char buf[8192];

	if (uwsgi.disable_sendfile) goto no_sendfile;

#if defined(__FreeBSD__) || defined(__DragonFly__)
	off_t sf_len = len;
        int sf_ret = sendfile(filefd, sockfd, pos, len, NULL,  &sf_len, 0);
        if (sf_ret == 0 || (sf_ret == -1 && errno == EAGAIN)) return sf_len;
        return -1;
#elif defined(__APPLE__) && !defined(NO_SENDFILE)
	off_t sf_len = len;
	int sf_ret = sendfile(filefd, sockfd, pos, &sf_len, NULL, 0);
	if (sf_ret == 0 || (sf_ret == -1 && errno == EAGAIN)) return sf_len;
	return -1;
#elif defined(__linux__) || defined(__GNU_kFreeBSD__)
	off_t off = pos;
	return sendfile(sockfd, filefd, &off, len);
#elif defined(__sun__)
	off_t off = pos;
	ssize_t wlen = sendfile(sockfd, filefd, &off, len);
	if (wlen < 0 && uwsgi_is_again()) {
		if (off - pos > 0) {
			return off-pos;
		}
	}
	return wlen;
#endif

no_sendfile:
	if (pos > 0) {
		if (lseek(filefd, pos, SEEK_SET) < 0) {
			uwsgi_error("uwsgi_sendfile_do()/seek()");
			return -1;
		}
	}
	ssize_t rlen = read(filefd, buf, UMIN(len, 8192));
	if (rlen <= 0) {
		uwsgi_error("uwsgi_sendfile_do()/read()");
		return -1;
	}
	return write(sockfd, buf, rlen);

}


/*
	simple non blocking sendfile implementation
	(generally used as fallback)
*/

int uwsgi_simple_sendfile(struct wsgi_request *wsgi_req, int fd, size_t pos, size_t len) {

	wsgi_req->write_pos = 0;

	for(;;) {
                int ret = wsgi_req->socket->proto_sendfile(wsgi_req, fd, pos, len);
                if (ret < 0) {
                        if (!uwsgi.ignore_write_errors) {
                                uwsgi_error("uwsgi_simple_sendfile()");
                        }
                        wsgi_req->write_errors++;
                        return -1;
                }
                if (ret == UWSGI_OK) {
                        break;
                }
                ret = uwsgi_wait_write_req(wsgi_req);
                if (ret < 0) {
                        wsgi_req->write_errors++;
                        return -1;
                }
                if (ret == 0) {
                        uwsgi_log("uwsgi_simple_sendfile() TIMEOUT !!!\n");
                        wsgi_req->write_errors++;
                        return -1;
                }
        }

	return 0;
}
