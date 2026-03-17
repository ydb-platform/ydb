#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

struct uwsgi_redislog_state {
	int fd;
	char *password;
	char *address;
	char *id;
	char *command;
	char *prefix;
	char msgsize[11];
	struct iovec iovec[7];
	char response[8];
};

static char *uwsgi_redis_logger_build_command(char *src) {
	ssize_t len = 4096;
	char *dst = uwsgi_calloc(len);
	char *orig_dst = dst;
	int count = 2;
	char *ptr = src;
	
	// first of all, count the number of spaces
	while(*ptr++) { if (*ptr == ' ') count++; }

	int pos = snprintf(dst, 4096, "*%d\r\n", count);
	dst+=pos;
	len -= pos;

	ptr = src;
	char *base = src;

	while(*ptr++) {
		if (*ptr == ' ') {
			pos = snprintf(dst, len, "$%d\r\n%.*s\r\n", (int) (ptr-base), (int) (ptr-base), base);
			if (pos >= len || pos < 0) {
				// i do not know what to do, better to exit...
				exit(1);
			}
			base = ptr+1;
			dst+=pos;
			len-= pos;
		}	
	}

	pos = snprintf(dst, len, "$%d\r\n%.*s\r\n", (int) ((ptr-1)-base), (int) ((ptr-1)-base), base);
	if (pos > len || pos < 0) {
		// i do not know what to do, better to exit...
		exit(1);
	}
	return orig_dst;
}

static ssize_t uwsgi_redis_logger_discard_response(struct uwsgi_redislog_state *uredislog) {
	ssize_t ret = 0, ret2;
again:
	// read til a \n is found (ugly but fast)
	ret2 = read(uredislog->fd, uredislog->response, 8);
	if (ret2 <= 0) {
		close(uredislog->fd);
		uredislog->fd = -1;
		return -1;
	}
	ret += ret2;
	if (!memchr(uredislog->response, '\n', ret2)) {
		goto again;
	}
	return ret;
}

ssize_t uwsgi_redis_logger(struct uwsgi_logger *ul, char *message, size_t len) {

	ssize_t ret;
	struct uwsgi_redislog_state *uredislog = NULL;

	if (!ul->configured) {

		if (!ul->data) {
			ul->data = uwsgi_calloc(sizeof(struct uwsgi_redislog_state));
			uredislog = (struct uwsgi_redislog_state *) ul->data;
		}

		if (ul->arg != NULL) {
			char *logarg = uwsgi_str(ul->arg);
			char *at = strchr(logarg, '@');
			if (at) {
			  *at = 0;
			  uredislog->password = logarg;
			  logarg = at + 1;
			}
			char *comma1 = strchr(logarg, ',');
			if (!comma1) {
				char *slash = strchr(logarg, '/');
				if (slash) {
				  *slash = 0;
				  uredislog->id = slash + 1;
				}
				uredislog->address = uwsgi_resolve_ip(logarg);
				goto done;
			}
			*comma1 = 0;
			char *slash = strchr(logarg, '/');
			if (slash) {
			  *slash = 0;
			  uredislog->id = slash + 1;
			}
			uredislog->address = logarg;
			comma1++;
			if (*comma1 == 0) goto done;

			char *comma2 = strchr(comma1,',');
			if (!comma2) {
				uredislog->command = uwsgi_redis_logger_build_command(comma1);
				goto done;
			}

			*comma2 = 0;
			uredislog->command = uwsgi_redis_logger_build_command(comma1);
			comma2++;
			if (*comma2 == 0) goto done;

			uredislog->prefix = comma2;
		}

done:

		if (!uredislog->password) uredislog->password = NULL;
		if (!uredislog->id) uredislog->id = "0";
		if (!uredislog->address) uredislog->address = uwsgi_str("127.0.0.1:6379");
		if (!uredislog->command) uredislog->command = "*3\r\n$7\r\npublish\r\n$5\r\nuwsgi\r\n";
		if (!uredislog->prefix) uredislog->prefix = "";

		uredislog->fd = -1;

		uredislog->iovec[0].iov_base = uredislog->command;
		uredislog->iovec[0].iov_len = strlen(uredislog->command);
		uredislog->iovec[1].iov_base = "$";
		uredislog->iovec[1].iov_len = 1;

		uredislog->iovec[2].iov_base = uredislog->msgsize;

		uredislog->iovec[3].iov_base = "\r\n";
		uredislog->iovec[3].iov_len = 2;

		uredislog->iovec[4].iov_base = uredislog->prefix;
		uredislog->iovec[4].iov_len = strlen(uredislog->prefix);

		uredislog->iovec[6].iov_base = "\r\n";
		uredislog->iovec[6].iov_len = 2;

		ul->configured = 1;
	}

	uredislog = (struct uwsgi_redislog_state *) ul->data;
	if (uredislog->fd == -1) {
		struct iovec	setup_iov;
		char		setup_buf[4096];
		uredislog->fd = uwsgi_connect(uredislog->address, uwsgi.socket_timeout, 0);
		if (uredislog->password) {
		  setup_iov.iov_len = snprintf(
		    setup_buf, sizeof (setup_buf), "*2\r\n$4\r\nauth\r\n$%zu\r\n%*s\r\n",
		    strlen(uredislog->password), (int)strlen(uredislog->password), uredislog->password);
		  setup_iov.iov_base = setup_buf;
		  ret = writev(uredislog->fd, &setup_iov, 1);
		  if (ret <= 0) {
		    close(uredislog->fd);
		    uredislog->fd = -1;
		    return -1;
		  }
		  uwsgi_redis_logger_discard_response(uredislog);
		}
		if (uredislog->id) {
		  setup_iov.iov_len = snprintf(
		    setup_buf, sizeof (setup_buf), "*2\r\n$6\r\nselect\r\n$%zu\r\n%*s\r\n",
	            strlen(uredislog->id), (int)strlen(uredislog->id), uredislog->id);
		  setup_iov.iov_base = setup_buf;
		  ret = writev(uredislog->fd, &setup_iov, 1);
		  if (ret <= 0) {
		    close(uredislog->fd);
		    uredislog->fd = -1;
		    return -1;
		  }
		  uwsgi_redis_logger_discard_response(uredislog);
		}
	}

	if (uredislog->fd == -1) return -1;

	// drop newline
        if (message[len-1] == '\n') len--;

	uwsgi_num2str2(len + uredislog->iovec[4].iov_len, uredislog->msgsize);
	uredislog->iovec[2].iov_len = strlen(uredislog->msgsize);

	uredislog->iovec[5].iov_base = message;
	uredislog->iovec[5].iov_len = len;
	
	ret = writev(uredislog->fd, uredislog->iovec, 7);
	if (ret <= 0) {
		close(uredislog->fd);
		uredislog->fd = -1;
		return -1;
	}

	uwsgi_redis_logger_discard_response(uredislog);

	return ret;

}

void uwsgi_redislog_register() {
	uwsgi_register_logger("redislog", uwsgi_redis_logger);
}

struct uwsgi_plugin redislog_plugin = {

        .name = "redislog",
        .on_load = uwsgi_redislog_register,

};

