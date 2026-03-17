#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	poll based fd waiter.
	Use it for blocking areas (like startup functions)
	DO NOT USE IN REQUEST PLUGINS !!!

*/
int uwsgi_waitfd_event(int fd, int timeout, int event) {

	int ret;
	struct pollfd upoll;

	if (!timeout)
		timeout = uwsgi.socket_timeout;

	timeout = timeout * 1000;
	if (timeout < 0)
		timeout = -1;

	upoll.fd = fd;
	upoll.events = event;
	upoll.revents = 0;
	ret = poll(&upoll, 1, timeout);

	if (ret < 0) {
		uwsgi_error("uwsgi_waitfd_event()/poll()");
	}
	else if (ret > 0) {
		if (upoll.revents & event) {
			return ret;
		}
		return -1;
	}

	return ret;
}

/*
	consume data from an fd (blocking)
*/
char *uwsgi_read_fd(int fd, size_t *size, int add_zero) {

	char stack_buf[4096];
	ssize_t len;
	char *buffer = NULL;

	len = 1;
	while (len > 0) {
		len = read(fd, stack_buf, 4096);
		if (len > 0) {
			*size += len;
			char *tmp = realloc(buffer, *size);
			if (!tmp) {
				uwsgi_error("uwsgi_read_fd()/realloc()");
				exit(1);
			}
			buffer = tmp;
			memcpy(buffer + (*size - len), stack_buf, len);
		}
	}

	if (add_zero) {
		*size = *size + 1;
		buffer = realloc(buffer, *size);
		if (!buffer) {
			uwsgi_error("uwsgi_read_fd()/realloc()");
			exit(1);
		}
		buffer[*size - 1] = 0;
	}

	return buffer;

}

// simply read the whole content of a file
char *uwsgi_simple_file_read(char *filename) {

	struct stat sb;
	char *buffer;
	ssize_t len;
	int fd = open(filename, O_RDONLY);
	if (fd < 0) {
		uwsgi_error_open(filename);
		goto end;
	}

	if (fstat(fd, &sb)) {
		uwsgi_error("fstat()");
		close(fd);
		goto end;
	}

	buffer = uwsgi_malloc(sb.st_size + 1);

	len = read(fd, buffer, sb.st_size);
	if (len != sb.st_size) {
		uwsgi_error("read()");
		free(buffer);
		close(fd);
		goto end;
	}

	close(fd);
	if (buffer[sb.st_size - 1] == '\n' || buffer[sb.st_size - 1] == '\r') {
		buffer[sb.st_size - 1] = 0;
	}
	buffer[sb.st_size] = 0;
	return buffer;
end:
	return (char *) "";

}

static char *uwsgi_scheme_fd(char *url, size_t *size, int add_zero) {
	int fd = atoi(url);
	return uwsgi_read_fd(fd, size, add_zero);
}

static char *uwsgi_scheme_exec(char *url, size_t *size, int add_zero) {
	int cpipe[2];
	if (pipe(cpipe)) {
		uwsgi_error("pipe()");
		exit(1);
	}
	uwsgi_run_command(url, NULL, cpipe[1]);
	char *buffer = uwsgi_read_fd(cpipe[0], size, add_zero);
	close(cpipe[0]);
	close(cpipe[1]);
	return buffer;
}

static char *uwsgi_scheme_http(char *url, size_t *size, int add_zero) {
	char byte;
        int body = 0;
	char *buffer = NULL;

		char *domain = url;
		char *uri = strchr(domain, '/');
		if (!uri) {
			uwsgi_log("invalid http url\n");
			exit(1);
		}
		uri[0] = 0;
		uwsgi_log("domain: %s\n", domain);

		char *colon = uwsgi_get_last_char(domain, ':');

		if (colon) {
			colon[0] = 0;
		}


		char *ip = uwsgi_resolve_ip(domain);
		if (!ip) {
			uwsgi_log("unable to resolve address %s\n", domain);
			exit(1);
		}

		if (colon) {
			colon[0] = ':';
			ip = uwsgi_concat2(ip, colon);
		}
		else {
			ip = uwsgi_concat2(ip, ":80");
		}

		int fd = uwsgi_connect(ip, 0, 0);

		if (fd < 0) {
			uwsgi_error("uwsgi_scheme_http()/connect()");
			exit(1);
		}

		free(ip);

		uri[0] = '/';

		if (write(fd, "GET ", 4) != 4) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		if (write(fd, uri, strlen(uri)) != (ssize_t) strlen(uri)) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		if (write(fd, " HTTP/1.0\r\n", 11) != 11) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		if (write(fd, "Host: ", 6) != 6) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}

		uri[0] = 0;
		if (write(fd, domain, strlen(domain)) != (ssize_t) strlen(domain)) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		uri[0] = '/';

		if (write(fd, "\r\nUser-Agent: uWSGI on ", 23) != 23) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		if (write(fd, uwsgi.hostname, uwsgi.hostname_len) != uwsgi.hostname_len) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}
		if (write(fd, "\r\n\r\n", 4) != 4) { uwsgi_error("uwsgi_scheme_http()/write()"); exit(1);}

		int http_status_code_ptr = 0;

		while (read(fd, &byte, 1) == 1) {
			if (byte == '\r' && body == 0) {
				body = 1;
			}
			else if (byte == '\n' && body == 1) {
				body = 2;
			}
			else if (byte == '\r' && body == 2) {
				body = 3;
			}
			else if (byte == '\n' && body == 3) {
				body = 4;
			}
			else if (body == 4) {
				*size = *size + 1;
				char *tmp = realloc(buffer, *size);
				if (!tmp) {
					uwsgi_error("uwsgi_open_and_read()/realloc()");
					exit(1);
				}
				buffer = tmp;
				buffer[*size - 1] = byte;
			}
			else {
				body = 0;
				http_status_code_ptr++;
				if (http_status_code_ptr == 10) {
					if (byte != '2') {
						uwsgi_log("Not usable HTTP response: %cxx\n", byte);
						if (uwsgi.has_emperor) {
							exit(UWSGI_EXILE_CODE);
						}
						else {
							exit(1);
						}
					}
				}
			}
		}

		close(fd);

		if (add_zero) {
			*size = *size + 1;
			char *tmp = realloc(buffer, *size);
			if (!tmp) {
				uwsgi_error("uwsgi_open_and_read()/realloc()");
				exit(1);
			}
			buffer = tmp;
			buffer[*size - 1] = 0;
		}

	return buffer;
}


static char *uwsgi_scheme_emperor(char *url, size_t *size, int add_zero) {

	if (uwsgi.emperor_fd_config < 0) {
		uwsgi_log("this is not a vassal instance\n");
		exit(1);
	}
	ssize_t rlen;
	struct uwsgi_header uh;
	size_t remains = 4;
	char *ptr = (char *) &uh;
	while(remains) {
		int ret = uwsgi_waitfd(uwsgi.emperor_fd_config, 5);
		if (ret <= 0) {
			uwsgi_log("[uwsgi-vassal] error waiting for config header %s !!!\n", url);
			exit(1);
		}
		rlen = read(uwsgi.emperor_fd_config, ptr, remains);
		if (rlen <= 0) {
			uwsgi_log("[uwsgi-vassal] error reading config header from %s !!!\n", url);
			exit(1);
		}
		ptr+=rlen;
		remains-=rlen;
	}

	remains = uh.pktsize;
	if (!remains) {
		uwsgi_log("[uwsgi-vassal] invalid config from %s\n", url);
		exit(1);
	}

	char *buffer = uwsgi_calloc(remains + add_zero);
	ptr = buffer;
	while (remains) {
		int ret = uwsgi_waitfd(uwsgi.emperor_fd_config, 5);
                if (ret <= 0) {
                	uwsgi_log("[uwsgi-vassal] error waiting for config %s !!!\n", url);
                        exit(1);
                }
		rlen = read(uwsgi.emperor_fd_config, ptr, remains);
		if (rlen <= 0) {
                	uwsgi_log("[uwsgi-vassal] error reading config from %s !!!\n", url);
                        exit(1);
                }
                ptr+=rlen;
                remains-=rlen;
	}

	*size = uh.pktsize + add_zero;
	return buffer;
}

static char *uwsgi_scheme_data(char *url, size_t *size, int add_zero) {
	char *buffer = NULL;
	int fd = open(uwsgi.binary_path, O_RDONLY);
	if (fd < 0) {
		uwsgi_error_open(uwsgi.binary_path);
		exit(1);
	}
	int slot = atoi(url);
	if (slot < 0) {
		uwsgi_log("invalid binary data slot requested\n");
		exit(1);
	}
	uwsgi_log("requesting binary data slot %d\n", slot);
	off_t fo = lseek(fd, 0, SEEK_END);
	if (fo < 0) {
		uwsgi_error("lseek()");
		uwsgi_log("invalid binary data slot requested\n");
		exit(1);
	}
	int i = 0;
	uint64_t datasize = 0;
	for (i = 0; i <= slot; i++) {
		fo = lseek(fd, -8, SEEK_CUR);
		if (fo < 0) {
			uwsgi_error("lseek()");
			uwsgi_log("invalid binary data slot requested\n");
			exit(1);
		}
		ssize_t len = read(fd, &datasize, 8);
		if (len != 8) {
			uwsgi_error("read()");
			uwsgi_log("invalid binary data slot requested\n");
			exit(1);
		}
		if (datasize == 0) {
			uwsgi_log("0 size binary data !!!\n");
			exit(1);
		}
		fo = lseek(fd, -(datasize + 8), SEEK_CUR);
		if (fo < 0) {
			uwsgi_error("lseek()");
			uwsgi_log("invalid binary data slot requested\n");
			exit(1);
		}

		if (i == slot) {
			*size = datasize;
			if (add_zero) {
				*size += 1;
			}
			buffer = uwsgi_malloc(*size);
			memset(buffer, 0, *size);
			len = read(fd, buffer, datasize);
			if (len != (ssize_t) datasize) {
				uwsgi_error("read()");
				uwsgi_log("invalid binary data slot requested\n");
				exit(1);
			}
		}
	}
	close(fd);

	return buffer;
}

static char *uwsgi_scheme_call(char *url, size_t *size, int add_zero) {
        char *(*func)(void) = dlsym(RTLD_DEFAULT, url);
	if (!func) {
		uwsgi_log("unable to find symbol %s\n", url);
                exit(1);
	}

	char *s = func();
	if (!s) {
		uwsgi_log("called symbol %s did not return a string\n", url);
                exit(1);
	}
        *size = strlen(s);
        if (add_zero) {
                *size += 1;
        }
        char *buffer = uwsgi_malloc(*size);
        memset(buffer, 0, *size);
        memcpy(buffer, s, strlen(s));

        return buffer;
}

static char *uwsgi_scheme_callint(char *url, size_t *size, int add_zero) {
        int (*func)(void) = dlsym(RTLD_DEFAULT, url);
        if (!func) {
                uwsgi_log("unable to find symbol %s\n", url);
                exit(1);
        }

	char *s = uwsgi_num2str(func());
        *size = strlen(s);
        if (add_zero) {
                *size += 1;
        }
        char *buffer = uwsgi_malloc(*size);
        memset(buffer, 0, *size);
        memcpy(buffer, s, strlen(s));
	free(s);

        return buffer;
}


static char *uwsgi_scheme_sym(char *url, size_t *size, int add_zero) {
	void *sym_start_ptr = NULL, *sym_end_ptr = NULL;
	char **raw_symbol = dlsym(RTLD_DEFAULT, url);
	if (raw_symbol) {
		sym_start_ptr = *raw_symbol;
		sym_end_ptr = sym_start_ptr + strlen(sym_start_ptr);
		goto found;
	}
	char *symbol = uwsgi_concat3("_binary_", url, "_start");
	sym_start_ptr = dlsym(RTLD_DEFAULT, symbol);
	if (!sym_start_ptr) {
		uwsgi_log("unable to find symbol %s\n", symbol);
		exit(1);
	}
	free(symbol);
	symbol = uwsgi_concat3("_binary_", url, "_end");
	sym_end_ptr = dlsym(RTLD_DEFAULT, symbol);
	if (!sym_end_ptr) {
		uwsgi_log("unable to find symbol %s\n", symbol);
		exit(1);
	}
	free(symbol);

found:

	*size = sym_end_ptr - sym_start_ptr;
	if (add_zero) {
		*size += 1;
	}
	char *buffer = uwsgi_malloc(*size);
	memset(buffer, 0, *size);
	memcpy(buffer, sym_start_ptr, sym_end_ptr - sym_start_ptr);

	return buffer;
}

static char *uwsgi_scheme_section(char *url, size_t *size, int add_zero) {
#ifdef UWSGI_ELF
	size_t s_len = 0;
	char *buffer = uwsgi_elf_section(uwsgi.binary_path, url, &s_len);
	if (!buffer) {
		uwsgi_log("unable to find section %s in %s\n", url + 10, uwsgi.binary_path);
		exit(1);
	}
	*size = s_len;
	if (add_zero)
		*size += 1;
	return buffer;
#else
	uwsgi_log("section:// scheme not supported on this platform\n");
	exit(1);
#endif
}

struct uwsgi_string_list *uwsgi_register_scheme(char *name, char * (*func)(char *, size_t *, int)) {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, uwsgi.schemes) {
		if (!strcmp(usl->value, name)) goto found;
	}

	usl = uwsgi_string_new_list(&uwsgi.schemes, name); 	

found:
	usl->custom_ptr = func;
	return usl;
}

char *uwsgi_open_and_read(char *url, size_t *size, int add_zero, char *magic_table[]) {

        struct stat sb;
        char *buffer = NULL;
        ssize_t len;
        char *magic_buf;
	int fd;

	*size = 0;

        // stdin ?
        if (!strcmp(url, "-")) {
                buffer = uwsgi_read_fd(0, size, add_zero);
		goto end;
        }
#ifdef UWSGI_EMBED_CONFIG
	else if (url[0] == 0) {
		*size = &UWSGI_EMBED_CONFIG_END - &UWSGI_EMBED_CONFIG;
		if (add_zero) {
			*size += 1;
		}
		buffer = uwsgi_malloc(*size);
		memset(buffer, 0, *size);
		memcpy(buffer, &UWSGI_EMBED_CONFIG, &UWSGI_EMBED_CONFIG_END - &UWSGI_EMBED_CONFIG);
		goto end;
	}
#endif

	struct uwsgi_string_list *usl = uwsgi_check_scheme(url);
	if (!usl) goto fallback;

	char *(*func)(char *, size_t *, int) = (char *(*)(char *, size_t *, int)) usl->custom_ptr;
	buffer = func(url + usl->len + 3, size, add_zero);
	if (buffer) goto end;
	// never here !!!
	uwsgi_log("unable to parse config file %s\n", url);
	exit(1);
	
	// fallback to file
fallback:
		fd = open(url, O_RDONLY);
		if (fd < 0) {
			uwsgi_error_open(url);
			exit(1);
		}

		if (fstat(fd, &sb)) {
			uwsgi_error("fstat()");
			exit(1);
		}

		if (S_ISFIFO(sb.st_mode)) {
			buffer = uwsgi_read_fd(fd, size, add_zero);
			close(fd);
			goto end;
		}

		// is it a potential virtual file (/proc, /sys...) ?
		int is_virtual = 0;
		if (sb.st_size == 0) {
			is_virtual = 1;
			sb.st_size = 4096;
		}

		buffer = uwsgi_malloc(sb.st_size + add_zero);

		len = read(fd, buffer, sb.st_size);
		if (!is_virtual) {
			if (len != sb.st_size) {
			}
		}
		else {
			if (len >= 0) {
				sb.st_size = len;
			}
			else {
				uwsgi_error("read()");
				exit(1);
			}
		}

		close(fd);

		*size = sb.st_size + add_zero;

		if (add_zero)
			buffer[sb.st_size] = 0;

end:

	if (magic_table) {
		// here we inject blobs
		struct uwsgi_string_list *usl = NULL;
		if (uwsgi.inject_before || uwsgi.inject_after) {
			struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
			uwsgi_foreach(usl, uwsgi.inject_before) {
				size_t rlen = 0;
				char *before = uwsgi_open_and_read(usl->value, &rlen, 0, NULL);
				if (uwsgi_buffer_append(ub, before, rlen)) {
					uwsgi_log("unable to inject data in the config file\n");
					exit(1);
				}	
				free(before);
			}
			if (uwsgi_buffer_append(ub, buffer, *size - add_zero)) {
                        	uwsgi_log("unable to inject data in the config file\n");
                                exit(1);
                        }
			uwsgi_foreach(usl, uwsgi.inject_after) {
				size_t rlen = 0;
                                char *after = uwsgi_open_and_read(usl->value, &rlen, 0, NULL);
                                if (uwsgi_buffer_append(ub, after, rlen)) {
                                        uwsgi_log("unable to inject data in the config file\n");
                                        exit(1);
                                }       
                                free(after);
			}
			if (add_zero) {
				if (uwsgi_buffer_append(ub, "\0", 1)) {
                                        uwsgi_log("unable to inject data in the config file\n");
                                        exit(1);
                                }
			}
			*size = ub->pos; 
			// free resources
			free(buffer);
			buffer = ub->buf;
			// aboid destroying buffer
			ub->buf = NULL;
			uwsgi_buffer_destroy(ub);
		}
		magic_buf = magic_sub(buffer, *size, size, magic_table);
		free(buffer);
		return magic_buf;
	}

	return buffer;
}

// attach an fd using UNIX sockets
int *uwsgi_attach_fd(int fd, int *count_ptr, char *code, size_t code_len) {

	struct msghdr msg;
	ssize_t len;
	char *id = NULL;

	struct iovec iov;
	struct cmsghdr *cmsg;
	int *ret;
	int i;
	int count = *count_ptr;

	void *msg_control = uwsgi_malloc(CMSG_SPACE(sizeof(int) * count));

	memset(msg_control, 0, CMSG_SPACE(sizeof(int) * count));

	if (code && code_len > 0) {
		// allocate space for code and num_sockets
		id = uwsgi_malloc(code_len + sizeof(int));
		memset(id, 0, code_len);
		iov.iov_len = code_len + sizeof(int);
	}

	iov.iov_base = id;

	memset(&msg, 0, sizeof(msg));

	msg.msg_name = NULL;
	msg.msg_namelen = 0;

	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	msg.msg_control = msg_control;
	msg.msg_controllen = CMSG_SPACE(sizeof(int) * count);

	msg.msg_flags = 0;

	len = recvmsg(fd, &msg, 0);
	if (len <= 0) {
		uwsgi_error("recvmsg()");
		free(msg_control);
		return NULL;
	}

	if (code && code_len > 0) {
		if (uwsgi_strncmp(id, code_len, code, code_len)) {
			free(msg_control);
			return NULL;
		}

		if ((size_t) len == code_len + sizeof(int)) {
			memcpy(&i, id + code_len, sizeof(int));
			if (i > count) {
				*count_ptr = i;
				free(msg_control);
				free(id);
				return NULL;
			}
		}
	}

	cmsg = CMSG_FIRSTHDR(&msg);
	if (!cmsg) {
		free(msg_control);
		return NULL;
	}

	if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
		free(msg_control);
		return NULL;
	}

	if ((size_t) (cmsg->cmsg_len - ((char *) CMSG_DATA(cmsg) - (char *) cmsg)) > (size_t) (sizeof(int) * (count + 1))) {
		uwsgi_log("not enough space for sockets data, consider increasing it\n");
		free(msg_control);
		return NULL;
	}

	ret = uwsgi_malloc(sizeof(int) * (count + 1));
	for (i = 0; i < count + 1; i++) {
		ret[i] = -1;
	}

	memcpy(ret, CMSG_DATA(cmsg), cmsg->cmsg_len - ((char *) CMSG_DATA(cmsg) - (char *) cmsg));

	free(msg_control);
	if (code && code_len > 0) {
		free(id);
	}

	return ret;
}

// signal free close
void uwsgi_protected_close(int fd) {

	sigset_t mask, oset;
	sigfillset(&mask);
	if (sigprocmask(SIG_BLOCK, &mask, &oset)) {
		uwsgi_error("sigprocmask()");
		exit(1);
	}
	close(fd);
	if (sigprocmask(SIG_SETMASK, &oset, NULL)) {
		uwsgi_error("sigprocmask()");
		exit(1);
	}
}

// signal free read
ssize_t uwsgi_protected_read(int fd, void *buf, size_t len) {

	sigset_t mask, oset;
	sigfillset(&mask);
	if (sigprocmask(SIG_BLOCK, &mask, &oset)) {
		uwsgi_error("sigprocmask()");
		exit(1);
	}

	ssize_t ret = read(fd, buf, len);

	if (sigprocmask(SIG_SETMASK, &oset, NULL)) {
		uwsgi_error("sigprocmask()");
		exit(1);
	}
	return ret;
}


// pipe datas from a fd to another (blocking)
ssize_t uwsgi_pipe(int src, int dst, int timeout) {
	char buf[8192];
	size_t written = -1;
	ssize_t len;

	for (;;) {
		int ret = uwsgi_waitfd(src, timeout);
		if (ret > 0) {
			len = read(src, buf, 8192);
			if (len == 0) {
				return written;
			}
			else if (len < 0) {
				uwsgi_error("read()");
				return -1;
			}

			size_t remains = len;
			while (remains > 0) {
				int ret = uwsgi_waitfd_write(dst, timeout);
				if (ret > 0) {
					len = write(dst, buf, remains);
					if (len > 0) {
						remains -= len;
						written += len;
					}
					else if (len == 0) {
						return written;
					}
					else {
						uwsgi_error("write()");
						return -1;
					}
				}
				else if (ret == 0) {
					goto timeout;
				}
				else {
					return -1;
				}
			}
		}
		else if (ret == 0) {
			goto timeout;
		}
		else {
			return -1;
		}
	}

	return written;
timeout:
	uwsgi_log("timeout while piping from %d to %d !!!\n", src, dst);
	return -1;
}

/*
	even if it is marked as non-blocking, so not use in request plugins as it uses poll() and not the hooks
*/
int uwsgi_write_nb(int fd, char *buf, size_t remains, int timeout) {
	char *ptr = buf;
	while(remains > 0) {
		int ret = uwsgi_waitfd_write(fd, timeout);
		if (ret > 0) {
			ssize_t len = write(fd, ptr, remains);
			if (len <= 0) {
				return -1;
			} 
			ptr += len;
			remains -= len;	
			continue;
		}
		return -1;
	}

	return 0;
}

/*
	this is like uwsgi_write_nb() but with fast initial write and hooked wait (use it in request plugin)
*/
int uwsgi_write_true_nb(int fd, char *buf, size_t remains, int timeout) {
        char *ptr = buf;
	int ret;

        while(remains > 0) {
		errno = 0;
		ssize_t len = write(fd, ptr, remains);
		if (len > 0) goto written;
		if (len == 0) return -1;		
		if (len < 0) {
			if (uwsgi_is_again()) goto wait;
			return -1;
		}
wait:
                ret = uwsgi.wait_write_hook(fd, timeout);
                if (ret > 0) {
			len = write(fd, ptr, remains);
			if (len > 0) goto written;
                }
                return -1;
written:
                ptr += len;
                remains -= len;
                continue;
        }

        return 0;
}




// like uwsgi_pipe but with fixed size
ssize_t uwsgi_pipe_sized(int src, int dst, size_t required, int timeout) {
	char buf[8192];
	size_t written = 0;
	ssize_t len;

	while (written < required) {
		int ret = uwsgi_waitfd(src, timeout);
		if (ret > 0) {
			len = read(src, buf, UMIN(8192, required - written));
			if (len == 0) {
				return written;
			}
			else if (len < 0) {
				uwsgi_error("read()");
				return -1;
			}

			size_t remains = len;
			while (remains > 0) {
				int ret = uwsgi_waitfd_write(dst, timeout);
				if (ret > 0) {
					len = write(dst, buf, remains);
					if (len > 0) {
						remains -= len;
						written += len;
					}
					else if (len == 0) {
						return written;
					}
					else {
						uwsgi_error("write()");
						return -1;
					}
				}
				else if (ret == 0) {
					goto timeout;
				}
				else {
					return -1;
				}
			}
		}
		else if (ret == 0) {
			goto timeout;
		}
		else {
			return -1;
		}
	}

	return written;
timeout:
	uwsgi_log("timeout while piping from %d to %d !!!\n", src, dst);
	return -1;
}


// check if an fd is valid
int uwsgi_valid_fd(int fd) {
	int ret = fcntl(fd, F_GETFL);
	if (ret == -1) {
		return 0;
	}
	return 1;
}

void uwsgi_close_all_fds(void) {
	int i;
	for (i = 3; i < (int) uwsgi.max_fd; i++) {
#ifdef __APPLE__
        	fcntl(i, F_SETFD, FD_CLOEXEC);
#else
                close(i);
#endif
	}
}

int uwsgi_read_uh(int fd, struct uwsgi_header *uh, int timeout) {
	char *ptr = (char *) uh;
	size_t remains = 4;
	while(remains > 0) {
		int ret = uwsgi_waitfd(fd, timeout);
		if (ret > 0) {
			ssize_t len = read(fd, ptr, remains);
			if (len <= 0) {
				return -1;
			}
			remains -=len;
			ptr +=len;
			continue;
		}
		return -1;
	}

	return 0;
}

int uwsgi_read_nb(int fd, char *buf, size_t remains, int timeout) {
	char *ptr = buf;
        while(remains > 0) {
                int ret = uwsgi_waitfd(fd, timeout);
                if (ret > 0) {
                        ssize_t len = read(fd, ptr, remains);
                        if (len <= 0) {
                                return -1;
                        }
                        remains -=len;
                        ptr +=len;
                        continue;
                }
                return -1;
        }

        return 0;
}

/*
        this is like uwsgi_read_nb() but with fast initial read and hooked wait (use it in request plugin)
*/
ssize_t uwsgi_read_true_nb(int fd, char *buf, size_t len, int timeout) {
        int ret;

	errno = 0;
	ssize_t rlen = read(fd, buf, len);
        if (rlen > 0) {
		return rlen;	
	}
        if (rlen == 0) return -1;
        if (rlen < 0) {
        	if (uwsgi_is_again()) goto wait;
        }
        return -1;
wait:
	errno = 0;
        ret = uwsgi.wait_read_hook(fd, timeout);
        if (ret > 0) {
		errno = 0;
        	rlen = read(fd, buf, len);
                if (rlen > 0) {
			return rlen;
                }
		return -1;
	}
        return ret;
}


/*
	like the previous one but consume the whole len (if possibile)
*/

int uwsgi_read_whole_true_nb(int fd, char *buf, size_t remains, int timeout) {
	char *ptr = buf;
	while(remains > 0) {
		ssize_t len = uwsgi_read_true_nb(fd, ptr, remains, timeout);
		if (len <= 0) return -1;
		ptr += len;
		remains -= len;
	}
	return 0;
}

/*
	this is a pretty magic function used for reading a full uwsgi response
	it is true non blocking, so you can use it in request plugins
	buffer is expected to be at least 4 bytes, rlen is a get/set value
*/

int uwsgi_read_with_realloc(int fd, char **buffer, size_t *rlen, int timeout, uint8_t *modifier1, uint8_t *modifier2) {
	if (*rlen < 4) return -1;
	char *buf = *buffer;
	int ret;

	// start reading the header
	char *ptr = buf;
	size_t remains = 4;
	while(remains > 0) {
		ssize_t len = read(fd, ptr, remains);
                if (len > 0) goto readok;
                if (len == 0) return -1;
                if (len < 0) {
                        if (uwsgi_is_again()) goto wait;
                        return -1;
                }
wait:
                ret = uwsgi.wait_read_hook(fd, timeout);
                if (ret > 0) {
                        len = read(fd, ptr, remains);
                        if (len > 0) goto readok;
                }
                return -1;
readok:
                ptr += len;
                remains -= len;
                continue;
        }

	struct uwsgi_header *uh = (struct uwsgi_header *) buf;
	uint16_t pktsize = uh->pktsize;
	if (modifier1)
		*modifier1 = uh->modifier1;
	if (modifier2)
		*modifier2 = uh->modifier2;
	
	if (pktsize > *rlen) {
		char *tmp_buf = realloc(buf, pktsize);
		if (!tmp_buf) {
			uwsgi_error("uwsgi_read_with_realloc()/realloc()");
			return -1;
		}
		*buffer = tmp_buf;
		buf = *buffer;
	}

	*rlen = pktsize;
	// read the body
	remains = pktsize;
	ptr = buf;
	while(remains > 0) {
                ssize_t len = read(fd, ptr, remains);
                if (len > 0) goto readok2;
                if (len == 0) return -1;
                if (len < 0) {
                        if (uwsgi_is_again()) goto wait2;
                        return -1;
                }
wait2:
                ret = uwsgi.wait_read_hook(fd, timeout);
                if (ret > 0) {
                        len = read(fd, ptr, remains);
                        if (len > 0) goto readok2;
                }
                return -1;
readok2:
                ptr += len;
                remains -= len;
                continue;
        }

	return 0;
	
}

/*

	this is a commodity (big) function to send a buffer and wsgi_req body to a socket
	and to receive back data (and send them to the client)

*/

int uwsgi_proxy_nb(struct wsgi_request *wsgi_req, char *addr, struct uwsgi_buffer *ub, size_t remains, int timeout) {

	struct uwsgi_buffer *headers = NULL;

	if (!ub) {
		return -1;
	}

	int fd = uwsgi_connect(addr, 0, 1);
	if (fd < 0) {
		return -1;
	}

	int ret = uwsgi.wait_write_hook(fd, timeout);
	if (ret <= 0) {
		goto end;
	}

	// send the request (+ remaining data)
	if (uwsgi_write_true_nb(fd, ub->buf, ub->pos, timeout)) {
		goto end;
	}

	// send the body
	while(remains > 0) {
		ssize_t rlen = 0;
		char *buf = uwsgi_request_body_read(wsgi_req, 8192, &rlen);
		if (!buf) {
			goto end;
		}
		if (buf == uwsgi.empty) break;
		// write data to the node
		if (uwsgi_write_true_nb(fd, buf, rlen, timeout)) {
			goto end;
		}
		remains -= rlen;
	}

	// read the response
	headers = uwsgi_buffer_new(8192);
	// max 64k headers
	ub->limit = UMAX16;
	for(;;) {
		char buf[8192];
		ssize_t rlen = uwsgi_read_true_nb(fd, buf, 8192, timeout);
		if (rlen > 0) {
			if (headers) {
				if (uwsgi_buffer_append(headers, buf, rlen)) {
					goto end;
				}
				// check if we have a full HTTP response
				if (uwsgi_is_full_http(headers)) {
					int ret = uwsgi_blob_to_response(wsgi_req, headers->buf, headers->pos);	
					if (ret) continue;
					uwsgi_buffer_destroy(headers);
					headers = NULL;
				}
			}
			else {
				if (uwsgi_response_write_body_do(wsgi_req, buf, rlen)) {
					break;
				}
			}
			continue;
		}
		break;
	}
	if (headers) uwsgi_buffer_destroy(headers);

	close(fd);
	return 0;
end:
	if (headers) uwsgi_buffer_destroy(headers);
	close(fd);
	return -1;
}

void uwsgi_file_write_do(struct uwsgi_string_list *usl) {

	struct uwsgi_string_list *fl = usl;
	while(fl) {
		char *equal = strchr(fl->value, '=');
		if (equal) {
			*equal = 0;
			FILE *f = fopen(fl->value, "w");
			if (!f) {
				uwsgi_error_open("uwsgi_file_write_do()");
				exit(1);
			}
			uwsgi_log("writing \"%s\" to \"%s\" ...\n", equal+1, fl->value);
			if (fprintf(f, "%s\n", equal+1) <= 0 || ferror(f) || fclose(f)) {
				uwsgi_error("uwsgi_file_write_do()");
				exit(1);
			}
		}
		else {
			uwsgi_log("unable to write empty value for \"%s\"\n", fl->value);
			exit(1);
		}
		*equal = '=';	
		fl = fl->next;
	}
}

int uwsgi_fd_is_safe(int fd) {
	int i;
	for(i=0;i<uwsgi.safe_fds_cnt;i++) {
		if (uwsgi.safe_fds[i] == fd) {
			return 1;
		}
	}
	return 0;
}
void uwsgi_add_safe_fd(int fd) {
	// check if the fd is already safe
	if (uwsgi_fd_is_safe(fd)) return;

	size_t len = sizeof(int) * (uwsgi.safe_fds_cnt+1);
	int *tmp = realloc(uwsgi.safe_fds, len);
	if (!tmp) {
		uwsgi_error("uwsgi_add_safe_fd()/realloc()");
		exit(1);
	}
	uwsgi.safe_fds = tmp;	
	uwsgi.safe_fds[uwsgi.safe_fds_cnt] = fd;	
	uwsgi.safe_fds_cnt++;
}

int uwsgi_is_again() {
	if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {
		return 1;
	}
	return 0;
}

void uwsgi_disconnect(struct wsgi_request *wsgi_req) {
	if (wsgi_req->socket) {
                wsgi_req->socket->proto_close(wsgi_req);
        }
        wsgi_req->fd_closed = 1;
}

int uwsgi_ready_fd(struct wsgi_request *wsgi_req) {
	if (wsgi_req->async_ready_fd) return wsgi_req->async_last_ready_fd;
	return -1;
}

void uwsgi_setup_schemes() {
	uwsgi_register_scheme("emperor", uwsgi_scheme_emperor);	
	uwsgi_register_scheme("http", uwsgi_scheme_http);	
	uwsgi_register_scheme("data", uwsgi_scheme_data);	
	uwsgi_register_scheme("sym", uwsgi_scheme_sym);	
	uwsgi_register_scheme("section", uwsgi_scheme_section);	
	uwsgi_register_scheme("fd", uwsgi_scheme_fd);	
	uwsgi_register_scheme("exec", uwsgi_scheme_exec);	
	uwsgi_register_scheme("call", uwsgi_scheme_call);	
	uwsgi_register_scheme("callint", uwsgi_scheme_callint);	
}

struct uwsgi_string_list *uwsgi_check_scheme(char *file) {
	struct uwsgi_string_list *usl;
	uwsgi_foreach(usl, uwsgi.schemes) {
		char *url = uwsgi_concat2(usl->value, "://");
		int ret = uwsgi_startswith(file, url, strlen(url));
		free(url);
		if (!ret) return usl;
	}
	return NULL;
}

void uwsgi_remap_fd(int fd, char *filename) {

	int fdin = open(filename, O_RDWR);
        if (fdin < 0) {
                uwsgi_error_open(filename);
                exit(1);
        }

        /* stdin */
        if (fdin != fd) {
                if (dup2(fdin, fd) < 0) {
                        uwsgi_error("uwsgi_remap_fd()/dup2()");
                        exit(1);
                }
                close(fdin);
        }
}

int uwsgi_is_connected(int fd) {
	int soopt;
        socklen_t solen = sizeof(int);

        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (void *) (&soopt), &solen) < 0) {
		return 0;
        }
        /* is something bad ? */
        if (soopt) return 0;
	return 1;
}


int uwsgi_pass_cred(int fd, char *code, size_t code_len) {
#ifdef SCM_CREDENTIALS
	struct msghdr cr_msg;
        struct cmsghdr *cmsg;
        struct iovec cr_iov;
        void *cr_msg_control = uwsgi_calloc(CMSG_SPACE(sizeof(struct ucred)));

        cr_iov.iov_base = code;
        cr_iov.iov_len = code_len;

        cr_msg.msg_name = NULL;
        cr_msg.msg_namelen = 0;

        cr_msg.msg_iov = &cr_iov;
        cr_msg.msg_iovlen = 1;

        cr_msg.msg_flags = 0;
        cr_msg.msg_control = cr_msg_control;
        cr_msg.msg_controllen = CMSG_SPACE(sizeof(struct ucred));

        cmsg = CMSG_FIRSTHDR(&cr_msg);
        cmsg->cmsg_len = CMSG_LEN(sizeof(struct ucred));
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_CREDENTIALS;

        struct ucred *u = (struct ucred*) CMSG_DATA(cmsg);
	u->pid = getpid();	
	u->uid = getuid();	
	u->gid = getgid();	

        if (sendmsg(fd, &cr_msg, 0) < 0) {
                uwsgi_error("uwsgi_pass_cred()/sendmsg()");
        	free(cr_msg_control);
		return -1;
        }

        free(cr_msg_control);
	return 0;
#else
	return -1;
#endif
}

int uwsgi_pass_cred2(int fd, char *code, size_t code_len, struct sockaddr *addr, size_t addr_len) {
#ifdef SCM_CREDENTIALS
        struct msghdr cr_msg;
        struct cmsghdr *cmsg;
        struct iovec cr_iov;
        void *cr_msg_control = uwsgi_calloc(CMSG_SPACE(sizeof(struct ucred)));

        cr_iov.iov_base = code;
        cr_iov.iov_len = code_len;

        cr_msg.msg_name = addr;
        cr_msg.msg_namelen = addr_len;

        cr_msg.msg_iov = &cr_iov;
        cr_msg.msg_iovlen = 1;

        cr_msg.msg_flags = 0;
        cr_msg.msg_control = cr_msg_control;
        cr_msg.msg_controllen = CMSG_SPACE(sizeof(struct ucred));

        cmsg = CMSG_FIRSTHDR(&cr_msg);
        cmsg->cmsg_len = CMSG_LEN(sizeof(struct ucred));
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_CREDENTIALS;

        struct ucred *u = (struct ucred*) CMSG_DATA(cmsg);
        u->pid = getpid();
        u->uid = getuid();
        u->gid = getgid();

        if (sendmsg(fd, &cr_msg, 0) < 0) {
                uwsgi_error("uwsgi_pass_cred2()/sendmsg()");
                free(cr_msg_control);
                return -1;
        }

        free(cr_msg_control);
        return 0;
#else
        return -1;
#endif
}


int uwsgi_recv_cred(int fd, char *code, size_t code_len, pid_t *pid, uid_t *uid, gid_t *gid) {
#ifdef SCM_CREDENTIALS
        struct iovec iov;
	int ret = -1;

        void *msg_control = uwsgi_calloc(CMSG_SPACE(sizeof(struct ucred)));

        iov.iov_base = uwsgi_malloc(code_len);
        iov.iov_len = code_len;

	struct msghdr msg;
        memset(&msg, 0, sizeof(msg));

        msg.msg_name = NULL;
        msg.msg_namelen = 0;

        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        msg.msg_control = msg_control;
        msg.msg_controllen = CMSG_SPACE(sizeof(struct ucred));

        ssize_t len = recvmsg(fd, &msg, 0);
	if (len <= 0) {
		uwsgi_error("uwsgi_recv_cred()/recvmsg()");
		goto clear;
	}

	struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
	if (!cmsg) goto clear;

	if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_CREDENTIALS) {
		goto clear;
        }

	if (uwsgi_strncmp(code, code_len, iov.iov_base, iov.iov_len)) goto clear;

	struct ucred *u = (struct ucred *) CMSG_DATA(cmsg);
	*pid = u->pid;
	*uid = u->uid;
	*gid = u->gid;
	ret = 0;

clear:
	free(msg_control);
	free(iov.iov_base);
	return ret;
#else
	return -1;
#endif
}

ssize_t uwsgi_recv_cred2(int fd, char *buf, size_t buf_len, pid_t *pid, uid_t *uid, gid_t *gid) {
#ifdef SCM_CREDENTIALS
        struct iovec iov;
        ssize_t ret = -1;

        void *msg_control = uwsgi_calloc(CMSG_SPACE(sizeof(struct ucred)));

        iov.iov_base = buf;
        iov.iov_len = buf_len;

        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));

        msg.msg_name = NULL;
        msg.msg_namelen = 0;

        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        msg.msg_control = msg_control;
        msg.msg_controllen = CMSG_SPACE(sizeof(struct ucred));

        ssize_t len = recvmsg(fd, &msg, 0);
        if (len <= 0) {
                uwsgi_error("uwsgi_recv_cred2()/recvmsg()");
                goto clear;
        }

        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        if (!cmsg) goto clear;

        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_CREDENTIALS) {
                goto clear;
        }

        struct ucred *u = (struct ucred *) CMSG_DATA(cmsg);
        *pid = u->pid;
        *uid = u->uid;
        *gid = u->gid;
        ret = len;

clear:
        free(msg_control);
        return ret;
#else
        return -1;
#endif
}

