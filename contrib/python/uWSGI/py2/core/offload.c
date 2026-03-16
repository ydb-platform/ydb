#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

/*

	uWSGI offloading subsystem

	steps to offload a task

	1) allocate a uwsgi_offload_request structure (could be on stack)
	struct uwsgi_offload_request uor;
	2) prepare it for a specific engine (last argument is the takeover flag)
	uwsgi_offload_setup(my_engine, &uor, 1)
	3) run it (last argument is the waiter)
	int uwsgi_offload_run(wsgi_req, &uor, NULL);

	between 2 and 3 you can set specific values

*/


extern struct uwsgi_server uwsgi;

#define uwsgi_offload_retry if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) return 0;
#define uwsgi_offload_0r_1w(x, y) if (event_queue_del_fd(ut->queue, x, event_queue_read())) return -1;\
					if (event_queue_fd_read_to_write(ut->queue, y)) return -1;

void uwsgi_offload_setup(struct uwsgi_offload_engine *uoe, struct uwsgi_offload_request *uor, struct wsgi_request *wsgi_req, uint8_t takeover) {

	memset(uor, 0, sizeof(struct uwsgi_offload_request));

	uor->engine = uoe;

	uor->s = wsgi_req->fd;
	uor->fd = -1;
	uor->fd2 = -1;
	// an engine could changes behaviour based on pipe anf takeover values
	uor->pipe[0] = -1;
	uor->pipe[1] = -1;
	uor->takeover = takeover;

}

static int uwsgi_offload_enqueue(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor) {
	struct uwsgi_core *uc = &uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id];
	uc->offloaded_requests++;
	// round robin
	if (uc->offload_rr >= uwsgi.offload_threads) {
		uc->offload_rr = 0;
	}
	struct uwsgi_thread *ut = uwsgi.offload_thread[uc->offload_rr];
	uc->offload_rr++;
	if (write(ut->pipe[0], uor, sizeof(struct uwsgi_offload_request)) != sizeof(struct uwsgi_offload_request)) {
		if (uor->takeover) {
			wsgi_req->fd_closed = 0;
		}
		return -1;
	}
#ifdef UWSGI_DEBUG
        uwsgi_log("[offload] created session %p\n", uor);
#endif
	return 0;
}

/*

	pipe offload engine:
		fd -> the file descriptor to read from
		len -> amount of data to transfer

*/

static int u_offload_pipe_prepare(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor) {

        if (uor->fd < 0 || !uor->len) {
                return -1;
        }
        return 0;
}

/*

        memory offload engine:
                buf -> pointer to the memory to transfer (memory is freed at the end)
                len -> amount of data to transfer

*/

static int u_offload_memory_prepare(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor) {

        if (!uor->buf || !uor->len) {
                return -1;
        }
        return 0;
}


/*

	transfer offload engine:
		name -> socket name
		ubuf -> data to send

*/

static int u_offload_transfer_prepare(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor) {

	if (!uor->name) {
		return -1;
	}

	uor->fd = uwsgi_connect(uor->name, 0, 1);
	if (uor->fd < 0) {
		uwsgi_error("u_offload_transfer_prepare()/connect()");
		return -1;
	}

	return 0;
}

/*

	sendfile offload engine:
		name -> filename to transfer
		fd -> file descriptor of file to transfer	
		fd2 -> sendfile destination (default wsgi_req->fd)
		len -> size of the file or amount of data to transfer
		pos -> position in the file

*/

static int u_offload_sendfile_prepare(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor) {

	if (!uor->name && uor->fd == -1) return -1;

	if (uor->name) {
		uor->fd = open(uor->name, O_RDONLY | O_NONBLOCK);
		if (uor->fd < 0) {
			uwsgi_error_open(uor->name);
			return -1;
		}
	}

	// make a fstat to get the file size
	if (!uor->len) {
		struct stat st;
		if (fstat(uor->fd, &st)) {
			uwsgi_error("u_offload_sendfile_prepare()/fstat()");
			if (uor->name) {
				close(uor->fd);
			}
			return -1;
		}
		uor->len = st.st_size;
	}

	if (uor->fd2 == -1) {
		uor->fd2 = uor->s;
	}
	uor->s = -1;

	return 0;
}

static void uwsgi_offload_close(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor) {

	// call the free function asap
	if (uor->free) {
		uor->free(uor);
	}

	// close the socket and the file descriptor
	if (uor->takeover && uor->s > -1) {
		close(uor->s);
	}
	if (uor->fd != -1) {
		close(uor->fd);
	}
	if (uor->fd2 != -1) {
		close(uor->fd2);
	}
	// remove the structure from the linked list;
	struct uwsgi_offload_request *prev = uor->prev;
	struct uwsgi_offload_request *next = uor->next;

	if (uor == ut->offload_requests_head) {
		ut->offload_requests_head = next;
	}

	if (uor == ut->offload_requests_tail) {
		ut->offload_requests_tail = prev;
	}

	if (prev) {
		prev->next = next;
	}

	if (next) {
		next->prev = prev;
	}

	if (uor->buf) {
		free(uor->buf);
	}

	if (uor->ubuf) {
		uwsgi_buffer_destroy(uor->ubuf);
	}

	if (uor->ubuf1) {
		uwsgi_buffer_destroy(uor->ubuf1);
	}
	if (uor->ubuf2) {
		uwsgi_buffer_destroy(uor->ubuf2);
	}
	if (uor->ubuf3) {
		uwsgi_buffer_destroy(uor->ubuf3);
	}
	if (uor->ubuf4) {
		uwsgi_buffer_destroy(uor->ubuf4);
	}
	if (uor->ubuf5) {
		uwsgi_buffer_destroy(uor->ubuf5);
	}
	if (uor->ubuf6) {
		uwsgi_buffer_destroy(uor->ubuf6);
	}
	if (uor->ubuf7) {
		uwsgi_buffer_destroy(uor->ubuf7);
	}
	if (uor->ubuf8) {
		uwsgi_buffer_destroy(uor->ubuf8);
	}

	if (uor->pipe[0] != -1) {
		close(uor->pipe[1]);
		close(uor->pipe[0]);
	}

#ifdef UWSGI_DEBUG
	uwsgi_log("[offload] destroyed session %p\n", uor);
#endif

	free(uor);
}

static void uwsgi_offload_append(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor) {

	if (!ut->offload_requests_head) {
		ut->offload_requests_head = uor;
	}

	if (ut->offload_requests_tail) {
		ut->offload_requests_tail->next = uor;
		uor->prev = ut->offload_requests_tail;
	}

	ut->offload_requests_tail = uor;
}

static struct uwsgi_offload_request *uwsgi_offload_get_by_fd(struct uwsgi_thread *ut, int s) {
	struct uwsgi_offload_request *uor = ut->offload_requests_head;
	while (uor) {
		if (uor->s == s || uor->fd == s || uor->fd2 == s) {
			return uor;
		}
		uor = uor->next;
	}

	return NULL;
}

static void uwsgi_offload_loop(struct uwsgi_thread *ut) {

	int i;
	void *events = event_queue_alloc(uwsgi.offload_threads_events);

	for (;;) {
		// TODO make timeout tunable
		int nevents = event_queue_wait_multi(ut->queue, -1, events, uwsgi.offload_threads_events);
		for (i = 0; i < nevents; i++) {
			int interesting_fd = event_queue_interesting_fd(events, i);
			if (interesting_fd == ut->pipe[1]) {
				struct uwsgi_offload_request *uor = uwsgi_malloc(sizeof(struct uwsgi_offload_request));
				ssize_t len = read(ut->pipe[1], uor, sizeof(struct uwsgi_offload_request));
				if (len != sizeof(struct uwsgi_offload_request)) {
					uwsgi_error("read()");
					free(uor);
					continue;
				}
				// cal the event function for the first time
				if (uor->engine->event_func(ut, uor, -1)) {
					uwsgi_offload_close(ut, uor);
					continue;
				}
				uwsgi_offload_append(ut, uor);
				continue;
			}

			// get the task from the interesting fd
			struct uwsgi_offload_request *uor = uwsgi_offload_get_by_fd(ut, interesting_fd);
			if (!uor)
				continue;
			// run the hook
			if (uor->engine->event_func(ut, uor, interesting_fd)) {
				uwsgi_offload_close(ut, uor);
			}
		}
	}
}

struct uwsgi_thread *uwsgi_offload_thread_start() {
	return uwsgi_thread_new(uwsgi_offload_loop);
}

/*

	offload memory transfer

        uor->len -> the size of the memory chunk
        uor->buf -> the memory to transfer
	uor->written -> written bytes

        status: none

*/

static int u_offload_memory_do(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor, int fd) {
	if (fd == -1) {
                if (event_queue_add_fd_write(ut->queue, uor->s)) return -1;
                return 0;
        }
	ssize_t rlen = write(uor->s, uor->buf + uor->written, uor->len - uor->written);
	if (rlen > 0) {
		uor->written += rlen;
		if (uor->written >= uor->len) {
			return -1;
		}
		return 0;
	}
        else if (rlen < 0) {
		uwsgi_offload_retry
                uwsgi_error("u_offload_memory_do()");
	}
	return -1;
}


/*

the offload task starts after having acquired the file fd

	uor->len -> the size of the file
	uor->pos -> start writing from pos (default 0)

	status: none

*/

static int u_offload_sendfile_do(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor, int fd) {

	if (fd == -1) {
		if (event_queue_add_fd_write(ut->queue, uor->fd2)) return -1;
		return 0;
	}
#if defined(__linux__) || defined(__sun__) || defined(__GNU_kFreeBSD__)
	ssize_t len = sendfile(uor->fd2, uor->fd, &uor->pos, 128 * 1024);
	if (len > 0) {
        	uor->written += len;
                if (uor->written >= uor->len) {
			return -1;
		}
		return 0;
	}
        else if (len < 0) {
		uwsgi_offload_retry
                uwsgi_error("u_offload_sendfile_do()");
	}
#elif defined(__FreeBSD__) || defined(__DragonFly__)
	off_t sbytes = 0;
	int ret = sendfile(uor->fd, uor->fd2, uor->pos, 0, NULL, &sbytes, 0);
	// transfer finished
	if (ret == -1) {
		uor->pos += sbytes;
		uwsgi_offload_retry
                uwsgi_error("u_offload_sendfile_do()");
	}
#elif defined(__APPLE__) && !defined(NO_SENDFILE)
	off_t len = 0;
        int ret = sendfile(uor->fd, uor->fd2, uor->pos, &len, NULL, 0);
        // transfer finished
        if (ret == -1) {
                uor->pos += len;
                uwsgi_offload_retry
                uwsgi_error("u_offload_sendfile_do()");
        }
#endif
	return -1;

}

/*

	pipe offloading
	status:
		0 -> waiting for data on fd
		1 -> waiting for write to s
*/

static int u_offload_pipe_do(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor, int fd) {
	
	ssize_t rlen;

	// setup
	if (fd == -1) {
		event_queue_add_fd_read(ut->queue, uor->fd);
		return 0;
	}

	switch(uor->status) {
		// read event from fd
		case 0:
			if (!uor->buf) {
				uor->buf = uwsgi_malloc(4096);
			}
			rlen = read(uor->fd, uor->buf, 4096);
			if (rlen > 0) {
				uor->to_write = rlen;
				uor->pos = 0;
				if (event_queue_del_fd(ut->queue, uor->fd, event_queue_read())) return -1;
				if (event_queue_add_fd_write(ut->queue, uor->s)) return -1;
				uor->status = 1;
				return 0;
			}
			if (rlen < 0) {
				uwsgi_offload_retry
				uwsgi_error("u_offload_pipe_do() -> read()");
			}
			return -1;
		// write event on s
		case 1:
			rlen = write(uor->s, uor->buf + uor->pos, uor->to_write);
			if (rlen > 0) {
				uor->to_write -= rlen;
				uor->pos += rlen;
				if (uor->to_write == 0) {
					if (event_queue_del_fd(ut->queue, uor->s, event_queue_write())) return -1;
					if (event_queue_add_fd_read(ut->queue, uor->fd)) return -1;
					uor->status = 0;
				}
				return 0;
			}
			else if (rlen < 0) {
				uwsgi_offload_retry
				uwsgi_error("u_offload_pipe_do() -> write()");
			}
			return -1;
		default:
			break;
	}

	return -1;
}



/*
the offload task starts soon after the call to connect()

	status:
		0 -> waiting for connection on fd
		1 -> sending request to fd (write event)
		2 -> start waiting for read on s and fd
		3 -> write to s
		4 -> write to fd
*/
static int u_offload_transfer_do(struct uwsgi_thread *ut, struct uwsgi_offload_request *uor, int fd) {
	
	ssize_t rlen;

	// setup
	if (fd == -1) {
		event_queue_add_fd_write(ut->queue, uor->fd);
		return 0;
	}

	switch(uor->status) {
		// waiting for connection
		case 0:
			if (fd == uor->fd) {
				uor->status = 1;
				// ok try to send the request right now...
				return u_offload_transfer_do(ut, uor, fd);
			}
			return -1;
		// write event (or just connected)
		case 1:
			if (fd == uor->fd) {
				// maybe we want only a connection...
				if (uor->ubuf->pos == 0) {
					uor->status = 2;
					if (event_queue_add_fd_read(ut->queue, uor->s)) return -1;
					if (event_queue_fd_write_to_read(ut->queue, uor->fd)) return -1;
					return 0;
				}
				rlen = write(uor->fd, uor->ubuf->buf + uor->written, uor->ubuf->pos-uor->written);	
				if (rlen > 0) {
					uor->written += rlen;
					if (uor->written >= (size_t)uor->ubuf->pos) {
						uor->status = 2;
						if (event_queue_add_fd_read(ut->queue, uor->s)) return -1;
						if (event_queue_fd_write_to_read(ut->queue, uor->fd)) return -1;
					}
					return 0;
				}
				else if (rlen < 0) {
					uwsgi_offload_retry
					uwsgi_error("u_offload_transfer_do() -> write()");
				}
			}	
			return -1;
		// read event from s or fd
		case 2:
			if (!uor->buf) {
				uor->buf = uwsgi_malloc(4096);
			}
			if (fd == uor->fd) {
				rlen = read(uor->fd, uor->buf, 4096);
				if (rlen > 0) {
					uor->to_write = rlen;
					uor->pos = 0;
					uwsgi_offload_0r_1w(uor->fd, uor->s)
					uor->status = 3;
					return 0;
				}
				if (rlen < 0) {
					uwsgi_offload_retry
					uwsgi_error("u_offload_transfer_do() -> read()/fd");
				}
			}
			else if (fd == uor->s) {
				rlen = read(uor->s, uor->buf, 4096);
				if (rlen > 0) {
					uor->to_write = rlen;
					uor->pos = 0;
					uwsgi_offload_0r_1w(uor->s, uor->fd)
					uor->status = 4;
					return 0;
				}
				if (rlen < 0) {
					uwsgi_offload_retry
					uwsgi_error("u_offload_transfer_do() -> read()/s");
				}
			}
			return -1;
		// write event on s
		case 3:
			rlen = write(uor->s, uor->buf + uor->pos, uor->to_write);
			if (rlen > 0) {
				uor->to_write -= rlen;
				uor->pos += rlen;
				if (uor->to_write == 0) {
					if (event_queue_fd_write_to_read(ut->queue, uor->s)) return -1;
					if (event_queue_add_fd_read(ut->queue, uor->fd)) return -1;
					uor->status = 2;
				}
				return 0;
			}
			else if (rlen < 0) {
				uwsgi_offload_retry
				uwsgi_error("u_offload_transfer_do() -> write()/s");
			}
			return -1;
		// write event on fd
		case 4:
			rlen = write(uor->fd, uor->buf + uor->pos, uor->to_write);
			if (rlen > 0) {
				uor->to_write -= rlen;
				uor->pos += rlen;
				if (uor->to_write == 0) {
					if (event_queue_fd_write_to_read(ut->queue, uor->fd)) return -1;
					if (event_queue_add_fd_read(ut->queue, uor->s)) return -1;
					uor->status = 2;
				}
				return 0;
			}
			else if (rlen < 0) {
				uwsgi_offload_retry
				uwsgi_error("u_offload_transfer_do() -> write()/fd");
			}
			return -1;
		default:
			break;
	}

	return -1;
}

int uwsgi_offload_run(struct wsgi_request *wsgi_req, struct uwsgi_offload_request *uor, int *wait) {

	if (uor->engine->prepare_func(wsgi_req, uor)) {
		return -1;
	}

	if (wait) {
                if (pipe(uor->pipe)) {
                        uwsgi_error("uwsgi_offload_setup()/pipe()");
                        return -1;
                }
                *wait = uor->pipe[0];
                uwsgi_socket_nb(uor->pipe[0]);
                uwsgi_socket_nb(uor->pipe[1]);
        }

        if (uor->takeover) {
                wsgi_req->fd_closed = 1;
        }

	if (uwsgi_offload_enqueue(wsgi_req, uor)) {
		close(uor->pipe[0]);
		close(uor->pipe[1]);
		if (uor->takeover) {
			wsgi_req->fd_closed = 0;
		}
		return -1;
        }

        return 0;
	
};

struct uwsgi_offload_engine *uwsgi_offload_engine_by_name(char *name) {
	struct uwsgi_offload_engine *uoe = uwsgi.offload_engines;
	while(uoe) {
		if (!strcmp(name, uoe->name)) {
			return uoe;
		}
	}
	return NULL;
}

struct uwsgi_offload_engine *uwsgi_offload_register_engine(char *name, int (*prepare_func)(struct wsgi_request *, struct uwsgi_offload_request *), int (*event_func) (struct uwsgi_thread *, struct uwsgi_offload_request *, int)) {

	struct uwsgi_offload_engine *old_engine=NULL,*engine=uwsgi.offload_engines;
	while(engine) {
		if (!strcmp(engine->name, name)) {
			return engine;
		}
		old_engine = engine;
		engine = engine->next;
	}

	engine = uwsgi_calloc(sizeof(struct uwsgi_offload_engine));
	engine->name = name;
	engine->prepare_func = prepare_func;
	engine->event_func = event_func;

	if (old_engine) {
		old_engine->next = engine;
	}
	else {
		uwsgi.offload_engines = engine;
	}

	return engine;
}

void uwsgi_offload_engines_register_all() {
	uwsgi.offload_engine_sendfile = uwsgi_offload_register_engine("sendfile", u_offload_sendfile_prepare, u_offload_sendfile_do);
	uwsgi.offload_engine_transfer = uwsgi_offload_register_engine("transfer", u_offload_transfer_prepare, u_offload_transfer_do);
	uwsgi.offload_engine_memory = uwsgi_offload_register_engine("memory", u_offload_memory_prepare, u_offload_memory_do);
	uwsgi.offload_engine_pipe = uwsgi_offload_register_engine("pipe", u_offload_pipe_prepare, u_offload_pipe_do);
}

int uwsgi_offload_request_sendfile_do(struct wsgi_request *wsgi_req, int fd, size_t pos, size_t len) {
	struct uwsgi_offload_request uor;
	uwsgi_offload_setup(uwsgi.offload_engine_sendfile, &uor, wsgi_req, 1);
	uor.fd = fd;
	uor.len = len;
	uor.pos = pos;
	return uwsgi_offload_run(wsgi_req, &uor, NULL);
}

int uwsgi_offload_request_net_do(struct wsgi_request *wsgi_req, char *socketname, struct uwsgi_buffer *ubuf) {
	struct uwsgi_offload_request uor;
	uwsgi_offload_setup(uwsgi.offload_engine_transfer, &uor, wsgi_req, 1);
	uor.name = socketname;
	uor.ubuf = ubuf;
	return uwsgi_offload_run(wsgi_req, &uor, NULL);
}

int uwsgi_offload_request_memory_do(struct wsgi_request *wsgi_req, char *buf, size_t len) {
        struct uwsgi_offload_request uor;
        uwsgi_offload_setup(uwsgi.offload_engine_memory, &uor, wsgi_req, 1);
        uor.buf = buf;
        uor.len = len;
        return uwsgi_offload_run(wsgi_req, &uor, NULL);
}

int uwsgi_offload_request_pipe_do(struct wsgi_request *wsgi_req, int fd, size_t len) {
        struct uwsgi_offload_request uor;
        uwsgi_offload_setup(uwsgi.offload_engine_pipe, &uor, wsgi_req, 1);
        uor.fd = fd;
        uor.len = len;
        return uwsgi_offload_run(wsgi_req, &uor, NULL);
}
