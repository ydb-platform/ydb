#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	This is a general-purpose async loop engine (it expects a coroutine-based approach)

	You can see it as an hub holding the following structures:

	1) the runqueue, cores ready to be run are appended to this list

	2) the fd list, this is a list of monitored file descriptors, a core can wait for all the file descriptors it needs

	3) the timeout value, if set, the current core will timeout after the specified number of seconds (unless an event cancels it)


	IMPORTANT: this is not a callback-based engine !!!

*/

// this is called whenever a new connection is ready, but there are no cores to handle it
void uwsgi_async_queue_is_full(time_t now) {
	if (now > uwsgi.async_queue_is_full) {
		uwsgi_log_verbose("[DANGER] async queue is full !!!\n");
		uwsgi.async_queue_is_full = now;
	}
}

void uwsgi_async_init() {

	uwsgi.async_queue = event_queue_init();

	if (uwsgi.async_queue < 0) {
		exit(1);
	}

	uwsgi_add_sockets_to_queue(uwsgi.async_queue, -1);

	uwsgi.rb_async_timeouts = uwsgi_init_rb_timer();

	// optimization, this array maps file descriptor to requests
        uwsgi.async_waiting_fd_table = uwsgi_calloc(sizeof(struct wsgi_request *) * uwsgi.max_fd);
        uwsgi.async_proto_fd_table = uwsgi_calloc(sizeof(struct wsgi_request *) * uwsgi.max_fd);

}

struct wsgi_request *find_wsgi_req_proto_by_fd(int fd) {
	return uwsgi.async_proto_fd_table[fd];
}

struct wsgi_request *find_wsgi_req_by_fd(int fd) {
	return uwsgi.async_waiting_fd_table[fd];
}

static void runqueue_remove(struct uwsgi_async_request *u_request) {

	struct uwsgi_async_request *parent = u_request->prev;
	struct uwsgi_async_request *child = u_request->next;

	if (parent) {
		parent->next = child;
	}
	if (child) {
		child->prev = parent;
	}

	if (u_request == uwsgi.async_runqueue) {
		uwsgi.async_runqueue = child;
	}

	if (u_request == uwsgi.async_runqueue_last) {
		uwsgi.async_runqueue_last = parent;
	}

	free(u_request);
}

static void runqueue_push(struct wsgi_request *wsgi_req) {

	// do not push the same request in the runqueue
	struct uwsgi_async_request *uar = uwsgi.async_runqueue;
	while(uar) {
		if (uar->wsgi_req == wsgi_req) return;
		uar = uar->next;
	}

	uar = uwsgi_malloc(sizeof(struct uwsgi_async_request));
	uar->prev = NULL;
	uar->next = NULL;
	uar->wsgi_req = wsgi_req;

	if (uwsgi.async_runqueue == NULL) {
		uwsgi.async_runqueue = uar;
	}
	else {
		uar->prev = uwsgi.async_runqueue_last;	
	}

	if (uwsgi.async_runqueue_last) {
		uwsgi.async_runqueue_last->next = uar;
	}
	uwsgi.async_runqueue_last = uar;
}

struct wsgi_request *find_first_available_wsgi_req() {

	struct wsgi_request *wsgi_req;

	if (uwsgi.async_queue_unused_ptr < 0) {
		return NULL;
	}

	wsgi_req = uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr];
	uwsgi.async_queue_unused_ptr--;
	return wsgi_req;
}

void async_reset_request(struct wsgi_request *wsgi_req) {
	if (wsgi_req->async_timeout) {
		uwsgi_del_rb_timer(uwsgi.rb_async_timeouts, wsgi_req->async_timeout);
		free(wsgi_req->async_timeout);
		wsgi_req->async_timeout = NULL;
	}
	
	struct uwsgi_async_fd *uaf = wsgi_req->waiting_fds;
	while (uaf) {
        	event_queue_del_fd(uwsgi.async_queue, uaf->fd, uaf->event);
                uwsgi.async_waiting_fd_table[uaf->fd] = NULL;
                struct uwsgi_async_fd *current_uaf = uaf;
                uaf = current_uaf->next;
                free(current_uaf);
	}

	wsgi_req->waiting_fds = NULL;
}

static void async_expire_timeouts(uint64_t now) {

	struct wsgi_request *wsgi_req;
	struct uwsgi_rb_timer *urbt;

	for (;;) {

		urbt = uwsgi_min_rb_timer(uwsgi.rb_async_timeouts, NULL);

		if (urbt == NULL)
			return;

		if (urbt->value <= now) {
			wsgi_req = (struct wsgi_request *) urbt->data;
			// timeout expired
			wsgi_req->async_timed_out = 1;
			// reset the request
			async_reset_request(wsgi_req);
			// push it in the runqueue
			runqueue_push(wsgi_req);
			continue;
		}

		break;
	}

}

int async_add_fd_read(struct wsgi_request *wsgi_req, int fd, int timeout) {

	if (uwsgi.async < 2 || !uwsgi.async_waiting_fd_table){ 
		uwsgi_log_verbose("ASYNC call without async mode !!!\n");
		return -1;
	}

	struct uwsgi_async_fd *last_uad = NULL, *uad = wsgi_req->waiting_fds;

	if (fd < 0)
		return -1;

	// find last slot
	while (uad) {
		last_uad = uad;
		uad = uad->next;
	}

	uad = uwsgi_malloc(sizeof(struct uwsgi_async_fd));
	uad->fd = fd;
	uad->event = event_queue_read();
	uad->prev = last_uad;
	uad->next = NULL;

	if (last_uad) {
		last_uad->next = uad;
	}
	else {
		wsgi_req->waiting_fds = uad;
	}

	if (timeout > 0) {
		async_add_timeout(wsgi_req, timeout);
	}
	uwsgi.async_waiting_fd_table[fd] = wsgi_req;
	wsgi_req->async_force_again = 1;
	return event_queue_add_fd_read(uwsgi.async_queue, fd);
}

static int async_wait_fd_read(int fd, int timeout) {

	struct wsgi_request *wsgi_req = current_wsgi_req();

	wsgi_req->async_ready_fd = 0;

	if (async_add_fd_read(wsgi_req, fd, timeout)) {
		return -1;
	}
	if (uwsgi.schedule_to_main) {
		uwsgi.schedule_to_main(wsgi_req);
	}
	if (wsgi_req->async_timed_out) {
		wsgi_req->async_timed_out = 0;
		return 0;
	}
	return 1;
}

static int async_wait_fd_read2(int fd0, int fd1, int timeout, int *fd) {

        struct wsgi_request *wsgi_req = current_wsgi_req();

        wsgi_req->async_ready_fd = 0;

        if (async_add_fd_read(wsgi_req, fd0, timeout)) {
                return -1;
        }

        if (async_add_fd_read(wsgi_req, fd1, timeout)) {
		// reset already registered fd
		async_reset_request(wsgi_req);
                return -1;
        }

        if (uwsgi.schedule_to_main) {
                uwsgi.schedule_to_main(wsgi_req);
        }

        if (wsgi_req->async_timed_out) {
                wsgi_req->async_timed_out = 0;
                return 0;
        }

	if (wsgi_req->async_ready_fd) {
		*fd = wsgi_req->async_last_ready_fd;
		return 1;
	}

        return -1;
}


void async_add_timeout(struct wsgi_request *wsgi_req, int timeout) {

	if (uwsgi.async < 2 || !uwsgi.rb_async_timeouts) {
		uwsgi_log_verbose("ASYNC call without async mode !!!\n");
		return;
	}

	wsgi_req->async_ready_fd = 0;

	if (timeout > 0 && wsgi_req->async_timeout == NULL) {
		wsgi_req->async_timeout = uwsgi_add_rb_timer(uwsgi.rb_async_timeouts, uwsgi_now() + timeout, wsgi_req);
	}

}

int async_add_fd_write(struct wsgi_request *wsgi_req, int fd, int timeout) {

	if (uwsgi.async < 2 || !uwsgi.async_waiting_fd_table) {
		uwsgi_log_verbose("ASYNC call without async mode !!!\n");
		return -1;
	}

	struct uwsgi_async_fd *last_uad = NULL, *uad = wsgi_req->waiting_fds;

	if (fd < 0)
		return -1;

	// find last slot
	while (uad) {
		last_uad = uad;
		uad = uad->next;
	}

	uad = uwsgi_malloc(sizeof(struct uwsgi_async_fd));
	uad->fd = fd;
	uad->event = event_queue_write();
	uad->prev = last_uad;
	uad->next = NULL;

	if (last_uad) {
		last_uad->next = uad;
	}
	else {
		wsgi_req->waiting_fds = uad;
	}

	if (timeout > 0) {
		async_add_timeout(wsgi_req, timeout);
	}

	uwsgi.async_waiting_fd_table[fd] = wsgi_req;
	wsgi_req->async_force_again = 1;
	return event_queue_add_fd_write(uwsgi.async_queue, fd);
}

static int async_wait_fd_write(int fd, int timeout) {
	struct wsgi_request *wsgi_req = current_wsgi_req();

	wsgi_req->async_ready_fd = 0;

	if (async_add_fd_write(wsgi_req, fd, timeout)) {
		return -1;
	}
	if (uwsgi.schedule_to_main) {
		uwsgi.schedule_to_main(wsgi_req);
	}
	if (wsgi_req->async_timed_out) {
		wsgi_req->async_timed_out = 0;
		return 0;
	}
	return 1;
}

void async_schedule_to_req(void) {
#ifdef UWSGI_ROUTING
        if (uwsgi_apply_routes(uwsgi.wsgi_req) == UWSGI_ROUTE_BREAK) {
		goto end;
        }
	// a trick to avoid calling routes again
	uwsgi.wsgi_req->is_routing = 1;
#endif
	uwsgi.wsgi_req->async_status = uwsgi.p[uwsgi.wsgi_req->uh->modifier1]->request(uwsgi.wsgi_req);
        if (uwsgi.wsgi_req->async_status <= UWSGI_OK) goto end;

	if (uwsgi.schedule_to_main) {
        	uwsgi.schedule_to_main(uwsgi.wsgi_req);
	}
	return;

end:
	async_reset_request(uwsgi.wsgi_req);
	uwsgi_close_request(uwsgi.wsgi_req);
	uwsgi.wsgi_req->async_status = UWSGI_OK;	
	uwsgi.async_queue_unused_ptr++;
        uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = uwsgi.wsgi_req;
}

void async_schedule_to_req_green(void) {
	struct wsgi_request *wsgi_req = uwsgi.wsgi_req;
#ifdef UWSGI_ROUTING
        if (uwsgi_apply_routes(wsgi_req) == UWSGI_ROUTE_BREAK) {
                goto end;
        }
#endif
        for(;;) {
		wsgi_req->async_status = uwsgi.p[wsgi_req->uh->modifier1]->request(wsgi_req);
                if (wsgi_req->async_status <= UWSGI_OK) {
                        break;
                }
                wsgi_req->switches++;
		if (uwsgi.schedule_fix) {
			uwsgi.schedule_fix(wsgi_req);
		}
                // switch after each yield
		if (uwsgi.schedule_to_main)
			uwsgi.schedule_to_main(wsgi_req);
        }

#ifdef UWSGI_ROUTING
end:
#endif
	// re-set the global state
	uwsgi.wsgi_req = wsgi_req;
        async_reset_request(wsgi_req);
        uwsgi_close_request(wsgi_req);
	// re-set the global state (routing could have changed it)
	uwsgi.wsgi_req = wsgi_req;
        wsgi_req->async_status = UWSGI_OK;
	uwsgi.async_queue_unused_ptr++;
        uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = wsgi_req;
	
}

static int uwsgi_async_wait_milliseconds_hook(int timeout) {
	struct wsgi_request *wsgi_req = current_wsgi_req();
	timeout = timeout / 1000;
	if (!timeout) timeout = 1;
	async_add_timeout(wsgi_req, timeout);
	wsgi_req->async_force_again = 1;
	if (uwsgi.schedule_to_main) {
                uwsgi.schedule_to_main(wsgi_req);
        }
        if (wsgi_req->async_timed_out) {
                wsgi_req->async_timed_out = 0;
                return 0;
        }

	return -1;
}

void async_loop() {

	if (uwsgi.async < 2) {
		uwsgi_log("the async loop engine requires async mode (--async <n>)\n");
		exit(1);
	}

	int interesting_fd, i;
	struct uwsgi_rb_timer *min_timeout;
	int timeout;
	int is_a_new_connection;
	int proto_parser_status;

	uint64_t now;

	struct uwsgi_async_request *current_request = NULL;

	void *events = event_queue_alloc(64);
	struct uwsgi_socket *uwsgi_sock;

	uwsgi_async_init();

	uwsgi.async_runqueue = NULL;

	uwsgi.wait_write_hook = async_wait_fd_write;
        uwsgi.wait_read_hook = async_wait_fd_read;
        uwsgi.wait_read2_hook = async_wait_fd_read2;
	uwsgi.wait_milliseconds_hook = uwsgi_async_wait_milliseconds_hook;

	if (uwsgi.signal_socket > -1) {
		event_queue_add_fd_read(uwsgi.async_queue, uwsgi.signal_socket);
		event_queue_add_fd_read(uwsgi.async_queue, uwsgi.my_signal_socket);
	}

	// set a default request manager
	if (!uwsgi.schedule_to_req)
		uwsgi.schedule_to_req = async_schedule_to_req;

	if (!uwsgi.schedule_to_main) {
		uwsgi_log("*** DANGER *** async mode without coroutine/greenthread engine loaded !!!\n");
	}

	while (uwsgi.workers[uwsgi.mywid].manage_next_request) {

		now = (uint64_t) uwsgi_now();
		if (uwsgi.async_runqueue) {
			timeout = 0;
		}
		else {
			min_timeout = uwsgi_min_rb_timer(uwsgi.rb_async_timeouts, NULL);
			if (min_timeout) {
				timeout = min_timeout->value - now;
				if (timeout <= 0) {
					async_expire_timeouts(now);
					timeout = 0;
				}
			}
			else {
				timeout = -1;
			}
		}

		uwsgi.async_nevents = event_queue_wait_multi(uwsgi.async_queue, timeout, events, 64);

		now = (uint64_t) uwsgi_now();
		// timeout ???
		if (uwsgi.async_nevents == 0) {
			async_expire_timeouts(now);
		}


		for (i = 0; i < uwsgi.async_nevents; i++) {
			// manage events
			interesting_fd = event_queue_interesting_fd(events, i);

			// signals are executed in the main stack... in the future we could have dedicated stacks for them
			if (uwsgi.signal_socket > -1 && (interesting_fd == uwsgi.signal_socket || interesting_fd == uwsgi.my_signal_socket)) {
				uwsgi_receive_signal(interesting_fd, "worker", uwsgi.mywid);
				continue;
			}

			is_a_new_connection = 0;

			// new request coming in ?
			uwsgi_sock = uwsgi.sockets;
			while (uwsgi_sock) {

				if (interesting_fd == uwsgi_sock->fd) {

					is_a_new_connection = 1;

					uwsgi.wsgi_req = find_first_available_wsgi_req();
					if (uwsgi.wsgi_req == NULL) {
						uwsgi_async_queue_is_full((time_t)now);
						break;
					}

					// on error re-insert the request in the queue
					wsgi_req_setup(uwsgi.wsgi_req, uwsgi.wsgi_req->async_id, uwsgi_sock);
					if (wsgi_req_simple_accept(uwsgi.wsgi_req, interesting_fd)) {
						uwsgi.async_queue_unused_ptr++;
						uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = uwsgi.wsgi_req;
						break;
					}

					if (wsgi_req_async_recv(uwsgi.wsgi_req)) {
						uwsgi.async_queue_unused_ptr++;
						uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = uwsgi.wsgi_req;
						break;
					}

					// by default the core is in UWSGI_AGAIN mode
					uwsgi.wsgi_req->async_status = UWSGI_AGAIN;
					// some protocol (like zeromq) do not need additional parsing, just push it in the runqueue
					if (uwsgi.wsgi_req->do_not_add_to_async_queue) {
						runqueue_push(uwsgi.wsgi_req);
					}

					break;
				}

				uwsgi_sock = uwsgi_sock->next;
			}

			if (!is_a_new_connection) {
				// proto event
				uwsgi.wsgi_req = find_wsgi_req_proto_by_fd(interesting_fd);
				if (uwsgi.wsgi_req) {
					proto_parser_status = uwsgi.wsgi_req->socket->proto(uwsgi.wsgi_req);
					// reset timeout
					async_reset_request(uwsgi.wsgi_req);
					// parsing complete
					if (!proto_parser_status) {
						// remove fd from event poll and fd proto table 
						uwsgi.async_proto_fd_table[interesting_fd] = NULL;
						event_queue_del_fd(uwsgi.async_queue, interesting_fd, event_queue_read());
						// put request in the runqueue (set it as UWSGI_OK to signal the first run)
						uwsgi.wsgi_req->async_status = UWSGI_OK;
						runqueue_push(uwsgi.wsgi_req);
						continue;
					}
					else if (proto_parser_status < 0) {
						uwsgi.async_proto_fd_table[interesting_fd] = NULL;
						close(interesting_fd);
						uwsgi.async_queue_unused_ptr++;
						uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = uwsgi.wsgi_req;
						continue;
					}
					// re-add timer
					async_add_timeout(uwsgi.wsgi_req, uwsgi.socket_timeout);
					continue;
				}

				// app-registered event
				uwsgi.wsgi_req = find_wsgi_req_by_fd(interesting_fd);
				// unknown fd, remove it (for safety)
				if (uwsgi.wsgi_req == NULL) {
					close(interesting_fd);
					continue;
				}

				// remove all the fd monitors and timeout
				async_reset_request(uwsgi.wsgi_req);
				uwsgi.wsgi_req->async_ready_fd = 1;
				uwsgi.wsgi_req->async_last_ready_fd = interesting_fd;

				// put the request in the runqueue again
				runqueue_push(uwsgi.wsgi_req);
			}
		}


		// event queue managed, give cpu to runqueue
		current_request = uwsgi.async_runqueue;

		while(current_request) {

			// current_request could be nulled on error/end of request
			struct uwsgi_async_request *next_request = current_request->next;

			uwsgi.wsgi_req = current_request->wsgi_req;
			uwsgi.schedule_to_req();
			uwsgi.wsgi_req->switches++;

			// request ended ?
			if (uwsgi.wsgi_req->async_status <= UWSGI_OK ||
				uwsgi.wsgi_req->waiting_fds || uwsgi.wsgi_req->async_timeout) {
				// remove from the runqueue
				runqueue_remove(current_request);
			}
			current_request = next_request;
		}

	}

}
