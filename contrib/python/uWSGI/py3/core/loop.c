#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

struct wsgi_request *threaded_current_wsgi_req() {
	return pthread_getspecific(uwsgi.tur_key);
}
struct wsgi_request *simple_current_wsgi_req() {
	return uwsgi.wsgi_req;
}


void uwsgi_register_loop(char *name, void (*func) (void)) {

	struct uwsgi_loop *old_loop = NULL, *loop = uwsgi.loops;

	while (loop) {
		// check if the loop engine is already registered
		if (!strcmp(name, loop->name))
			return;
		old_loop = loop;
		loop = loop->next;
	}

	loop = uwsgi_calloc(sizeof(struct uwsgi_loop));
	loop->name = name;
	loop->loop = func;

	if (old_loop) {
		old_loop->next = loop;
	}
	else {
		uwsgi.loops = loop;
	}
}

void *uwsgi_get_loop(char *name) {

	struct uwsgi_loop *loop = uwsgi.loops;

	while (loop) {
		if (!strcmp(name, loop->name)) {
			return loop->loop;
		}
		loop = loop->next;
	}
	return NULL;
}

/*

	this is the default (simple) loop.

	it will run simple_loop_run function for each spawned thread

	simple_loop_run monitors sockets and signals descriptors
	and manages them.

*/

void simple_loop() {
	uwsgi_loop_cores_run(simple_loop_run);
	// Other threads may still run. Make sure they will stop.
	uwsgi.workers[uwsgi.mywid].manage_next_request = 0;

	if (uwsgi.workers[uwsgi.mywid].shutdown_sockets)
		uwsgi_shutdown_all_sockets();
}

void uwsgi_loop_cores_run(void *(*func) (void *)) {
	int i;
	for (i = 1; i < uwsgi.threads; i++) {
		long j = i;
		pthread_create(&uwsgi.workers[uwsgi.mywid].cores[i].thread_id, &uwsgi.threads_attr, func, (void *) j);
	}
	long y = 0;
	func((void *) y);
}

void uwsgi_setup_thread_req(long core_id, struct wsgi_request *wsgi_req) {
	int i;
	sigset_t smask;

	pthread_setspecific(uwsgi.tur_key, (void *) wsgi_req);

	if (core_id > 0) {
		// block all signals on new threads
		sigfillset(&smask);
#ifdef UWSGI_DEBUG
		sigdelset(&smask, SIGSEGV);
#endif
		pthread_sigmask(SIG_BLOCK, &smask, NULL);

		// run per-thread socket hook
		struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
		while (uwsgi_sock) {
			if (uwsgi_sock->proto_thread_fixup) {
				uwsgi_sock->proto_thread_fixup(uwsgi_sock, core_id);
			}
			uwsgi_sock = uwsgi_sock->next;
		}

		for (i = 0; i < 256; i++) {
			if (uwsgi.p[i]->init_thread) {
				uwsgi.p[i]->init_thread(core_id);
			}
		}
	}

}

void simple_loop_run_int(int core_id) {
	long y = core_id;
	simple_loop_run((void *) y);
}

void *simple_loop_run(void *arg1) {

	long core_id = (long) arg1;

	struct wsgi_request *wsgi_req = &uwsgi.workers[uwsgi.mywid].cores[core_id].req;

	if (uwsgi.threads > 1) {
		uwsgi_setup_thread_req(core_id, wsgi_req);
	}
	// initialize the main event queue to monitor sockets
	int main_queue = event_queue_init();

	uwsgi_add_sockets_to_queue(main_queue, core_id);
	event_queue_add_fd_read(main_queue, uwsgi.loop_stop_pipe[0]);

	if (uwsgi.signal_socket > -1) {
		event_queue_add_fd_read(main_queue, uwsgi.signal_socket);
		event_queue_add_fd_read(main_queue, uwsgi.my_signal_socket);
	}


	// ok we are ready, let's start managing requests and signals
	while (uwsgi.workers[uwsgi.mywid].manage_next_request) {

		wsgi_req_setup(wsgi_req, core_id, NULL);

		if (wsgi_req_accept(main_queue, wsgi_req)) {
			continue;
		}

		if (wsgi_req_recv(main_queue, wsgi_req)) {
			uwsgi_destroy_request(wsgi_req);
			continue;
		}

		uwsgi_close_request(wsgi_req);
	}

	// end of the loop
	if (uwsgi.workers[uwsgi.mywid].destroy && uwsgi.workers[0].pid > 0) {
#ifdef __APPLE__
		kill(uwsgi.workers[0].pid, SIGTERM);
#else
		if (uwsgi.propagate_touch) {
			kill(uwsgi.workers[0].pid, SIGHUP);
		}
		else {
			gracefully_kill(0);
		}
#endif
	}
	return NULL;
}
