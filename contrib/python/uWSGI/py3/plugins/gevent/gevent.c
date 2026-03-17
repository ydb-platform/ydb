#include <contrib/python/uWSGI/py3/config.h>
#include "gevent.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;
struct uwsgi_gevent ugevent;

static void uwsgi_opt_setup_gevent(char *opt, char *value, void *null) {

	// set async mode
	uwsgi_opt_set_int(opt, value, &uwsgi.async);
	if (uwsgi.socket_timeout < 30) {
		uwsgi.socket_timeout = 30;
	}
	// set loop engine
	uwsgi.loop = "gevent";

}

static struct uwsgi_option gevent_options[] = {
        {"gevent", required_argument, 0, "a shortcut enabling gevent loop engine with the specified number of async cores and optimal parameters", uwsgi_opt_setup_gevent, NULL, UWSGI_OPT_THREADS},
        {"gevent-monkey-patch", no_argument, 0, "call gevent.monkey.patch_all() automatically on startup", uwsgi_opt_true, &ugevent.monkey, 0},
        {"gevent-early-monkey-patch", no_argument, 0, "call gevent.monkey.patch_all() automatically before app loading", uwsgi_opt_true, &ugevent.early_monkey, 0},
        {"gevent-wait-for-hub", no_argument, 0, "wait for gevent hub's death instead of the control greenlet", uwsgi_opt_true, &ugevent.wait_for_hub, 0},
        {0, 0, 0, 0, 0, 0, 0},

};


/*
	Dumb greenlet used for controlling the shutdown (originally uWSGI only wait for the hub)
*/
PyObject *py_uwsgi_gevent_ctrl_gl(PyObject *self, PyObject *args) {
	for(;;) {
		PyObject *gevent_sleep_args = PyTuple_New(1);
                PyTuple_SetItem(gevent_sleep_args, 0, PyInt_FromLong(60));
                PyObject *gswitch = PyObject_CallObject(ugevent.greenlet_switch, gevent_sleep_args);
		// could be NULL on exception
		if (!gswitch) {
			// just for being paranid
			if (PyErr_Occurred()) {
				PyErr_Clear();
				break;
			}
		}
                Py_XDECREF(gswitch);
                Py_DECREF(gevent_sleep_args);
	}
	Py_INCREF(Py_None);
	return Py_None;
}

PyObject *py_uwsgi_gevent_graceful(PyObject *self, PyObject *args) {

	int running_cores = 0;
	int rounds = 0;

	uwsgi_log("Gracefully killing worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
        uwsgi.workers[uwsgi.mywid].manage_next_request = 0;

	if (uwsgi.signal_socket > -1) {
		uwsgi_log_verbose("stopping gevent signals watchers for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
		PyObject_CallMethod(ugevent.my_signal_watcher, "stop", NULL);
		PyObject_CallMethod(ugevent.signal_watcher, "stop", NULL);
	}

	uwsgi_log_verbose("stopping gevent sockets watchers for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
	int i,count = uwsgi_count_sockets(uwsgi.sockets);
	for(i=0;i<count;i++) {
		PyObject_CallMethod(ugevent.watchers[i], "stop", NULL);
	}
	uwsgi_log_verbose("main gevent watchers stopped for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);

retry:
	running_cores = 0;
	for(i=0;i<uwsgi.async;i++) {
		if (uwsgi.workers[uwsgi.mywid].cores[i].in_request) {
			struct wsgi_request *wsgi_req = &uwsgi.workers[uwsgi.mywid].cores[i].req;
			if (!rounds) {
				uwsgi_log_verbose("worker %d (pid: %d) core %d is managing \"%.*s %.*s\" for %.*s\n", uwsgi.mywid, uwsgi.mypid, i, 
					wsgi_req->method_len, wsgi_req->method, wsgi_req->uri_len, wsgi_req->uri,
					wsgi_req->remote_addr_len, wsgi_req->remote_addr);
			}
			running_cores++;
		}
	}

	if (running_cores > 0) {
		uwsgi_log_verbose("waiting for %d running requests on worker %d (pid: %d)...\n", running_cores, uwsgi.mywid, uwsgi.mypid);
		PyObject *gevent_sleep_args = PyTuple_New(1);
		PyTuple_SetItem(gevent_sleep_args, 0, PyInt_FromLong(1));
		PyObject *gswitch = python_call(ugevent.greenlet_switch, gevent_sleep_args, 0, NULL);
		Py_DECREF(gswitch);
		Py_DECREF(gevent_sleep_args);
		rounds++;
		goto retry;
	}

	if (!ugevent.wait_for_hub) {
		PyObject_CallMethod(ugevent.ctrl_gl, "kill", NULL);
	}

	Py_INCREF(Py_None);
	return Py_None;
}

PyObject *py_uwsgi_gevent_int(PyObject *self, PyObject *args) {

        uwsgi_log("Brutally killing worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
        uwsgi.workers[uwsgi.mywid].manage_next_request = 0;

        if (uwsgi.signal_socket > -1) {
                uwsgi_log_verbose("stopping gevent signals watchers for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
                PyObject_CallMethod(ugevent.my_signal_watcher, "stop", NULL);
                PyObject_CallMethod(ugevent.signal_watcher, "stop", NULL);
        }

        uwsgi_log_verbose("stopping gevent sockets watchers for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);
        int i,count = uwsgi_count_sockets(uwsgi.sockets);
        for(i=0;i<count;i++) {
                PyObject_CallMethod(ugevent.watchers[i], "stop", NULL);
        }
        uwsgi_log_verbose("main gevent watchers stopped for worker %d (pid: %d)...\n", uwsgi.mywid, uwsgi.mypid);

        if (!ugevent.wait_for_hub) {
                PyObject_CallMethod(ugevent.ctrl_gl, "kill", NULL);
        }

        Py_INCREF(Py_None);
        return Py_None;
}


static void uwsgi_gevent_gbcw() {

	// already running
	if (ugevent.destroy) return;

	uwsgi_log("...The work of process %d is done. Seeya!\n", getpid());

	uwsgi_time_bomb(uwsgi.worker_reload_mercy, 0);
	
	py_uwsgi_gevent_graceful(NULL, NULL);

	ugevent.destroy = 1;
}

struct wsgi_request *uwsgi_gevent_current_wsgi_req(void) {
	struct wsgi_request *wsgi_req = NULL;
	PyObject *current_greenlet = GET_CURRENT_GREENLET;
	PyObject *py_wsgi_req = PyObject_GetAttrString(current_greenlet, "uwsgi_wsgi_req");
	// not in greenlet
	if (!py_wsgi_req) {
		uwsgi_log("[BUG] current_wsgi_req NOT FOUND !!!\n");
		goto end;		
	}
	wsgi_req = (struct wsgi_request*) PyLong_AsLong(py_wsgi_req);
	Py_DECREF(py_wsgi_req);
end:
	Py_DECREF(current_greenlet);
	return wsgi_req;
}



PyObject *py_uwsgi_gevent_signal_handler(PyObject * self, PyObject * args) {

	int signal_socket;

	if (!PyArg_ParseTuple(args, "i:uwsgi_gevent_signal_handler", &signal_socket)) {
        	return NULL;
	}

	uwsgi_receive_signal(signal_socket, "worker", uwsgi.mywid);

	Py_INCREF(Py_None);
	return Py_None;
}

// the following two functions are called whenever an event is available in the signal queue
// they both trigger the same function
PyObject *py_uwsgi_gevent_signal(PyObject * self, PyObject * args) {

	PyTuple_SetItem(ugevent.signal_args, 1, PyInt_FromLong(uwsgi.signal_socket));

	// spawn the signal_handler greenlet
        PyObject *new_gl = python_call(ugevent.spawn, ugevent.signal_args, 0, NULL);
        Py_DECREF(new_gl);

	Py_INCREF(Py_None);
	return Py_None;
	
}

// yes copy&paste no-DRY for me :P
PyObject *py_uwsgi_gevent_my_signal(PyObject * self, PyObject * args) {

	PyTuple_SetItem(ugevent.signal_args, 1, PyInt_FromLong(uwsgi.my_signal_socket));

	// spawn the signal_handler greenlet
        PyObject *new_gl = python_call(ugevent.spawn, ugevent.signal_args, 0, NULL);
        Py_DECREF(new_gl);

	Py_INCREF(Py_None);
	return Py_None;
}


PyObject *py_uwsgi_gevent_main(PyObject * self, PyObject * args) {

	// hack to retrieve the socket address
	PyObject *py_uwsgi_sock = PyTuple_GetItem(args, 0);
        struct uwsgi_socket *uwsgi_sock = (struct uwsgi_socket *) PyLong_AsLong(py_uwsgi_sock);
	struct wsgi_request *wsgi_req = NULL;
edge:
	wsgi_req = find_first_available_wsgi_req();

	if (wsgi_req == NULL) {
		uwsgi_async_queue_is_full(uwsgi_now());
		goto clear;
	}

	// fill wsgi_request structure
	wsgi_req_setup(wsgi_req, wsgi_req->async_id, uwsgi_sock );

	// mark core as used
	uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].in_request = 1;

	// accept the connection (since uWSGI 1.5 all of the sockets are non-blocking)
	if (wsgi_req_simple_accept(wsgi_req, uwsgi_sock->fd)) {
		free_req_queue;
		if (uwsgi_sock->retry && uwsgi_sock->retry[wsgi_req->async_id]) {
			goto edge;
		}	
		// in case of errors (or thundering herd, just rest it)
		uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].in_request = 0;
		goto clear;
	}

	wsgi_req->start_of_request = uwsgi_micros();
	wsgi_req->start_of_request_in_sec = wsgi_req->start_of_request/1000000;

	// enter harakiri mode
        if (uwsgi.harakiri_options.workers > 0) {
                set_harakiri(uwsgi.harakiri_options.workers);
        }

	// hack to easily pass wsgi_req pointer to the greenlet
	PyTuple_SetItem(ugevent.greenlet_args, 1, PyLong_FromLong((long)wsgi_req));

	// spawn the request greenlet
	PyObject *new_gl = python_call(ugevent.spawn, ugevent.greenlet_args, 0, NULL);
	Py_DECREF(new_gl);

	if (uwsgi_sock->edge_trigger) {
#ifdef UWSGI_DEBUG
		uwsgi_log("i am an edge triggered socket !!!\n");
#endif
		goto edge;
	}

clear:
	Py_INCREF(Py_None);
	return Py_None;
}


PyObject *py_uwsgi_gevent_request(PyObject * self, PyObject * args) {

	PyObject *py_wsgi_req = PyTuple_GetItem(args, 0);
	struct wsgi_request *wsgi_req = (struct wsgi_request *) PyLong_AsLong(py_wsgi_req);

	PyObject *greenlet_switch = NULL;

	PyObject *current_greenlet = GET_CURRENT_GREENLET;
	// another hack to retrieve the current wsgi_req;
	PyObject_SetAttrString(current_greenlet, "uwsgi_wsgi_req", py_wsgi_req);

	// if in edge-triggered mode read from socket now !!!
	if (wsgi_req->socket->edge_trigger) {
		int status = wsgi_req->socket->proto(wsgi_req);
		if (status < 0) {
			goto end2;
		}
		goto request;
	}

	greenlet_switch = PyObject_GetAttrString(current_greenlet, "switch");

	for(;;) {
		int ret = uwsgi.wait_read_hook(wsgi_req->fd, uwsgi.socket_timeout);
                wsgi_req->switches++;

                if (ret <= 0) {
                        goto end;
                }

                int status = wsgi_req->socket->proto(wsgi_req);
                if (status < 0) {
                        goto end;
                }
                else if (status == 0) {
                        break;
                }
	}

request:

#ifdef UWSGI_ROUTING
	if (uwsgi_apply_routes(wsgi_req) == UWSGI_ROUTE_BREAK) {
		goto end;
	}
#endif

	for(;;) {
		if (uwsgi.p[wsgi_req->uh->modifier1]->request(wsgi_req) <= UWSGI_OK) {
			goto end;
		}
		wsgi_req->switches++;
		// switch after each yield
		GEVENT_SWITCH;
	}

end:
	if (greenlet_switch) {
		Py_DECREF(greenlet_switch);
	}
end2:
	Py_DECREF(current_greenlet);

	uwsgi_close_request(wsgi_req);
	free_req_queue;


	if (uwsgi.workers[uwsgi.mywid].manage_next_request == 0) {
		int running_cores = 0;
		int i;
          for(i=0;i<uwsgi.async;i++) {
            if (uwsgi.workers[uwsgi.mywid].cores[i].in_request) {
              running_cores++;
            }
          }

          if (running_cores == 0) {
            // no need to worry about freeing memory
            PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");
            if (uwsgi_dict) {
              PyObject *ae = PyDict_GetItemString(uwsgi_dict, "atexit");
              if (ae) {
                python_call(ae, PyTuple_New(0), 0, NULL);
              }
            }
          }
        }

	Py_INCREF(Py_None);
	return Py_None;

}

PyMethodDef uwsgi_gevent_main_def[] = { {"uwsgi_gevent_main", py_uwsgi_gevent_main, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_request_def[] = { {"uwsgi_gevent_request", py_uwsgi_gevent_request, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_signal_def[] = { {"uwsgi_gevent_signal", py_uwsgi_gevent_signal, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_my_signal_def[] = { {"uwsgi_gevent_my_signal", py_uwsgi_gevent_my_signal, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_signal_handler_def[] = { {"uwsgi_gevent_signal_handler", py_uwsgi_gevent_signal_handler, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_unix_signal_handler_def[] = { {"uwsgi_gevent_unix_signal_handler", py_uwsgi_gevent_graceful, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_unix_signal_int_handler_def[] = { {"uwsgi_gevent_unix_signal_int_handler", py_uwsgi_gevent_int, METH_VARARGS, ""} };
PyMethodDef uwsgi_gevent_ctrl_gl_def[] = { {"uwsgi_gevent_ctrl_gl_handler", py_uwsgi_gevent_ctrl_gl, METH_VARARGS, ""} };

static void gil_gevent_get() {
	pthread_setspecific(up.upt_gil_key, (void *) PyGILState_Ensure());
}

static void gil_gevent_release() {
	PyGILState_Release((PyGILState_STATE)(long) pthread_getspecific(up.upt_gil_key));
}

static void monkey_patch() {
	PyObject *gevent_monkey_dict = get_uwsgi_pydict("gevent.monkey");
        if (!gevent_monkey_dict) uwsgi_pyexit;
        PyObject *gevent_monkey_patch_all = PyDict_GetItemString(gevent_monkey_dict, "patch_all");
        if (!gevent_monkey_patch_all) uwsgi_pyexit;
        PyObject *ret = python_call(gevent_monkey_patch_all, PyTuple_New(0), 0, NULL);
        if (!ret) uwsgi_pyexit;
}
static void gevent_loop() {

	// ensure SIGPIPE is ignored
	signal(SIGPIPE, SIG_IGN);

	if (!uwsgi.has_threads && uwsgi.mywid == 1) {
		uwsgi_log("!!! Running gevent without threads IS NOT recommended, enable them with --enable-threads !!!\n");
	}

	if (uwsgi.socket_timeout < 30) {
		uwsgi_log("!!! Running gevent with a socket-timeout lower than 30 seconds is not recommended, tune it with --socket-timeout !!!\n");
	}

	// get the GIL
	UWSGI_GET_GIL

	up.gil_get = gil_gevent_get;
	up.gil_release = gil_gevent_release;

	uwsgi.wait_write_hook = uwsgi_gevent_wait_write_hook;
	uwsgi.wait_read_hook = uwsgi_gevent_wait_read_hook;
	uwsgi.wait_milliseconds_hook = uwsgi_gevent_wait_milliseconds_hook;

	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;

	if (uwsgi.async < 2) {
		uwsgi_log("the gevent loop engine requires async mode (--async <n>)\n");
		exit(1);
	}

	uwsgi.current_wsgi_req = uwsgi_gevent_current_wsgi_req;

	PyObject *gevent_dict = get_uwsgi_pydict("gevent");
	if (!gevent_dict) uwsgi_pyexit;

	PyObject *gevent_version = PyDict_GetItemString(gevent_dict, "version_info");
	if (!gevent_version) uwsgi_pyexit;

	if (PyInt_AsLong(PyTuple_GetItem(gevent_version, 0)) < 1) {
		uwsgi_log("uWSGI requires at least gevent 1.x version\n");
		exit(1);
	}

	// call gevent.monkey.patch_all() if requested
	if (ugevent.monkey) {
		monkey_patch();
	}

	ugevent.spawn = PyDict_GetItemString(gevent_dict, "spawn");
	if (!ugevent.spawn) uwsgi_pyexit;

	ugevent.signal = PyDict_GetItemString(gevent_dict, "signal_handler");
	if (!ugevent.signal) {
		// gevent.signal_handler appears in gevent 1.3.
		// On older gevent, fall back to the deprecated gevent.signal.
		ugevent.signal = PyDict_GetItemString(gevent_dict, "signal");
		if (!ugevent.signal) uwsgi_pyexit;
	}

	ugevent.greenlet_switch = PyDict_GetItemString(gevent_dict, "sleep");
	if (!ugevent.greenlet_switch) uwsgi_pyexit;

	ugevent.greenlet_switch_args = PyTuple_New(0);
	Py_INCREF(ugevent.greenlet_switch_args);
	
	PyObject *gevent_get_hub = PyDict_GetItemString(gevent_dict, "get_hub");

	ugevent.hub = python_call(gevent_get_hub, PyTuple_New(0), 0, NULL);
	if (!ugevent.hub) uwsgi_pyexit;

	ugevent.get_current = PyDict_GetItemString(gevent_dict, "getcurrent");
	if (!ugevent.get_current) uwsgi_pyexit;

	ugevent.get_current_args = PyTuple_New(0);
	Py_INCREF(ugevent.get_current_args);
	

	ugevent.hub_loop = PyObject_GetAttrString(ugevent.hub, "loop");
	if (!ugevent.hub_loop) uwsgi_pyexit;

	// main greenlet waiting for connection (one greenlet per-socket)
	PyObject *uwsgi_gevent_main = PyCFunction_New(uwsgi_gevent_main_def, NULL);
	Py_INCREF(uwsgi_gevent_main);

	// greenlet to run at each request
	PyObject *uwsgi_request_greenlet = PyCFunction_New(uwsgi_gevent_request_def, NULL);
	Py_INCREF(uwsgi_request_greenlet);

	// pre-fill the greenlet args
	ugevent.greenlet_args = PyTuple_New(2);
	PyTuple_SetItem(ugevent.greenlet_args, 0, uwsgi_request_greenlet);
		
	if (uwsgi.signal_socket > -1) {
		// and these are the watcher for signal sockets

		ugevent.signal_watcher = PyObject_CallMethod(ugevent.hub_loop, "io", "ii", uwsgi.signal_socket, 1);
        	if (!ugevent.signal_watcher) uwsgi_pyexit;

		ugevent.my_signal_watcher = PyObject_CallMethod(ugevent.hub_loop, "io", "ii", uwsgi.my_signal_socket, 1);
        	if (!ugevent.my_signal_watcher) uwsgi_pyexit;

		PyObject *uwsgi_greenlet_signal = PyCFunction_New(uwsgi_gevent_signal_def, NULL);
        	Py_INCREF(uwsgi_greenlet_signal);

		PyObject *uwsgi_greenlet_my_signal = PyCFunction_New(uwsgi_gevent_my_signal_def, NULL);
        	Py_INCREF(uwsgi_greenlet_my_signal);

		PyObject *uwsgi_greenlet_signal_handler = PyCFunction_New(uwsgi_gevent_signal_handler_def, NULL);
        	Py_INCREF(uwsgi_greenlet_signal_handler);

		ugevent.signal_args = PyTuple_New(2);
		PyTuple_SetItem(ugevent.signal_args, 0, uwsgi_greenlet_signal_handler);

		// start the two signal watchers
		if (!PyObject_CallMethod(ugevent.signal_watcher, "start", "O", uwsgi_greenlet_signal)) uwsgi_pyexit;
		if (!PyObject_CallMethod(ugevent.my_signal_watcher, "start", "O", uwsgi_greenlet_my_signal)) uwsgi_pyexit;

	}

	// start a greenlet for each socket
	ugevent.watchers = uwsgi_malloc(sizeof(PyObject *) * uwsgi_count_sockets(uwsgi.sockets));
	int i = 0;
	while(uwsgi_sock) {
		// this is the watcher for server socket
		ugevent.watchers[i] = PyObject_CallMethod(ugevent.hub_loop, "io", "ii", uwsgi_sock->fd, 1);
		if (!ugevent.watchers[i]) uwsgi_pyexit;
	
		// start the main greenlet
		PyObject_CallMethod(ugevent.watchers[i], "start", "Ol", uwsgi_gevent_main,(long)uwsgi_sock);
		uwsgi_sock = uwsgi_sock->next;
		i++;
	}

	// patch goodbye_cruel_world
	uwsgi.gbcw_hook = uwsgi_gevent_gbcw;

	// map SIGHUP with gevent.signal
	PyObject *ge_signal_tuple = PyTuple_New(2);
	PyTuple_SetItem(ge_signal_tuple, 0, PyInt_FromLong(SIGHUP));
	PyObject *uwsgi_gevent_unix_signal_handler = PyCFunction_New(uwsgi_gevent_unix_signal_handler_def, NULL);
        Py_INCREF(uwsgi_gevent_unix_signal_handler);
	PyTuple_SetItem(ge_signal_tuple, 1, uwsgi_gevent_unix_signal_handler);

	python_call(ugevent.signal, ge_signal_tuple, 0, NULL);

	// map SIGINT/SIGTERM with gevent.signal
	ge_signal_tuple = PyTuple_New(2);
	PyTuple_SetItem(ge_signal_tuple, 0, PyInt_FromLong(SIGINT));
	PyObject *uwsgi_gevent_unix_signal_int_handler = PyCFunction_New(uwsgi_gevent_unix_signal_int_handler_def, NULL);
        Py_INCREF(uwsgi_gevent_unix_signal_int_handler);
	PyTuple_SetItem(ge_signal_tuple, 1, uwsgi_gevent_unix_signal_int_handler);
	python_call(ugevent.signal, ge_signal_tuple, 0, NULL);

	ge_signal_tuple = PyTuple_New(2);
	PyTuple_SetItem(ge_signal_tuple, 0, PyInt_FromLong(SIGTERM));
	PyTuple_SetItem(ge_signal_tuple, 1, uwsgi_gevent_unix_signal_int_handler);
	python_call(ugevent.signal, ge_signal_tuple, 0, NULL);




	PyObject *wait_for_me = ugevent.hub;

	if (!ugevent.wait_for_hub) {
		// spawn the control greenlet
		PyObject *uwsgi_greenlet_ctrl_gl_handler = PyCFunction_New(uwsgi_gevent_ctrl_gl_def, NULL);
                Py_INCREF(uwsgi_greenlet_ctrl_gl_handler);
		PyObject *ctrl_gl_args = PyTuple_New(1);
                PyTuple_SetItem(ctrl_gl_args, 0, uwsgi_greenlet_ctrl_gl_handler);
        	ugevent.ctrl_gl = python_call(ugevent.spawn, ctrl_gl_args, 0, NULL);
		Py_INCREF(ugevent.ctrl_gl);
		wait_for_me = ugevent.ctrl_gl;
	}

	for(;;) {
		if (!PyObject_CallMethod(wait_for_me, "join", NULL)) {
			PyErr_Print();
		}
		else {
			break;
		}
	}

	// no need to worry about freeing memory
        PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");
        if (uwsgi_dict) {
                PyObject *ae = PyDict_GetItemString(uwsgi_dict, "atexit");
                if (ae) {
                        python_call(ae, PyTuple_New(0), 0, NULL);
                }
        }

	if (uwsgi.workers[uwsgi.mywid].manage_next_request == 0) {
		uwsgi_log("goodbye to the gevent Hub on worker %d (pid: %d)\n", uwsgi.mywid, uwsgi.mypid);
		if (ugevent.destroy) {
			exit(0);
		}
		exit(UWSGI_RELOAD_CODE);
	}

	uwsgi_log("the gevent Hub is no more :(\n");

}

static void gevent_preinit_apps() {
	// call gevent.monkey.patch_all() if requested
        if (ugevent.early_monkey) {
                monkey_patch();
        }
}

static void gevent_init() {
	uwsgi_register_loop( (char *) "gevent", gevent_loop);
}


struct uwsgi_plugin gevent_plugin = {

	.name = "gevent",
	.options = gevent_options,
	.preinit_apps = gevent_preinit_apps,
	.on_load = gevent_init,
};
