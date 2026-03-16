#include <contrib/python/uWSGI/py2/config.h>
#include "../python/uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;

struct uwsgi_tornado {
	PyObject *ioloop;
	PyObject *functools;
	PyObject *request;
	PyObject *read;
	PyObject *write;
	PyObject *hook_fd;
	PyObject *hook_timeout;
	PyObject *hook_fix;
} utornado;

#define free_req_queue uwsgi.async_queue_unused_ptr++; uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = wsgi_req

static void uwsgi_opt_setup_tornado(char *opt, char *value, void *null) {

	// set async mode
	uwsgi_opt_set_int(opt, value, &uwsgi.async);
	if (uwsgi.socket_timeout < 30) {
		uwsgi.socket_timeout = 30;
	}
	// set loop engine
	uwsgi.loop = "tornado";

}

static struct uwsgi_option tornado_options[] = {
        {"tornado", required_argument, 0, "a shortcut enabling tornado loop engine with the specified number of async cores and optimal parameters", uwsgi_opt_setup_tornado, NULL, UWSGI_OPT_THREADS},
        {0, 0, 0, 0, 0, 0, 0},

};

static void gil_tornado_get() {
	pthread_setspecific(up.upt_gil_key, (void *) PyGILState_Ensure());
}

static void gil_tornado_release() {
	PyGILState_Release((PyGILState_STATE) pthread_getspecific(up.upt_gil_key));
}

static int uwsgi_tornado_wait_read_hook(int fd, int timeout) {

	/*

		def hook_fd(wsgi_req, fd, events):
			uwsgi.resume(wsgi_req)

		def hook_timeout(wsgi_req):
			wsgi_req->timed_out = 1;
			uwsgi.resume(wsgi_req)

		io_loop.add_handler(fd, functools.partial(hook, wsgi_req), io_loop.READ)
		t = io_loop.add_timeout(timeout, functools.partial(hook, wsgi_req))
		uwsgi.suspend()
		io_loop.remove_handler(fd)
		io_loop.remove_timeout(t)
		if wsgi_req->timed_out: return 0

	*/

	struct wsgi_request *wsgi_req = current_wsgi_req();

	PyObject *cb_fd = PyObject_CallMethod(utornado.functools, "partial", "Ol", utornado.hook_fd, (long) wsgi_req);
	if (!cb_fd) goto error;
	PyObject *cb_timeout = PyObject_CallMethod(utornado.functools, "partial", "Ol", utornado.hook_timeout, (long) wsgi_req);
	if (!cb_timeout) {
		Py_DECREF(cb_fd);
		goto error;
	}

	if (PyObject_CallMethod(utornado.ioloop, "add_handler", "iOO", fd, cb_fd, utornado.read) == NULL) {
		Py_DECREF(cb_fd);
		Py_DECREF(cb_timeout);
		goto error;
	}

	PyObject *ob_timeout = PyObject_CallMethod(utornado.ioloop, "add_timeout", "iO", uwsgi_now() + timeout, cb_timeout);
	if (!ob_timeout) {
		Py_DECREF(cb_fd);
		Py_DECREF(cb_timeout);
		goto error;
	}

	// back to ioloop
	if (uwsgi.schedule_to_main) uwsgi.schedule_to_main(wsgi_req);
	// back from ioloop

	if (PyObject_CallMethod(utornado.ioloop, "remove_handler", "i", fd) == NULL) PyErr_Print();
	if (PyObject_CallMethod(utornado.ioloop, "remove_timeout", "O", ob_timeout) == NULL) PyErr_Print();

	Py_DECREF(ob_timeout);
	Py_DECREF(cb_fd);
	Py_DECREF(cb_timeout);

	if (wsgi_req->async_timed_out) return 0;

        return 1;

error:
	PyErr_Print();
	return -1;
}

static int uwsgi_tornado_wait_write_hook(int fd, int timeout) {

        struct wsgi_request *wsgi_req = current_wsgi_req();

        PyObject *cb_fd = PyObject_CallMethod(utornado.functools, "partial", "Ol", utornado.hook_fd, (long) wsgi_req);
        if (!cb_fd) goto error;
        PyObject *cb_timeout = PyObject_CallMethod(utornado.functools, "partial", "Ol", utornado.hook_timeout, (long) wsgi_req);
        if (!cb_timeout) {
                Py_DECREF(cb_fd);
                goto error;
        }

        if (PyObject_CallMethod(utornado.ioloop, "add_handler", "iOO", fd, cb_fd, utornado.write) == NULL) {
                Py_DECREF(cb_fd);
                Py_DECREF(cb_timeout);
                goto error;
        }

        PyObject *ob_timeout = PyObject_CallMethod(utornado.ioloop, "add_timeout", "iO", uwsgi_now() + timeout, cb_timeout);
        if (!ob_timeout) {
                Py_DECREF(cb_fd);
                Py_DECREF(cb_timeout);
                goto error;
        }

        // back to ioloop
        if (uwsgi.schedule_to_main) {
		uwsgi.schedule_to_main(wsgi_req);
	}
        // back from ioloop

        if (PyObject_CallMethod(utornado.ioloop, "remove_handler", "i", fd) == NULL) PyErr_Print();
        if (PyObject_CallMethod(utornado.ioloop, "remove_timeout", "O", ob_timeout) == NULL) PyErr_Print();

        Py_DECREF(ob_timeout);
        Py_DECREF(cb_fd);
        Py_DECREF(cb_timeout);

        if (wsgi_req->async_timed_out) return 0;

        return 1;

error:
        PyErr_Print();
        return -1;
}



PyObject *py_uwsgi_tornado_request(PyObject *self, PyObject *args) {
        int fd = -1;
        PyObject *events = NULL;
        if (!PyArg_ParseTuple(args, "iO:uwsgi_tornado_request", &fd, &events)) {
		uwsgi_log_verbose("[BUG] invalid arguments for tornado callback !!!\n");
		exit(1);
        }

        struct wsgi_request *wsgi_req = find_wsgi_req_proto_by_fd(fd);
	uwsgi.wsgi_req = wsgi_req;

	int status = wsgi_req->socket->proto(wsgi_req);
	if (status > 0) goto again;
	if (PyObject_CallMethod(utornado.ioloop, "remove_handler", "i", fd) == NULL) {
		PyErr_Print();
		goto end;
	}

	if (status == 0) {
		// we call this two time... overengineering :(
		uwsgi.async_proto_fd_table[wsgi_req->fd] = NULL;
		uwsgi.schedule_to_req();
		goto again;
	}

end:
	uwsgi.async_proto_fd_table[wsgi_req->fd] = NULL;
	uwsgi_close_request(uwsgi.wsgi_req);	
	free_req_queue;
again:
	Py_INCREF(Py_None);
	return Py_None;
}


PyObject *py_uwsgi_tornado_accept(PyObject *self, PyObject *args) {
	int fd = -1;
	PyObject *events = NULL;
	if (!PyArg_ParseTuple(args, "iO:uwsgi_tornado_accept", &fd, &events)) {
                return NULL;
        }

	struct wsgi_request *wsgi_req = find_first_available_wsgi_req();

        if (wsgi_req == NULL) {
                uwsgi_async_queue_is_full(uwsgi_now());
                goto end;
        }

	uwsgi.wsgi_req = wsgi_req;
	// TODO better to move it to a function api ...
	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	while(uwsgi_sock) {
		if (uwsgi_sock->fd == fd) break;
		uwsgi_sock = uwsgi_sock->next;
	}

	if (!uwsgi_sock) {
		free_req_queue;
		goto end;
	}

        // fill wsgi_request structure
        wsgi_req_setup(wsgi_req, wsgi_req->async_id, uwsgi_sock );

        // mark core as used
        uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].in_request = 1;

        // accept the connection (since uWSGI 1.5 all of the sockets are non-blocking)
        if (wsgi_req_simple_accept(wsgi_req, uwsgi_sock->fd)) {
                // in case of errors (or thundering herd, just reset it)
                uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].in_request = 0;
                free_req_queue;
                goto end;
        }

        wsgi_req->start_of_request = uwsgi_micros();
        wsgi_req->start_of_request_in_sec = wsgi_req->start_of_request/1000000;

        // enter harakiri mode
        if (uwsgi.harakiri_options.workers > 0) {
                set_harakiri(uwsgi.harakiri_options.workers);
        }

	uwsgi.async_proto_fd_table[wsgi_req->fd] = wsgi_req;

	// add callback for protocol
	if (PyObject_CallMethod(utornado.ioloop, "add_handler", "iOO", wsgi_req->fd, utornado.request, utornado.read) == NULL) {
		free_req_queue;
		PyErr_Print();
	}

end:
	Py_INCREF(Py_None);
	return Py_None;
}

PyObject *py_uwsgi_tornado_hook_fd(PyObject *self, PyObject *args) {
	long wsgi_req_ptr = 0;
        int fd = -1;
        PyObject *events = NULL;
        if (!PyArg_ParseTuple(args, "liO:uwsgi_tornado_hook_fd", &wsgi_req_ptr, &fd, &events)) {
                return NULL;
        }

	uwsgi.wsgi_req = (struct wsgi_request *) wsgi_req_ptr;
	uwsgi.schedule_to_req();

	Py_INCREF(Py_None);
        return Py_None;
}

PyObject *py_uwsgi_tornado_hook_timeout(PyObject *self, PyObject *args) {
        long wsgi_req_ptr = 0;
        if (!PyArg_ParseTuple(args, "l", &wsgi_req_ptr)) {
                return NULL;
        }

        uwsgi.wsgi_req = (struct wsgi_request *) wsgi_req_ptr;
	uwsgi.wsgi_req->async_timed_out = 1;
        uwsgi.schedule_to_req();

        Py_INCREF(Py_None);
        return Py_None;
}

PyObject *py_uwsgi_tornado_hook_fix(PyObject *self, PyObject *args) {
        long wsgi_req_ptr = 0;
        if (!PyArg_ParseTuple(args, "l", &wsgi_req_ptr)) {
                return NULL;
        }

        uwsgi.wsgi_req = (struct wsgi_request *) wsgi_req_ptr;
        uwsgi.schedule_to_req();

        Py_INCREF(Py_None);
        return Py_None;
}


PyMethodDef uwsgi_tornado_accept_def[] = { {"uwsgi_tornado_accept", py_uwsgi_tornado_accept, METH_VARARGS, ""} };
PyMethodDef uwsgi_tornado_request_def[] = { {"uwsgi_tornado_request", py_uwsgi_tornado_request, METH_VARARGS, ""} };
PyMethodDef uwsgi_tornado_hook_fd_def[] = { {"uwsgi_tornado_hook_fd", py_uwsgi_tornado_hook_fd, METH_VARARGS, ""} };
PyMethodDef uwsgi_tornado_hook_timeout_def[] = { {"uwsgi_tornado_hook_timeout", py_uwsgi_tornado_hook_timeout, METH_VARARGS, ""} };
PyMethodDef uwsgi_tornado_hook_fix_def[] = { {"uwsgi_tornado_hook_fix", py_uwsgi_tornado_hook_fix, METH_VARARGS, ""} };

static void uwsgi_tornado_schedule_fix(struct wsgi_request *wsgi_req) {
	PyObject *cb_fix = PyObject_CallMethod(utornado.functools, "partial", "Ol", utornado.hook_fix, (long) wsgi_req);
        if (!cb_fix) goto error;

        if (PyObject_CallMethod(utornado.ioloop, "add_callback", "O", cb_fix) == NULL) {
                Py_DECREF(cb_fix);
                goto error;
        }

        Py_DECREF(cb_fix);
        return; 

error:
        PyErr_Print();
} 

static void tornado_loop() {

	if (!uwsgi.has_threads && uwsgi.mywid == 1) {
		uwsgi_log("!!! Running tornado without threads IS NOT recommended, enable them with --enable-threads !!!\n");
	}

	if (uwsgi.socket_timeout < 30) {
		uwsgi_log("!!! Running tornado with a socket-timeout lower than 30 seconds is not recommended, tune it with --socket-timeout !!!\n");
	}

	if (!uwsgi.async_waiting_fd_table)
                uwsgi.async_waiting_fd_table = uwsgi_calloc(sizeof(struct wsgi_request *) * uwsgi.max_fd);
        if (!uwsgi.async_proto_fd_table)
                uwsgi.async_proto_fd_table = uwsgi_calloc(sizeof(struct wsgi_request *) * uwsgi.max_fd);

	// get the GIL
	UWSGI_GET_GIL

	up.gil_get = gil_tornado_get;
	up.gil_release = gil_tornado_release;

	uwsgi.wait_write_hook = uwsgi_tornado_wait_write_hook;
	uwsgi.wait_read_hook = uwsgi_tornado_wait_read_hook;

	uwsgi.schedule_fix = uwsgi_tornado_schedule_fix;

	if (uwsgi.async < 2) {
		uwsgi_log("the tornado loop engine requires async mode (--async <n>)\n");
		exit(1);
	}

	if (!uwsgi.schedule_to_main) {
                uwsgi_log("*** DANGER *** tornado mode without coroutine/greenthread engine loaded !!!\n");
        }

	PyObject *tornado_dict = get_uwsgi_pydict("tornado.ioloop");
	if (!tornado_dict) uwsgi_pyexit;

	PyObject *tornado_IOLoop = PyDict_GetItemString(tornado_dict, "IOLoop");
	if (!tornado_IOLoop) uwsgi_pyexit;

	utornado.ioloop = PyObject_CallMethod(tornado_IOLoop, "instance", NULL);
	if (!utornado.ioloop) uwsgi_pyexit;


	 // main greenlet waiting for connection (one greenlet per-socket)
        PyObject *uwsgi_tornado_accept = PyCFunction_New(uwsgi_tornado_accept_def, NULL);
	Py_INCREF(uwsgi_tornado_accept);

	utornado.request = PyCFunction_New(uwsgi_tornado_request_def, NULL);
	if (!utornado.request) uwsgi_pyexit;
	utornado.hook_fd = PyCFunction_New(uwsgi_tornado_hook_fd_def, NULL);
	if (!utornado.hook_fd) uwsgi_pyexit;
	utornado.hook_timeout = PyCFunction_New(uwsgi_tornado_hook_timeout_def, NULL);
	if (!utornado.hook_timeout) uwsgi_pyexit;
	utornado.hook_fix = PyCFunction_New(uwsgi_tornado_hook_fix_def, NULL);
	if (!utornado.hook_fix) uwsgi_pyexit;

	utornado.read = PyObject_GetAttrString(utornado.ioloop, "READ");
	if (!utornado.read) uwsgi_pyexit;
	utornado.write = PyObject_GetAttrString(utornado.ioloop, "WRITE");
	if (!utornado.write) uwsgi_pyexit;

	utornado.functools = PyImport_ImportModule("functools"); 
	if (!utornado.functools)  uwsgi_pyexit;
	
	Py_INCREF(utornado.request);
	Py_INCREF(utornado.hook_fd);
	Py_INCREF(utornado.hook_timeout);
	Py_INCREF(utornado.hook_fix);
	Py_INCREF(utornado.read);
	Py_INCREF(utornado.write);

	// call add_handler on each socket
	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	while(uwsgi_sock) {
		if (PyObject_CallMethod(utornado.ioloop, "add_handler", "iOO", uwsgi_sock->fd, uwsgi_tornado_accept, utornado.read) == NULL) {
			uwsgi_pyexit;
		}
		uwsgi_sock = uwsgi_sock->next;
	}	

	if (PyObject_CallMethod(utornado.ioloop, "start", NULL) == NULL) {
		uwsgi_pyexit;
	}

	// never here ?
}

static void tornado_init() {

	uwsgi_register_loop( (char *) "tornado", tornado_loop);
}


struct uwsgi_plugin tornado_plugin = {

	.name = "tornado",
	.options = tornado_options,
	.on_load = tornado_init,
};
