#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;
extern struct uwsgi_plugin python_plugin;


PyObject *uwsgi_Input_iter(PyObject *self) {
        Py_INCREF(self);
        return self;
}

PyObject *uwsgi_Input_getline(uwsgi_Input *self, long hint) {
	struct wsgi_request *wsgi_req = self->wsgi_req;
	ssize_t rlen = 0;

	UWSGI_RELEASE_GIL
	char *buf = uwsgi_request_body_readline(wsgi_req, hint, &rlen);
	UWSGI_GET_GIL
	if (buf == uwsgi.empty) {
		return PyString_FromString("");
	}
	if (buf) {
		return PyString_FromStringAndSize(buf, rlen);
	}

	if (rlen < 0) {
       		return PyErr_Format(PyExc_IOError, "error during readline(%ld) on wsgi.input", hint);
        }

	return PyErr_Format(PyExc_IOError, "timeout during readline(%ld) on wsgi.input", hint);
}

PyObject *uwsgi_Input_next(PyObject* self) {

	PyObject *line = uwsgi_Input_getline((uwsgi_Input *)self, 0);
	if (!line) return NULL;

	if (PyString_Size(line) == 0) {
		Py_DECREF(line);
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	return line;

}

static void uwsgi_Input_free(uwsgi_Input *self) {
    	PyObject_Del(self);
}

static PyObject *uwsgi_Input_read(uwsgi_Input *self, PyObject *args) {

	long arg_len = 0;

	if (!PyArg_ParseTuple(args, "|l:read", &arg_len)) {
		return NULL;
	}

	struct wsgi_request *wsgi_req = self->wsgi_req;
	ssize_t rlen = 0;

	UWSGI_RELEASE_GIL
	char *buf = uwsgi_request_body_read(wsgi_req, arg_len, &rlen);
	UWSGI_GET_GIL
	if (buf == uwsgi.empty) {
		return PyString_FromString("");
	}

	if (buf) {
		return PyString_FromStringAndSize(buf, rlen);
	}

	// error ?
	if (rlen < 0) {
       		return PyErr_Format(PyExc_IOError, "error during read(%ld) on wsgi.input", arg_len);
	}

	// timeout ?
       	return PyErr_Format(PyExc_IOError, "timeout during read(%ld) on wsgi.input", arg_len);
		
}

static PyObject *uwsgi_Input_readline(uwsgi_Input *self, PyObject *args) {

	long hint = 0;

	if (!PyArg_ParseTuple(args, "|l:readline", &hint)) {
                return NULL;
        }

	PyObject *line = uwsgi_Input_getline(self, hint);
	if (!line) return NULL;

	if (PyString_Size(line) == 0) {
		Py_DECREF(line);
		return PyString_FromString("");
	}
	return line;

}

static PyObject *uwsgi_Input_readlines(uwsgi_Input *self, PyObject *args) {

	long hint = 0;

        if (!PyArg_ParseTuple(args, "|l:readline", &hint)) {
                return NULL;
        }


	PyObject *res = PyList_New(0);
	for(;;) {
		PyObject *line = uwsgi_Input_getline(self, hint);
		if (!line) {
			Py_DECREF(res);
			return NULL;
		}
		if (PyString_Size(line) == 0) {
			Py_DECREF(line);
			return res;
		}
		PyList_Append(res, line);	
		Py_DECREF(line);
	}

	return res;
}

static PyObject *uwsgi_Input_close(uwsgi_Input *self, PyObject *args) {

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *uwsgi_Input_seek(uwsgi_Input *self, PyObject *args) {
	long pos = 0;
	int whence = 0;

	if (!uwsgi.post_buffering) {
		return PyErr_Format(PyExc_IOError, "seeking wsgi.input without post_buffering is IMPOSSIBLE !!!");
	}

	if (!PyArg_ParseTuple(args, "l|i:seek", &pos, &whence)) {
                return NULL;
        }

	/*
		uwsgi_request_body_seek() uses SEEK_SET for positive value and SEEK_CUR for negative
		yous hould always try to transform the "pos" value to an absolute position.
	*/

	// current
	if (whence == 1) {
		pos += self->wsgi_req->post_pos;
	}
	// end of stream
	if (whence == 2) {
		pos += self->wsgi_req->post_cl;
	}
	
	if (pos < 0 || pos > (off_t)self->wsgi_req->post_cl) {
		return PyErr_Format(PyExc_IOError, "invalid seek position for wsgi.input");
	}
	
	uwsgi_request_body_seek(self->wsgi_req, pos);

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *uwsgi_Input_fileno(uwsgi_Input *self, PyObject *args) {

	return PyInt_FromLong(self->wsgi_req->fd);
}

static PyObject *uwsgi_Input_tell(uwsgi_Input *self, PyObject *args) {

        return PyLong_FromLong(self->wsgi_req->post_pos);
}


static PyMethodDef uwsgi_Input_methods[] = {
	{ "read",      (PyCFunction)uwsgi_Input_read,      METH_VARARGS, 0 },
	{ "readline",  (PyCFunction)uwsgi_Input_readline,  METH_VARARGS, 0 },
	{ "readlines", (PyCFunction)uwsgi_Input_readlines, METH_VARARGS, 0 },
// add close to allow mod_wsgi compatibility
	{ "close",     (PyCFunction)uwsgi_Input_close,     METH_VARARGS, 0 },
	{ "seek",     (PyCFunction)uwsgi_Input_seek,     METH_VARARGS, 0 },
	{ "tell",     (PyCFunction)uwsgi_Input_tell,     METH_VARARGS, 0 },
	{ "fileno",     (PyCFunction)uwsgi_Input_fileno,     METH_VARARGS, 0 },
	{ NULL, NULL}
};


PyTypeObject uwsgi_InputType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "uwsgi._Input",  /*tp_name */
        sizeof(uwsgi_Input),     /*tp_basicsize */
        0,                      /*tp_itemsize */
        (destructor) uwsgi_Input_free,	/*tp_dealloc */
        0,                      /*tp_print */
        0,                      /*tp_getattr */
        0,                      /*tp_setattr */
        0,                      /*tp_compare */
        0,                      /*tp_repr */
        0,                      /*tp_as_number */
        0,                      /*tp_as_sequence */
        0,                      /*tp_as_mapping */
        0,                      /*tp_hash */
        0,                      /*tp_call */
        0,                      /*tp_str */
        0,                      /*tp_getattr */
        0,                      /*tp_setattr */
        0,                      /*tp_as_buffer */
#if defined(Py_TPFLAGS_HAVE_ITER)
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
#else
        Py_TPFLAGS_DEFAULT,
#endif
        "uwsgi input object.",      /* tp_doc */
        0,                      /* tp_traverse */
        0,                      /* tp_clear */
        0,                      /* tp_richcompare */
        0,                      /* tp_weaklistoffset */
        uwsgi_Input_iter,        /* tp_iter: __iter__() method */
        uwsgi_Input_next,         /* tp_iternext: next() method */
	uwsgi_Input_methods,
	0,0,0,0,0,0,0,0,0,0,0,0
};


PyObject *py_uwsgi_write(PyObject * self, PyObject * args) {
	PyObject *data;
	char *content;
	size_t content_len;

	struct wsgi_request *wsgi_req = py_current_wsgi_req();

	data = PyTuple_GetItem(args, 0);
	if (PyString_Check(data)) {
		content = PyString_AsString(data);
		content_len = PyString_Size(data);
		UWSGI_RELEASE_GIL
		uwsgi_response_write_body_do(wsgi_req, content, content_len);
		UWSGI_GET_GIL
		// this is a special case for the write callable
		// no need to honout write-errors-exception-only
		if (wsgi_req->write_errors > uwsgi.write_errors_tolerance && !uwsgi.disable_write_exception) {
                        uwsgi_py_write_set_exception(wsgi_req);
			return NULL;
		}
	}

	Py_INCREF(Py_None);
	return Py_None;
}

PyObject *py_eventfd_read(PyObject * self, PyObject * args) {
	int fd, timeout = 0;

	struct wsgi_request *wsgi_req = py_current_wsgi_req();

	if (!PyArg_ParseTuple(args, "i|i", &fd, &timeout)) {
		return NULL;
	}

	if (async_add_fd_read(wsgi_req, fd, timeout)) {
		return PyErr_Format(PyExc_IOError, "unable to fd %d to the event queue", fd);
	}

	return PyString_FromString("");
}


PyObject *py_eventfd_write(PyObject * self, PyObject * args) {
	int fd, timeout = 0;

	struct wsgi_request *wsgi_req = py_current_wsgi_req();

	if (!PyArg_ParseTuple(args, "i|i", &fd, &timeout)) {
		return NULL;
	}

	if (async_add_fd_write(wsgi_req, fd, timeout)) {
		return PyErr_Format(PyExc_IOError, "unable to fd %d to the event queue", fd);
	}

	return PyString_FromString("");
}

int uwsgi_request_wsgi(struct wsgi_request *wsgi_req) {

	if (wsgi_req->is_raw) return uwsgi_request_python_raw(wsgi_req);

	struct uwsgi_app *wi;

	if (wsgi_req->async_force_again) {
		wi = &uwsgi_apps[wsgi_req->app_id];
		wsgi_req->async_force_again = 0;
		UWSGI_GET_GIL
		// get rid of timeout
		if (wsgi_req->async_timed_out) {
			PyDict_SetItemString(wsgi_req->async_environ, "x-wsgiorg.fdevent.timeout", Py_True);
			wsgi_req->async_timed_out = 0;
		}
		else {
			PyDict_SetItemString(wsgi_req->async_environ, "x-wsgiorg.fdevent.timeout", Py_None);
		}

		if (wsgi_req->async_ready_fd) {
			PyDict_SetItemString(wsgi_req->async_environ, "uwsgi.ready_fd", PyInt_FromLong(wsgi_req->async_last_ready_fd));
			wsgi_req->async_ready_fd = 0;
		}
		else {
			PyDict_SetItemString(wsgi_req->async_environ, "uwsgi.ready_fd", Py_None);
		}
		int ret = manage_python_response(wsgi_req);
		if (ret == UWSGI_OK) goto end;
		UWSGI_RELEASE_GIL
		if (ret == UWSGI_AGAIN) {
			wsgi_req->async_force_again = 1;
		}
		return ret;
	}

	/* Standard WSGI request */
	if (!wsgi_req->uh->pktsize) {
		uwsgi_log( "Empty python request. skip.\n");
		return -1;
	}

	if (uwsgi_parse_vars(wsgi_req)) {
		return -1;
	}

	if (wsgi_req->dynamic) {
        	// this part must be heavy locked in threaded modes
                if (uwsgi.threads > 1) {
                	pthread_mutex_lock(&up.lock_pyloaders);
                }
	}

	if ( (wsgi_req->app_id = uwsgi_get_app_id(wsgi_req, wsgi_req->appid, wsgi_req->appid_len, python_plugin.modifier1))  == -1) {
		if (wsgi_req->dynamic) {
			UWSGI_GET_GIL
			if (uwsgi.single_interpreter) {
				wsgi_req->app_id = init_uwsgi_app(LOADER_DYN, (void *) wsgi_req, wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
			}
			else {
				wsgi_req->app_id = init_uwsgi_app(LOADER_DYN, (void *) wsgi_req, wsgi_req, NULL, PYTHON_APP_TYPE_WSGI);
			}
			UWSGI_RELEASE_GIL
		}

		if (wsgi_req->app_id == -1 && !uwsgi.no_default_app && uwsgi.default_app > -1) {
                	if (uwsgi_apps[uwsgi.default_app].modifier1 == python_plugin.modifier1) {
                        	wsgi_req->app_id = uwsgi.default_app;
                        }
		}
	}

	if (wsgi_req->dynamic) {
		if (uwsgi.threads > 1) {
			pthread_mutex_unlock(&up.lock_pyloaders);
		}
	}


	if (wsgi_req->app_id == -1) {
		uwsgi_500(wsgi_req);
		uwsgi_log("--- no python application found, check your startup logs for errors ---\n");
		goto clear2;
	}

	wi = &uwsgi_apps[wsgi_req->app_id];

	up.swap_ts(wsgi_req, wi);

	
	if (wi->chdir[0] != 0) {
#ifdef UWSGI_DEBUG
		uwsgi_debug("chdir to %s\n", wi->chdir);
#endif
		if (chdir(wi->chdir)) {
			uwsgi_error("chdir()");
		}
	}


	UWSGI_GET_GIL

	// no fear of race conditions for this counter as it is already protected by the GIL
	wi->requests++;

	// create WSGI environ
	wsgi_req->async_environ = up.wsgi_env_create(wsgi_req, wi);


	wsgi_req->async_result = wi->request_subhandler(wsgi_req, wi);


	if (wsgi_req->async_result) {


		while (wi->response_subhandler(wsgi_req) != UWSGI_OK) {
			if (uwsgi.async > 1) {
				UWSGI_RELEASE_GIL
				wsgi_req->async_force_again = 1;
				return UWSGI_AGAIN;
			}
			else {
				wsgi_req->switches++;
			}
		}


	}

	// this object must be freed/cleared always
end:
	if (wsgi_req->async_input) {
                Py_DECREF((PyObject *)wsgi_req->async_input);
        }
        if (wsgi_req->async_environ) {
		up.wsgi_env_destroy(wsgi_req);
        }

	UWSGI_RELEASE_GIL

	up.reset_ts(wsgi_req, wi);

clear2:

	return UWSGI_OK;

}

void uwsgi_after_request_wsgi(struct wsgi_request *wsgi_req) {

	if (up.after_req_hook) {
		if (uwsgi.harakiri_no_arh) {
			// leave harakiri mode
        		if (uwsgi.workers[uwsgi.mywid].harakiri > 0)
                		set_harakiri(0);
		}
		UWSGI_GET_GIL
		PyObject *arh = python_call(up.after_req_hook, up.after_req_hook_args, 0, NULL);
        	if (!arh) {
			uwsgi_manage_exception(wsgi_req, 0);
                }
		else {
			Py_DECREF(arh);
		}
		PyErr_Clear();
		UWSGI_RELEASE_GIL
	}

	log_request(wsgi_req);
}

PyObject *py_uwsgi_sendfile(PyObject * self, PyObject * args) {
	int chunk_size;
	PyObject *filelike;
	struct wsgi_request *wsgi_req = py_current_wsgi_req();

	if (!PyArg_ParseTuple(args, "O|i:uwsgi_sendfile", &filelike, &chunk_size)) {
		return NULL;
	}

	if (!PyObject_HasAttrString(filelike, "read")) {
		PyErr_SetString(PyExc_AttributeError, "object has no attribute 'read'");
		return NULL;
	}

	// wsgi.file_wrapper called a second time? Forget the old reference.
	if (wsgi_req->async_sendfile) {
		Py_DECREF(wsgi_req->async_sendfile);
	}

	// XXX: Not 100% sure why twice.
	//      Maybe: We keep one at async_sendfile and transfer
	//      one to the caller (even though he gave it to us).
	Py_INCREF(filelike);
	Py_INCREF(filelike);
	wsgi_req->async_sendfile = filelike;
	wsgi_req->sendfile_fd_chunk = chunk_size;
	return filelike;
}

void threaded_swap_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {

	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
		UWSGI_GET_GIL
                PyThreadState_Swap(uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].ts[wsgi_req->app_id]);
		UWSGI_RELEASE_GIL
	}

}

void threaded_reset_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {
	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
		UWSGI_GET_GIL
        	PyThreadState_Swap((PyThreadState *) pthread_getspecific(up.upt_save_key));
        	UWSGI_RELEASE_GIL
	}
}


void simple_reset_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {
	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
        	// restoring main interpreter
                PyThreadState_Swap(up.main_thread);
	}
}


void simple_swap_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {

	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
                // set the interpreter
                PyThreadState_Swap(wi->interpreter);
	}
}

void simple_threaded_reset_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {
	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
        	// restoring main interpreter
		UWSGI_GET_GIL
                PyThreadState_Swap(up.main_thread);
		UWSGI_RELEASE_GIL
	}
}


void simple_threaded_swap_ts(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {

	if (uwsgi.single_interpreter == 0 && wi->interpreter != up.main_thread) {
                // set the interpreter
		UWSGI_GET_GIL
                PyThreadState_Swap(wi->interpreter);
		UWSGI_RELEASE_GIL
	}
}
