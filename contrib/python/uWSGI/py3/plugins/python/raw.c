#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;

static int manage_raw_response(struct wsgi_request *wsgi_req) {
	int ret = 0;
	if (!wsgi_req->async_force_again) {
		ret = uwsgi_python_send_body(wsgi_req, (PyObject *) wsgi_req->async_result);
		if (ret == 0) {
			if (PyInt_Check((PyObject *) wsgi_req->async_result) || PyObject_HasAttrString((PyObject *) wsgi_req->async_result, "fileno")) {
				// is it a file ?
				int fd = PyObject_AsFileDescriptor((PyObject *) wsgi_req->async_result);
				if (fd >= 0) {
					wsgi_req->sendfile_fd = fd;
					uwsgi_response_sendfile_do(wsgi_req, fd, 0, 0);
					wsgi_req->sendfile_fd = -1;
					return UWSGI_OK;
				}
			}
		}
	}
	if (ret == 0) {
		if (!wsgi_req->async_placeholder) {
			wsgi_req->async_placeholder = PyObject_GetIter((PyObject *) wsgi_req->async_result);
			if (!wsgi_req->async_placeholder)
				return UWSGI_OK;
		}

		PyObject *pychunk = PyIter_Next((PyObject *) wsgi_req->async_placeholder);
		if (!pychunk)
			return UWSGI_OK;
		ret = uwsgi_python_send_body(wsgi_req, pychunk);
		if (ret == 0) {
			if (PyInt_Check(pychunk) || PyObject_HasAttrString(pychunk, "fileno")) {
				// is it a file ?
				int fd = PyObject_AsFileDescriptor(pychunk);
				if (fd >= 0) {
					wsgi_req->sendfile_fd = fd;
					uwsgi_response_sendfile_do(wsgi_req, fd, 0, 0);
					wsgi_req->sendfile_fd = -1;
				}
			}
		}
		Py_DECREF(pychunk);
		return UWSGI_AGAIN;
	}
	return UWSGI_OK;
}

int uwsgi_request_python_raw(struct wsgi_request *wsgi_req) {
	if (!up.raw_callable)
		return UWSGI_OK;

	// back from async
	if (wsgi_req->async_force_again) {
		UWSGI_GET_GIL int ret = manage_raw_response(wsgi_req);
		if (ret == UWSGI_AGAIN) {
			wsgi_req->async_force_again = 1;
			UWSGI_RELEASE_GIL return UWSGI_AGAIN;
		}
		goto end;
	}

	UWSGI_GET_GIL PyObject * args = PyTuple_New(1);
	PyTuple_SetItem(args, 0, PyInt_FromLong(wsgi_req->fd));
	wsgi_req->async_result = PyObject_CallObject(up.raw_callable, args);
	Py_DECREF(args);
	if (wsgi_req->async_result) {
		for (;;) {
			int ret = manage_raw_response(wsgi_req);
			if (ret == UWSGI_AGAIN) {
				wsgi_req->async_force_again = 1;
				if (uwsgi.async > 1) {
					UWSGI_RELEASE_GIL return UWSGI_AGAIN;
				}
				continue;
			}
			break;
		}
	}
end:
	if (PyErr_Occurred())
		PyErr_Print();
	if (wsgi_req->async_result) {
		Py_DECREF((PyObject *) wsgi_req->async_result);
	}
	UWSGI_RELEASE_GIL;
	return UWSGI_OK;
}
