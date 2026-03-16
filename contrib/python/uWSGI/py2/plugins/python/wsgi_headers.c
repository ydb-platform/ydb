#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;

// check here

PyObject *py_uwsgi_spit(PyObject * self, PyObject * args) {
	PyObject *headers, *head;
	PyObject *h_key, *h_value;
	PyObject *exc_info = NULL;
	size_t i;

	struct wsgi_request *wsgi_req = py_current_wsgi_req();

	// avoid double sending of headers
	if (wsgi_req->headers_sent) {
		return PyErr_Format(PyExc_IOError, "headers already sent");
	}

	// this must be done before headers management
	if (PyTuple_Size(args) > 2) {
		exc_info = PyTuple_GetItem(args, 2);
		if (exc_info && exc_info != Py_None) {
			PyObject *exc_type = PyTuple_GetItem(exc_info, 0);	
			PyObject *exc_val = PyTuple_GetItem(exc_info, 1);
			PyObject *exc_tb = PyTuple_GetItem(exc_info, 2);

			if (!exc_type || !exc_val || !exc_tb) {
				return NULL;
			}

			Py_INCREF(exc_type);
			Py_INCREF(exc_val);
			Py_INCREF(exc_tb);
			// in this way, error will be reported to the log
			PyErr_Restore(exc_type, exc_val, exc_tb);

			// the error is reported, let's continue...
			// return NULL
		}
	}

	head = PyTuple_GetItem(args, 0);
	if (!head) {
		return PyErr_Format(PyExc_TypeError, "start_response() takes at least 2 arguments");
	}

#ifdef PYTHREE
	// check for web3
        if ((self != Py_None && !PyUnicode_Check(head)) || (self == Py_None && !PyBytes_Check(head))) {
#else
	if (!PyString_Check(head)) {
#endif
		return PyErr_Format(PyExc_TypeError, "http status must be a string");
	}

	char *status_line = NULL;
	size_t status_line_len = 0;
#ifdef PYTHREE
	PyObject *zero = NULL;
	PyObject *zero2 = NULL;
		if (self != Py_None) {
			zero = PyUnicode_AsLatin1String(head);
			if (!zero) {
				return PyErr_Format(PyExc_TypeError, "http status string must be encodable in latin1");
			}
			status_line = PyBytes_AsString(zero);
			status_line_len = PyBytes_Size(zero);
		}
		else {
			status_line = PyBytes_AsString(head);
			status_line_len = PyBytes_Size(head);
		}
#else
		status_line = PyString_AsString(head);
		status_line_len = PyString_Size(head);
#endif
	if (uwsgi_response_prepare_headers(wsgi_req, status_line, status_line_len)) {
#ifdef PYTHREE
		Py_DECREF(zero);
#endif
		goto end;
	}

#ifdef PYTHREE
	Py_DECREF(zero);
#endif

	headers = PyTuple_GetItem(args, 1);
	if (!headers) {
		return PyErr_Format(PyExc_TypeError, "start_response() takes at least 2 arguments");
	}

	if (!PyList_Check(headers)) {
		return PyErr_Format(PyExc_TypeError, "http headers must be in a python list");
	}

	size_t h_count = PyList_Size(headers);

	for (i = 0; i < h_count; i++) {
		head = PyList_GetItem(headers, i);
		if (!head) {
			return NULL;
		}
		if (!PyTuple_Check(head)) {
			return PyErr_Format(PyExc_TypeError, "http header must be defined in a tuple");
		}
		h_key = PyTuple_GetItem(head, 0);
		if (!h_key) {
			return PyErr_Format(PyExc_TypeError, "http header must be a 2-item tuple");
		}
#ifdef PYTHREE
		if ((self != Py_None && !PyUnicode_Check(h_key)) || (self == Py_None && !PyBytes_Check(h_key))) {
#else
        	if (!PyString_Check(h_key)) {
#endif
			return PyErr_Format(PyExc_TypeError, "http header key must be a string");
		}
		h_value = PyTuple_GetItem(head, 1);
		if (!h_value) {
			return PyErr_Format(PyExc_TypeError, "http header must be a 2-item tuple");
		}
#ifdef PYTHREE
		if ((self != Py_None && !PyUnicode_Check(h_value)) || (self == Py_None && !PyBytes_Check(h_value))) {
#else
        	if (!PyString_Check(h_value)) {
#endif
			return PyErr_Format(PyExc_TypeError, "http header value must be a string");
		}

		
		char *k = NULL; size_t kl = 0;
		char *v = NULL; size_t vl = 0;

#ifdef PYTHREE
		if (self != Py_None) {
			zero = PyUnicode_AsLatin1String(h_key);
			if (!zero) {
				return PyErr_Format(PyExc_TypeError, "http header must be encodable in latin1");
			}
			k = PyBytes_AsString(zero);
			kl = PyBytes_Size(zero);
		}
		else {
			k = PyBytes_AsString(h_key);
			kl = PyBytes_Size(h_key);
		}
#else
		k = PyString_AsString(h_key);
		kl = PyString_Size(h_key);
#endif

#ifdef PYTHREE
		if (self != Py_None) {
			zero2 = PyUnicode_AsLatin1String(h_value);
			if (!zero2) {
				return PyErr_Format(PyExc_TypeError, "http header must be encodable in latin1");
			}
			v = PyBytes_AsString(zero2);
			vl = PyBytes_Size(zero2);
		}
		else {
			v = PyBytes_AsString(h_value);
			vl = PyBytes_Size(h_value);
		}
#else
		v = PyString_AsString(h_value);
		vl = PyString_Size(h_value);
#endif

		if (uwsgi_response_add_header(wsgi_req, k, kl, v, vl)) {
#ifdef PYTHREE
			Py_DECREF(zero);
			Py_DECREF(zero2);
#endif
			return PyErr_Format(PyExc_TypeError, "unable to add header to the response");
		}

#ifdef PYTHREE
		Py_DECREF(zero);
		Py_DECREF(zero2);
#endif

	}

	if (up.start_response_nodelay) {
		UWSGI_RELEASE_GIL
		if (uwsgi_response_write_headers_do(wsgi_req)) {
			UWSGI_GET_GIL
			return PyErr_Format(PyExc_IOError, "unable to directly send headers");
		}
		UWSGI_GET_GIL
	}

end:
	Py_INCREF(up.wsgi_writeout);
	return up.wsgi_writeout;
}

