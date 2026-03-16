#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;
extern PyTypeObject uwsgi_InputType;


void *uwsgi_request_subhandler_pump(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {

	PyObject *zero;

	int i;
        PyObject *pydictkey, *pydictvalue;

	char *port = memchr(wsgi_req->host, ':', wsgi_req->host_len);
	if (port) {

		zero = PyString_FromStringAndSize(wsgi_req->host, (port-wsgi_req->host));
		PyDict_SetItemString(wsgi_req->async_environ, "server_name", zero);
		Py_DECREF(zero);

		zero = PyString_FromStringAndSize(port, wsgi_req->host_len-((port+1)-wsgi_req->host));
		PyDict_SetItemString(wsgi_req->async_environ, "server_port", zero);
		Py_DECREF(zero);
	}
	else {

		zero = PyString_FromStringAndSize(wsgi_req->host, wsgi_req->host_len);
		PyDict_SetItemString(wsgi_req->async_environ, "server_name", zero);
		Py_DECREF(zero);

		zero = PyString_FromStringAndSize("80", 2);
		PyDict_SetItemString(wsgi_req->async_environ, "server_port", zero);
		Py_DECREF(zero);
	}

	zero = PyString_FromStringAndSize(wsgi_req->remote_addr, wsgi_req->remote_addr_len);
	PyDict_SetItemString(wsgi_req->async_environ, "remote_addr", zero);
	Py_DECREF(zero);

	zero = PyString_FromStringAndSize(wsgi_req->path_info, wsgi_req->path_info_len);
	PyDict_SetItemString(wsgi_req->async_environ, "uri", zero);
	Py_DECREF(zero);

	if (wsgi_req->query_string_len > 0) {
		zero = PyString_FromStringAndSize(wsgi_req->query_string, wsgi_req->query_string_len);
		PyDict_SetItemString(wsgi_req->async_environ, "query_string", zero);
		Py_DECREF(zero);
	}

	zero = PyString_FromStringAndSize(uwsgi_lower(wsgi_req->method, wsgi_req->method_len), wsgi_req->method_len);
	PyDict_SetItemString(wsgi_req->async_environ, "method", zero);
	Py_DECREF(zero);

	if (wsgi_req->post_cl > 0) {
		PyDict_SetItemString(wsgi_req->async_environ, "content_length", PyInt_FromLong(wsgi_req->post_cl));
		if (wsgi_req->content_type_len > 0) {
			zero = PyString_FromStringAndSize(wsgi_req->content_type, wsgi_req->content_type_len);
                	PyDict_SetItemString(wsgi_req->async_environ, "content_type", zero);
                	Py_DECREF(zero);
		}
	}



	PyObject *headers = PyDict_New();

        for (i = 0; i < wsgi_req->var_cnt; i += 2) {
#ifdef UWSGI_DEBUG
                uwsgi_debug("%.*s: %.*s\n", wsgi_req->hvec[i].iov_len, wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i+1].iov_len, wsgi_req->hvec[i+1].iov_base);
#endif
		if (wsgi_req->hvec[i].iov_len < 6) continue;
		if (!uwsgi_startswith(wsgi_req->hvec[i].iov_base, "HTTP_", 5)) {
			(void) uwsgi_lower(wsgi_req->hvec[i].iov_base+5, wsgi_req->hvec[i].iov_len-5);
#ifdef PYTHREE
                	pydictkey = PyUnicode_DecodeLatin1(wsgi_req->hvec[i].iov_base+5, wsgi_req->hvec[i].iov_len-5, NULL);
                	pydictvalue = PyUnicode_DecodeLatin1(wsgi_req->hvec[i + 1].iov_base, wsgi_req->hvec[i + 1].iov_len, NULL);
#else
                	pydictkey = PyString_FromStringAndSize(wsgi_req->hvec[i].iov_base+5, wsgi_req->hvec[i].iov_len-5);
                	pydictvalue = PyString_FromStringAndSize(wsgi_req->hvec[i + 1].iov_base, wsgi_req->hvec[i + 1].iov_len);
#endif
			PyObject *old_value = PyDict_GetItem(headers, pydictkey);
			if (old_value) {
				if (PyString_Check(old_value)) {
					PyObject *new_value = PyList_New(0);
					PyList_Append(new_value, old_value);
					old_value = new_value;
                			PyDict_SetItem(headers, pydictkey, old_value);
					Py_DECREF(old_value);
				}
				PyList_Append(old_value, pydictvalue);
			}
			else {
                		PyDict_SetItem(headers, pydictkey, pydictvalue);
			}
                	Py_DECREF(pydictkey);
                	Py_DECREF(pydictvalue);
		}
        }

	PyDict_SetItemString(wsgi_req->async_environ, "headers", headers);
	Py_DECREF(headers);

        // create wsgi.input custom object
        wsgi_req->async_input = (PyObject *) PyObject_New(uwsgi_Input, &uwsgi_InputType);
        ((uwsgi_Input*)wsgi_req->async_input)->wsgi_req = wsgi_req;

        PyDict_SetItemString(wsgi_req->async_environ, "body", wsgi_req->async_input);

	if (wsgi_req->scheme_len > 0) {
		zero = PyString_FromStringAndSize(wsgi_req->scheme, wsgi_req->scheme_len);
	}
	else if (wsgi_req->https_len > 0) {
		if (!strncasecmp(wsgi_req->https, "on", 2) || wsgi_req->https[0] == '1') {
			zero = PyString_FromString("https");
		}
		else {
			zero = PyString_FromString("http");
		}
	}
	else {
		zero = PyString_FromString("http");
	}
	PyDict_SetItemString(wsgi_req->async_environ, "scheme", zero);
	Py_DECREF(zero);


	wsgi_req->async_app = wi->callable;

	// export .env only in non-threaded mode
        if (uwsgi.threads < 2) {
        	PyDict_SetItemString(up.embedded_dict, "env", wsgi_req->async_environ);
        }

        PyDict_SetItemString(wsgi_req->async_environ, "uwsgi.version", wi->uwsgi_version);

        if (uwsgi.cores > 1) {
                PyDict_SetItemString(wsgi_req->async_environ, "uwsgi.core", PyInt_FromLong(wsgi_req->async_id));
        }

        PyDict_SetItemString(wsgi_req->async_environ, "uwsgi.node", wi->uwsgi_node);

	// call

	if (PyTuple_GetItem(wsgi_req->async_args, 0) != wsgi_req->async_environ) {
	    if (PyTuple_SetItem(wsgi_req->async_args, 0, wsgi_req->async_environ)) {
	        uwsgi_log_verbose("unable to set environ to the python application callable, consider using the holy env allocator\n");
	        return NULL;
	    }
	}
	return python_call(wsgi_req->async_app, wsgi_req->async_args, uwsgi.catch_exceptions, wsgi_req);
}


int uwsgi_response_subhandler_pump(struct wsgi_request *wsgi_req) {

	PyObject *pychunk;
	int i;

	char sc[4];

	// ok its a yield
	if (!wsgi_req->async_placeholder) {
		if (PyDict_Check((PyObject *)wsgi_req->async_result)) {


			PyObject *status = PyDict_GetItemString((PyObject *)wsgi_req->async_result, "status");
			if (!status) {
				uwsgi_log("invalid Pump response (status code).\n"); 
				goto clear; 
			}

			PyObject *headers = PyDict_GetItemString((PyObject *)wsgi_req->async_result, "headers");
			if (!headers) {
				uwsgi_log("invalid Pump response (headers).\n"); 
				goto clear; 
			}


			wsgi_req->async_placeholder =  PyDict_GetItemString((PyObject *)wsgi_req->async_result, "body");
			if (!wsgi_req->async_placeholder) {
				uwsgi_log("invalid Pump response (body).\n"); 
				goto clear; 
			}

			// get the status code
			if (!PyInt_Check(status)) {
				uwsgi_log("invalid Pump response (status code).\n"); 
				goto clear; 
			}

			if (uwsgi_num2str2n(PyInt_AsLong(status), sc, 4) != 3) {
				uwsgi_log("invalid Pump response (status code).\n"); 
				goto clear; 
			}

			if (uwsgi_response_prepare_headers(wsgi_req, sc, 3)) {
				uwsgi_log("unable to prepare response headers\n");
			}

			PyObject *hhkey, *hhvalue;
#ifdef UWSGI_PYTHON_OLD
			int hhpos = 0;
#else
			Py_ssize_t hhpos = 0;
#endif
			while (PyDict_Next(headers, &hhpos, &hhkey, &hhvalue)) {
				if (!PyString_Check(hhkey)) continue;

				char *k = PyString_AsString(hhkey);
				size_t kl = PyString_Size(hhkey);	
				k[0] = toupper((int) k[0]);

				if (PyList_Check(hhvalue)) {
					for(i=0;i<PyList_Size(hhvalue);i++) {
						PyObject *item = PyList_GetItem(hhvalue, i);
						if (PyString_Check(item)) {
							if (uwsgi_response_add_header(wsgi_req, k, kl, PyString_AsString(item), PyString_Size(item))) goto clear;
						}
					}	
				}
				else if (PyString_Check(hhvalue)) {
					if (uwsgi_response_add_header(wsgi_req, k, kl, PyString_AsString(hhvalue), PyString_Size(hhvalue))) goto clear;
				}
			}

			Py_INCREF((PyObject *)wsgi_req->async_placeholder);

			if (PyString_Check((PyObject *)wsgi_req->async_placeholder)) {
				UWSGI_RELEASE_GIL
				uwsgi_response_write_body_do(wsgi_req, PyString_AsString(wsgi_req->async_placeholder), PyString_Size(wsgi_req->async_placeholder));
				UWSGI_GET_GIL
				uwsgi_py_check_write_errors {
                                        uwsgi_py_write_exception(wsgi_req);
                                }
                		goto clear;
        		}
#ifdef PYTHREE
			else if ((wsgi_req->sendfile_fd = PyObject_AsFileDescriptor((PyObject *)wsgi_req->async_placeholder)) > -1) {
#else
			else if (PyFile_Check((PyObject *)wsgi_req->async_placeholder)) {
				wsgi_req->sendfile_fd = fileno(PyFile_AsFile((PyObject *)wsgi_req->async_placeholder));
#endif
				UWSGI_RELEASE_GIL
                		uwsgi_response_sendfile_do(wsgi_req, wsgi_req->sendfile_fd, 0, 0);
                		UWSGI_GET_GIL
				uwsgi_py_check_write_errors {
                                        uwsgi_py_write_exception(wsgi_req);
                                }
				goto clear;
			}

			PyObject *tmp = (PyObject *)wsgi_req->async_placeholder;

			wsgi_req->async_placeholder = PyObject_GetIter( (PyObject *)wsgi_req->async_placeholder );

			Py_DECREF(tmp);

			if (!wsgi_req->async_placeholder) {
				goto clear;
			}

			if (uwsgi.async > 1) {
				return UWSGI_AGAIN;
			}

		}
		else {
			uwsgi_log("invalid Pump response.\n"); 
			goto clear; 
		}
	}



	pychunk = PyIter_Next(wsgi_req->async_placeholder);

	if (!pychunk) {
		if (PyErr_Occurred()) {
			uwsgi_manage_exception(wsgi_req, uwsgi.catch_exceptions);
		}
		goto clear;
	}


	if (PyString_Check(pychunk)) {
		UWSGI_RELEASE_GIL
		uwsgi_response_write_body_do(wsgi_req, PyString_AsString(pychunk), PyString_Size(pychunk));
		UWSGI_GET_GIL
		uwsgi_py_check_write_errors {
                	uwsgi_py_write_exception(wsgi_req);
			Py_DECREF(pychunk);
			goto clear;
                }
	}


	Py_DECREF(pychunk);
	return UWSGI_AGAIN;

clear:
	Py_XDECREF((PyObject *)wsgi_req->async_placeholder);

	Py_DECREF((PyObject *)wsgi_req->async_result);
	PyErr_Clear();

	return UWSGI_OK;
}

