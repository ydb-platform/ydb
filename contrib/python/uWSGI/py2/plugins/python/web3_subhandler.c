#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;
extern struct uwsgi_plugin python_plugin;
extern PyTypeObject uwsgi_InputType;

void *uwsgi_request_subhandler_web3(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {

	PyObject *zero;

	int i;
        PyObject *pydictkey, *pydictvalue;
        char *path_info;

        for (i = 0; i < wsgi_req->var_cnt; i += 2) {
#ifdef UWSGI_DEBUG
                uwsgi_debug("%.*s: %.*s\n", wsgi_req->hvec[i].iov_len, wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i+1].iov_len, wsgi_req->hvec[i+1].iov_base);
#endif
#ifdef PYTHREE
                pydictkey = PyUnicode_DecodeLatin1(wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i].iov_len, NULL);
                pydictvalue = PyUnicode_DecodeLatin1(wsgi_req->hvec[i + 1].iov_base, wsgi_req->hvec[i + 1].iov_len, NULL);
#else
                pydictkey = PyString_FromStringAndSize(wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i].iov_len);
                pydictvalue = PyString_FromStringAndSize(wsgi_req->hvec[i + 1].iov_base, wsgi_req->hvec[i + 1].iov_len);
#endif
                PyDict_SetItem(wsgi_req->async_environ, pydictkey, pydictvalue);
                Py_DECREF(pydictkey);
                Py_DECREF(pydictvalue);
        }

        if (wsgi_req->uh->modifier1 == UWSGI_MODIFIER_MANAGE_PATH_INFO) {
                wsgi_req->uh->modifier1 = python_plugin.modifier1;
                pydictkey = PyDict_GetItemString(wsgi_req->async_environ, "SCRIPT_NAME");
                if (pydictkey) {
                        if (PyString_Check(pydictkey)) {
                                pydictvalue = PyDict_GetItemString(wsgi_req->async_environ, "PATH_INFO");
                                if (pydictvalue) {
                                        if (PyString_Check(pydictvalue)) {
                                                path_info = PyString_AsString(pydictvalue);
                                                PyDict_SetItemString(wsgi_req->async_environ, "PATH_INFO", PyString_FromString(path_info + PyString_Size(pydictkey)));
                                        }
                                }
                        }
                }
        }

        // create wsgi.input custom object
        wsgi_req->async_input = (PyObject *) PyObject_New(uwsgi_Input, &uwsgi_InputType);
        ((uwsgi_Input*)wsgi_req->async_input)->wsgi_req = wsgi_req;

        PyDict_SetItemString(wsgi_req->async_environ, "web3.input", wsgi_req->async_input);

	PyDict_SetItemString(wsgi_req->async_environ, "web3.version", wi->gateway_version);

	zero = PyFile_FromFile(stderr, "web3_input", "w", NULL);
	PyDict_SetItemString(wsgi_req->async_environ, "web3.errors", zero);
	Py_DECREF(zero);

	PyDict_SetItemString(wsgi_req->async_environ, "web3.run_once", Py_False);

	PyDict_SetItemString(wsgi_req->async_environ, "web3.multithread", Py_False);
	if (uwsgi.numproc == 1) {
		PyDict_SetItemString(wsgi_req->async_environ, "web3.multiprocess", Py_False);
	}
	else {
		PyDict_SetItemString(wsgi_req->async_environ, "web3.multiprocess", Py_True);
	}

	if (wsgi_req->scheme_len > 0) {
		zero = UWSGI_PYFROMSTRINGSIZE(wsgi_req->scheme, wsgi_req->scheme_len);
	}
	else if (wsgi_req->https_len > 0) {
		if (!strncasecmp(wsgi_req->https, "on", 2) || wsgi_req->https[0] == '1') {
			zero = UWSGI_PYFROMSTRING("https");
		}
		else {
			zero = UWSGI_PYFROMSTRING("http");
		}
	}
	else {
		zero = UWSGI_PYFROMSTRING("http");
	}
	PyDict_SetItemString(wsgi_req->async_environ, "web3.url_scheme", zero);
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


int uwsgi_response_subhandler_web3(struct wsgi_request *wsgi_req) {

	PyObject *pychunk;

	// ok its a yield
	if (!wsgi_req->async_placeholder) {
		if (PyTuple_Check((PyObject *)wsgi_req->async_result)) {
			if (PyTuple_Size((PyObject *)wsgi_req->async_result) != 3) { 
				uwsgi_log("invalid Web3 response.\n"); 
				goto clear; 
			} 



			wsgi_req->async_placeholder = PyTuple_GetItem((PyObject *)wsgi_req->async_result, 0); 
			Py_INCREF((PyObject *)wsgi_req->async_placeholder);

			PyObject *spit_args = PyTuple_New(2);

			PyObject *status = PyTuple_GetItem((PyObject *)wsgi_req->async_result, 1);
			Py_INCREF(status);
			PyTuple_SetItem(spit_args, 0, status);

			PyObject *headers = PyTuple_GetItem((PyObject *)wsgi_req->async_result, 2);
			Py_INCREF(headers);
			PyTuple_SetItem(spit_args, 1, headers);

			if (py_uwsgi_spit(Py_None, spit_args) == NULL) { 
				PyErr_Print();
				Py_DECREF(spit_args);
				goto clear; 
			} 

			Py_DECREF(spit_args);

			if (PyString_Check((PyObject *)wsgi_req->async_placeholder)) {
				char *content = PyString_AsString(wsgi_req->async_placeholder);
				size_t content_len = PyString_Size(wsgi_req->async_placeholder);
				UWSGI_RELEASE_GIL
				uwsgi_response_write_body_do(wsgi_req, content, content_len);
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
			uwsgi_log("invalid Web3 response.\n"); 
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
		char *content = PyString_AsString(pychunk);
		size_t content_len = PyString_Size(pychunk);
		UWSGI_RELEASE_GIL
		uwsgi_response_write_body_do(wsgi_req, content, content_len);
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

