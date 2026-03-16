#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;

int manage_python_response(struct wsgi_request *wsgi_req) {
	// use standard WSGI response parse
	return uwsgi_response_subhandler_wsgi(wsgi_req);
}

char *uwsgi_python_get_exception_type(PyObject *exc) {
	char *class_name = NULL;
#if !defined(PYTHREE)
	if (PyClass_Check(exc)) {
		class_name = PyString_AsString( ((PyClassObject*)(exc))->cl_name );
	}
	else {
#endif
		class_name = (char *) ((PyTypeObject*)exc)->tp_name;
#if !defined(PYTHREE)
	}
#endif

	if (class_name) {
		char *dot = strrchr(class_name, '.');
		if (dot) class_name = dot+1;

		PyObject *module_name = PyObject_GetAttrString(exc, "__module__");
		if (module_name) {
#ifdef PYTHREE
			char *mod_name = NULL;
			PyObject *zero = PyUnicode_AsUTF8String(module_name);
			if (zero) {
				mod_name = PyString_AsString(zero);
			}
#else
			char *mod_name = PyString_AsString(module_name);
#endif
			if (mod_name && strcmp(mod_name, "exceptions") ) {
				char *ret = uwsgi_concat3(mod_name, ".", class_name);
#ifdef PYTHREE
				Py_DECREF(zero);
#endif
				Py_DECREF(module_name);
				return ret;
			}
			Py_DECREF(module_name);
			return uwsgi_str(class_name);
		}
	}

	return NULL;
}

struct uwsgi_buffer *uwsgi_python_backtrace(struct wsgi_request *wsgi_req) {
	PyObject *type = NULL;
        PyObject *value = NULL;
        PyObject *traceback = NULL;
	struct uwsgi_buffer *ub = NULL;

	PyErr_Fetch(&type, &value, &traceback);
        PyErr_NormalizeException(&type, &value, &traceback);

	// traceback could not be available
	if (!traceback) goto end;

	PyObject *traceback_module = PyImport_ImportModule("traceback");
	if (!traceback_module) {
		goto end;
	}

	PyObject *traceback_dict = PyModule_GetDict(traceback_module);
	PyObject *extract_tb = PyDict_GetItemString(traceback_dict, "extract_tb");
	if (!extract_tb) goto end;
	PyObject *args = PyTuple_New(1);
	Py_INCREF(traceback);
	PyTuple_SetItem(args, 0, traceback);
	PyObject *result = PyObject_CallObject(extract_tb, args);
	Py_DECREF(args);

	if (!result) goto end;

	ub = uwsgi_buffer_new(4096);
	Py_ssize_t i;
	// we have to build a uwsgi array with 5 items (4 are taken from the python tb)
	for(i=0;i< PySequence_Size(result);i++) {
		PyObject *t = PySequence_GetItem(result, i);
		PyObject *tb_filename = PySequence_GetItem(t, 0);
		PyObject *tb_lineno = PySequence_GetItem(t, 1);
		PyObject *tb_function = PySequence_GetItem(t, 2);
		PyObject *tb_text = PySequence_GetItem(t, 3);

		int64_t line_no = PyInt_AsLong(tb_lineno);
#ifdef PYTHREE
		PyObject *zero = NULL;
		if (tb_filename) {
			zero = PyUnicode_AsUTF8String(tb_filename);
			if (!zero) goto end0;

			// filename
                	if (uwsgi_buffer_u16le(ub, PyString_Size(zero))) { Py_DECREF(zero); goto end0; }
                	if (uwsgi_buffer_append(ub, PyString_AsString(zero), PyString_Size(zero))) { Py_DECREF(zero); goto end0; }

			Py_DECREF(zero);
		}
		else {
                	if (uwsgi_buffer_u16le(ub, 0)) { goto end0; }
		}

                // lineno
                if (uwsgi_buffer_append_valnum(ub, line_no)) goto end0;

		if (tb_function) {
			zero = PyUnicode_AsUTF8String(tb_function);
                	if (!zero) goto end0;

                	// function
                	if (uwsgi_buffer_u16le(ub, PyString_Size(zero))) { Py_DECREF(zero); goto end0; }
                	if (uwsgi_buffer_append(ub, PyString_AsString(zero), PyString_Size(zero))) { Py_DECREF(zero); goto end0; }
			Py_DECREF(zero);
		}
		else {
                	if (uwsgi_buffer_u16le(ub, 0)) { goto end0; }
		}


		if (tb_text) {
			zero = PyUnicode_AsUTF8String(tb_text);
                	if (!zero) goto end0;

                	// text
                	if (uwsgi_buffer_u16le(ub, PyString_Size(zero))) { Py_DECREF(zero); goto end0; }
                	if (uwsgi_buffer_append(ub, PyString_AsString(zero), PyString_Size(zero))) { Py_DECREF(zero); goto end0; }

			Py_DECREF(zero);
		}
		else {
                	if (uwsgi_buffer_u16le(ub, 0)) { goto end0; }
		}
		
#else
		// filename
		if (uwsgi_buffer_u16le(ub, PyString_Size(tb_filename))) goto end0;
		if (uwsgi_buffer_append(ub, PyString_AsString(tb_filename), PyString_Size(tb_filename))) goto end0;

		// lineno
		if (uwsgi_buffer_append_valnum(ub, line_no)) goto end0;

		// function
		if (uwsgi_buffer_u16le(ub, PyString_Size(tb_function))) goto end0;
                if (uwsgi_buffer_append(ub, PyString_AsString(tb_function), PyString_Size(tb_function))) goto end0;

		// text
		if (uwsgi_buffer_u16le(ub, PyString_Size(tb_text))) goto end0;
                if (uwsgi_buffer_append(ub, PyString_AsString(tb_text), PyString_Size(tb_text))) goto end0;
#endif

		// custom (unused)
		if (uwsgi_buffer_u16le(ub, 0)) goto end0;
                if (uwsgi_buffer_append(ub, "", 0)) goto end0;
		
	}

	Py_DECREF(result);
	goto end;

end0:
	Py_DECREF(result);
	uwsgi_buffer_destroy(ub);
	ub = NULL;
end:
        PyErr_Restore(type, value, traceback);
        return ub;
}


struct uwsgi_buffer *uwsgi_python_exception_class(struct wsgi_request *wsgi_req) {
	PyObject *type = NULL;
        PyObject *value = NULL;
        PyObject *traceback = NULL;
	struct uwsgi_buffer *ub = NULL;

	PyErr_Fetch(&type, &value, &traceback);
	PyErr_NormalizeException(&type, &value, &traceback);

	char *class = uwsgi_python_get_exception_type(type);
	if (class) {
		size_t class_len = strlen(class);
		ub = uwsgi_buffer_new(class_len);
		if (uwsgi_buffer_append(ub, class, class_len)) {
			uwsgi_buffer_destroy(ub);
			ub = NULL;
			goto end;
		}
	}
end:
	free(class);
	PyErr_Restore(type, value, traceback);
	return ub;
}

struct uwsgi_buffer *uwsgi_python_exception_msg(struct wsgi_request *wsgi_req) {
        PyObject *type = NULL;
        PyObject *value = NULL;
        PyObject *traceback = NULL;
        struct uwsgi_buffer *ub = NULL;

        PyErr_Fetch(&type, &value, &traceback);
        PyErr_NormalizeException(&type, &value, &traceback);

	// value could be NULL ?
	if (!value) goto end;

#ifdef PYTHREE
	char *msg = NULL;
	PyObject *zero = PyUnicode_AsUTF8String( PyObject_Str(value) );
	if (zero) {
        	msg = PyString_AsString( zero );
	}
#else
        char *msg = PyString_AsString( PyObject_Str(value) );
#endif
        if (msg) {
                size_t msg_len = strlen(msg);
                ub = uwsgi_buffer_new(msg_len);
                if (uwsgi_buffer_append(ub, msg, msg_len)) {
#ifdef PYTHREE
			Py_DECREF(zero);
#endif
                        uwsgi_buffer_destroy(ub);
                        ub = NULL;
                        goto end;
                }
#ifdef PYTHREE
		Py_DECREF(zero);
#endif
        }
end:
        PyErr_Restore(type, value, traceback);
        return ub;
}

struct uwsgi_buffer *uwsgi_python_exception_repr(struct wsgi_request *wsgi_req) {
	
	struct uwsgi_buffer *ub_class = uwsgi_python_exception_class(wsgi_req);
	if (!ub_class) return NULL;

	struct uwsgi_buffer *ub_msg = uwsgi_python_exception_msg(wsgi_req);
	if (!ub_msg) {
		uwsgi_buffer_destroy(ub_class);
		return NULL;
	}

	struct uwsgi_buffer *ub = uwsgi_buffer_new(ub_class->pos + 2 + ub_msg->pos);
	if (uwsgi_buffer_append(ub, ub_class->buf, ub_class->pos)) goto error;
	if (uwsgi_buffer_append(ub, ": ", 2)) goto error;
	if (uwsgi_buffer_append(ub, ub_msg->buf, ub_msg->pos)) goto error;

	uwsgi_buffer_destroy(ub_class);
	uwsgi_buffer_destroy(ub_msg);

	return ub;

error:
	uwsgi_buffer_destroy(ub_class);
	uwsgi_buffer_destroy(ub_msg);
	uwsgi_buffer_destroy(ub);
	return NULL;
}

void uwsgi_python_exception_log(struct wsgi_request *wsgi_req) {
	PyErr_Print();
}

PyObject *python_call(PyObject *callable, PyObject *args, int catch, struct wsgi_request *wsgi_req) {

	//uwsgi_log("ready to call %p %p\n", callable, args);

	PyObject *pyret = PyObject_CallObject(callable, args);

	//uwsgi_log("called\n");

	if (PyErr_Occurred()) {
		if (wsgi_req) {
			uwsgi_manage_exception(wsgi_req, catch);
		}
		else {
			PyErr_Print();
		}
	}

#ifdef UWSGI_DEBUG
	if (pyret) {
		uwsgi_debug("called %p %p %d\n", callable, args, pyret->ob_refcnt);
	}
#endif

	return pyret;
}

int uwsgi_python_call(struct wsgi_request *wsgi_req, PyObject *callable, PyObject *args) {

	wsgi_req->async_result = python_call(callable, args, 0, wsgi_req);

	if (wsgi_req->async_result) {
		while ( manage_python_response(wsgi_req) != UWSGI_OK) {
			if (uwsgi.async > 1) {
				return UWSGI_AGAIN;
			}
		}
	}

	return UWSGI_OK;
}

void init_pyargv() {

	char *ap;

	char *argv0 = "uwsgi";

	if (up.pyrun) {
		argv0 = up.pyrun;
	}

#ifdef PYTHREE
	wchar_t *pname = uwsgi_calloc(sizeof(wchar_t) * (strlen(argv0)+1));
	mbstowcs(pname, argv0, strlen(argv0)+1);
#else
	char *pname = argv0;
#endif

	up.argc = 1;
	if (up.argv) {
		char *tmp_ptr = uwsgi_str(up.argv);
#ifdef __sun__
                // FIX THIS !!!
		char *ctx = NULL;
                ap = strtok_r(tmp_ptr, " ", &ctx);
                while ((ap = strtok_r(NULL, " ", &ctx)) != NULL) {
#else
                while ((ap = strsep(&tmp_ptr, " \t")) != NULL) {
#endif
			if (*ap != '\0') {
				up.argc++;
			}
		}

		free(tmp_ptr);
	}

#ifdef PYTHREE
	up.py_argv = uwsgi_calloc(sizeof(wchar_t *) * up.argc+1);
#else
	up.py_argv = uwsgi_calloc(sizeof(char *) * up.argc+1);
#endif

	up.py_argv[0] = pname;


	if (up.argv) {

		char *py_argv_copy = uwsgi_str(up.argv);
		up.argc = 1;
#ifdef PYTHREE
		wchar_t *wcargv = uwsgi_calloc( sizeof( wchar_t ) * (strlen(py_argv_copy)+1));
#endif

#ifdef __sun__
		// FIX THIS !!!
		char *ctx = NULL;
		ap = strtok_r(py_argv_copy, " ", &ctx);
		while ((ap = strtok_r(NULL, " ", &ctx)) != NULL) {
#else
		while ((ap = strsep(&py_argv_copy, " \t")) != NULL) {
#endif
				if (*ap != '\0') {
#ifdef PYTHREE
					mbstowcs( wcargv, ap, strlen(ap));
					up.py_argv[up.argc] = wcargv;
					wcargv += strlen(ap) + 1;
#else
					up.py_argv[up.argc] = ap;
#endif
					up.argc++;
				}
		}

	}

	PySys_SetArgv(up.argc, up.py_argv);

	PyObject *sys_dict = get_uwsgi_pydict("sys");
	if (!sys_dict) {
		uwsgi_log("unable to load python sys module !!!\n");
		exit(1);
	}
	if (!up.executable)
		up.executable = uwsgi.binary_path;
#ifdef PYTHREE
	PyDict_SetItemString(sys_dict, "executable", PyUnicode_FromString(up.executable));
#else
	PyDict_SetItemString(sys_dict, "executable", PyString_FromString(up.executable));
#endif


}
