#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;


char *uwsgi_python_get_thread_name(PyObject *thread_id) {
	PyObject *threading_module = PyImport_ImportModule("threading");
	if (!threading_module) return NULL;
	
	PyObject *threading_dict = PyModule_GetDict(threading_module);
	if (!threading_dict) return NULL;

	PyObject *threading_enumerate = PyDict_GetItemString(threading_dict, "enumerate");
	if (!threading_enumerate) return NULL;

	PyObject *threads_list = PyObject_CallObject(threading_enumerate, (PyObject *)NULL);
	if (!threads_list) return NULL;

	PyObject *threads_list_iter = PyObject_GetIter(threads_list);
	if (!threads_list_iter) goto clear;

	PyObject *threads_list_next = PyIter_Next(threads_list_iter);
	while(threads_list_next) {
		PyObject *thread_ident = PyObject_GetAttrString(threads_list_next, "ident");
		if (!thread_ident) goto clear2;
		if (PyInt_AsLong(thread_ident) == PyInt_AsLong(thread_id)) {
			PyObject *thread_name = PyObject_GetAttrString(threads_list_next, "name");
			if (!thread_name) goto clear2;
#ifdef PYTHREE
			PyObject *thread_name_utf8 = PyUnicode_AsEncodedString(thread_name, "ASCII", "strict");
			if (!thread_name_utf8) goto clear2;
			char *name = NULL;
			char *tmp_name = PyBytes_AS_STRING(thread_name_utf8);
			if (tmp_name) {
				name = uwsgi_str(tmp_name);	
				Py_DECREF(thread_name_utf8);
			}
#else
			char *name = PyString_AsString(thread_name);
#endif
			Py_DECREF(threads_list_next);
			Py_DECREF(threads_list_iter);
			Py_DECREF(threads_list);
			return name;
		}
		Py_DECREF(threads_list_next);
		threads_list_next = PyIter_Next(threads_list_iter);
	}

clear2:
	Py_DECREF(threads_list_iter);
clear:
	Py_DECREF(threads_list);
	return NULL;
}

void *uwsgi_python_tracebacker_thread(void *foobar) {

	struct iovec iov[11];

	PyObject *new_thread = uwsgi_python_setup_thread("uWSGITraceBacker");
	if (!new_thread) return NULL;

	struct sockaddr_un so_sun;
	socklen_t so_sun_len = 0;

	char *str_wid = uwsgi_num2str(uwsgi.mywid);
	char *sock_path = uwsgi_concat2(up.tracebacker, str_wid);

	int current_defer_accept = uwsgi.no_defer_accept;
        uwsgi.no_defer_accept = 1;
	int fd = bind_to_unix(sock_path, uwsgi.listen_queue, uwsgi.chmod_socket, uwsgi.abstract_socket);
	if (fd < 0) {
		uwsgi.no_defer_accept = current_defer_accept;
		free(str_wid);
		free(sock_path);
		return NULL;
	}
        uwsgi.no_defer_accept = current_defer_accept;

	PyObject *traceback_module = PyImport_ImportModule("traceback");
	if (!traceback_module) {
		free(str_wid);
		free(sock_path);
		close(fd);
		return NULL;
	}
	PyObject *traceback_dict = PyModule_GetDict(traceback_module);
	PyObject *extract_stack = PyDict_GetItemString(traceback_dict, "extract_stack");

	PyObject *sys_module = PyImport_ImportModule("sys");
	PyObject *sys_dict = PyModule_GetDict(sys_module);

	PyObject *_current_frames = PyDict_GetItemString(sys_dict, "_current_frames");

	uwsgi_log("python tracebacker for worker %d available on %s\n", uwsgi.mywid, sock_path);

	for(;;) {
		UWSGI_RELEASE_GIL;
		int client_fd = accept(fd, (struct sockaddr *) &so_sun, &so_sun_len);
		if (client_fd < 0) {
			uwsgi_error("accept()");
			UWSGI_GET_GIL;
			continue;
		}
		UWSGI_GET_GIL;
// here is the core of the tracebacker
		PyObject *current_frames = PyObject_CallObject(_current_frames, (PyObject *)NULL);
		if (!current_frames) goto end2;

		PyObject *current_frames_items = PyObject_GetAttrString(current_frames, "items");
		if (!current_frames_items) goto end;

		PyObject *frames_ret = PyObject_CallObject(current_frames_items, (PyObject *)NULL);
		if (!frames_ret) goto end3;

		PyObject *frames_iter = PyObject_GetIter(frames_ret);
		if (!frames_iter) goto end4;


		// we have the first frame, lets parse it
		if (write(client_fd, "*** uWSGI Python tracebacker output ***\n\n", 41) < 0) {
			uwsgi_error("write()");
		}
		PyObject *frame = PyIter_Next(frames_iter);
		while(frame) {
			PyObject *thread_id = PyTuple_GetItem(frame, 0);
			if (!thread_id) goto next2;

			PyObject *stack = PyTuple_GetItem(frame, 1);
			if (!stack) goto next2;

			PyObject *arg_tuple = PyTuple_New(1);
			PyTuple_SetItem(arg_tuple, 0, stack);
			Py_INCREF(stack);
			PyObject *stacktrace = PyObject_CallObject( extract_stack, arg_tuple);
			Py_DECREF(arg_tuple);
			if (!stacktrace) goto next2;
			
			PyObject *stacktrace_iter = PyObject_GetIter(stacktrace);
			if (!stacktrace_iter) { Py_DECREF(stacktrace); goto next2;}

			PyObject *st_items = PyIter_Next(stacktrace_iter);
			// we have the first traceback item
			while(st_items) {
#ifdef PYTHREE
				int thread_name_need_free = 0;

			        	     	
				PyObject *st_filename = PyObject_GetAttrString(st_items, "filename");
				if (!st_filename) { Py_DECREF(st_items); goto next; }
				PyObject *st_lineno = PyObject_GetAttrString(st_items, "lineno");
				if (!st_lineno) {Py_DECREF(st_items); goto next;}
				PyObject *st_name = PyObject_GetAttrString(st_items, "name");
				if (!st_name) {Py_DECREF(st_items); goto next;}

				PyObject *st_line = PyObject_GetAttrString(st_items, "line");
#else
				PyObject *st_filename = PyTuple_GetItem(st_items, 0);
 				if (!st_filename) { Py_DECREF(st_items); goto next; }
				PyObject *st_lineno = PyTuple_GetItem(st_items, 1);
 				if (!st_lineno) {Py_DECREF(st_items); goto next;}
				PyObject *st_name = PyTuple_GetItem(st_items, 2);
 				if (!st_name) {Py_DECREF(st_items); goto next;}
 
				PyObject *st_line = PyTuple_GetItem(st_items, 3);
#endif				
				iov[0].iov_base = "thread_id = ";
				iov[0].iov_len = 12;

				iov[1].iov_base = uwsgi_python_get_thread_name(thread_id);
				if (!iov[1].iov_base) {
					iov[1].iov_base = "<UnnamedPythonThread>";
				}
#ifdef PYTHREE
				else {
					thread_name_need_free = 1;
				}
#endif
				iov[1].iov_len = strlen(iov[1].iov_base);

				iov[2].iov_base = " filename = ";
				iov[2].iov_len = 12;

#ifdef PYTHREE
				PyObject *st_filename_utf8 = PyUnicode_AsEncodedString(st_filename, "ASCII", "strict");
				if (!st_filename_utf8) {
					if (thread_name_need_free) free(iov[1].iov_base);
					goto next;
				}
				iov[3].iov_base = PyBytes_AS_STRING(st_filename_utf8);
#else
				iov[3].iov_base = PyString_AsString(st_filename);
#endif
				iov[3].iov_len = strlen(iov[3].iov_base);

				iov[4].iov_base = " lineno = ";
				iov[4].iov_len = 10 ;

				iov[5].iov_base = uwsgi_num2str(PyInt_AsLong(st_lineno));
				iov[5].iov_len = strlen(iov[5].iov_base);

				iov[6].iov_base = " function = ";
				iov[6].iov_len = 12 ;

#ifdef PYTHREE
                                PyObject *st_name_utf8 = PyUnicode_AsEncodedString(st_name, "ASCII", "strict");
                                if (!st_name_utf8) {
					if (thread_name_need_free) free(iov[1].iov_base);
					Py_DECREF(st_filename_utf8);
					goto next;
				}
				iov[7].iov_base = PyBytes_AS_STRING(st_name_utf8);
#else
				iov[7].iov_base = PyString_AsString(st_name);
#endif
                                iov[7].iov_len = strlen(iov[7].iov_base);

				iov[8].iov_base = "";
				iov[8].iov_len = 0 ;

				iov[9].iov_base = "";
				iov[9].iov_len = 0;

				iov[10].iov_base = "\n";
				iov[10].iov_len = 1;

#ifdef PYTHREE
				PyObject *st_line_utf8 = NULL;
#endif
				if (st_line) {
					iov[8].iov_base = " line = ";
					iov[8].iov_len = 8;
#ifdef PYTHREE
                                	PyObject *st_line_utf8 = PyUnicode_AsEncodedString(st_line, "ASCII", "strict");
                                	if (!st_line_utf8) {
						if (thread_name_need_free) free(iov[1].iov_base);
                                        	Py_DECREF(st_filename_utf8);
                                        	Py_DECREF(st_name_utf8);
                                        	goto next;
                                	}
					iov[9].iov_base = PyBytes_AS_STRING(st_line_utf8);
#else
					iov[9].iov_base = PyString_AsString(st_line);
#endif
					iov[9].iov_len = strlen(iov[9].iov_base);
				}

				if (writev(client_fd, iov, 11) < 0) {
					uwsgi_error("writev()");
				}

				// free the line_no
				free(iov[5].iov_base);
				Py_DECREF(st_items);
#ifdef PYTHREE
				Py_DECREF(st_filename_utf8);
				Py_DECREF(st_name_utf8);
				if (st_line_utf8) {
					Py_DECREF(st_line_utf8);
				}
				if (thread_name_need_free)
					free(iov[1].iov_base);
#endif
				st_items = PyIter_Next(stacktrace_iter);
			}
			if (write(client_fd, "\n", 1) < 0) {
				uwsgi_error("write()");
			}
next:
			Py_DECREF(stacktrace_iter);
			Py_DECREF(stacktrace);
next2:
			Py_DECREF(frame);
			frame = PyIter_Next(frames_iter);
		}

		Py_DECREF(frames_iter);
end4:	
		Py_DECREF(frames_ret);
end3:	
		Py_DECREF(current_frames_items);
end:
		Py_DECREF(current_frames);
end2:
		close(client_fd);
	}
	return NULL;
}
