#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

/* notes

   exit(1) on every malloc error: apps can be dinamically loaded so on memory problem
   it is better to let the master process manager respawn the worker.

   TODO dynamic loading on prefork+thread looks flaky... NEED TO FIX IT
   */

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;
extern struct uwsgi_plugin python_plugin;

extern char **environ;

PyMethodDef uwsgi_sendfile_method[] = {{"uwsgi_sendfile", py_uwsgi_sendfile, METH_VARARGS, ""}};

PyMethodDef uwsgi_eventfd_read_method[] = { {"uwsgi_eventfd_read", py_eventfd_read, METH_VARARGS, ""}};
PyMethodDef uwsgi_eventfd_write_method[] = { {"uwsgi_eventfd_write", py_eventfd_write, METH_VARARGS, ""}};

void set_dyn_pyhome(char *home, uint16_t pyhome_len) {


	char venv_version[30];
	PyObject *site_module;

	PyObject *pysys_dict = get_uwsgi_pydict("sys");

	PyObject *pypath = PyDict_GetItemString(pysys_dict, "path");
	if (!pypath) {
		PyErr_Print();
		exit(1);
	}

        // simulate a pythonhome directive
        if (uwsgi.wsgi_req->home_len > 0) {

                PyObject *venv_path = UWSGI_PYFROMSTRINGSIZE(uwsgi.wsgi_req->home, uwsgi.wsgi_req->home_len);

#ifdef UWSGI_DEBUG
                uwsgi_debug("setting dynamic virtualenv to %.*s\n", uwsgi.wsgi_req->home_len, uwsgi.wsgi_req->home);
#endif

                PyDict_SetItemString(pysys_dict, "prefix", venv_path);
                PyDict_SetItemString(pysys_dict, "exec_prefix", venv_path);

                bzero(venv_version, 30);
                if (snprintf(venv_version, 30, "/lib/python%d.%d", PY_MAJOR_VERSION, PY_MINOR_VERSION) == -1) {
                        return;
                }

                // check here
                PyString_Concat(&venv_path, PyString_FromString(venv_version));

                if (PyList_Insert(pypath, 0, venv_path)) {
                        PyErr_Print();
                }

                site_module = PyImport_ImportModule("site");
                if (site_module) {
                        PyImport_ReloadModule(site_module);
                }

        }
}


int init_uwsgi_app(int loader, void *arg1, struct wsgi_request *wsgi_req, PyThreadState *interpreter, int app_type) {

	PyObject *app_list = NULL, *applications = NULL;


	if (uwsgi_apps_cnt >= uwsgi.max_apps) {
		uwsgi_log("ERROR: you cannot load more than %d apps in a worker\n", uwsgi.max_apps);
		return -1;
	}


	int id = uwsgi_apps_cnt;

	int multiapp = 0;

	int i;

	struct uwsgi_app *wi;

	time_t now = uwsgi_now();

	if (uwsgi_get_app_id(NULL, wsgi_req->appid, wsgi_req->appid_len, -1) != -1) {
		uwsgi_log( "mountpoint %.*s already configured. skip.\n", wsgi_req->appid_len, wsgi_req->appid);
		return -1;
	}

	wi = &uwsgi_apps[id];

	memset(wi, 0, sizeof(struct uwsgi_app));
	wi->modifier1 = python_plugin.modifier1;
	wi->mountpoint_len = wsgi_req->appid_len < 0xff ? wsgi_req->appid_len : (0xff-1);
	if (wi->mountpoint_len > 0) {
		strncpy(wi->mountpoint, wsgi_req->appid, wi->mountpoint_len);
	}

	// dynamic chdir ?
	if (wsgi_req->chdir_len > 0) {
		strncpy(wi->chdir, wsgi_req->chdir, wsgi_req->chdir_len < 0xff ? wsgi_req->chdir_len : (0xff-1));
#ifdef UWSGI_DEBUG
		uwsgi_debug("chdir to %s\n", wi->chdir);
#endif
		if (chdir(wi->chdir)) {
			uwsgi_error("chdir()");
		}
	}

	// Initialize a new environment for the new interpreter

	// reload "os" environ to allow dynamic setenv()
	if (up.reload_os_env) {

                char **e, *p;
                PyObject *k, *env_value;

        	PyObject *os_module = PyImport_ImportModule("os");
        	if (os_module) {
                	PyObject *os_module_dict = PyModule_GetDict(os_module);
                	PyObject *py_environ = PyDict_GetItemString(os_module_dict, "environ");
			if (py_environ) {
                		for (e = environ; *e != NULL; e++) {
                        		p = strchr(*e, '=');
                        		if (p == NULL) continue;

#ifdef PYTHREE
					k = PyUnicode_FromStringAndSize(*e, (int)(p-*e));
#else
					k = PyString_FromStringAndSize(*e, (int)(p-*e));
#endif
					if (k == NULL) {
                                		PyErr_Print();
                                		continue;
					}

#ifdef PYTHREE
					env_value = PyUnicode_FromString(p+1);
#else
					env_value = PyString_FromString(p+1);
#endif
                        		if (env_value == NULL) {
                                		PyErr_Print();
						Py_DECREF(k);
                                		continue;
                        		}

#ifdef UWSGI_DEBUG
#ifdef PYTHREE
					uwsgi_log("%s = %s\n", PyUnicode_AsUTF8(k), PyUnicode_AsUTF8(env_value));
#else
					uwsgi_log("%s = %s\n", PyString_AsString(k), PyString_AsString(env_value));
#endif
#endif

                        		if (PyObject_SetItem(py_environ, k, env_value)) {
                                		PyErr_Print();
                        		}

                        		Py_DECREF(k);
                        		Py_DECREF(env_value);

                	}

		}
        	}
	}

	if (interpreter == NULL && id) {

		wi->interpreter = Py_NewInterpreter();
		if (!wi->interpreter) {
			uwsgi_log( "unable to initialize the new python interpreter\n");
			exit(1);
		}
		PyThreadState_Swap(wi->interpreter);
		init_pyargv();

		// we need to initialize an embedded module for every interpreter
		init_uwsgi_embedded_module();
		init_uwsgi_vars();

	}
	else if (interpreter) {
		wi->interpreter = interpreter;
	}
	else {
		wi->interpreter = up.main_thread;
	}

	if (wsgi_req->home_len) {
		set_dyn_pyhome(wsgi_req->home, wsgi_req->home_len);
	}

	if (wsgi_req->touch_reload_len > 0 && wsgi_req->touch_reload_len < 0xff) {
		struct stat trst;
		strncpy(wi->touch_reload, wsgi_req->touch_reload, wsgi_req->touch_reload_len);
		if (!stat(wi->touch_reload, &trst)) {
			wi->touch_reload_mtime = trst.st_mtime;
		}
	}

	wi->callable = up.loaders[loader](arg1);

	if (!wi->callable) {
		uwsgi_log("unable to load app %d (mountpoint='%s') (callable not found or import error)\n", id, wi->mountpoint);
		goto doh;
	}

	// the module contains multiple apps
	if (PyDict_Check((PyObject *)wi->callable)) {
		applications = wi->callable;
		uwsgi_log("found a multiapp module...\n");
		app_list = PyDict_Keys(applications);
		multiapp = PyList_Size(app_list);
		if (multiapp < 1) {
			uwsgi_log("you have to define at least one app in the applications dictionary\n");
			goto doh;
		}

		PyObject *app_mnt = PyList_GetItem(app_list, 0);
		if (!PyString_Check(app_mnt)) {
			uwsgi_log("the app mountpoint must be a string\n");
			goto doh;
		}
		char *tmp_mountpoint = PyString_AsString(app_mnt);
		wi->mountpoint_len = strlen(wi->mountpoint) < 0xff ? strlen(wi->mountpoint) : (0xff-1);
		strncpy(wi->mountpoint, tmp_mountpoint, wi->mountpoint_len);
		wsgi_req->appid = wi->mountpoint;
		wsgi_req->appid_len = wi->mountpoint_len;
#ifdef UWSGI_DEBUG
		uwsgi_log("main mountpoint = %s\n", wi->mountpoint);
#endif
		wi->callable = PyDict_GetItem(applications, app_mnt);
		if (PyString_Check((PyObject *) wi->callable)) {
			PyObject *callables_dict = get_uwsgi_pydict((char *)arg1);
			if (callables_dict) {
				wi->callable = PyDict_GetItem(callables_dict, (PyObject *)wi->callable);
				if (!wi->callable) {
					uwsgi_log("skipping broken app %s\n", wsgi_req->appid);
					goto multiapp;
				}
			}
		}
	}

	Py_INCREF((PyObject *)wi->callable);

	wi->environ = malloc(sizeof(PyObject*)*uwsgi.cores);
	if (!wi->environ) {
		uwsgi_error("malloc()");
		exit(1);
	}

	for(i=0;i<uwsgi.cores;i++) {
		wi->environ[i] = PyDict_New();
		if (!wi->environ[i]) {
			uwsgi_log("unable to allocate new env dictionary for app\n");
			exit(1);
		}
	}

	wi->argc = 1;

	if (app_type == PYTHON_APP_TYPE_WSGI) {
#ifdef UWSGI_DEBUG
		uwsgi_log("-- WSGI callable selected --\n");
#endif
		wi->request_subhandler = uwsgi_request_subhandler_wsgi;
		wi->response_subhandler = uwsgi_response_subhandler_wsgi;
		wi->argc = 2;
	}
	else if (app_type == PYTHON_APP_TYPE_WEB3) {
#ifdef UWSGI_DEBUG
		uwsgi_log("-- Web3 callable selected --\n");
#endif
		wi->request_subhandler = uwsgi_request_subhandler_web3;
		wi->response_subhandler = uwsgi_response_subhandler_web3;
	}
	else if (app_type == PYTHON_APP_TYPE_PUMP) {
#ifdef UWSGI_DEBUG
		uwsgi_log("-- Pump callable selected --\n");
#endif
		wi->request_subhandler = uwsgi_request_subhandler_pump;
		wi->response_subhandler = uwsgi_response_subhandler_pump;
	}

	wi->args = malloc(sizeof(PyObject*)*uwsgi.cores);
	if (!wi->args) {
		uwsgi_error("malloc()");
		exit(1);
	}

	for(i=0;i<uwsgi.cores;i++) {
		wi->args[i] = PyTuple_New(wi->argc);
		if (!wi->args[i]) {
			uwsgi_log("unable to allocate new tuple for app args\n");
			exit(1);
		}
		Py_INCREF(Py_None);
		PyTuple_SetItem(wi->args[i], 0, Py_None);

		// add start_response on WSGI app
		if (app_type == PYTHON_APP_TYPE_WSGI) {
			Py_INCREF((PyObject *)up.wsgi_spitout);
			if (PyTuple_SetItem(wi->args[i], 1, up.wsgi_spitout)) {
				uwsgi_log("unable to set start_response in args tuple\n");
				exit(1);
			}
		}
	}

	if (app_type == PYTHON_APP_TYPE_WSGI) {
		// prepare sendfile() for WSGI app
		wi->sendfile = PyCFunction_New(uwsgi_sendfile_method, NULL);

		wi->eventfd_read = PyCFunction_New(uwsgi_eventfd_read_method, NULL);
		wi->eventfd_write = PyCFunction_New(uwsgi_eventfd_write_method, NULL);
	}

	// cache most used values
	wi->error = PyFile_FromFile(stderr, "wsgi_errors", "w", NULL);
	Py_INCREF((PyObject *)wi->error);

	wi->gateway_version = PyTuple_New(2);
        PyTuple_SetItem(wi->gateway_version, 0, PyInt_FromLong(1));
        PyTuple_SetItem(wi->gateway_version, 1, PyInt_FromLong(0));
	Py_INCREF((PyObject *)wi->gateway_version);

	wi->uwsgi_version = PyString_FromString(UWSGI_VERSION);
	Py_INCREF((PyObject *)wi->uwsgi_version);

	wi->uwsgi_node = PyString_FromString(uwsgi.hostname);
	Py_INCREF((PyObject *)wi->uwsgi_node);

	if (uwsgi.threads > 1 && id) {
		// if we have multiple threads we need to initialize a PyThreadState for each one
		for(i=0;i<uwsgi.threads;i++) {
			uwsgi.workers[uwsgi.mywid].cores[i].ts[id] = PyThreadState_New( ((PyThreadState *)wi->interpreter)->interp);
			//uwsgi_log("%p\n", uwsgi.workers[uwsgi.mywid].cores[i].ts[id]);
			if (!uwsgi.workers[uwsgi.mywid].cores[i].ts[id]) {
				uwsgi_log("unable to allocate new PyThreadState structure for app %s", wi->mountpoint);
				goto doh;
			}
			// cow ?
			if (uwsgi.mywid == 0) {
				int j;
				for(j=1;j<=uwsgi.numproc;j++) {
					uwsgi.workers[j].cores[i].ts[id] = uwsgi.workers[0].cores[i].ts[id];
				}
			}
		}
		PyThreadState_Swap((PyThreadState *) pthread_getspecific(up.upt_save_key) );
	}
	else if (interpreter == NULL && id) {
		PyThreadState_Swap(up.main_thread);
	}

	const char *default_app = "";

	if (!uwsgi.no_default_app) {
		if ((wsgi_req->appid_len == 0 || (wsgi_req->appid_len = 1 && wsgi_req->appid[0] == '/')) && uwsgi.default_app == -1) {
			default_app = " (default app)" ;
			uwsgi.default_app = id;
		}
	}

	wi->started_at = now;
	wi->startup_time = uwsgi_now() - now;

	if (app_type == PYTHON_APP_TYPE_WSGI) {
		uwsgi_log( "WSGI app %d (mountpoint='%.*s') ready in %d seconds on interpreter %p pid: %d%s\n", id, wi->mountpoint_len, wi->mountpoint, (int) wi->startup_time, wi->interpreter, (int) getpid(), default_app);
	}
	else if (app_type == PYTHON_APP_TYPE_WEB3) {
		uwsgi_log( "Web3 app %d (mountpoint='%.*s') ready in %d seconds on interpreter %p pid: %d%s\n", id, wi->mountpoint_len, wi->mountpoint, (int) wi->startup_time, wi->interpreter, (int) getpid(), default_app);
	}
	else if (app_type == PYTHON_APP_TYPE_PUMP) {
		uwsgi_log( "Pump app %d (mountpoint='%.*s') ready in %d seconds on interpreter %p pid: %d%s\n", id, wi->mountpoint_len, wi->mountpoint, (int) wi->startup_time, wi->interpreter, (int) getpid(), default_app);
	}


	uwsgi_apps_cnt++;

multiapp:
	if (multiapp > 1) {
		for(i=1;i<multiapp;i++) {
			PyObject *app_mnt = PyList_GetItem(app_list, i);
			if (!PyString_Check(app_mnt)) {
				uwsgi_log("applications dictionary key must be a string, skipping.\n");
				continue;
			}

			wsgi_req->appid = PyString_AsString(app_mnt);
			wsgi_req->appid_len = strlen(wsgi_req->appid);
			PyObject *a_callable = PyDict_GetItem(applications, app_mnt);
			if (PyString_Check(a_callable)) {

				PyObject *callables_dict = get_uwsgi_pydict((char *)arg1);
				if (callables_dict) {
					a_callable = PyDict_GetItem(callables_dict, a_callable);
				}
			}
			if (!a_callable) {
				uwsgi_log("skipping broken app %s\n", wsgi_req->appid);
				continue;
			}
			init_uwsgi_app(LOADER_CALLABLE, a_callable, wsgi_req, wi->interpreter, app_type);
		}
	}

	// emulate COW
	uwsgi_emulate_cow_for_apps(id);

	return id;

doh:
	if (PyErr_Occurred())
		PyErr_Print();
	if (interpreter == NULL && id) {
		Py_EndInterpreter(wi->interpreter);
		if (uwsgi.threads > 1) {
			PyThreadState_Swap((PyThreadState *) pthread_getspecific(up.upt_save_key));
		}
		else {
			PyThreadState_Swap(up.main_thread);
		}
	}
	return -1;
}

char *get_uwsgi_pymodule(char *module) {

	char *quick_callable;

	if ( (quick_callable = strchr(module, ':')) ) {
		quick_callable[0] = 0;
		quick_callable++;
		return quick_callable;
	}

	return NULL;
}

PyObject *get_uwsgi_pydict(char *module) {

	PyObject *wsgi_module, *wsgi_dict;

	wsgi_module = PyImport_ImportModule(module);
	if (!wsgi_module) {
		PyErr_Print();
		return NULL;
	}

	wsgi_dict = PyModule_GetDict(wsgi_module);
	if (!wsgi_dict) {
		PyErr_Print();
		return NULL;
	}

	return wsgi_dict;

}

PyObject *uwsgi_uwsgi_loader(void *arg1) {

	PyObject *wsgi_dict;

	char *quick_callable;

	PyObject *tmp_callable;
	PyObject *applications;
	PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");

	char *module = (char *) arg1;

	quick_callable = get_uwsgi_pymodule(module);
	if (quick_callable == NULL) {
		if (up.callable) {
			quick_callable = up.callable;
		}
		else {
			quick_callable = "application";
		}
		wsgi_dict = get_uwsgi_pydict(module);
	}
	else {
		wsgi_dict = get_uwsgi_pydict(module);
		module[strlen(module)] = ':';
	}

	if (!wsgi_dict) {
		return NULL;
	}

	applications = PyDict_GetItemString(uwsgi_dict, "applications");
	if (applications && PyDict_Check(applications)) return applications;

	applications = PyDict_GetItemString(wsgi_dict, "applications");
	if (applications && PyDict_Check(applications)) return applications;

	// quick callable -> thanks gunicorn for the idea
	// we have extended the concept a bit...
	if (quick_callable[strlen(quick_callable) -2 ] == '(' && quick_callable[strlen(quick_callable) -1] ==')') {
		quick_callable[strlen(quick_callable) -2 ] = 0;
		tmp_callable = PyDict_GetItemString(wsgi_dict, quick_callable);
		quick_callable[strlen(quick_callable)] = '(';
		if (tmp_callable) {
			return python_call(tmp_callable, PyTuple_New(0), 0, NULL);
		}
	}

	return PyDict_GetItemString(wsgi_dict, quick_callable);

}

/* this is the mount loader, it loads app on mountpoint automagically */
PyObject *uwsgi_mount_loader(void *arg1) {

	PyObject *callable = NULL;
	char *what = (char *) arg1;

	if ( !strcmp(what+strlen(what)-3, ".py") || !strcmp(what+strlen(what)-5, ".wsgi")) {
		callable = uwsgi_file_loader((void *)what);
		if (!callable) exit(UWSGI_FAILED_APP_CODE);
	}
	else if (!strcmp(what+strlen(what)-4, ".ini")) {
		callable = uwsgi_paste_loader((void *)what);
	}
	else if (strchr(what, ':')) {
		callable = uwsgi_uwsgi_loader((void *)what);
	}

	return callable;
}


/* this is the dynamic loader, it loads app reading information from a wsgi_request */
PyObject *uwsgi_dyn_loader(void *arg1) {

	PyObject *callable = NULL;
	char *tmpstr;

	struct wsgi_request *wsgi_req = (struct wsgi_request *) arg1;

	// MANAGE UWSGI_SCRIPT
	if (wsgi_req->script_len > 0) {
		tmpstr = uwsgi_strncopy(wsgi_req->script, wsgi_req->script_len);
		callable = uwsgi_uwsgi_loader((void *)tmpstr);
		free(tmpstr);
	}
	// MANAGE UWSGI_MODULE
	else if (wsgi_req->module_len > 0) {
		if (wsgi_req->callable_len > 0) {
			tmpstr = uwsgi_concat3n(wsgi_req->module, wsgi_req->module_len, ":", 1, wsgi_req->callable, wsgi_req->callable_len);
		}
		else {
			tmpstr = uwsgi_strncopy(wsgi_req->module, wsgi_req->module_len);
		}
		callable = uwsgi_uwsgi_loader((void *)tmpstr);
		free(tmpstr);
	}
	// MANAGE UWSGI_FILE
	else if (wsgi_req->file_len > 0) {
		tmpstr = uwsgi_strncopy(wsgi_req->file, wsgi_req->file_len);
		callable = uwsgi_file_loader((void *)tmpstr);
		free(tmpstr);
	}
	// TODO MANAGE UWSGI_PASTE
/*
	else if (wsgi_req->wsgi_paste_len > 0) {
		tmpstr = uwsgi_strncopy(wsgi_req->paste, wsgi_req->paste_len);
		callable = uwsgi_paste_loader((void *)tmpstr);
		free(tmpstr);
	}
*/

	return callable;
}


/* trying to emulate Graham's mod_wsgi, this will allows easy and fast migrations */
PyObject *uwsgi_file_loader(void *arg1) {

	char *filename = (char *) arg1;
	PyObject *wsgi_file_module, *wsgi_file_dict;
	PyObject *wsgi_file_callable;

	char *callable = up.callable;
	if (!callable) callable = "application";

	char *pythonized_filename = uwsgi_pythonize(filename);
	char *py_filename = uwsgi_concat2("uwsgi_file_", pythonized_filename);
	free(pythonized_filename);

	wsgi_file_module = uwsgi_pyimport_by_filename(py_filename, filename);
	if (!wsgi_file_module) {
		PyErr_Print();
		free(py_filename);
		return NULL;
	}

	wsgi_file_dict = PyModule_GetDict(wsgi_file_module);
	if (!wsgi_file_dict) {
		PyErr_Print();
		Py_DECREF(wsgi_file_module);
		free(py_filename);
		return NULL;
	}

	wsgi_file_callable = PyDict_GetItemString(wsgi_file_dict, callable);
	if (!wsgi_file_callable) {
		PyErr_Print();
		Py_DECREF(wsgi_file_dict);
		Py_DECREF(wsgi_file_module);
                free(py_filename);
		uwsgi_log( "unable to find \"application\" callable in file %s\n", filename);
		return NULL;
	}

	if (!PyFunction_Check(wsgi_file_callable) && !PyCallable_Check(wsgi_file_callable)) {
		uwsgi_log( "\"application\" must be a callable object in file %s\n", filename);
		Py_DECREF(wsgi_file_callable);
		Py_DECREF(wsgi_file_dict);
		Py_DECREF(wsgi_file_module);
                free(py_filename);
		return NULL;
	}

        free(py_filename);

	return wsgi_file_callable;

}

PyObject *uwsgi_pecan_loader(void *arg1) {

	char *pecan = (char *) arg1;
	PyObject *pecan_module, *pecan_dict, *pecan_deploy;
	PyObject *pecan_arg, *pecan_app;

	uwsgi_log( "Loading pecan environment: %s\n", pecan);

	pecan_module = PyImport_ImportModule("pecan.deploy");
	if (!pecan_module) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	pecan_dict = PyModule_GetDict(pecan_module);
	if (!pecan_dict) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	pecan_deploy = PyDict_GetItemString(pecan_dict, "deploy");
	if (!pecan_deploy) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	pecan_arg = PyTuple_New(1);
	if (!pecan_arg) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	if (PyTuple_SetItem(pecan_arg, 0, UWSGI_PYFROMSTRING(pecan))) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	pecan_app = PyObject_CallObject(pecan_deploy, pecan_arg);
	if (!pecan_app) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	return pecan_app;
}

PyObject *uwsgi_paste_loader(void *arg1) {

	char *paste = (char *) arg1;
	PyObject *paste_module, *paste_dict, *paste_loadapp;
	PyObject *paste_arg, *paste_app;

	uwsgi_log( "Loading paste environment: %s\n", paste);

	if (up.paste_logger) {
		PyObject *paste_logger_dict = get_uwsgi_pydict("logging.config");
		if (paste_logger_dict) {
			PyObject *paste_logger_fileConfig = PyDict_GetItemString(paste_logger_dict, "fileConfig");
			if (paste_logger_fileConfig) {
				PyObject *paste_logger_arg = PyTuple_New(1);
				if (!paste_logger_arg) {
					PyErr_Print();
					exit(UWSGI_FAILED_APP_CODE);
				}
				PyTuple_SetItem(paste_logger_arg, 0, UWSGI_PYFROMSTRING(paste+7));
				if (python_call(paste_logger_fileConfig, paste_logger_arg, 0, NULL)) {
					PyErr_Print();
				}
			}
		}
	}

	paste_module = PyImport_ImportModule("paste.deploy");
	if (!paste_module) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	paste_dict = PyModule_GetDict(paste_module);
	if (!paste_dict) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	paste_loadapp = PyDict_GetItemString(paste_dict, "loadapp");
	if (!paste_loadapp) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	paste_arg = PyTuple_New(1);
	if (!paste_arg) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	if (PyTuple_SetItem(paste_arg, 0, UWSGI_PYFROMSTRING(paste))) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	paste_app = PyObject_CallObject(paste_loadapp, paste_arg);
	if (!paste_app) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}

	return paste_app;
}

PyObject *uwsgi_eval_loader(void *arg1) {

	char *code = (char *) arg1;

	PyObject *wsgi_eval_module, *wsgi_eval_callable = NULL;

	PyObject *wsgi_compiled_node;

	wsgi_compiled_node = Py_CompileString(code, "uwsgi_eval_config", Py_file_input);
	if (!wsgi_compiled_node) {
		PyErr_Print();
		uwsgi_log( "failed to compile eval code\n");
		exit(UWSGI_FAILED_APP_CODE);
	}

	wsgi_eval_module = PyImport_ExecCodeModule("uwsgi_eval_config", wsgi_compiled_node);
	if (!wsgi_eval_module) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}


	Py_DECREF(wsgi_compiled_node);

	up.loader_dict = PyModule_GetDict(wsgi_eval_module);
	if (!up.loader_dict) {
		PyErr_Print();
		exit(UWSGI_FAILED_APP_CODE);
	}


	if (up.callable) {
		wsgi_eval_callable = PyDict_GetItemString(up.loader_dict, up.callable);
	}
	else {

		wsgi_eval_callable = PyDict_GetItemString(up.loader_dict, "application");
	}

	if (wsgi_eval_callable) {
		if (!PyFunction_Check(wsgi_eval_callable) && !PyCallable_Check(wsgi_eval_callable)) {
			uwsgi_log( "you must define a callable object in your code\n");
			exit(UWSGI_FAILED_APP_CODE);
		}
	}

	return wsgi_eval_callable;

}

PyObject *uwsgi_callable_loader(void *arg1) {
	return (PyObject *) arg1;
}

PyObject *uwsgi_string_callable_loader(void *arg1) {
	char *callable = (char *) arg1;

	return PyDict_GetItem(up.loader_dict, UWSGI_PYFROMSTRING(callable));
}
