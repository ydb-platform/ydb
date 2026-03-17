#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

/*

	This is the official python plugin, the one with the modifier1 mapped to 0

	Since uWSGI 1.0 it is heavily based on Graham Dumpleton mod_wsgi (for Apache)

*/

extern struct uwsgi_server uwsgi;
struct uwsgi_python up;

#include <glob.h>

extern PyTypeObject uwsgi_InputType;

void uwsgi_opt_pythonpath(char *opt, char *value, void *foobar) {

	int i;
	glob_t g;

	if (glob(value, GLOB_MARK, NULL, &g)) {
		uwsgi_string_new_list(&up.python_path, value);
	}
	else {
		for (i = 0; i < (int) g.gl_pathc; i++) {
	        	uwsgi_string_new_list(&up.python_path, g.gl_pathv[i]);
	        }
	}
}

void uwsgi_opt_pyshell(char *opt, char *value, void *foobar) {

	uwsgi.honour_stdin = 1;
	if (value) {
		up.pyshell = value;
	}
	else {
		up.pyshell = "";
	}

	if (!strcmp("pyshell-oneshot", opt)) {
		up.pyshell_oneshot = 1;
	}
}

void uwsgi_opt_pyrun(char *opt, char *value, void *foobar) {
	uwsgi.honour_stdin = 1;
	uwsgi.command_mode = 1;
	up.pyrun = value;
}

void uwsgi_opt_pyver(char *opt, char *foo, void *bar) {
	const char *version = Py_GetVersion();
	const char *space = strchr(version, ' ');
	if (space) {
        	fprintf(stdout, "%.*s\n", (int) (space-version), version);
	}
	else {
        	fprintf(stdout, "%s\n", version);
	}
	exit(0);
}


void uwsgi_opt_ini_paste(char *opt, char *value, void *foobar) {

	uwsgi_opt_load_ini(opt, value, NULL);

	if (value[0] != '/') {
		up.paste = uwsgi_concat4("config:", uwsgi.cwd, "/", value);
	}
	else {
		up.paste = uwsgi_concat2("config:", value);
        }

	if (!strcmp("ini-paste-logged", opt)) {
		up.paste_logger = 1;
	}
	
}

struct uwsgi_option uwsgi_python_options[] = {
	{"wsgi-file", required_argument, 0, "load .wsgi file", uwsgi_opt_set_str, &up.file_config, 0},
	{"file", required_argument, 0, "load .wsgi file", uwsgi_opt_set_str, &up.file_config, 0},
	{"eval", required_argument, 0, "eval python code", uwsgi_opt_set_str, &up.eval, 0},
	{"module", required_argument,'w', "load a WSGI module", uwsgi_opt_set_str, &up.wsgi_config, 0},
	{"wsgi", required_argument, 'w', "load a WSGI module", uwsgi_opt_set_str, &up.wsgi_config, 0},
	{"callable", required_argument, 0, "set default WSGI callable name", uwsgi_opt_set_str, &up.callable, 0},
	{"test", required_argument, 'J', "test a module import", uwsgi_opt_set_str, &up.test_module, 0},
	{"home", required_argument, 'H', "set PYTHONHOME/virtualenv", uwsgi_opt_set_str, &up.home, 0},
	{"virtualenv", required_argument, 'H', "set PYTHONHOME/virtualenv", uwsgi_opt_set_str, &up.home, 0},
	{"venv", required_argument, 'H', "set PYTHONHOME/virtualenv", uwsgi_opt_set_str, &up.home, 0},
	{"pyhome", required_argument, 'H', "set PYTHONHOME/virtualenv", uwsgi_opt_set_str, &up.home, 0},
	{"py-programname", required_argument, 0, "set python program name", uwsgi_opt_set_str, &up.programname, 0},
	{"py-program-name", required_argument, 0, "set python program name", uwsgi_opt_set_str, &up.programname, 0},

	{"pythonpath", required_argument, 0, "add directory (or glob) to pythonpath", uwsgi_opt_pythonpath, NULL,  0},
	{"python-path", required_argument, 0, "add directory (or glob) to pythonpath", uwsgi_opt_pythonpath, NULL, 0},
	{"pp", required_argument, 0, "add directory (or glob) to pythonpath", uwsgi_opt_pythonpath, NULL, 0},

	{"pymodule-alias", required_argument, 0, "add a python alias module", uwsgi_opt_add_string_list, &up.pymodule_alias, 0},
	{"post-pymodule-alias", required_argument, 0, "add a python module alias after uwsgi module initialization", uwsgi_opt_add_string_list, &up.post_pymodule_alias, 0},

	{"import", required_argument, 0, "import a python module", uwsgi_opt_add_string_list, &up.import_list, 0},
	{"pyimport", required_argument, 0, "import a python module", uwsgi_opt_add_string_list, &up.import_list, 0},
	{"py-import", required_argument, 0, "import a python module", uwsgi_opt_add_string_list, &up.import_list, 0},
	{"python-import", required_argument, 0, "import a python module", uwsgi_opt_add_string_list, &up.import_list, 0},

	{"shared-import", required_argument, 0, "import a python module in all of the processes", uwsgi_opt_add_string_list, &up.shared_import_list, 0},
	{"shared-pyimport", required_argument, 0, "import a python module in all of the processes", uwsgi_opt_add_string_list, &up.shared_import_list, 0},
	{"shared-py-import", required_argument, 0, "import a python module in all of the processes", uwsgi_opt_add_string_list, &up.shared_import_list, 0},
	{"shared-python-import", required_argument, 0, "import a python module in all of the processes", uwsgi_opt_add_string_list, &up.shared_import_list, 0},

	{"spooler-import", required_argument, 0, "import a python module in the spooler", uwsgi_opt_add_string_list, &up.spooler_import_list},
	{"spooler-pyimport", required_argument, 0, "import a python module in the spooler", uwsgi_opt_add_string_list, &up.spooler_import_list},
	{"spooler-py-import", required_argument, 0, "import a python module in the spooler", uwsgi_opt_add_string_list, &up.spooler_import_list},
	{"spooler-python-import", required_argument, 0, "import a python module in the spooler", uwsgi_opt_add_string_list, &up.spooler_import_list},

	{"pyargv", required_argument, 0, "manually set sys.argv", uwsgi_opt_set_str, &up.argv, 0},
	{"optimize", required_argument, 'O', "set python optimization level", uwsgi_opt_set_int, &up.optimize, 0},


	{"pecan", required_argument, 0, "load a pecan config file", uwsgi_opt_set_str, &up.pecan, 0},

	{"paste", required_argument, 0, "load a paste.deploy config file", uwsgi_opt_set_str, &up.paste, 0},
	{"paste-logger", no_argument, 0, "enable paste fileConfig logger", uwsgi_opt_true, &up.paste_logger, 0},


	{"web3", required_argument, 0, "load a web3 app", uwsgi_opt_set_str, &up.web3, 0},
	{"pump", required_argument, 0, "load a pump app", uwsgi_opt_set_str, &up.pump, 0},
	{"wsgi-lite", required_argument, 0, "load a wsgi-lite app", uwsgi_opt_set_str, &up.wsgi_lite, 0},
	{"ini-paste", required_argument, 0, "load a paste.deploy config file containing uwsgi section", uwsgi_opt_ini_paste, NULL, UWSGI_OPT_IMMEDIATE},
	{"ini-paste-logged", required_argument, 0, "load a paste.deploy config file containing uwsgi section (load loggers too)", uwsgi_opt_ini_paste, NULL, UWSGI_OPT_IMMEDIATE},
	{"reload-os-env", no_argument, 0, "force reload of os.environ at each request", uwsgi_opt_true, &up.reload_os_env, 0},
#ifndef __CYGWIN__
	{"no-site", no_argument, 0, "do not import site module", uwsgi_opt_true, &Py_NoSiteFlag, 0},
#endif
	{"pyshell", optional_argument, 0, "run an interactive python shell in the uWSGI environment", uwsgi_opt_pyshell, NULL, 0},
	{"pyshell-oneshot", optional_argument, 0, "run an interactive python shell in the uWSGI environment (one-shot variant)", uwsgi_opt_pyshell, NULL, 0},

	{"python", required_argument, 0, "run a python script in the uWSGI environment", uwsgi_opt_pyrun, NULL, 0},
	{"py", required_argument, 0, "run a python script in the uWSGI environment", uwsgi_opt_pyrun, NULL, 0},
	{"pyrun", required_argument, 0, "run a python script in the uWSGI environment", uwsgi_opt_pyrun, NULL, 0},

	{"py-tracebacker", required_argument, 0, "enable the uWSGI python tracebacker", uwsgi_opt_set_str, &up.tracebacker, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},

	{"py-auto-reload", required_argument, 0, "monitor python modules mtime to trigger reload (use only in development)", uwsgi_opt_set_int, &up.auto_reload, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},
	{"py-autoreload", required_argument, 0, "monitor python modules mtime to trigger reload (use only in development)", uwsgi_opt_set_int, &up.auto_reload, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},
	{"python-auto-reload", required_argument, 0, "monitor python modules mtime to trigger reload (use only in development)", uwsgi_opt_set_int, &up.auto_reload, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},
	{"python-autoreload", required_argument, 0, "monitor python modules mtime to trigger reload (use only in development)", uwsgi_opt_set_int, &up.auto_reload, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},
	{"py-auto-reload-ignore", required_argument, 0, "ignore the specified module during auto-reload scan (can be specified multiple times)", uwsgi_opt_add_string_list, &up.auto_reload_ignore, UWSGI_OPT_THREADS|UWSGI_OPT_MASTER},

	{"wsgi-env-behaviour", required_argument, 0, "set the strategy for allocating/deallocating the WSGI env", uwsgi_opt_set_str, &up.wsgi_env_behaviour, 0},
	{"wsgi-env-behavior", required_argument, 0, "set the strategy for allocating/deallocating the WSGI env", uwsgi_opt_set_str, &up.wsgi_env_behaviour, 0},
	{"start_response-nodelay", no_argument, 0, "send WSGI http headers as soon as possible (PEP violation)", uwsgi_opt_true, &up.start_response_nodelay, 0},

	{"wsgi-strict", no_argument, 0, "try to be fully PEP compliant disabling optimizations", uwsgi_opt_true, &up.wsgi_strict, 0},
	{"wsgi-accept-buffer", no_argument, 0, "accept CPython buffer-compliant objects as WSGI response in addition to string/bytes", uwsgi_opt_true, &up.wsgi_accept_buffer, 0},
	{"wsgi-accept-buffers", no_argument, 0, "accept CPython buffer-compliant objects as WSGI response in addition to string/bytes", uwsgi_opt_true, &up.wsgi_accept_buffer, 0},

	{"wsgi-disable-file-wrapper", no_argument, 0, "disable wsgi.file_wrapper feature", uwsgi_opt_true, &up.wsgi_disable_file_wrapper, 0},

	{"python-version", no_argument, 0, "report python version", uwsgi_opt_pyver, NULL, UWSGI_OPT_IMMEDIATE},

	{"python-raw", required_argument, 0, "load a python file for managing raw requests", uwsgi_opt_set_str, &up.raw, 0},

#if defined(PYTHREE) || defined(Py_TPFLAGS_HAVE_NEWBUFFER)
	{"py-sharedarea", required_argument, 0, "create a sharedarea from a python bytearray object of the specified size", uwsgi_opt_add_string_list, &up.sharedarea, 0},
#endif

	{"py-call-osafterfork", no_argument, 0, "enable child processes running cpython to trap OS signals", uwsgi_opt_true, &up.call_osafterfork, 0},
	{"py-call-uwsgi-fork-hooks", no_argument, 0, "call pre and post hooks when uswgi forks to update the internal interpreter state of CPython", uwsgi_opt_true, &up.call_uwsgi_fork_hooks, 0},

	{"python-worker-override", required_argument, 0, "override worker with the specified python script", uwsgi_opt_set_str, &up.worker_override, 0},

	{"py-executable", required_argument, 0, "override sys.executable value", uwsgi_opt_set_str, &up.executable, 0},
	{"py-sys-executable", required_argument, 0, "override sys.executable value", uwsgi_opt_set_str, &up.executable, 0},

	{0, 0, 0, 0, 0, 0, 0},
};

/* this routine will be called after each fork to reinitialize the various locks */
void uwsgi_python_pthread_prepare(void) {
	pthread_mutex_lock(&up.lock_pyloaders);
}

void uwsgi_python_pthread_parent(void) {
	pthread_mutex_unlock(&up.lock_pyloaders);
}

void uwsgi_python_pthread_child(void) {
	pthread_mutex_init(&up.lock_pyloaders, NULL);
}

PyMethodDef uwsgi_spit_method[] = { {"uwsgi_spit", py_uwsgi_spit, METH_VARARGS, ""} };
PyMethodDef uwsgi_write_method[] = { {"uwsgi_write", py_uwsgi_write, METH_VARARGS, ""} };

PyDoc_STRVAR(uwsgi_py_doc, "uWSGI api module.");

#ifdef PYTHREE
static PyModuleDef uwsgi_module3 = {
	PyModuleDef_HEAD_INIT,
	"uwsgi",
	uwsgi_py_doc,
	-1,
	NULL,
};
PyObject *init_uwsgi3(void) {
	return PyModule_Create(&uwsgi_module3);
}
#endif

int uwsgi_python_init() {

	char *pyversion = strchr(Py_GetVersion(), '\n');
	if (!pyversion) {
        	uwsgi_log_initial("Python version: %s\n", Py_GetVersion());
	}
	else {
        	uwsgi_log_initial("Python version: %.*s %s\n", pyversion-Py_GetVersion(), Py_GetVersion(), Py_GetCompiler()+1);
	}

	if (Py_IsInitialized()) {
		uwsgi_log("--- Python VM already initialized ---\n");
		PyGILState_Ensure();
		goto ready;
	}

	if (up.home != NULL) {
		if (!uwsgi_is_dir(up.home)) {
			uwsgi_log("!!! Python Home is not a directory: %s !!!\n", up.home);
		}
#ifdef PYTHREE
		// check for PEP 405 virtualenv (starting from python 3.3)
		char *pep405_env = uwsgi_concat2(up.home, "/pyvenv.cfg");
		if (uwsgi_file_exists(pep405_env)) {
			uwsgi_log("PEP 405 virtualenv detected: %s\n", up.home);
			free(pep405_env);
			goto pep405;
		}
		free(pep405_env);

		// build the PYTHONHOME wchar path
		wchar_t *wpyhome;
		size_t len = strlen(up.home) + 1; 
		wpyhome = uwsgi_calloc(sizeof(wchar_t) * len );
		if (!wpyhome) {
			uwsgi_error("malloc()");
			exit(1);
		}
		mbstowcs(wpyhome, up.home, len);
		Py_SetPythonHome(wpyhome);
		// do not free this memory !!!
		//free(wpyhome);
pep405:
#else
		Py_SetPythonHome(up.home);
#endif
		uwsgi_log("Set PythonHome to %s\n", up.home);
	}

	char *program_name = up.programname;
	if (!program_name) {
		program_name = uwsgi.binary_path;
	}

#ifdef PYTHREE
	if (!up.programname) {
		if (up.home) {
			program_name = uwsgi_concat2(up.home, "/bin/python");
		}
	}

	wchar_t *pname = uwsgi_calloc(sizeof(wchar_t) * (strlen(program_name)+1));
	mbstowcs(pname, program_name, strlen(program_name)+1);
	Py_SetProgramName(pname);
#ifdef UWSGI_PY312
	PyImport_AppendInittab("uwsgi", init_uwsgi3);
#endif
#else
	Py_SetProgramName(program_name);
#endif


	Py_OptimizeFlag = up.optimize;

	Py_Initialize();

ready:

	if (!uwsgi.has_threads) {
		uwsgi_log_initial("*** Python threads support is disabled. You can enable it with --enable-threads ***\n");
	}

	up.wsgi_spitout = PyCFunction_New(uwsgi_spit_method, NULL);
	up.wsgi_writeout = PyCFunction_New(uwsgi_write_method, NULL);

	up.main_thread = PyThreadState_Get();

        // by default set a fake GIL (little impact on performance)
        up.gil_get = gil_fake_get;
        up.gil_release = gil_fake_release;

        up.swap_ts = simple_swap_ts;
        up.reset_ts = simple_reset_ts;
	
#if defined(PYTHREE) || defined(Py_TPFLAGS_HAVE_NEWBUFFER)
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, up.sharedarea) {
		uint64_t len = uwsgi_n64(usl->value);
		PyObject *obj = PyByteArray_FromStringAndSize(NULL, len);
        	char *storage = PyByteArray_AsString(obj);
		Py_INCREF(obj);
		struct uwsgi_sharedarea *sa = uwsgi_sharedarea_init_ptr(storage, len);
		sa->obj = obj;
	}
#endif

	uwsgi_log_initial("Python main interpreter initialized at %p\n", up.main_thread);

	return 1;

}

void uwsgi_python_reset_random_seed() {

	PyObject *random_module, *random_dict, *random_seed;

        // reinitialize the random seed (thanks Jonas Borgström)
        random_module = PyImport_ImportModule("random");
        if (random_module) {
                random_dict = PyModule_GetDict(random_module);
                if (random_dict) {
                        random_seed = PyDict_GetItemString(random_dict, "seed");
                        if (random_seed) {
                                PyObject *random_args = PyTuple_New(1);
                                // pass no args
                                PyTuple_SetItem(random_args, 0, Py_None);
                                PyObject_CallObject(random_seed, random_args);
                                if (PyErr_Occurred()) {
                                        PyErr_Print();
                                }
                        }
                }
        }
}


void uwsgi_python_atexit() {

	if (uwsgi.mywid == 0) goto realstuff;

	// if hijacked do not run atexit hooks
	if (uwsgi.workers[uwsgi.mywid].hijacked)
		return;

	// if busy do not run atexit hooks
	if (uwsgi_worker_is_busy(uwsgi.mywid))
		return;

	// managing atexit in async mode is a real pain...skip it for now
	if (uwsgi.async > 1)
		return;
realstuff:

	// this time we use this higher level function
	// as this code can be executed in a signal handler

#ifdef __GNU_kFreeBSD__
	return;
#endif

	if (!Py_IsInitialized()) {
		return;
	}

	// always call it
	PyGILState_Ensure();

	// no need to worry about freeing memory
	PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");
	if (uwsgi_dict) {
		PyObject *ae = PyDict_GetItemString(uwsgi_dict, "atexit");
		if (ae) {
			python_call(ae, PyTuple_New(0), 0, NULL);
		}
	}

	// this part is a 1:1 copy of mod_wsgi 3.x
        // it is required to fix some atexit bug with python 3
	// and to shutdown useless threads complaints
	PyObject *module = PyImport_ImportModule("atexit");
	Py_XDECREF(module);

	if (uwsgi.has_threads) {
		if (!PyImport_AddModule("dummy_threading"))
			PyErr_Clear();
	}

	// For the reasons explained in
	// https://github.com/unbit/uwsgi/issues/1384, tearing down
	// the interpreter can be very expensive.
	if (uwsgi.skip_atexit_teardown)
		return;

	Py_Finalize();
}

void uwsgi_python_post_fork() {

	// Need to acquire the gil when no master process is used as first worker
	// will not have been forked like others
	// Necessary if uwsgi fork hooks called to update interpreter state
	if (up.call_uwsgi_fork_hooks && !uwsgi.master_process && uwsgi.mywid == 1) {
		UWSGI_GET_GIL
	}

	if (uwsgi.i_am_a_spooler) {
		UWSGI_GET_GIL
	}	

	// reset python signal flags so child processes can trap signals
	// Necessary if uwsgi fork hooks not called to update interpreter state
	if (!up.call_uwsgi_fork_hooks && up.call_osafterfork) {
#ifdef HAS_NOT_PYOS_FORK_STABLE_API
		PyOS_AfterFork();
#else
		PyOS_AfterFork_Child();
#endif
	}

	uwsgi_python_reset_random_seed();

	// call the post_fork_hook
	PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");
	if (uwsgi_dict) {
		PyObject *pfh = PyDict_GetItemString(uwsgi_dict, "post_fork_hook");
		if (pfh) {
			python_call(pfh, PyTuple_New(0), 0, NULL);
		}
	}
	PyErr_Clear();

	if (uwsgi.mywid > 0) {
		uwsgi_python_set_thread_name(0);
		if (up.auto_reload) {
			// spawn the reloader thread
			pthread_t par_tid;
			pthread_create(&par_tid, NULL, uwsgi_python_autoreloader_thread, NULL);
		}
		if (up.tracebacker) {
			// spawn the tracebacker thread
			pthread_t ptb_tid;
			pthread_create(&ptb_tid, NULL, uwsgi_python_tracebacker_thread, NULL);
		}
	}

UWSGI_RELEASE_GIL

}

PyObject *uwsgi_pyimport_by_filename(char *name, char *filename) {

	char *pycontent;
	PyObject *py_compiled_node, *py_file_module;
	int is_a_package = 0;
	struct stat pystat;
	char *real_filename = filename;


	if (!uwsgi_check_scheme(filename)) {

		FILE *pyfile = fopen(filename, "r");
		if (!pyfile) {
			uwsgi_log("failed to open python file %s\n", filename);
			return NULL;
		}

		if (fstat(fileno(pyfile), &pystat)) {
			fclose(pyfile);
			uwsgi_error("fstat()");
			return NULL;
		}

		if (S_ISDIR(pystat.st_mode)) {
			is_a_package = 1;
			fclose(pyfile);
			real_filename = uwsgi_concat2(filename, "/__init__.py");
			pyfile = fopen(real_filename, "r");
			if (!pyfile) {
				uwsgi_error_open(real_filename);
				free(real_filename);
				return NULL;
			}
		}

		fclose(pyfile);
		pycontent = uwsgi_simple_file_read(real_filename);

		if (!pycontent) {
			if (is_a_package) {
				free(real_filename);
			}
			uwsgi_log("no data read from file %s\n", real_filename);
			return NULL;
		}

	}
	else {
		size_t pycontent_size = 0;
		pycontent = uwsgi_open_and_read(filename, &pycontent_size, 1, NULL);

		if (!pycontent) {
			uwsgi_log("no data read from url %s\n", real_filename);
			return NULL;
		}
	}

	py_compiled_node = Py_CompileString(pycontent, real_filename, Py_file_input);
	if (!py_compiled_node) {
		PyErr_Print();
		uwsgi_log("failed to compile %s\n", real_filename);
		return NULL;
	}

	if (is_a_package) {
		py_file_module = PyImport_AddModule(name);
		if (py_file_module) {
			PyModule_AddObject(py_file_module, "__path__", Py_BuildValue("[O]", PyString_FromString(filename)));
		}
		free(real_filename);
	}

	py_file_module = PyImport_ExecCodeModule(name, py_compiled_node);
	if (!py_file_module) {
		PyErr_Print();
		return NULL;
	}

	Py_DECREF(py_compiled_node);

	return py_file_module;

}

void init_uwsgi_vars() {

	PyObject *pysys, *pysys_dict, *pypath;

	PyObject *modules = PyImport_GetModuleDict();
	PyObject *tmp_module;

	/* add cwd to pythonpath */
	pysys = PyImport_ImportModule("sys");
	if (!pysys) {
		PyErr_Print();
		exit(1);
	}
	pysys_dict = PyModule_GetDict(pysys);

#ifdef PYTHREE
	// In python3, stdout / stderr was changed to be buffered (a bug according
	// to many):
	// - https://bugs.python.org/issue13597
	// - https://bugs.python.org/issue13601
	// We'll fix this by manually restore the unbuffered behaviour.
	// In the case of a tty, this fix breaks readline support in interactive
	// debuggers so we'll only do this in the non-tty case.
	if (!Py_FdIsInteractive(stdin, NULL)) {
#ifdef HAS_NO_ERRORS_IN_PyFile_FromFd
		PyObject *new_stdprint = PyFile_FromFd(2, NULL, "w", _IOLBF, NULL, NULL, 0);
#else
		PyObject *new_stdprint = PyFile_FromFd(2, NULL, "w", _IOLBF, NULL, "backslashreplace", NULL, 0);
#endif
		PyDict_SetItemString(pysys_dict, "stdout", new_stdprint);
		PyDict_SetItemString(pysys_dict, "__stdout__", new_stdprint);
		PyDict_SetItemString(pysys_dict, "stderr", new_stdprint);
		PyDict_SetItemString(pysys_dict, "__stderr__", new_stdprint);
	}
#endif
	pypath = PyDict_GetItemString(pysys_dict, "path");
	if (!pypath) {
		PyErr_Print();
		exit(1);
	}

	if (PyList_Insert(pypath, 0, UWSGI_PYFROMSTRING(".")) != 0) {
		PyErr_Print();
	}

	struct uwsgi_string_list *uppp = up.python_path;
	while(uppp) {
		if (PyList_Insert(pypath, 0, UWSGI_PYFROMSTRING(uppp->value)) != 0) {
			PyErr_Print();
		}
		else {
			uwsgi_log("added %s to pythonpath.\n", uppp->value);
		}

		uppp = uppp->next;
	}

	struct uwsgi_string_list *uppma = up.pymodule_alias;
	while(uppma) {
		// split key=value
		char *value = strchr(uppma->value, '=');
		if (!value) {
			uwsgi_log("invalid pymodule-alias syntax\n");
			goto next;
		}
		value[0] = 0;
		if (!strchr(value + 1, '/')) {
			// this is a standard pymodule
			tmp_module = PyImport_ImportModule(value + 1);
			if (!tmp_module) {
				PyErr_Print();
				exit(1);
			}

			PyDict_SetItemString(modules, uppma->value, tmp_module);
		}
		else {
			// this is a filepath that need to be mapped
			tmp_module = uwsgi_pyimport_by_filename(uppma->value, value + 1);
			if (!tmp_module) {
				PyErr_Print();
				exit(1);
			}
		}
		uwsgi_log("mapped virtual pymodule \"%s\" to real pymodule \"%s\"\n", uppma->value, value + 1);
		// reset original value
		value[0] = '=';

next:
		uppma = uppma->next;
	}

}



void init_uwsgi_embedded_module() {
	PyObject *new_uwsgi_module, *zero;
	int i;

	if (PyType_Ready(&uwsgi_InputType) < 0) {
		PyErr_Print();
		uwsgi_log("could not initialize the uwsgi python module\n");
		exit(1);
	}

	/* initialize for stats */
	up.workers_tuple = PyTuple_New(uwsgi.numproc);
	for (i = 0; i < uwsgi.numproc; i++) {
		zero = PyDict_New();
		Py_INCREF(zero);
		PyTuple_SetItem(up.workers_tuple, i, zero);
	}


#ifdef PYTHREE
#ifndef UWSGI_PY312
	PyImport_AppendInittab("uwsgi", init_uwsgi3);
#endif
	new_uwsgi_module = PyImport_AddModule("uwsgi");
#else
	new_uwsgi_module = Py_InitModule3("uwsgi", NULL, uwsgi_py_doc);
#endif
	if (new_uwsgi_module == NULL) {
		uwsgi_log("could not initialize the uwsgi python module\n");
		exit(1);
	}



	Py_INCREF((PyObject *) &uwsgi_InputType);

	up.embedded_dict = PyModule_GetDict(new_uwsgi_module);
	if (!up.embedded_dict) {
		uwsgi_log("could not get uwsgi module __dict__\n");
		exit(1);
	}

	// just for safety
	Py_INCREF(up.embedded_dict);


	if (PyDict_SetItemString(up.embedded_dict, "version", PyString_FromString(UWSGI_VERSION))) {
		PyErr_Print();
		exit(1);
	}

	PyObject *uwsgi_py_version_info = PyTuple_New(5);

	PyTuple_SetItem(uwsgi_py_version_info, 0, PyInt_FromLong(UWSGI_VERSION_BASE));
	PyTuple_SetItem(uwsgi_py_version_info, 1, PyInt_FromLong(UWSGI_VERSION_MAJOR));
	PyTuple_SetItem(uwsgi_py_version_info, 2, PyInt_FromLong(UWSGI_VERSION_MINOR));
	PyTuple_SetItem(uwsgi_py_version_info, 3, PyInt_FromLong(UWSGI_VERSION_REVISION));
	PyTuple_SetItem(uwsgi_py_version_info, 4, PyString_FromString(UWSGI_VERSION_CUSTOM));

	if (PyDict_SetItemString(up.embedded_dict, "version_info", uwsgi_py_version_info)) {
		PyErr_Print();
		exit(1);
	}



	if (PyDict_SetItemString(up.embedded_dict, "hostname", PyString_FromStringAndSize(uwsgi.hostname, uwsgi.hostname_len))) {
		PyErr_Print();
		exit(1);
	}

	if (uwsgi.mode) {
		if (PyDict_SetItemString(up.embedded_dict, "mode", PyString_FromString(uwsgi.mode))) {
			PyErr_Print();
			exit(1);
		}
	}

	if (uwsgi.pidfile) {
		if (PyDict_SetItemString(up.embedded_dict, "pidfile", PyString_FromString(uwsgi.pidfile))) {
			PyErr_Print();
			exit(1);
		}
	}

	if (uwsgi.spoolers) {
		int sc = 0;
		struct uwsgi_spooler *uspool = uwsgi.spoolers;
		while(uspool) { sc++; uspool = uspool->next;}

		PyObject *py_spooler_tuple = PyTuple_New(sc);

		uspool = uwsgi.spoolers;
		sc = 0;

		while(uspool) {
			PyTuple_SetItem(py_spooler_tuple, sc, PyString_FromString(uspool->dir));
			sc++;
			uspool = uspool->next;
		}

		if (PyDict_SetItemString(up.embedded_dict, "spoolers", py_spooler_tuple)) {
                	PyErr_Print();
                	exit(1);
        	}
	}

	if (PyDict_SetItemString(up.embedded_dict, "SPOOL_RETRY", PyInt_FromLong(-1))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "SPOOL_OK", PyInt_FromLong(-2))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "SPOOL_IGNORE", PyInt_FromLong(0))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "numproc", PyInt_FromLong(uwsgi.numproc))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "has_threads", PyInt_FromLong(uwsgi.has_threads))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "cores", PyInt_FromLong(uwsgi.cores))) {
		PyErr_Print();
		exit(1);
	}

	if (uwsgi.loop) {
		if (PyDict_SetItemString(up.embedded_dict, "loop", PyString_FromString(uwsgi.loop))) {
			PyErr_Print();
			exit(1);
		}
	}
	else {
		PyDict_SetItemString(up.embedded_dict, "loop", Py_None);
	}

	PyObject *py_opt_dict = PyDict_New();
	for (i = 0; i < uwsgi.exported_opts_cnt; i++) {
		if (PyDict_Contains(py_opt_dict, PyString_FromString(uwsgi.exported_opts[i]->key))) {
			PyObject *py_opt_item = PyDict_GetItemString(py_opt_dict, uwsgi.exported_opts[i]->key);
			if (PyList_Check(py_opt_item)) {
				if (uwsgi.exported_opts[i]->value == NULL) {
					PyList_Append(py_opt_item, Py_True);
				}
				else {
					PyList_Append(py_opt_item, PyString_FromString(uwsgi.exported_opts[i]->value));
				}
			}
			else {
				PyObject *py_opt_list = PyList_New(0);
				PyList_Append(py_opt_list, py_opt_item);
				if (uwsgi.exported_opts[i]->value == NULL) {
					PyList_Append(py_opt_list, Py_True);
				}
				else {
					PyList_Append(py_opt_list, PyString_FromString(uwsgi.exported_opts[i]->value));
				}

				PyDict_SetItemString(py_opt_dict, uwsgi.exported_opts[i]->key, py_opt_list);
			}
		}
		else {
			if (uwsgi.exported_opts[i]->value == NULL) {
				PyDict_SetItemString(py_opt_dict, uwsgi.exported_opts[i]->key, Py_True);
			}
			else {
				PyDict_SetItemString(py_opt_dict, uwsgi.exported_opts[i]->key, PyString_FromString(uwsgi.exported_opts[i]->value));
			}
		}
	}

	if (PyDict_SetItemString(up.embedded_dict, "opt", py_opt_dict)) {
		PyErr_Print();
		exit(1);
	}

	PyObject *py_sockets_list = PyList_New(0);
	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	while(uwsgi_sock) {
		if (uwsgi_sock->bound) {
			PyList_Append(py_sockets_list, PyInt_FromLong(uwsgi_sock->fd));
		}
		uwsgi_sock = uwsgi_sock->next;
	}
	if (PyDict_SetItemString(up.embedded_dict, "sockets", py_sockets_list)) {
                PyErr_Print();
                exit(1);
        }

	PyObject *py_magic_table = PyDict_New();
	uint8_t mtk;
	for (i = 0; i <= 0xff; i++) {
		// a bit of magic :P
		mtk = i;
                if (uwsgi.magic_table[i]) {
			if (uwsgi.magic_table[i][0] != 0) {
				PyDict_SetItem(py_magic_table, PyString_FromStringAndSize((char *) &mtk, 1), PyString_FromString(uwsgi.magic_table[i]));
			}
		}
        }

	if (PyDict_SetItemString(up.embedded_dict, "magic_table", py_magic_table)) {
		PyErr_Print();
		exit(1);
	}

#ifdef UNBIT
	if (PyDict_SetItemString(up.embedded_dict, "unbit", Py_True)) {
#else
	if (PyDict_SetItemString(up.embedded_dict, "unbit", Py_None)) {
#endif
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "buffer_size", PyInt_FromLong(uwsgi.buffer_size))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "started_on", PyInt_FromLong(uwsgi.start_tv.tv_sec))) {
		PyErr_Print();
		exit(1);
	}

	if (PyDict_SetItemString(up.embedded_dict, "start_response", up.wsgi_spitout)) {
		PyErr_Print();
		exit(1);
	}


	if (PyDict_SetItemString(up.embedded_dict, "applications", Py_None)) {
		PyErr_Print();
		exit(1);
	}

	if (uwsgi.is_a_reload) {
		if (PyDict_SetItemString(up.embedded_dict, "is_a_reload", Py_True)) {
			PyErr_Print();
			exit(1);
		}
	}
	else {
		if (PyDict_SetItemString(up.embedded_dict, "is_a_reload", Py_False)) {
			PyErr_Print();
			exit(1);
		}
	}

	init_uwsgi_module_advanced(new_uwsgi_module);

	if (uwsgi.spoolers) {
		init_uwsgi_module_spooler(new_uwsgi_module);
	}

	if (uwsgi.sharedareas) {
		init_uwsgi_module_sharedarea(new_uwsgi_module);
	}

	init_uwsgi_module_cache(new_uwsgi_module);

	if (uwsgi.queue_size > 0) {
		init_uwsgi_module_queue(new_uwsgi_module);
	}

	if (uwsgi.snmp) {
		init_uwsgi_module_snmp(new_uwsgi_module);
	}

	if (up.extension) {
		up.extension();
	}
}



int uwsgi_python_magic(char *mountpoint, char *lazy) {

	char *qc = strchr(lazy, ':');
	if (qc) {
		qc[0] = 0;
		up.callable = qc + 1;
	}

	if (!strcmp(lazy + strlen(lazy) - 3, ".py")) {
		up.file_config = lazy;
		return 1;
	}
	else if (!strcmp(lazy + strlen(lazy) - 5, ".wsgi")) {
		up.file_config = lazy;
		return 1;
	}
	else if (qc && strchr(lazy, '.')) {
		up.wsgi_config = lazy;
		return 1;
	}

	// reset lazy
	if (qc) {
		qc[0] = ':';
	}
	return 0;

}

int uwsgi_python_mount_app(char *mountpoint, char *app) {

	int id;

	if (strchr(app, ':') || uwsgi_endswith(app, ".py") || uwsgi_endswith(app, ".wsgi")) {
		uwsgi.wsgi_req->appid = mountpoint;
		uwsgi.wsgi_req->appid_len = strlen(mountpoint);
		// lazy ?
        	if (uwsgi.mywid > 0) UWSGI_GET_GIL
		if (uwsgi.single_interpreter) {
			id = init_uwsgi_app(LOADER_MOUNT, app, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
		}
		else {
			id = init_uwsgi_app(LOADER_MOUNT, app, uwsgi.wsgi_req, NULL, PYTHON_APP_TYPE_WSGI);
		}
		// lazy ?
        	if (uwsgi.mywid > 0) UWSGI_RELEASE_GIL
		return id;
	}
	return -1;

}

char *uwsgi_pythonize(char *orig) {

	char *name;
	size_t i, len, offset = 0;

	if (!strncmp(orig, "sym://", 6)) {
		offset = 6;
	}
	else if (!strncmp(orig, "http://", 7)) {
		offset = 7;
	}
	else if (!strncmp(orig, "data://", 7)) {
		offset = 7;
	}

	name = uwsgi_concat2(orig+offset, "");
	len = strlen(name);
	for(i=0;i<len;i++) {
		if (name[i] == '.') {
			name[i] = '_';
		}
		else if (name[i] == '/') {
			name[i] = '_';
		}
	}


	if ((name[len-3] == '.' || name[len-3] == '_') && name[len-2] == 'p' && name[len-1] == 'y') {
		name[len-3] = 0;
	}

	return name;

}

void uwsgi_python_spooler_init(void) {

	struct uwsgi_string_list *upli = up.spooler_import_list;

	UWSGI_GET_GIL

        while(upli) {
                if (strchr(upli->value, '/') || uwsgi_endswith(upli->value, ".py")) {
                        uwsgi_pyimport_by_filename(uwsgi_pythonize(upli->value), upli->value);
                }
                else {
                        if (PyImport_ImportModule(upli->value) == NULL) {
                                PyErr_Print();
                        }
                }
                upli = upli->next;
        }

	UWSGI_RELEASE_GIL
	

}

// this is the default (fake) allocator for WSGI's env
// the dictionary is created on app loading (one for each async core/thread) and reused (clearing it after each request, constantly)
//
// from a python-programmer point of view it is a hack/cheat but it does not violate the WSGI standard
// and it is a bit faster than the "holy" allocator
void *uwsgi_python_create_env_cheat(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {
        wsgi_req->async_args = wi->args[wsgi_req->async_id];
	Py_INCREF((PyObject *)wi->environ[wsgi_req->async_id]);
	return wi->environ[wsgi_req->async_id];
}

void uwsgi_python_destroy_env_cheat(struct wsgi_request *wsgi_req) {
	PyDict_Clear((PyObject *)wsgi_req->async_environ);
}

// this is the "holy" allocator for WSGI's env
// Armin Ronacher told me this is what most of python programmers expect
// I cannot speak for that as i am a perl guy, and i expect only black-magic things :P
//
// this should be the default one, but changing default behaviours (even if they are wrong)
// always make my customers going berserk...
//
// it is only slightly (better: irrelevant) slower, so no fear in enabling it...


void *uwsgi_python_create_env_holy(struct wsgi_request *wsgi_req, struct uwsgi_app *wi) {
	wsgi_req->async_args = PyTuple_New(2);
	// set start_response()
	Py_INCREF(Py_None);
	Py_INCREF(up.wsgi_spitout);
	PyTuple_SetItem((PyObject *)wsgi_req->async_args, 0, Py_None);
	PyTuple_SetItem((PyObject *)wsgi_req->async_args, 1, up.wsgi_spitout);
	PyObject *env = PyDict_New();
	return env;
}

void uwsgi_python_destroy_env_holy(struct wsgi_request *wsgi_req) {
	if (uwsgi.threads < 2) {
		// in non-multithread modes, we set uwsgi.env, so let's remove it now
		// to equalise the refcount of the environ
		PyDict_DelItemString(up.embedded_dict, "env");
	}
	// avoid to decref environ if it is mapped to the python callable
	if (PyTuple_GetItem(wsgi_req->async_args, 0) != wsgi_req->async_environ) {
		Py_DECREF((PyObject *)wsgi_req->async_environ);
        }
	Py_DECREF((PyObject *) wsgi_req->async_args);
}


// this hook will be executed by master (or worker1 when master is not requested, so COW is in place)
void uwsgi_python_preinit_apps() {

	// GIL was released in previous initialization steps but init_pyargv expects
	// the GIL to be acquired
	// Necessary if uwsgi fork hooks called to update interpreter state
	if (up.call_uwsgi_fork_hooks) {
		UWSGI_GET_GIL
	}

	init_pyargv();

        init_uwsgi_embedded_module();

#ifdef __linux__
	uwsgi_init_symbol_import();
#endif

        if (up.test_module != NULL) {
                if (PyImport_ImportModule(up.test_module)) {
                        exit(0);
                }
                exit(1);
        }

	if (up.wsgi_env_behaviour) {
		if (!strcmp(up.wsgi_env_behaviour, "holy")) {
			up.wsgi_env_create = uwsgi_python_create_env_holy;
			up.wsgi_env_destroy = uwsgi_python_destroy_env_holy;
		}
		else if (!strcmp(up.wsgi_env_behaviour, "cheat")) {
			up.wsgi_env_create = uwsgi_python_create_env_cheat;
			up.wsgi_env_destroy = uwsgi_python_destroy_env_cheat;
		}
		else {
			uwsgi_log("invalid wsgi-env-behaviour value: %s\n", up.wsgi_env_behaviour);
			exit(1);
		}
	}
	else {
		up.wsgi_env_create = uwsgi_python_create_env_cheat;
		up.wsgi_env_destroy = uwsgi_python_destroy_env_cheat;
	}

        init_uwsgi_vars();

	// load shared imports
	struct uwsgi_string_list *upli = up.shared_import_list;
	while(upli) {
		if (strchr(upli->value, '/') || uwsgi_endswith(upli->value, ".py")) {
			uwsgi_pyimport_by_filename(uwsgi_pythonize(upli->value), upli->value);
		}
		else {
			if (PyImport_ImportModule(upli->value) == NULL) {
				PyErr_Print();
			}
		}
		upli = upli->next;
	}

	// Release the GIL before moving on forward with initialization
	// Necessary if uwsgi fork hooks called to update interpreter state
	if (up.call_uwsgi_fork_hooks) {
		UWSGI_RELEASE_GIL
	}

}

void uwsgi_python_init_apps() {

	// lazy ?
	// Also necessary if uwsgi fork hooks called to update interpreter state
	if (uwsgi.mywid > 0 || up.call_uwsgi_fork_hooks) {
		UWSGI_GET_GIL;
	}

	// prepare for stack suspend/resume
	if (uwsgi.async > 1) {
#ifdef UWSGI_PY314
		up.current_py_recursion_remaining = uwsgi_malloc(sizeof(int)*uwsgi.async);
#elif defined UWSGI_PY312
		up.current_c_recursion_remaining = uwsgi_malloc(sizeof(int)*uwsgi.async);
		up.current_py_recursion_remaining = uwsgi_malloc(sizeof(int)*uwsgi.async);
#elif defined UWSGI_PY311
		up.current_recursion_remaining = uwsgi_malloc(sizeof(int)*uwsgi.async);
#else
		up.current_recursion_depth = uwsgi_malloc(sizeof(int)*uwsgi.async);
#endif
		up.current_frame = uwsgi_malloc(sizeof(up.current_frame[0])*uwsgi.async);
	}

        // setup app loaders
        up.loaders[LOADER_DYN] = uwsgi_dyn_loader;
        up.loaders[LOADER_UWSGI] = uwsgi_uwsgi_loader;
        up.loaders[LOADER_FILE] = uwsgi_file_loader;
        up.loaders[LOADER_PECAN] = uwsgi_pecan_loader;
        up.loaders[LOADER_PASTE] = uwsgi_paste_loader;
        up.loaders[LOADER_EVAL] = uwsgi_eval_loader;
        up.loaders[LOADER_MOUNT] = uwsgi_mount_loader;
        up.loaders[LOADER_CALLABLE] = uwsgi_callable_loader;
        up.loaders[LOADER_STRING_CALLABLE] = uwsgi_string_callable_loader;

	struct uwsgi_string_list *upli = up.import_list;
	while(upli) {
		if (strchr(upli->value, '/') || uwsgi_endswith(upli->value, ".py")) {
			uwsgi_pyimport_by_filename(uwsgi_pythonize(upli->value), upli->value);
		}
		else {
			if (PyImport_ImportModule(upli->value) == NULL) {
				PyErr_Print();
			}
		}
		upli = upli->next;
	}

	struct uwsgi_string_list *uppa = up.post_pymodule_alias;
	PyObject *modules = PyImport_GetModuleDict();
	PyObject *tmp_module;
	while(uppa) {
                // split key=value
                char *value = strchr(uppa->value, '=');
                if (!value) {
                        uwsgi_log("invalid pymodule-alias syntax\n");
			goto next;
                }
                value[0] = 0;
                if (!strchr(value + 1, '/')) {
                        // this is a standard pymodule
                        tmp_module = PyImport_ImportModule(value + 1);
                        if (!tmp_module) {
                                PyErr_Print();
                                exit(1);
                        }

                        PyDict_SetItemString(modules, uppa->value, tmp_module);
                }
                else {
                        // this is a filepath that need to be mapped
                        tmp_module = uwsgi_pyimport_by_filename(uppa->value, value + 1);
                        if (!tmp_module) {
                                PyErr_Print();
                                exit(1);
                        }
                }
                uwsgi_log("mapped virtual pymodule \"%s\" to real pymodule \"%s\"\n", uppa->value, value + 1);
                // reset original value
                value[0] = '=';

next:
		uppa = uppa->next;
        }

	if (up.raw) {
		up.raw_callable = uwsgi_file_loader(up.raw);
		if (up.raw_callable) {
			Py_INCREF(up.raw_callable);
		}
	}

	if (up.wsgi_config != NULL) {
		init_uwsgi_app(LOADER_UWSGI, up.wsgi_config, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
	}

	if (up.file_config != NULL) {
		init_uwsgi_app(LOADER_FILE, up.file_config, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
	}
	if (up.pecan != NULL) {
		init_uwsgi_app(LOADER_PECAN, up.pecan, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
	}
	if (up.paste != NULL) {
		init_uwsgi_app(LOADER_PASTE, up.paste, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
	}
	if (up.eval != NULL) {
		init_uwsgi_app(LOADER_EVAL, up.eval, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI);
	}
	if (up.web3 != NULL) {
		init_uwsgi_app(LOADER_UWSGI, up.web3, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WEB3);
	}
	if (up.pump != NULL) {
		init_uwsgi_app(LOADER_UWSGI, up.pump, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_PUMP);
	}
	if (up.wsgi_lite != NULL) {
		init_uwsgi_app(LOADER_UWSGI, up.wsgi_lite, uwsgi.wsgi_req, up.main_thread, PYTHON_APP_TYPE_WSGI_LITE);
	}

	if (uwsgi.profiler) {
		if (!strcmp(uwsgi.profiler, "pycall")) {
			PyEval_SetProfile(uwsgi_python_profiler_call, NULL);
		}
		else if (!strcmp(uwsgi.profiler, "pyline")) {
			PyEval_SetTrace(uwsgi_python_tracer, NULL);
		}
	}

	PyObject *uwsgi_dict = get_uwsgi_pydict("uwsgi");
        if (uwsgi_dict) {
                up.after_req_hook = PyDict_GetItemString(uwsgi_dict, "after_req_hook");
                if (up.after_req_hook) {
			Py_INCREF(up.after_req_hook);
			up.after_req_hook_args = PyTuple_New(0);
			Py_INCREF(up.after_req_hook_args);
		}
	}
	// lazy ?
	// Also necessary if uwsgi fork hooks called to update interpreter state
	if (uwsgi.mywid > 0 || up.call_uwsgi_fork_hooks) {
		UWSGI_RELEASE_GIL;
	}

}

void uwsgi_python_master_fixup(int step) {

	static int master_fixed = 0;
	static int worker_fixed = 0;

	if (!uwsgi.master_process) return;

	// Skip master fixup if uwsgi fork hooks called to update interpreter state
	if (up.call_uwsgi_fork_hooks)
		return;

	if (uwsgi.has_threads) {
		if (step == 0) {
			if (!master_fixed) {
				UWSGI_RELEASE_GIL;
				master_fixed = 1;
			}
		}	
		else {
			if (!worker_fixed) {
				UWSGI_GET_GIL;
				worker_fixed = 1;
			}
		}
	}
}

void uwsgi_python_pre_uwsgi_fork() {
	if (!up.call_uwsgi_fork_hooks)
		return;

	if (uwsgi.has_threads) {
		// Acquire the gil and import lock before forking in order to avoid
		// deadlocks in workers
		UWSGI_GET_GIL
#ifdef HAS_NOT_PYOS_FORK_STABLE_API
		_PyImport_AcquireLock();
#else
		PyOS_BeforeFork();
#endif
	}
}


void uwsgi_python_post_uwsgi_fork(int step) {
	if (!up.call_uwsgi_fork_hooks)
		return;

	if (uwsgi.has_threads) {
		if (step == 0) {
			// Release locks within master process
#ifdef HAS_NOT_PYOS_FORK_STABLE_API
			_PyImport_ReleaseLock();
#else
			PyOS_AfterFork_Parent();
#endif
			UWSGI_RELEASE_GIL
		}
		else {
			// Ensure thread state and locks are cleaned up in child process
#ifdef HAS_NOT_PYOS_FORK_STABLE_API
			PyOS_AfterFork();
#else
			PyOS_AfterFork_Child();
#endif
		}
	}
}

void uwsgi_python_enable_threads() {

#ifdef UWSGI_SHOULD_CALL_PYEVAL_INITTHREADS
	PyEval_InitThreads();
#endif
	if (pthread_key_create(&up.upt_save_key, NULL)) {
		uwsgi_error("pthread_key_create()");
		exit(1);
	}
	if (pthread_key_create(&up.upt_gil_key, NULL)) {
		uwsgi_error("pthread_key_create()");
		exit(1);
	}
	pthread_setspecific(up.upt_save_key, (void *) PyThreadState_Get());
	pthread_setspecific(up.upt_gil_key, (void *) PyThreadState_Get());
	pthread_mutex_init(&up.lock_pyloaders, NULL);
	pthread_atfork(uwsgi_python_pthread_prepare, uwsgi_python_pthread_parent, uwsgi_python_pthread_child);

	up.gil_get = gil_real_get;
	up.gil_release = gil_real_release;

	up.swap_ts = simple_threaded_swap_ts;
	up.reset_ts = simple_threaded_reset_ts;

	if (uwsgi.threads > 1) {
		up.swap_ts = threaded_swap_ts;
		up.reset_ts = threaded_reset_ts;
	}

	// Release the newly created gil from call to PyEval_InitThreads above
	// Necessary if uwsgi fork hooks called to update interpreter state
	if (up.call_uwsgi_fork_hooks) {
		UWSGI_RELEASE_GIL
	}

	uwsgi_log("python threads support enabled\n");
	

}

void uwsgi_python_set_thread_name(int core_id) {
	// call threading.current_thread (taken from mod_wsgi, but removes DECREFs as thread in uWSGI are fixed)
	PyObject *threading_module = PyImport_ImportModule("threading");
        if (threading_module) {
                PyObject *threading_module_dict = PyModule_GetDict(threading_module);
                if (threading_module_dict) {
                        PyObject *threading_current = PyDict_GetItemString(threading_module_dict, "current_thread");
                        if (threading_current) {
                                PyObject *current_thread = PyObject_CallObject(threading_current, (PyObject *)NULL);
                                if (!current_thread) {
                                        // ignore the error
                                        PyErr_Clear();
                                }
                                else {
#ifdef PYTHREE
                                        PyObject_SetAttrString(current_thread, "name", PyUnicode_FromFormat("uWSGIWorker%dCore%d", uwsgi.mywid, core_id));
#else
                                        PyObject_SetAttrString(current_thread, "name", PyString_FromFormat("uWSGIWorker%dCore%d", uwsgi.mywid, core_id));
#endif
                                        Py_INCREF(current_thread);
                                }
                        }
                }
        }
}

void uwsgi_python_init_thread(int core_id) {

	// set a new ThreadState for each thread
	PyThreadState *pts;
	pts = PyThreadState_New(up.main_thread->interp);
	pthread_setspecific(up.upt_save_key, (void *) pts);
	pthread_setspecific(up.upt_gil_key, (void *) pts);
#ifdef UWSGI_DEBUG
	uwsgi_log("python ThreadState %d = %p\n", core_id, pts);
#endif
	UWSGI_GET_GIL;
	uwsgi_python_set_thread_name(core_id);
	UWSGI_RELEASE_GIL;
	

}

int uwsgi_check_python_mtime(PyObject *times_dict, char *filename) {
	struct stat st;

	PyObject *py_mtime = PyDict_GetItemString(times_dict, filename); 
	if (!py_mtime) {
		if (stat(filename, &st)) {
			return 0;
		}
		PyDict_SetItemString(times_dict, filename, PyLong_FromLong(st.st_mtime));
	}
	// the record is already tracked;
	else {
		long mtime = PyLong_AsLong(py_mtime);

		if (stat(filename, &st)) {
			return 0;
		}

		if ((long) st.st_mtime != mtime) {
			uwsgi_log("[uwsgi-python-reloader] module/file %s has been modified\n", filename);
			kill(uwsgi.workers[0].pid, SIGHUP);
			return 1;
		}
	}
	return 0;
}

PyObject *uwsgi_python_setup_thread(char *name) {

	// block signals on this thread
        sigset_t smask;
        sigfillset(&smask);
#ifndef UWSGI_DEBUG
        sigdelset(&smask, SIGSEGV);
#endif
        pthread_sigmask(SIG_BLOCK, &smask, NULL);

        PyThreadState *pts = PyThreadState_New(up.main_thread->interp);
        pthread_setspecific(up.upt_save_key, (void *) pts);
        pthread_setspecific(up.upt_gil_key, (void *) pts);

        UWSGI_GET_GIL;

        PyObject *threading_module = PyImport_ImportModule("threading");
        if (threading_module) {
                PyObject *threading_module_dict = PyModule_GetDict(threading_module);
                if (threading_module_dict) {
                        PyObject *threading_current = PyDict_GetItemString(threading_module_dict, "current_thread");
                        if (threading_current) {
                                PyObject *current_thread = PyObject_CallObject(threading_current, (PyObject *)NULL);
                                if (!current_thread) {
                                        // ignore the error
                                        PyErr_Clear();
                                }
                                else {
                                        PyObject_SetAttrString(current_thread, "name", PyString_FromString(name));
                                        Py_INCREF(current_thread);
					return current_thread;
                                }
                        }
                }

        }

        return NULL;
}


void *uwsgi_python_autoreloader_thread(void *foobar) {

	PyObject *new_thread = uwsgi_python_setup_thread("uWSGIAutoReloader");
	if (!new_thread) return NULL;

	PyObject *modules = PyImport_GetModuleDict();

	if (uwsgi.mywid == 1) {
		uwsgi_log("Python auto-reloader enabled\n");
	}

	PyObject *times_dict = PyDict_New();
	char *filename;
	for(;;) {
		UWSGI_RELEASE_GIL;
		sleep(up.auto_reload);
		UWSGI_GET_GIL;
		if (uwsgi.lazy || uwsgi.lazy_apps) {
			// do not start monitoring til the first app is loaded (required for lazy mode)
			if (uwsgi_apps_cnt == 0) continue;
		}
#ifdef UWSGI_PYTHON_OLD
                int pos = 0;
#else
                Py_ssize_t pos = 0;
#endif
		PyObject *mod_name, *mod;
                while (PyDict_Next(modules, &pos, &mod_name, &mod)) {
			if (mod == NULL) continue;
			int found = 0;
			struct uwsgi_string_list *usl = up.auto_reload_ignore;
			while(usl) {
#ifdef PYTHREE
				PyObject *zero = PyUnicode_AsUTF8String(mod_name);
				char *str_mod_name = PyString_AsString(zero);
				int ret_cmp = strcmp(usl->value, str_mod_name);
				Py_DECREF(zero);
				if (!ret_cmp) {
#else
				if (!strcmp(usl->value, PyString_AsString(mod_name))) {
#endif
					found = 1;
					break;
				}
				usl = usl->next;
			}
			if (found) continue;
			if (!PyObject_HasAttrString(mod, "__file__")) continue;
			PyObject *mod_file = PyObject_GetAttrString(mod, "__file__");
			if (!mod_file) continue;
			if (mod_file == Py_None) continue;
#ifdef PYTHREE
			PyObject *zero = PyUnicode_AsUTF8String(mod_file);
			char *mod_filename = PyString_AsString(zero);
#else
			char *mod_filename = PyString_AsString(mod_file);
#endif
			if (!mod_filename) {
#ifdef PYTHREE
				Py_DECREF(zero);
#endif
				continue;
			}
			char *ext = strrchr(mod_filename, '.');
			if (ext && (!strcmp(ext+1, "pyc") || !strcmp(ext+1, "pyd") || !strcmp(ext+1, "pyo"))) {
				filename = uwsgi_concat2n(mod_filename, strlen(mod_filename)-1, "", 0);
			}
			else {
				filename = uwsgi_concat2(mod_filename, "");
			}
			if (uwsgi_check_python_mtime(times_dict, filename)) {
				UWSGI_RELEASE_GIL;
				return NULL;
			}
			free(filename);
#ifdef PYTHREE
			Py_DECREF(zero);
#endif
		}
	}

	return NULL;
}

void uwsgi_python_suspend(struct wsgi_request *wsgi_req) {

	PyGILState_STATE pgst = PyGILState_Ensure();
	PyThreadState *tstate = PyThreadState_GET();
	PyGILState_Release(pgst);

	if (wsgi_req) {
#ifdef UWSGI_PY314
		up.current_py_recursion_remaining[wsgi_req->async_id] = tstate->py_recursion_remaining;
		up.current_frame[wsgi_req->async_id] = tstate->current_frame;
#elif defined UWSGI_PY313
		up.current_c_recursion_remaining[wsgi_req->async_id] = tstate->c_recursion_remaining;
		up.current_py_recursion_remaining[wsgi_req->async_id] = tstate->py_recursion_remaining;
		up.current_frame[wsgi_req->async_id] = tstate->current_frame;
#elif defined UWSGI_PY312
		up.current_c_recursion_remaining[wsgi_req->async_id] = tstate->c_recursion_remaining;
		up.current_py_recursion_remaining[wsgi_req->async_id] = tstate->py_recursion_remaining;
		up.current_frame[wsgi_req->async_id] = tstate->cframe;
#elif defined UWSGI_PY311
		up.current_recursion_remaining[wsgi_req->async_id] = tstate->recursion_remaining;
		up.current_frame[wsgi_req->async_id] = tstate->cframe;
#else
		up.current_recursion_depth[wsgi_req->async_id] = tstate->recursion_depth;
		up.current_frame[wsgi_req->async_id] = tstate->frame;
#endif
	}
	else {
#ifdef UWSGI_PY314
		up.current_main_py_recursion_remaining = tstate->py_recursion_remaining;
		up.current_main_frame = tstate->current_frame;
#elif defined UWSGI_PY313
		up.current_main_c_recursion_remaining = tstate->c_recursion_remaining;
		up.current_main_py_recursion_remaining = tstate->py_recursion_remaining;
		up.current_main_frame = tstate->current_frame;
#elif defined UWSGI_PY312
		up.current_main_c_recursion_remaining = tstate->c_recursion_remaining;
		up.current_main_py_recursion_remaining = tstate->py_recursion_remaining;
		up.current_main_frame = tstate->cframe;
#elif defined UWSGI_PY311
		up.current_main_recursion_remaining = tstate->recursion_remaining;
		up.current_main_frame = tstate->cframe;
#else
		up.current_main_recursion_depth = tstate->recursion_depth;
		up.current_main_frame = tstate->frame;
#endif
	}

}

char *uwsgi_python_code_string(char *id, char *code, char *function, char *key, uint16_t keylen) {

	PyObject *cs_module = NULL;
	PyObject *cs_dict = NULL;

	UWSGI_GET_GIL;

	cs_module = PyImport_ImportModule(id);
	if (!cs_module) {
		PyErr_Clear();
		cs_module = uwsgi_pyimport_by_filename(id, code);
	}

	if (!cs_module) {
		UWSGI_RELEASE_GIL;
		return NULL;
	}

	cs_dict = PyModule_GetDict(cs_module);
	if (!cs_dict) {
		PyErr_Print();
		UWSGI_RELEASE_GIL;
		return NULL;
	}
	
	PyObject *func = PyDict_GetItemString(cs_dict, function);
	if (!func) {
		uwsgi_log("function %s not available in %s\n", function, code);
		PyErr_Print();
		UWSGI_RELEASE_GIL;
		return NULL;
	}

	PyObject *args = PyTuple_New(1);

	PyTuple_SetItem(args, 0, PyString_FromStringAndSize(key, keylen));

	PyObject *ret = python_call(func, args, 0, NULL);
	Py_DECREF(args);
	if (ret && PyString_Check(ret)) {
		char *val = PyString_AsString(ret);
		UWSGI_RELEASE_GIL;
		return val;
	}

	UWSGI_RELEASE_GIL;
	return NULL;
	
}

int uwsgi_python_signal_handler(uint8_t sig, void *handler) {

	UWSGI_GET_GIL;

	PyObject *args = PyTuple_New(1);
	PyObject *ret;

	if (!args)
		goto clear;

	if (!handler) goto clear;


	PyTuple_SetItem(args, 0, PyInt_FromLong(sig));

	ret = python_call(handler, args, 0, NULL);
	Py_DECREF(args);
	if (ret) {
		Py_DECREF(ret);
		UWSGI_RELEASE_GIL;
		return 0;
	}

clear:
	UWSGI_RELEASE_GIL;
	return -1;
}

uint64_t uwsgi_python_rpc(void *func, uint8_t argc, char **argv, uint16_t argvs[], char **buffer) {

	UWSGI_GET_GIL;

	uint8_t i;
	char *rv;
	size_t rl;

	PyObject *pyargs = PyTuple_New(argc);
	PyObject *ret;

	if (!pyargs) {
		UWSGI_RELEASE_GIL;
		return 0;
	}

	for (i = 0; i < argc; i++) {
		PyTuple_SetItem(pyargs, i, PyString_FromStringAndSize(argv[i], argvs[i]));
	}

	ret = python_call((PyObject *) func, pyargs, 0, NULL);
	Py_DECREF(pyargs);
	if (ret) {
		if (PyString_Check(ret)) {
			rv = PyString_AsString(ret);
			rl = PyString_Size(ret);
			if (rl > 0) {
				*buffer = uwsgi_malloc(rl);
				memcpy(*buffer, rv, rl);
				Py_DECREF(ret);
				UWSGI_RELEASE_GIL;
				return rl;
			}
		}
		Py_DECREF(ret);
	}

	if (PyErr_Occurred())
		PyErr_Print();

	UWSGI_RELEASE_GIL;

	return 0;

}

void uwsgi_python_add_item(char *key, uint16_t keylen, char *val, uint16_t vallen, void *data) {

	PyObject *pydict = (PyObject *) data;

	PyObject *o_key = PyString_FromStringAndSize(key, keylen);
	PyObject *zero = PyString_FromStringAndSize(val, vallen);
	PyDict_SetItem(pydict, o_key, zero);
	Py_DECREF(o_key);
	Py_DECREF(zero);
}

PyObject *uwsgi_python_dict_from_spooler_content(char *filename, char *buf, uint16_t len, char *body, size_t body_len) {

	PyObject *spool_dict = PyDict_New();

	PyObject *value = PyString_FromString(filename);
	PyDict_SetItemString(spool_dict, "spooler_task_name", value);
	Py_DECREF(value);

	if (uwsgi_hooked_parse(buf, len, uwsgi_python_add_item, spool_dict))
		return NULL;

	if (body && body_len > 0) {
		PyObject *value = PyString_FromStringAndSize(body, body_len);
		PyDict_SetItemString(spool_dict, "body", value);
		Py_DECREF(value);
	}

	return spool_dict;
}

int uwsgi_python_spooler(char *filename, char *buf, uint16_t len, char *body, size_t body_len) {

	static int random_seed_reset = 0;

	UWSGI_GET_GIL;

	if (!random_seed_reset) {
		uwsgi_python_reset_random_seed();
		random_seed_reset = 1;	
	}

	if (!up.embedded_dict) {
		// ignore
		UWSGI_RELEASE_GIL;
		return 0;
	}

	PyObject *spool_func = PyDict_GetItemString(up.embedded_dict, "spooler");
	if (!spool_func) {
		// ignore
		UWSGI_RELEASE_GIL;
		return 0;
	}

	int retval = -1;
	PyObject *pyargs = PyTuple_New(1);
	PyObject *ret = NULL;

	PyObject *spool_dict = uwsgi_python_dict_from_spooler_content(filename, buf, len, body, body_len);
	if (!spool_dict) {
		retval = -2;
		goto clear;
	}

	// PyTuple_SetItem steals a reference !!!
	Py_INCREF(spool_dict);
	PyTuple_SetItem(pyargs, 0, spool_dict);

	ret = python_call(spool_func, pyargs, 0, NULL);

	if (ret) {
		if (!PyInt_Check(ret)) {
			retval = -1; // error, retry
		} else {
			retval = (int) PyInt_AsLong(ret);
		}
		goto clear;
	}

	if (PyErr_Occurred())
		PyErr_Print();

	// error, retry
	retval = -1;

clear:
	Py_XDECREF(ret);
	Py_XDECREF(pyargs);
	Py_XDECREF(spool_dict);
	UWSGI_RELEASE_GIL;
	return retval;
}

void uwsgi_python_resume(struct wsgi_request *wsgi_req) {

	PyGILState_STATE pgst = PyGILState_Ensure();
	PyThreadState *tstate = PyThreadState_GET();
	PyGILState_Release(pgst);

	if (wsgi_req) {
#ifdef UWSGI_PY314
		tstate->py_recursion_remaining = up.current_py_recursion_remaining[wsgi_req->async_id];
		tstate->current_frame = up.current_frame[wsgi_req->async_id];
#elif defined UWSGI_PY313
		tstate->c_recursion_remaining = up.current_c_recursion_remaining[wsgi_req->async_id];
		tstate->py_recursion_remaining = up.current_py_recursion_remaining[wsgi_req->async_id];
		tstate->current_frame = up.current_frame[wsgi_req->async_id];
#elif defined UWSGI_PY312
		tstate->c_recursion_remaining = up.current_c_recursion_remaining[wsgi_req->async_id];
		tstate->py_recursion_remaining = up.current_py_recursion_remaining[wsgi_req->async_id];
		tstate->cframe = up.current_frame[wsgi_req->async_id];
#elif defined UWSGI_PY311
		tstate->recursion_remaining = up.current_recursion_remaining[wsgi_req->async_id];
		tstate->cframe = up.current_frame[wsgi_req->async_id];
#else
		tstate->recursion_depth = up.current_recursion_depth[wsgi_req->async_id];
		tstate->frame = up.current_frame[wsgi_req->async_id];
#endif
	}
	else {
#ifdef UWSGI_PY314
		tstate->py_recursion_remaining = up.current_main_py_recursion_remaining;
		tstate->current_frame = up.current_main_frame;
#elif defined UWSGI_PY313
		tstate->c_recursion_remaining = up.current_main_c_recursion_remaining;
		tstate->py_recursion_remaining = up.current_main_py_recursion_remaining;
		tstate->current_frame = up.current_main_frame;
#elif defined UWSGI_PY312
		tstate->c_recursion_remaining = up.current_main_c_recursion_remaining;
		tstate->py_recursion_remaining = up.current_main_py_recursion_remaining;
		tstate->cframe = up.current_main_frame;
#elif defined UWSGI_PY311
		tstate->recursion_remaining = up.current_main_recursion_remaining;
		tstate->cframe = up.current_main_frame;
#else
		tstate->recursion_depth = up.current_main_recursion_depth;
		tstate->frame = up.current_main_frame;
#endif
	}

}

void uwsgi_python_fixup() {
	// set hacky modifier 30
	uwsgi.p[30] = uwsgi_malloc( sizeof(struct uwsgi_plugin) );
	memcpy(uwsgi.p[30], uwsgi.p[0], sizeof(struct uwsgi_plugin) );
	uwsgi.p[30]->init_thread = NULL;
	uwsgi.p[30]->atexit = NULL;
}

void uwsgi_python_hijack(void) {

	// the pyshell will be execute only in the first worker

	FILE *pyfile;
	if (up.pyrun) {
		uwsgi.workers[uwsgi.mywid].hijacked = 1;
		UWSGI_GET_GIL;
		pyfile = fopen(up.pyrun, "r");
		if (!pyfile) {
			uwsgi_error_open(up.pyrun);
			exit(1);
		}
		PyRun_SimpleFile(pyfile, up.pyrun);	
		// could be never executed
		exit(0);
	}

	if (up.pyshell_oneshot && uwsgi.workers[uwsgi.mywid].hijacked_count > 0) {
		uwsgi.workers[uwsgi.mywid].hijacked = 0;
		return;
	}
	if (up.pyshell && uwsgi.mywid == 1) {
		uwsgi.workers[uwsgi.mywid].hijacked = 1;
		uwsgi.workers[uwsgi.mywid].hijacked_count++;
		// re-map stdin to stdout and stderr if we are logging to a file
		if (uwsgi.logfile) {
			if (dup2(0, 1) < 0) {
				uwsgi_error("dup2()");
			}
			if (dup2(0, 2) < 0) {
				uwsgi_error("dup2()");
			}
		}
		UWSGI_GET_GIL;
		int ret = -1;
		if (up.pyshell[0] != 0) {
			ret = PyRun_SimpleString(up.pyshell);
		}
		else {
			PyImport_ImportModule("readline");

			ret = PyRun_InteractiveLoop(stdin, "uwsgi");
		}
		if (up.pyshell_oneshot) {
			exit(UWSGI_DE_HIJACKED_CODE);
		}

		if (ret == 0) {
			exit(UWSGI_QUIET_CODE);
		}
		exit(0);
	}
}

int uwsgi_python_mule(char *opt) {

	if (uwsgi_endswith(opt, ".py")) {
		UWSGI_GET_GIL;
		uwsgi_pyimport_by_filename("__main__", opt);
		UWSGI_RELEASE_GIL;
		return 1;
	}
	else if (strchr(opt, ':')) {
		UWSGI_GET_GIL;
		PyObject *result = NULL;
		PyObject *arglist = Py_BuildValue("()");
		PyObject *callable = uwsgi_mount_loader(opt);
		if (callable) {
			result = PyObject_CallObject(callable, arglist);
		}
		Py_XDECREF(result);
		Py_XDECREF(arglist);
		Py_XDECREF(callable);
		UWSGI_RELEASE_GIL;
		return 1;
	}
	return 0;
	
}

int uwsgi_python_mule_msg(char *message, size_t len) {

	UWSGI_GET_GIL;

	PyObject *mule_msg_hook = PyDict_GetItemString(up.embedded_dict, "mule_msg_hook");
        if (!mule_msg_hook) {
                // ignore
                UWSGI_RELEASE_GIL;
                return 0;
        }

	PyObject *pyargs = PyTuple_New(1);
        PyTuple_SetItem(pyargs, 0, PyString_FromStringAndSize(message, len));

        PyObject *ret = python_call(mule_msg_hook, pyargs, 0, NULL);
	Py_DECREF(pyargs);
	if (ret) {
		Py_DECREF(ret);
	}

	if (PyErr_Occurred())
                PyErr_Print();

	UWSGI_RELEASE_GIL;
	return 1;
}

static void uwsgi_python_harakiri(int wid) {

	if (up.tracebacker) {

        	char buf[8192];
		char *wid_str = uwsgi_num2str(wid);
		char *address = uwsgi_concat2(up.tracebacker, wid_str);

        	int fd = uwsgi_connect(address, -1, 0);
		if (fd < 1)
			goto exit;

		for(;;) {
                	int ret = uwsgi_waitfd(fd, uwsgi.socket_timeout);
                	if (ret <= 0) goto cleanup;
                	ssize_t len = read(fd, buf, 8192);
                	if (len <= 0) goto cleanup;
                	uwsgi_log("%.*s", (int) len, buf);
		}

cleanup:
		close(fd);
exit:
		free(wid_str);
		free(address);
	}

}
/*
	# you can use this logger to offload logging to python
	# be sure to configure it to not log to stderr otherwise you will generate a loop
	import logging
	logging.basicConfig(filename='/tmp/pippo.log')
*/
static ssize_t uwsgi_python_logger(struct uwsgi_logger *ul, char *message, size_t len) {
	if (!Py_IsInitialized()) return -1;

	UWSGI_GET_GIL

	if (!ul->configured) {
		PyObject *py_logging = PyImport_ImportModule("logging");
		if (!py_logging) goto clear;
		PyObject *py_logging_dict = PyModule_GetDict(py_logging);
		if (!py_logging_dict) goto clear;
		PyObject *py_getLogger = PyDict_GetItemString(py_logging_dict, "getLogger");
		if (!py_getLogger) goto clear;
		PyObject *py_getLogger_args = NULL;
		if (ul->arg) {
			py_getLogger_args = PyTuple_New(1);
			PyTuple_SetItem(py_getLogger_args, 0, UWSGI_PYFROMSTRING(ul->arg));
		}
                ul->data = (void *) PyObject_CallObject(py_getLogger, py_getLogger_args);
                if (PyErr_Occurred()) {
                	PyErr_Clear();
                }
		Py_XDECREF(py_getLogger_args);
		if (!ul->data) goto clear;
		ul->configured = 1;
	}

	PyObject_CallMethod((PyObject *) ul->data, "error", "(s#)", message, len);
        if (PyErr_Occurred()) {
        	PyErr_Clear();
        }
	UWSGI_RELEASE_GIL
	return len;
clear:

	UWSGI_RELEASE_GIL
	return -1;
}

static void uwsgi_python_on_load() {
	uwsgi_register_logger("python", uwsgi_python_logger);
}

static int uwsgi_python_worker() {
	if (!up.worker_override)
		return 0;
	UWSGI_GET_GIL;
	// ensure signals can be used again from python
	// Necessary if fork hooks have been not used to update interpreter state
	if (!up.call_osafterfork && !up.call_uwsgi_fork_hooks)
#ifdef HAS_NOT_PYOS_FORK_STABLE_API
		PyOS_AfterFork();
#else
		PyOS_AfterFork_Child();
#endif
	FILE *pyfile = fopen(up.worker_override, "r");
	if (!pyfile) {
		uwsgi_error_open(up.worker_override);
		exit(1);
	}
	PyRun_SimpleFile(pyfile, up.worker_override);
	return 1;
}

struct uwsgi_plugin python_plugin = {
	.name = "python",
	.alias = "python",
	.modifier1 = 0,
	.init = uwsgi_python_init,
	.post_fork = uwsgi_python_post_fork,
	.options = uwsgi_python_options,
	.request = uwsgi_request_wsgi,
	.after_request = uwsgi_after_request_wsgi,

	.preinit_apps = uwsgi_python_preinit_apps,
	.init_apps = uwsgi_python_init_apps,

	.fixup = uwsgi_python_fixup,
	.master_fixup = uwsgi_python_master_fixup,

	.mount_app = uwsgi_python_mount_app,

	.enable_threads = uwsgi_python_enable_threads,
	.init_thread = uwsgi_python_init_thread,

	.magic = uwsgi_python_magic,

	.suspend = uwsgi_python_suspend,
	.resume = uwsgi_python_resume,

	.harakiri = uwsgi_python_harakiri,

	.hijack_worker = uwsgi_python_hijack,
	.spooler_init = uwsgi_python_spooler_init,

	.signal_handler = uwsgi_python_signal_handler,
	.rpc = uwsgi_python_rpc,

	.mule = uwsgi_python_mule,
	.mule_msg = uwsgi_python_mule_msg,

	.on_load = uwsgi_python_on_load,

	.spooler = uwsgi_python_spooler,

	.atexit = uwsgi_python_atexit,

	.code_string = uwsgi_python_code_string,

	.exception_class = uwsgi_python_exception_class,
	.exception_msg = uwsgi_python_exception_msg,
	.exception_repr = uwsgi_python_exception_repr,
	.exception_log = uwsgi_python_exception_log,
	.backtrace = uwsgi_python_backtrace,

	.worker = uwsgi_python_worker,

	.pre_uwsgi_fork = uwsgi_python_pre_uwsgi_fork,
	.post_uwsgi_fork = uwsgi_python_post_uwsgi_fork,
};
