#include <uwsgi.h>
/* See https://docs.python.org/3.10/whatsnew/3.10.html#id2 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pythread.h>

#include <frameobject.h>

#define PYTHON_APP_TYPE_WSGI		0
#define PYTHON_APP_TYPE_WEB3		1
#define PYTHON_APP_TYPE_WSGI2		2
#define PYTHON_APP_TYPE_PUMP		3
#define PYTHON_APP_TYPE_WSGI_LITE	4

#if PY_MINOR_VERSION == 4 && PY_MAJOR_VERSION == 2
#define Py_ssize_t ssize_t
#define UWSGI_PYTHON_OLD
#endif

#if (PY_VERSION_HEX >= 0x030b0000)
#  define UWSGI_PY311
#endif

#if (PY_VERSION_HEX >= 0x030c0000)
#  define UWSGI_PY312
#endif

#if (PY_VERSION_HEX >= 0x030d0000)
#  define UWSGI_PY313
#endif

#if (PY_VERSION_HEX >= 0x030e0000)
#  define UWSGI_PY314
#endif

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION < 7
#define HAS_NOT_PyMemoryView_FromBuffer
#endif

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION < 7
#define HAS_NOT_PyFrame_GetLineNumber 
#endif

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 2
#define HAS_NOT_PyFrame_GetLineNumber 
#endif

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 0
#define HAS_NO_ERRORS_IN_PyFile_FromFd
#endif

#if (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 7) || PY_MAJOR_VERSION < 3
#define HAS_NOT_PYOS_FORK_STABLE_API
#endif

#if PY_MAJOR_VERSION > 2
#define PYTHREE
#endif

#if (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 7) || PY_MAJOR_VERSION < 3
#define UWSGI_SHOULD_CALL_PYEVAL_INITTHREADS
#endif

#if (PY_VERSION_HEX < 0x03090000)
#ifndef Py_SET_SIZE
#define Py_SET_SIZE(o, size) ((o)->ob_size = (size))
#endif
#endif

#define UWSGI_GET_GIL up.gil_get();
#define UWSGI_RELEASE_GIL up.gil_release();

#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(x, y) PyObject_HEAD_INIT(x) y,
#endif

#define uwsgi_py_write_set_exception(x) if (!uwsgi.disable_write_exception) { PyErr_SetString(PyExc_IOError, "write error"); };
#define uwsgi_py_write_exception(x) uwsgi_py_write_set_exception(x); uwsgi_manage_exception(x, 0);


#define uwsgi_py_check_write_errors if (wsgi_req->write_errors > 0 && uwsgi.write_errors_exception_only) {\
                        uwsgi_py_write_set_exception(wsgi_req);\
                }\
                else if (wsgi_req->write_errors > uwsgi.write_errors_tolerance)\

PyAPI_FUNC(PyObject *) PyMarshal_WriteObjectToString(PyObject *, int);
PyAPI_FUNC(PyObject *) PyMarshal_ReadObjectFromString(char *, Py_ssize_t);

#ifdef PYTHREE
#define UWSGI_PYFROMSTRING(x) PyUnicode_FromString(x)
#define UWSGI_PYFROMSTRINGSIZE(x, y) PyUnicode_FromStringAndSize(x, y)
#define PyInt_FromLong	PyLong_FromLong
#define PyInt_AsLong	PyLong_AsLong
#define PyInt_Check	PyLong_Check
#define PyString_Check	PyBytes_Check
#define	PyString_FromStringAndSize	PyBytes_FromStringAndSize
#define	PyString_FromFormat	PyBytes_FromFormat
#define	PyString_FromString	PyBytes_FromString
#define	PyString_Size		PyBytes_Size
#define	PyString_Concat		PyBytes_Concat
#define	PyString_AsString	(char *) PyBytes_AsString
#define PyFile_FromFile(A,B,C,D) PyFile_FromFd(fileno((A)), (B), (C), -1, NULL, NULL, NULL, 0)
#define uwsgi_py_dict_get(a, b) PyDict_GetItem(a, PyBytes_FromString(b));
#define uwsgi_py_dict_del(a, b) PyDict_DelItem(a, PyBytes_FromString(b));

#else
#define UWSGI_PYFROMSTRING(x) PyString_FromString(x)
#define UWSGI_PYFROMSTRINGSIZE(x, y) PyString_FromStringAndSize(x, y)
#define uwsgi_py_dict_get(a, b) PyDict_GetItemString(a, b)
#define uwsgi_py_dict_del(a, b) PyDict_DelItemString(a, b)
#endif

#define LOADER_DYN              0
#define LOADER_UWSGI            1
#define LOADER_FILE             2
#define LOADER_PASTE            3
#define LOADER_EVAL             4
#define LOADER_CALLABLE         5
#define LOADER_STRING_CALLABLE  6
#define LOADER_MOUNT            7
#define LOADER_PECAN            8

#define LOADER_MAX              9

typedef struct uwsgi_Input {
        PyObject_HEAD
        struct wsgi_request *wsgi_req;
} uwsgi_Input;

struct uwsgi_python {

	char *home;
	int optimize;

	char *argv;
	int argc;

#ifdef PYTHREE
	wchar_t **py_argv;
#else
	char **py_argv;
#endif

	PyObject *wsgi_spitout;
	PyObject *wsgi_writeout;

	PyThreadState *main_thread;

	char *test_module;

	char *pyshell;
	int pyshell_oneshot;

	
	struct uwsgi_string_list *python_path;
	struct uwsgi_string_list *import_list;
	struct uwsgi_string_list *shared_import_list;
	struct uwsgi_string_list *spooler_import_list;
	struct uwsgi_string_list *post_pymodule_alias;
	struct uwsgi_string_list *pymodule_alias;

	PyObject *loader_dict;
	PyObject* (*loaders[LOADER_MAX]) (void *);

	char *pecan;

	char *wsgi_config;
	char *file_config;
	char *paste;
	int paste_logger;
	char *eval;

	char *web3;
	char *pump;
	char *wsgi_lite;

	char *callable;

#ifdef UWSGI_PY314
	int *current_py_recursion_remaining;
	struct _PyInterpreterFrame **current_frame;

	int current_main_py_recursion_remaining;
	struct _PyInterpreterFrame *current_main_frame;
#elif defined UWSGI_PY313
	int *current_c_recursion_remaining;
	int *current_py_recursion_remaining;
	struct _PyInterpreterFrame **current_frame;

	int current_main_c_recursion_remaining;
	int current_main_py_recursion_remaining;
	struct _PyInterpreterFrame *current_main_frame;
#elif defined UWSGI_PY312
	int *current_c_recursion_remaining;
	int *current_py_recursion_remaining;
	_PyCFrame **current_frame;

	int current_main_c_recursion_remaining;
	int current_main_py_recursion_remaining;
	_PyCFrame *current_main_frame;
#elif defined UWSGI_PY311
	int *current_recursion_remaining;
	_PyCFrame **current_frame;

	int current_main_recursion_remaining;
	_PyCFrame *current_main_frame;
#else
	int *current_recursion_depth;
	struct _frame **current_frame;

	int current_main_recursion_depth;
	struct _frame *current_main_frame;
#endif

	void (*swap_ts)(struct wsgi_request *, struct uwsgi_app *);
	void (*reset_ts)(struct wsgi_request *, struct uwsgi_app *);

	pthread_key_t upt_save_key;
	pthread_key_t upt_gil_key;
	pthread_mutex_t lock_pyloaders;
	void (*gil_get) (void);
	void (*gil_release) (void);
	int auto_reload;
	char *tracebacker;
	struct uwsgi_string_list *auto_reload_ignore;

	PyObject *workers_tuple;
	PyObject *embedded_dict;

	char *wsgi_env_behaviour;

	void *(*wsgi_env_create)(struct wsgi_request *, struct uwsgi_app *);
	void (*wsgi_env_destroy)(struct wsgi_request *);


	int pep3333_input;

	void (*extension)(void);

	int reload_os_env;

	PyObject *after_req_hook;
	PyObject *after_req_hook_args;

	char *pyrun;
	int start_response_nodelay;

	char *programname;
	int wsgi_strict;
	int wsgi_accept_buffer;

	char *raw;
	PyObject *raw_callable;

	struct uwsgi_string_list *sharedarea;

	int call_osafterfork;

	int wsgi_disable_file_wrapper;

	char *worker_override;

	char *executable;

	int call_uwsgi_fork_hooks;
};



void init_uwsgi_vars(void);
void init_uwsgi_embedded_module(void);


void uwsgi_wsgi_config(char *);
void uwsgi_paste_config(char *);
void uwsgi_file_config(char *);
void uwsgi_eval_config(char *);

int init_uwsgi_app(int, void *, struct wsgi_request *, PyThreadState *, int);


PyObject *py_eventfd_read(PyObject *, PyObject *);
PyObject *py_eventfd_write(PyObject *, PyObject *);


int manage_python_response(struct wsgi_request *);
int uwsgi_python_call(struct wsgi_request *, PyObject *, PyObject *);
PyObject *python_call(PyObject *, PyObject *, int, struct wsgi_request *);

PyObject *py_uwsgi_sendfile(PyObject *, PyObject *);

PyObject *py_uwsgi_write(PyObject *, PyObject *);
PyObject *py_uwsgi_spit(PyObject *, PyObject *);

void init_pyargv(void);

void *uwsgi_request_subhandler_web3(struct wsgi_request *, struct uwsgi_app *);
int uwsgi_response_subhandler_web3(struct wsgi_request *);

void *uwsgi_request_subhandler_pump(struct wsgi_request *, struct uwsgi_app *);
int uwsgi_response_subhandler_pump(struct wsgi_request *);

PyObject *uwsgi_uwsgi_loader(void *);
PyObject *uwsgi_dyn_loader(void *);
PyObject *uwsgi_file_loader(void *);
PyObject *uwsgi_eval_loader(void *);
PyObject *uwsgi_pecan_loader(void *);
PyObject *uwsgi_paste_loader(void *);
PyObject *uwsgi_callable_loader(void *);
PyObject *uwsgi_string_callable_loader(void *);
PyObject *uwsgi_mount_loader(void *);

char *get_uwsgi_pymodule(char *);
PyObject *get_uwsgi_pydict(char *);

int uwsgi_request_wsgi(struct wsgi_request *);
void uwsgi_after_request_wsgi(struct wsgi_request *);

void *uwsgi_request_subhandler_wsgi(struct wsgi_request *, struct uwsgi_app*);
int uwsgi_response_subhandler_wsgi(struct wsgi_request *);

void gil_real_get(void);
void gil_real_release(void);
void gil_fake_get(void);
void gil_fake_release(void);

void init_uwsgi_module_advanced(PyObject *);
void init_uwsgi_module_spooler(PyObject *);
void init_uwsgi_module_sharedarea(PyObject *);
void init_uwsgi_module_cache(PyObject *);
void init_uwsgi_module_queue(PyObject *);
void init_uwsgi_module_snmp(PyObject *);

PyObject *uwsgi_python_dict_from_spooler_content(char *, char *, uint16_t, char *, size_t);

PyObject *uwsgi_pyimport_by_filename(char *, char *);

void threaded_swap_ts(struct wsgi_request *, struct uwsgi_app *);
void simple_swap_ts(struct wsgi_request *, struct uwsgi_app *);
void simple_threaded_swap_ts(struct wsgi_request *, struct uwsgi_app *);
void threaded_reset_ts(struct wsgi_request *, struct uwsgi_app *);
void simple_reset_ts(struct wsgi_request *, struct uwsgi_app *);
void simple_threaded_reset_ts(struct wsgi_request *, struct uwsgi_app *);

int uwsgi_python_profiler_call(PyObject *, PyFrameObject *, int, PyObject *);
int uwsgi_python_tracer(PyObject *, PyFrameObject *, int, PyObject *);

void uwsgi_python_reset_random_seed(void);

char *uwsgi_pythonize(char *);
void *uwsgi_python_autoreloader_thread(void *);
void *uwsgi_python_tracebacker_thread(void *);

int uwsgi_python_do_send_headers(struct wsgi_request *);
void *uwsgi_python_tracebacker_thread(void *);
PyObject *uwsgi_python_setup_thread(char *);

struct uwsgi_buffer *uwsgi_python_exception_class(struct wsgi_request *);
struct uwsgi_buffer *uwsgi_python_exception_msg(struct wsgi_request *);
struct uwsgi_buffer *uwsgi_python_exception_repr(struct wsgi_request *);
struct uwsgi_buffer *uwsgi_python_backtrace(struct wsgi_request *);
void uwsgi_python_exception_log(struct wsgi_request *);

int uwsgi_python_send_body(struct wsgi_request *, PyObject *);

int uwsgi_request_python_raw(struct wsgi_request *);

void uwsgi_python_set_thread_name(int);

void uwsgi_python_add_item(char *, uint16_t, char *, uint16_t, void *);

#define py_current_wsgi_req() current_wsgi_req();\
			if (!wsgi_req) {\
				return PyErr_Format(PyExc_SystemError, "you can call uwsgi api function only from the main callable");\
			}

#define uwsgi_pyexit {PyErr_Print();exit(1);}

#ifdef __linux__
int uwsgi_init_symbol_import(void);
#endif
