#include "../python/uwsgi_python.h"

int uwsgi_gevent_wait_write_hook(int, int);
int uwsgi_gevent_wait_read_hook(int, int);
int uwsgi_gevent_wait_milliseconds_hook(int);

#define GEVENT_SWITCH PyObject *gswitch = python_call(ugevent.greenlet_switch, ugevent.greenlet_switch_args, 0, NULL); if (gswitch) { Py_DECREF(gswitch); }
#define GET_CURRENT_GREENLET python_call(ugevent.get_current, ugevent.get_current_args, 0, NULL)
#define free_req_queue uwsgi.async_queue_unused_ptr++; uwsgi.async_queue_unused[uwsgi.async_queue_unused_ptr] = wsgi_req
#define stop_the_watchers if (timer) { ret = PyObject_CallMethod(timer, "stop", NULL);\
                          if (ret) { Py_DECREF(ret); } }\
                          ret = PyObject_CallMethod(watcher, "stop", NULL);\
                          if (ret) { Py_DECREF(ret); }

#define stop_the_watchers_and_clear stop_the_watchers\
                        Py_DECREF(current); Py_DECREF(current_greenlet);\
                        Py_DECREF(watcher);\
                        if (timer) {Py_DECREF(timer);}

#define stop_the_io ret = PyObject_CallMethod(watcher, "stop", NULL);\
                    if (ret) { Py_DECREF(ret); }

#define stop_the_io_and_clear stop_the_io;\
                        Py_DECREF(current); Py_DECREF(current_greenlet);\
                        Py_DECREF(watcher);\

#define stop_the_c_watchers if (c_watchers) { int j;\
                                for(j=0;j<count;j++) {\
                                        if (c_watchers[j]) {\
                                                ret = PyObject_CallMethod(watcher, "stop", NULL);\
                                                if (ret) { Py_DECREF(ret); }\
                                                Py_DECREF(c_watchers[j]);\
                                        }\
                                }\
                                free(c_watchers);\
                        }




struct uwsgi_gevent {
        PyObject *greenlet_switch;
        PyObject *greenlet_switch_args;
        PyObject *get_current;
        PyObject *get_current_args;
        PyObject *hub;
        PyObject *hub_loop;
        PyObject *spawn;
        PyObject *signal;
        PyObject *greenlet_args;
        PyObject *signal_args;
        PyObject *my_signal_watcher;
        PyObject *signal_watcher;
        PyObject **watchers;
	PyObject *ctrl_gl;
	int destroy;
	int monkey;
	int wait_for_hub;
	int early_monkey;
};

