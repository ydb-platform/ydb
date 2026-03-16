#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;

#ifdef HAS_NOT_PyFrame_GetLineNumber
int PyFrame_GetLineNumber(PyFrameObject *frame) {
	if (frame->f_trace) {
		return frame->f_lineno;
	}
	else {
		return PyCode_Addr2Line(frame->f_code, frame->f_lasti);
	}
}
#endif

#if PY_VERSION_HEX < 0x030900B1
PyCodeObject* PyFrame_GetCode(PyFrameObject *frame)
{
	Py_INCREF(frame->f_code);
	return frame->f_code;
}
#endif

#ifdef PYTHREE
#undef PyString_AsString
static char *PyString_AsString(PyObject *o) {
	PyObject *zero = PyUnicode_AsLatin1String(o);
	if (!zero) return "";
        return PyBytes_AsString(zero);
}
#endif

int uwsgi_python_profiler_call(PyObject *obj, PyFrameObject *frame, int what, PyObject *arg) {

	static uint64_t last_ts = 0;
        uint64_t now = uwsgi_micros();
        uint64_t delta = 0;
	PyCodeObject *code;

	switch(what) {
		case PyTrace_CALL:
			if (last_ts == 0) delta = 0;
                	else delta = now - last_ts;
                	last_ts = now;
			code = PyFrame_GetCode(frame);
			uwsgi_log("[uWSGI Python profiler %llu] CALL: %s (line %d) -> %s %d args, stacksize %d\n",
				(unsigned long long) delta,
				PyString_AsString(code->co_filename),
				PyFrame_GetLineNumber(frame),
				PyString_AsString(code->co_name), code->co_argcount, code->co_stacksize);
			Py_DECREF(code);
			break;
		case PyTrace_C_CALL:
			if (last_ts == 0) delta = 0;
                	else delta = now - last_ts;
                	last_ts = now;
			code = PyFrame_GetCode(frame);
			uwsgi_log("[uWSGI Python profiler %llu] C CALL: %s (line %d) -> %s %d args, stacksize %d\n",
				(unsigned long long) delta,
				PyString_AsString(code->co_filename),
				PyFrame_GetLineNumber(frame),
				PyEval_GetFuncName(arg), code->co_argcount, code->co_stacksize);
			Py_DECREF(code);
			break;
	}

	return 0;
}

int uwsgi_python_tracer(PyObject *obj, PyFrameObject *frame, int what, PyObject *arg) {

	static uint64_t last_ts = 0;
	uint64_t now = uwsgi_micros();
	uint64_t delta = 0;

	if (what == PyTrace_LINE) {
		if (last_ts == 0) {
			delta = 0;
		}
		else {
			delta = now - last_ts;
		}
		last_ts = now;
		PyCodeObject *code = PyFrame_GetCode(frame);
		uwsgi_log("[uWSGI Python profiler %llu] file %s line %d: %s argc:%d\n", (unsigned long long)delta,  PyString_AsString(code->co_filename), PyFrame_GetLineNumber(frame), PyString_AsString(code->co_name), code->co_argcount);
		Py_DECREF(code);
	}

        return 0;
}

