/* SCTP bindings for Python
 *
 * _sctp.c: C-side bindings
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or (at your
 * option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; If not, see <http://www.gnu.org/licenses/>.
 *
 * Elvis Pfützenreuter (elvis.pfutzenreuter@{gmail.com,indt.org.br})
 * Copyright (c) 2005 Instituto Nokia de Tecnologia
 */

#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <arpa/inet.h>
#include <memory.h>
#include <string.h>
#include <errno.h>
#include "_sctp.h"


/* Python 2 and 3 initialization mess */

struct module_state {
    PyObject *error;
};

#if PY_MAJOR_VERSION >= 3

    #define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))

#else

    #define GETSTATE(m) (&_state)
    static struct module_state _state;

#endif

static PyObject * error_out(PyObject *m) {
    struct module_state *st = GETSTATE(m);
    PyErr_SetString(st->error, "something bad happened");
    return NULL;
}


static PyObject* getconstant(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_multibuf(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_noconnect(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_prsctp(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_sat_network(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_setprimary(PyObject* dummy, PyObject* args);
static PyObject* have_sctp_addip(PyObject* dummy, PyObject* args);
static PyObject* get_mappedv4(PyObject* dummy, PyObject* args);
static PyObject* set_mappedv4(PyObject* dummy, PyObject* args);
static PyObject* get_nodelay(PyObject* dummy, PyObject* args);
static PyObject* set_nodelay(PyObject* dummy, PyObject* args);
static PyObject* get_initparams(PyObject* dummy, PyObject* args);
static PyObject* set_initparams(PyObject* dummy, PyObject* args);
static PyObject* peeloff(PyObject* dummy, PyObject* args);
static PyObject* get_events(PyObject* dummy, PyObject* args);
static PyObject* set_events(PyObject* dummy, PyObject* args);
static PyObject* get_maxseg(PyObject* dummy, PyObject* args);
static PyObject* set_maxseg(PyObject* dummy, PyObject* args);
static PyObject* get_disable_fragments(PyObject* dummy, PyObject* args);
static PyObject* set_disable_fragments(PyObject* dummy, PyObject* args);
static PyObject* get_autoclose(PyObject* dummy, PyObject* args);
static PyObject* set_autoclose(PyObject* dummy, PyObject* args);
static PyObject* get_adaptation(PyObject* dummy, PyObject* args);
static PyObject* set_adaptation(PyObject* dummy, PyObject* args);
static PyObject* get_sndbuf(PyObject* dummy, PyObject* args);
static PyObject* set_sndbuf(PyObject* dummy, PyObject* args);
static PyObject* get_rcvbuf(PyObject* dummy, PyObject* args);
static PyObject* set_rcvbuf(PyObject* dummy, PyObject* args);
static PyObject* set_peer_primary(PyObject* dummy, PyObject* args);
static PyObject* set_primary(PyObject* dummy, PyObject* args);
static PyObject* bindx(PyObject* dummy, PyObject* args);
static PyObject* connectx(PyObject* dummy, PyObject* args);
static PyObject* getpaddrs(PyObject* dummy, PyObject* args);
static PyObject* getladdrs(PyObject* dummy, PyObject* args);
static PyObject* sctp_send_msg(PyObject* dummy, PyObject* args);
static PyObject* sctp_recv_msg(PyObject* dummy, PyObject* args);
static PyObject* _sockaddr_test(PyObject* dummy, PyObject* args);

static PyObject* get_status(PyObject* dummy, PyObject* args);
static PyObject* get_rtoinfo(PyObject* dummy, PyObject* args);
static PyObject* get_paddrinfo(PyObject* dummy, PyObject* args);
static PyObject* get_assocparams(PyObject* dummy, PyObject* args);
static PyObject* get_paddrparams(PyObject* dummy, PyObject* args);

static PyObject* set_rtoinfo(PyObject* dummy, PyObject* args);
static PyObject* set_assocparams(PyObject* dummy, PyObject* args);
static PyObject* set_paddrparams(PyObject* dummy, PyObject* args);

static int to_sockaddr(const char *caddr, int port, struct sockaddr* saddr, int* slen);
static int from_sockaddr(struct sockaddr* saddr, int* family, int* slen, int* port, char* caddr, int cnt);

static PyMethodDef _sctp_methods[] = 
{
    {"error_out", (PyCFunction)error_out, METH_NOARGS, NULL},
	{"getconstant", getconstant, METH_VARARGS, ""},
	{"have_sctp_multibuf", have_sctp_multibuf, METH_VARARGS, ""},
	{"have_sctp_noconnect", have_sctp_noconnect, METH_VARARGS, ""},
	{"have_sctp_sat_network", have_sctp_sat_network, METH_VARARGS, ""},
	{"have_sctp_setprimary", have_sctp_setprimary, METH_VARARGS, ""},
	{"have_sctp_prsctp", have_sctp_prsctp, METH_VARARGS, ""},
	{"have_sctp_addip", have_sctp_addip, METH_VARARGS, ""},
	{"bindx", bindx, METH_VARARGS, ""},
	{"connectx", connectx, METH_VARARGS, ""},
	{"getpaddrs", getpaddrs, METH_VARARGS, ""},
	{"getladdrs", getladdrs, METH_VARARGS, ""},
	{"peeloff", peeloff, METH_VARARGS, ""},
	{"sctp_send_msg", sctp_send_msg, METH_VARARGS, ""},
	{"sctp_recv_msg", sctp_recv_msg, METH_VARARGS, ""},
	{"set_peer_primary", set_peer_primary, METH_VARARGS, ""},
	{"set_primary", set_primary, METH_VARARGS, ""},
	{"get_autoclose", get_autoclose, METH_VARARGS, ""},
	{"set_autoclose", set_autoclose, METH_VARARGS, ""},
	{"get_initparams", get_initparams, METH_VARARGS, ""},
	{"set_initparams", set_initparams, METH_VARARGS, ""},
	{"get_nodelay", get_nodelay, METH_VARARGS, ""},
	{"set_nodelay", set_nodelay, METH_VARARGS, ""},
	{"get_adaptation", get_adaptation, METH_VARARGS, ""},
	{"set_adaptation", set_adaptation, METH_VARARGS, ""},
	{"get_sndbuf", get_sndbuf, METH_VARARGS, ""},
	{"set_sndbuf", set_sndbuf, METH_VARARGS, ""},
	{"get_rcvbuf", get_rcvbuf, METH_VARARGS, ""},
	{"set_rcvbuf", set_rcvbuf, METH_VARARGS, ""},
	{"get_disable_fragments", get_disable_fragments, METH_VARARGS, ""},
	{"set_disable_fragments", set_disable_fragments, METH_VARARGS, ""},
	{"get_events", get_events, METH_VARARGS, ""},
	{"set_events", set_events, METH_VARARGS, ""},
	{"get_mappedv4", get_mappedv4, METH_VARARGS, ""},
	{"set_mappedv4", set_mappedv4, METH_VARARGS, ""},
	{"get_maxseg", get_maxseg, METH_VARARGS, ""},
	{"set_maxseg", set_maxseg, METH_VARARGS, ""},
	{"_sockaddr_test", _sockaddr_test, METH_VARARGS, ""},
	{"get_status", get_status, METH_VARARGS, ""},
	{"get_rtoinfo", get_rtoinfo, METH_VARARGS, ""},
	{"get_paddrinfo", get_paddrinfo, METH_VARARGS, ""},
	{"get_assocparams", get_assocparams, METH_VARARGS, ""},
	{"get_paddrparams", get_paddrparams, METH_VARARGS, ""},
	{"set_rtoinfo", set_rtoinfo, METH_VARARGS, ""},
	{"set_assocparams", set_assocparams, METH_VARARGS, ""},
	{"set_paddrparams", set_paddrparams, METH_VARARGS, ""},
	{ NULL, NULL, 0, NULL }
};

// replacing oldschool CPython module init, with more recent routines
 
#if PY_MAJOR_VERSION >= 3
    
    #define Py23_PyLong_FromLong      PyLong_FromLong
    #define Py23_PyLong_Check         PyLong_Check
    #define Py23_PyLong_AsLong        PyLong_AsLong
    #define Py23_PyUnicode_FromFormat PyUnicode_FromFormat
    
    static int _sctp_traverse(PyObject *m, visitproc visit, void *arg) {
        Py_VISIT(GETSTATE(m)->error);
        return 0;
    }

    static int _sctp_clear(PyObject *m) {
        Py_CLEAR(GETSTATE(m)->error);
        return 0;
    }

    static struct PyModuleDef moduledef = {
            PyModuleDef_HEAD_INIT,
            "_sctp",
	        "SCTP protocol low-level bindings",
            sizeof(struct module_state),
            _sctp_methods,
            NULL,
            _sctp_traverse,
            _sctp_clear,
            NULL
    };

    #define INITERROR return NULL

    PyObject * PyInit__sctp(void)

#else

    #define INITERROR return
    // adding some conversion for the CPython 2 API
    #define Py23_PyLong_FromLong      PyInt_FromLong
    #define Py23_PyLong_Check         PyInt_Check
    #define Py23_PyLong_AsLong        PyInt_AsLong
    #define Py23_PyUnicode_FromFormat PyString_FromFormat
    
    void init_sctp(void)

#endif

{
#if PY_MAJOR_VERSION >= 3
    
    PyObject *module = PyModule_Create(&moduledef);
    
#else

    PyObject *module = Py_InitModule4(
        "_sctp",
        _sctp_methods,
        "SCTP protocol low-level bindings",
        0,
        PYTHON_API_VERSION);

#endif

    if (module == NULL)
        INITERROR;
    struct module_state *st = GETSTATE(module);

    st->error = PyErr_NewException("_sctp.Error", NULL, NULL);
    if (st->error == NULL) {
        Py_DECREF(module);
        INITERROR;
    }

#if PY_MAJOR_VERSION >= 3
    
        return module;
    
#endif
}

/*

static PyModuleDef sctpmodule = {
	PyModuleDef_HEAD_INIT,
	"_sctp",
	"SCTP protocol low-level bindings",
	-1,
	_sctpMethods,
	NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit__sctp(void)
{
	PyObject* m;
	m = PyModule_Create(&sctpmodule);
	if(m == NULL)
		return NULL;
	return m;
};

*/

typedef struct ktuple {
	char* key;
	int value;
} ktuple;

#ifndef MSG_UNORDERED
/* Newest version of SCTP Sockets changed macro names to SCTP_* to avoid MSG_* namespace pollution */
#define MSG_UNORDERED SCTP_UNORDERED
#define MSG_ADDR_OVER SCTP_ADDR_OVER
#define MSG_ABORT     SCTP_ABORT
#define MSG_EOF       SCTP_EOF
#endif

static ktuple _constants[] = 
{
	{"BINDX_ADD", SCTP_BINDX_ADD_ADDR},
	{"BINDX_REMOVE", SCTP_BINDX_REM_ADDR},
	{"SOL_SCTP", SOL_SCTP},
	{"IPPROTO_SCTP", IPPROTO_SCTP},
	{"SOCK_SEQPACKET", SOCK_SEQPACKET},
	{"SOCK_STREAM", SOCK_STREAM},
	{"MSG_UNORDERED", MSG_UNORDERED},
	{"MSG_ADDR_OVER", MSG_ADDR_OVER},
#ifdef SCTP_DRAFT10_LEVEL
	{"MSG_SENDALL", MSG_SENDALL},
#else
	{"MSG_SENDALL", 0},
#endif
	{"MSG_ABORT", MSG_ABORT},
	{"MSG_EOF", MSG_EOF},
	{"MSG_EOR", MSG_EOR},
	{"MSG_FIN", MSG_FIN},
	{"MSG_DONTROUTE", MSG_DONTROUTE},
	{"MSG_NOTIFICATION", MSG_NOTIFICATION},
	{"SCTP_COMM_UP", SCTP_COMM_UP},
	{"SCTP_COMM_LOST", SCTP_COMM_LOST},
	{"SCTP_DATA_UNSENT", SCTP_DATA_UNSENT},
	{"SCTP_PARTIAL_DELIVERY_ABORTED", SCTP_PARTIAL_DELIVERY_ABORTED},
#ifdef SCTP_DRAFT10_LEVEL
	{"SPP_HB_DISABLED", SPP_HB_DISABLED},
	{"SPP_HB_ENABLED", SPP_HB_ENABLED},
	{"SPP_PMTUD_DISABLED", SPP_PMTUD_DISABLED},
	{"SPP_PMTUD_ENABLED", SPP_PMTUD_ENABLED},
	{"SPP_SACKDELAY_DISABLED", SPP_SACKDELAY_DISABLED},
	{"SPP_SACKDELAY_ENABLED", SPP_SACKDELAY_ENABLED},
#else
	{"SPP_HB_DISABLED", 0},
	{"SPP_HB_ENABLED", 0},
	{"SPP_PMTUD_DISABLED", 0},
	{"SPP_PMTUD_ENABLED", 0},
	{"SPP_SACKDELAY_DISABLED", 0},
	{"SPP_SACKDELAY_ENABLED", 0},
#endif
#ifdef SCTP_DRAFT10_LEVEL
	{"SCTP_BOUND", SCTP_BOUND},
	{"SCTP_LISTEN", SCTP_LISTEN},
#else
	{"SCTP_BOUND", -1},
	{"SCTP_LISTEN", -1},
#endif
	{"SCTP_DATA_SENT", SCTP_DATA_SENT},
	{"SCTP_RESTART", SCTP_RESTART},
	{"SCTP_SHUTDOWN_COMP", SCTP_SHUTDOWN_COMP},
	{"SCTP_CANT_STR_ASSOC", SCTP_CANT_STR_ASSOC},
	{"SCTP_FAILED_THRESHOLD", SCTP_FAILED_THRESHOLD},
	{"SCTP_RECEIVED_SACK", SCTP_RECEIVED_SACK},
	{"SCTP_HEARTBEAT_SUCCESS", SCTP_HEARTBEAT_SUCCESS},
	{"SCTP_RESPONSE_TO_USER_REQ", SCTP_RESPONSE_TO_USER_REQ},
	{"SCTP_INTERNAL_ERROR", SCTP_INTERNAL_ERROR},
	{"SCTP_SHUTDOWN_GUARD_EXPIRES", SCTP_SHUTDOWN_GUARD_EXPIRES},
	{"SCTP_PEER_FAULTY", SCTP_PEER_FAULTY},
	{"SCTP_ADDR_AVAILABLE", SCTP_ADDR_AVAILABLE},
	{"SCTP_ADDR_UNREACHABLE", SCTP_ADDR_UNREACHABLE},
	{"SCTP_ADDR_REMOVED", SCTP_ADDR_REMOVED},
	{"SCTP_ADDR_MADE_PRIM", SCTP_ADDR_MADE_PRIM},
	{"SCTP_ADDR_ADDED", SCTP_ADDR_ADDED},
	{"SCTP_INACTIVE", SCTP_INACTIVE},
	{"SCTP_ACTIVE", SCTP_ACTIVE},
	{"SCTP_EMPTY", SCTP_EMPTY},
	{"SCTP_CLOSED", SCTP_CLOSED},
	{"SCTP_COOKIE_WAIT", SCTP_COOKIE_WAIT},
	{"SCTP_COOKIE_ECHOED", SCTP_COOKIE_ECHOED},
	{"SCTP_ESTABLISHED", SCTP_ESTABLISHED},
	{"SCTP_SHUTDOWN_PENDING", SCTP_SHUTDOWN_PENDING},
	{"SCTP_SHUTDOWN_SENT", SCTP_SHUTDOWN_SENT},
	{"SCTP_SHUTDOWN_RECEIVED", SCTP_SHUTDOWN_RECEIVED},
	{"SCTP_SHUTDOWN_ACK_SENT", SCTP_SHUTDOWN_ACK_SENT},
	{"SCTP_SN_TYPE_BASE", SCTP_SN_TYPE_BASE},
	{"SCTP_ASSOC_CHANGE", SCTP_ASSOC_CHANGE},
	{"SCTP_PEER_ADDR_CHANGE", SCTP_PEER_ADDR_CHANGE},
	{"SCTP_SEND_FAILED", SCTP_SEND_FAILED},
	{"SCTP_REMOTE_ERROR", SCTP_REMOTE_ERROR},
	{"SCTP_SHUTDOWN_EVENT", SCTP_SHUTDOWN_EVENT},
	{"SCTP_PARTIAL_DELIVERY_EVENT", SCTP_PARTIAL_DELIVERY_EVENT}, 
	{"SCTP_ADAPTATION_INDICATION", SCTP_ADAPTATION_INDICATION},
	{0, -1}
};

static PyObject* getconstant(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	
	char* needle;
	const struct ktuple* haystack;
	
	if (PyArg_ParseTuple(args, "s", &needle)) {
		for(haystack = &(_constants[0]); haystack->key; ++haystack) {
			if (strcmp(haystack->key, needle) == 0) {
				ret = Py23_PyLong_FromLong(haystack->value);
				break;
			}
		}
	}

	return ret;
}

static PyObject* have_sctp_sat_network(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_SAT_NETWORK_CAPABILITY
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* have_sctp_setprimary(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_CANSET_PRIMARY
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* have_sctp_addip(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_ADDIP
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* have_sctp_prsctp(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_PRSCTP
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* have_sctp_multibuf(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_MULTIBUF
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* have_sctp_noconnect(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
#ifdef HAVE_SCTP_NOCONNECT
	ret = Py_True; Py_INCREF(ret);
#else
	ret = Py_False; Py_INCREF(ret);
#endif
	return ret;
}

static PyObject* get_mappedv4(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);
	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_I_WANT_MAPPED_V4_ADDR, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = PyBool_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_mappedv4(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_I_WANT_MAPPED_V4_ADDR, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_nodelay(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);
	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_NODELAY, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = PyBool_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_nodelay(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_NODELAY, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_assocparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	int fd;
	struct sctp_assocparams v;
	socklen_t lv = sizeof(v);
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && Py23_PyLong_Check(oassoc_id);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.sasoc_assoc_id = Py23_PyLong_AsLong(oassoc_id);

	if (getsockopt(fd, SOL_SCTP, SCTP_ASSOCINFO, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "assocmaxrxt", Py23_PyLong_FromLong(v.sasoc_asocmaxrxt));
		PyDict_SetItemString(dict, "number_peer_destinations", Py23_PyLong_FromLong(v.sasoc_number_peer_destinations));
		PyDict_SetItemString(dict, "peer_rwnd", Py23_PyLong_FromLong(v.sasoc_peer_rwnd));
		PyDict_SetItemString(dict, "local_rwnd", Py23_PyLong_FromLong(v.sasoc_local_rwnd));
		PyDict_SetItemString(dict, "cookie_life", Py23_PyLong_FromLong(v.sasoc_cookie_life));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* set_assocparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	PyObject* oassocmaxrxt;
	PyObject* onumber_peer_destinations;
	PyObject* opeer_rwnd;
	PyObject* olocal_rwnd;
	PyObject* ocookie_life;

	int fd;
	struct sctp_assocparams v;
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && (oassocmaxrxt = PyDict_GetItemString(dict, "assocmaxrxt"));
	ok = ok && (onumber_peer_destinations = PyDict_GetItemString(dict, "number_peer_destinations"));
	ok = ok && (opeer_rwnd = PyDict_GetItemString(dict, "peer_rwnd"));
	ok = ok && (olocal_rwnd = PyDict_GetItemString(dict, "local_rwnd"));
	ok = ok && (ocookie_life = PyDict_GetItemString(dict, "cookie_life"));
	ok = ok && Py23_PyLong_Check(oassoc_id);
	ok = ok && Py23_PyLong_Check(oassocmaxrxt);
	ok = ok && Py23_PyLong_Check(onumber_peer_destinations);
	ok = ok && Py23_PyLong_Check(opeer_rwnd);
	ok = ok && Py23_PyLong_Check(olocal_rwnd);
	ok = ok && Py23_PyLong_Check(ocookie_life);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.sasoc_assoc_id = Py23_PyLong_AsLong(oassoc_id);
	v.sasoc_asocmaxrxt = Py23_PyLong_AsLong(oassocmaxrxt);
	v.sasoc_number_peer_destinations = Py23_PyLong_AsLong(onumber_peer_destinations);
	v.sasoc_peer_rwnd = Py23_PyLong_AsLong(opeer_rwnd);
	v.sasoc_local_rwnd = Py23_PyLong_AsLong(olocal_rwnd);
	v.sasoc_cookie_life = Py23_PyLong_AsLong(ocookie_life);

	if (setsockopt(fd, SOL_SCTP, SCTP_ASSOCINFO, &v, sizeof(v))) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "assocmaxrxt", Py23_PyLong_FromLong(v.sasoc_asocmaxrxt));
		PyDict_SetItemString(dict, "number_peer_destinations", Py23_PyLong_FromLong(v.sasoc_number_peer_destinations));
		PyDict_SetItemString(dict, "peer_rwnd", Py23_PyLong_FromLong(v.sasoc_peer_rwnd));
		PyDict_SetItemString(dict, "local_rwnd", Py23_PyLong_FromLong(v.sasoc_local_rwnd));
		PyDict_SetItemString(dict, "cookie_life", Py23_PyLong_FromLong(v.sasoc_cookie_life));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* get_paddrparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	PyObject *oaddresstuple;

	const char* address;
	int port;
	int fd;
	int slen_dummy;

	struct sctp_paddrparams v;
	socklen_t lv = sizeof(v);
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && (oaddresstuple = PyDict_GetItemString(dict, "sockaddr"));
	ok = ok && PyArg_ParseTuple(oaddresstuple, "si", &address, &port);
	ok = ok && Py23_PyLong_Check(oassoc_id);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.spp_assoc_id = Py23_PyLong_AsLong(oassoc_id);

	if (! to_sockaddr(address, port, (struct sockaddr*) &(v.spp_address), &slen_dummy)) {
		PyErr_SetString(PyExc_ValueError, "address could not be translated");
		return ret;
	}

	if (getsockopt(fd, SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "hbinterval", Py23_PyLong_FromLong(v.spp_hbinterval));
		PyDict_SetItemString(dict, "pathmaxrxt", Py23_PyLong_FromLong(v.spp_pathmaxrxt));
#ifdef SCTP_DRAFT10_LEVEL
		PyDict_SetItemString(dict, "pathmtu", Py23_PyLong_FromLong(v.spp_pathmtu));
		PyDict_SetItemString(dict, "sackdelay", Py23_PyLong_FromLong(v.spp_sackdelay));
		PyDict_SetItemString(dict, "flags", Py23_PyLong_FromLong(v.spp_flags));
#endif
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* set_paddrparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	PyObject *oaddresstuple;
	PyObject* ohbinterval;
	PyObject* opathmaxrxt;
	PyObject* opathmtu;
	PyObject* osackdelay;
	PyObject* oflags;

	const char* address;
	int port;

	int fd;
	struct sctp_paddrparams v;
	int ok;
	int slen_dummy;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);

	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && (oaddresstuple = PyDict_GetItemString(dict, "sockaddr"));
	ok = ok && (ohbinterval = PyDict_GetItemString(dict, "hbinterval"));
	ok = ok && (opathmaxrxt = PyDict_GetItemString(dict, "pathmaxrxt"));
	ok = ok && (opathmtu = PyDict_GetItemString(dict, "pathmtu"));
	ok = ok && (osackdelay = PyDict_GetItemString(dict, "sackdelay"));
	ok = ok && (oflags = PyDict_GetItemString(dict, "flags"));

	ok = ok && PyArg_ParseTuple(oaddresstuple, "si", &address, &port);

	ok = ok && Py23_PyLong_Check(oassoc_id);
	ok = ok && Py23_PyLong_Check(ohbinterval);
	ok = ok && Py23_PyLong_Check(opathmaxrxt);
	ok = ok && Py23_PyLong_Check(opathmtu);
	ok = ok && Py23_PyLong_Check(osackdelay);
	ok = ok && Py23_PyLong_Check(oflags);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.spp_assoc_id = Py23_PyLong_AsLong(oassoc_id);
	v.spp_hbinterval = Py23_PyLong_AsLong(ohbinterval);
	v.spp_pathmaxrxt = Py23_PyLong_AsLong(opathmaxrxt);
#ifdef SCTP_DRAFT10_LEVEL
	v.spp_pathmtu = Py23_PyLong_AsLong(opathmtu);
	v.spp_sackdelay = Py23_PyLong_AsLong(osackdelay);
	v.spp_flags = Py23_PyLong_AsLong(oflags);
#endif

	if (! to_sockaddr(address, port, (struct sockaddr*) &(v.spp_address), &slen_dummy)) {
		PyErr_SetString(PyExc_ValueError, "address could not be translated");
		return ret;
	}

	if (setsockopt(fd, SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &v, sizeof(v))) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "hbinterval", Py23_PyLong_FromLong(v.spp_hbinterval));
		PyDict_SetItemString(dict, "pathmaxrxt", Py23_PyLong_FromLong(v.spp_pathmaxrxt));
#ifdef SCTP_DRAFT10_LEVEL
		PyDict_SetItemString(dict, "pathmtu", Py23_PyLong_FromLong(v.spp_pathmtu));
		PyDict_SetItemString(dict, "sackdelay", Py23_PyLong_FromLong(v.spp_sackdelay));
		PyDict_SetItemString(dict, "flags", Py23_PyLong_FromLong(v.spp_flags));
#endif
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* get_status(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* dict2;
	PyObject* oassoc_id;

	PyObject* oaddr;
	char caddr[256];
	int family, len, port;

	int fd;
	struct sctp_status v;
	socklen_t lv = sizeof(v);
	int ok;
	
	ok = PyArg_ParseTuple(args, "iOO", &fd, &dict, &dict2) && \
						PyDict_Check(dict) && PyDict_Check(dict2);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && Py23_PyLong_Check(oassoc_id);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.sstat_assoc_id = Py23_PyLong_AsLong(oassoc_id);

	if (getsockopt(fd, SOL_SCTP, SCTP_STATUS, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "state", Py23_PyLong_FromLong(v.sstat_state));
		PyDict_SetItemString(dict, "rwnd", Py23_PyLong_FromLong(v.sstat_rwnd));
		PyDict_SetItemString(dict, "unackdata", Py23_PyLong_FromLong(v.sstat_unackdata));
		PyDict_SetItemString(dict, "penddata", Py23_PyLong_FromLong(v.sstat_penddata));
		PyDict_SetItemString(dict, "instrms", Py23_PyLong_FromLong(v.sstat_instrms));
		PyDict_SetItemString(dict, "outstrms", Py23_PyLong_FromLong(v.sstat_outstrms));
		PyDict_SetItemString(dict, "fragmentation_point", Py23_PyLong_FromLong(v.sstat_fragmentation_point));

		if (from_sockaddr((struct sockaddr*) &(v.sstat_primary.spinfo_address), &family, 
					&len, &port, caddr, sizeof(caddr))) {
			oaddr = PyTuple_New(2);
			PyTuple_SetItem(oaddr, 0, PyUnicode_FromString(caddr));
			PyTuple_SetItem(oaddr, 1, Py23_PyLong_FromLong(port));
		} else {
			// something went wrong
			oaddr = Py_None;
			Py_INCREF(Py_None);
		}
		
		PyDict_SetItemString(dict2, "sockaddr", oaddr);
		PyDict_SetItemString(dict2, "assoc_id", Py23_PyLong_FromLong(v.sstat_primary.spinfo_assoc_id));
		PyDict_SetItemString(dict2, "state", Py23_PyLong_FromLong(v.sstat_primary.spinfo_state));
		PyDict_SetItemString(dict2, "cwnd", Py23_PyLong_FromLong(v.sstat_primary.spinfo_cwnd));
		PyDict_SetItemString(dict2, "srtt", Py23_PyLong_FromLong(v.sstat_primary.spinfo_srtt));
		PyDict_SetItemString(dict2, "rto", Py23_PyLong_FromLong(v.sstat_primary.spinfo_rto));
		PyDict_SetItemString(dict2, "mtu", Py23_PyLong_FromLong(v.sstat_primary.spinfo_mtu));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* get_paddrinfo(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	PyObject* oaddresstuple;
	const char* address;
	int port;
	int fd;
	int slen_dummy;
	struct sctp_paddrinfo v;
	socklen_t lv = sizeof(v);
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && (oaddresstuple = PyDict_GetItemString(dict, "sockaddr"));
	ok = ok && Py23_PyLong_Check(oassoc_id);
	ok = ok && PyArg_ParseTuple(oaddresstuple, "si", &address, &port);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.spinfo_assoc_id = Py23_PyLong_AsLong(oassoc_id);
	if (! to_sockaddr(address, port, (struct sockaddr*) &(v.spinfo_address), &slen_dummy)) {
		PyErr_SetString(PyExc_ValueError, "address could not be translated");
		return ret;
	}

	if (getsockopt(fd, SOL_SCTP, SCTP_GET_PEER_ADDR_INFO, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "state", Py23_PyLong_FromLong(v.spinfo_state));
		PyDict_SetItemString(dict, "cwnd", Py23_PyLong_FromLong(v.spinfo_cwnd));
		PyDict_SetItemString(dict, "srtt", Py23_PyLong_FromLong(v.spinfo_srtt));
		PyDict_SetItemString(dict, "rto", Py23_PyLong_FromLong(v.spinfo_rto));
		PyDict_SetItemString(dict, "mtu", Py23_PyLong_FromLong(v.spinfo_mtu));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* get_rtoinfo(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	int fd;
	struct sctp_rtoinfo v;
	socklen_t lv = sizeof(v);
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && Py23_PyLong_Check(oassoc_id);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.srto_assoc_id = Py23_PyLong_AsLong(oassoc_id);

	if (getsockopt(fd, SOL_SCTP, SCTP_RTOINFO, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "initial", Py23_PyLong_FromLong(v.srto_initial));
		PyDict_SetItemString(dict, "max", Py23_PyLong_FromLong(v.srto_max));
		PyDict_SetItemString(dict, "min", Py23_PyLong_FromLong(v.srto_min));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* set_rtoinfo(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	PyObject* oassoc_id;
	PyObject* oinitial;
	PyObject* omin;
	PyObject* omax;

	int fd;
	struct sctp_rtoinfo v;
	int ok;
	
	ok = PyArg_ParseTuple(args, "iO", &fd, &dict) && PyDict_Check(dict);
	ok = ok && (oassoc_id = PyDict_GetItemString(dict, "assoc_id"));
	ok = ok && (oinitial = PyDict_GetItemString(dict, "initial"));
	ok = ok && (omin = PyDict_GetItemString(dict, "min"));
	ok = ok && (omax = PyDict_GetItemString(dict, "max"));
	ok = ok && Py23_PyLong_Check(oassoc_id);
	ok = ok && Py23_PyLong_Check(oinitial);
	ok = ok && Py23_PyLong_Check(omin);
	ok = ok && Py23_PyLong_Check(omax);

	if (! ok) {
		return ret;
	}

	bzero(&v, sizeof(v));
	v.srto_assoc_id = Py23_PyLong_AsLong(oassoc_id);
	v.srto_initial = Py23_PyLong_AsLong(oinitial);
	v.srto_min = Py23_PyLong_AsLong(omin);
	v.srto_max = Py23_PyLong_AsLong(omax);

	if (setsockopt(fd, SOL_SCTP, SCTP_RTOINFO, &v, sizeof(v))) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyDict_SetItemString(dict, "initial", Py23_PyLong_FromLong(v.srto_initial));
		PyDict_SetItemString(dict, "max", Py23_PyLong_FromLong(v.srto_max));
		PyDict_SetItemString(dict, "min", Py23_PyLong_FromLong(v.srto_min));
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* get_initparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	struct sctp_initmsg v;
	socklen_t lv = sizeof(v);

	if (! PyArg_ParseTuple(args, "i", &fd)) {
		return ret;
	}

	if (getsockopt(fd, SOL_SCTP, SCTP_INITMSG, &v, &lv)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		ret = PyDict_New();
		PyDict_SetItemString(ret, "_num_ostreams", Py23_PyLong_FromLong(v.sinit_num_ostreams));
		PyDict_SetItemString(ret, "_max_instreams", Py23_PyLong_FromLong(v.sinit_max_instreams));
		PyDict_SetItemString(ret, "_max_attempts", Py23_PyLong_FromLong(v.sinit_max_attempts));
		PyDict_SetItemString(ret, "_max_init_timeo", Py23_PyLong_FromLong(v.sinit_max_attempts));
	}

	return ret;
}

static PyObject* set_initparams(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	PyObject *ov, *o_num_ostreams, *o_max_instreams, *o_max_attempts, *o_max_init_timeo;
	struct sctp_initmsg v;

	int ok = PyArg_ParseTuple(args, "iO", &fd, &ov) && PyDict_Check(ov);

	ok = ok && (o_num_ostreams = PyDict_GetItemString(ov, "_num_ostreams"));
	ok = ok && (o_max_instreams = PyDict_GetItemString(ov, "_max_instreams"));
	ok = ok && (o_max_attempts = PyDict_GetItemString(ov, "_max_attempts"));
	ok = ok && (o_max_init_timeo = PyDict_GetItemString(ov, "_max_init_timeo"));

	ok = ok && (Py23_PyLong_Check(o_num_ostreams) != 0);
	ok = ok && (Py23_PyLong_Check(o_max_instreams) != 0);
	ok = ok && (Py23_PyLong_Check(o_max_attempts) != 0);
	ok = ok && (Py23_PyLong_Check(o_max_init_timeo) != 0);

	if (ok) {
		memset(&v, 0, sizeof(v));
		v.sinit_num_ostreams = Py23_PyLong_AsLong(o_num_ostreams);
		v.sinit_max_instreams = Py23_PyLong_AsLong(o_max_instreams);
		v.sinit_max_attempts = Py23_PyLong_AsLong(o_max_attempts);
		v.sinit_max_init_timeo = Py23_PyLong_AsLong(o_max_init_timeo);
		
		if (setsockopt(fd, SOL_SCTP, SCTP_INITMSG, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* peeloff(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int v1, v2, fd;
	
	if (PyArg_ParseTuple(args, "ii", &v1, &v2)) {
		fd = sctp_peeloff(v1, v2);
		if (fd < 0) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(fd);
		}
	}

	return ret;
}

static PyObject* get_events(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	struct sctp_event_subscribe v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_EVENTS, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = PyDict_New();
			PyDict_SetItemString(ret, "_data_io", PyBool_FromLong(v.sctp_data_io_event));
			PyDict_SetItemString(ret, "_association", PyBool_FromLong(v.sctp_association_event));
			PyDict_SetItemString(ret, "_address", PyBool_FromLong(v.sctp_address_event));
			PyDict_SetItemString(ret, "_send_failure", PyBool_FromLong(v.sctp_send_failure_event));
			PyDict_SetItemString(ret, "_peer_error", PyBool_FromLong(v.sctp_peer_error_event));
			PyDict_SetItemString(ret, "_shutdown", PyBool_FromLong(v.sctp_shutdown_event));
			PyDict_SetItemString(ret, "_partial_delivery", PyBool_FromLong(v.sctp_partial_delivery_event));
			PyDict_SetItemString(ret, "_adaptation_layer", PyBool_FromLong(v.sctp_adaptation_layer_event));
		}
	}
	return ret;
}

static PyObject* set_events(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	PyObject *ov, *o_data_io, *o_association, *o_address, *o_send_failure;
	PyObject *o_peer_error, *o_shutdown, *o_partial_delivery, *o_adaptation_layer;
	struct sctp_event_subscribe v;
	int ok = PyArg_ParseTuple(args, "iO", &fd, &ov) && PyDict_Check(ov);

	ok = ok && (o_data_io = PyDict_GetItemString(ov, "_data_io"));
	ok = ok && (o_association = PyDict_GetItemString(ov, "_association"));
	ok = ok && (o_address = PyDict_GetItemString(ov, "_address"));
	ok = ok && (o_send_failure = PyDict_GetItemString(ov, "_send_failure"));
	ok = ok && (o_peer_error = PyDict_GetItemString(ov, "_peer_error"));
	ok = ok && (o_shutdown = PyDict_GetItemString(ov, "_shutdown"));
	ok = ok && (o_partial_delivery = PyDict_GetItemString(ov, "_partial_delivery"));
	ok = ok && (o_adaptation_layer = PyDict_GetItemString(ov, "_adaptation_layer"));

	ok = ok && (Py23_PyLong_Check(o_data_io) != 0);
	ok = ok && (Py23_PyLong_Check(o_association) != 0);
	ok = ok && (Py23_PyLong_Check(o_address) != 0);
	ok = ok && (Py23_PyLong_Check(o_send_failure) != 0);
	ok = ok && (Py23_PyLong_Check(o_peer_error) != 0);
	ok = ok && (Py23_PyLong_Check(o_shutdown) != 0);
	ok = ok && (Py23_PyLong_Check(o_send_failure) != 0);
	ok = ok && (Py23_PyLong_Check(o_adaptation_layer) != 0);

	if (ok) {
		memset(&v, 0, sizeof(v));
		v.sctp_data_io_event = Py23_PyLong_AsLong(o_data_io);
		v.sctp_association_event = Py23_PyLong_AsLong(o_association);
		v.sctp_address_event = Py23_PyLong_AsLong(o_address);
		v.sctp_send_failure_event = Py23_PyLong_AsLong(o_send_failure);
		v.sctp_peer_error_event = Py23_PyLong_AsLong(o_peer_error);
		v.sctp_shutdown_event = Py23_PyLong_AsLong(o_shutdown);
		v.sctp_partial_delivery_event = Py23_PyLong_AsLong(o_partial_delivery);
		v.sctp_adaptation_layer_event = Py23_PyLong_AsLong(o_adaptation_layer);
		
		if (setsockopt(fd, SOL_SCTP, SCTP_EVENTS, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_maxseg(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_MAXSEG, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_maxseg(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_MAXSEG, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_disable_fragments(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_DISABLE_FRAGMENTS, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = PyBool_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_disable_fragments(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_DISABLE_FRAGMENTS, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_autoclose(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_AUTOCLOSE, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_autoclose(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_AUTOCLOSE, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_adaptation(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);
	
	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SCTP, SCTP_ADAPTATION_LAYER, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_adaptation(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SCTP, SCTP_ADAPTATION_LAYER, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_sndbuf(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_sndbuf(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static PyObject* get_rcvbuf(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;
	socklen_t lv = sizeof(v);

	if (PyArg_ParseTuple(args, "i", &fd)) {
		if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &v, &lv)) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py23_PyLong_FromLong(v);
		}
	}
	return ret;
}

static PyObject* set_rcvbuf(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd, v;

	if (PyArg_ParseTuple(args, "ii", &fd, &v)) {
		if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &v, sizeof(v))) {
			PyErr_SetFromErrno(PyExc_IOError);
		} else {
			ret = Py_None; Py_INCREF(ret);
		}
	}
	return ret;
}

static int to_sockaddr(const char *caddr, int port, struct sockaddr* saddr, int* slen)
{
	int ret = 1;

	*slen = 0;

#ifdef DEBUG
	printf("[DEBUG to_sockaddr] converting caddr: %s, port: %d\n", caddr, port);
#endif

	if (strcmp(caddr, "") == 0) {
		saddr->sa_family = AF_INET;
		((struct sockaddr_in*) saddr)->sin_addr.s_addr = INADDR_ANY;
	} else if (strcmp(caddr, "<broadcast>") == 0) {
		saddr->sa_family = AF_INET;
		((struct sockaddr_in*) saddr)->sin_addr.s_addr = INADDR_BROADCAST;
	} else if (inet_pton(AF_INET6, caddr, &(((struct sockaddr_in6*)saddr)->sin6_addr)) > 0) {
		saddr->sa_family = AF_INET6;
	} else if (inet_pton(AF_INET, caddr, &(((struct sockaddr_in*)saddr)->sin_addr)) > 0) {
		saddr->sa_family = AF_INET;
	} else {
		saddr->sa_family = -1;
		ret = 0;
	}

	if (saddr->sa_family == AF_INET) {
		((struct sockaddr_in*) saddr)->sin_port = htons(port);
		*slen = sizeof(struct sockaddr_in);
	} else if (saddr->sa_family == AF_INET6) {
		((struct sockaddr_in6*) saddr)->sin6_port = htons(port);
		*slen = sizeof(struct sockaddr_in6);
	}

#ifdef DEBUG
	printf("[DEBUG to_sockaddr] sockaddr result is family: 0x%x, s_addr: 0x%x, port: 0x%x\n", 
			((struct sockaddr_in*)saddr)->sin_family,
			((struct sockaddr_in*)saddr)->sin_addr.s_addr,
			((struct sockaddr_in*)saddr)->sin_port);
#endif
	return ret;
}

static int from_sockaddr(struct sockaddr* saddr, int* family, int* slen, int* port, char* caddr, int cnt)
{
	int ret = 0;
	*family = saddr->sa_family;
	if (*family == AF_INET) {
		*slen = sizeof(struct sockaddr_in);
		if (inet_ntop(AF_INET, &(((struct sockaddr_in*) saddr)->sin_addr), caddr, cnt)) {
			*port = ntohs(((struct sockaddr_in*) saddr)->sin_port);
			ret = 1;
		}
	} else if (*family == AF_INET6) {
		*slen = sizeof(struct sockaddr_in6);
		if (inet_ntop(AF_INET6, &(((struct sockaddr_in6*) saddr)->sin6_addr), caddr, cnt)) {
			*port = ntohs(((struct sockaddr_in6*) saddr)->sin6_port);
			ret = 1;
		}
	}
	return ret;
}

static PyObject* _sockaddr_test(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* addrtupleret = 0;
	char *caddr;
	int port;
	struct sockaddr_storage saddr;
	int slen;
	int family;
	char caddr2[256];

	if (! PyArg_ParseTuple(args, "(si)", &caddr, &port)) {
		return ret;
	}

	printf("DEBUG: addr=%s, port=%d\n", caddr, port);

	if (! to_sockaddr(caddr, port, (struct sockaddr*) &saddr, &slen)) {
		PyErr_SetString(PyExc_ValueError, "address could not be translated");
		return ret;
	}
	port = 0;
	caddr = 0;

	if (! from_sockaddr((struct sockaddr*) &saddr, &family, &slen, &port, caddr2, sizeof(caddr2))) {
		PyErr_SetString(PyExc_ValueError, "address could not be de-translated");
		return ret;
	}

	ret = PyTuple_New(4);
	addrtupleret = PyTuple_New(2);
	PyTuple_SetItem(ret, 0, Py23_PyUnicode_FromFormat("family %d, size %d, address %s.%d", family, slen, caddr2, port));
	PyTuple_SetItem(ret, 1, Py23_PyLong_FromLong(family));
	PyTuple_SetItem(ret, 2, Py23_PyLong_FromLong(slen));
	PyTuple_SetItem(ret, 3, addrtupleret);
	PyTuple_SetItem(addrtupleret, 0, PyUnicode_FromString(caddr2));
	PyTuple_SetItem(addrtupleret, 1, Py23_PyLong_FromLong(port));

	return ret;
}

static PyObject* set_peer_primary(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	int l;
	int assoc_id;
	char* addr;
	int port;
	struct sctp_setpeerprim ssp;

	if (! PyArg_ParseTuple(args, "ii(si)", &fd, &assoc_id, &addr, &port)) {
		return ret;
	}

	bzero(&ssp, sizeof(ssp));
	ssp.sspp_assoc_id = assoc_id;
	if (! to_sockaddr(addr, port, (struct sockaddr*) &(ssp.sspp_addr), &l)) {
		PyErr_SetString(PyExc_ValueError, "Invalid address");
		return ret;
	}

	if (setsockopt(fd, SOL_SCTP, SCTP_SET_PEER_PRIMARY_ADDR, &ssp, sizeof(ssp))) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* set_primary(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	int l;
	int assoc_id;
	char* addr;
	struct sctp_prim ssp;
	int port;

	if (! PyArg_ParseTuple(args, "ii(si)", &fd, &assoc_id, &addr, &port)) {
		return ret;
	}

	bzero(&ssp, sizeof(ssp));
	ssp.ssp_assoc_id = assoc_id;
	if (! to_sockaddr(addr, port, (struct sockaddr*) &(ssp.ssp_addr), &l)) {
		PyErr_SetString(PyExc_ValueError, "Invalid address");
		return ret;
	}

	if (setsockopt(fd, SOL_SCTP, SCTP_PRIMARY_ADDR, &ssp, sizeof(ssp))) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		ret = Py_None; Py_INCREF(ret);
	}

	return ret;
}

static PyObject* bindx(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	PyObject* addrs;
	struct sockaddr saddr;
	struct sockaddr* saddrs;
	int saddr_len, saddrs_len;
	int addrcount;
	int flags;
	int x;

	if (! PyArg_ParseTuple(args, "iOi", &fd, &addrs, &flags)) {
		return ret;
	}

	if (! PySequence_Check(addrs)) {
		PyErr_SetString(PyExc_ValueError, "Second parameter must be a sequence of address/port tuples");
		return ret;
	}

	addrcount = PySequence_Length(addrs);
	if (addrcount <= 0) {
		PyErr_SetString(PyExc_ValueError, "Second parameter must be a non-empty sequence");
		return ret;
	}

	saddrs_len = 0;
	saddrs = (struct sockaddr*) malloc(saddrs_len);

	for(x = 0; x < addrcount; ++x) {
		const char* caddr;
		int iport;

		PyObject* otuple = PySequence_GetItem(addrs, x);

		if (! PyArg_ParseTuple(otuple, "si", &caddr, &iport)) {
			free(saddrs);
			return ret;
		}
		
		if (! to_sockaddr(caddr, iport, &saddr, &saddr_len)) {
			PyErr_Format(PyExc_ValueError, "Invalid address: %s", caddr);
			free(saddrs);
			return ret;
		}

		if (saddr_len == 0) {
			PyErr_Format(PyExc_ValueError, "Invalid address family: %s", caddr);
			free(saddrs);
			return ret;
		}

		saddrs = realloc(saddrs, saddrs_len + saddr_len);
		memcpy( ((char*) saddrs) + saddrs_len, &saddr, saddr_len);
		saddrs_len += saddr_len;
		
#ifdef DEBUG
        printf("[DEBUG bindx] x: %d, caddr: %s, iport: %d, saddrs_len: %d\n",
               x, caddr, iport, saddrs_len);
#endif

	}

	if (sctp_bindx(fd, saddrs, addrcount, flags)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		ret = Py_None; Py_INCREF(ret);
	}

	free(saddrs);
	return ret;
}

static PyObject* connectx(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	PyObject* dict;
	int fd;
	PyObject* addrs;
	struct sockaddr saddr;
	struct sockaddr* saddrs;
	sctp_assoc_t id;
	int saddr_len, saddrs_len;
	int addrcount;
	int x;

	if (! PyArg_ParseTuple(args, "iOO", &fd, &addrs, &dict)) {
		return ret;
	}

	if (! PySequence_Check(addrs)) {
		PyErr_SetString(PyExc_ValueError, "Second parameter must be a sequence of address/port tuples");
		return ret;
	}

	addrcount = PySequence_Length(addrs);
	if (addrcount <= 0) {
		PyErr_SetString(PyExc_ValueError, "Second parameter must be a non-empty sequence");
		return ret;
	}

	saddrs_len = 0;
	saddrs = (struct sockaddr*) malloc(saddrs_len);

	for(x = 0; x < addrcount; ++x) {
		const char* caddr;
		int iport;

		PyObject* otuple = PySequence_GetItem(addrs, x);

		if (! PyArg_ParseTuple(otuple, "si", &caddr, &iport)) {
			free(saddrs);
			return ret;
		}
		
		if (! to_sockaddr(caddr, iport, &saddr, &saddr_len)) {
			PyErr_Format(PyExc_ValueError, "Invalid address: %s", caddr);
			free(saddrs);
			return ret;
		}

		if (saddr_len == 0) {
			PyErr_Format(PyExc_ValueError, "Invalid address family: %s", caddr);
			free(saddrs);
			return ret;
		}

		saddrs = realloc(saddrs, saddrs_len + saddr_len);
		memcpy( ((char*) saddrs) + saddrs_len, &saddr, saddr_len);
		saddrs_len += saddr_len;
	}

	if (sctp_connectx(fd, saddrs, addrcount, &id)) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		if(PyDict_Check(dict)) PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(id));
		ret = Py_None; Py_INCREF(ret);
	}

	free(saddrs);
	return ret;
}

static PyObject* getpaddrs(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	int assoc_id;
	struct sockaddr* saddrs;
	int count;
	int x;
	char addr[256];

	if (! PyArg_ParseTuple(args, "ii", &fd, &assoc_id)) {
		return ret;
	}

	count = sctp_getpaddrs(fd, assoc_id, &saddrs);

	if (count < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		if (count == 0) {
			saddrs = 0;
		}
		
		ret = PyTuple_New(count);
		char *p = (char*) saddrs;

		for(x = 0; x < count; ++x) {
			int len;
			int family;
			int port;
			PyObject* oaddr;

			if (from_sockaddr((struct sockaddr*) p, &family, &len, &port, 
										addr, sizeof(addr))) {
				oaddr = PyTuple_New(2);
				PyTuple_SetItem(oaddr, 0, PyUnicode_FromString(addr));
				PyTuple_SetItem(oaddr, 1, Py23_PyLong_FromLong(port));
				PyTuple_SetItem(ret, x, oaddr);
			} else {
				// something's wrong; not safe to continue
				break;
			}
			
			p += len;
		}

		sctp_freepaddrs(saddrs);

		// If something went wrong, the remaining expected addresses will be set to None.
		for(; x < count; ++x) {
			PyTuple_SetItem(ret, x, Py_None);
			Py_INCREF(Py_None);

		}
	}

	return ret;
}

static PyObject* getladdrs(PyObject* dummy, PyObject* args)
{
	PyObject* ret = 0;
	int fd;
	int assoc_id;
	struct sockaddr* saddrs;
	int count;
	int x;
	char addr[256];

	if (! PyArg_ParseTuple(args, "ii", &fd, &assoc_id)) {
		return ret;
	}

	count = sctp_getladdrs(fd, assoc_id, &saddrs);

	if (count < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
	} else {
		if (count == 0) {
			saddrs = 0;
		}
		
		ret = PyTuple_New(count);
		char *p = (char*) saddrs;
	
		for(x = 0; x < count; ++x) {
			int len;
			int family;
			int port;
			PyObject* oaddr;

			if (from_sockaddr((struct sockaddr*) p, &family, &len, &port, 
										addr, sizeof(addr))) {
				oaddr = PyTuple_New(2);
				PyTuple_SetItem(oaddr, 0, PyUnicode_FromString(addr));
				PyTuple_SetItem(oaddr, 1, Py23_PyLong_FromLong(port));
				PyTuple_SetItem(ret, x, oaddr);
			} else {
				// something's wrong; not safe to continue
				break;
			}
			
			p += len;
		}

		sctp_freeladdrs(saddrs);

		// If something went wrong, the remaining expected addresses will be set to None.
		for(; x < count; ++x) {
			PyTuple_SetItem(ret, x, Py_None);
			Py_INCREF(Py_None);

		}
	}

	return ret;
}

static PyObject* sctp_send_msg(PyObject* dummy, PyObject* args)
{
	Py_ssize_t msg_len;
	int fd, size_sent, flags, stream, ttl, context;
	int ppid;
	const char *msg;
	char *to;
	int port;

	struct sockaddr_storage sto;
	struct sockaddr_storage *psto = &sto;
	int sto_len;

	PyObject *ret = 0;

	if (! PyArg_ParseTuple(args, "is#(si)iiiii", &fd, &msg, &msg_len, &to, &port, 
					&ppid, &flags, &stream, &ttl, &context)) {
		return ret;
	}

	if (msg_len <= 0 && (! (flags & MSG_EOF))) {
		PyErr_SetString(PyExc_ValueError, "Empty messages are not allowed, except if coupled with the MSG_EOF flag.");
		return ret;
	}

	if (strlen(to) == 0) {
		// special case: should pass NULL 
		sto_len = 0;
		psto = 0;
	} else {
		if (! to_sockaddr(to, port, (struct sockaddr*) psto, &sto_len)) {
			PyErr_SetString(PyExc_ValueError, "Invalid Address");
			return ret;
		}
	}

	Py_BEGIN_ALLOW_THREADS
	size_sent = sctp_sendmsg(fd, msg, msg_len, (struct sockaddr*) psto, sto_len, ppid, 
					flags, stream, ttl, context);
	Py_END_ALLOW_THREADS

	if (size_sent < 0) {
		PyErr_SetFromErrno(PyExc_IOError);
		return ret;
	}

	ret = Py23_PyLong_FromLong(size_sent);
	return ret;
}

void interpret_sndrcvinfo(PyObject* dict, const struct sctp_sndrcvinfo* sinfo)
{
	PyDict_SetItemString(dict, "stream", Py23_PyLong_FromLong(sinfo->sinfo_stream));
	PyDict_SetItemString(dict, "ssn", Py23_PyLong_FromLong(sinfo->sinfo_ssn));
	PyDict_SetItemString(dict, "flags", Py23_PyLong_FromLong(sinfo->sinfo_flags));
	PyDict_SetItemString(dict, "ppid", Py23_PyLong_FromLong(sinfo->sinfo_ppid));
	PyDict_SetItemString(dict, "context", Py23_PyLong_FromLong(sinfo->sinfo_context));
	PyDict_SetItemString(dict, "timetolive", Py23_PyLong_FromLong(sinfo->sinfo_timetolive));
	PyDict_SetItemString(dict, "tsn", Py23_PyLong_FromLong(sinfo->sinfo_tsn));
	PyDict_SetItemString(dict, "cumtsn", Py23_PyLong_FromLong(sinfo->sinfo_cumtsn));
	PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(sinfo->sinfo_assoc_id));
}

void interpret_notification(PyObject* dict, const void *pnotif, int size)
{
	const union sctp_notification *notif = pnotif;
	PyDict_SetItemString(dict, "type", Py23_PyLong_FromLong(notif->sn_header.sn_type));
	PyDict_SetItemString(dict, "flags", Py23_PyLong_FromLong(notif->sn_header.sn_flags));
	PyDict_SetItemString(dict, "length", Py23_PyLong_FromLong(notif->sn_header.sn_length));

	switch (notif->sn_header.sn_type) {
	case SCTP_ASSOC_CHANGE:
		{
		const struct sctp_assoc_change* n = &(notif->sn_assoc_change);
		PyDict_SetItemString(dict, "state", Py23_PyLong_FromLong(n->sac_state));
		PyDict_SetItemString(dict, "error", Py23_PyLong_FromLong(n->sac_error));
		PyDict_SetItemString(dict, "outbound_streams", Py23_PyLong_FromLong(n->sac_outbound_streams));
		PyDict_SetItemString(dict, "inbound_streams", Py23_PyLong_FromLong(n->sac_inbound_streams));
		PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->sac_assoc_id));
		}
		break;
	case SCTP_PEER_ADDR_CHANGE: 
		{
		const struct sctp_paddr_change* n = &(notif->sn_paddr_change);
		char caddr[256];
		int family;
		int len;
		int port;
		PyObject* oaddr;

		if (from_sockaddr((struct sockaddr*) &(n->spc_aaddr), &family, &len, &port, 
									caddr, sizeof(caddr))) {
			oaddr = PyTuple_New(2);
			PyTuple_SetItem(oaddr, 0, PyUnicode_FromString(caddr));
			PyTuple_SetItem(oaddr, 1, Py23_PyLong_FromLong(port));
		} else {
			// something went wrong
			oaddr = Py_None;
			Py_INCREF(Py_None);
		}

		PyDict_SetItemString(dict, "addr", oaddr);
		PyDict_SetItemString(dict, "state", Py23_PyLong_FromLong(n->spc_state));
		PyDict_SetItemString(dict, "error", Py23_PyLong_FromLong(n->spc_error));
		PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->spc_assoc_id));
		}
		break;
	case SCTP_SEND_FAILED:
		{
		const struct sctp_send_failed* n = &(notif->sn_send_failed);
		const char* cdata = ((char*) notif) + sizeof(struct sctp_send_failed);
		int ldata = size - sizeof(struct sctp_send_failed);

		if (ldata >= 0) {
			PyObject* info = PyDict_New();
			interpret_sndrcvinfo(info, &(n->ssf_info));
			PyDict_SetItemString(dict, "_info", info);
			PyDict_SetItemString(dict, "error", Py23_PyLong_FromLong(n->ssf_error));
			PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->ssf_assoc_id));
			PyDict_SetItemString(dict, "data", PyUnicode_FromStringAndSize(cdata, ldata));
		}
		}
		break;
	case SCTP_REMOTE_ERROR:
		{
		const struct sctp_remote_error* n = &(notif->sn_remote_error);
		const char* cdata = ((char*) notif) + sizeof(struct sctp_remote_error);
		int ldata = size - sizeof(struct sctp_remote_error);
		
		if (ldata >= 0) {
			PyDict_SetItemString(dict, "error", Py23_PyLong_FromLong(n->sre_error));
			PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->sre_assoc_id));
			PyDict_SetItemString(dict, "data", PyUnicode_FromStringAndSize(cdata, ldata));
		}
		}
		break;
	case SCTP_SHUTDOWN_EVENT:
		{
		const struct sctp_shutdown_event* n = &(notif->sn_shutdown_event);
		PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->sse_assoc_id));
		}
		break;
	case SCTP_PARTIAL_DELIVERY_EVENT:
		{
		const struct sctp_pdapi_event* n = &(notif->sn_pdapi_event);
		PyDict_SetItemString(dict, "indication", Py23_PyLong_FromLong(n->pdapi_indication));
		PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->pdapi_assoc_id));
		}
		break;
	case SCTP_ADAPTATION_INDICATION:
		{
		const struct sctp_adaptation_event* n = &(notif->sn_adaptation_event);
		PyDict_SetItemString(dict, "adaptation_ind", Py23_PyLong_FromLong(n->sai_adaptation_ind));
		PyDict_SetItemString(dict, "assoc_id", Py23_PyLong_FromLong(n->sai_assoc_id));
		}
		break;
	}
}

static PyObject* sctp_recv_msg(PyObject* dummy, PyObject* args)
{
	int fd;
	size_t max_len;

	struct sockaddr_storage sfrom;
	socklen_t sfrom_len = sizeof(sfrom);
	int family;
	int len;
	int port;
	char cfrom[256];
	char *msg;
	int size;
	int flags = 0;
	struct sctp_sndrcvinfo sinfo;

	PyObject* notification = PyDict_New();
	PyObject* ret = 0;
	PyObject* oaddr = 0;
	
	if (! PyArg_ParseTuple(args, "in", &fd, &max_len)) {
		return ret;
	}

	msg = malloc(max_len);
	if (! msg) {
		PyErr_SetString(PyExc_MemoryError, "Out of memory, malloc() failed");
		return ret;
	}

	bzero(&sfrom, sizeof(sfrom));
	bzero(&sinfo, sizeof(sinfo));

	Py_BEGIN_ALLOW_THREADS
	size = sctp_recvmsg(fd, msg, max_len, (struct sockaddr*) &sfrom, &sfrom_len, &sinfo, &flags);
	Py_END_ALLOW_THREADS

	if (size < 0) {
		free(msg);
		PyErr_SetFromErrno(PyExc_IOError);
		return ret;
	}

	if (flags & MSG_NOTIFICATION) {
		interpret_notification(notification, msg, size);
		size = -1;
	} else {
		interpret_sndrcvinfo(notification, &sinfo);
	}

	if (from_sockaddr((struct sockaddr*) &sfrom, &family, &len, &port, cfrom, sizeof(cfrom))) {
		oaddr = PyTuple_New(2);
		PyTuple_SetItem(oaddr, 0, PyUnicode_FromString(cfrom));
		PyTuple_SetItem(oaddr, 1, Py23_PyLong_FromLong(port));
	} else {
		// something went wrong
		oaddr = Py_None;
		Py_INCREF(Py_None);
	}
			
	ret = PyTuple_New(4);
	PyTuple_SetItem(ret, 0, oaddr);
	PyTuple_SetItem(ret, 1, Py23_PyLong_FromLong(flags));
	if (size >= 0) {
		PyTuple_SetItem(ret, 2, PyBytes_FromStringAndSize(msg, size));
	} else {
		PyTuple_SetItem(ret, 2, Py_None);
		Py_INCREF(Py_None);
	}
	PyTuple_SetItem(ret, 3, notification);

	free(msg);
	return ret;
}

