#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;

void gil_real_get() {
	//uwsgi_log("LOCK %d\n", uwsgi.mywid);
#if !defined(PYTHREE)
	PyEval_AcquireLock();
	PyThreadState_Swap((PyThreadState *) pthread_getspecific(up.upt_gil_key));
#else
	PyEval_RestoreThread((PyThreadState *) pthread_getspecific(up.upt_gil_key));
#endif

	//uwsgi_log("LOCKED !!! %d\n", uwsgi.mywid);
}

void gil_real_release() {
	//uwsgi_log("UNLOCK %d\n", uwsgi.mywid);
#if !defined(PYTHREE)
	pthread_setspecific(up.upt_gil_key, (void *) PyThreadState_Swap(NULL));
	PyEval_ReleaseLock();	
#else
	pthread_setspecific(up.upt_gil_key, (void *) PyThreadState_Get());
	PyEval_SaveThread();
#endif
}


void gil_fake_get() {}
void gil_fake_release() {}
