#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	uWSGI timebomb

	this is a simple thread waiting for a timeout and calling exit
	with the specified value.

	You can use it as a last resort in async apps that could block
	normal uWSGI behaviours

*/

struct time_bomb {
	int timeout;
	int exit_code;
};

static void *time_bomb(void *arg) {

	// block all signals
        sigset_t smask;
        sigfillset(&smask);
        pthread_sigmask(SIG_BLOCK, &smask, NULL);

	struct time_bomb *tb = (struct time_bomb *) arg;

	struct timeval tv;
	tv.tv_sec = tb->timeout;
	tv.tv_usec = 0;

	select(0, NULL, NULL, NULL, &tv);
	uwsgi_log_verbose("*** BOOOOOOM ***\n");
	exit(tb->exit_code);
	
}

void uwsgi_time_bomb(int timeout, int exit_code) {

	pthread_t time_bomb_thread;

	struct time_bomb *tb = uwsgi_malloc(sizeof(struct time_bomb));
	tb->timeout = timeout;
	tb->exit_code = exit_code;
	
	if (pthread_create(&time_bomb_thread, NULL, time_bomb, (void *) tb)) {
        	uwsgi_error("pthread_create()");
                uwsgi_log("unable to setup the time bomb, goodbye\n");
		exit(exit_code);
	}
        else {
        	uwsgi_log_verbose("Fire in the hole !!! (%d seconds to detonation)\n", timeout);
        }
	
}
