#include <contrib/python/uWSGI/py2/config.h>
#include "../../uwsgi.h"

extern struct uwsgi_server uwsgi;
int use_nagios = 0;

struct uwsgi_option nagios_options[] = {

	{"nagios", no_argument, 0, "nagios check", uwsgi_opt_true, &use_nagios, UWSGI_OPT_NO_INITIAL},
        {0, 0, 0, 0, 0, 0, 0},

};


int nagios() {

	struct uwsgi_header uh;
	char *buf = NULL;

	if (!use_nagios) {
		return 1;
	}

	if (!uwsgi.sockets) {
		fprintf(stdout, "UWSGI UNKNOWN: you have specified an invalid socket\n");
		exit(3);
	}


	int fd = uwsgi_connect(uwsgi.sockets->name, uwsgi.socket_timeout, 0);
	if (fd < 0) {
		fprintf(stdout, "UWSGI CRITICAL: could not connect() to workers %s\n", strerror(errno));
		if (errno == EPERM || errno == EACCES) {
			exit(3);
		}
		exit(2);
	}

	uh.modifier1 = UWSGI_MODIFIER_PING;
	uh.pktsize = 0;
	uh.modifier2 = 0;
	if (write(fd, &uh, 4) != 4) {
		uwsgi_error("write()");
		fprintf(stdout, "UWSGI CRITICAL: could not send ping packet to workers\n");
		exit(2);
	}


	int ret = uwsgi_read_response(fd, &uh, uwsgi.socket_timeout, &buf);

	if (ret == -2) {
		fprintf(stdout, "UWSGI CRITICAL: timed out waiting for response\n");
		exit(2);
	}
	else if (ret == -1) {
		fprintf(stdout, "UWSGI CRITICAL: error reading response\n");
		exit(2);
	}
	else {
		if (uh.pktsize > 0 && buf) {
			fprintf(stdout, "UWSGI WARNING: %.*s\n", uh.pktsize, buf);
			exit(1);
		}
		else {
			fprintf(stdout, "UWSGI OK: armed and ready\n");
			exit(0);
		}
	}

	// never here
	fprintf(stdout, "UWSGI UNKNOWN: probably you hit a bug of uWSGI !!!\n");
	exit(3);
}

struct uwsgi_plugin nagios_plugin = {
	
	.name = "nagios",
	.options = nagios_options,
	.init = nagios,
};
