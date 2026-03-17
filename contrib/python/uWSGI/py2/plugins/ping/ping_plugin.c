#include <contrib/python/uWSGI/py2/config.h>
#include "../../uwsgi.h"

extern struct uwsgi_server uwsgi;

struct uwsgi_ping {
	char *ping;
	int ping_timeout;
} uping;

struct uwsgi_option uwsgi_ping_options[] = {
	{"ping", required_argument, 0, "ping specified uwsgi host", uwsgi_opt_set_str, &uping.ping, UWSGI_OPT_NO_INITIAL | UWSGI_OPT_NO_SERVER},
	{"ping-timeout", required_argument, 0, "set ping timeout", uwsgi_opt_set_int, &uping.ping_timeout, 0},
	{ 0, 0, 0, 0, 0, 0, 0 }
};

static void ping() {

	struct uwsgi_header uh;
	char *buf = NULL;

	// use a 3 secs timeout by default
	if (!uping.ping_timeout) uping.ping_timeout = 3;

	uwsgi_log("PING uwsgi host %s (timeout: %d)\n", uping.ping, uping.ping_timeout);

	int fd = uwsgi_connect(uping.ping, uping.ping_timeout, 0);
	if (fd < 0) {
		exit(1);
	}

	uh.modifier1 = UWSGI_MODIFIER_PING;
	uh.pktsize = 0;
	uh.modifier2 = 0;

	if (write(fd, &uh, 4) != 4) {
		uwsgi_error("write()");
		exit(2);
	}

	int ret = uwsgi_read_response(fd, &uh, uping.ping_timeout, &buf);
	if (ret < 0) {
		exit(1);
	}
	else {
		if (uh.pktsize > 0) {
			uwsgi_log("[WARNING] node %s message: %.*s\n", uping.ping, uh.pktsize, buf);
			exit(2);
		}
		else {
			exit(0);
		}
	}

}


int ping_init() {

	if (uping.ping) {
		ping();
		//never here
	}

	return 1;
}

/* uwsgi PING|100 */
int uwsgi_request_ping(struct wsgi_request *wsgi_req) {
	char len;

	uwsgi_log( "PING\n");
	wsgi_req->uh->modifier2 = 1;
	wsgi_req->uh->pktsize = 0;
	wsgi_req->do_not_account = 1;

	len = strlen(uwsgi.shared->warning_message);
	if (len > 0) {
		// TODO: check endianess ?
		wsgi_req->uh->pktsize = len;
	}

	if (uwsgi_response_write_body_do(wsgi_req, (char *) wsgi_req->uh, 4)) {
		return -1;
	}

	if (len > 0) {
		if (uwsgi_response_write_body_do(wsgi_req, uwsgi.shared->warning_message, len)) {
			return -1;
		}
	}

	return UWSGI_OK;
}

struct uwsgi_plugin ping_plugin = {

	.name = "ping",
	.modifier1 = 100,
	.options = uwsgi_ping_options,
	.request = uwsgi_request_ping,
	.init = ping_init,
};
