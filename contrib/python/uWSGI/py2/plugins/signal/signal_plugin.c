#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

/* request 110 */
int uwsgi_request_signal(struct wsgi_request *wsgi_req) {
	
	uint8_t ret_status = 1;

	struct uwsgi_header uh;
	if (uwsgi_signal_send(uwsgi.signal_socket, wsgi_req->uh->modifier2) < 0) {
		ret_status = 0;
	}

        uh.modifier1 = 255;
       	uh.pktsize = 0;
       	uh.modifier2 = ret_status;
	
	if (uwsgi_response_write_body_do(wsgi_req, (char *) &uh, 4)) {
                return -1;
        }

        return UWSGI_OK;
}


struct uwsgi_plugin signal_plugin = {

        .name = "signal",
        .modifier1 = 110,
        .request = uwsgi_request_signal,

};

