#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

/*

this plugin, allows remote spooling of jobs

*/

extern struct uwsgi_server uwsgi;

int uwsgi_request_spooler(struct wsgi_request *wsgi_req) {

	struct uwsgi_header uh;

        // TODO get the spooler from the modifier2

        if (uwsgi.spoolers == NULL) {
                uwsgi_log("the spooler is inactive !!!...skip\n");
		uh.modifier1 = 255;
		uh.pktsize = 0;
		uh.modifier2 = 0;
		uwsgi_response_write_body_do(wsgi_req, (char *) &uh, 4);
                return -1;
        }

        char *filename = uwsgi_spool_request(NULL, wsgi_req->buffer, wsgi_req->uh->pktsize, NULL, 0);
        uh.modifier1 = 255;
        uh.pktsize = 0;
        if (filename) {
                uh.modifier2 = 1;
		if (uwsgi_response_write_body_do(wsgi_req, (char *) &uh, 4)) {
                        uwsgi_log("disconnected client, remove spool file.\n");
                        /* client disconnect, remove spool file */
                        if (unlink(filename)) {
                                uwsgi_error("uwsgi_request_spooler()/unlink()");
                                uwsgi_log("something horrible happened !!! check your spooler ASAP !!!\n");
                                exit(1);
                        }
                }
		free(filename);
                return 0;
        }
        else {
                /* announce a failed spool request */
                uh.modifier2 = 0;
		uwsgi_response_write_body_do(wsgi_req, (char *) &uh, 4);
        }

        return -1;
}


struct uwsgi_plugin spooler_plugin = {

	.name = "spooler",
	.modifier1 = 17,
	
	.request = uwsgi_request_spooler,
};
