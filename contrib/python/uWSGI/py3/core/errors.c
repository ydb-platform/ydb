#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

static int error_page(struct wsgi_request *wsgi_req, struct uwsgi_string_list *l) {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, l) {
		struct stat st;
		if (!stat(usl->value, &st)) {
			int fd = open(usl->value, O_RDONLY);
			if (fd >= 0) {
				if (uwsgi_response_add_header(wsgi_req, "Content-Type", 12, "text/html", 9)) { close(fd); return 0;}
				if (uwsgi_response_add_content_length(wsgi_req, st.st_size)) { close(fd); return 0;}
				uwsgi_response_sendfile_do(wsgi_req, fd, 0, st.st_size);			
				return -1;
			}
		}
	}
	return 0;
}

// generate internal server error message
void uwsgi_500(struct wsgi_request *wsgi_req) {
	if (uwsgi_response_prepare_headers(wsgi_req, "500 Internal Server Error", 25)) return;
	if (uwsgi_response_add_connection_close(wsgi_req)) return;
	if (error_page(wsgi_req, uwsgi.error_page_500)) return;
	if (uwsgi_response_add_header(wsgi_req, "Content-Type", 12, "text/plain", 10)) return;
	uwsgi_response_write_body_do(wsgi_req, "Internal Server Error", 21);
}


void uwsgi_404(struct wsgi_request *wsgi_req) {
	if (uwsgi_response_prepare_headers(wsgi_req, "404 Not Found", 13)) return;
	if (uwsgi_response_add_connection_close(wsgi_req)) return;
	if (error_page(wsgi_req, uwsgi.error_page_404)) return;
	if (uwsgi_response_add_header(wsgi_req, "Content-Type", 12, "text/plain", 10)) return;
	uwsgi_response_write_body_do(wsgi_req, "Not Found", 9);
}

void uwsgi_403(struct wsgi_request *wsgi_req) {
	if (uwsgi_response_prepare_headers(wsgi_req, "403 Forbidden", 13)) return;
	if (uwsgi_response_add_connection_close(wsgi_req)) return;
	if (error_page(wsgi_req, uwsgi.error_page_403)) return;
	if (uwsgi_response_add_content_type(wsgi_req, "text/plain", 10)) return;
	uwsgi_response_write_body_do(wsgi_req, "Forbidden", 9);
}

void uwsgi_405(struct wsgi_request *wsgi_req) {
        if (uwsgi_response_prepare_headers(wsgi_req, "405 Method Not Allowed", 22)) return;
        if (uwsgi_response_add_connection_close(wsgi_req)) return;
        if (error_page(wsgi_req, uwsgi.error_page_403)) return;
        if (uwsgi_response_add_content_type(wsgi_req, "text/plain", 10)) return;
        uwsgi_response_write_body_do(wsgi_req, "Method Not Allowed", 18);
}

void uwsgi_redirect_to_slash(struct wsgi_request *wsgi_req) {

	char *redirect = NULL;
	size_t redirect_len = 0;

        if (uwsgi_response_prepare_headers(wsgi_req, "301 Moved Permanently", 21)) return;
	if (uwsgi_response_add_connection_close(wsgi_req)) return;

	if (wsgi_req->query_string_len == 0) {
		redirect = uwsgi_concat2n(wsgi_req->path_info, wsgi_req->path_info_len, "/", 1);
		redirect_len = wsgi_req->path_info_len + 1;
	}
        else {
		redirect = uwsgi_concat3n(wsgi_req->path_info, wsgi_req->path_info_len, "/?", 2, wsgi_req->query_string, wsgi_req->query_string_len);
		redirect_len = wsgi_req->path_info_len + 2 + wsgi_req->query_string_len;
        }
        uwsgi_response_add_header(wsgi_req, "Location", 8, redirect, redirect_len);
	free(redirect);	
        return;
}

