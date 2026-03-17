#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

struct uwsgi_router_file_conf {

	char *filename;
	size_t filename_len;

	char *status;
	size_t status_len;

	char *content_type;
	size_t content_type_len;
	
	char *mime;

	char *no_cl;

	char *no_headers;
};

int uwsgi_routing_func_static(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data, ur->data_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	uwsgi_file_serve(wsgi_req, ub->buf, ub->pos, NULL, 0, 1);
	uwsgi_buffer_destroy(ub);
	if (ur->custom == 1)
		return UWSGI_ROUTE_NEXT;
	return UWSGI_ROUTE_BREAK;
}

int uwsgi_routing_func_file(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	char buf[32768];
	struct stat st;
	int ret = UWSGI_ROUTE_BREAK;
	size_t remains = 0;

	struct uwsgi_router_file_conf *urfc = (struct uwsgi_router_file_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->filename, urfc->filename_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	int fd = open(ub->buf, O_RDONLY);
	if (fd < 0) {
		if (ur->custom)
			ret = UWSGI_ROUTE_NEXT;
		goto end; 
	}

	if (fstat(fd, &st)) {
		goto end2;
	}

	struct uwsgi_buffer *ub_s = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->status, urfc->status_len);
        if (!ub_s) goto end2;

	// static file - don't update avg_rt after request
	wsgi_req->do_not_account_avg_rt = 1;

	if (urfc->no_headers) goto send;

	if (uwsgi_response_prepare_headers(wsgi_req, ub_s->buf, ub_s->pos)) {
		uwsgi_buffer_destroy(ub_s);
		goto end2;
	}
	uwsgi_buffer_destroy(ub_s);
	if (!urfc->no_cl) {
		if (uwsgi_response_add_content_length(wsgi_req, st.st_size)) goto end2;
	}
	if (urfc->mime) {
		size_t mime_type_len = 0;
		char *mime_type = uwsgi_get_mime_type(ub->buf, ub->pos, &mime_type_len);
		if (mime_type) {
			if (uwsgi_response_add_content_type(wsgi_req, mime_type, mime_type_len)) goto end2;
		}
		else {
			if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
		}
	}
	else {
		if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
	}

send:
	
	remains = st.st_size;
	while(remains) {
		ssize_t rlen = read(fd, buf, UMIN(32768, remains));
		if (rlen <= 0) goto end2;
		if (uwsgi_response_write_body_do(wsgi_req, buf, rlen)) goto end2;
		remains -= rlen;
	}
	
end2:
	close(fd);
end:
        uwsgi_buffer_destroy(ub);
        return ret;
}

int uwsgi_routing_func_sendfile(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

        struct stat st;
        int ret = UWSGI_ROUTE_BREAK;

        struct uwsgi_router_file_conf *urfc = (struct uwsgi_router_file_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->filename, urfc->filename_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        int fd = open(ub->buf, O_RDONLY);
        if (fd < 0) {
                if (ur->custom)
                        ret = UWSGI_ROUTE_NEXT;
                goto end;
        }

        if (fstat(fd, &st)) {
                goto end2;
        }

        struct uwsgi_buffer *ub_s = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->status, urfc->status_len);
        if (!ub_s) goto end2;

        // static file - don't update avg_rt after request
        wsgi_req->do_not_account_avg_rt = 1;

	if (urfc->no_headers) goto send;

        if (uwsgi_response_prepare_headers(wsgi_req, ub_s->buf, ub_s->pos)) {
                uwsgi_buffer_destroy(ub_s);
                goto end2;
        }
        uwsgi_buffer_destroy(ub_s);
        if (uwsgi_response_add_content_length(wsgi_req, st.st_size)) goto end2;
        if (urfc->mime) {
                size_t mime_type_len = 0;
                char *mime_type = uwsgi_get_mime_type(ub->buf, ub->pos, &mime_type_len);
                if (mime_type) {
                        if (uwsgi_response_add_content_type(wsgi_req, mime_type, mime_type_len)) goto end2;
                }
                else {
                        if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
                }
        }
        else {
                if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
        }

send:

	if (!wsgi_req->headers_sent) {
                if (uwsgi_response_write_headers_do(wsgi_req)) goto end2;
	}

	if (!uwsgi_simple_sendfile(wsgi_req, fd, 0, st.st_size)) {
		wsgi_req->via = UWSGI_VIA_SENDFILE;
		wsgi_req->response_size += st.st_size;
	}

end2:
        close(fd);
end:
        uwsgi_buffer_destroy(ub);
        return ret;
}

int uwsgi_routing_func_fastfile(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

        struct stat st;
        int ret = UWSGI_ROUTE_BREAK;

        struct uwsgi_router_file_conf *urfc = (struct uwsgi_router_file_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->filename, urfc->filename_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        int fd = open(ub->buf, O_RDONLY);
        if (fd < 0) {
                if (ur->custom)
                        ret = UWSGI_ROUTE_NEXT;
                goto end;
        }

        if (fstat(fd, &st)) {
                goto end2;
        }

        struct uwsgi_buffer *ub_s = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urfc->status, urfc->status_len);
        if (!ub_s) goto end2;

        // static file - don't update avg_rt after request
        wsgi_req->do_not_account_avg_rt = 1;

	if (urfc->no_headers) goto send;

        if (uwsgi_response_prepare_headers(wsgi_req, ub_s->buf, ub_s->pos)) {
                uwsgi_buffer_destroy(ub_s);
                goto end2;
        }
        uwsgi_buffer_destroy(ub_s);
        if (uwsgi_response_add_content_length(wsgi_req, st.st_size)) goto end2;
        if (urfc->mime) {
                size_t mime_type_len = 0;
                char *mime_type = uwsgi_get_mime_type(ub->buf, ub->pos, &mime_type_len);
                if (mime_type) {
                        if (uwsgi_response_add_content_type(wsgi_req, mime_type, mime_type_len)) goto end2;
                }
                else {
                        if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
                }
        }
        else {
                if (uwsgi_response_add_content_type(wsgi_req, urfc->content_type, urfc->content_type_len)) goto end2;
        }

send:

	if (!wsgi_req->headers_sent) {
                if (uwsgi_response_write_headers_do(wsgi_req)) goto end2;
        }

	if (wsgi_req->socket->can_offload) {
		if (!uwsgi_offload_request_sendfile_do(wsgi_req, fd, 0, st.st_size)) {
                        wsgi_req->via = UWSGI_VIA_OFFLOAD;
                        wsgi_req->response_size += st.st_size;
                	// the fd will be closed by the offload engine
			goto end;
		}
	}

        if (!uwsgi_simple_sendfile(wsgi_req, fd, 0, st.st_size)) {
                wsgi_req->via = UWSGI_VIA_SENDFILE;
                wsgi_req->response_size += st.st_size;
        }

end2:
        close(fd);
end:
        uwsgi_buffer_destroy(ub);
        return ret;
}



static int uwsgi_router_static(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_static;
	ur->data = args;
	ur->data_len = strlen(args);
	return 0;
}

static int uwsgi_router_static_next(struct uwsgi_route *ur, char *args) {
	ur->custom = 1;
	return uwsgi_router_static(ur, args);
}

static int uwsgi_router_file(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_file;
        ur->data = args;
        ur->data_len = strlen(args);
        struct uwsgi_router_file_conf *urfc = uwsgi_calloc(sizeof(struct uwsgi_router_file_conf));
        if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "filename", &urfc->filename,
                        "status", &urfc->status,
                        "content_type", &urfc->content_type,
                        "nocl", &urfc->no_cl,
                        "no_cl", &urfc->no_cl,
                        "no_content_length", &urfc->no_cl,
                        "mime", &urfc->mime,
                        "no_headers", &urfc->no_headers,
                        NULL)) {
                        uwsgi_log("invalid file route syntax: %s\n", args);
			return -1;
	}

	if (!urfc->filename) {
		uwsgi_log("you have to specifify a filename for the \"file\" router\n");
		return -1;
	}

	urfc->filename_len = strlen(urfc->filename);
	if (!urfc->content_type) {
		urfc->content_type = "text/html";
	}
	urfc->content_type_len = strlen(urfc->content_type);

	if (!urfc->status) {
		urfc->status = "200 OK";
	}
	urfc->status_len = strlen(urfc->status);

	ur->data2 = urfc;
	return 0;
}

static int uwsgi_router_file_next(struct uwsgi_route *ur, char *args) {
	ur->custom = 1;
	return uwsgi_router_file(ur, args);
}

static int uwsgi_router_sendfile(struct uwsgi_route *ur, char *args) {
	int ret =  uwsgi_router_file(ur, args);
	ur->func = uwsgi_routing_func_sendfile;
	return ret;
}

static int uwsgi_router_sendfile_next(struct uwsgi_route *ur, char *args) {
	ur->custom = 1;
	int ret =  uwsgi_router_file(ur, args);
	ur->func = uwsgi_routing_func_sendfile;
	return ret;
}

static int uwsgi_router_fastfile(struct uwsgi_route *ur, char *args) {
        int ret =  uwsgi_router_file(ur, args);
        ur->func = uwsgi_routing_func_fastfile;
        return ret;
}

static int uwsgi_router_fastfile_next(struct uwsgi_route *ur, char *args) {
        ur->custom = 1;
        int ret =  uwsgi_router_file(ur, args);
        ur->func = uwsgi_routing_func_fastfile;
        return ret;
}

static void router_static_register(void) {

	uwsgi_register_router("static", uwsgi_router_static);
	uwsgi_register_router("static-next", uwsgi_router_static_next);
	uwsgi_register_router("file", uwsgi_router_file);
	uwsgi_register_router("file-next", uwsgi_router_file_next);
	uwsgi_register_router("sendfile", uwsgi_router_sendfile);
	uwsgi_register_router("sendfile-next", uwsgi_router_sendfile_next);
	uwsgi_register_router("fastfile", uwsgi_router_fastfile);
	uwsgi_register_router("fastfile-next", uwsgi_router_fastfile_next);
}

struct uwsgi_plugin router_static_plugin = {

	.name = "router_static",
	.on_load = router_static_register,
};
#else
struct uwsgi_plugin router_static_plugin = {
	.name = "router_static",
};
#endif
