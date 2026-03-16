#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

struct uwsgi_router_tofile_conf {

	// the name of the file
	char *filename;
	size_t filename_len;

	// the mode of the file
	char *mode;
	size_t mode_len;
};

// this is allocated for each transformation
struct uwsgi_transformation_tofile_conf {
	struct uwsgi_buffer *filename;
};

static int transform_tofile(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {
	
	struct uwsgi_transformation_tofile_conf *uttc = (struct uwsgi_transformation_tofile_conf *) ut->data;
	struct uwsgi_buffer *ub = ut->chunk;

	// store only successfull response
	if (wsgi_req->write_errors == 0 && wsgi_req->status == 200 && ub->pos > 0) {
		if (uttc->filename) {
			int fd = open(uttc->filename->buf, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
			if (fd < 0) {
				uwsgi_error_open(uttc->filename->buf);
				goto end;
			}
			// lock the file
			if (uwsgi_fcntl_lock(fd)) {
				close(fd);
				goto end;
			}
			// write to it
			size_t remains = ub->pos;
			while(remains) {
				ssize_t rlen = write(fd, ub->buf + (ub->pos - remains), remains);
				if (rlen <= 0) {
					uwsgi_req_error("transform_tofile()/write()");
					unlink(uttc->filename->buf);
					break;
				}
				remains -= rlen;
			}
                        // unlock/close
			close(fd);
		}
	}

end:
	// free resources
	if (uttc->filename) uwsgi_buffer_destroy(uttc->filename);
	free(uttc);
	// reset the buffer
        return 0;
}


// be tolerant on errors
static int uwsgi_routing_func_tofile(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){
	struct uwsgi_router_tofile_conf *urtc = (struct uwsgi_router_tofile_conf *) ur->data2;

	struct uwsgi_transformation_tofile_conf *uttc = uwsgi_calloc(sizeof(struct uwsgi_transformation_tofile_conf));

	// build key and name
        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        uttc->filename = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urtc->filename, urtc->filename_len);
        if (!uttc->filename) goto error;
	
	uwsgi_add_transformation(wsgi_req, transform_tofile, uttc);
	return UWSGI_ROUTE_NEXT;

error:
	if (uttc->filename) uwsgi_buffer_destroy(uttc->filename);
	free(uttc);
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_tofile(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_tofile;
	ur->data = args;
	ur->data_len = strlen(args);
	struct uwsgi_router_tofile_conf *urtc = uwsgi_calloc(sizeof(struct uwsgi_router_tofile_conf));
	if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "filename", &urtc->filename,
                        "name", &urtc->filename,
                        "mode", &urtc->mode, NULL)) {
                uwsgi_log("invalid tofile route syntax: %s\n", args);
		goto error;
	}

        if (!urtc->filename) {
		uwsgi_log("invalid tofile route syntax, you need to specify a filename\n");
		goto error;
	}
        urtc->filename_len = strlen(urtc->filename);
	ur->data2 = urtc;
        return 0;
error:
	if (urtc->filename) free(urtc->filename);
	free(urtc);
	return -1;
}

static void router_tofile_register() {
	uwsgi_register_router("tofile", uwsgi_router_tofile);
	uwsgi_register_router("to-file", uwsgi_router_tofile);
}

struct uwsgi_plugin transformation_tofile_plugin = {
	.name = "transformation_tofile",
	.on_load = router_tofile_register,
};

#else
struct uwsgi_plugin transformation_tofile_plugin = {
	.name = "transformation_tofile",
};
#endif
