#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	upload progress facilities

*/

extern struct uwsgi_server uwsgi;

char *uwsgi_upload_progress_create(struct wsgi_request *wsgi_req, int *fd) {
	const char *x_progress_id = "X-Progress-ID=";
	char *xpi_ptr = (char *) x_progress_id;
	uint16_t i;
	char *upload_progress_filename = NULL;

	if (wsgi_req->uri_len <= 51)
		return NULL;


	for (i = 0; i < wsgi_req->uri_len; i++) {
		if (wsgi_req->uri[i] == xpi_ptr[0]) {
			if (xpi_ptr[0] == '=') {
				if (wsgi_req->uri + i + 36 <= wsgi_req->uri + wsgi_req->uri_len) {
					upload_progress_filename = wsgi_req->uri + i + 1;
				}
				break;
			}
			xpi_ptr++;
		}
		else {
			xpi_ptr = (char *) x_progress_id;
		}
	}

	// now check for valid uuid (from spec available at http://en.wikipedia.org/wiki/Universally_unique_identifier)
	if (!upload_progress_filename)
		return NULL;

	uwsgi_log("upload progress uuid = %.*s\n", 36, upload_progress_filename);
	if (!check_hex(upload_progress_filename, 8))
		return NULL;
	if (upload_progress_filename[8] != '-')
		return NULL;

	if (!check_hex(upload_progress_filename + 9, 4))
		return NULL;
	if (upload_progress_filename[13] != '-')
		return NULL;

	if (!check_hex(upload_progress_filename + 14, 4))
		return NULL;
	if (upload_progress_filename[18] != '-')
		return NULL;

	if (!check_hex(upload_progress_filename + 19, 4))
		return NULL;
	if (upload_progress_filename[23] != '-')
		return NULL;

	if (!check_hex(upload_progress_filename + 24, 12))
		return NULL;

	upload_progress_filename = uwsgi_concat4n(uwsgi.upload_progress, strlen(uwsgi.upload_progress), "/", 1, upload_progress_filename, 36, ".js", 3);
	// here we use O_EXCL to avoid eventual application bug in uuid generation/using
	*fd = open(upload_progress_filename, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP);
	if (*fd < 0) {
		uwsgi_error_open(upload_progress_filename);
		free(upload_progress_filename);
		return NULL;
	}

	return upload_progress_filename;
}

int uwsgi_upload_progress_update(struct wsgi_request *wsgi_req, int fd, size_t remains) {
	char buf[4096];

	int ret = snprintf(buf, 4096, "{ \"state\" : \"uploading\", \"received\" : %llu, \"size\" : %llu }\r\n", (unsigned long long) (wsgi_req->post_cl - remains), (unsigned long long) wsgi_req->post_cl);
	if (ret <= 0 || ret >= 4096) {
		return -1;
	}

	if (lseek(fd, 0, SEEK_SET)) {
		uwsgi_error("uwsgi_upload_progress_update()/lseek()");
		return -1;
	}

	if (write(fd, buf, ret) != ret) {
		uwsgi_error("uwsgi_upload_progress_update()/write()");
		return -1;
	}

	if (fsync(fd)) {
		uwsgi_error("uwsgi_upload_progress_update()/fsync()");
		return -1;
	}
	return 0;
}

void uwsgi_upload_progress_destroy(char *filename, int fd) {
	close(fd);
	if (unlink(filename)) {
		uwsgi_error("uwsgi_upload_progress_destroy()/unlink()");
	}
	free(filename);
}
