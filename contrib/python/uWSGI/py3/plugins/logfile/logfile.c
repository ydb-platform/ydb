#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>
extern struct uwsgi_server uwsgi;

struct logfile_data {
	char *logfile;
	char *backupname;
	uint64_t maxsize;
};

static ssize_t uwsgi_file_logger(struct uwsgi_logger *ul, char *message, size_t len) {

	if (!ul->configured) {
		if (ul->arg) {
			int is_keyval = 0;
			char *backupname = NULL;
			char *maxsize = NULL;
			char *logfile = NULL;

			if (strchr(ul->arg, '=')) {
				if (uwsgi_kvlist_parse(ul->arg, strlen(ul->arg), ',', '=',
					"logfile", &logfile, "backupname", &backupname, "maxsize", &maxsize, NULL)) {
					uwsgi_log("[uwsgi-logfile] invalid keyval syntax\n");
					exit(1);
				}
				is_keyval = 1;
			}
			if (is_keyval) {
				if (!logfile) {
					uwsgi_log("[uwsgi-logfile] missing logfile key\n");
					return 0;
				}

				if (maxsize) {
					struct logfile_data *data = uwsgi_malloc(sizeof(struct logfile_data));
					data->logfile = logfile;
					data->backupname = backupname;
					data->maxsize = (uint64_t)strtoull(maxsize, NULL, 10);
					ul->data = data;

					free(maxsize);
					maxsize = NULL;
				}
			} else {
				logfile = ul->arg;
			}

			ul->fd = open(logfile, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP);
			if (ul->fd >= 0) {
				ul->configured = 1;
			}	
		}
	}

	if (ul->fd >= 0) {
		ssize_t written = write(ul->fd, message, len);

		if (ul->data) {
			struct logfile_data *data = ul->data;
			off_t logsize = lseek(ul->fd, 0, SEEK_CUR);

			if (data->maxsize > 0 && (uint64_t) logsize > data->maxsize) {
				uwsgi_log_do_rotate(data->logfile, data->backupname, logsize, ul->fd);
			}
		}

		return written;
	}

	return 0;
}

static ssize_t uwsgi_fd_logger(struct uwsgi_logger *ul, char *message, size_t len) {

        if (!ul->configured) {
		ul->fd = -1;
                if (ul->arg) ul->fd = atoi(ul->arg);
                ul->configured = 1;
        }

        if (ul->fd >= 0) {
                return write(ul->fd, message, len);
        }
        return 0;

}

static ssize_t uwsgi_stdio_logger(struct uwsgi_logger *ul, char *message, size_t len) {

        if (uwsgi.original_log_fd >= 0) {
                return write(uwsgi.original_log_fd, message, len);
        }
        return 0;
}


void uwsgi_file_logger_register() {
	uwsgi_register_logger("file", uwsgi_file_logger);
	uwsgi_register_logger("fd", uwsgi_fd_logger);
	uwsgi_register_logger("stdio", uwsgi_stdio_logger);
}

struct uwsgi_plugin logfile_plugin = {

        .name = "logfile",
        .on_load = uwsgi_file_logger_register,

};

