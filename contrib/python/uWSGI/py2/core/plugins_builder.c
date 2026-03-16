#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

#define UWSGI_BUILD_DIR ".uwsgi_plugins_builder"

/*

	steps:

		mkdir(.uwsgi_plugin_builder)
		generate .uwsgi_plugin_builder/uwsgi.h
		generate .uwsgi_plugin_builder/uwsgiconfig.py
		setenv(UWSGI_PLUGINS_BUILDER_CFLAGS=uwsgi_cflags)
		exec PYTHON .uwsgi_plugin_builder/uwsgiconfig.py --extra-plugin <directory> [name]

*/

void uwsgi_build_plugin(char *directory) {

	if (!uwsgi_file_exists(UWSGI_BUILD_DIR)) {
		if (mkdir(UWSGI_BUILD_DIR, S_IRWXU) < 0) {
        		uwsgi_error("uwsgi_build_plugin()/mkdir() " UWSGI_BUILD_DIR "/");
			_exit(1);
		}
	}

	char *dot_h = uwsgi_get_dot_h();
	if (!dot_h) {
		uwsgi_log("unable to generate uwsgi.h");
		_exit(1);
	}

	if (strlen(dot_h) == 0) {
		free(dot_h);
		uwsgi_log("invalid uwsgi.h");
		_exit(1);
	}

	int dot_h_fd = open(UWSGI_BUILD_DIR "/uwsgi.h", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	if (dot_h_fd < 0) {
		uwsgi_error_open(UWSGI_BUILD_DIR "/uwsgi.h");
		free(dot_h);
		_exit(1);
	}

	ssize_t dot_h_len = (ssize_t) strlen(dot_h);
	if (write(dot_h_fd, dot_h, dot_h_len) != dot_h_len) {
		uwsgi_error("uwsgi_build_plugin()/write()");
		_exit(1);
	}

	char *config_py = uwsgi_get_config_py();
        if (!config_py) {
                uwsgi_log("unable to generate uwsgiconfig.py");
                _exit(1);
        }

        if (strlen(config_py) == 0) {
                uwsgi_log("invalid uwsgiconfig.py");
                _exit(1);
        }

        int config_py_fd = open(UWSGI_BUILD_DIR "/uwsgiconfig.py", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
        if (config_py_fd < 0) {
                uwsgi_error_open(UWSGI_BUILD_DIR "/uwsgiconfig.py");
                _exit(1);
        }

        ssize_t config_py_len = (ssize_t) strlen(config_py);
        if (write(config_py_fd, config_py, config_py_len) != config_py_len) {
                uwsgi_error("uwsgi_build_plugin()/write()");
                _exit(1);
        }

	char *cflags = uwsgi_get_cflags();
	if (!cflags) {
		uwsgi_log("unable to find cflags\n");
		_exit(1);
	}
	if (strlen(cflags) == 0) {
		uwsgi_log("invalid cflags\n");
		_exit(1);
	}

	if (setenv("UWSGI_PLUGINS_BUILDER_CFLAGS", cflags, 1)) {
		uwsgi_error("uwsgi_build_plugin()/setenv()");
		_exit(1);
	}
	
	// now run the python script
	char *argv[6];

	argv[0] = getenv("PYTHON");
	if (!argv[0]) argv[0] = "python";

	argv[1] = UWSGI_BUILD_DIR "/uwsgiconfig.py";
	argv[2] = "--extra-plugin";
	char *space = strchr(directory, ' ');
	if (space) {
		*space = 0;
		argv[3] = directory;
                argv[4] = space+1;
		argv[5] = NULL;
	}
	else {
		argv[3] = directory;
		argv[4] = NULL;
	}

	execvp(argv[0], argv);
	// never here...	
	_exit(1);
}
