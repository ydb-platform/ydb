#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*
   ini file must be read ALL into memory.
   This memory must not be freed for all the server lifecycle
   */

static char *last_file = NULL;

void ini_rstrip(char *line) {

	off_t i;

	for (i = strlen(line) - 1; i >= 0; i--) {
		if (line[i] == ' ' || line[i] == '\t' || line[i] == '\r') {
			line[i] = 0;
			continue;
		}
		break;
	}
}

char *ini_lstrip(char *line) {

	off_t i;
	char *ptr = line;

	for (i = 0; i < (int) strlen(line); i++) {
		if (line[i] == ' ' || line[i] == '\t' || line[i] == '\r') {
			ptr++;
			continue;
		}
		break;
	}

	return ptr;
}

char *ini_get_key(char *key) {

	off_t i;
	char *ptr = key;

	for (i = 0; i < (int) strlen(key); i++) {
		ptr++;
		if (key[i] == '=') {
			key[i] = 0;
			return ptr;
		}
	}

	return ptr;
}

char *ini_get_line(char *ini, size_t size) {

	size_t i;
	char *ptr = ini;

	for (i = 0; i < size; i++) {
		ptr++;
		if (ini[i] == '\n') {
			ini[i] = 0;
			return ptr;
		}
	}

	// check if it is a stupid file without \n at the end
	if (ptr > ini) {
		return ptr;
	}

	return NULL;

}

void uwsgi_ini_config(char *file, char *magic_table[]) {

	size_t len = 0;
	char *ini;

	char *ini_line;

	char *section = "";
	char *key;
	char *val;

	char *section_asked = "uwsgi";
	char *colon;
	int got_section = 0;


	if (uwsgi_check_scheme(file)) {
		colon = uwsgi_get_last_char(file, '/');
		colon = uwsgi_get_last_char(colon, ':');
	}
	else {
		colon = uwsgi_get_last_char(file, ':');
	}

	if (colon) {
		colon[0] = 0;
		if (colon[1] != 0) {
			section_asked = colon + 1;
		}

		if (colon == file) {
			file = last_file;
		}
	}

	if (file[0] != 0 && file != last_file) {
		uwsgi_log_initial("[uWSGI] getting INI configuration from %s\n", file);
	}

	ini = uwsgi_open_and_read(file, &len, 1, magic_table);
	if (file != last_file) {
		if (last_file) {
			free(last_file);
		}
		last_file = uwsgi_str(file);
	}

	while (len) {
		ini_line = ini_get_line(ini, len);
		if (ini_line == NULL) {
			break;
		}

		// skip empty line
		key = ini_lstrip(ini);
		ini_rstrip(key);
		if (key[0] != 0) {
			if (key[0] == '[') {
				section = key + 1;
				section[strlen(section) - 1] = 0;
			}
			else if (key[0] == ';' || key[0] == '#') {
				// this is a comment
			}
			else {
				// val is always valid, but (obviously) can be ignored
				val = ini_get_key(key);

				if (!strcmp(section, section_asked)) {
					got_section = 1;
					ini_rstrip(key);
					val = ini_lstrip(val);
					ini_rstrip(val);
					add_exported_option((char *) key, val, 0);
				}
			}
		}


		len -= (ini_line - ini);
		ini += (ini_line - ini);

	}

	if (!got_section) {
		uwsgi_log("*** WARNING: Can't find section \"%s\" in INI configuration file %s ***\n", section_asked, file);
	}

	if (colon) {
		colon[0] = ':';
	}


}
