#include <contrib/python/uWSGI/py2/config.h>
#ifdef UWSGI_YAML

#include "uwsgi.h"

extern struct uwsgi_server uwsgi;


#ifndef UWSGI_LIBYAML
/*
   yaml file must be read ALL into memory.
   This memory must not be freed for all the server lifecycle
   */

void yaml_rstrip(char *line) {

	off_t i;

	for (i = strlen(line) - 1; i >= 0; i--) {
		if (line[i] == ' ' || line[i] == '\t') {
			line[i] = 0;
			continue;
		}
		break;
	}
}

char *yaml_lstrip(char *line) {

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


int yaml_get_depth(char *line) {

	off_t i;
	int depth = 0;

	for (i = 0; i < (int) strlen(line); i++) {
		if (line[i] == ' ') {
			depth++;
			continue;
		}
		else if (line[i] == '\t' || line[i] == '\r') {
			depth += 8;
			continue;
		}
		break;
	}

	return depth;
}

char *yaml_get_line(char *yaml, size_t size) {

	size_t i;
	char *ptr = yaml;
	int comment = 0;

	for (i = 0; i < size; i++) {
		ptr++;
		if (yaml[i] == '#') {
			yaml[i] = 0;
			comment = 1;
		}
		else if (yaml[i] == '\n') {
			yaml[i] = 0;
			return ptr;
		}
		else if (comment) {
			yaml[i] = 0;
		}
	}

	// check if it is a stupid file without \n at the end
	if (ptr > yaml) {
		return ptr;
	}

	return NULL;

}
#else
#include <yaml.h>
#endif

void uwsgi_yaml_config(char *file, char *magic_table[]) {

	size_t len = 0;
	char *yaml;

	int in_uwsgi_section = 0;

	char *key = NULL;
	char *val = NULL;

	char *section_asked = "uwsgi";
	char *colon;

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
	}

	uwsgi_log_initial("[uWSGI] getting YAML configuration from %s\n", file);

	yaml = uwsgi_open_and_read(file, &len, 1, magic_table);

#ifdef UWSGI_LIBYAML
	yaml_parser_t parser;
	yaml_token_t token;
	int status = 0;
	int parsing = 1;

	if (!yaml_parser_initialize(&parser)) {
		uwsgi_log("unable to initialize YAML parser (libyaml)\n");
		exit(1);
	}

	yaml_parser_set_input_string(&parser, (unsigned char *) yaml, (size_t) len - 1);

	while (parsing) {
		if (!yaml_parser_scan(&parser, &token)) {
			uwsgi_log("error parsing YAML file: %s (%c)\n", parser.problem, yaml[parser.problem_offset]);
			exit(1);
		}
		switch (token.type) {
		case YAML_STREAM_END_TOKEN:
			parsing = 0;
			break;
		case YAML_KEY_TOKEN:
			status = 1;
			break;
		case YAML_VALUE_TOKEN:
			status = 2;
			break;
		case YAML_FLOW_SEQUENCE_START_TOKEN:
		case YAML_BLOCK_SEQUENCE_START_TOKEN:
			if (in_uwsgi_section)
				in_uwsgi_section++;
			// fallthrough
		case YAML_FLOW_ENTRY_TOKEN:
		case YAML_BLOCK_ENTRY_TOKEN:
			status = 3;  // inside a sequence
			break;
		case YAML_BLOCK_MAPPING_START_TOKEN:
			if (in_uwsgi_section) {
				in_uwsgi_section++;
				break;
			}
			if (key) {
				if (!strcmp(section_asked, key)) {
					in_uwsgi_section = 1;
				}
			}
			break;
		case YAML_BLOCK_END_TOKEN:
		case YAML_FLOW_SEQUENCE_END_TOKEN:
			if (in_uwsgi_section)
				parsing = !!(--in_uwsgi_section);
			key = NULL;
			status = 0;
			break;
		case YAML_SCALAR_TOKEN:
			if (status == 1) {
				key = (char *) token.data.scalar.value;
			}
			else if (status == 2 || status == 3) {
				val = (char *) token.data.scalar.value;
				if (key && val && in_uwsgi_section) {
					add_exported_option(key, val, 0);
				}

				// If this was the scalar of a value token, forget the state.
				if (status == 2) {
					key = NULL;
					status = 0;
				}
			}
			else {
				uwsgi_log("unsupported YAML token %d in %s block\n", token.type, section_asked);
				parsing = 0;
				break;
			}
			break;
		default:
			status = 0;
		}
	}

#else
	int depth;
	int current_depth = 0;
	char *yaml_line;
	char *section = "";


	while (len) {
		yaml_line = yaml_get_line(yaml, len);
		if (yaml_line == NULL) {
			break;
		}

		// skip empty line
		if (yaml[0] == 0)
			goto next;
		depth = yaml_get_depth(yaml);
		if (depth <= current_depth) {
			current_depth = depth;
			// end the parsing cycle
			if (in_uwsgi_section)
				return;
		}
		else if (depth > current_depth && !in_uwsgi_section) {
			goto next;
		}

		key = yaml_lstrip(yaml);
		// skip empty line
		if (key[0] == 0)
			goto next;

		// skip list and {} defined dict
		if (key[0] == '-' || key[0] == '[' || key[0] == '{') {
			if (in_uwsgi_section)
				return;
			goto next;
		}

		if (!in_uwsgi_section) {
			section = strchr(key, ':');
			if (!section)
				goto next;
			section[0] = 0;
			if (!strcmp(key, section_asked)) {
				in_uwsgi_section = 1;
			}
		}
		else {
			// get dict value       
			val = strstr(key, ": ");
			if (!val) {
				val = strstr(key, ":\t");
			}
			if (!val)
				return;
			// get the right key
			val[0] = 0;
			// yeah overengeneering....
			yaml_rstrip(key);

			val = yaml_lstrip(val + 2);
			yaml_rstrip(val);

			//uwsgi_log("YAML: %s = %s\n", key, val);

			add_exported_option((char *) key, val, 0);
		}
next:
		len -= (yaml_line - yaml);
		yaml += (yaml_line - yaml);

	}
#endif

	if (colon) colon[0] = ':';

}

#endif
