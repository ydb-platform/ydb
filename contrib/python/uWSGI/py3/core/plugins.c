#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

#ifdef UWSGI_ELF
static void uwsgi_plugin_parse_section(char *filename) {
	size_t s_len = 0;
	char *buf = uwsgi_elf_section(filename, "uwsgi", &s_len);
	if (buf) {
		char *ctx = NULL;
		char *p = strtok_r(buf, "\n", &ctx);
		while (p) {
			char *equal = strchr(p, '=');
			if (equal) {
				*equal = 0;
				if (!strcmp(p, "requires")) {
					if (!plugin_already_loaded(equal+1)) {	
						uwsgi_load_plugin(-1, equal + 1, NULL);
					}
				}
			}
			p = strtok_r(NULL, "\n", &ctx);
		}
		free(buf);
	}
}
#endif

struct uwsgi_plugin *uwsgi_plugin_get(const char *plugin) {
	int i;

	for (i = 0; i < 256; i++) {
		if (uwsgi.p[i]->name) {
			if (!strcmp(plugin, uwsgi.p[i]->name)) {
#ifdef UWSGI_DEBUG
				uwsgi_log("%s plugin already available\n", plugin);
#endif
				return uwsgi.p[i];
			}
		}
		if (uwsgi.p[i]->alias) {
			if (!strcmp(plugin, uwsgi.p[i]->alias)) {
#ifdef UWSGI_DEBUG
				uwsgi_log("%s plugin already available\n", plugin);
#endif
				return uwsgi.p[i];
			}
		}
	}

	for (i = 0; i < uwsgi.gp_cnt; i++) {

		if (uwsgi.gp[i]->name) {
			if (!strcmp(plugin, uwsgi.gp[i]->name)) {
#ifdef UWSGI_DEBUG
				uwsgi_log("%s plugin already available\n", plugin);
#endif
				return uwsgi.p[i];
			}
		}
		if (uwsgi.gp[i]->alias) {
			if (!strcmp(plugin, uwsgi.gp[i]->alias)) {
#ifdef UWSGI_DEBUG
				uwsgi_log("%s plugin already available\n", plugin);
#endif
				return uwsgi.p[i];
			}
		}
	}

	return NULL;
}

int plugin_already_loaded(const char *plugin) {
	struct uwsgi_plugin *up = uwsgi_plugin_get(plugin);
	if (up) return 1;
	return 0;
}


void *uwsgi_load_plugin(int modifier, char *plugin, char *has_option) {

	void *plugin_handle = NULL;
	char *plugin_abs_path = NULL;
	char *plugin_filename = NULL;

	int need_free = 0;
	char *plugin_name = uwsgi_strip(uwsgi_str(plugin));
	char *plugin_symbol_name_start;

	struct uwsgi_plugin *up;
	char linkpath_buf[1024], linkpath[1024];
	int linkpath_size;

	char *colon = strchr(plugin_name, ':');
	if (colon) {
		colon[0] = 0;
		modifier = atoi(plugin_name);
		plugin_name = colon + 1;
		colon[0] = ':';
	}

	char *init_func = strchr(plugin_name, '|');
	if (init_func) {
		init_func[0] = 0;
		init_func++;	
	}

	if (!uwsgi_endswith(plugin_name, "_plugin.so")) {
		plugin_name = uwsgi_concat2(plugin_name, "_plugin.so");
		need_free = 1;
	}

	plugin_symbol_name_start = plugin_name;

	// step 1: check for absolute plugin (stop if it fails)
	if (strchr(plugin_name, '/')) {
#ifdef UWSGI_ELF
		uwsgi_plugin_parse_section(plugin_name);
#endif
		plugin_handle = dlopen(plugin_name, RTLD_NOW | RTLD_GLOBAL);
		if (!plugin_handle) {
			if (!has_option)
				uwsgi_log("%s\n", dlerror());
			goto end;
		}
		plugin_symbol_name_start = uwsgi_get_last_char(plugin_name, '/');
		plugin_symbol_name_start++;
		plugin_abs_path = plugin_name;
		goto success;
	}

	// step dir, check for user-supplied plugins directory
	struct uwsgi_string_list *pdir = uwsgi.plugins_dir;
	while (pdir) {
		plugin_filename = uwsgi_concat3(pdir->value, "/", plugin_name);
#ifdef UWSGI_ELF
		uwsgi_plugin_parse_section(plugin_filename);
#endif
		plugin_handle = dlopen(plugin_filename, RTLD_NOW | RTLD_GLOBAL);
		if (plugin_handle) {
			plugin_abs_path = plugin_filename;
			//free(plugin_filename);
			goto success;
		}
		free(plugin_filename);
		plugin_filename = NULL;
		pdir = pdir->next;
	}

	// last step: search in compile-time plugin_dir
	if (!plugin_handle) {
		plugin_filename = uwsgi_concat3(UWSGI_PLUGIN_DIR, "/", plugin_name);
#ifdef UWSGI_ELF
		uwsgi_plugin_parse_section(plugin_filename);
#endif
		plugin_handle = dlopen(plugin_filename, RTLD_NOW | RTLD_GLOBAL);
		plugin_abs_path = plugin_filename;
		//free(plugin_filename);
	}

success:
	if (!plugin_handle) {
		if (!has_option)
			uwsgi_log("!!! UNABLE to load uWSGI plugin: %s !!!\n", dlerror());
	}
	else {
		if (init_func) {
			void (*plugin_init_func)() = dlsym(plugin_handle, init_func);
			if (plugin_init_func) {
				plugin_init_func();
			}
		}
		char *plugin_entry_symbol = uwsgi_concat2n(plugin_symbol_name_start, strlen(plugin_symbol_name_start) - 3, "", 0);
		up = dlsym(plugin_handle, plugin_entry_symbol);
		if (!up) {
			// is it a link ?
			memset(linkpath_buf, 0, 1024);
			memset(linkpath, 0, 1024);
			if ((linkpath_size = readlink(plugin_abs_path, linkpath_buf, 1023)) > 0) {
				do {
					linkpath_buf[linkpath_size] = '\0';
					strncpy(linkpath, linkpath_buf, linkpath_size + 1);
				} while ((linkpath_size = readlink(linkpath, linkpath_buf, 1023)) > 0);
#ifdef UWSGI_DEBUG
				uwsgi_log("%s\n", linkpath);
#endif
				free(plugin_entry_symbol);
				char *slash = uwsgi_get_last_char(linkpath, '/');
				if (!slash) {
					slash = linkpath;
				}
				else {
					slash++;
				}
				plugin_entry_symbol = uwsgi_concat2n(slash, strlen(slash) - 3, "", 0);
				up = dlsym(plugin_handle, plugin_entry_symbol);
			}
		}
		if (up) {
			if (!up->name) {
				uwsgi_log("the loaded plugin (%s) has no .name attribute\n", plugin_name);
				if (dlclose(plugin_handle)) {
					uwsgi_error("dlclose()");
				}
				if (need_free)
					free(plugin_name);
				if (plugin_filename)
					free(plugin_filename);
				free(plugin_entry_symbol);
				return NULL;
			}
			if (plugin_already_loaded(up->name)) {
				if (dlclose(plugin_handle)) {
					uwsgi_error("dlclose()");
				}
				if (need_free)
					free(plugin_name);
				if (plugin_filename)
					free(plugin_filename);
				free(plugin_entry_symbol);
				return NULL;
			}
			if (has_option) {
				struct uwsgi_option *op = up->options;
				int found = 0;
				while (op && op->name) {
					if (!strcmp(has_option, op->name)) {
						found = 1;
						break;
					}
					op++;
				}
				if (!found) {
					if (dlclose(plugin_handle)) {
						uwsgi_error("dlclose()");
					}
					if (need_free)
						free(plugin_name);
					if (plugin_filename)
						free(plugin_filename);
					free(plugin_entry_symbol);
					return NULL;
				}

			}
			if (modifier != -1) {
				fill_plugin_table(modifier, up);
				up->modifier1 = modifier;
			}
			else {
				fill_plugin_table(up->modifier1, up);
			}
			if (need_free)
				free(plugin_name);
			if (plugin_filename)
				free(plugin_filename);
			free(plugin_entry_symbol);

			if (up->on_load)
				up->on_load();
			return plugin_handle;
		}
		if (!has_option)
			uwsgi_log("%s\n", dlerror());
	}

end:
	if (need_free)
		free(plugin_name);
	if (plugin_filename)
		free(plugin_filename);

	return NULL;
}

int uwsgi_try_autoload(char *option) {
	DIR *d;
	struct dirent *dp;
	// step dir, check for user-supplied plugins directory
	struct uwsgi_string_list *pdir = uwsgi.plugins_dir;
	while (pdir) {
		d = opendir(pdir->value);
		if (d) {
			while ((dp = readdir(d)) != NULL) {
				if (uwsgi_endswith(dp->d_name, "_plugin.so")) {
					char *filename = uwsgi_concat3(pdir->value, "/", dp->d_name);
					if (uwsgi_load_plugin(-1, filename, option)) {
						uwsgi_log("option \"%s\" found in plugin %s\n", option, filename);
						free(filename);
						closedir(d);
						// add new options
						build_options();
						return 1;
					}
					free(filename);
				}
			}
			closedir(d);
		}
		pdir = pdir->next;
	}

	// last step: search in compile-time plugin_dir
	d = opendir(UWSGI_PLUGIN_DIR);
	if (!d)
		return 0;

	while ((dp = readdir(d)) != NULL) {
		if (uwsgi_endswith(dp->d_name, "_plugin.so")) {
			char *filename = uwsgi_concat3(UWSGI_PLUGIN_DIR, "/", dp->d_name);
			if (uwsgi_load_plugin(-1, filename, option)) {
				uwsgi_log("option \"%s\" found in plugin %s\n", option, filename);
				free(filename);
				closedir(d);
				// add new options
				build_options();
				return 1;
			}
			free(filename);
		}
	}

	closedir(d);

	return 0;

}
