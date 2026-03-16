#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	pluggable configuration system

*/

extern struct uwsgi_server uwsgi;

struct uwsgi_configurator *uwsgi_register_configurator(char *name, void (*func)(char *, char **)) {
	struct uwsgi_configurator *old_uc = NULL,*uc = uwsgi.configurators;
        while(uc) {
                if (!strcmp(uc->name, name)) {
                        return uc;
                }
                old_uc = uc;
                uc = uc->next;
        }

        uc = uwsgi_calloc(sizeof(struct uwsgi_configurator));
        uc->name = name;
        uc->func = func;

        if (old_uc) {
                old_uc->next = uc;
        }
        else {
                uwsgi.configurators = uc;
        }

        return uc;
}

int uwsgi_logic_opt_if_exists(char *key, char *value) {

        if (uwsgi_file_exists(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_not_exists(char *key, char *value) {

        if (!uwsgi_file_exists(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}


int uwsgi_logic_opt_for(char *key, char *value) {

        char *p, *ctx = NULL;
	uwsgi_foreach_token(uwsgi.logic_opt_data, " ", p, ctx) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", p), 0);
        }

        return 1;
}

int uwsgi_logic_opt_for_glob(char *key, char *value) {

        glob_t g;
        int i;
        if (glob(uwsgi.logic_opt_data, GLOB_MARK | GLOB_NOCHECK, NULL, &g)) {
                uwsgi_error("uwsgi_logic_opt_for_glob()");
                return 0;
        }

        for (i = 0; i < (int) g.gl_pathc; i++) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", g.gl_pathv[i]), 0);
        }

        globfree(&g);

        return 1;
}

int uwsgi_logic_opt_for_readline(char *key, char *value) {

	char line[1024];

        FILE *fh = fopen(uwsgi.logic_opt_data, "r");
        if (fh) {
                while (fgets(line, 1024, fh)) {
			add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi_chomp(uwsgi_str(line))), 0);
                }
                fclose(fh);
                return 1;
        }
        uwsgi_error_open(uwsgi.logic_opt_data);
	return 0;
}

int uwsgi_logic_opt_for_times(char *key, char *value) {

        int num = atoi(uwsgi.logic_opt_data);
        int i;
        char str_num[11];

        for (i = 1; i <= num; i++) {
                int ret = uwsgi_num2str2(i, str_num);
                // security check
                if (ret < 0 || ret > 11) {
                        exit(1);
                }
                add_exported_option(key, uwsgi_substitute(value, "%(_)", str_num), 0);
        }

        return 1;
}


int uwsgi_logic_opt_if_opt(char *key, char *value) {

        // check for env-value syntax
        char *equal = strchr(uwsgi.logic_opt_data, '=');
        if (equal)
                *equal = 0;

        char *p = uwsgi_get_exported_opt(uwsgi.logic_opt_data);
        if (equal)
                *equal = '=';

        if (p) {
                if (equal) {
                        if (strcmp(equal + 1, p))
                                return 0;
                }
                add_exported_option(key, uwsgi_substitute(value, "%(_)", p), 0);
                return 1;
        }

        return 0;
}


int uwsgi_logic_opt_if_not_opt(char *key, char *value) {

        // check for env-value syntax
        char *equal = strchr(uwsgi.logic_opt_data, '=');
        if (equal)
                *equal = 0;

        char *p = uwsgi_get_exported_opt(uwsgi.logic_opt_data);
        if (equal)
                *equal = '=';

        if (p) {
                if (equal) {
                        if (!strcmp(equal + 1, p))
                                return 0;
                }
                else {
                        return 0;
                }
        }

        add_exported_option(key, uwsgi_substitute(value, "%(_)", p), 0);
        return 1;
}



int uwsgi_logic_opt_if_env(char *key, char *value) {

        // check for env-value syntax
        char *equal = strchr(uwsgi.logic_opt_data, '=');
        if (equal)
                *equal = 0;

        char *p = getenv(uwsgi.logic_opt_data);
        if (equal)
                *equal = '=';

        if (p) {
                if (equal) {
                        if (strcmp(equal + 1, p))
                                return 0;
                }
                add_exported_option(key, uwsgi_substitute(value, "%(_)", p), 0);
                return 1;
        }

        return 0;
}


int uwsgi_logic_opt_if_not_env(char *key, char *value) {

        // check for env-value syntax
        char *equal = strchr(uwsgi.logic_opt_data, '=');
        if (equal)
                *equal = 0;

        char *p = getenv(uwsgi.logic_opt_data);
        if (equal)
                *equal = '=';

        if (p) {
                if (equal) {
                        if (!strcmp(equal + 1, p))
                                return 0;
                }
                else {
                        return 0;
                }
        }

        add_exported_option(key, uwsgi_substitute(value, "%(_)", p), 0);
        return 1;
}

int uwsgi_logic_opt_if_reload(char *key, char *value) {
        if (uwsgi.is_a_reload) {
                add_exported_option(key, value, 0);
                return 1;
        }
        return 0;
}

int uwsgi_logic_opt_if_not_reload(char *key, char *value) {
        if (!uwsgi.is_a_reload) {
                add_exported_option(key, value, 0);
                return 1;
        }
        return 0;
}

int uwsgi_logic_opt_if_file(char *key, char *value) {

        if (uwsgi_is_file(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_not_file(char *key, char *value) {

        if (!uwsgi_is_file(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_dir(char *key, char *value) {

        if (uwsgi_is_dir(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_not_dir(char *key, char *value) {

        if (!uwsgi_is_dir(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}



int uwsgi_logic_opt_if_plugin(char *key, char *value) {

        if (plugin_already_loaded(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_not_plugin(char *key, char *value) {

        if (!plugin_already_loaded(uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_hostname(char *key, char *value) {

        if (!strcmp(uwsgi.hostname, uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

int uwsgi_logic_opt_if_not_hostname(char *key, char *value) {

        if (strcmp(uwsgi.hostname, uwsgi.logic_opt_data)) {
                add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
                return 1;
        }

        return 0;
}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
int uwsgi_logic_opt_if_hostname_match(char *key, char *value) {
	if (uwsgi_regexp_match_pattern(uwsgi.logic_opt_data, uwsgi.hostname)) {
		add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
		return 1;
	}
	return 0;
}

int uwsgi_logic_opt_if_not_hostname_match(char *key, char *value) {
	if (!uwsgi_regexp_match_pattern(uwsgi.logic_opt_data, uwsgi.hostname)) {
		add_exported_option(key, uwsgi_substitute(value, "%(_)", uwsgi.logic_opt_data), 0);
		return 1;
	}
	return 0;
}
#endif

int uwsgi_count_options(struct uwsgi_option *uopt) {

        struct uwsgi_option *aopt;
        int count = 0;

        while ((aopt = uopt)) {
                if (!aopt->name)
                        break;
                count++;
                uopt++;
        }

        return count;
}

int uwsgi_opt_exists(char *name) {
	struct uwsgi_option *op = uwsgi.options;
	while (op->name) {
		if (!strcmp(name, op->name)) return 1;
		op++;
	}
	return 0;	
}

/*
	avoid loops here !!!
*/
struct uwsgi_option *uwsgi_opt_get(char *name) {
        struct uwsgi_option *op;
	int round = 0;
retry:
	round++;
	if (round > 2) goto end;
	op = uwsgi.options;

        while (op->name) {
                if (!strcmp(name, op->name)) {
                        return op;
                }
                op++;
        }

	if (uwsgi.autoload) {
		if (uwsgi_try_autoload(name)) goto retry;
	}

end:
	if (uwsgi.strict) {
                uwsgi_log("[strict-mode] unknown config directive: %s\n", name);
                exit(1);
        }

        return NULL;
}



void add_exported_option(char *key, char *value, int configured) {
	add_exported_option_do(key, value, configured, 0);
}

void add_exported_option_do(char *key, char *value, int configured, int placeholder_only) {

	struct uwsgi_string_list *blacklist = uwsgi.blacklist;
	struct uwsgi_string_list *whitelist = uwsgi.whitelist;

	while (blacklist) {
		if (!strcmp(key, blacklist->value)) {
			uwsgi_log("uWSGI error: forbidden option \"%s\" (by blacklist)\n", key);
			exit(1);
		}
		blacklist = blacklist->next;
	}

	if (whitelist) {
		int allowed = 0;
		while (whitelist) {
			if (!strcmp(key, whitelist->value)) {
				allowed = 1;
				break;
			}
			whitelist = whitelist->next;
		}
		if (!allowed) {
			uwsgi_log("uWSGI error: forbidden option \"%s\" (by whitelist)\n", key);
			exit(1);
		}
	}

	if (uwsgi.blacklist_context) {
		if (uwsgi_list_has_str(uwsgi.blacklist_context, key)) {
			uwsgi_log("uWSGI error: forbidden option \"%s\" (by blacklist)\n", key);
			exit(1);
		}
	}

	if (uwsgi.whitelist_context) {
                if (!uwsgi_list_has_str(uwsgi.whitelist_context, key)) {
                        uwsgi_log("uWSGI error: forbidden option \"%s\" (by whitelist)\n", key);
                        exit(1);
                }
        }

	if (uwsgi.logic_opt_running)
		goto add;

	if (!strcmp(key, "end") || !strcmp(key, "endfor") || !strcmp(key, "endif")
		|| !strcmp(key, "end-if") || !strcmp(key, "end-for")) {
		if (uwsgi.logic_opt_data) {
			free(uwsgi.logic_opt_data);
		}
		uwsgi.logic_opt = NULL;
		uwsgi.logic_opt_arg = NULL;
		uwsgi.logic_opt_cycles = 0;
		uwsgi.logic_opt_data = NULL;
	}

	if (uwsgi.logic_opt) {
		if (uwsgi.logic_opt_data) {
			free(uwsgi.logic_opt_data);
		}
		uwsgi.logic_opt_data = uwsgi_str(uwsgi.logic_opt_arg);
		uwsgi.logic_opt_cycles++;
		uwsgi.logic_opt_running = 1;
		uwsgi.logic_opt(key, value);
		uwsgi.logic_opt_running = 0;
		return;
	}

add:

	if (!uwsgi.exported_opts) {
		uwsgi.exported_opts = uwsgi_malloc(sizeof(struct uwsgi_opt *));
	}
	else {
		uwsgi.exported_opts = realloc(uwsgi.exported_opts, sizeof(struct uwsgi_opt *) * (uwsgi.exported_opts_cnt + 1));
		if (!uwsgi.exported_opts) {
			uwsgi_error("realloc()");
			exit(1);
		}
	}

	int id = uwsgi.exported_opts_cnt;
	uwsgi.exported_opts[id] = uwsgi_malloc(sizeof(struct uwsgi_opt));
	uwsgi.exported_opts[id]->key = key;
	uwsgi.exported_opts[id]->value = value;
	uwsgi.exported_opts[id]->configured = configured;
	uwsgi.exported_opts_cnt++;
	uwsgi.dirty_config = 1;

	if (placeholder_only) {
		if (uwsgi_opt_exists(key)) {
			uwsgi_log("you cannot use %s as a placeholder, it is already available as an option\n");
			exit(1);
		}
		uwsgi.exported_opts[id]->configured = 1;
		return;
	}

	struct uwsgi_option *op = uwsgi_opt_get(key);
	if (op) {
		// requires master ?
		if (op->flags & UWSGI_OPT_MASTER) {
			uwsgi.master_process = 1;
		}
		// requires log_master ?
		if (op->flags & UWSGI_OPT_LOG_MASTER) {
			uwsgi.master_process = 1;
			uwsgi.log_master = 1;
		}
		if (op->flags & UWSGI_OPT_REQ_LOG_MASTER) {
			uwsgi.master_process = 1;
			uwsgi.log_master = 1;
			uwsgi.req_log_master = 1;
		}
		// requires threads ?
		if (op->flags & UWSGI_OPT_THREADS) {
			uwsgi.has_threads = 1;
		}
		// requires cheaper mode ?
		if (op->flags & UWSGI_OPT_CHEAPER) {
			uwsgi.cheaper = 1;
		}
		// requires virtualhosting ?
		if (op->flags & UWSGI_OPT_VHOST) {
			uwsgi.vhost = 1;
		}
		// requires memusage ?
		if (op->flags & UWSGI_OPT_MEMORY) {
			uwsgi.force_get_memusage = 1;
		}
		// requires auto procname ?
		if (op->flags & UWSGI_OPT_PROCNAME) {
			uwsgi.auto_procname = 1;
		}
		// requires lazy ?
		if (op->flags & UWSGI_OPT_LAZY) {
			uwsgi.lazy = 1;
		}
		// requires no_initial ?
		if (op->flags & UWSGI_OPT_NO_INITIAL) {
			uwsgi.no_initial_output = 1;
		}
		// requires no_server ?
		if (op->flags & UWSGI_OPT_NO_SERVER) {
			uwsgi.no_server = 1;
		}
		// requires post_buffering ?
		if (op->flags & UWSGI_OPT_POST_BUFFERING) {
			if (!uwsgi.post_buffering)
				uwsgi.post_buffering = 4096;
		}
		// requires building mime dict ?
		if (op->flags & UWSGI_OPT_MIME) {
			uwsgi.build_mime_dict = 1;
		}
		// enable metrics ?
		if (op->flags & UWSGI_OPT_METRICS) {
                        uwsgi.has_metrics = 1;
                }
		// immediate ?
		if (op->flags & UWSGI_OPT_IMMEDIATE) {
			op->func(key, value, op->data);
			uwsgi.exported_opts[id]->configured = 1;
		}
	}
}

void uwsgi_fallback_config() {
	if (uwsgi.fallback_config && uwsgi.last_exit_code == 1) {
		uwsgi_log_verbose("!!! %s (pid: %d) exited with status %d !!!\n", uwsgi.binary_path, (int) getpid(), uwsgi.last_exit_code);
		uwsgi_log_verbose("!!! Fallback config to %s !!!\n", uwsgi.fallback_config);
		char *argv[3];
		argv[0] = uwsgi.binary_path;
		argv[1] = uwsgi.fallback_config;
		argv[2] = NULL;
        	execvp(uwsgi.binary_path, argv);
        	uwsgi_error("execvp()");
        	// never here
	}
}

int uwsgi_manage_opt(char *key, char *value) {

        struct uwsgi_option *op = uwsgi_opt_get(key);
	if (op) {
        	op->func(key, value, op->data);
                return 1;
        }
        return 0;

}

void uwsgi_configure() {

        int i;

        // and now apply the remaining configs
restart:
        for (i = 0; i < uwsgi.exported_opts_cnt; i++) {
                if (uwsgi.exported_opts[i]->configured)
                        continue;
                uwsgi.dirty_config = 0;
                uwsgi.exported_opts[i]->configured = uwsgi_manage_opt(uwsgi.exported_opts[i]->key, uwsgi.exported_opts[i]->value);
		// some option could cause a dirty config tree
                if (uwsgi.dirty_config)
                        goto restart;
        }

}


void uwsgi_opt_custom(char *key, char *value, void *data ) {
        struct uwsgi_custom_option *uco = (struct uwsgi_custom_option *)data;
        size_t i, count = 1;
        size_t value_len = 0;
        if (value)
                value_len = strlen(value);
        off_t pos = 0;
        char **opt_argv;
        char *tmp_val = NULL, *p = NULL;

        // now count the number of args
        for (i = 0; i < value_len; i++) {
                if (value[i] == ' ') {
                        count++;
                }
        }

        // allocate a tmp array
        opt_argv = uwsgi_calloc(sizeof(char *) * count);
        //make a copy of the value;
        if (value_len > 0) {
                tmp_val = uwsgi_str(value);
                // fill the array of options
                char *p, *ctx = NULL;
                uwsgi_foreach_token(tmp_val, " ", p, ctx) {
                        opt_argv[pos] = p;
                        pos++;
                }
        }
        else {
                // no argument specified
                opt_argv[0] = "";
        }

#ifdef UWSGI_DEBUG
        uwsgi_log("found custom option %s with %d args\n", key, count);
#endif

        // now make a copy of the option template
        char *tmp_opt = uwsgi_str(uco->value);
        // split it
        char *ctx = NULL;
        uwsgi_foreach_token(tmp_opt, ";", p, ctx) {
                char *equal = strchr(p, '=');
                if (!equal)
                        goto clear;
                *equal = '\0';

                // build the key
                char *new_key = uwsgi_str(p);
                for (i = 0; i < count; i++) {
                        char *old_key = new_key;
                        char *tmp_num = uwsgi_num2str(i + 1);
                        char *placeholder = uwsgi_concat2((char *) "$", tmp_num);
                        free(tmp_num);
                        new_key = uwsgi_substitute(old_key, placeholder, opt_argv[i]);
                        if (new_key != old_key)
                                free(old_key);
                        free(placeholder);
                }

                // build the value
                char *new_value = uwsgi_str(equal + 1);
                for (i = 0; i < count; i++) {
                        char *old_value = new_value;
                        char *tmp_num = uwsgi_num2str(i + 1);
                        char *placeholder = uwsgi_concat2((char *) "$", tmp_num);
                        free(tmp_num);
                        new_value = uwsgi_substitute(old_value, placeholder, opt_argv[i]);
                        if (new_value != old_value)
                                free(old_value);
                        free(placeholder);
                }
                // ignore return value here
                uwsgi_manage_opt(new_key, new_value);
        }

clear:
        free(tmp_val);
        free(tmp_opt);
        free(opt_argv);

}

char *uwsgi_get_exported_opt(char *key) {

        int i;

        for (i = 0; i < uwsgi.exported_opts_cnt; i++) {
                if (!strcmp(uwsgi.exported_opts[i]->key, key)) {
                        return uwsgi.exported_opts[i]->value;
                }
        }

        return NULL;
}

char *uwsgi_get_optname_by_index(int index) {

        struct uwsgi_option *op = uwsgi.options;

        while (op->name) {
                if (op->shortcut == index) {
                        return op->name;
                }
                op++;
        }

        return NULL;
}

/*

	this works as a pipeline

	processes = 2
	cpu_cores = 8
	foobar = %(processes cpu_cores + 2)

	translate as:

		step1 = proceses cpu_cores = 2 8 = 28 (string concatenation)

		step1 + = step1_apply_func_plus (func token)

		step1_apply_func_plus 2 = 28 + 2 = 30 (math)

*/

char *uwsgi_manage_placeholder(char *key) {
	enum {
		concat = 0,
		sum,
		sub,
		mul,
		div,
	} state;

	state = concat;
	char *current_value = NULL;

	char *space = strchr(key, ' ');
	if (!space) {
		return uwsgi_get_exported_opt(key);
	}
	// let's start the heavy metal here
	char *tmp_value = uwsgi_str(key);
	char *p, *ctx = NULL;
        uwsgi_foreach_token(tmp_value, " ", p, ctx) {
		char *value = NULL;
		if (is_a_number(p)) {
			value = uwsgi_str(p);
		}
		else if (!strcmp(p, "+")) {
			state = sum;
			continue;
		}
		else if (!strcmp(p, "-")) {
			state = sub;
			continue;
		}
		else if (!strcmp(p, "*")) {
			state = mul;
			continue;
		}
		else if (!strcmp(p, "/")) {
			state = div;
			continue;
		}
		else if (!strcmp(p, "++")) {
			if (current_value) {
				int64_t tmp_num = strtoll(current_value, NULL, 10);
				free(current_value);
				current_value = uwsgi_64bit2str(tmp_num+1);
			}
			state = concat;
			continue;
		}
		else if (!strcmp(p, "--")) {
			if (current_value) {
				int64_t tmp_num = strtoll(current_value, NULL, 10);
				free(current_value);
				current_value = uwsgi_64bit2str(tmp_num-1);
			}
			state = concat;
			continue;
		}
		// find the option
		else {
			char *ov = uwsgi_get_exported_opt(p);
			if (!ov) ov = "";
			value = uwsgi_str(ov);
		}

		int64_t arg1n = 0, arg2n = 0;
		char *arg1 = "", *arg2 = "";	

		switch(state) {
			case concat:
				if (current_value) arg1 = current_value;
				if (value) arg2 = value;
				char *ret = uwsgi_concat2(arg1, arg2);
				if (current_value) free(current_value);
				current_value = ret;	
				break;
			case sum:
				if (current_value) arg1n = strtoll(current_value, NULL, 10);
				if (value) arg2n = strtoll(value, NULL, 10);
				if (current_value) free(current_value);
				current_value = uwsgi_64bit2str(arg1n + arg2n);
				break;
			case sub:
				if (current_value) arg1n = strtoll(current_value, NULL, 10);
				if (value) arg2n = strtoll(value, NULL, 10);
				if (current_value) free(current_value);
				current_value = uwsgi_64bit2str(arg1n - arg2n);
				break;
			case mul:
				if (current_value) arg1n = strtoll(current_value, NULL, 10);
				if (value) arg2n = strtoll(value, NULL, 10);
				if (current_value) free(current_value);
				current_value = uwsgi_64bit2str(arg1n * arg2n);
				break;
			case div:
				if (current_value) arg1n = strtoll(current_value, NULL, 10);
				if (value) arg2n = strtoll(value, NULL, 10);
				if (current_value) free(current_value);
				// avoid division by zero
				if (arg2n == 0) {
					current_value = uwsgi_64bit2str(0);
				}
				else {
					current_value = uwsgi_64bit2str(arg1n / arg2n);
				}
				break;
			default:
				break;
		}

		// over engineering
		if (value)
			free(value);

		// reset state to concat
		state = concat;
	}
	free(tmp_value);

	return current_value;
}

void uwsgi_opt_resolve(char *opt, char *value, void *foo) {
        char *equal = strchr(value, '=');
        if (!equal) {
                uwsgi_log("invalid resolve syntax, must be placeholder=domain\n");
                exit(1);
        }
        char *ip = uwsgi_resolve_ip(equal+1);
        if (!ip) {
		uwsgi_log("unable to resolve name %s\n", equal+1);
                uwsgi_error("uwsgi_resolve_ip()");
                exit(1);
        }
        char *new_opt = uwsgi_concat2n(value, (equal-value)+1, ip, strlen(ip));
        uwsgi_opt_set_placeholder(opt, new_opt, (void *) 1);
}
