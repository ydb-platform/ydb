#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	advanced (pluggable) hooks

	they are executed before the other hooks, and can be extended by plugins

	if a plugin tries to register an hook with a name already available in the list, its function
	will be overridden

*/

struct uwsgi_hook *uwsgi_hook_by_name(char *name) {
	struct uwsgi_hook *uh = uwsgi.hooks;
	while(uh) {
		if (!strcmp(uh->name, name)) {
			return uh;
		}
		uh = uh->next;
	}
	return NULL;
}

void uwsgi_register_hook(char *name, int (*func)(char *)) {
	struct uwsgi_hook *old_uh = NULL, *uh = uwsgi.hooks;
        while(uh) {
                if (!strcmp(uh->name, name)) {
                        uh->func = func;
			return;
                }
		old_uh = uh;
		uh = uh->next;
        }

	uh = uwsgi_calloc(sizeof(struct uwsgi_hook));
	uh->name = name;
	uh->func = func;

	if (old_uh) {
		old_uh->next = uh;
	}
	else {
		uwsgi.hooks = uh;
	}
}

static int uwsgi_hook_alarm(char *arg) {
	char *space = strchr(arg,' ');
	if (!space) {
		uwsgi_log("invalid alarm hook syntax, must be: <alarm> <msg>\n");
		return -1;
	}
	*space = 0;
	uwsgi_alarm_trigger(arg, space+1,  strlen(space+1));
	*space = ' ';
	return 0;
}

static int uwsgi_hook_chdir(char *arg) {
	int ret = chdir(arg);
	if (ret) {
		uwsgi_error("uwsgi_hook_chdir()");
	}
	return ret;
}

static int uwsgi_hook_mkdir(char *arg) {
        int ret = mkdir(arg, 0777);
        if (ret) {
                uwsgi_error("uwsgi_hook_mkdir()");
        }
        return ret;
}

static int uwsgi_hook_putenv(char *arg) {
        int ret = putenv(arg);
        if (ret) {
                uwsgi_error("uwsgi_hook_putenv()");
        }
        return ret;
}

static int uwsgi_hook_exec(char *arg) {
	int ret = uwsgi_run_command_and_wait(NULL, arg);
	if (ret != 0) {
        	uwsgi_log("command \"%s\" exited with non-zero code: %d\n", arg, ret);
        }
	return ret;
}

static int uwsgi_hook_safeexec(char *arg) {
        int ret = uwsgi_run_command_and_wait(NULL, arg);
        if (ret != 0) {
                uwsgi_log("command \"%s\" exited with non-zero code: %d\n", arg, ret);
        }
        return 0;
}

static int uwsgi_hook_exit(char *arg) {
	int exit_code = 0;
	if (strlen(arg) > 1) {
		exit_code = atoi(arg);
	}
	exit(exit_code);
}

static int uwsgi_hook_print(char *arg) {
	char *line = uwsgi_concat2(arg, "\n");
	uwsgi_log(line);
	free(line);
	return 0;
}

static int uwsgi_hook_unlink(char *arg) {
	int ret = unlink(arg);
	if (ret) {
		uwsgi_error("uwsgi_hook_unlink()/unlink()");
	}
	return ret;
}

static int uwsgi_hook_writefifo(char *arg) {
	char *space = strchr(arg, ' ');
	if (!space) {
		uwsgi_log("invalid hook writefifo syntax, must be: <file> <string>\n");
		return -1;
	}
	*space = 0;
	int fd = open(arg, O_WRONLY|O_NONBLOCK);
	if (fd < 0) {
		uwsgi_error_open(arg);
		*space = ' ';
		if (errno == ENODEV) return 0;
#ifdef ENXIO
		if (errno == ENXIO) return 0;
#endif
		return -1;
	}
	*space = ' ';
	size_t l = strlen(space+1);
	if (write(fd, space+1, l) != (ssize_t) l) {
		uwsgi_error("uwsgi_hook_writefifo()/write()");
		close(fd);
		return -1;
	}
	close(fd);
        return 0;
}

static int uwsgi_hook_write(char *arg) {
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook write syntax, must be: <file> <string>\n");
                return -1;
        }
        *space = 0;
        int fd = open(arg, O_WRONLY|O_CREAT|O_TRUNC, 0666);
        if (fd < 0) {
                uwsgi_error_open(arg);
                *space = ' ';
                return -1;
        }
        *space = ' ';
        size_t l = strlen(space+1);
        if (write(fd, space+1, l) != (ssize_t) l) {
                uwsgi_error("uwsgi_hook_write()/write()");
                close(fd);
                return -1;
        }
        close(fd);
        return 0;
}

static int uwsgi_hook_creat(char *arg) {
        int fd = open(arg, O_WRONLY|O_CREAT|O_TRUNC, 0666);
        if (fd < 0) {
                uwsgi_error_open(arg);
                return -1;
        }
        close(fd);
        return 0;
}


static int uwsgi_hook_append(char *arg) {
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook append syntax, must be: <file> <string>\n");
                return -1;
        }
        *space = 0;
        int fd = open(arg, O_WRONLY|O_CREAT|O_APPEND, 0666);
        if (fd < 0) {
                uwsgi_error_open(arg);
                *space = ' ';
                return -1;
        }
        *space = ' ';
        size_t l = strlen(space+1);
        if (write(fd, space+1, l) != (ssize_t) l) {
                uwsgi_error("uwsgi_hook_append()/write()");
                close(fd);
                return -1;
        }
        close(fd);
        return 0;
}


static int uwsgi_hook_writen(char *arg) {
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook writen syntax, must be: <file> <string>\n");
                return -1;
        }
        *space = 0;
        int fd = open(arg, O_WRONLY|O_CREAT|O_TRUNC, 0666);
        if (fd < 0) {
                uwsgi_error_open(arg);
                *space = ' ';
                return -1;
        }
        *space = ' ';
        size_t l = strlen(space+1);
	char *buf = uwsgi_malloc(l + 1);
	memcpy(buf, space+1, l);
	buf[l] = '\n';
        if (write(fd, buf, l+1) != (ssize_t) (l+1)) {
                uwsgi_error("uwsgi_hook_writen()/write()");
		free(buf);
                close(fd);
                return -1;
        }
	free(buf);
        close(fd);
        return 0;
}

static int uwsgi_hook_appendn(char *arg) {
        char *space = strchr(arg, ' ');
	if (space)
        	*space = 0;
        int fd = open(arg, O_WRONLY|O_CREAT|O_APPEND, 0666);
        if (fd < 0) {
                uwsgi_error_open(arg);
		if (space)
                	*space = ' ';
                return -1;
        }
	if (!space) {
		// simple newline
		if (write(fd, "\n", 1) != 1) {
                	uwsgi_error("uwsgi_hook_appendn()/write()");
			close(fd);
			return -1;
		}
		close(fd);
		return 0;
	}

        *space = ' ';
        size_t l = strlen(space+1);
        char *buf = uwsgi_malloc(l + 1);
	memcpy(buf, space+1, l);
        buf[l] = '\n';
        if (write(fd, buf, l+1) != (ssize_t) (l+1)) {
                uwsgi_error("uwsgi_hook_appendn()/write()");
                free(buf);
                close(fd);
                return -1;
        }
        free(buf);
        close(fd);
        return 0;
}



static int uwsgi_hook_chmod(char *arg) {
	char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook chmod syntax, must be: <file> <mode>\n");
                return -1;
        }
        *space = 0;
	int error = 0;
	mode_t mask = uwsgi_mode_t(space+1, &error);
	if (error) {
		uwsgi_log("invalid hook chmod mask: %s\n", space+1); 
		*space = ' ';
		return -1;
	}

	int ret = chmod(arg, mask);
	*space = ' ';
	if (ret) {
		uwsgi_error("uwsgi_hook_chmod()/chmod()");
	}
	return ret;
}

static int uwsgi_hook_sticky(char *arg) {
	struct stat st;
	if (stat(arg, &st)) {
                uwsgi_error("uwsgi_hook_sticky()/stat()");
		return -1;
	}
        if (chmod(arg, st.st_mode | S_ISVTX)) {
                uwsgi_error("uwsgi_hook_sticky()/chmod()");
		return -1;
        }
        return 0;
}


static int uwsgi_hook_chown(char *arg) {
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook chown syntax, must be: <file> <uid> <gid>\n");
                return -1;
        }
        *space = 0;

	char *space2 = strchr(space+1, ' ');
	if (!space2) {
		*space = ' ';
                uwsgi_log("invalid hook chown syntax, must be: <file> <uid> <gid>\n");
                return -1;
	}
	*space2 = 0;

	struct passwd *pw = getpwnam(space+1);
	if (!pw) {
		uwsgi_log("unable to find uid %s\n", space+1);
		*space = ' ';
		*space2 = ' ';
		return -1;
	}

	struct group *gr = getgrnam(space2+1);
	if (!gr) {
                uwsgi_log("unable to find gid %s\n", space2+1);
                *space = ' ';
                *space2 = ' ';
                return -1;
        }
        int ret = chown(arg, pw->pw_uid, gr->gr_gid);
        *space = ' ';
        *space2 = ' ';
        if (ret) {
                uwsgi_error("uwsgi_hook_chown()/chown)");
        }
        return ret;
}

static int uwsgi_hook_chown2(char *arg) {
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook chown2 syntax, must be: <file> <uid> <gid>\n");
                return -1;
        }
        *space = 0;

        char *space2 = strchr(space+1, ' ');
        if (!space2) {
                *space = ' ';
                uwsgi_log("invalid hook chown2 syntax, must be: <file> <uid> <gid>\n");
                return -1;
        }
        *space2 = 0;

	if (!is_a_number(space+1)) {
		uwsgi_log("invalid hook chown2 syntax, uid must be a number\n");
		*space = ' ';
		*space2 = ' ';
		return -1;
	}

	if (!is_a_number(space2+1)) {
                uwsgi_log("invalid hook chown2 syntax, gid must be a number\n");
                *space = ' ';
                *space2 = ' ';
                return -1;
        }

        int ret = chown(arg, atoi(space+1), atoi(space2+1));
        *space = ' ';
        *space2 = ' ';
        if (ret) {
                uwsgi_error("uwsgi_hook_chown2()/chown)");
        }
        return ret;
}


#ifdef __sun__
extern int sethostname(char *, int);
#endif
static int uwsgi_hook_hostname(char *arg) {
#ifdef __CYGWIN__
	return -1;
#else
	return sethostname(arg, strlen(arg));
#endif
}

static int uwsgi_hook_unix_signal(char *arg) {
	char *space = strchr(arg, ' ');
	if (!space) {
		uwsgi_log("invalid unix_signal syntax, must be <signum> <func>\n");
		return -1;
	}
	*space = 0;
	int signum = atoi(arg);
	*space = ' ';
	void (*func)(int) = dlsym(RTLD_DEFAULT, space+1);
	if (!func) {
		uwsgi_log("unable to find function \"%s\"\n", space+1);
		return -1;
	}
	uwsgi_unix_signal(signum, func);
	return 0;
}


static int uwsgi_hook_callint(char *arg) {
        char *space = strchr(arg, ' ');
        if (space) {
                *space = 0;
                int num = atoi(space+1);
                void (*func)(int) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                	uwsgi_log("unable to call function \"%s(%d)\"\n", arg, num);
                        *space = ' ';
                        return -1;
		}
                *space = ' ';
                func(num);
        }
        else {
                void (*func)(void) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                        uwsgi_log("unable to call function \"%s\"\n", arg);
                        return -1;
                }
                func();
        }
        return 0;
}


static int uwsgi_hook_call(char *arg) {
	char *space = strchr(arg, ' ');
	if (space) {
		*space = 0;
		void (*func)(char *) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                	uwsgi_log("unable to call function \"%s(%s)\"\n", arg, space + 1);
			*space = ' ';
			return -1;
		}
		*space = ' ';
                func(space + 1);
	}
	else {
		void (*func)(void) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                	uwsgi_log("unable to call function \"%s\"\n", arg);
			return -1;
		}
                func();
	}
	return 0;
}

static int uwsgi_hook_callintret(char *arg) {
        char *space = strchr(arg, ' ');
        if (space) {
                *space = 0;
                int num = atoi(space+1);
                int (*func)(int) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                        uwsgi_log("unable to call function \"%s(%d)\"\n", arg, num);
                        *space = ' ';
                        return -1;
                }
                *space = ' ';
                return func(num);
        }
        int (*func)(void) = dlsym(RTLD_DEFAULT, arg);
        if (!func) {
        	uwsgi_log("unable to call function \"%s\"\n", arg);
                return -1;
        }
	return func();
}


static int uwsgi_hook_callret(char *arg) {
        char *space = strchr(arg, ' ');
        if (space) {
                *space = 0;
                int (*func)(char *) = dlsym(RTLD_DEFAULT, arg);
                if (!func) {
                        uwsgi_log("unable to call function \"%s(%s)\"\n", arg, space + 1);
                        *space = ' ';
                        return -1;
                }
                *space = ' ';
                return func(space + 1);
        }
        int (*func)(void) = dlsym(RTLD_DEFAULT, arg);
        if (!func) {
        	uwsgi_log("unable to call function \"%s\"\n", arg);
                return -1;
        }
        return func();
}

static int uwsgi_hook_rpc(char *arg) {

	int ret = -1;
	size_t i, argc = 0;
        char **rargv = uwsgi_split_quoted(arg, strlen(arg), " \t", &argc);
        if (!argc) goto end;
	if (argc > 256) goto destroy;

        char *argv[256];
        uint16_t argvs[256];

        char *node = NULL;
        char *func = rargv[0];

	char *at = strchr(func, '@');
	if (at) {
		*at = 0;
		node = at + 1;
	}

        for(i=0;i<(argc-1);i++) {
		size_t a_len = strlen(rargv[i+1]);
		if (a_len > 0xffff) goto destroy;
                argv[i] = rargv[i+1] ;
                argvs[i] = a_len;
        }

        uint64_t size = 0;
        // response must be always freed
        char *response = uwsgi_do_rpc(node, func, argc-1, argv, argvs, &size);
        if (response) {
		if (at) *at = '@';
		uwsgi_log("[rpc result from \"%s\"] %.*s\n", rargv[0], size, response);
                free(response);
		ret = 0;
        }

destroy:
        for(i=0;i<argc;i++) {
                free(rargv[i]);
        }
end:
	free(rargv);
	return ret;
}

static int uwsgi_hook_retryrpc(char *arg) {
	for(;;) {
		int ret = uwsgi_hook_rpc(arg);
		if (!ret) break;
		sleep(2);
	}
	return 0;
}

static int uwsgi_hook_wait_for_fs(char *arg) {
	return uwsgi_wait_for_fs(arg, 0);
}

static int uwsgi_hook_wait_for_file(char *arg) {
	return uwsgi_wait_for_fs(arg, 1);
}

static int uwsgi_hook_wait_for_dir(char *arg) {
	return uwsgi_wait_for_fs(arg, 2);
}

static int uwsgi_hook_wait_for_socket(char *arg) {
	return uwsgi_wait_for_socket(arg);
}

static int spinningfifo_hook(char *arg) {
        int fd;
        char *space = strchr(arg, ' ');
        if (!space) {
                uwsgi_log("invalid hook spinningfifo syntax, must be: <file> <string>\n");
                return -1;
        }
        *space = 0;
retry:
        uwsgi_log("waiting for %s ...\n", arg);
        fd = open(arg, O_WRONLY|O_NONBLOCK);
        if (fd < 0) {
                if (errno == ENODEV || errno == ENOENT) {
                        sleep(1);
                        goto retry;
                }
#ifdef ENXIO
                if (errno == ENXIO) {
                        sleep(1);
                        goto retry;
                }
#endif
                uwsgi_error_open(arg);
                *space = ' ';
                return -1;
        }
        *space = ' ';
        size_t l = strlen(space+1);
        if (write(fd, space+1, l) != (ssize_t) l) {
                uwsgi_error("spinningfifo_hook()/write()");
                close(fd);
                return -1;
        }
        close(fd);
        return 0;
}

void uwsgi_register_base_hooks() {
	uwsgi_register_hook("cd", uwsgi_hook_chdir);
	uwsgi_register_hook("chdir", uwsgi_hook_chdir);

	uwsgi_register_hook("mkdir", uwsgi_hook_mkdir);
	uwsgi_register_hook("putenv", uwsgi_hook_putenv);
	uwsgi_register_hook("chmod", uwsgi_hook_chmod);
	uwsgi_register_hook("chown", uwsgi_hook_chown);
	uwsgi_register_hook("chown2", uwsgi_hook_chown2);

	uwsgi_register_hook("sticky", uwsgi_hook_sticky);

	uwsgi_register_hook("exec", uwsgi_hook_exec);
	uwsgi_register_hook("safeexec", uwsgi_hook_safeexec);

	uwsgi_register_hook("create", uwsgi_hook_creat);
	uwsgi_register_hook("creat", uwsgi_hook_creat);

	uwsgi_register_hook("write", uwsgi_hook_write);
	uwsgi_register_hook("writen", uwsgi_hook_writen);
	uwsgi_register_hook("append", uwsgi_hook_append);
	uwsgi_register_hook("appendn", uwsgi_hook_appendn);
	uwsgi_register_hook("writefifo", uwsgi_hook_writefifo);
	uwsgi_register_hook("unlink", uwsgi_hook_unlink);

	uwsgi_register_hook("mount", uwsgi_mount_hook);
	uwsgi_register_hook("umount", uwsgi_umount_hook);

	uwsgi_register_hook("call", uwsgi_hook_call);
	uwsgi_register_hook("callret", uwsgi_hook_callret);

	uwsgi_register_hook("callint", uwsgi_hook_callint);
	uwsgi_register_hook("callintret", uwsgi_hook_callintret);

	uwsgi_register_hook("hostname", uwsgi_hook_hostname);

	uwsgi_register_hook("alarm", uwsgi_hook_alarm);

	uwsgi_register_hook("rpc", uwsgi_hook_rpc);
	uwsgi_register_hook("retryrpc", uwsgi_hook_retryrpc);

	uwsgi_register_hook("wait_for_fs", uwsgi_hook_wait_for_fs);
	uwsgi_register_hook("wait_for_file", uwsgi_hook_wait_for_file);
	uwsgi_register_hook("wait_for_dir", uwsgi_hook_wait_for_dir);

	uwsgi_register_hook("wait_for_socket", uwsgi_hook_wait_for_socket);

	uwsgi_register_hook("unix_signal", uwsgi_hook_unix_signal);

	uwsgi_register_hook("spinningfifo", spinningfifo_hook);

	// for testing
	uwsgi_register_hook("exit", uwsgi_hook_exit);
	uwsgi_register_hook("print", uwsgi_hook_print);
	uwsgi_register_hook("log", uwsgi_hook_print);
}

void uwsgi_hooks_run(struct uwsgi_string_list *l, char *phase, int fatal) {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, l) {
		char *colon = strchr(usl->value, ':');
		if (!colon) {
			uwsgi_log("invalid hook syntax, must be hook:args\n");
			exit(1);
		}
		*colon = 0;
		int private = 0;
		char *action = usl->value;
		// private hook ?
		if (action[0] == '!') {
			action++;
			private = 1;
		}
		struct uwsgi_hook *uh = uwsgi_hook_by_name(action);
		if (!uh) {
			uwsgi_log("hook action not found: %s\n", action);
			exit(1);
		}
		*colon = ':';

		if (private) {
			uwsgi_log("running --- PRIVATE HOOK --- (%s)...\n", phase);
		}
		else {
			uwsgi_log("running \"%s\" (%s)...\n", usl->value, phase);
		}
			
		int ret = uh->func(colon+1);
		if (fatal && ret != 0) {
			uwsgi_log_verbose("FATAL hook failed, destroying instance\n");
			if (uwsgi.master_process) {
				if (uwsgi.workers) {
					if (uwsgi.workers[0].pid == getpid()) {
						kill_them_all(0);
						return;
					}
					else {
                                        	if (kill(uwsgi.workers[0].pid, SIGINT)) {
							uwsgi_error("uwsgi_hooks_run()/kill()");
							exit(1);
						}
						return;
                                	}
				}
			}
			exit(1);
		}
	}
}
