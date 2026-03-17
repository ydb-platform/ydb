#include <contrib/python/uWSGI/py2/config.h>
#include "../uwsgi.h"


extern struct uwsgi_server uwsgi;

#ifndef CLONE_NEWUTS
#define CLONE_NEWUTS 0x04000000
#endif

#ifndef CLONE_NEWPID
#define CLONE_NEWPID 0x20000000
#endif

#ifndef CLONE_NEWIPC
#define CLONE_NEWIPC 0x08000000
#endif

#ifndef CLONE_NEWNET
#define CLONE_NEWNET 0x40000000
#endif

int uwsgi_is_a_keep_mount(char *mp) {
	struct uwsgi_string_list *usl = uwsgi.ns_keep_mount;
	while(usl) {
		char *colon = strchr(usl->value, ':');
		if (colon) {
			if (!strcmp(colon+1, mp)) {
				return 1;
			}
		}
		else {
			// slip first part
			if (!uwsgi_startswith(usl->value, uwsgi.ns, strlen(uwsgi.ns))) {
				char *skipped = usl->value + strlen(uwsgi.ns);
				if (uwsgi.ns[strlen(uwsgi.ns)-1] == '/') {
					skipped--;
				}	
				if (!strcmp(skipped, mp)) {
					return 1;
				}
			}
			else {
				if (!strcmp(usl->value, mp)) {
					return 1;
				}
			}
		}
		usl = usl->next;
	}

	return 0;
	
}

static int uwsgi_ns_start(void *v_argv) {
	uwsgi_start(v_argv);
	return uwsgi_run();
}

void linux_namespace_start(void *argv) {
	for (;;) {
		char stack[PTHREAD_STACK_MIN];
		int waitpid_status;
		char *pid_str = NULL;
		uwsgi_log("*** jailing uWSGI in %s ***\n", uwsgi.ns);
		int clone_flags = SIGCHLD | CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWIPC | CLONE_NEWNS;
		if (uwsgi.ns_net) {
			clone_flags |= CLONE_NEWNET;
		}
		pid_t pid = clone(uwsgi_ns_start, stack + PTHREAD_STACK_MIN, clone_flags, (void *) argv);
		if (pid == -1) {
			uwsgi_error("clone()");
			exit(1);
		}
#if defined(MS_REC) && defined(MS_PRIVATE)
		if (mount(NULL, "/", NULL, MS_REC|MS_PRIVATE, NULL)) {
			uwsgi_error("mount()");
			exit(1);
		}
#endif
		pid_str = uwsgi_num2str((int) pid);
		// run the post-jail scripts
		if (setenv("UWSGI_JAIL_PID", pid_str, 1)) {
			uwsgi_error("setenv()");
		}
		free(pid_str);
		uwsgi_hooks_run(uwsgi.hook_post_jail, "post-jail", 1);
        	struct uwsgi_string_list *usl = uwsgi.exec_post_jail;
        	while(usl) {
                	uwsgi_log("running \"%s\" (post-jail)...\n", usl->value);
                	int ret = uwsgi_run_command_and_wait(NULL, usl->value);
                	if (ret != 0) {
                        	uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
                        	exit(1);
                	}
                	usl = usl->next;
        	}

		uwsgi_foreach(usl, uwsgi.call_post_jail) {
                        if (uwsgi_call_symbol(usl->value)) {
                                uwsgi_log("unable to call function \"%s\"\n", usl->value);
				exit(1);
                        }
                }

		uwsgi_log("waiting for jailed master (pid: %d) death...\n", (int) pid);
		pid = waitpid(pid, &waitpid_status, 0);
		if (pid < 0) {
			uwsgi_error("waitpid()");
			exit(1);
		}

		// in Linux this is reliable
		if (WIFEXITED(waitpid_status) && WEXITSTATUS(waitpid_status) == 1) {
			exit(1);
		}
		else if (uwsgi.exit_on_reload && WIFEXITED(waitpid_status) && WEXITSTATUS(waitpid_status) == 0) {
			uwsgi_log("jailed master process exited and exit-on-reload is enabled, shutting down\n");
			exit(0);
		}


		uwsgi_log("pid %d ended. Respawning...\n", (int) pid);
	}

	// never here
}



void linux_namespace_jail() {

	char *ns_tmp_mountpoint = NULL, *ns_tmp_mountpoint2 = NULL;

	if (getpid() != 1) {
		uwsgi_log("your kernel does not support linux pid namespace\n");
		exit(1);
	}

	char *ns_hostname = strchr(uwsgi.ns, ':');
	if (ns_hostname) {
		ns_hostname[0] = 0;
		ns_hostname++;
		if (sethostname(ns_hostname, strlen(ns_hostname))) {
			uwsgi_error("sethostname()");
		}
	}

	FILE *procmounts;
	char line[1024];
	int unmounted = 1;
	char *delim0, *delim1;

	if (chdir(uwsgi.ns)) {
		uwsgi_error("chdir()");
		exit(1);
	}

	if (strcmp(uwsgi.ns, "/")) {
		ns_tmp_mountpoint = uwsgi_concat2(uwsgi.ns, "/.uwsgi_ns_tmp_mountpoint");
		if (mkdir(ns_tmp_mountpoint, S_IRWXU) < 0) {
			uwsgi_error("mkdir() ns_tmp_mountpoint");
			exit(1);
		}

		ns_tmp_mountpoint2 = uwsgi_concat2(ns_tmp_mountpoint, "/.uwsgi_ns_tmp_mountpoint");
		if (mkdir(ns_tmp_mountpoint2, S_IRWXU) < 0) {
			uwsgi_error("mkdir() ns_tmp_mountpoint2");
			exit(1);
                }

		if (mount(uwsgi.ns, ns_tmp_mountpoint, "none", MS_BIND, NULL)) {
			uwsgi_error("mount()");
		}
		if (chdir(ns_tmp_mountpoint)) {
			uwsgi_error("chdir()");
		}

		if (pivot_root(".", ns_tmp_mountpoint2)) {
			uwsgi_error("pivot_root()");
			exit(1);
		}



		if (chdir("/")) {
			uwsgi_error("chdir()");
			exit(1);
		}

	}

	uwsgi_log("remounting /proc\n");
	if (mount("proc", "/proc", "proc", 0, NULL)) {
		uwsgi_error("mount()");
		exit(1);
	}

	struct uwsgi_string_list *usl = uwsgi.ns_keep_mount;
	while(usl) {
		// bind mounting keep-mount items
		char *keep_mountpoint = usl->value;
		char *destination = strchr(usl->value, ':');
		if (destination) {
			keep_mountpoint = uwsgi_concat2n(usl->value, destination - usl->value, "", 0);
		}
		char *ks = uwsgi_concat2("/.uwsgi_ns_tmp_mountpoint", keep_mountpoint);
		if (!destination) {
			destination = usl->value;
			// skip first part of the name if under the jail
			if (!uwsgi_startswith(destination, uwsgi.ns, strlen(uwsgi.ns))) {
				if (uwsgi.ns[strlen(uwsgi.ns)-1] == '/') {
					destination += strlen(uwsgi.ns)-1;
				}
				else {
					destination += strlen(uwsgi.ns);
				}
			}
		}
		else {
			free(keep_mountpoint);
			destination++;
		}

		uwsgi_log("remounting %s to %s\n", ks+25, destination);
		if (mount(ks, destination, "none", MS_BIND, NULL)) {
			uwsgi_error("mount()");
		}
		free(ks);
		usl = usl->next;
	}

	while (unmounted) {

		unmounted = 0;
		procmounts = fopen("/proc/self/mounts", "r");
		if (!procmounts)
			break;
		while (fgets(line, 1024, procmounts) != NULL) {
			delim0 = strchr(line, ' ');
			if (!delim0)
				continue;
			delim0++;
			delim1 = strchr(delim0, ' ');
			if (!delim1)
				continue;
			*delim1 = 0;
			// and now check for keep-mounts
			if (uwsgi_is_a_keep_mount(delim0)) continue;
			if (!strcmp(delim0, "/") || !strcmp(delim0, "/proc"))
				continue;
			if (!umount(delim0)) {
				unmounted++;
			}
		}
		fclose(procmounts);
	}

	if (rmdir("/.uwsgi_ns_tmp_mountpoint/.uwsgi_ns_tmp_mountpoint")) {
		uwsgi_error("rmdir()");
	}
	if (rmdir("/.uwsgi_ns_tmp_mountpoint")) {
		uwsgi_error("rmdir()");
	}

	if (strcmp(uwsgi.ns, "/")) {
		free(ns_tmp_mountpoint2);
		free(ns_tmp_mountpoint);
	}



}
