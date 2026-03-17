#include <contrib/python/uWSGI/py3/config.h>
/*

The uWSGI Emperor

*/
#include "uwsgi.h"


extern struct uwsgi_server uwsgi;
extern char **environ;

void emperor_send_stats(int);

time_t emperor_throttle;
int emperor_throttle_level;
int emperor_warming_up = 1;

struct uwsgi_instance *ui;

time_t on_royal_death = 0;

/*

	blacklist subsystem

	failed unloyal vassals are blacklisted and throttled

*/
struct uwsgi_emperor_blacklist_item {
	char id[0xff];
	struct timeval first_attempt;
	struct timeval last_attempt;
	int throttle_level;
	int attempt;
	struct uwsgi_emperor_blacklist_item *prev;
	struct uwsgi_emperor_blacklist_item *next;
};

struct uwsgi_emperor_blacklist_item *emperor_blacklist;

/*
this should be placed in core/socket.c but we realized it was needed
only after 2.0 so we cannot change uwsgi.h

basically it is a stripped down bind_to_tcp/bind_to_unix with rollback
*/
static int on_demand_bind(char *socket_name) {
	union uwsgi_sockaddr us;
	socklen_t addr_len = sizeof(struct sockaddr_un);
	char *is_tcp = strchr(socket_name, ':');
	int af_family = is_tcp ? AF_INET : AF_UNIX;
	int fd = socket(af_family, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;

	memset(&us, 0, sizeof(union uwsgi_sockaddr));

	if (is_tcp) {
		int reuse = 1;
		if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const void *) &reuse, sizeof(int)) < 0) {
			goto error;
		}
		us.sa_in.sin_family = AF_INET;
		us.sa_in.sin_port = htons(atoi(is_tcp + 1));
		*is_tcp = 0;
		if (socket_name[0] != 0) {
			us.sa_in.sin_addr.s_addr = inet_addr(socket_name);
		}
		else {
			us.sa_in.sin_addr.s_addr = INADDR_ANY;
		}
		*is_tcp = ':';
		addr_len = sizeof(struct sockaddr_in);
	}
	else {
		if (unlink(socket_name) != 0 && errno != ENOENT) {
			goto error;
		}

		us.sa_un.sun_family = AF_UNIX;
		memcpy(us.sa_un.sun_path, socket_name, UMIN(strlen(socket_name), 102));
		addr_len = strlen(socket_name) + ((void *) us.sa_un.sun_path - (void *) &us.sa_un);
	}

	if (bind(fd, (struct sockaddr *) &us, addr_len) != 0) {
		goto error;
	}

	if (!is_tcp) {
		if (chmod(socket_name, 0666)) {
			goto error;
		}
	}

	if (listen(fd, uwsgi.listen_queue) != 0) {
		goto error;
	}

	return fd;

error:
	close(fd);
	return -1;
}

struct uwsgi_emperor_blacklist_item *uwsgi_emperor_blacklist_check(char *id) {
	struct uwsgi_emperor_blacklist_item *uebi = emperor_blacklist;
	while (uebi) {
		if (!strcmp(uebi->id, id)) {
			return uebi;
		}
		uebi = uebi->next;
	}
	return NULL;
}


void uwsgi_emperor_blacklist_add(char *id) {

	// check if the item is already in the blacklist        
	struct uwsgi_emperor_blacklist_item *uebi = uwsgi_emperor_blacklist_check(id);
	if (uebi) {
		gettimeofday(&uebi->last_attempt, NULL);
		if (uebi->throttle_level < (uwsgi.emperor_max_throttle * 1000)) {
			uebi->throttle_level += (uwsgi.emperor_throttle * 1000);
		}
		else {
			uwsgi_log_verbose("[emperor] maximum throttle level for vassal %s reached !!!\n", id);
			uebi->throttle_level = uebi->throttle_level / 2;
		}
		uebi->attempt++;
		if (uebi->attempt == 2) {
			uwsgi_log_verbose("[emperor] unloyal bad behaving vassal found: %s throttling it...\n", id);
		}
		return;
	}

	uebi = emperor_blacklist;
	if (!uebi) {
		uebi = uwsgi_calloc(sizeof(struct uwsgi_emperor_blacklist_item));
		uebi->prev = NULL;
		emperor_blacklist = uebi;
	}
	else {
		while (uebi) {
			if (!uebi->next) {
				uebi->next = uwsgi_calloc(sizeof(struct uwsgi_emperor_blacklist_item));
				uebi->next->prev = uebi;
				uebi = uebi->next;
				break;
			}
			uebi = uebi->next;
		}
	}

	snprintf(uebi->id, 0xff, "%s", id);
	gettimeofday(&uebi->first_attempt, NULL);
	memcpy(&uebi->last_attempt, &uebi->first_attempt, sizeof(struct timeval));
	uebi->throttle_level = uwsgi.emperor_throttle;
	uebi->next = NULL;

}

void uwsgi_emperor_blacklist_remove(char *id) {

	struct uwsgi_emperor_blacklist_item *uebi = uwsgi_emperor_blacklist_check(id);
	if (!uebi)
		return;

	// ok let's remove the item
	//is it the first item ?
	if (uebi == emperor_blacklist) {
		emperor_blacklist = uebi->next;
	}

	struct uwsgi_emperor_blacklist_item *next = uebi->next;
	struct uwsgi_emperor_blacklist_item *prev = uebi->prev;

	if (next)
		next->prev = prev;

	if (prev)
		prev->next = next;

	free(uebi);
}


struct uwsgi_emperor_scanner *emperor_scanners;

static int has_extra_extension(char *name) {
	struct uwsgi_string_list *usl = uwsgi.emperor_extra_extension;
	while (usl) {
		if (uwsgi_endswith(name, usl->value)) {
			return 1;
		}
		usl = usl->next;
	}
	return 0;
}

int uwsgi_emperor_is_valid(char *name) {

	if (uwsgi_endswith(name, ".xml") || uwsgi_endswith(name, ".ini") || uwsgi_endswith(name, ".yml") || uwsgi_endswith(name, ".yaml") || uwsgi_endswith(name, ".js") || uwsgi_endswith(name, ".json") || has_extra_extension(name)) {

		if (strlen(name) < 0xff) {
			return 1;
		}
	}


	return 0;
}

static char *emperor_check_on_demand_socket(char *filename) {
	size_t len = 0;
	if (uwsgi.emperor_on_demand_extension) {
		char *tmp = uwsgi_concat2(filename, uwsgi.emperor_on_demand_extension);
		int fd = open(tmp, O_RDONLY);
		free(tmp);
		if (fd < 0)
			return NULL;
		char *ret = uwsgi_read_fd(fd, &len, 1);
		close(fd);
		// change the first non printable character to 0
		size_t i;
		for (i = 0; i < len; i++) {
			if (ret[i] < 32) {
				ret[i] = 0;
				break;
			}
		}
		if (ret[0] == 0) {
			free(ret);
			return NULL;
		}
		return ret;
	}
	else if (uwsgi.emperor_on_demand_directory) {
		// we need to build the socket path automagically
		char *start_of_vassal_name = uwsgi_get_last_char(filename, '/');
		if (!start_of_vassal_name) {
			start_of_vassal_name = filename;
		}
		else {
			start_of_vassal_name++;
		}
		char *last_dot = uwsgi_get_last_char(filename, '.');
		if (!last_dot)
			return NULL;

		return uwsgi_concat4n(uwsgi.emperor_on_demand_directory, strlen(uwsgi.emperor_on_demand_directory), "/", 1, start_of_vassal_name, last_dot - start_of_vassal_name, ".socket", 7);
	}
	else if (uwsgi.emperor_on_demand_exec) {
		int cpipe[2];
		if (pipe(cpipe)) {
			uwsgi_error("emperor_check_on_demand_socket()pipe()");
			return NULL;
		}
		char *cmd = uwsgi_concat4(uwsgi.emperor_on_demand_exec, " \"", filename, "\"");
		int r = uwsgi_run_command(cmd, NULL, cpipe[1]);
		free(cmd);
		if (r < 0) {
			close(cpipe[0]);
			close(cpipe[1]);
			return NULL;
		}
		char *ret = uwsgi_read_fd(cpipe[0], &len, 1);
		close(cpipe[0]);
		close(cpipe[1]);
		// change the first non prinabel character to 0
		size_t i;
		for (i = 0; i < len; i++) {
			if (ret[i] < 32) {
				ret[i] = 0;
				break;
			}
		}
		if (ret[0] == 0) {
			free(ret);
			return NULL;
		}
		return ret;
	}
	return NULL;
}

// this is the monitor for non-glob directories
void uwsgi_imperial_monitor_directory(struct uwsgi_emperor_scanner *ues) {
	struct uwsgi_instance *ui_current;
	struct dirent *de;
	struct stat st;

	if (chdir(ues->arg)) {
		uwsgi_error("chdir()");
		return;
	}

	DIR *dir = opendir(".");
	while ((de = readdir(dir)) != NULL) {

		if (!uwsgi_emperor_is_valid(de->d_name))
			continue;

		if (uwsgi.emperor_nofollow) {
			if (lstat(de->d_name, &st))
				continue;
			if (!S_ISLNK(st.st_mode) && !S_ISREG(st.st_mode))
				continue;
		}
		else {
			if (stat(de->d_name, &st))
				continue;
			if (!S_ISREG(st.st_mode))
				continue;
		}

		ui_current = emperor_get(de->d_name);

		uid_t t_uid = st.st_uid;
		gid_t t_gid = st.st_gid;

		if (uwsgi.emperor_tyrant && uwsgi.emperor_tyrant_nofollow) {
			struct stat lst;
			if (lstat(de->d_name, &lst)) {
				uwsgi_error("[emperor-tyrant]/lstat()");
				if (ui_current) {
					uwsgi_log("!!! availability of file %s changed. stopping the instance... !!!\n", de->d_name);
					emperor_stop(ui_current);
				}
				continue;
			}
			t_uid = lst.st_uid;
			t_gid = lst.st_gid;
		}

		if (ui_current) {
			// check if uid or gid are changed, in such case, stop the instance
			if (uwsgi.emperor_tyrant) {
				if (t_uid != ui_current->uid || t_gid != ui_current->gid) {
					uwsgi_log("!!! permissions of file %s changed. stopping the instance... !!!\n", de->d_name);
					emperor_stop(ui_current);
					continue;
				}
			}
			// check if mtime is changed and the uWSGI instance must be reloaded
			if (st.st_mtime > ui_current->last_mod) {
				emperor_respawn(ui_current, st.st_mtime);
			}
		}
		else {
			char *socket_name = emperor_check_on_demand_socket(de->d_name);
			emperor_add(ues, de->d_name, st.st_mtime, NULL, 0, t_uid, t_gid, socket_name);
			if (socket_name)
				free(socket_name);
		}
	}
	closedir(dir);

	// now check for removed instances
	struct uwsgi_instance *c_ui = ui->ui_next;

	while (c_ui) {
		if (c_ui->scanner == ues) {
			if (c_ui->zerg) {
				char *colon = strrchr(c_ui->name, ':');
				if (!colon) {
					emperor_stop(c_ui);
				}
				else {
					char *filename = uwsgi_calloc(0xff);
					memcpy(filename, c_ui->name, colon - c_ui->name);
					if (uwsgi.emperor_nofollow) {
						if (lstat(filename, &st)) {
							emperor_stop(c_ui);
						}
					}
					else {
						if (stat(filename, &st)) {
							emperor_stop(c_ui);
						}
					}
					free(filename);
				}
			}
			else {
				if (uwsgi.emperor_nofollow) {
					if (lstat(c_ui->name, &st)) {
						emperor_stop(c_ui);
					}
				}
				else {
					if (stat(c_ui->name, &st)) {
						emperor_stop(c_ui);
					}
				}
			}
		}
		c_ui = c_ui->ui_next;
	}
}

// this is the monitor for glob patterns
void uwsgi_imperial_monitor_glob(struct uwsgi_emperor_scanner *ues) {

	glob_t g;
	int i;
	struct stat st;
	struct uwsgi_instance *ui_current;

	if (chdir(uwsgi.cwd)) {
		uwsgi_error("uwsgi_imperial_monitor_glob()/chdir()");
		exit(1);
	}

	if (glob(ues->arg, GLOB_MARK | GLOB_NOCHECK, NULL, &g)) {
		uwsgi_error("uwsgi_imperial_monitor_glob()/glob()");
		return;
	}

	for (i = 0; i < (int) g.gl_pathc; i++) {

		if (!uwsgi_emperor_is_valid(g.gl_pathv[i]))
			continue;

		if (uwsgi.emperor_nofollow) {
			if (lstat(g.gl_pathv[i], &st))
				continue;
			if (!S_ISREG(st.st_mode) && !S_ISLNK(st.st_mode))
				continue;
		}
		else {
			if (stat(g.gl_pathv[i], &st))
				continue;
			if (!S_ISREG(st.st_mode))
				continue;
		}

		ui_current = emperor_get(g.gl_pathv[i]);

		uid_t t_uid = st.st_uid;
		gid_t t_gid = st.st_gid;

		if (uwsgi.emperor_tyrant && uwsgi.emperor_tyrant_nofollow) {
			struct stat lst;
			if (lstat(g.gl_pathv[i], &lst)) {
				uwsgi_error("[emperor-tyrant]/lstat()");
				if (ui_current) {
					uwsgi_log("!!! availability of file %s changed. stopping the instance... !!!\n", g.gl_pathv[i]);
					emperor_stop(ui_current);
				}
				continue;
			}
			t_uid = lst.st_uid;
			t_gid = lst.st_gid;
		}

		if (ui_current) {
			// check if uid or gid are changed, in such case, stop the instance
			if (uwsgi.emperor_tyrant) {
				if (t_uid != ui_current->uid || t_gid != ui_current->gid) {
					uwsgi_log("!!! permissions of file %s changed. stopping the instance... !!!\n", g.gl_pathv[i]);
					emperor_stop(ui_current);
					continue;
				}
			}
			// check if mtime is changed and the uWSGI instance must be reloaded
			if (st.st_mtime > ui_current->last_mod) {
				emperor_respawn(ui_current, st.st_mtime);
			}
		}
		else {
			char *socket_name = emperor_check_on_demand_socket(g.gl_pathv[i]);
			emperor_add(ues, g.gl_pathv[i], st.st_mtime, NULL, 0, t_uid, t_gid, socket_name);
			if (socket_name)
				free(socket_name);
		}

	}
	globfree(&g);

	// now check for removed instances
	struct uwsgi_instance *c_ui = ui->ui_next;

	while (c_ui) {
		if (c_ui->scanner == ues) {
			if (c_ui->zerg) {
				char *colon = strrchr(c_ui->name, ':');
				if (!colon) {
					emperor_stop(c_ui);
				}
				else {
					char *filename = uwsgi_calloc(0xff);
					memcpy(filename, c_ui->name, colon - c_ui->name);
					if (uwsgi.emperor_nofollow) {
						if (lstat(filename, &st)) {
							emperor_stop(c_ui);
						}
					}
					else {
						if (stat(filename, &st)) {
							emperor_stop(c_ui);
						}
					}
					free(filename);
				}
			}
			else {
				if (uwsgi.emperor_nofollow) {
					if (lstat(c_ui->name, &st)) {
						emperor_stop(c_ui);
					}
				}
				else {
					if (stat(c_ui->name, &st)) {
						emperor_stop(c_ui);
					}
				}
			}
		}
		c_ui = c_ui->ui_next;
	}


}

void uwsgi_register_imperial_monitor(char *name, void (*init) (struct uwsgi_emperor_scanner *), void (*func) (struct uwsgi_emperor_scanner *)) {

	struct uwsgi_imperial_monitor *uim = uwsgi.emperor_monitors;
	if (!uim) {
		uim = uwsgi_calloc(sizeof(struct uwsgi_imperial_monitor));
		uwsgi.emperor_monitors = uim;
	}
	else {
		while (uim) {
			if (!uim->next) {
				uim->next = uwsgi_calloc(sizeof(struct uwsgi_imperial_monitor));
				uim = uim->next;
				break;
			}
			uim = uim->next;
		}
	}

	uim->scheme = name;
	uim->init = init;
	uim->func = func;
	uim->next = NULL;
}


// the sad death of an Emperor
static void royal_death(int signum) {

	if (on_royal_death) {
		uwsgi_log("[emperor] *** RAGNAROK ALREADY EVOKED (mercyless in %d seconds)***\n", uwsgi.reload_mercy - (uwsgi_now() - on_royal_death));
		return;
	}

	struct uwsgi_instance *c_ui = ui->ui_next;

	if (uwsgi.vassals_stop_hook) {


		while (c_ui) {
			uwsgi_log("[emperor] running vassal stop-hook: %s %s\n", uwsgi.vassals_stop_hook, c_ui->name);
			if (uwsgi.emperor_absolute_dir) {
				if (setenv("UWSGI_VASSALS_DIR", uwsgi.emperor_absolute_dir, 1)) {
					uwsgi_error("setenv()");
				}
			}
			int stop_hook_ret = uwsgi_run_command_and_wait(uwsgi.vassals_stop_hook, c_ui->name);
			uwsgi_log("[emperor] %s stop-hook returned %d\n", c_ui->name, stop_hook_ret);
			c_ui = c_ui->ui_next;
		}
	}

	uwsgi_log("[emperor] *** RAGNAROK EVOKED ***\n");

	while (c_ui) {
		emperor_stop(c_ui);
		c_ui = c_ui->ui_next;
	}

	if (!uwsgi.reload_mercy)
		uwsgi.reload_mercy = 30;
	on_royal_death = uwsgi_now();
}

// massive reload of vassals
static void emperor_massive_reload(int signum) {
	struct uwsgi_instance *c_ui = ui->ui_next;

	while (c_ui) {
		emperor_respawn(c_ui, uwsgi_now());
		c_ui = c_ui->ui_next;
	}
}


static void emperor_stats(int signum) {

	struct uwsgi_instance *c_ui = ui->ui_next;

	while (c_ui) {

		uwsgi_log("vassal instance %s (last modified %lld) status %d loyal %d zerg %d\n", c_ui->name, (long long) c_ui->last_mod, c_ui->status, c_ui->loyal, c_ui->zerg);

		c_ui = c_ui->ui_next;
	}

}

struct uwsgi_instance *emperor_get_by_fd(int fd) {

	struct uwsgi_instance *c_ui = ui;

	while (c_ui->ui_next) {
		c_ui = c_ui->ui_next;

		if (c_ui->pipe[0] == fd) {
			return c_ui;
		}
	}
	return NULL;
}

struct uwsgi_instance *emperor_get_by_socket_fd(int fd) {

	struct uwsgi_instance *c_ui = ui;

	while (c_ui->ui_next) {
		c_ui = c_ui->ui_next;

		// over engineering...
		if (c_ui->on_demand_fd > -1 && c_ui->on_demand_fd == fd) {
			return c_ui;
		}
	}
	return NULL;
}



struct uwsgi_instance *emperor_get(char *name) {

	struct uwsgi_instance *c_ui = ui;

	while (c_ui->ui_next) {
		c_ui = c_ui->ui_next;

		if (!strcmp(c_ui->name, name)) {
			return c_ui;
		}
	}
	return NULL;
}

void emperor_del(struct uwsgi_instance *c_ui) {

	struct uwsgi_instance *parent_ui = c_ui->ui_prev;
	struct uwsgi_instance *child_ui = c_ui->ui_next;

	parent_ui->ui_next = child_ui;
	if (child_ui) {
		child_ui->ui_prev = parent_ui;
	}

	// this will destroy the whole uWSGI instance (and workers)
	if (c_ui->pipe[0] != -1)
		close(c_ui->pipe[0]);
	if (c_ui->pipe[1] != -1)
		close(c_ui->pipe[1]);

	if (c_ui->use_config) {
		if (c_ui->pipe_config[0] != -1)
			close(c_ui->pipe_config[0]);
		if (c_ui->pipe_config[1] != -1)
			close(c_ui->pipe_config[1]);
	}

	if (uwsgi.vassals_stop_hook) {
		uwsgi_log("[emperor] running vassal stop-hook: %s %s\n", uwsgi.vassals_stop_hook, c_ui->name);
		if (uwsgi.emperor_absolute_dir) {
			if (setenv("UWSGI_VASSALS_DIR", uwsgi.emperor_absolute_dir, 1)) {
				uwsgi_error("setenv()");
			}
		}
		int stop_hook_ret = uwsgi_run_command_and_wait(uwsgi.vassals_stop_hook, c_ui->name);
		uwsgi_log("[emperor] %s stop-hook returned %d\n", c_ui->name, stop_hook_ret);
	}

	uwsgi_log_verbose("[emperor] removed uwsgi instance %s\n", c_ui->name);
	// put the instance in the blacklist (or update its throttling value)
	if (!c_ui->loyal && !uwsgi.emperor_no_blacklist) {
		uwsgi_emperor_blacklist_add(c_ui->name);
	}

	if (c_ui->zerg) {
		uwsgi.emperor_broodlord_count--;
	}

	if (c_ui->socket_name) {
		free(c_ui->socket_name);
	}

	if (c_ui->config)
		free(c_ui->config);

	if (c_ui->on_demand_fd > -1) {
		close(c_ui->on_demand_fd);
	}

	free(c_ui);

}

void emperor_back_to_ondemand(struct uwsgi_instance *c_ui) {
	if (c_ui->status > 0)
		return;

	// remove uWSGI instance

	if (c_ui->pid != -1) {
		if (write(c_ui->pipe[0], uwsgi.emperor_graceful_shutdown ? "\2" : "\0", 1) != 1) {
			uwsgi_error("emperor_stop()/write()");
		}
	}

	c_ui->status = 2;
	c_ui->cursed_at = uwsgi_now();

	uwsgi_log_verbose("[emperor] bringing back instance %s to on-demand mode\n", c_ui->name);
}

void emperor_stop(struct uwsgi_instance *c_ui) {
	if (c_ui->status == 1)
		return;
	// remove uWSGI instance

	if (c_ui->pid != -1) {
		if (write(c_ui->pipe[0], uwsgi.emperor_graceful_shutdown ? "\2" : "\0", 1) != 1) {
			uwsgi_error("emperor_stop()/write()");
		}
	}

	c_ui->status = 1;
	c_ui->cursed_at = uwsgi_now();

	uwsgi_log_verbose("[emperor] stop the uwsgi instance %s\n", c_ui->name);
}

void emperor_curse(struct uwsgi_instance *c_ui) {
	if (c_ui->status == 1)
		return;
	// curse uWSGI instance

	// take in account on-demand mode
	if (c_ui->status == 0)
		c_ui->status = 1;
	c_ui->cursed_at = uwsgi_now();

	uwsgi_log_verbose("[emperor] curse the uwsgi instance %s (pid: %d)\n", c_ui->name, (int) c_ui->pid);

}

// send configuration (if required to the vassal)
static void emperor_push_config(struct uwsgi_instance *c_ui) {
	struct uwsgi_header uh;

	if (c_ui->use_config) {
		uh.modifier1 = 115;
		uh.pktsize = c_ui->config_len;
		uh.modifier2 = 0;
		if (write(c_ui->pipe_config[0], &uh, 4) != 4) {
			uwsgi_error("[uwsgi-emperor] write() header config");
		}
		else {
			if (write(c_ui->pipe_config[0], c_ui->config, c_ui->config_len) != (long) c_ui->config_len) {
				uwsgi_error("[uwsgi-emperor] write() config");
			}
		}
	}
}

void emperor_respawn(struct uwsgi_instance *c_ui, time_t mod) {

	// if the vassal is being destroyed, do not honour respawns
	if (c_ui->status > 0)
		return;

	// check if we are in on_demand mode (the respawn will be ignored)
	if (c_ui->pid == -1 && c_ui->on_demand_fd > -1) {
		c_ui->last_mod = mod;
		// reset readyness
		c_ui->ready = 0;
		// reset accepting
		c_ui->accepting = 0;
		uwsgi_log_verbose("[emperor] updated configuration for \"on demand\" instance %s\n", c_ui->name);
		return;
	}

	// reload the uWSGI instance
	if (write(c_ui->pipe[0], "\1", 1) != 1) {
		// the vassal could be already dead, better to curse it
		uwsgi_error("emperor_respawn/write()");
		emperor_curse(c_ui);
		return;
	}

	// push the config to the config pipe (if needed)
	emperor_push_config(c_ui);

	c_ui->respawns++;
	c_ui->last_mod = mod;
	c_ui->last_run = uwsgi_now();
	// reset readyness
	c_ui->ready = 0;
	// reset accepting
	c_ui->accepting = 0;

	uwsgi_log_verbose("[emperor] reload the uwsgi instance %s\n", c_ui->name);
}

void emperor_add(struct uwsgi_emperor_scanner *ues, char *name, time_t born, char *config, uint32_t config_size, uid_t uid, gid_t gid, char *socket_name) {

	struct uwsgi_instance *c_ui = ui;
	struct uwsgi_instance *n_ui = NULL;
	struct timeval tv;

#ifdef UWSGI_DEBUG
	uwsgi_log("\n\nVASSAL %s %d %.*s %d %d\n", name, born, config_size, config, uid, gid);
#endif

	if (strlen(name) > (0xff - 1)) {
		uwsgi_log("[emperor] invalid vassal name: %s\n", name);
		return;
	}


	gettimeofday(&tv, NULL);
	int now = tv.tv_sec;
	uint64_t micros = (tv.tv_sec * 1000ULL * 1000ULL) + tv.tv_usec;

	// blacklist check
	struct uwsgi_emperor_blacklist_item *uebi = uwsgi_emperor_blacklist_check(name);
	if (uebi) {
		uint64_t i_micros = (uebi->last_attempt.tv_sec * 1000ULL * 1000ULL) + uebi->last_attempt.tv_usec + uebi->throttle_level;
		if (i_micros > micros) {
			return;
		}
	}

	// TODO make it meaningful
	if (now - emperor_throttle < 1) {
		emperor_throttle_level = emperor_throttle_level * 2;
	}
	else {
		if (emperor_throttle_level > uwsgi.emperor_throttle) {
			emperor_throttle_level = emperor_throttle_level / 2;
		}

		if (emperor_throttle_level < uwsgi.emperor_throttle) {
			emperor_throttle_level = uwsgi.emperor_throttle;
		}
	}

	emperor_throttle = now;
#ifdef UWSGI_DEBUG
	uwsgi_log("emperor throttle = %d\n", emperor_throttle_level);
#endif
	/*
	if (emperor_warming_up) {
		if (emperor_throttle_level > 0) {
			// wait 10 milliseconds in case of fork-bombing
			// pretty random value, but should avoid the load average to increase
			usleep(10 * 1000);
		}
	}
	else {
		usleep(emperor_throttle_level * 1000);
	}
	*/

	if (uwsgi.emperor_tyrant) {
		if (uid == 0 || gid == 0) {
			uwsgi_log("[emperor-tyrant] invalid permissions for vassal %s\n", name);
			return;
		}
	}

	while (c_ui->ui_next) {
		c_ui = c_ui->ui_next;
	}

	n_ui = uwsgi_calloc(sizeof(struct uwsgi_instance));

	if (config) {
		n_ui->use_config = 1;
		n_ui->config = config;
		n_ui->config_len = config_size;
	}

	c_ui->ui_next = n_ui;
#ifdef UWSGI_DEBUG
	uwsgi_log("c_ui->ui_next = %p\n", c_ui->ui_next);
#endif
	n_ui->ui_prev = c_ui;

	if (strchr(name, ':')) {
		n_ui->zerg = 1;
		uwsgi.emperor_broodlord_count++;
	}

	n_ui->scanner = ues;
	memcpy(n_ui->name, name, strlen(name));
	n_ui->born = born;
	n_ui->uid = uid;
	n_ui->gid = gid;
	n_ui->last_mod = born;
	// start non-ready
	n_ui->last_ready = 0;
	n_ui->ready = 0;
	// start without loyalty
	n_ui->last_loyal = 0;
	n_ui->loyal = 0;

	n_ui->first_run = uwsgi_now();
	n_ui->last_run = n_ui->first_run;
	n_ui->on_demand_fd = -1;
	if (socket_name) {
		n_ui->socket_name = uwsgi_str(socket_name);
	}

	n_ui->pid = -1;
	n_ui->pipe[0] = -1;
	n_ui->pipe[1] = -1;

	n_ui->pipe_config[0] = -1;
	n_ui->pipe_config[1] = -1;

	// ok here we check if we need to bind to the specified socket or continue with the activation
	if (socket_name) {
		n_ui->on_demand_fd = on_demand_bind(socket_name);
		if (n_ui->on_demand_fd < 0) {
			uwsgi_error("emperor_add()/bind()");
			emperor_del(n_ui);
			return;
		}

		event_queue_add_fd_read(uwsgi.emperor_queue, n_ui->on_demand_fd);
		uwsgi_log("[uwsgi-emperor] %s -> \"on demand\" instance detected, waiting for connections on socket \"%s\" ...\n", name, socket_name);
		return;
	}

	if (uwsgi_emperor_vassal_start(n_ui)) {
		// clear the vassal
		emperor_del(n_ui);
	}
}

static int uwsgi_emperor_spawn_vassal(struct uwsgi_instance *);

int uwsgi_emperor_vassal_start(struct uwsgi_instance *n_ui) {

	pid_t pid;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, n_ui->pipe)) {
		uwsgi_error("socketpair()");
		return -1;
	}
	uwsgi_socket_nb(n_ui->pipe[0]);

	event_queue_add_fd_read(uwsgi.emperor_queue, n_ui->pipe[0]);

	if (n_ui->use_config) {
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, n_ui->pipe_config)) {
			uwsgi_error("socketpair()");
			return -1;
		}
		uwsgi_socket_nb(n_ui->pipe_config[0]);
	}

	if (n_ui->zerg) {
		uwsgi.emperor_broodlord_num++;
	}

	// TODO pre-start hook

	// a new uWSGI instance will start 
#if defined(__linux__) && !defined(OBSOLETE_LINUX_KERNEL) && !defined(__ia64__)
	if (uwsgi.emperor_clone) {
		char stack[PTHREAD_STACK_MIN];
		pid = clone((int (*)(void *)) uwsgi_emperor_spawn_vassal, stack + PTHREAD_STACK_MIN, SIGCHLD | uwsgi.emperor_clone, (void *) n_ui);
	}
	else {
#endif
		pid = fork();
#if defined(__linux__) && !defined(OBSOLETE_LINUX_KERNEL) && !defined(__ia64__)
	}
#endif
	if (pid < 0) {
		uwsgi_error("uwsgi_emperor_spawn_vassal()/fork()")
	}
	else if (pid > 0) {
		n_ui->pid = pid;
		// close the right side of the pipe
		close(n_ui->pipe[1]);
		n_ui->pipe[1] = -1;
		/* THE ON-DEMAND file descriptor is left mapped to the emperor to allow fast-respawn
		   // TODO add an option to force closing it
		   // close the "on demand" socket
		   if (n_ui->on_demand_fd > -1) {
		   close(n_ui->on_demand_fd);
		   n_ui->on_demand_fd = -1;
		   }
		 */
		if (n_ui->use_config) {
			close(n_ui->pipe_config[1]);
			n_ui->pipe_config[1] = -1;
			emperor_push_config(n_ui);
		}

		// once the config is sent we can run hooks (they can fail)
		// exec hooks have access to all of the currently defined vars + UWSGI_VASSAL_PID, UWSGI_VASSAL_UID, UWSGI_VASSAL_GID, UWSGI_VASSAL_CONFIG
		uwsgi_hooks_run(uwsgi.hook_as_emperor, "as-emperor", 0);
		struct uwsgi_string_list *usl;
		uwsgi_foreach(usl, uwsgi.mount_as_emperor) {
			uwsgi_log("mounting \"%s\" (as-emperor for vassal \"%s\" pid: %d uid: %d gid: %d)...\n", usl->value, n_ui->name, n_ui->pid, n_ui->uid, n_ui->gid);
			if (uwsgi_mount_hook(usl->value)) {
				uwsgi_log("unable to mount %s\n", usl->value);
			}
		}

		uwsgi_foreach(usl, uwsgi.umount_as_emperor) {
			uwsgi_log("un-mounting \"%s\" (as-emperor for vassal \"%s\" pid: %d uid: %d gid: %d)...\n", usl->value, n_ui->name, n_ui->pid, n_ui->uid, n_ui->gid);
			if (uwsgi_umount_hook(usl->value)) {
				uwsgi_log("unable to umount %s\n", usl->value);
			}
		}
		uwsgi_foreach(usl, uwsgi.exec_as_emperor) {
			uwsgi_log("running \"%s\" (as-emperor for vassal \"%s\" pid: %d uid: %d gid: %d)...\n", usl->value, n_ui->name, n_ui->pid, n_ui->uid, n_ui->gid);
			char *argv[4];
			argv[0] = uwsgi_concat2("UWSGI_VASSAL_CONFIG=", n_ui->name);
			char argv_pid[17 + 11];
			snprintf(argv_pid, 17 + 11, "UWSGI_VASSAL_PID=%d", (int) n_ui->pid);
			argv[1] = argv_pid;
			char argv_uid[17 + 11];
			snprintf(argv_uid, 17 + 11, "UWSGI_VASSAL_UID=%d", (int) n_ui->uid);
			argv[2] = argv_uid;
			char argv_gid[17 + 11];
			snprintf(argv_gid, 17 + 11, "UWSGI_VASSAL_GID=%d", (int) n_ui->gid);
			argv[3] = argv_gid;
			int ret = uwsgi_run_command_putenv_and_wait(NULL, usl->value, argv, 4);
			uwsgi_log("command \"%s\" exited with code: %d\n", usl->value, ret);
			free(argv[0]);
		}
		// 4 call hooks
		// config / config + pid / config + pid + uid + gid
		// call
		uwsgi_foreach(usl, uwsgi.call_as_emperor) {
			void (*func) (void) = dlsym(RTLD_DEFAULT, usl->value);
			if (!func) {
				uwsgi_log("unable to call function \"%s\"\n", usl->value);
			}
			else {
				func();
			}
		}

		uwsgi_foreach(usl, uwsgi.call_as_emperor1) {
			void (*func) (char *) = dlsym(RTLD_DEFAULT, usl->value);
			if (!func) {
				uwsgi_log("unable to call function \"%s\"\n", usl->value);
			}
			else {
				func(n_ui->name);
			}
		}

		uwsgi_foreach(usl, uwsgi.call_as_emperor2) {
			void (*func) (char *, pid_t) = dlsym(RTLD_DEFAULT, usl->value);
			if (!func) {
				uwsgi_log("unable to call function \"%s\"\n", usl->value);
			}
			else {
				func(n_ui->name, n_ui->pid);
			}
		}

		uwsgi_foreach(usl, uwsgi.call_as_emperor4) {
			void (*func) (char *, pid_t, uid_t, gid_t) = dlsym(RTLD_DEFAULT, usl->value);
			if (!func) {
				uwsgi_log("unable to call function \"%s\"\n", usl->value);
			}
			else {
				func(n_ui->name, n_ui->pid, n_ui->uid, n_ui->gid);
			}
		}
		return 0;
	}
	else {
		uwsgi_emperor_spawn_vassal(n_ui);
	}
	return -1;
}

static int uwsgi_emperor_spawn_vassal(struct uwsgi_instance *n_ui) {
	int i;

	// run plugin hooks for the vassal
	for (i = 0; i < 256; i++) {
                if (uwsgi.p[i]->vassal) {
                        uwsgi.p[i]->vassal(n_ui);
                }
        }

        for (i = 0; i < uwsgi.gp_cnt; i++) {
                if (uwsgi.gp[i]->vassal) {
                        uwsgi.gp[i]->vassal(n_ui);
                }
        }

#ifdef __linux__
	if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0)) {
		uwsgi_error("prctl()");
	}
#ifdef CLONE_NEWUSER
	if (uwsgi.emperor_clone & CLONE_NEWUSER) {
		if (setuid(0)) {
			uwsgi_error("uwsgi_emperor_spawn_vassal()/setuid(0)");
			exit(1);
		}
	}
#endif

#ifdef UWSGI_CAP
#if defined(CAP_LAST_CAP) && defined(PR_CAPBSET_READ) && defined(PR_CAPBSET_DROP)
	if (uwsgi.emperor_cap && uwsgi.emperor_cap_count > 0) {
		int i;
		for (i = 0; i <= CAP_LAST_CAP; i++) {

			int has_cap = prctl(PR_CAPBSET_READ, i, 0, 0, 0);
			if (has_cap == 1) {
				if (i == CAP_SETPCAP)
					continue;
				int j;
				int found = 0;
				for (j = 0; j < uwsgi.emperor_cap_count; j++) {
					if (uwsgi.emperor_cap[j] == (int) i) {
						found = 1;
						break;
					}
				}
				if (found)
					continue;
				if (prctl(PR_CAPBSET_DROP, i, 0, 0, 0)) {
					uwsgi_error("uwsgi_emperor_spawn_vassal()/prctl()");
					uwsgi_log_verbose("unable to drop capability %lu\n", i);
					exit(1);
				}
			}
		}
		// just for being paranoid
#ifdef SECBIT_KEEP_CAPS
		if (prctl(SECBIT_KEEP_CAPS, 1, 0, 0, 0) < 0) {
			uwsgi_error("prctl()");
			exit(1);
		}
#else
		if (prctl(PR_SET_KEEPCAPS, 1, 0, 0, 0) < 0) {
			uwsgi_error("prctl()");
			exit(1);
		}
#endif
		uwsgi_log("capabilities applied for vassal %s (pid: %d)\n", n_ui->name, (int) getpid());
	}
#endif
#endif
#endif

	if (uwsgi.emperor_tyrant) {
		uwsgi_log("[emperor-tyrant] dropping privileges to %d %d for instance %s\n", (int) n_ui->uid, (int) n_ui->gid, n_ui->name);
		if (setgid(n_ui->gid)) {
			uwsgi_error("setgid()");
			exit(1);
		}
		if (setgroups(0, NULL)) {
			uwsgi_error("setgroups()");
			exit(1);
		}

		if (setuid(n_ui->uid)) {
			uwsgi_error("setuid()");
			exit(1);
		}

	}

	unsetenv("UWSGI_RELOADS");
	unsetenv("NOTIFY_SOCKET");

	char *uef = uwsgi_num2str(n_ui->pipe[1]);
	if (setenv("UWSGI_EMPEROR_FD", uef, 1)) {
		uwsgi_error("setenv()");
		exit(1);
	}
	free(uef);

	// add UWSGI_BROODLORD_NUM
	if (n_ui->zerg) {
		uef = uwsgi_num2str(uwsgi.emperor_broodlord_num);
		if (setenv("UWSGI_BROODLORD_NUM", uef, 1)) {
			uwsgi_error("setenv()");
			exit(1);
		}
		free(uef);
	}

	if (n_ui->use_config) {
		uef = uwsgi_num2str(n_ui->pipe_config[1]);
		if (setenv("UWSGI_EMPEROR_FD_CONFIG", uef, 1)) {
			uwsgi_error("setenv()");
			exit(1);
		}
		free(uef);
	}

	char **uenvs = environ;
	while (*uenvs) {
		if (!strncmp(*uenvs, "UWSGI_VASSAL_", 13) && strchr(*uenvs, '=')) {
			char *oe = uwsgi_concat2n(*uenvs, strchr(*uenvs, '=') - *uenvs, "", 0), *ne;
#ifdef UNSETENV_VOID
			unsetenv(oe);
#else
			if (unsetenv(oe)) {
				uwsgi_error("unsetenv()");
				free(oe);
				break;
			}
#endif
			free(oe);

			ne = uwsgi_concat2("UWSGI_", *uenvs + 13);
#ifdef UWSGI_DEBUG
			uwsgi_log("putenv %s\n", ne);
#endif

			if (putenv(ne)) {
				uwsgi_error("putenv()");
			}
			// do not free ne as putenv will add it to the environ
			uenvs = environ;
			continue;
		}
		uenvs++;
	}

	// close the left side of the pipe
	close(n_ui->pipe[0]);

	if (n_ui->use_config) {
		close(n_ui->pipe_config[0]);
	}

	int counter = 4;
	struct uwsgi_string_list *uct;
	uwsgi_foreach(uct, uwsgi.vassals_templates_before) counter += 2;
	uwsgi_foreach(uct, uwsgi.vassals_includes_before) counter += 2;
	uwsgi_foreach(uct, uwsgi.vassals_set) counter += 2;
	uwsgi_foreach(uct, uwsgi.vassals_templates) counter += 2;
	uwsgi_foreach(uct, uwsgi.vassals_includes) counter += 2;

	char **vassal_argv = uwsgi_malloc(sizeof(char *) * counter);
	// set args
	vassal_argv[0] = uwsgi.emperor_wrapper ? uwsgi.emperor_wrapper : uwsgi.binary_path;

	// reset counter
	counter = 1;

	uwsgi_foreach(uct, uwsgi.vassals_templates_before) {
		vassal_argv[counter] = "--inherit";
		vassal_argv[counter + 1] = uct->value;
		counter += 2;
	}

	uwsgi_foreach(uct, uwsgi.vassals_includes_before) {
		vassal_argv[counter] = "--include";
		vassal_argv[counter + 1] = uct->value;
		counter += 2;
	}

	uwsgi_foreach(uct, uwsgi.vassals_set) {
		vassal_argv[counter] = "--set";
		vassal_argv[counter + 1] = uct->value;
		counter += 2;
	}

	char *colon = NULL;
	if (uwsgi.emperor_broodlord) {
		colon = strchr(n_ui->name, ':');
		if (colon) {
			colon[0] = 0;
		}
	}
	// initialize to a default value
	vassal_argv[counter] = "--inherit";

	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 4), ".xml"))
		vassal_argv[counter] = "--xml";
	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 4), ".ini"))
		vassal_argv[counter] = "--ini";
	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 4), ".yml"))
		vassal_argv[counter] = "--yaml";
	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 5), ".yaml"))
		vassal_argv[counter] = "--yaml";
	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 3), ".js"))
		vassal_argv[counter] = "--json";
	if (!strcmp(n_ui->name + (strlen(n_ui->name) - 5), ".json"))
		vassal_argv[counter] = "--json";
	struct uwsgi_string_list *usl = uwsgi.emperor_extra_extension;
	while (usl) {
		if (uwsgi_endswith(n_ui->name, usl->value)) {
			vassal_argv[counter] = "--config";
			break;
		}
		usl = usl->next;
	}
	if (colon)
		colon[0] = ':';

	// start config filename...
	counter++;

	vassal_argv[counter] = n_ui->name;
	if (uwsgi.emperor_magic_exec) {
		if (!access(n_ui->name, R_OK | X_OK)) {
			vassal_argv[counter] = uwsgi_concat2("exec://", n_ui->name);
		}

	}

	if (n_ui->use_config) {
		vassal_argv[counter] = uwsgi_concat2("emperor://", n_ui->name);
	}

	// start templates,includes,inherit...
	counter++;

	uwsgi_foreach(uct, uwsgi.vassals_templates) {
		vassal_argv[counter] = "--inherit";
		vassal_argv[counter + 1] = uct->value;
		counter += 2;
	}

	uwsgi_foreach(uct, uwsgi.vassals_includes) {
		vassal_argv[counter] = "--include";
		vassal_argv[counter + 1] = uct->value;
		counter += 2;
	}

	vassal_argv[counter] = NULL;

	// disable stdin OR map it to the "on demand" socket
	if (n_ui->on_demand_fd > -1) {
		if (n_ui->on_demand_fd != 0) {
			if (dup2(n_ui->on_demand_fd, 0) < 0) {
				uwsgi_error("dup2()");
				exit(1);
			}
			close(n_ui->on_demand_fd);
		}
	}
	else {
		uwsgi_remap_fd(0, "/dev/null");
	}

	// close all of the unneded fd
	for (i = 3; i < (int) uwsgi.max_fd; i++) {
		if (uwsgi_fd_is_safe(i))
			continue;
		if (n_ui->use_config) {
			if (i == n_ui->pipe_config[1])
				continue;
		}
		if (i != n_ui->pipe[1]) {
			close(i);
		}
	}

	// run start hook (can fail)
	if (uwsgi.vassals_start_hook) {
		uwsgi_log("[emperor] running vassal start-hook: %s %s\n", uwsgi.vassals_start_hook, n_ui->name);
		if (uwsgi.emperor_absolute_dir) {
			if (setenv("UWSGI_VASSALS_DIR", uwsgi.emperor_absolute_dir, 1)) {
				uwsgi_error("setenv()");
			}
		}
		int start_hook_ret = uwsgi_run_command_and_wait(uwsgi.vassals_start_hook, n_ui->name);
		uwsgi_log("[emperor] %s start-hook returned %d\n", n_ui->name, start_hook_ret);
	}

	uwsgi_hooks_run(uwsgi.hook_as_vassal, "as-vassal", 1);

	uwsgi_foreach(usl, uwsgi.mount_as_vassal) {
		uwsgi_log("mounting \"%s\" (as-vassal)...\n", usl->value);
		if (uwsgi_mount_hook(usl->value)) {
			exit(1);
		}
	}

	uwsgi_foreach(usl, uwsgi.umount_as_vassal) {
		uwsgi_log("un-mounting \"%s\" (as-vassal)...\n", usl->value);
		if (uwsgi_umount_hook(usl->value)) {
			exit(1);
		}
	}

	// run exec hooks (cannot fail)
	uwsgi_foreach(usl, uwsgi.exec_as_vassal) {
		uwsgi_log("running \"%s\" (as-vassal)...\n", usl->value);
		int ret = uwsgi_run_command_and_wait(NULL, usl->value);
		if (ret != 0) {
			uwsgi_log("command \"%s\" exited with non-zero code: %d\n", usl->value, ret);
			exit(1);
		}
	}

	// run low-level hooks
	uwsgi_foreach(usl, uwsgi.call_as_vassal) {
		void (*func) (void) = dlsym(RTLD_DEFAULT, usl->value);
		if (!func) {
			uwsgi_log("unable to call function \"%s\"\n", usl->value);
			exit(1);
		}
		func();
	}

	uwsgi_foreach(usl, uwsgi.call_as_vassal1) {
		void (*func) (char *) = dlsym(RTLD_DEFAULT, usl->value);
		if (!func) {
			uwsgi_log("unable to call function \"%s\"\n", usl->value);
			exit(1);
		}
		func(n_ui->name);
	}

	uwsgi_foreach(usl, uwsgi.call_as_vassal3) {
		void (*func) (char *, uid_t, gid_t) = dlsym(RTLD_DEFAULT, usl->value);
		if (!func) {
			uwsgi_log("unable to call function \"%s\"\n", usl->value);
			exit(1);
		}
		func(n_ui->name, n_ui->uid, n_ui->gid);
	}

	// ->vassal_before_exec
	for (i = 0; i < 256; i++) {
                if (uwsgi.p[i]->vassal_before_exec) {
                        uwsgi.p[i]->vassal_before_exec(n_ui);
                }
        }

        for (i = 0; i < uwsgi.gp_cnt; i++) {
                if (uwsgi.gp[i]->vassal) {
                        uwsgi.gp[i]->vassal_before_exec(n_ui);
                }
        }

	
	if (uwsgi.emperor_wrapper_override) {
		char *orig_wrapper = vassal_argv[0];	
		uwsgi_foreach(usl, uwsgi.emperor_wrapper_override) {
			vassal_argv[0] = usl->value;
			uwsgi_log("[emperor] trying to use %s as binary wrapper ...\n", usl->value);
			execvp(vassal_argv[0], vassal_argv);
			// not here if the binary is found
		}
		vassal_argv[0] = orig_wrapper;
	}

	// start !!!
	if (execvp(vassal_argv[0], vassal_argv)) {
		uwsgi_error("execvp()");
	}
	uwsgi_log("[emperor] binary path: %s\n", vassal_argv[0]);
	uwsgi_log("[emperor] is the uwsgi binary in your system PATH ?\n");

	// trying fallback
	uwsgi_foreach(usl, uwsgi.emperor_wrapper_fallback) {
		uwsgi_log("[emperor] trying to use %s as binary fallback ...\n", usl->value);
                vassal_argv[0] = usl->value;
                execvp(vassal_argv[0], vassal_argv);
                // not here if the binary is found
        }
	// never here
	exit(UWSGI_EXILE_CODE);

	return 0;
}

void uwsgi_imperial_monitor_glob_init(struct uwsgi_emperor_scanner *ues) {
	if (chdir(uwsgi.cwd)) {
		uwsgi_error("chdir()");
		exit(1);
	}

	uwsgi.emperor_absolute_dir = uwsgi.cwd;

	if (!uwsgi_startswith(ues->arg, "glob://", 7)) {
		ues->arg += 7;
	}
}

void uwsgi_imperial_monitor_directory_init(struct uwsgi_emperor_scanner *ues) {

	if (!uwsgi_startswith(ues->arg, "dir://", 6)) {
		ues->arg += 6;
	}

	if (chdir(ues->arg)) {
		uwsgi_error("chdir()");
		exit(1);
	}

	uwsgi.emperor_absolute_dir = uwsgi_malloc(PATH_MAX + 1);
	if (realpath(".", uwsgi.emperor_absolute_dir) == NULL) {
		uwsgi_error("realpath()");
		exit(1);
	}

	ues->arg = uwsgi.emperor_absolute_dir;

}

struct uwsgi_imperial_monitor *imperial_monitor_get_by_id(char *scheme) {
	struct uwsgi_imperial_monitor *uim = uwsgi.emperor_monitors;
	while (uim) {
		if (!strcmp(uim->scheme, scheme)) {
			return uim;
		}
		uim = uim->next;
	}
	return NULL;
}

struct uwsgi_imperial_monitor *imperial_monitor_get_by_scheme(char *arg) {
	struct uwsgi_imperial_monitor *uim = uwsgi.emperor_monitors;
	while (uim) {
		char *scheme = uwsgi_concat2(uim->scheme, "://");
		if (!uwsgi_starts_with(arg, strlen(arg), scheme, strlen(scheme))) {
			free(scheme);
			return uim;
		}
		free(scheme);
		uim = uim->next;
	}
	return NULL;
}

void emperor_add_scanner(struct uwsgi_imperial_monitor *monitor, char *arg) {
	struct uwsgi_emperor_scanner *ues = emperor_scanners;
	if (!ues) {
		ues = uwsgi_calloc(sizeof(struct uwsgi_emperor_scanner));
		emperor_scanners = ues;
	}
	else {
		while (ues) {
			if (!ues->next) {
				ues->next = uwsgi_calloc(sizeof(struct uwsgi_emperor_scanner));
				ues = ues->next;
				break;
			}
			ues = ues->next;
		}
	}

	ues->arg = arg;
	ues->monitor = monitor;
	ues->next = NULL;
	ues->fd = -1;
	// run the init hook
	ues->monitor->init(ues);
}

void uwsgi_emperor_run_scanners(void) {
	struct uwsgi_emperor_scanner *ues = emperor_scanners;
	while (ues) {
		ues->monitor->func(ues);
		ues = ues->next;
	}
	emperor_warming_up = 0;
}

void emperor_build_scanners() {
	struct uwsgi_string_list *usl = uwsgi.emperor;
	glob_t g;
	while (usl) {
		struct uwsgi_imperial_monitor *uim = imperial_monitor_get_by_scheme(usl->value);
		if (uim) {
			emperor_add_scanner(uim, usl->value);
		}
		else {
			// check for "glob" and fallback to "dir"
			if (!glob(usl->value, GLOB_MARK | GLOB_NOCHECK, NULL, &g)) {
				if (g.gl_pathc == 1 && g.gl_pathv[0][strlen(g.gl_pathv[0]) - 1] == '/') {
					globfree(&g);
					goto dir;
				}
				globfree(&g);
				uim = imperial_monitor_get_by_id("glob");
				emperor_add_scanner(uim, usl->value);
				goto next;
			}
dir:
			uim = imperial_monitor_get_by_id("dir");
			emperor_add_scanner(uim, usl->value);
		}
next:
		usl = usl->next;
	}
}

int uwsgi_emperor_scanner_event(int fd) {

	struct uwsgi_emperor_scanner *ues = emperor_scanners;
	while (ues) {
		if (ues->fd > -1 && ues->fd == fd) {
			ues->event_func(ues);
			return 1;
		}
		ues = ues->next;
	}

	return 0;

}

static void emperor_wakeup(int sn) {
}

static void emperor_cleanup(int signum) {
	uwsgi_log_verbose("[emperor] cleaning up blacklist ...\n");
	struct uwsgi_emperor_blacklist_item *uebi = emperor_blacklist;
	while (uebi) {
		struct uwsgi_emperor_blacklist_item *next = uebi->next;
		free(uebi);
		uebi = next;
	}
	emperor_blacklist = NULL;
}

void emperor_loop() {

	// monitor a directory

	struct uwsgi_instance ui_base;
	struct uwsgi_instance *ui_current;

	pid_t diedpid;
	int waitpid_status;
	int has_children = 0;
	int i_am_alone = 0;
	int i;

	void *events;
	int nevents;
	int interesting_fd;
	char notification_message[64];
	struct rlimit rl;

	uwsgi.disable_nuclear_blast = 1;

	uwsgi.emperor_stats_fd = -1;

	if (uwsgi.emperor_pidfile) {
		uwsgi_write_pidfile(uwsgi.emperor_pidfile);
	}

	signal(SIGPIPE, SIG_IGN);
	signal(SIGWINCH, emperor_wakeup);
	uwsgi_unix_signal(SIGINT, royal_death);
	uwsgi_unix_signal(SIGTERM, royal_death);
	uwsgi_unix_signal(SIGQUIT, royal_death);
	uwsgi_unix_signal(SIGUSR1, emperor_stats);
	uwsgi_unix_signal(SIGHUP, emperor_massive_reload);
	uwsgi_unix_signal(SIGURG, emperor_cleanup);

	memset(&ui_base, 0, sizeof(struct uwsgi_instance));

	if (getrlimit(RLIMIT_NOFILE, &rl)) {
		uwsgi_error("getrlimit()");
		exit(1);
	}

	uwsgi.max_fd = rl.rlim_cur;

	emperor_throttle_level = uwsgi.emperor_throttle;
	emperor_throttle = 0;

	// the queue must be initialized before adding scanners
	uwsgi.emperor_queue = event_queue_init();

	emperor_build_scanners();

	events = event_queue_alloc(64);

	if (uwsgi.has_emperor) {
		uwsgi_log("*** starting uWSGI sub-Emperor ***\n");
	}
	else {
		uwsgi_log("*** starting uWSGI Emperor ***\n");
	}

	if (uwsgi.emperor_stats) {
		char *tcp_port = strchr(uwsgi.emperor_stats, ':');
		if (tcp_port) {
			// disable deferred accept for this socket
			int current_defer_accept = uwsgi.no_defer_accept;
			uwsgi.no_defer_accept = 1;
			uwsgi.emperor_stats_fd = bind_to_tcp(uwsgi.emperor_stats, uwsgi.listen_queue, tcp_port);
			uwsgi.no_defer_accept = current_defer_accept;
		}
		else {
			uwsgi.emperor_stats_fd = bind_to_unix(uwsgi.emperor_stats, uwsgi.listen_queue, uwsgi.chmod_socket, uwsgi.abstract_socket);
		}

		event_queue_add_fd_read(uwsgi.emperor_queue, uwsgi.emperor_stats_fd);
		uwsgi_log("*** Emperor stats server enabled on %s fd: %d ***\n", uwsgi.emperor_stats, uwsgi.emperor_stats_fd);
	}

	ui = &ui_base;

	int freq = 0;

	uwsgi_hooks_run(uwsgi.hook_emperor_start, "emperor-start", 1);

	// signal parent-Emperor about my loyalty
	if (uwsgi.has_emperor && !uwsgi.loyal) {
		uwsgi_log("announcing my loyalty to the Emperor...\n");
		char byte = 17;
		if (write(uwsgi.emperor_fd, &byte, 1) != 1) {
			uwsgi_error("write()");
		}
		uwsgi.loyal = 1;
	}

	for (;;) {

		if (on_royal_death) {
			if (!ui->ui_next)
				break;
			if (uwsgi_now() - on_royal_death >= uwsgi.reload_mercy) {
				ui_current = ui->ui_next;
				while (ui_current) {
					uwsgi_log_verbose("[emperor] NO MERCY for vassal %s !!!\n", ui_current->name);
					if (kill(ui_current->pid, SIGKILL) < 0) {
						uwsgi_error("[emperor] kill()");
						emperor_del(ui_current);
						break;
					}
					ui_current = ui_current->ui_next;
				}
				break;
			}
			ui_current = ui->ui_next;
			while (ui_current) {
				struct uwsgi_instance *dead_vassal = ui_current;
				ui_current = ui_current->ui_next;
				pid_t dead_pid = waitpid(dead_vassal->pid, &waitpid_status, WNOHANG);
				if (dead_pid > 0 || dead_pid < 0) {
					emperor_del(dead_vassal);
				}
			}
			sleep(1);
			continue;
		}

		if (!i_am_alone) {
			diedpid = waitpid(uwsgi.emperor_pid, &waitpid_status, WNOHANG);
			if (diedpid < 0 || diedpid > 0) {
				i_am_alone = 1;
			}
		}

		nevents = event_queue_wait_multi(uwsgi.emperor_queue, freq, events, 64);
		freq = uwsgi.emperor_freq;

		for (i = 0; i < nevents; i++) {
			interesting_fd = event_queue_interesting_fd(events, i);

			if (uwsgi.emperor_stats && uwsgi.emperor_stats_fd > -1 && interesting_fd == uwsgi.emperor_stats_fd) {
				emperor_send_stats(uwsgi.emperor_stats_fd);
				continue;
			}

			// check if a monitor is mapped to that file descriptor
			if (uwsgi_emperor_scanner_event(interesting_fd)) {
				continue;
			}

			ui_current = emperor_get_by_fd(interesting_fd);
			if (ui_current) {
				char byte;
				ssize_t rlen = read(interesting_fd, &byte, 1);
				// retry if needed
				if (rlen < 0 && uwsgi_is_again()) continue;
				if (rlen <= 0) {
					// SAFE
					event_queue_del_fd(uwsgi.emperor_queue, interesting_fd, event_queue_read());
					if (ui_current->status > 0) {
						// temporarily set frequency to a low value , so we can eventually fast-restart the instance
						freq = ui_current->status;
					}
					emperor_curse(ui_current);
				}
				else {
					if (byte == 17) {
						ui_current->loyal = 1;
						ui_current->last_loyal = uwsgi_now();
						uwsgi_log_verbose("[emperor] vassal %s is now loyal\n", ui_current->name);
						// remove it from the blacklist
						uwsgi_emperor_blacklist_remove(ui_current->name);
						// TODO post-start hook
					}
					// heartbeat can be used for spotting blocked instances
					else if (byte == 26) {
						ui_current->last_heartbeat = uwsgi_now();
					}
					else if (byte == 22) {
						// command 22 changes meaning when in "on_demand" mode  
						if (ui_current->on_demand_fd != -1) {
							emperor_back_to_ondemand(ui_current);
						}
						else {
							emperor_stop(ui_current);
						}
					}
					else if (byte == 30 && uwsgi.emperor_broodlord > 0 && uwsgi.emperor_broodlord_count < uwsgi.emperor_broodlord) {
						uwsgi_log_verbose("[emperor] going in broodlord mode: launching zergs for %s\n", ui_current->name);
						char *zerg_name = uwsgi_concat3(ui_current->name, ":", "zerg");
						// here we discard socket name as broodlord/zerg cannot be on demand
						emperor_add(ui_current->scanner, zerg_name, uwsgi_now(), NULL, 0, ui_current->uid, ui_current->gid, NULL);
						free(zerg_name);
					}
					else if (byte == 5) {
						ui_current->accepting = 1;
						ui_current->last_accepting = uwsgi_now();
						uwsgi_log_verbose("[emperor] vassal %s is ready to accept requests\n", ui_current->name);
					}
					else if (byte == 1) {
						ui_current->ready = 1;
						ui_current->last_ready = uwsgi_now();
						uwsgi_log_verbose("[emperor] vassal %s has been spawned\n", ui_current->name);
					}
					else if (byte == 2) {
						emperor_push_config(ui_current);
					}
				}
			}
			else {
				ui_current = emperor_get_by_socket_fd(interesting_fd);
				if (ui_current) {
					event_queue_del_fd(uwsgi.emperor_queue, ui_current->on_demand_fd, event_queue_read());
					if (uwsgi_emperor_vassal_start(ui_current)) {
						emperor_del(ui_current);
					}
				}
				else {
					uwsgi_log("[emperor] unrecognized vassal event on fd %d\n", interesting_fd);
					close(interesting_fd);
				}
			}

		}

		uwsgi_emperor_run_scanners();

		// check for heartbeat (if required)
		ui_current = ui->ui_next;
		while (ui_current) {
			if (ui_current->last_heartbeat > 0) {
#ifdef UWSGI_DEBUG
				uwsgi_log("%d %d %d %d\n", ui_current->last_heartbeat, uwsgi.emperor_heartbeat, ui_current->last_heartbeat + uwsgi.emperor_heartbeat, uwsgi_now());
#endif
				if ((ui_current->last_heartbeat + uwsgi.emperor_heartbeat) < uwsgi_now()) {
					uwsgi_log("[emperor] vassal %s sent no heartbeat in last %d seconds, brutally respawning it...\n", ui_current->name, uwsgi.emperor_heartbeat);
					// set last_heartbeat to 0 avoiding races
					ui_current->last_heartbeat = 0;
					if (ui_current->pid > 0) {
						if (kill(ui_current->pid, SIGKILL) < 0) {
							uwsgi_error("[emperor] kill()");
							emperor_del(ui_current);
							break;
						}
					}
				}
			}
			ui_current = ui_current->ui_next;
		}


recheck:
		// check for removed instances
		ui_current = ui;
		has_children = 0;
		while (ui_current->ui_next) {
			ui_current = ui_current->ui_next;
			if (ui_current->pid > -1) {
				has_children++;
			}
		}

		if (uwsgi.notify) {
			if (snprintf(notification_message, 64, "The Emperor is governing %d vassals", has_children) >= 34) {
				uwsgi_notify(notification_message);
			}
		}

		if (has_children) {
			diedpid = waitpid(WAIT_ANY, &waitpid_status, WNOHANG);
		}
		else {
			// vacuum
			waitpid(WAIT_ANY, &waitpid_status, WNOHANG);
			diedpid = 0;
		}
		if (diedpid < 0) {
			// it looks like it happens when OOM is triggered to Linux cgroup, but it could be a uWSGI bug :P
			// by the way, fallback to a clean situation...
			if (errno == ECHILD) {
				uwsgi_log("--- MUTINY DETECTED !!! IMPALING VASSALS... ---\n");
				ui_current = ui->ui_next;
				while (ui_current) {
					struct uwsgi_instance *rebel_vassal = ui_current;
					ui_current = ui_current->ui_next;
					emperor_del(rebel_vassal);
				}
			}
			else {
				uwsgi_error("waitpid()");
			}
		}
		ui_current = ui;
		while (ui_current->ui_next) {
			ui_current = ui_current->ui_next;
			time_t now = uwsgi_now();
			if (diedpid > 0 && ui_current->pid == diedpid) {
				if (ui_current->status == 0) {
					// respawn an accidentally dead instance if its exit code is not UWSGI_EXILE_CODE
					if (WIFEXITED(waitpid_status) && WEXITSTATUS(waitpid_status) == UWSGI_EXILE_CODE) {
						// SAFE
						emperor_del(ui_current);
					}
					else {
						// UNSAFE
						char *config = NULL;
						if (ui_current->config) {
							config = uwsgi_str(ui_current->config);
						}
						char *socket_name = NULL;
						if (ui_current->socket_name) {
							socket_name = uwsgi_str(ui_current->socket_name);
						}
						emperor_add(ui_current->scanner, ui_current->name, ui_current->last_mod, config, ui_current->config_len, ui_current->uid, ui_current->gid, socket_name);
						// temporarily set frequency to 0, so we can eventually fast-restart the instance
						emperor_del(ui_current);
						freq = 0;
					}
					break;
				}
				else if (ui_current->status == 1) {
					// remove 'marked for dead' instance
					emperor_del(ui_current);
					// temporarily set frequency to 0, so we can eventually fast-restart the instance
					freq = 0;
					break;
				}
				// back to on_demand mode ...
				else if (ui_current->status == 2) {
					event_queue_add_fd_read(uwsgi.emperor_queue, ui_current->on_demand_fd);
					close(ui_current->pipe[0]);
					ui_current->pipe[0] = -1;
					if (ui_current->use_config) {
						close(ui_current->pipe_config[0]);
						ui_current->pipe_config[0] = -1;
					}
					ui_current->pid = -1;
					ui_current->status = 0;
					ui_current->cursed_at = 0;
					ui_current->ready = 0;
					ui_current->accepting = 0;
					uwsgi_log("[uwsgi-emperor] %s -> back to \"on demand\" mode, waiting for connections on socket \"%s\" ...\n", ui_current->name, ui_current->socket_name);
					break;
				}
			}
			else if (ui_current->cursed_at > 0) {
				if (ui_current->pid == -1) {
					emperor_del(ui_current);
					// temporarily set frequency to 0, so we can eventually fast-restart the instance
					freq = 0;
					break;
				}
				else if (now - ui_current->cursed_at >= uwsgi.emperor_curse_tolerance) {
					ui_current->cursed_at = now;
					if (kill(ui_current->pid, SIGKILL) < 0) {
						uwsgi_error("[emperor] kill()");
						// delete the vassal, something is seriously wrong better to not leak memory...
						emperor_del(ui_current);
					}
					break;
				}
			}
		}

		// if waitpid returned an item, let's check for another (potential) one
		if (diedpid > 0)
			goto recheck;


	}

	uwsgi_log_verbose("The Emperor is buried.\n");
	uwsgi_notify("The Emperor is buried.");
	exit(0);

}

void emperor_send_stats(int fd) {

	struct sockaddr_un client_src;
	socklen_t client_src_len = 0;

	int client_fd = accept(fd, (struct sockaddr *) &client_src, &client_src_len);
	if (client_fd < 0) {
		uwsgi_error("accept()");
		return;
	}

	if (uwsgi.stats_http) {
		if (uwsgi_send_http_stats(client_fd)) {
			close(client_fd);
			return;
		}
	}

	struct uwsgi_stats *us = uwsgi_stats_new(8192);

	if (uwsgi_stats_keyval_comma(us, "version", UWSGI_VERSION))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "pid", (unsigned long long) getpid()))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "uid", (unsigned long long) getuid()))
		goto end;
	if (uwsgi_stats_keylong_comma(us, "gid", (unsigned long long) getgid()))
		goto end;

	char *cwd = uwsgi_get_cwd();
	if (uwsgi_stats_keyval_comma(us, "cwd", cwd))
		goto end0;

	if (uwsgi_stats_key(us, "emperor"))
		goto end0;
	if (uwsgi_stats_list_open(us))
		goto end0;
	struct uwsgi_emperor_scanner *ues = emperor_scanners;
	while (ues) {
		if (uwsgi_stats_str(us, ues->arg))
			goto end0;
		ues = ues->next;
		if (ues) {
			if (uwsgi_stats_comma(us))
				goto end0;
		}
	}
	if (uwsgi_stats_list_close(us))
		goto end0;

	if (uwsgi_stats_comma(us))
		goto end0;

	if (uwsgi_stats_keylong_comma(us, "emperor_tyrant", (unsigned long long) uwsgi.emperor_tyrant))
		goto end0;

	// will be zero for now
	if (uwsgi_stats_keylong_comma(us, "throttle_level", (unsigned long long) 0))
		goto end0;


	if (uwsgi_stats_key(us, "vassals"))
		goto end0;
	if (uwsgi_stats_list_open(us))
		goto end0;

	struct uwsgi_instance *c_ui = ui->ui_next;

	while (c_ui) {
		if (uwsgi_stats_object_open(us))
			goto end0;

		if (uwsgi_stats_keyval_comma(us, "id", c_ui->name))
			goto end0;

		if (uwsgi_stats_keyslong_comma(us, "pid", (long long) c_ui->pid))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "born", (unsigned long long) c_ui->born))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_mod", (unsigned long long) c_ui->last_mod))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_heartbeat", (unsigned long long) c_ui->last_heartbeat))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "loyal", (unsigned long long) c_ui->loyal))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "ready", (unsigned long long) c_ui->ready))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "accepting", (unsigned long long) c_ui->accepting))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_loyal", (unsigned long long) c_ui->last_loyal))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_ready", (unsigned long long) c_ui->last_ready))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_accepting", (unsigned long long) c_ui->last_accepting))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "first_run", (unsigned long long) c_ui->first_run))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "last_run", (unsigned long long) c_ui->last_run))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "cursed", (unsigned long long) c_ui->cursed_at))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "zerg", (unsigned long long) c_ui->zerg))
			goto end0;

		if (uwsgi_stats_keyval_comma(us, "on_demand", c_ui->socket_name ? c_ui->socket_name : ""))
			goto end0;

		if (uwsgi_stats_keylong_comma(us, "uid", (unsigned long long) c_ui->uid))
			goto end0;
		if (uwsgi_stats_keylong_comma(us, "gid", (unsigned long long) c_ui->gid))
			goto end0;

		if (uwsgi_stats_keyval_comma(us, "monitor", c_ui->scanner->arg))
			goto end0;

		if (uwsgi_stats_keylong(us, "respawns", (unsigned long long) c_ui->respawns))
			goto end0;

		if (uwsgi_stats_object_close(us))
			goto end0;

		c_ui = c_ui->ui_next;

		if (c_ui) {
			if (uwsgi_stats_comma(us))
				goto end0;
		}
	}


	if (uwsgi_stats_list_close(us))
		goto end0;

	if (uwsgi_stats_comma(us))
		goto end0;

	if (uwsgi_stats_key(us, "blacklist"))
		goto end0;
	if (uwsgi_stats_list_open(us))
		goto end0;

	struct uwsgi_emperor_blacklist_item *uebi = emperor_blacklist;
	while (uebi) {

		if (uwsgi_stats_object_open(us))
			goto end0;

		if (uwsgi_stats_keyval_comma(us, "id", uebi->id))
			goto end0;


		if (uwsgi_stats_keylong_comma(us, "throttle_level", uebi->throttle_level / 1000))
			goto end0;

		if (uwsgi_stats_keylong_comma(us, "attempt", (unsigned long long) uebi->attempt))
			goto end0;

		if (uwsgi_stats_keylong_comma(us, "first_attempt", (unsigned long long) uebi->first_attempt.tv_sec))
			goto end0;

		if (uwsgi_stats_keylong(us, "last_attempt", (unsigned long long) uebi->last_attempt.tv_sec))
			goto end0;

		if (uwsgi_stats_object_close(us))
			goto end0;


		uebi = uebi->next;
		if (uebi) {
			if (uwsgi_stats_comma(us))
				goto end0;
		}
	}


	if (uwsgi_stats_list_close(us))
		goto end0;

	if (uwsgi_stats_object_close(us))
		goto end0;

	size_t remains = us->pos;
	off_t pos = 0;
	while (remains > 0) {
		int ret = uwsgi_waitfd_write(client_fd, uwsgi.socket_timeout);
		if (ret <= 0) {
			goto end0;
		}
		ssize_t res = write(client_fd, us->base + pos, remains);
		if (res <= 0) {
			if (res < 0) {
				uwsgi_error("write()");
			}
			goto end0;
		}
		pos += res;
		remains -= res;
	}

end0:
	free(cwd);
end:
	free(us->base);
	free(us);
	close(client_fd);
}

void uwsgi_emperor_start() {

	if (!uwsgi.sockets && !ushared->gateways_cnt && !uwsgi.master_process) {
		if (uwsgi.emperor_procname) {
			uwsgi_set_processname(uwsgi.emperor_procname);
		}
		uwsgi_notify_ready();
		emperor_loop();
		// never here
		exit(1);
	}

	if (uwsgi.emperor_procname) {
		uwsgi.emperor_pid = uwsgi_fork(uwsgi.emperor_procname);
	}
	else {
		uwsgi.emperor_pid = uwsgi_fork("uWSGI Emperor");
	}

	if (uwsgi.emperor_pid < 0) {
		uwsgi_error("pid()");
		exit(1);
	}
	else if (uwsgi.emperor_pid == 0) {
#ifdef __linux__
		if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0)) {
			uwsgi_error("prctl()");
		}
#endif
		emperor_loop();
		// never here
		exit(1);
	}

}

void uwsgi_check_emperor() {
	char *emperor_fd_pass = getenv("UWSGI_EMPEROR_PROXY");
	if (emperor_fd_pass) {
		for (;;) {
			int proxy_fd = uwsgi_connect(emperor_fd_pass, 30, 0);
			if (proxy_fd < 0) {
				uwsgi_error("uwsgi_check_emperor()/uwsgi_connect()");
				sleep(1);
				continue;
			}
			int count = 2;
			int *fds = uwsgi_attach_fd(proxy_fd, &count, "uwsgi-emperor", 13);
			if (fds && count > 0) {
				char *env_emperor_fd = uwsgi_num2str(fds[0]);
				if (setenv("UWSGI_EMPEROR_FD", env_emperor_fd, 1)) {
					uwsgi_error("uwsgi_check_emperor()/setenv(UWSGI_EMPEROR_FD)");
					free(env_emperor_fd);
					int i;
					for (i = 0; i < count; i++)
						close(fds[i]);
					goto next;
				}
				free(env_emperor_fd);
				if (count > 1) {
					char *env_emperor_fd_config = uwsgi_num2str(fds[1]);
					if (setenv("UWSGI_EMPEROR_FD_CONFIG", env_emperor_fd_config, 1)) {
						uwsgi_error("uwsgi_check_emperor()/setenv(UWSGI_EMPEROR_FD_CONFIG)");
						free(env_emperor_fd_config);
						int i;
						for (i = 0; i < count; i++)
							close(fds[i]);
						goto next;
					}
					free(env_emperor_fd_config);
				}
				if (fds)
					free(fds);
				close(proxy_fd);
				break;
			}
next:
			if (fds)
				free(fds);
			close(proxy_fd);
			sleep(1);
		}
		unsetenv("UWSGI_EMPEROR_PROXY");
	}

	char *emperor_env = getenv("UWSGI_EMPEROR_FD");
	if (emperor_env) {
		uwsgi.has_emperor = 1;
		uwsgi.emperor_fd = atoi(emperor_env);
		uwsgi.master_process = 1;
		uwsgi_log("*** has_emperor mode detected (fd: %d) ***\n", uwsgi.emperor_fd);

		if (getenv("UWSGI_EMPEROR_FD_CONFIG")) {
			uwsgi.emperor_fd_config = atoi(getenv("UWSGI_EMPEROR_FD_CONFIG"));
		}
	}

}

void uwsgi_emperor_simple_do(struct uwsgi_emperor_scanner *ues, char *name, char *config, time_t ts, uid_t uid, gid_t gid, char *socket_name) {

	if (!uwsgi_emperor_is_valid(name))
		return;

	struct uwsgi_instance *ui_current = emperor_get(name);

	if (ui_current) {

		// skip in case the instance is going down...
		if (ui_current->status > 0)
			return;

		// check if uid or gid are changed, in such case, stop the instance
		if (uwsgi.emperor_tyrant) {
			if (uid != ui_current->uid || gid != ui_current->gid) {
				uwsgi_log("[emperor-tyrant] !!! permissions of vassal %s changed. stopping the instance... !!!\n", name);
				emperor_stop(ui_current);
				return;
			}
		}
		// check if mtime is changed and the uWSGI instance must be reloaded
		if (ts > ui_current->last_mod) {
			// now we neeed a special check for allowing an instance to move to "on_demand" mode (and back)
			// allowing means "stoppping the instance"
			if ((!ui_current->socket_name && ui_current->on_demand_fd == -1) && socket_name) {
				uwsgi_log("[uwsgi-emperor] %s -> requested move to \"on demand\" mode for socket \"%s\" ...\n", name, socket_name);
				emperor_stop(ui_current);
				return;
			}
			else if ((ui_current->socket_name && ui_current->on_demand_fd > -1) && !socket_name) {
				uwsgi_log("[uwsgi-emperor] %s -> asked for leaving \"on demand\" mode for socket \"%s\" ...\n", name, ui_current->socket_name);
				emperor_stop(ui_current);
				return;
			}

			// make a new config (free the old one) if needed
			if (config) {
				if (ui_current->config)
					free(ui_current->config);
				ui_current->config = uwsgi_str(config);
				ui_current->config_len = strlen(ui_current->config);
			}
			// reload the instance
			emperor_respawn(ui_current, ts);
		}
	}
	else {
		// make a copy of the config as it will be freed
		char *new_config = NULL;
		size_t new_config_len = 0;
		if (config) {
			new_config = uwsgi_str(config);
			new_config_len = strlen(new_config);
		}
		emperor_add(ues, name, ts, new_config, new_config_len, uid, gid, socket_name);
	}
}

void uwsgi_master_manage_emperor() {
	char byte;
#ifdef UWSGI_EVENT_USE_PORT
	// special cose for port event system
	// place the socket in non-blocking mode
        uwsgi_socket_nb(uwsgi.emperor_fd);
#endif
	ssize_t rlen = read(uwsgi.emperor_fd, &byte, 1);
#ifdef UWSGI_EVENT_USE_PORT
	// special cose for port event system
	// and place back in blocking mode
        uwsgi_socket_b(uwsgi.emperor_fd);
#endif
	if (rlen > 0) {
		uwsgi_log_verbose("received message %d from emperor\n", byte);
		// remove me
		if (byte == 0) {
			uwsgi_hooks_run(uwsgi.hook_emperor_stop, "emperor-stop", 0);
			close(uwsgi.emperor_fd);
			if (!uwsgi.status.brutally_reloading && !uwsgi.status.brutally_destroying) {
				kill_them_all(0);
			}
		}
		// reload me
		else if (byte == 1) {
			uwsgi_hooks_run(uwsgi.hook_emperor_reload, "emperor-reload", 0);
			// un-lazy the stack to trigger a real reload
			uwsgi.lazy = 0;
			uwsgi_block_signal(SIGHUP);
			grace_them_all(0);
			uwsgi_unblock_signal(SIGHUP);
		}
		// remove me gracefully
		else if (byte == 2) {
			uwsgi_hooks_run(uwsgi.hook_emperor_stop, "emperor-stop", 0);
			close(uwsgi.emperor_fd);
			if (!uwsgi.status.brutally_reloading && !uwsgi.status.brutally_destroying) {
				gracefully_kill_them_all(0);
			}
		}
	}
#ifdef UWSGI_EVENT_USE_PORT
        // special cose for port event system
	else if (rlen < 0 && uwsgi_is_again()) {
		return;
	}
#endif
	else {
		uwsgi_error("uwsgi_master_manage_emperor()/read()");
		uwsgi_log("lost connection with my emperor !!!\n");
		uwsgi_hooks_run(uwsgi.hook_emperor_lost, "emperor-lost", 0);
		close(uwsgi.emperor_fd);
		if (!uwsgi.status.brutally_reloading)
			kill_them_all(0);
		sleep(2);
		exit(1);
	}

}

void uwsgi_master_manage_emperor_proxy() {

	struct sockaddr_un epsun;
	socklen_t epsun_len = sizeof(struct sockaddr_un);

	int ep_client = accept(uwsgi.emperor_fd_proxy, (struct sockaddr *) &epsun, &epsun_len);
	if (ep_client < 0) {
		uwsgi_error("uwsgi_master_manage_emperor_proxy()/accept()");
		return;
	}

	int num_fds = 1;
	if (uwsgi.emperor_fd_config > -1)
		num_fds++;

	struct msghdr ep_msg;
	void *ep_msg_control = uwsgi_malloc(CMSG_SPACE(sizeof(int) * num_fds));
	struct iovec ep_iov[2];
	struct cmsghdr *cmsg;

	ep_iov[0].iov_base = "uwsgi-emperor";
	ep_iov[0].iov_len = 13;
	ep_iov[1].iov_base = &num_fds;
	ep_iov[1].iov_len = sizeof(int);

	ep_msg.msg_name = NULL;
	ep_msg.msg_namelen = 0;

	ep_msg.msg_iov = ep_iov;
	ep_msg.msg_iovlen = 2;

	ep_msg.msg_flags = 0;
	ep_msg.msg_control = ep_msg_control;
	ep_msg.msg_controllen = CMSG_SPACE(sizeof(int) * num_fds);

	cmsg = CMSG_FIRSTHDR(&ep_msg);
	cmsg->cmsg_len = CMSG_LEN(sizeof(int) * num_fds);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;

	unsigned char *ep_fd_ptr = CMSG_DATA(cmsg);

	memcpy(ep_fd_ptr, &uwsgi.emperor_fd, sizeof(int));
	if (num_fds > 1) {
		memcpy(ep_fd_ptr + sizeof(int), &uwsgi.emperor_fd_config, sizeof(int));
	}

	if (sendmsg(ep_client, &ep_msg, 0) < 0) {
		uwsgi_error("uwsgi_master_manage_emperor_proxy()/sendmsg()");
	}

	free(ep_msg_control);

	close(ep_client);
}

static void emperor_notify_ready() {
	if (!uwsgi.has_emperor)
		return;
	char byte = 1;
	if (write(uwsgi.emperor_fd, &byte, 1) != 1) {
		uwsgi_error("emperor_notify_ready()/write()");
		exit(1);
	}
}

void uwsgi_setup_emperor() {
	if (!uwsgi.has_emperor)
		return;
	uwsgi.notify_ready = emperor_notify_ready;
}
