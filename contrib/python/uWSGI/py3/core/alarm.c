#include <contrib/python/uWSGI/py3/config.h>
#include "../uwsgi.h"

extern struct uwsgi_server uwsgi;

// generate a uwsgi signal on alarm
void uwsgi_alarm_init_signal(struct uwsgi_alarm_instance *uai) {
	uai->data8 = atoi(uai->arg);
}

void uwsgi_alarm_func_signal(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	uwsgi_route_signal(uai->data8);
}

// simply log an alarm
void uwsgi_alarm_init_log(struct uwsgi_alarm_instance *uai) {
}

void uwsgi_alarm_func_log(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	if (msg[len-1] != '\n') {
		if (uai->arg && strlen(uai->arg) > 0) {
			uwsgi_log_verbose("ALARM: %s %.*s\n", uai->arg, len, msg);
		}
		else {
			uwsgi_log_verbose("ALARM: %.*s\n", len, msg);
		}
	}
	else {
		if (uai->arg && strlen(uai->arg) > 0) {
			uwsgi_log_verbose("ALARM: %s %.*s", uai->arg, len, msg);
		}
		else {
			uwsgi_log_verbose("ALARM: %.*s", len, msg);
		}
	}
}

// run a command on alarm
void uwsgi_alarm_init_cmd(struct uwsgi_alarm_instance *uai) {
	uai->data_ptr = uai->arg;
}

void uwsgi_alarm_func_cmd(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	int pipe[2];
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, pipe)) {
		return;
	}
	uwsgi_socket_nb(pipe[0]);
	uwsgi_socket_nb(pipe[1]);
	if (write(pipe[1], msg, len) != (ssize_t) len) {
		close(pipe[0]);
		close(pipe[1]);
		return;
	}
	uwsgi_run_command(uai->data_ptr, pipe, -1);
	close(pipe[0]);
	close(pipe[1]);
}

// pass the log line to a mule

void uwsgi_alarm_init_mule(struct uwsgi_alarm_instance *uai) {
	uai->data32 = atoi(uai->arg);
	if (uai->data32 > (uint32_t) uwsgi.mules_cnt) {
		uwsgi_log_alarm("] invalid mule_id (%d mules available), fallback to 0\n", uwsgi.mules_cnt);
		uai->data32 = 0;
	}
}

void uwsgi_alarm_func_mule(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	// skip if mules are not available
	if (uwsgi.mules_cnt == 0)
		return;
	int fd = uwsgi.shared->mule_queue_pipe[0];
	if (uai->data32 > 0) {
		int mule_id = uai->data32 - 1;
		fd = uwsgi.mules[mule_id].queue_pipe[0];
	}
	mule_send_msg(fd, msg, len);
}


// register a new alarm
void uwsgi_register_alarm(char *name, void (*init) (struct uwsgi_alarm_instance *), void (*func) (struct uwsgi_alarm_instance *, char *, size_t)) {
	struct uwsgi_alarm *old_ua = NULL, *ua = uwsgi.alarms;
	while (ua) {
		// skip already initialized alarms
		if (!strcmp(ua->name, name)) {
			return;
		}
		old_ua = ua;
		ua = ua->next;
	}

	ua = uwsgi_calloc(sizeof(struct uwsgi_alarm));
	ua->name = name;
	ua->init = init;
	ua->func = func;

	if (old_ua) {
		old_ua->next = ua;
	}
	else {
		uwsgi.alarms = ua;
	}
}

// register embedded alarms
void uwsgi_register_embedded_alarms() {
	uwsgi_register_alarm("signal", uwsgi_alarm_init_signal, uwsgi_alarm_func_signal);
	uwsgi_register_alarm("cmd", uwsgi_alarm_init_cmd, uwsgi_alarm_func_cmd);
	uwsgi_register_alarm("mule", uwsgi_alarm_init_mule, uwsgi_alarm_func_mule);
	uwsgi_register_alarm("log", uwsgi_alarm_init_log, uwsgi_alarm_func_log);
}

static int uwsgi_alarm_add(char *name, char *plugin, char *arg) {
	struct uwsgi_alarm *ua = uwsgi.alarms;
	while (ua) {
		if (!strcmp(ua->name, plugin)) {
			break;
		}
		ua = ua->next;
	}

	if (!ua)
		return -1;

	struct uwsgi_alarm_instance *old_uai = NULL, *uai = uwsgi.alarm_instances;
	while (uai) {
		old_uai = uai;
		uai = uai->next;
	}

	uai = uwsgi_calloc(sizeof(struct uwsgi_alarm_instance));
	uai->name = name;
	uai->alarm = ua;
	uai->arg = arg;
	uai->last_msg = uwsgi_malloc(uwsgi.log_master_bufsize);

	if (old_uai) {
		old_uai->next = uai;
	}
	else {
		uwsgi.alarm_instances = uai;
	}

	ua->init(uai);
	return 0;
}

// get an alarm instance by its name
static struct uwsgi_alarm_instance *uwsgi_alarm_get_instance(char *name) {
	struct uwsgi_alarm_instance *uai = uwsgi.alarm_instances;
	while (uai) {
		if (!strcmp(name, uai->name)) {
			return uai;
		}
		uai = uai->next;
	}
	return NULL;
}


#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
static int uwsgi_alarm_log_add(char *alarms, char *regexp, int negate) {

	struct uwsgi_alarm_log *old_ual = NULL, *ual = uwsgi.alarm_logs;
	while (ual) {
		old_ual = ual;
		ual = ual->next;
	}

	ual = uwsgi_calloc(sizeof(struct uwsgi_alarm_log));
	if (uwsgi_regexp_build(regexp, &ual->pattern)) {
		free(ual);
		return -1;
	}
	ual->negate = negate;

	if (old_ual) {
		old_ual->next = ual;
	}
	else {
		uwsgi.alarm_logs = ual;
	}

	// map instances to the log
	char *list = uwsgi_str(alarms);
	char *p, *ctx = NULL;
	uwsgi_foreach_token(list, ",", p, ctx) {
		struct uwsgi_alarm_instance *uai = uwsgi_alarm_get_instance(p);
		if (!uai) {
			free(list);
			return -1;
		}
		struct uwsgi_alarm_ll *old_uall = NULL, *uall = ual->alarms;
		while (uall) {
			old_uall = uall;
			uall = uall->next;
		}

		uall = uwsgi_calloc(sizeof(struct uwsgi_alarm_ll));
		uall->alarm = uai;
		if (old_uall) {
			old_uall->next = uall;
		}
		else {
			ual->alarms = uall;
		}
	}
	free(list);
	return 0;
}
#endif

static void uwsgi_alarm_thread_loop(struct uwsgi_thread *ut) {
	// add uwsgi_alarm_fd;
	struct uwsgi_alarm_fd *uafd = uwsgi.alarm_fds;
	while(uafd) {
		event_queue_add_fd_read(ut->queue, uafd->fd);
		uafd = uafd->next;
	}
	char *buf = uwsgi_malloc(uwsgi.alarm_msg_size + sizeof(long));
	for (;;) {
		int interesting_fd = -1;
                int ret = event_queue_wait(ut->queue, -1, &interesting_fd);
		if (ret > 0) {
			if (interesting_fd == ut->pipe[1]) {
				ssize_t len = read(ut->pipe[1], buf, uwsgi.alarm_msg_size + sizeof(long));
				if (len > (ssize_t)(sizeof(long) + 1)) {
					size_t msg_size = len - sizeof(long);
					char *msg = buf + sizeof(long);
					long ptr = 0;
					memcpy(&ptr, buf, sizeof(long));
					struct uwsgi_alarm_instance *uai = (struct uwsgi_alarm_instance *) ptr;
					if (!uai)
						break;
					uwsgi_alarm_run(uai, msg, msg_size);
				}
			}
			// check for alarm_fd
			else {
				uafd = uwsgi.alarm_fds;
				int fd_read = 0;
				while(uafd) {
					if (interesting_fd == uafd->fd) {
						if (fd_read) goto raise;	
						size_t remains = uafd->buf_len;
						while(remains) {
							ssize_t len = read(uafd->fd, uafd->buf + (uafd->buf_len-remains), remains);
							if (len <= 0) {
								uwsgi_error("[uwsgi-alarm-fd]/read()");
								uwsgi_log("[uwsgi-alarm-fd] i will stop monitoring fd %d\n", uafd->fd);
								event_queue_del_fd(ut->queue, uafd->fd, event_queue_read());
								break;
							}
							remains-=len;
						}
						fd_read = 1;
raise:
						uwsgi_alarm_run(uafd->alarm, uafd->msg, uafd->msg_len);
					}
					uafd = uafd->next;
				}
			}
		}
	}
	free(buf);
}

// initialize alarms, instances and log regexps
void uwsgi_alarms_init() {

	if (!uwsgi.master_process) return;

	// first of all, create instance of alarms
	struct uwsgi_string_list *usl = uwsgi.alarm_list;
	while (usl) {
		char *line = uwsgi_str(usl->value);
		char *space = strchr(line, ' ');
		if (!space) {
			uwsgi_log("invalid alarm syntax: %s\n", usl->value);
			exit(1);
		}
		*space = 0;
		char *plugin = space + 1;
		char *colon = strchr(plugin, ':');
		if (!colon) {
			uwsgi_log("invalid alarm syntax: %s\n", usl->value);
			exit(1);
		}
		*colon = 0;
		char *arg = colon + 1;
		// here the alarm is mapped to a name and initialized
		if (uwsgi_alarm_add(line, plugin, arg)) {
			uwsgi_log("invalid alarm: %s\n", usl->value);
			exit(1);
		}
		usl = usl->next;
	}

	if (!uwsgi.alarm_instances) return;

	// map alarm file descriptors
	usl = uwsgi.alarm_fd_list;
	while(usl) {
		char *space0 = strchr(usl->value, ' ');
		if (!space0) {
			uwsgi_log("invalid alarm-fd syntax: %s\n", usl->value);
			exit(1);
		}
		*space0 = 0;
		size_t buf_len = 1;
		char *space1 = strchr(space0+1, ' ');
		if (!space1) {
			uwsgi_log("invalid alarm-fd syntax: %s\n", usl->value);
                        exit(1);
		}

		char *colon = strchr(space0+1, ':');
		if (colon) {
			buf_len = strtoul(colon+1, NULL, 10);
			*colon = 0;
		}
		int fd = atoi(space0+1);
		uwsgi_add_alarm_fd(fd, usl->value, buf_len, space1+1, strlen(space1+1));
		*space0 = ' ';
		*space1 = ' ';
		if (colon) {
			*colon = ':';
		}
		usl = usl->next;
	}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	// then map log-alarm
	usl = uwsgi.alarm_logs_list;
	while (usl) {
		char *line = uwsgi_str(usl->value);
		char *space = strchr(line, ' ');
		if (!space) {
			uwsgi_log("invalid log-alarm syntax: %s\n", usl->value);
			exit(1);
		}
		*space = 0;
		char *regexp = space + 1;
		// here the log-alarm is created
		if (uwsgi_alarm_log_add(line, regexp, usl->custom)) {
			uwsgi_log("invalid log-alarm: %s\n", usl->value);
			exit(1);
		}

		usl = usl->next;
	}
#endif
}

void uwsgi_alarm_thread_start() {
	if (!uwsgi.alarm_instances) return;
	// start the alarm_thread
	uwsgi.alarm_thread = uwsgi_thread_new(uwsgi_alarm_thread_loop);
	if (!uwsgi.alarm_thread) {
		uwsgi_log("unable to spawn alarm thread\n");
		exit(1);
	}
}

void uwsgi_alarm_trigger_uai(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	struct iovec iov[2];
	iov[0].iov_base = &uai;
	iov[0].iov_len = sizeof(long);
	iov[1].iov_base = msg;
	iov[1].iov_len = len;

	// now send the message to the alarm thread
	if (writev(uwsgi.alarm_thread->pipe[0], iov, 2) != (ssize_t) (len+sizeof(long))) {
		uwsgi_error("[uwsgi-alarm-error] uwsgi_alarm_trigger()/writev()");
	}
}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
// check if a log should raise an alarm
void uwsgi_alarm_log_check(char *msg, size_t len) {
	if (!uwsgi_strncmp(msg, len, "[uwsgi-alarm", 12))
		return;
	struct uwsgi_alarm_log *ual = uwsgi.alarm_logs;
	while (ual) {
		if (uwsgi_regexp_match(ual->pattern, msg, len) >= 0) {
			if (!ual->negate) {
				struct uwsgi_alarm_ll *uall = ual->alarms;
				while (uall) {
					if (uwsgi.alarm_cheap)
						uwsgi_alarm_trigger_uai(uall->alarm, msg, len);
					else
						uwsgi_alarm_run(uall->alarm, msg, len);
					uall = uall->next;
				}
			}
			else {
				break;
			}
		}
		ual = ual->next;
	}
}
#endif

// call the alarm func
void uwsgi_alarm_run(struct uwsgi_alarm_instance *uai, char *msg, size_t len) {
	time_t now = uwsgi_now();
	// avoid alarm storming/loop if last message is the same
	if (!uwsgi_strncmp(msg, len, uai->last_msg, uai->last_msg_size)) {
		if (now - uai->last_run < uwsgi.alarm_freq)
			return;
	}
	uai->alarm->func(uai, msg, len);
	uai->last_run = uwsgi_now();
	memcpy(uai->last_msg, msg, len);
	uai->last_msg_size = len;
}

// this is the api function workers,mules and whatever you want can call from code
void uwsgi_alarm_trigger(char *alarm_instance_name, char *msg, size_t len) {
	if (!uwsgi.alarm_thread) return;
	if (len > uwsgi.alarm_msg_size) return;
	struct uwsgi_alarm_instance *uai = uwsgi_alarm_get_instance(alarm_instance_name);
	if (!uai) return;

	uwsgi_alarm_trigger_uai(uai, msg, len);
}

struct uwsgi_alarm_fd *uwsgi_add_alarm_fd(int fd, char *alarm, size_t buf_len, char *msg, size_t msg_len) {
	struct uwsgi_alarm_fd *old_uafd = NULL, *uafd = uwsgi.alarm_fds;
	struct uwsgi_alarm_instance *uai = uwsgi_alarm_get_instance(alarm);
	if (!uai) {
		uwsgi_log("unable to find alarm \"%s\"\n", alarm);
		exit(1);
	}

	if (!buf_len) buf_len = 1;	

	while(uafd) {
		// check if an equal alarm has been added
		if (uafd->fd == fd && uafd->alarm == uai) {
			return uafd;
		}
		old_uafd = uafd;
		uafd = uafd->next;
	}

	uafd = uwsgi_calloc(sizeof(struct uwsgi_alarm_fd));
	uafd->fd = fd;
	uafd->buf = uwsgi_malloc(buf_len);
	uafd->buf_len = buf_len;
	uafd->msg = msg;
	uafd->msg_len = msg_len;
	uafd->alarm = uai;

	if (!old_uafd) {
		uwsgi.alarm_fds = uafd;
	}
	else {
		old_uafd->next = uafd;
	}

	// avoid the fd to be closed
	uwsgi_add_safe_fd(fd);
	uwsgi_log("[uwsgi-alarm] added fd %d\n", fd);

	return uafd;
}
