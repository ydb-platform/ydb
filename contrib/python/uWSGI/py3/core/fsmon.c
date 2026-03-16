#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

#ifdef UWSGI_EVENT_FILEMONITOR_USE_INOTIFY
#ifndef OBSOLETE_LINUX_KERNEL
#include <sys/inotify.h>
#endif
#endif

static int fsmon_add(struct uwsgi_fsmon *fs) {
#ifdef UWSGI_EVENT_FILEMONITOR_USE_INOTIFY
#ifndef OBSOLETE_LINUX_KERNEL
	static int inotify_fd = -1;
	if (inotify_fd == -1) {
		inotify_fd = inotify_init();
		if (inotify_fd < 0) {
			uwsgi_error("fsmon_add()/inotify_init()");
			return -1;
		}
		if (event_queue_add_fd_read(uwsgi.master_queue, inotify_fd)) {
			uwsgi_error("fsmon_add()/event_queue_add_fd_read()");
			return -1;
		}
	}
	int wd = inotify_add_watch(inotify_fd, fs->path, IN_ATTRIB | IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY | IN_MOVE_SELF | IN_MOVED_FROM | IN_MOVED_TO);
	if (wd < 0) {
		uwsgi_error("fsmon_add()/inotify_add_watch()");
		return -1;
	}
	fs->fd = inotify_fd;
	fs->id = wd;
	return 0;
#endif
#endif
#ifdef UWSGI_EVENT_FILEMONITOR_USE_KQUEUE
	struct kevent kev;
	int fd = open(fs->path, O_RDONLY);
	if (fd < 0) {
		uwsgi_error_open(fs->path);
		uwsgi_error("fsmon_add()/open()");
		return -1;
	}

	EV_SET(&kev, fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE | NOTE_DELETE | NOTE_EXTEND | NOTE_ATTRIB | NOTE_RENAME | NOTE_REVOKE, 0, 0);
	if (kevent(uwsgi.master_queue, &kev, 1, NULL, 0, NULL) < 0) {
		uwsgi_error("fsmon_add()/kevent()");
		return -1;
	}
	fs->fd = fd;
	return 0;
#endif
	uwsgi_log("[uwsgi-fsmon] filesystem monitoring interface not available in this platform !!!\n");
	return 1;
}

static void fsmon_reload(struct uwsgi_fsmon *fs) {
	uwsgi_block_signal(SIGHUP);
	grace_them_all(0);
	uwsgi_unblock_signal(SIGHUP);
}

static void fsmon_brutal_reload(struct uwsgi_fsmon *fs) {
	if (uwsgi.die_on_term) {
		uwsgi_block_signal(SIGQUIT);
		reap_them_all(0);
		uwsgi_unblock_signal(SIGQUIT);
	}
	else {
		uwsgi_block_signal(SIGTERM);
		reap_them_all(0);
		uwsgi_unblock_signal(SIGTERM);
	}
}

static void fsmon_signal(struct uwsgi_fsmon *fs) {
	uwsgi_route_signal(atoi((char *) fs->data));
}

void uwsgi_fsmon_setup() {
	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, uwsgi.fs_reload) {
		uwsgi_register_fsmon(usl->value, fsmon_reload, NULL);
	}
	uwsgi_foreach(usl, uwsgi.fs_brutal_reload) {
		uwsgi_register_fsmon(usl->value, fsmon_brutal_reload, NULL);
	}
	uwsgi_foreach(usl, uwsgi.fs_signal) {
		char *copy = uwsgi_str(usl->value);
		char *space = strchr(copy, ' ');
		if (!space) {
			uwsgi_log("[uwsgi-fsmon] invalid syntax: \"%s\"\n", usl->value);
			free(copy);
			continue;
		}
		*space = 0;
		uwsgi_register_fsmon(copy, fsmon_signal, space + 1);
	}

	struct uwsgi_fsmon *fs = uwsgi.fsmon;
	while (fs) {
		if (fsmon_add(fs)) {
			uwsgi_log("[uwsgi-fsmon] unable to register monitor for \"%s\"\n", fs->path);
		}
		else {
			uwsgi_log("[uwsgi-fsmon] registered monitor for \"%s\"\n", fs->path);
		}
		fs = fs->next;
	}
}


struct uwsgi_fsmon *uwsgi_register_fsmon(char *path, void (*func) (struct uwsgi_fsmon *), void *data) {
	struct uwsgi_fsmon *old_fs = NULL, *fs = uwsgi.fsmon;
	while(fs) {
		old_fs = fs;
		fs = fs->next;
	}

	fs = uwsgi_calloc(sizeof(struct uwsgi_fsmon));
	fs->path = path;
	fs->func = func;
	fs->data = data;
	
	if (old_fs) {
		old_fs->next = fs;
	}
	else {
		uwsgi.fsmon = fs;
	}

	return fs;
}

static struct uwsgi_fsmon *uwsgi_fsmon_ack(int interesting_fd) {
	struct uwsgi_fsmon *found_fs = NULL;
	struct uwsgi_fsmon *fs = uwsgi.fsmon;
	while (fs) {
		if (fs->fd == interesting_fd) {
			found_fs = fs;
			break;
		}
		fs = fs->next;
	}

#ifdef UWSGI_EVENT_FILEMONITOR_USE_INOTIFY
#ifndef OBSOLETE_LINUX_KERNEL
	if (!found_fs)
		return NULL;
	found_fs = NULL;
	unsigned int isize = 0;
	if (ioctl(interesting_fd, FIONREAD, &isize) < 0) {
		uwsgi_error("uwsgi_fsmon_ack()/ioctl()");
		return NULL;
	}
	if (isize == 0)
		return NULL;
	struct inotify_event *ie = uwsgi_malloc(isize);
	// read from the inotify descriptor
	ssize_t len = read(interesting_fd, ie, isize);
	if (len < 0) {
		free(ie);
		uwsgi_error("uwsgi_fsmon_ack()/read()");
		return NULL;
	}
	fs = uwsgi.fsmon;
	while (fs) {
		if (fs->fd == interesting_fd && fs->id == ie->wd) {
			found_fs = fs;
			break;
		}
		fs = fs->next;
	}
	free(ie);
#endif
#endif
	return found_fs;
}

int uwsgi_fsmon_event(int interesting_fd) {

	struct uwsgi_fsmon *fs = uwsgi_fsmon_ack(interesting_fd);
	if (!fs)
		return 0;

	uwsgi_log_verbose("[uwsgi-fsmon] detected event on \"%s\"\n", fs->path);
	fs->func(fs);
	return 1;
}
