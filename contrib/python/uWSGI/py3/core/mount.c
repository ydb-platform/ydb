#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	jail systems (Linux namespaces, FreeBSD jails...) heavily rely on mount/umount

	to simplify setups (expecially because the mount command could not be available in
	the initial phase of jailing, we need an api to mount/umount filesystems

	int uwsgi_mount(char *fs, char *what, char *where, int flags);
	int uwsgi_umount(char *where, int flags);

*/

#ifdef __linux__
#ifndef MS_REC
#define MS_REC 16384
#endif
#include <sys/statfs.h>
#endif

struct uwsgi_mount_flag {
	char *key;
	uint64_t value;
};

static struct uwsgi_mount_flag umflags[] = {
#ifdef MS_BIND
	{"bind", MS_BIND},
#endif
#ifdef MS_DIRSYNC
	{"dirsync", MS_DIRSYNC},
#endif
#ifdef MS_MANDLOCK
	{"mandlock", MS_MANDLOCK},
#endif
#ifdef MS_MOVE
	{"move", MS_MOVE},
#endif
#ifdef MS_NOATIME
	{"noatime", MS_NOATIME},
#endif
#ifdef MS_NODEV
	{"nodev", MS_NODEV},
#endif
#ifdef MS_NOEXEC
	{"noexec", MS_NOEXEC},
#endif
#ifdef MS_RDONLY
	{"rdonly", MS_RDONLY},
	{"readonly", MS_RDONLY},
	{"ro", MS_RDONLY},
#endif
#ifdef MS_REMOUNT
	{"remount", MS_REMOUNT},
#endif
#ifdef MS_NOSUID
	{"nosuid", MS_NOSUID},
#endif
#ifdef MS_SYNCHRONOUS
	{"sync", MS_SYNCHRONOUS},
#endif
#ifdef MNT_FORCE
	{"force", MNT_FORCE},
#endif
#ifdef MNT_DETACH
	{"detach", MNT_DETACH},
#endif
#ifdef MS_REC
	{"rec", MS_REC},
	{"recursive", MS_REC},
#endif
#ifdef MS_PRIVATE
	{"private", MS_PRIVATE},
#endif
#ifdef MS_SHARED
	{"shared", MS_SHARED},
#endif
#ifdef MNT_RDONLY
	{"rdonly", MNT_RDONLY},
	{"readonly", MNT_RDONLY},
	{"ro", MNT_RDONLY},
#endif
#ifdef MNT_NOEXEC
	{"noexec", MNT_NOEXEC},
#endif
#ifdef MNT_NOSUID
	{"nosuid", MNT_NOSUID},
#endif
#ifdef MNT_NOATIME
	{"noatime", MNT_NOATIME},
#endif
#ifdef MNT_SNAPSHOT
	{"snapshot", MNT_SNAPSHOT},
#endif
	{NULL, 0},
};

uint64_t uwsgi_mount_flag(char *mflag) {
	struct uwsgi_mount_flag *umf = umflags;
	while(umf->key) {
		if (!strcmp(umf->key, mflag)) return umf->value;
		umf++; 
	}
	return 0;
}

int uwsgi_mount(char *fs, char *what, char *where, char *flags, char *data) {
#if defined(__linux__) || defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
#if defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
	struct iovec iov[6];
#endif
	unsigned long mountflags = 0;
	if (!flags) goto parsed;
	char *mflags = uwsgi_str(flags);
	char *p, *ctx = NULL;
	uwsgi_foreach_token(mflags, ",", p, ctx) {
		unsigned long flag = (unsigned long) uwsgi_mount_flag(p);
		if (!flag) {
			uwsgi_log("unknown mount flag \"%s\"\n", p);
			exit(1);
		}
		mountflags |= flag;
	}
	free(mflags);
parsed:
#ifdef __linux__
	return mount(what, where, fs, mountflags, data);
#elif defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
	iov[0].iov_base = "fstype";
	iov[0].iov_len = 7;
	iov[1].iov_base = fs;
	iov[1].iov_len = strlen(fs) + 1;

	iov[2].iov_base = "fspath";
	iov[2].iov_len = 7;
	iov[3].iov_base = where;
	iov[3].iov_len = strlen(where) + 1;

	iov[4].iov_base = "target";
        iov[4].iov_len = 7;
        iov[5].iov_base = what;
        iov[5].iov_len = strlen(what) + 1;

	return nmount(iov, 6, (int) mountflags);
#endif
#endif
	return -1;
}

int uwsgi_umount(char *where, char *flags) {
#if defined(__linux__) || defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
	unsigned long mountflags = 0;
        if (!flags) goto parsed;
        char *mflags = uwsgi_str(flags);
        char *p, *ctx = NULL;
	uwsgi_foreach_token(mflags, ",", p, ctx) {
                unsigned long flag = (unsigned long) uwsgi_mount_flag(p);
                if (!flag) {
                        uwsgi_log("unknown umount flag \"%s\"\n", p);
                        exit(1);
                }
                mountflags |= flag;
        }
        free(mflags);
parsed:
#ifdef __linux__
	// we need a special case for recursive umount
	if (mountflags & MS_REC) {
		mountflags &= ~MS_REC;
		int unmounted = 1;
		char *slashed = uwsgi_concat2(where, "/");
		while (unmounted) {
                	unmounted = 0;
                	FILE *procmounts = fopen("/proc/self/mounts", "r");
                	if (!procmounts) {
				uwsgi_log("the /proc filesystem must be mounted for recursive umount\n");
				uwsgi_error("open()");
				exit(1);
			}
			char line[1024];
                	while (fgets(line, 1024, procmounts) != NULL) {
                        	char *delim0 = strchr(line, ' ');
                        	if (!delim0) continue;
                        	delim0++;
                        	char *delim1 = strchr(delim0, ' ');
                        	if (!delim1) continue;
                        	*delim1 = 0;
				if (!uwsgi_starts_with(delim0, strlen(delim0), slashed, strlen(slashed))) goto unmountable; 
                                continue;
unmountable:
                        	if (!umount2(delim0, mountflags)) unmounted++;
                	}
                	fclose(procmounts);
        	}
		free(slashed);
	}
        return umount2(where, mountflags);
#elif defined(__FreeBSD__) || defined(__GNU_kFreeBSD__)
	return unmount(where, mountflags);
#endif
#endif
        return -1;
}

int uwsgi_mount_hook(char *arg) {
	char *data = NULL;
	char *tmp_arg = uwsgi_str(arg);
	char *fs = tmp_arg;
	char *what = strchr(fs, ' ');
	if (!what) goto error;
	*what = 0; what++;
	char *where = strchr(what, ' ');
	if (!where) goto error;
	*where = 0; where++;
	char *flags = strchr(where, ' ');
	if (flags) {
		*flags = 0; flags++;
		data = strchr(flags, ' ');
		if (data) {
			*data = 0; data++;
		}
	}
	if (uwsgi_mount(fs, what, where, flags, data)) {
		uwsgi_error("uwsgi_mount()");
		free(tmp_arg);
		return -1;
	}	
	free(tmp_arg);
	return 0;
error:
	free(tmp_arg);
	uwsgi_log("uwsgi_mount_hook() invalid syntax\n");
	return -1;
}

int uwsgi_umount_hook(char *arg) {
	char *tmp_arg = uwsgi_str(arg);
	char *where = tmp_arg;
        char *flags = strchr(where, ' ');
        if (flags) {
        	*flags = 0; flags++;
	}
        if (uwsgi_umount(where, flags)) {
                uwsgi_error("uwsgi_umount()");
                free(tmp_arg);
                return -1;
        }       
        free(tmp_arg);
        return 0;
}

int uwsgi_check_mountpoint(char *mountpoint) {
#ifdef __linux__
	struct statfs sfs;
	int ret = statfs(mountpoint, &sfs);
	if (ret) {
		uwsgi_error("uwsgi_check_mountpoint()/statfs()");
		return -1;
	}
	return 0;
#else
	uwsgi_log("this platform does not support mountpoints check !!!\n");
	return -1;
#endif
}
