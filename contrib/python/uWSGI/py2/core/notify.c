#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;



void uwsgi_systemd_notify(char *message) {

	struct msghdr *msghdr = (struct msghdr *) uwsgi.notification_object;
	struct iovec *iovec = msghdr->msg_iov;

	iovec[0].iov_base = "STATUS=";
	iovec[0].iov_len = 7;

	iovec[1].iov_base = message;
	iovec[1].iov_len = strlen(message);

	iovec[2].iov_base = "\n";
	iovec[2].iov_len = 1;

	msghdr->msg_iovlen = 3;

	if (sendmsg(uwsgi.notification_fd, msghdr, 0) < 0) {
		uwsgi_error("sendmsg()");
	}
}

void uwsgi_systemd_notify_ready(void) {

	struct msghdr *msghdr = (struct msghdr *) uwsgi.notification_object;
	struct iovec *iovec = msghdr->msg_iov;

	iovec[0].iov_base = "STATUS=uWSGI is ready\nREADY=1\n";
	iovec[0].iov_len = 30;

	msghdr->msg_iovlen = 1;

	if (sendmsg(uwsgi.notification_fd, msghdr, 0) < 0) {
		uwsgi_error("sendmsg()");
	}
}


void uwsgi_systemd_init(char *systemd_socket) {

	struct sockaddr_un *sd_sun;
	struct msghdr *msghdr;

	uwsgi.notification_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
	if (uwsgi.notification_fd < 0) {
		uwsgi_error("socket()");
		return;
	}

	size_t len = strlen(systemd_socket);
	sd_sun = uwsgi_malloc(sizeof(struct sockaddr_un));
	memset(sd_sun, 0, sizeof(struct sockaddr_un));
	sd_sun->sun_family = AF_UNIX;
	strncpy(sd_sun->sun_path, systemd_socket, UMIN(len, sizeof(sd_sun->sun_path)));
	if (sd_sun->sun_path[0] == '@')
		sd_sun->sun_path[0] = 0;

	msghdr = uwsgi_malloc(sizeof(struct msghdr));
	memset(msghdr, 0, sizeof(struct msghdr));

	msghdr->msg_iov = uwsgi_malloc(sizeof(struct iovec) * 3);
	memset(msghdr->msg_iov, 0, sizeof(struct iovec) * 3);

	msghdr->msg_name = sd_sun;
	msghdr->msg_namelen = sizeof(struct sockaddr_un) - (sizeof(sd_sun->sun_path) - len);

	uwsgi.notification_object = msghdr;

	uwsgi.notify = uwsgi_systemd_notify;
	uwsgi.notify_ready = uwsgi_systemd_notify_ready;

}

int uwsgi_notify_socket_manage(int fd) {
	char buf[8192];
        ssize_t rlen = read(fd, buf, 8192);
        if (rlen < 0) {
                if (uwsgi_is_again()) return 0;
                uwsgi_error("uwsgi_notify_socket_manage()/read()");
                exit(1);
        }

	if (rlen > 0) {
		uwsgi_log_verbose("[notify-socket] %.*s\n", rlen, buf);
        }

        return 0;
}

int uwsgi_notify_msg(char *dst, char *msg, size_t len) {
	static int notify_fd = -1;
	if (notify_fd < 0) {
		notify_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
		if (notify_fd < 0) {
			uwsgi_error("uwsgi_notify_msg()/socket()");
			return -1;
		}
	}
	struct sockaddr_un un_addr;
	memset(&un_addr, 0, sizeof(struct sockaddr_un));
        un_addr.sun_family = AF_UNIX;
        // use 102 as the magic number
        strncat(un_addr.sun_path, dst, 102);
        if (sendto(notify_fd, msg, len, 0, (struct sockaddr *) &un_addr, sizeof(un_addr)) < 0) {
		return -1;
	}
	return 0;
}
