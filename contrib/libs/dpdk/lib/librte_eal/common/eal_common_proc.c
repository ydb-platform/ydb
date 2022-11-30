#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2016-2018 Intel Corporation
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <libgen.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <rte_alarm.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_tailq.h>

#include "eal_memcfg.h"
#include "eal_private.h"
#include "eal_filesystem.h"
#include "eal_internal_cfg.h"

static int mp_fd = -1;
static char mp_filter[PATH_MAX];   /* Filter for secondary process sockets */
static char mp_dir_path[PATH_MAX]; /* The directory path for all mp sockets */
static pthread_mutex_t mp_mutex_action = PTHREAD_MUTEX_INITIALIZER;
static char peer_name[PATH_MAX];

struct action_entry {
	TAILQ_ENTRY(action_entry) next;
	char action_name[RTE_MP_MAX_NAME_LEN];
	rte_mp_t action;
};

/** Double linked list of actions. */
TAILQ_HEAD(action_entry_list, action_entry);

static struct action_entry_list action_entry_list =
	TAILQ_HEAD_INITIALIZER(action_entry_list);

enum mp_type {
	MP_MSG, /* Share message with peers, will not block */
	MP_REQ, /* Request for information, Will block for a reply */
	MP_REP, /* Response to previously-received request */
	MP_IGN, /* Response telling requester to ignore this response */
};

struct mp_msg_internal {
	int type;
	struct rte_mp_msg msg;
};

struct async_request_param {
	rte_mp_async_reply_t clb;
	struct rte_mp_reply user_reply;
	struct timespec end;
	int n_responses_processed;
};

struct pending_request {
	TAILQ_ENTRY(pending_request) next;
	enum {
		REQUEST_TYPE_SYNC,
		REQUEST_TYPE_ASYNC
	} type;
	char dst[PATH_MAX];
	struct rte_mp_msg *request;
	struct rte_mp_msg *reply;
	int reply_received;
	RTE_STD_C11
	union {
		struct {
			struct async_request_param *param;
		} async;
		struct {
			pthread_cond_t cond;
		} sync;
	};
};

TAILQ_HEAD(pending_request_list, pending_request);

static struct {
	struct pending_request_list requests;
	pthread_mutex_t lock;
} pending_requests = {
	.requests = TAILQ_HEAD_INITIALIZER(pending_requests.requests),
	.lock = PTHREAD_MUTEX_INITIALIZER,
	/**< used in async requests only */
};

/* forward declarations */
static int
mp_send(struct rte_mp_msg *msg, const char *peer, int type);

/* for use with alarm callback */
static void
async_reply_handle(void *arg);

/* for use with process_msg */
static struct pending_request *
async_reply_handle_thread_unsafe(void *arg);

static void
trigger_async_action(struct pending_request *req);

static struct pending_request *
find_pending_request(const char *dst, const char *act_name)
{
	struct pending_request *r;

	TAILQ_FOREACH(r, &pending_requests.requests, next) {
		if (!strcmp(r->dst, dst) &&
		    !strcmp(r->request->name, act_name))
			break;
	}

	return r;
}

static void
create_socket_path(const char *name, char *buf, int len)
{
	const char *prefix = eal_mp_socket_path();

	if (strlen(name) > 0)
		snprintf(buf, len, "%s_%s", prefix, name);
	else
		strlcpy(buf, prefix, len);
}

int
rte_eal_primary_proc_alive(const char *config_file_path)
{
	int config_fd;

	if (config_file_path)
		config_fd = open(config_file_path, O_RDONLY);
	else {
		const char *path;

		path = eal_runtime_config_path();
		config_fd = open(path, O_RDONLY);
	}
	if (config_fd < 0)
		return 0;

	int ret = lockf(config_fd, F_TEST, 0);
	close(config_fd);

	return !!ret;
}

static struct action_entry *
find_action_entry_by_name(const char *name)
{
	struct action_entry *entry;

	TAILQ_FOREACH(entry, &action_entry_list, next) {
		if (strncmp(entry->action_name, name, RTE_MP_MAX_NAME_LEN) == 0)
			break;
	}

	return entry;
}

static int
validate_action_name(const char *name)
{
	if (name == NULL) {
		RTE_LOG(ERR, EAL, "Action name cannot be NULL\n");
		rte_errno = EINVAL;
		return -1;
	}
	if (strnlen(name, RTE_MP_MAX_NAME_LEN) == 0) {
		RTE_LOG(ERR, EAL, "Length of action name is zero\n");
		rte_errno = EINVAL;
		return -1;
	}
	if (strnlen(name, RTE_MP_MAX_NAME_LEN) == RTE_MP_MAX_NAME_LEN) {
		rte_errno = E2BIG;
		return -1;
	}
	return 0;
}

int
rte_mp_action_register(const char *name, rte_mp_t action)
{
	struct action_entry *entry;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (validate_action_name(name) != 0)
		return -1;

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		rte_errno = ENOTSUP;
		return -1;
	}

	entry = malloc(sizeof(struct action_entry));
	if (entry == NULL) {
		rte_errno = ENOMEM;
		return -1;
	}
	strlcpy(entry->action_name, name, sizeof(entry->action_name));
	entry->action = action;

	pthread_mutex_lock(&mp_mutex_action);
	if (find_action_entry_by_name(name) != NULL) {
		pthread_mutex_unlock(&mp_mutex_action);
		rte_errno = EEXIST;
		free(entry);
		return -1;
	}
	TAILQ_INSERT_TAIL(&action_entry_list, entry, next);
	pthread_mutex_unlock(&mp_mutex_action);
	return 0;
}

void
rte_mp_action_unregister(const char *name)
{
	struct action_entry *entry;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (validate_action_name(name) != 0)
		return;

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		return;
	}

	pthread_mutex_lock(&mp_mutex_action);
	entry = find_action_entry_by_name(name);
	if (entry == NULL) {
		pthread_mutex_unlock(&mp_mutex_action);
		return;
	}
	TAILQ_REMOVE(&action_entry_list, entry, next);
	pthread_mutex_unlock(&mp_mutex_action);
	free(entry);
}

static int
read_msg(struct mp_msg_internal *m, struct sockaddr_un *s)
{
	int msglen;
	struct iovec iov;
	struct msghdr msgh;
	char control[CMSG_SPACE(sizeof(m->msg.fds))];
	struct cmsghdr *cmsg;
	int buflen = sizeof(*m) - sizeof(m->msg.fds);

	memset(&msgh, 0, sizeof(msgh));
	iov.iov_base = m;
	iov.iov_len  = buflen;

	msgh.msg_name = s;
	msgh.msg_namelen = sizeof(*s);
	msgh.msg_iov = &iov;
	msgh.msg_iovlen = 1;
	msgh.msg_control = control;
	msgh.msg_controllen = sizeof(control);

	msglen = recvmsg(mp_fd, &msgh, 0);
	if (msglen < 0) {
		RTE_LOG(ERR, EAL, "recvmsg failed, %s\n", strerror(errno));
		return -1;
	}

	if (msglen != buflen || (msgh.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
		RTE_LOG(ERR, EAL, "truncated msg\n");
		return -1;
	}

	/* read auxiliary FDs if any */
	for (cmsg = CMSG_FIRSTHDR(&msgh); cmsg != NULL;
		cmsg = CMSG_NXTHDR(&msgh, cmsg)) {
		if ((cmsg->cmsg_level == SOL_SOCKET) &&
			(cmsg->cmsg_type == SCM_RIGHTS)) {
			memcpy(m->msg.fds, CMSG_DATA(cmsg), sizeof(m->msg.fds));
			break;
		}
	}
	/* sanity-check the response */
	if (m->msg.num_fds < 0 || m->msg.num_fds > RTE_MP_MAX_FD_NUM) {
		RTE_LOG(ERR, EAL, "invalid number of fd's received\n");
		return -1;
	}
	if (m->msg.len_param < 0 || m->msg.len_param > RTE_MP_MAX_PARAM_LEN) {
		RTE_LOG(ERR, EAL, "invalid received data length\n");
		return -1;
	}
	return 0;
}

static void
process_msg(struct mp_msg_internal *m, struct sockaddr_un *s)
{
	struct pending_request *pending_req;
	struct action_entry *entry;
	struct rte_mp_msg *msg = &m->msg;
	rte_mp_t action = NULL;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	RTE_LOG(DEBUG, EAL, "msg: %s\n", msg->name);

	if (m->type == MP_REP || m->type == MP_IGN) {
		struct pending_request *req = NULL;

		pthread_mutex_lock(&pending_requests.lock);
		pending_req = find_pending_request(s->sun_path, msg->name);
		if (pending_req) {
			memcpy(pending_req->reply, msg, sizeof(*msg));
			/* -1 indicates that we've been asked to ignore */
			pending_req->reply_received =
				m->type == MP_REP ? 1 : -1;

			if (pending_req->type == REQUEST_TYPE_SYNC)
				pthread_cond_signal(&pending_req->sync.cond);
			else if (pending_req->type == REQUEST_TYPE_ASYNC)
				req = async_reply_handle_thread_unsafe(
						pending_req);
		} else
			RTE_LOG(ERR, EAL, "Drop mp reply: %s\n", msg->name);
		pthread_mutex_unlock(&pending_requests.lock);

		if (req != NULL)
			trigger_async_action(req);
		return;
	}

	pthread_mutex_lock(&mp_mutex_action);
	entry = find_action_entry_by_name(msg->name);
	if (entry != NULL)
		action = entry->action;
	pthread_mutex_unlock(&mp_mutex_action);

	if (!action) {
		if (m->type == MP_REQ && !internal_conf->init_complete) {
			/* if this is a request, and init is not yet complete,
			 * and callback wasn't registered, we should tell the
			 * requester to ignore our existence because we're not
			 * yet ready to process this request.
			 */
			struct rte_mp_msg dummy;

			memset(&dummy, 0, sizeof(dummy));
			strlcpy(dummy.name, msg->name, sizeof(dummy.name));
			mp_send(&dummy, s->sun_path, MP_IGN);
		} else {
			RTE_LOG(ERR, EAL, "Cannot find action: %s\n",
				msg->name);
		}
	} else if (action(msg, s->sun_path) < 0) {
		RTE_LOG(ERR, EAL, "Fail to handle message: %s\n", msg->name);
	}
}

static void *
mp_handle(void *arg __rte_unused)
{
	struct mp_msg_internal msg;
	struct sockaddr_un sa;

	while (1) {
		if (read_msg(&msg, &sa) == 0)
			process_msg(&msg, &sa);
	}

	return NULL;
}

static int
timespec_cmp(const struct timespec *a, const struct timespec *b)
{
	if (a->tv_sec < b->tv_sec)
		return -1;
	if (a->tv_sec > b->tv_sec)
		return 1;
	if (a->tv_nsec < b->tv_nsec)
		return -1;
	if (a->tv_nsec > b->tv_nsec)
		return 1;
	return 0;
}

enum async_action {
	ACTION_FREE, /**< free the action entry, but don't trigger callback */
	ACTION_TRIGGER /**< trigger callback, then free action entry */
};

static enum async_action
process_async_request(struct pending_request *sr, const struct timespec *now)
{
	struct async_request_param *param;
	struct rte_mp_reply *reply;
	bool timeout, last_msg;

	param = sr->async.param;
	reply = &param->user_reply;

	/* did we timeout? */
	timeout = timespec_cmp(&param->end, now) <= 0;

	/* if we received a response, adjust relevant data and copy message. */
	if (sr->reply_received == 1 && sr->reply) {
		struct rte_mp_msg *msg, *user_msgs, *tmp;

		msg = sr->reply;
		user_msgs = reply->msgs;

		tmp = realloc(user_msgs, sizeof(*msg) *
				(reply->nb_received + 1));
		if (!tmp) {
			RTE_LOG(ERR, EAL, "Fail to alloc reply for request %s:%s\n",
				sr->dst, sr->request->name);
			/* this entry is going to be removed and its message
			 * dropped, but we don't want to leak memory, so
			 * continue.
			 */
		} else {
			user_msgs = tmp;
			reply->msgs = user_msgs;
			memcpy(&user_msgs[reply->nb_received],
					msg, sizeof(*msg));
			reply->nb_received++;
		}

		/* mark this request as processed */
		param->n_responses_processed++;
	} else if (sr->reply_received == -1) {
		/* we were asked to ignore this process */
		reply->nb_sent--;
	} else if (timeout) {
		/* count it as processed response, but don't increment
		 * nb_received.
		 */
		param->n_responses_processed++;
	}

	free(sr->reply);

	last_msg = param->n_responses_processed == reply->nb_sent;

	return last_msg ? ACTION_TRIGGER : ACTION_FREE;
}

static void
trigger_async_action(struct pending_request *sr)
{
	struct async_request_param *param;
	struct rte_mp_reply *reply;

	param = sr->async.param;
	reply = &param->user_reply;

	param->clb(sr->request, reply);

	/* clean up */
	free(sr->async.param->user_reply.msgs);
	free(sr->async.param);
	free(sr->request);
	free(sr);
}

static struct pending_request *
async_reply_handle_thread_unsafe(void *arg)
{
	struct pending_request *req = (struct pending_request *)arg;
	enum async_action action;
	struct timespec ts_now;
	struct timeval now;

	if (gettimeofday(&now, NULL) < 0) {
		RTE_LOG(ERR, EAL, "Cannot get current time\n");
		goto no_trigger;
	}
	ts_now.tv_nsec = now.tv_usec * 1000;
	ts_now.tv_sec = now.tv_sec;

	action = process_async_request(req, &ts_now);

	TAILQ_REMOVE(&pending_requests.requests, req, next);

	if (rte_eal_alarm_cancel(async_reply_handle, req) < 0) {
		/* if we failed to cancel the alarm because it's already in
		 * progress, don't proceed because otherwise we will end up
		 * handling the same message twice.
		 */
		if (rte_errno == EINPROGRESS) {
			RTE_LOG(DEBUG, EAL, "Request handling is already in progress\n");
			goto no_trigger;
		}
		RTE_LOG(ERR, EAL, "Failed to cancel alarm\n");
	}

	if (action == ACTION_TRIGGER)
		return req;
no_trigger:
	free(req);
	return NULL;
}

static void
async_reply_handle(void *arg)
{
	struct pending_request *req;

	pthread_mutex_lock(&pending_requests.lock);
	req = async_reply_handle_thread_unsafe(arg);
	pthread_mutex_unlock(&pending_requests.lock);

	if (req != NULL)
		trigger_async_action(req);
}

static int
open_socket_fd(void)
{
	struct sockaddr_un un;

	peer_name[0] = '\0';
	if (rte_eal_process_type() == RTE_PROC_SECONDARY)
		snprintf(peer_name, sizeof(peer_name),
				"%d_%"PRIx64, getpid(), rte_rdtsc());

	mp_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
	if (mp_fd < 0) {
		RTE_LOG(ERR, EAL, "failed to create unix socket\n");
		return -1;
	}

	memset(&un, 0, sizeof(un));
	un.sun_family = AF_UNIX;

	create_socket_path(peer_name, un.sun_path, sizeof(un.sun_path));

	unlink(un.sun_path); /* May still exist since last run */

	if (bind(mp_fd, (struct sockaddr *)&un, sizeof(un)) < 0) {
		RTE_LOG(ERR, EAL, "failed to bind %s: %s\n",
			un.sun_path, strerror(errno));
		close(mp_fd);
		return -1;
	}

	RTE_LOG(INFO, EAL, "Multi-process socket %s\n", un.sun_path);
	return mp_fd;
}

static void
close_socket_fd(void)
{
	char path[PATH_MAX];

	if (mp_fd < 0)
		return;

	close(mp_fd);
	create_socket_path(peer_name, path, sizeof(path));
	unlink(path);
}

int
rte_mp_channel_init(void)
{
	char path[PATH_MAX];
	int dir_fd;
	pthread_t mp_handle_tid;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* in no shared files mode, we do not have secondary processes support,
	 * so no need to initialize IPC.
	 */
	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC will be disabled\n");
		rte_errno = ENOTSUP;
		return -1;
	}

	/* create filter path */
	create_socket_path("*", path, sizeof(path));
	strlcpy(mp_filter, basename(path), sizeof(mp_filter));

	/* path may have been modified, so recreate it */
	create_socket_path("*", path, sizeof(path));
	strlcpy(mp_dir_path, dirname(path), sizeof(mp_dir_path));

	/* lock the directory */
	dir_fd = open(mp_dir_path, O_RDONLY);
	if (dir_fd < 0) {
		RTE_LOG(ERR, EAL, "failed to open %s: %s\n",
			mp_dir_path, strerror(errno));
		return -1;
	}

	if (flock(dir_fd, LOCK_EX)) {
		RTE_LOG(ERR, EAL, "failed to lock %s: %s\n",
			mp_dir_path, strerror(errno));
		close(dir_fd);
		return -1;
	}

	if (open_socket_fd() < 0) {
		close(dir_fd);
		return -1;
	}

	if (rte_ctrl_thread_create(&mp_handle_tid, "rte_mp_handle",
			NULL, mp_handle, NULL) < 0) {
		RTE_LOG(ERR, EAL, "failed to create mp thread: %s\n",
			strerror(errno));
		close(mp_fd);
		close(dir_fd);
		mp_fd = -1;
		return -1;
	}

	/* unlock the directory */
	flock(dir_fd, LOCK_UN);
	close(dir_fd);

	return 0;
}

void
rte_mp_channel_cleanup(void)
{
	close_socket_fd();
}

/**
 * Return -1, as fail to send message and it's caused by the local side.
 * Return 0, as fail to send message and it's caused by the remote side.
 * Return 1, as succeed to send message.
 *
 */
static int
send_msg(const char *dst_path, struct rte_mp_msg *msg, int type)
{
	int snd;
	struct iovec iov;
	struct msghdr msgh;
	struct cmsghdr *cmsg;
	struct sockaddr_un dst;
	struct mp_msg_internal m;
	int fd_size = msg->num_fds * sizeof(int);
	char control[CMSG_SPACE(fd_size)];

	m.type = type;
	memcpy(&m.msg, msg, sizeof(*msg));

	memset(&dst, 0, sizeof(dst));
	dst.sun_family = AF_UNIX;
	strlcpy(dst.sun_path, dst_path, sizeof(dst.sun_path));

	memset(&msgh, 0, sizeof(msgh));
	memset(control, 0, sizeof(control));

	iov.iov_base = &m;
	iov.iov_len = sizeof(m) - sizeof(msg->fds);

	msgh.msg_name = &dst;
	msgh.msg_namelen = sizeof(dst);
	msgh.msg_iov = &iov;
	msgh.msg_iovlen = 1;
	msgh.msg_control = control;
	msgh.msg_controllen = sizeof(control);

	cmsg = CMSG_FIRSTHDR(&msgh);
	cmsg->cmsg_len = CMSG_LEN(fd_size);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	memcpy(CMSG_DATA(cmsg), msg->fds, fd_size);

	do {
		snd = sendmsg(mp_fd, &msgh, 0);
	} while (snd < 0 && errno == EINTR);

	if (snd < 0) {
		rte_errno = errno;
		/* Check if it caused by peer process exits */
		if (errno == ECONNREFUSED &&
				rte_eal_process_type() == RTE_PROC_PRIMARY) {
			unlink(dst_path);
			return 0;
		}
		RTE_LOG(ERR, EAL, "failed to send to (%s) due to %s\n",
			dst_path, strerror(errno));
		return -1;
	}

	return 1;
}

static int
mp_send(struct rte_mp_msg *msg, const char *peer, int type)
{
	int dir_fd, ret = 0;
	DIR *mp_dir;
	struct dirent *ent;

	if (!peer && (rte_eal_process_type() == RTE_PROC_SECONDARY))
		peer = eal_mp_socket_path();

	if (peer) {
		if (send_msg(peer, msg, type) < 0)
			return -1;
		else
			return 0;
	}

	/* broadcast to all secondary processes */
	mp_dir = opendir(mp_dir_path);
	if (!mp_dir) {
		RTE_LOG(ERR, EAL, "Unable to open directory %s\n",
				mp_dir_path);
		rte_errno = errno;
		return -1;
	}

	dir_fd = dirfd(mp_dir);
	/* lock the directory to prevent processes spinning up while we send */
	if (flock(dir_fd, LOCK_SH)) {
		RTE_LOG(ERR, EAL, "Unable to lock directory %s\n",
			mp_dir_path);
		rte_errno = errno;
		closedir(mp_dir);
		return -1;
	}

	while ((ent = readdir(mp_dir))) {
		char path[PATH_MAX];

		if (fnmatch(mp_filter, ent->d_name, 0) != 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", mp_dir_path,
			 ent->d_name);
		if (send_msg(path, msg, type) < 0)
			ret = -1;
	}
	/* unlock the dir */
	flock(dir_fd, LOCK_UN);

	/* dir_fd automatically closed on closedir */
	closedir(mp_dir);
	return ret;
}

static int
check_input(const struct rte_mp_msg *msg)
{
	if (msg == NULL) {
		RTE_LOG(ERR, EAL, "Msg cannot be NULL\n");
		rte_errno = EINVAL;
		return -1;
	}

	if (validate_action_name(msg->name) != 0)
		return -1;

	if (msg->len_param < 0) {
		RTE_LOG(ERR, EAL, "Message data length is negative\n");
		rte_errno = EINVAL;
		return -1;
	}

	if (msg->num_fds < 0) {
		RTE_LOG(ERR, EAL, "Number of fd's is negative\n");
		rte_errno = EINVAL;
		return -1;
	}

	if (msg->len_param > RTE_MP_MAX_PARAM_LEN) {
		RTE_LOG(ERR, EAL, "Message data is too long\n");
		rte_errno = E2BIG;
		return -1;
	}

	if (msg->num_fds > RTE_MP_MAX_FD_NUM) {
		RTE_LOG(ERR, EAL, "Cannot send more than %d FDs\n",
			RTE_MP_MAX_FD_NUM);
		rte_errno = E2BIG;
		return -1;
	}

	return 0;
}

int
rte_mp_sendmsg(struct rte_mp_msg *msg)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (check_input(msg) != 0)
		return -1;

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		rte_errno = ENOTSUP;
		return -1;
	}

	RTE_LOG(DEBUG, EAL, "sendmsg: %s\n", msg->name);
	return mp_send(msg, NULL, MP_MSG);
}

static int
mp_request_async(const char *dst, struct rte_mp_msg *req,
		struct async_request_param *param, const struct timespec *ts)
{
	struct rte_mp_msg *reply_msg;
	struct pending_request *pending_req, *exist;
	int ret = -1;

	pending_req = calloc(1, sizeof(*pending_req));
	reply_msg = calloc(1, sizeof(*reply_msg));
	if (pending_req == NULL || reply_msg == NULL) {
		RTE_LOG(ERR, EAL, "Could not allocate space for sync request\n");
		rte_errno = ENOMEM;
		ret = -1;
		goto fail;
	}

	pending_req->type = REQUEST_TYPE_ASYNC;
	strlcpy(pending_req->dst, dst, sizeof(pending_req->dst));
	pending_req->request = req;
	pending_req->reply = reply_msg;
	pending_req->async.param = param;

	/* queue already locked by caller */

	exist = find_pending_request(dst, req->name);
	if (exist) {
		RTE_LOG(ERR, EAL, "A pending request %s:%s\n", dst, req->name);
		rte_errno = EEXIST;
		ret = -1;
		goto fail;
	}

	ret = send_msg(dst, req, MP_REQ);
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Fail to send request %s:%s\n",
			dst, req->name);
		ret = -1;
		goto fail;
	} else if (ret == 0) {
		ret = 0;
		goto fail;
	}
	param->user_reply.nb_sent++;

	/* if alarm set fails, we simply ignore the reply */
	if (rte_eal_alarm_set(ts->tv_sec * 1000000 + ts->tv_nsec / 1000,
			      async_reply_handle, pending_req) < 0) {
		RTE_LOG(ERR, EAL, "Fail to set alarm for request %s:%s\n",
			dst, req->name);
		ret = -1;
		goto fail;
	}
	TAILQ_INSERT_TAIL(&pending_requests.requests, pending_req, next);

	return 0;
fail:
	free(pending_req);
	free(reply_msg);
	return ret;
}

static int
mp_request_sync(const char *dst, struct rte_mp_msg *req,
	       struct rte_mp_reply *reply, const struct timespec *ts)
{
	int ret;
	struct rte_mp_msg msg, *tmp;
	struct pending_request pending_req, *exist;

	pending_req.type = REQUEST_TYPE_SYNC;
	pending_req.reply_received = 0;
	strlcpy(pending_req.dst, dst, sizeof(pending_req.dst));
	pending_req.request = req;
	pending_req.reply = &msg;
	pthread_cond_init(&pending_req.sync.cond, NULL);

	exist = find_pending_request(dst, req->name);
	if (exist) {
		RTE_LOG(ERR, EAL, "A pending request %s:%s\n", dst, req->name);
		rte_errno = EEXIST;
		return -1;
	}

	ret = send_msg(dst, req, MP_REQ);
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Fail to send request %s:%s\n",
			dst, req->name);
		return -1;
	} else if (ret == 0)
		return 0;

	TAILQ_INSERT_TAIL(&pending_requests.requests, &pending_req, next);

	reply->nb_sent++;

	do {
		ret = pthread_cond_timedwait(&pending_req.sync.cond,
				&pending_requests.lock, ts);
	} while (ret != 0 && ret != ETIMEDOUT);

	TAILQ_REMOVE(&pending_requests.requests, &pending_req, next);

	if (pending_req.reply_received == 0) {
		RTE_LOG(ERR, EAL, "Fail to recv reply for request %s:%s\n",
			dst, req->name);
		rte_errno = ETIMEDOUT;
		return -1;
	}
	if (pending_req.reply_received == -1) {
		RTE_LOG(DEBUG, EAL, "Asked to ignore response\n");
		/* not receiving this message is not an error, so decrement
		 * number of sent messages
		 */
		reply->nb_sent--;
		return 0;
	}

	tmp = realloc(reply->msgs, sizeof(msg) * (reply->nb_received + 1));
	if (!tmp) {
		RTE_LOG(ERR, EAL, "Fail to alloc reply for request %s:%s\n",
			dst, req->name);
		rte_errno = ENOMEM;
		return -1;
	}
	memcpy(&tmp[reply->nb_received], &msg, sizeof(msg));
	reply->msgs = tmp;
	reply->nb_received++;
	return 0;
}

int
rte_mp_request_sync(struct rte_mp_msg *req, struct rte_mp_reply *reply,
		const struct timespec *ts)
{
	int dir_fd, ret = -1;
	DIR *mp_dir;
	struct dirent *ent;
	struct timeval now;
	struct timespec end;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	RTE_LOG(DEBUG, EAL, "request: %s\n", req->name);

	reply->nb_sent = 0;
	reply->nb_received = 0;
	reply->msgs = NULL;

	if (check_input(req) != 0)
		goto end;

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		rte_errno = ENOTSUP;
		return -1;
	}

	if (gettimeofday(&now, NULL) < 0) {
		RTE_LOG(ERR, EAL, "Failed to get current time\n");
		rte_errno = errno;
		goto end;
	}

	end.tv_nsec = (now.tv_usec * 1000 + ts->tv_nsec) % 1000000000;
	end.tv_sec = now.tv_sec + ts->tv_sec +
			(now.tv_usec * 1000 + ts->tv_nsec) / 1000000000;

	/* for secondary process, send request to the primary process only */
	if (rte_eal_process_type() == RTE_PROC_SECONDARY) {
		pthread_mutex_lock(&pending_requests.lock);
		ret = mp_request_sync(eal_mp_socket_path(), req, reply, &end);
		pthread_mutex_unlock(&pending_requests.lock);
		goto end;
	}

	/* for primary process, broadcast request, and collect reply 1 by 1 */
	mp_dir = opendir(mp_dir_path);
	if (!mp_dir) {
		RTE_LOG(ERR, EAL, "Unable to open directory %s\n", mp_dir_path);
		rte_errno = errno;
		goto end;
	}

	dir_fd = dirfd(mp_dir);
	/* lock the directory to prevent processes spinning up while we send */
	if (flock(dir_fd, LOCK_SH)) {
		RTE_LOG(ERR, EAL, "Unable to lock directory %s\n",
			mp_dir_path);
		rte_errno = errno;
		goto close_end;
	}

	pthread_mutex_lock(&pending_requests.lock);
	while ((ent = readdir(mp_dir))) {
		char path[PATH_MAX];

		if (fnmatch(mp_filter, ent->d_name, 0) != 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", mp_dir_path,
			 ent->d_name);

		/* unlocks the mutex while waiting for response,
		 * locks on receive
		 */
		if (mp_request_sync(path, req, reply, &end))
			goto unlock_end;
	}
	ret = 0;

unlock_end:
	pthread_mutex_unlock(&pending_requests.lock);
	/* unlock the directory */
	flock(dir_fd, LOCK_UN);

close_end:
	/* dir_fd automatically closed on closedir */
	closedir(mp_dir);

end:
	if (ret) {
		free(reply->msgs);
		reply->nb_received = 0;
		reply->msgs = NULL;
	}
	return ret;
}

int
rte_mp_request_async(struct rte_mp_msg *req, const struct timespec *ts,
		rte_mp_async_reply_t clb)
{
	struct rte_mp_msg *copy;
	struct pending_request *dummy;
	struct async_request_param *param;
	struct rte_mp_reply *reply;
	int dir_fd, ret = 0;
	DIR *mp_dir;
	struct dirent *ent;
	struct timeval now;
	struct timespec *end;
	bool dummy_used = false;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	RTE_LOG(DEBUG, EAL, "request: %s\n", req->name);

	if (check_input(req) != 0)
		return -1;

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		rte_errno = ENOTSUP;
		return -1;
	}

	if (gettimeofday(&now, NULL) < 0) {
		RTE_LOG(ERR, EAL, "Failed to get current time\n");
		rte_errno = errno;
		return -1;
	}
	copy = calloc(1, sizeof(*copy));
	dummy = calloc(1, sizeof(*dummy));
	param = calloc(1, sizeof(*param));
	if (copy == NULL || dummy == NULL || param == NULL) {
		RTE_LOG(ERR, EAL, "Failed to allocate memory for async reply\n");
		rte_errno = ENOMEM;
		goto fail;
	}

	/* copy message */
	memcpy(copy, req, sizeof(*copy));

	param->n_responses_processed = 0;
	param->clb = clb;
	end = &param->end;
	reply = &param->user_reply;

	end->tv_nsec = (now.tv_usec * 1000 + ts->tv_nsec) % 1000000000;
	end->tv_sec = now.tv_sec + ts->tv_sec +
			(now.tv_usec * 1000 + ts->tv_nsec) / 1000000000;
	reply->nb_sent = 0;
	reply->nb_received = 0;
	reply->msgs = NULL;

	/* we have to lock the request queue here, as we will be adding a bunch
	 * of requests to the queue at once, and some of the replies may arrive
	 * before we add all of the requests to the queue.
	 */
	pthread_mutex_lock(&pending_requests.lock);

	/* we have to ensure that callback gets triggered even if we don't send
	 * anything, therefore earlier we have allocated a dummy request. fill
	 * it, and put it on the queue if we don't send any requests.
	 */
	dummy->type = REQUEST_TYPE_ASYNC;
	dummy->request = copy;
	dummy->reply = NULL;
	dummy->async.param = param;
	dummy->reply_received = 1; /* short-circuit the timeout */

	/* for secondary process, send request to the primary process only */
	if (rte_eal_process_type() == RTE_PROC_SECONDARY) {
		ret = mp_request_async(eal_mp_socket_path(), copy, param, ts);

		/* if we didn't send anything, put dummy request on the queue */
		if (ret == 0 && reply->nb_sent == 0) {
			TAILQ_INSERT_TAIL(&pending_requests.requests, dummy,
					next);
			dummy_used = true;
		}

		pthread_mutex_unlock(&pending_requests.lock);

		/* if we couldn't send anything, clean up */
		if (ret != 0)
			goto fail;
		return 0;
	}

	/* for primary process, broadcast request */
	mp_dir = opendir(mp_dir_path);
	if (!mp_dir) {
		RTE_LOG(ERR, EAL, "Unable to open directory %s\n", mp_dir_path);
		rte_errno = errno;
		goto unlock_fail;
	}
	dir_fd = dirfd(mp_dir);

	/* lock the directory to prevent processes spinning up while we send */
	if (flock(dir_fd, LOCK_SH)) {
		RTE_LOG(ERR, EAL, "Unable to lock directory %s\n",
			mp_dir_path);
		rte_errno = errno;
		goto closedir_fail;
	}

	while ((ent = readdir(mp_dir))) {
		char path[PATH_MAX];

		if (fnmatch(mp_filter, ent->d_name, 0) != 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", mp_dir_path,
			 ent->d_name);

		if (mp_request_async(path, copy, param, ts))
			ret = -1;
	}
	/* if we didn't send anything, put dummy request on the queue */
	if (ret == 0 && reply->nb_sent == 0) {
		TAILQ_INSERT_HEAD(&pending_requests.requests, dummy, next);
		dummy_used = true;
	}

	/* finally, unlock the queue */
	pthread_mutex_unlock(&pending_requests.lock);

	/* unlock the directory */
	flock(dir_fd, LOCK_UN);

	/* dir_fd automatically closed on closedir */
	closedir(mp_dir);

	/* if dummy was unused, free it */
	if (!dummy_used)
		free(dummy);

	return ret;
closedir_fail:
	closedir(mp_dir);
unlock_fail:
	pthread_mutex_unlock(&pending_requests.lock);
fail:
	free(dummy);
	free(param);
	free(copy);
	return -1;
}

int
rte_mp_reply(struct rte_mp_msg *msg, const char *peer)
{
	RTE_LOG(DEBUG, EAL, "reply: %s\n", msg->name);
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (check_input(msg) != 0)
		return -1;

	if (peer == NULL) {
		RTE_LOG(ERR, EAL, "peer is not specified\n");
		rte_errno = EINVAL;
		return -1;
	}

	if (internal_conf->no_shconf) {
		RTE_LOG(DEBUG, EAL, "No shared files mode enabled, IPC is disabled\n");
		return 0;
	}

	return mp_send(msg, peer, MP_REP);
}

/* Internally, the status of the mp feature is represented as a three-state:
 * - "unknown" as long as no secondary process attached to a primary process
 *   and there was no call to rte_mp_disable yet,
 * - "enabled" as soon as a secondary process attaches to a primary process,
 * - "disabled" when a primary process successfully called rte_mp_disable,
 */
enum mp_status {
	MP_STATUS_UNKNOWN,
	MP_STATUS_DISABLED,
	MP_STATUS_ENABLED,
};

static bool
set_mp_status(enum mp_status status)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	uint8_t expected;
	uint8_t desired;

	expected = MP_STATUS_UNKNOWN;
	desired = status;
	if (__atomic_compare_exchange_n(&mcfg->mp_status, &expected, desired,
			false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
		return true;

	return __atomic_load_n(&mcfg->mp_status, __ATOMIC_RELAXED) == desired;
}

bool
rte_mp_disable(void)
{
	return set_mp_status(MP_STATUS_DISABLED);
}

bool
__rte_mp_enable(void)
{
	return set_mp_status(MP_STATUS_ENABLED);
}
