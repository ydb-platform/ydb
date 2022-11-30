#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Intel Corporation
 */

#ifndef RTE_EXEC_ENV_WINDOWS
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <dlfcn.h>
#endif /* !RTE_EXEC_ENV_WINDOWS */

/* we won't link against libbsd, so just always use DPDKs-specific strlcpy */
#undef RTE_USE_LIBBSD
#include <rte_string_fns.h>
#include <rte_common.h>
#include <rte_spinlock.h>
#include <rte_version.h>

#include "rte_telemetry.h"
#include "telemetry_json.h"
#include "telemetry_data.h"
#include "rte_telemetry_legacy.h"

#define MAX_CMD_LEN 56
#define MAX_HELP_LEN 64
#define MAX_OUTPUT_LEN (1024 * 16)
#define MAX_CONNECTIONS 10

#ifndef RTE_EXEC_ENV_WINDOWS
static void *
client_handler(void *socket);
#endif /* !RTE_EXEC_ENV_WINDOWS */

struct cmd_callback {
	char cmd[MAX_CMD_LEN];
	telemetry_cb fn;
	char help[MAX_HELP_LEN];
};

#ifndef RTE_EXEC_ENV_WINDOWS
struct socket {
	int sock;
	char path[sizeof(((struct sockaddr_un *)0)->sun_path)];
	handler fn;
	uint16_t *num_clients;
};
static struct socket v2_socket; /* socket for v2 telemetry */
static struct socket v1_socket; /* socket for v1 telemetry */
#endif /* !RTE_EXEC_ENV_WINDOWS */
static char telemetry_log_error[1024]; /* Will contain error on init failure */
/* list of command callbacks, with one command registered by default */
static struct cmd_callback callbacks[TELEMETRY_MAX_CALLBACKS];
static int num_callbacks; /* How many commands are registered */
/* Used when accessing or modifying list of command callbacks */
static rte_spinlock_t callback_sl = RTE_SPINLOCK_INITIALIZER;
#ifndef RTE_EXEC_ENV_WINDOWS
static uint16_t v2_clients;
#endif /* !RTE_EXEC_ENV_WINDOWS */

int
rte_telemetry_register_cmd(const char *cmd, telemetry_cb fn, const char *help)
{
	int i = 0;

	if (strlen(cmd) >= MAX_CMD_LEN || fn == NULL || cmd[0] != '/'
			|| strlen(help) >= MAX_HELP_LEN)
		return -EINVAL;
	if (num_callbacks >= TELEMETRY_MAX_CALLBACKS)
		return -ENOENT;

	rte_spinlock_lock(&callback_sl);
	while (i < num_callbacks && strcmp(cmd, callbacks[i].cmd) > 0)
		i++;
	if (i != num_callbacks)
		/* Move elements to keep the list alphabetical */
		memmove(callbacks + i + 1, callbacks + i,
			sizeof(struct cmd_callback) * (num_callbacks - i));

	strlcpy(callbacks[i].cmd, cmd, MAX_CMD_LEN);
	callbacks[i].fn = fn;
	strlcpy(callbacks[i].help, help, MAX_HELP_LEN);
	num_callbacks++;
	rte_spinlock_unlock(&callback_sl);

	return 0;
}

#ifndef RTE_EXEC_ENV_WINDOWS

static int
list_commands(const char *cmd __rte_unused, const char *params __rte_unused,
		struct rte_tel_data *d)
{
	int i;

	rte_tel_data_start_array(d, RTE_TEL_STRING_VAL);
	for (i = 0; i < num_callbacks; i++)
		rte_tel_data_add_array_string(d, callbacks[i].cmd);
	return 0;
}

static int
json_info(const char *cmd __rte_unused, const char *params __rte_unused,
		struct rte_tel_data *d)
{
	rte_tel_data_start_dict(d);
	rte_tel_data_add_dict_string(d, "version", rte_version());
	rte_tel_data_add_dict_int(d, "pid", getpid());
	rte_tel_data_add_dict_int(d, "max_output_len", MAX_OUTPUT_LEN);
	return 0;
}

static int
command_help(const char *cmd __rte_unused, const char *params,
		struct rte_tel_data *d)
{
	int i;

	if (!params)
		return -1;
	rte_tel_data_start_dict(d);
	rte_spinlock_lock(&callback_sl);
	for (i = 0; i < num_callbacks; i++)
		if (strcmp(params, callbacks[i].cmd) == 0) {
			rte_tel_data_add_dict_string(d, params,
					callbacks[i].help);
			break;
		}
	rte_spinlock_unlock(&callback_sl);
	if (i == num_callbacks)
		return -1;
	return 0;
}

static int
container_to_json(const struct rte_tel_data *d, char *out_buf, size_t buf_len)
{
	size_t used = 0;
	unsigned int i;

	if (d->type != RTE_TEL_ARRAY_U64 && d->type != RTE_TEL_ARRAY_INT
			&& d->type != RTE_TEL_ARRAY_STRING)
		return snprintf(out_buf, buf_len, "null");

	used = rte_tel_json_empty_array(out_buf, buf_len, 0);
	if (d->type == RTE_TEL_ARRAY_U64)
		for (i = 0; i < d->data_len; i++)
			used = rte_tel_json_add_array_u64(out_buf,
				buf_len, used,
				d->data.array[i].u64val);
	if (d->type == RTE_TEL_ARRAY_INT)
		for (i = 0; i < d->data_len; i++)
			used = rte_tel_json_add_array_int(out_buf,
				buf_len, used,
				d->data.array[i].ival);
	if (d->type == RTE_TEL_ARRAY_STRING)
		for (i = 0; i < d->data_len; i++)
			used = rte_tel_json_add_array_string(out_buf,
				buf_len, used,
				d->data.array[i].sval);
	return used;
}

static void
output_json(const char *cmd, const struct rte_tel_data *d, int s)
{
	char out_buf[MAX_OUTPUT_LEN];

	char *cb_data_buf;
	size_t buf_len, prefix_used, used = 0;
	unsigned int i;

	RTE_BUILD_BUG_ON(sizeof(out_buf) < MAX_CMD_LEN +
			RTE_TEL_MAX_SINGLE_STRING_LEN + 10);
	switch (d->type) {
	case RTE_TEL_NULL:
		used = snprintf(out_buf, sizeof(out_buf), "{\"%.*s\":null}",
				MAX_CMD_LEN, cmd ? cmd : "none");
		break;
	case RTE_TEL_STRING:
		used = snprintf(out_buf, sizeof(out_buf), "{\"%.*s\":\"%.*s\"}",
				MAX_CMD_LEN, cmd,
				RTE_TEL_MAX_SINGLE_STRING_LEN, d->data.str);
		break;
	case RTE_TEL_DICT:
		prefix_used = snprintf(out_buf, sizeof(out_buf), "{\"%.*s\":",
				MAX_CMD_LEN, cmd);
		cb_data_buf = &out_buf[prefix_used];
		buf_len = sizeof(out_buf) - prefix_used - 1; /* space for '}' */

		used = rte_tel_json_empty_obj(cb_data_buf, buf_len, 0);
		for (i = 0; i < d->data_len; i++) {
			const struct tel_dict_entry *v = &d->data.dict[i];
			switch (v->type) {
			case RTE_TEL_STRING_VAL:
				used = rte_tel_json_add_obj_str(cb_data_buf,
						buf_len, used,
						v->name, v->value.sval);
				break;
			case RTE_TEL_INT_VAL:
				used = rte_tel_json_add_obj_int(cb_data_buf,
						buf_len, used,
						v->name, v->value.ival);
				break;
			case RTE_TEL_U64_VAL:
				used = rte_tel_json_add_obj_u64(cb_data_buf,
						buf_len, used,
						v->name, v->value.u64val);
				break;
			case RTE_TEL_CONTAINER:
			{
				char temp[buf_len];
				const struct container *cont =
						&v->value.container;
				if (container_to_json(cont->data,
						temp, buf_len) != 0)
					used = rte_tel_json_add_obj_json(
							cb_data_buf,
							buf_len, used,
							v->name, temp);
				if (!cont->keep)
					rte_tel_data_free(cont->data);
			}
			}
		}
		used += prefix_used;
		used += strlcat(out_buf + used, "}", sizeof(out_buf) - used);
		break;
	case RTE_TEL_ARRAY_STRING:
	case RTE_TEL_ARRAY_INT:
	case RTE_TEL_ARRAY_U64:
	case RTE_TEL_ARRAY_CONTAINER:
		prefix_used = snprintf(out_buf, sizeof(out_buf), "{\"%.*s\":",
				MAX_CMD_LEN, cmd);
		cb_data_buf = &out_buf[prefix_used];
		buf_len = sizeof(out_buf) - prefix_used - 1; /* space for '}' */

		used = rte_tel_json_empty_array(cb_data_buf, buf_len, 0);
		for (i = 0; i < d->data_len; i++)
			if (d->type == RTE_TEL_ARRAY_STRING)
				used = rte_tel_json_add_array_string(
						cb_data_buf,
						buf_len, used,
						d->data.array[i].sval);
			else if (d->type == RTE_TEL_ARRAY_INT)
				used = rte_tel_json_add_array_int(cb_data_buf,
						buf_len, used,
						d->data.array[i].ival);
			else if (d->type == RTE_TEL_ARRAY_U64)
				used = rte_tel_json_add_array_u64(cb_data_buf,
						buf_len, used,
						d->data.array[i].u64val);
			else if (d->type == RTE_TEL_ARRAY_CONTAINER) {
				char temp[buf_len];
				const struct container *rec_data =
						&d->data.array[i].container;
				if (container_to_json(rec_data->data,
						temp, buf_len) != 0)
					used = rte_tel_json_add_array_json(
							cb_data_buf,
							buf_len, used, temp);
				if (!rec_data->keep)
					rte_tel_data_free(rec_data->data);
			}
		used += prefix_used;
		used += strlcat(out_buf + used, "}", sizeof(out_buf) - used);
		break;
	}
	if (write(s, out_buf, used) < 0)
		perror("Error writing to socket");
}

static void
perform_command(telemetry_cb fn, const char *cmd, const char *param, int s)
{
	struct rte_tel_data data;

	int ret = fn(cmd, param, &data);
	if (ret < 0) {
		char out_buf[MAX_CMD_LEN + 10];
		int used = snprintf(out_buf, sizeof(out_buf), "{\"%.*s\":null}",
				MAX_CMD_LEN, cmd ? cmd : "none");
		if (write(s, out_buf, used) < 0)
			perror("Error writing to socket");
		return;
	}
	output_json(cmd, &data, s);
}

static int
unknown_command(const char *cmd __rte_unused, const char *params __rte_unused,
		struct rte_tel_data *d)
{
	return d->type = RTE_TEL_NULL;
}

static void *
client_handler(void *sock_id)
{
	int s = (int)(uintptr_t)sock_id;
	char buffer[1024];
	char info_str[1024];
	snprintf(info_str, sizeof(info_str),
			"{\"version\":\"%s\",\"pid\":%d,\"max_output_len\":%d}",
			rte_version(), getpid(), MAX_OUTPUT_LEN);
	if (write(s, info_str, strlen(info_str)) < 0) {
		close(s);
		return NULL;
	}

	/* receive data is not null terminated */
	int bytes = read(s, buffer, sizeof(buffer) - 1);
	while (bytes > 0) {
		buffer[bytes] = 0;
		const char *cmd = strtok(buffer, ",");
		const char *param = strtok(NULL, "\0");
		telemetry_cb fn = unknown_command;
		int i;

		if (cmd && strlen(cmd) < MAX_CMD_LEN) {
			rte_spinlock_lock(&callback_sl);
			for (i = 0; i < num_callbacks; i++)
				if (strcmp(cmd, callbacks[i].cmd) == 0) {
					fn = callbacks[i].fn;
					break;
				}
			rte_spinlock_unlock(&callback_sl);
		}
		perform_command(fn, cmd, param, s);

		bytes = read(s, buffer, sizeof(buffer) - 1);
	}
	close(s);
	__atomic_sub_fetch(&v2_clients, 1, __ATOMIC_RELAXED);
	return NULL;
}

static void *
socket_listener(void *socket)
{
	while (1) {
		pthread_t th;
		struct socket *s = (struct socket *)socket;
		int s_accepted = accept(s->sock, NULL, NULL);
		if (s_accepted < 0) {
			snprintf(telemetry_log_error,
				sizeof(telemetry_log_error),
				"Error with accept, telemetry thread quitting");
			return NULL;
		}
		if (s->num_clients != NULL) {
			uint16_t conns = __atomic_load_n(s->num_clients,
					__ATOMIC_RELAXED);
			if (conns >= MAX_CONNECTIONS) {
				close(s_accepted);
				continue;
			}
			__atomic_add_fetch(s->num_clients, 1,
					__ATOMIC_RELAXED);
		}
		pthread_create(&th, NULL, s->fn, (void *)(uintptr_t)s_accepted);
		pthread_detach(th);
	}
	return NULL;
}

static inline char *
get_socket_path(const char *runtime_dir, const int version)
{
	static char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/dpdk_telemetry.v%d",
			strlen(runtime_dir) ? runtime_dir : "/tmp", version);
	return path;
}

static void
unlink_sockets(void)
{
	if (v2_socket.path[0])
		unlink(v2_socket.path);
	if (v1_socket.path[0])
		unlink(v1_socket.path);
}

static int
create_socket(char *path)
{
	int sock = socket(AF_UNIX, SOCK_SEQPACKET, 0);
	if (sock < 0) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
				"Error with socket creation, %s",
				strerror(errno));
		return -1;
	}

	struct sockaddr_un sun = {.sun_family = AF_UNIX};
	strlcpy(sun.sun_path, path, sizeof(sun.sun_path));
	unlink(sun.sun_path);
	if (bind(sock, (void *) &sun, sizeof(sun)) < 0) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
				"Error binding socket: %s",
				strerror(errno));
		sun.sun_path[0] = 0;
		goto error;
	}

	if (listen(sock, 1) < 0) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
				"Error calling listen for socket: %s",
				strerror(errno));
		goto error;
	}

	return sock;

error:
	close(sock);
	unlink_sockets();
	return -1;
}

static int
telemetry_legacy_init(const char *runtime_dir, rte_cpuset_t *cpuset)
{
	pthread_t t_old;

	if (num_legacy_callbacks == 1) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
			 "No legacy callbacks, legacy socket not created");
		return -1;
	}

	v1_socket.fn = legacy_client_handler;
	if ((size_t) snprintf(v1_socket.path, sizeof(v1_socket.path),
			"%s/telemetry", runtime_dir)
			>= sizeof(v1_socket.path)) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
				"Error with socket binding, path too long");
		return -1;
	}
	v1_socket.sock = create_socket(v1_socket.path);
	if (v1_socket.sock < 0)
		return -1;
	pthread_create(&t_old, NULL, socket_listener, &v1_socket);
	pthread_setaffinity_np(t_old, sizeof(*cpuset), cpuset);

	return 0;
}

static int
telemetry_v2_init(const char *runtime_dir, rte_cpuset_t *cpuset)
{
	pthread_t t_new;

	v2_socket.num_clients = &v2_clients;
	rte_telemetry_register_cmd("/", list_commands,
			"Returns list of available commands, Takes no parameters");
	rte_telemetry_register_cmd("/info", json_info,
			"Returns DPDK Telemetry information. Takes no parameters");
	rte_telemetry_register_cmd("/help", command_help,
			"Returns help text for a command. Parameters: string command");
	v2_socket.fn = client_handler;
	if (strlcpy(v2_socket.path, get_socket_path(runtime_dir, 2),
			sizeof(v2_socket.path)) >= sizeof(v2_socket.path)) {
		snprintf(telemetry_log_error, sizeof(telemetry_log_error),
				"Error with socket binding, path too long");
		return -1;
	}

	v2_socket.sock = create_socket(v2_socket.path);
	if (v2_socket.sock < 0)
		return -1;
	pthread_create(&t_new, NULL, socket_listener, &v2_socket);
	pthread_setaffinity_np(t_new, sizeof(*cpuset), cpuset);
	atexit(unlink_sockets);

	return 0;
}

#endif /* !RTE_EXEC_ENV_WINDOWS */

int32_t
rte_telemetry_init(const char *runtime_dir, rte_cpuset_t *cpuset,
		const char **err_str)
{
#ifndef RTE_EXEC_ENV_WINDOWS
	if (telemetry_v2_init(runtime_dir, cpuset) != 0) {
		*err_str = telemetry_log_error;
		return -1;
	}
	if (telemetry_legacy_init(runtime_dir, cpuset) != 0) {
		*err_str = telemetry_log_error;
	}
#else /* RTE_EXEC_ENV_WINDOWS */
	RTE_SET_USED(runtime_dir);
	RTE_SET_USED(cpuset);
	RTE_SET_USED(err_str);

	snprintf(telemetry_log_error, sizeof(telemetry_log_error),
		"DPDK Telemetry is not supported on Windows.");
#endif /* RTE_EXEC_ENV_WINDOWS */

	return 0;
}
