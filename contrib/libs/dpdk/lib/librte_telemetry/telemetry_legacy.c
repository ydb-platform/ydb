#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Intel Corporation
 */

#ifndef RTE_EXEC_ENV_WINDOWS
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <pthread.h>
#endif /* !RTE_EXEC_ENV_WINDOWS */

/* we won't link against libbsd, so just always use DPDKs-specific strlcpy */
#undef RTE_USE_LIBBSD
#include <rte_string_fns.h>
#include <rte_common.h>
#include <rte_spinlock.h>

#include "rte_telemetry_legacy.h"

#define MAX_LEN 128
#define BUF_SIZE 1024
#define CLIENTS_UNREG_ACTION "\"action\":2"
#define CLIENTS_CMD "\"command\":\"clients\""
#define CLIENTS_DATA "\"data\":{\"client_path\":\""
#define STATS_ACTION "\"action\":0"
#define DATA_REQ_LABEL "\"data\":"
#define TELEMETRY_LEGACY_MAX_CALLBACKS 4


static int
register_client(const char *cmd __rte_unused,
		const char *params __rte_unused,
		char *buffer, int buf_len);

struct json_command {
	char action[MAX_LEN];
	char cmd[MAX_LEN];
	char data[MAX_LEN];
	telemetry_legacy_cb fn;

};

struct json_command callbacks[TELEMETRY_LEGACY_MAX_CALLBACKS] = {
		{
			.action = "\"action\":1",
			.cmd = CLIENTS_CMD,
			.data = CLIENTS_DATA,
			.fn = register_client
		}
};
int num_legacy_callbacks = 1;
static rte_spinlock_t callback_sl = RTE_SPINLOCK_INITIALIZER;

int
rte_telemetry_legacy_register(const char *cmd,
		enum rte_telemetry_legacy_data_req data_req,
		telemetry_legacy_cb fn)
{
	if (fn == NULL)
		return -EINVAL;
	if (num_legacy_callbacks >= (int) RTE_DIM(callbacks))
		return -ENOENT;

	rte_spinlock_lock(&callback_sl);
	strlcpy(callbacks[num_legacy_callbacks].action, STATS_ACTION, MAX_LEN);
	snprintf(callbacks[num_legacy_callbacks].cmd, MAX_LEN,
			"\"command\":\"%s\"", cmd);
	snprintf(callbacks[num_legacy_callbacks].data, MAX_LEN,
			data_req ? "%s{\"" : "%snull",
			DATA_REQ_LABEL);
	callbacks[num_legacy_callbacks].fn = fn;
	num_legacy_callbacks++;
	rte_spinlock_unlock(&callback_sl);

	return 0;
}

static int
register_client(const char *cmd __rte_unused, const char *params,
		char *buffer __rte_unused, int buf_len __rte_unused)
{
#ifndef RTE_EXEC_ENV_WINDOWS
	pthread_t th;
	char data[BUF_SIZE];
	int fd;
	struct sockaddr_un addrs;
#endif /* !RTE_EXEC_ENV_WINDOWS */

	if (!strchr(params, ':')) {
		fprintf(stderr, "Invalid data\n");
		return -1;
	}
#ifndef RTE_EXEC_ENV_WINDOWS
	strlcpy(data, strchr(params, ':'), sizeof(data));
	memcpy(data, &data[strlen(":\"")], strlen(data));
	if (!strchr(data, '\"')) {
		fprintf(stderr, "Invalid client data\n");
		return -1;
	}
	*strchr(data, '\"') = 0;

	fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
	if (fd < 0) {
		perror("Failed to open socket");
		return -1;
	}
	addrs.sun_family = AF_UNIX;
	strlcpy(addrs.sun_path, data, sizeof(addrs.sun_path));

	if (connect(fd, (struct sockaddr *)&addrs, sizeof(addrs)) == -1) {
		perror("\nClient connection error\n");
		close(fd);
		return -1;
	}
	pthread_create(&th, NULL, &legacy_client_handler,
			(void *)(uintptr_t)fd);
#endif /* !RTE_EXEC_ENV_WINDOWS */
	return 0;
}

#ifndef RTE_EXEC_ENV_WINDOWS

static int
send_error_response(int s, int err)
{
	const char *desc;
	char out_buf[100000];

	switch (err) {
	case -ENOMEM:
		desc = "Memory Allocation Error";
		break;
	case -EINVAL:
		desc = "Invalid Argument 404";
		break;
	case -EPERM:
		desc = "Unknown";
		break;
	default:
		/* Default case keeps behaviour of Telemetry library */
		printf("\nInvalid error type: %d\n", err);
		return -EINVAL;
	}
	int used = snprintf(out_buf, sizeof(out_buf), "{\"status_code\": "
			"\"Status Error: %s\", \"data\": null}", desc);
	if (write(s, out_buf, used) < 0) {
		perror("Error writing to socket");
		return -1;
	}
	return 0;
}

static void
perform_command(telemetry_legacy_cb fn, const char *param, int s)
{
	char out_buf[100000];
	int ret, used = 0;

	ret = fn("", param, out_buf, sizeof(out_buf));
	if (ret < 0) {
		ret = send_error_response(s, ret);
		if (ret < 0)
			printf("\nCould not send error response\n");
		return;
	}
	used += ret;
	if (write(s, out_buf, used) < 0)
		perror("Error writing to socket");
}

static int
parse_client_request(char *buffer, int buf_len, int s)
{
	int i;
	char *data = buffer + buf_len;
	telemetry_legacy_cb fn = NULL;
	const char *valid_sep = ",}";
	if (buffer[0] != '{' || buffer[buf_len - 1] != '}')
		return -EPERM;

	if (strstr(buffer, CLIENTS_UNREG_ACTION) && strstr(buffer, CLIENTS_CMD)
			&& strstr(buffer, CLIENTS_DATA))
		return 0;

	for (i = 0; i < num_legacy_callbacks; i++) {
		char *action_ptr = strstr(buffer, callbacks[i].action);
		char *cmd_ptr = strstr(buffer, callbacks[i].cmd);
		char *data_ptr = strstr(buffer, callbacks[i].data);
		if (!action_ptr || !cmd_ptr || !data_ptr)
			continue;

		char action_sep = action_ptr[strlen(callbacks[i].action)];
		char cmd_sep = cmd_ptr[strlen(callbacks[i].cmd)];
		if (!(strchr(valid_sep, action_sep) && strchr(valid_sep,
				cmd_sep)))
			return -EPERM;
		char data_sep;

		if (!strchr(data_ptr, '{'))
			data_sep = data_ptr[strlen(callbacks[i].data)];
		else {
			if (!strchr(data_ptr, '}'))
				return -EINVAL;
			char *data_end = strchr(data_ptr, '}');
			data = data_ptr + strlen(DATA_REQ_LABEL);
			data_sep = data_end[1];
			data_end[1] = 0;
		}
		if (!strchr(valid_sep, data_sep))
			return -EPERM;
		fn = callbacks[i].fn;
		break;
	}

	if (!fn)
		return -EINVAL;
	perform_command(fn, data, s);
	return 0;
}

void *
legacy_client_handler(void *sock_id)
{
	int s = (int)(uintptr_t)sock_id;
	int ret;
	char buffer_recv[BUF_SIZE];
	/* receive data is not null terminated */
	int bytes = read(s, buffer_recv, sizeof(buffer_recv) - 1);

	while (bytes > 0) {
		buffer_recv[bytes] = 0;
		int i, j;
		char buffer[BUF_SIZE];
		for (i = 0, j = 0; buffer_recv[i] != '\0'; i++) {
			buffer[j] = buffer_recv[i];
			j += !isspace(buffer_recv[i]);
		}
		buffer[j] = 0;
		ret = parse_client_request(buffer, j, s);
		if (ret < 0) {
			ret = send_error_response(s, ret);
			if (ret < 0)
				printf("\nCould not send error response\n");
		}
		bytes = read(s, buffer_recv, sizeof(buffer_recv) - 1);
	}
	close(s);
	return NULL;
}

#endif /* !RTE_EXEC_ENV_WINDOWS */
