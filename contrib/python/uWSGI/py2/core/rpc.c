#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

int uwsgi_register_rpc(char *name, struct uwsgi_plugin *plugin, uint8_t args, void *func) {

	struct uwsgi_rpc *urpc;
	int ret = -1;

	if (uwsgi.mywid == 0 && uwsgi.workers[0].pid != uwsgi.mypid) {
		uwsgi_log("only the master and the workers can register RPC functions\n");
		return -1;
	}

	if (strlen(name) >= UMAX8)  {
	      uwsgi_log("the supplied RPC name string is too long, max size is %d\n", UMAX8-1);
	      return -1;
	}

	uwsgi_lock(uwsgi.rpc_table_lock);

	// first check if a function is already registered
	size_t i;
	for(i=0;i<uwsgi.shared->rpc_count[uwsgi.mywid];i++) {
		int pos = (uwsgi.mywid * uwsgi.rpc_max) + i;
		urpc = &uwsgi.rpc_table[pos];
		if (!strcmp(name, urpc->name)) {
			goto already;
		}
	}

	if (uwsgi.shared->rpc_count[uwsgi.mywid] < uwsgi.rpc_max) {
		int pos = (uwsgi.mywid * uwsgi.rpc_max) + uwsgi.shared->rpc_count[uwsgi.mywid];
		urpc = &uwsgi.rpc_table[pos];
		uwsgi.shared->rpc_count[uwsgi.mywid]++;
already:
		memcpy(urpc->name, name, strlen(name));
		urpc->plugin = plugin;
		urpc->args = args;
		urpc->func = func;
		urpc->shared = uwsgi.mywid == 0 ? 1 : 0;

		ret = 0;
		if (uwsgi.mywid == 0) {
			uwsgi_log("registered shared/inherited RPC function \"%s\"\n", name);
		}
		else {
			uwsgi_log("registered RPC function \"%s\" on worker %d\n", name, uwsgi.mywid);
		}
	}

	// implement cow
	if (uwsgi.mywid == 0) {
		int i;
		for(i=1;i<=uwsgi.numproc;i++) {
			uwsgi.shared->rpc_count[i] = uwsgi.shared->rpc_count[0];
			int pos = (i * uwsgi.rpc_max);
			memcpy(&uwsgi.rpc_table[pos], uwsgi.rpc_table, sizeof(struct uwsgi_rpc) * uwsgi.rpc_max);
		}
	}

	uwsgi_unlock(uwsgi.rpc_table_lock);

	return ret;
}

uint64_t uwsgi_rpc(char *name, uint8_t argc, char *argv[], uint16_t argvs[], char **output) {

	struct uwsgi_rpc *urpc = NULL;
	uint64_t i;
	uint64_t ret = 0;

	int pos = (uwsgi.mywid * uwsgi.rpc_max);

	for (i = 0; i < uwsgi.shared->rpc_count[uwsgi.mywid]; i++) {
		if (uwsgi.rpc_table[pos + i].name[0] != 0) {
			if (!strcmp(uwsgi.rpc_table[pos + i].name, name)) {
				urpc = &uwsgi.rpc_table[pos + i];
				break;
			}
		}
	}

	if (urpc) {
		if (urpc->plugin->rpc) {
			ret = urpc->plugin->rpc(urpc->func, argc, argv, argvs, output);
		}
	}

	return ret;
}

static void rpc_context_hook(char *key, uint16_t kl, char *value, uint16_t vl, void *data) {
	size_t *r = (size_t *) data;

	if (!uwsgi_strncmp(key, kl, "CONTENT_LENGTH", 14)) {
		*r = uwsgi_str_num(value, vl);
	}
}

char *uwsgi_do_rpc(char *node, char *func, uint8_t argc, char *argv[], uint16_t argvs[], uint64_t * len) {

	uint8_t i;
	uint16_t ulen;
	struct uwsgi_header *uh = NULL;
	char *buffer = NULL;

	*len = 0;

	if (node == NULL || !strcmp(node, "")) {
		// allocate the whole buffer
		if (!uwsgi.rpc_table) {
                	uwsgi_log("local rpc subsystem is still not initialized !!!\n");
                	return NULL;
        	}
		*len = uwsgi_rpc(func, argc, argv, argvs, &buffer);
		if (buffer)
			return buffer;
		return NULL;
	}


	// connect to node (async way)
	int fd = uwsgi_connect(node, 0, 1);
	if (fd < 0)
		return NULL;

	// wait for connection;
	int ret = uwsgi.wait_write_hook(fd, uwsgi.socket_timeout);
	if (ret <= 0) {
		close(fd);
		return NULL;
	}

	// prepare a uwsgi array
	size_t buffer_size = 2 + strlen(func);

	for (i = 0; i < argc; i++) {
		buffer_size += 2 + argvs[i];
	}

	if (buffer_size > 0xffff) {
		uwsgi_log("RPC packet length overflow!!! Must be less than or equal to 65535, have %llu\n", buffer_size);
		return NULL;
	}

	// allocate the whole buffer
	buffer = uwsgi_malloc(4+buffer_size);

	// set the uwsgi header
	uh = (struct uwsgi_header *) buffer;
	uh->modifier1 = 173;
	uh->pktsize = (uint16_t) buffer_size;
	uh->modifier2 = 0;

	// add func to the array
	char *bufptr = buffer + 4;
	ulen = strlen(func);
	*bufptr++ = (uint8_t) (ulen & 0xff);
	*bufptr++ = (uint8_t) ((ulen >> 8) & 0xff);
	memcpy(bufptr, func, ulen);
	bufptr += ulen;

	for (i = 0; i < argc; i++) {
		ulen = argvs[i];
		*bufptr++ = (uint8_t) (ulen & 0xff);
		*bufptr++ = (uint8_t) ((ulen >> 8) & 0xff);
		memcpy(bufptr, argv[i], ulen);
		bufptr += ulen;
	}

	// ok the request is ready, let's send it in non blocking way
	if (uwsgi_write_true_nb(fd, buffer, buffer_size+4, uwsgi.socket_timeout)) {
		goto error;
	}

	// ok time to wait for the response in non blocking way
	size_t rlen = buffer_size+4;
	uint8_t modifier2 = 0;
	if (uwsgi_read_with_realloc(fd, &buffer, &rlen, uwsgi.socket_timeout, NULL, &modifier2)) {
		goto error;
	}

	// 64bit response ?
	if (modifier2 == 5) {
		size_t content_len = 0;
		if (uwsgi_hooked_parse(buffer, rlen, rpc_context_hook, &content_len )) goto error;

		if (content_len > rlen) {
			char *tmp_buf = realloc(buffer, content_len);
			if (!tmp_buf) goto error;
			buffer = tmp_buf;
		}

		rlen = content_len;

		// read the raw value from the socket
                if (uwsgi_read_whole_true_nb(fd, buffer, rlen, uwsgi.socket_timeout)) {
			goto error;
                }
	}

	close(fd);
	*len = rlen;
	if (*len == 0) {
		goto error2;
	}
	return buffer;

error:
	close(fd);
error2:
	free(buffer);
	return NULL;

}


void uwsgi_rpc_init() {
	uwsgi.rpc_table = uwsgi_calloc_shared((sizeof(struct uwsgi_rpc) * uwsgi.rpc_max) * (uwsgi.numproc+1));
	uwsgi.shared->rpc_count = uwsgi_calloc_shared(sizeof(uint64_t) * (uwsgi.numproc+1));
}
