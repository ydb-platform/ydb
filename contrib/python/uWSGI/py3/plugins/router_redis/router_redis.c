#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

#define REDIS_BUFSIZE 8192

extern struct uwsgi_server uwsgi;

/*

	redis internal router and transformation

	route = /^foobar1(.*)/ redis:addr=127.0.0.1:11211,key=foo$1poo
	route = /^foobar1(.*)/ redisstore:addr=127.0.0.1:11211,key=foo$1poo

*/

struct uwsgi_router_redis_conf {

	char *addr;
	size_t addr_len;

	char *key;
	size_t key_len;

	char *content_type;
	size_t content_type_len;

	char *no_offload;
	char *expires;
	
};

// this is allocated for each transformation
struct uwsgi_transformation_redis_conf {
	struct uwsgi_buffer *addr;
        struct uwsgi_buffer *key;
        char *expires;
};


static size_t redis_firstline_parse(char *buf, size_t len) {
	// check for "$0"
	if (len < 2) return 0;
	if (buf[0] != '$') return 0;
	if (buf[1] == '-') return 0;
	return uwsgi_str_num(buf + 1, len - 1);
}

// store an item in redis
static void redis_store(char *addr, struct uwsgi_buffer *key, struct uwsgi_buffer *value, char *expires) {
	
	int timeout = uwsgi.socket_timeout;

        int fd = uwsgi_connect(addr, 0, 1);
        if (fd < 0) return;

	// wait for connection
        int ret = uwsgi.wait_write_hook(fd, timeout);
        if (ret <= 0) goto end;

	// build the request
	struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
	if (uwsgi_buffer_append(ub, "*3\r\n$3\r\nSET\r\n$", 14)) goto end2;
	if (uwsgi_buffer_num64(ub, key->pos)) goto end2;
	if (uwsgi_buffer_append(ub, "\r\n" , 2)) goto end2;
	if (uwsgi_buffer_append(ub, key->buf, key->pos)) goto end2;
	if (uwsgi_buffer_append(ub, "\r\n$" , 3)) goto end2;
	if (uwsgi_buffer_num64(ub, value->pos)) goto end2;
	if (uwsgi_buffer_append(ub, "\r\n" , 2)) goto end2;
        if (uwsgi_write_true_nb(fd, ub->buf, ub->pos, timeout)) goto end2;
        if (uwsgi_write_true_nb(fd, value->buf, value->pos, timeout)) goto end2;
	ub->pos = 0;
	if (strcmp(expires, "0")) {
		if (uwsgi_buffer_append(ub, "\r\n*3\r\n$6\r\nEXPIRE\r\n$" , 19)) goto end2;
		if (uwsgi_buffer_num64(ub, key->pos)) goto end2;
        	if (uwsgi_buffer_append(ub, "\r\n" , 2)) goto end2;
        	if (uwsgi_buffer_append(ub, key->buf, key->pos)) goto end2;
        	if (uwsgi_buffer_append(ub, "\r\n$" , 3)) goto end2;
		if (uwsgi_buffer_num64(ub, strlen(expires))) goto end2;
		if (uwsgi_buffer_append(ub, "\r\n" , 2)) goto end2;
		if (uwsgi_buffer_append(ub, expires, strlen(expires))) goto end2;
	}
	if (uwsgi_buffer_append(ub, "\r\n" , 2)) goto end2;
        if (uwsgi_write_true_nb(fd, ub->buf, ub->pos, timeout)) goto end2;
	
	// we are not interested in command result... (ugly but it works)
end2:
	uwsgi_buffer_destroy(ub);
end:
	close(fd);
}

static int transform_redis(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {
        struct uwsgi_transformation_redis_conf *utrc = (struct uwsgi_transformation_redis_conf *) ut->data;
        struct uwsgi_buffer *ub = ut->chunk;

        // store only successfull response
        if (wsgi_req->write_errors == 0 && wsgi_req->status == 200 && ub->pos > 0) {
		redis_store(utrc->addr->buf, utrc->key, ub, utrc->expires);
        }

        // free resources
        uwsgi_buffer_destroy(utrc->key);
        uwsgi_buffer_destroy(utrc->addr);
        free(utrc);
        return 0;
}


// be tolerant on errors
static int uwsgi_routing_func_redis_store(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){
        struct uwsgi_router_redis_conf *urrc = (struct uwsgi_router_redis_conf *) ur->data2;

        struct uwsgi_transformation_redis_conf *utrc = uwsgi_calloc(sizeof(struct uwsgi_transformation_redis_conf));

        // build key and name
        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        utrc->key = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urrc->key, urrc->key_len);
        if (!utrc->key) goto error;

        utrc->addr = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urrc->addr, urrc->addr_len);
        if (!utrc->addr) goto error;

        utrc->expires = urrc->expires;

        uwsgi_add_transformation(wsgi_req, transform_redis, utrc);

        return UWSGI_ROUTE_NEXT;

error:
        if (utrc->key) uwsgi_buffer_destroy(utrc->key);
        if (utrc->addr) uwsgi_buffer_destroy(utrc->addr);
        free(utrc);
        return UWSGI_ROUTE_NEXT;
}


static int uwsgi_routing_func_redis(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){
	// this is the buffer for the redis response
	char buf[REDIS_BUFSIZE];
	size_t i;
	char last_char = 0;

	struct uwsgi_router_redis_conf *urrc = (struct uwsgi_router_redis_conf *) ur->data2;

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	struct uwsgi_buffer *ub_key = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urrc->key, urrc->key_len);
        if (!ub_key) return UWSGI_ROUTE_BREAK;

	struct uwsgi_buffer *ub_addr = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urrc->addr, urrc->addr_len);
        if (!ub_addr) {
		uwsgi_buffer_destroy(ub_key);
		return UWSGI_ROUTE_BREAK;
	}

	int fd = uwsgi_connect(ub_addr->buf, 0, 1);
	if (fd < 0) {
		uwsgi_buffer_destroy(ub_key);
		uwsgi_buffer_destroy(ub_addr);
		goto end;
	}

        // wait for connection;
        int ret = uwsgi.wait_write_hook(fd, uwsgi.socket_timeout);
        if (ret <= 0) {
		uwsgi_buffer_destroy(ub_key) ;
		uwsgi_buffer_destroy(ub_addr);
		close(fd);
		goto end;
        }

	// build the request and send it
	char *cmd = uwsgi_concat3n("get ", 4, ub_key->buf, ub_key->pos, "\r\n", 2);
	if (uwsgi_write_true_nb(fd, cmd, 6+ub_key->pos, uwsgi.socket_timeout)) {
		uwsgi_buffer_destroy(ub_key);
		uwsgi_buffer_destroy(ub_addr);
		free(cmd);
		close(fd);
		goto end;
	}
	uwsgi_buffer_destroy(ub_key);
	uwsgi_buffer_destroy(ub_addr);
	free(cmd);

	// ok, start reading the response...
	// first we need to get a full line;
	size_t found = 0;
	size_t pos = 0;
	for(;;) {
		ssize_t len = read(fd, buf + pos, REDIS_BUFSIZE - pos);
		if (len > 0) {
			pos += len;
			goto read;
		}
		if (len < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) goto wait;
		}
		close(fd);
		goto end;
wait:
		ret = uwsgi.wait_read_hook(fd, uwsgi.socket_timeout);
		// when we have a chunk try to read the first line
		if (ret > 0) {
			len = read(fd, buf + pos, REDIS_BUFSIZE - pos);
			if (len > 0) {
				pos += len;
				goto read;
			}
		}
		close(fd);
		goto end;
read:
		for(i=0;i<pos;i++) {
			if (last_char == '\r' && buf[i] == '\n') {
				found = i-1;
				break;
			}
			last_char = buf[i];
		}
		if (found) break;
	}

	// ok parse the first line
	size_t response_size = redis_firstline_parse(buf, found);
	if (response_size == 0) {
		close(fd);
		goto end;
	}

	// from now on, every error will trigger a BREAK...

	// send headers
	if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) goto error;
	if (uwsgi_response_add_content_type(wsgi_req, urrc->content_type, urrc->content_type_len)) goto error;
	if (uwsgi_response_add_content_length(wsgi_req, response_size)) goto error;

	// the first chunk could already contains part of the body
	size_t remains = pos-(found+2);
	if (remains >= response_size) {
		uwsgi_response_write_body_do(wsgi_req, buf+found+2, response_size);
		goto done;	
	}

	// send what we have
	if (uwsgi_response_write_body_do(wsgi_req, buf+found+2, remains)) goto error;

	// and now start reading til the output is consumed
	response_size -= remains;

	// try to offload via the pipe engine
	if (wsgi_req->socket->can_offload && !ur->custom && !urrc->no_offload) {
        	if (!uwsgi_offload_request_pipe_do(wsgi_req, fd, response_size)) {
                	wsgi_req->via = UWSGI_VIA_OFFLOAD;
                        return UWSGI_ROUTE_BREAK;
                }
        }

	while(response_size > 0) {
		ssize_t len = read(fd, buf, UMIN(REDIS_BUFSIZE, response_size));
		if (len > 0) goto write;
		if (len < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) goto wait2;
                }
		goto error;
wait2:
		ret = uwsgi.wait_read_hook(fd, uwsgi.socket_timeout);
		if (ret > 0) {
                        len = read(fd, buf, UMIN(REDIS_BUFSIZE, response_size));
			if (len > 0) goto write;
		}
		goto error;
write:
		if (uwsgi_response_write_body_do(wsgi_req, buf, len)) goto error;
		response_size -= len;
	}

done:
	close(fd);
	if (ur->custom)
                return UWSGI_ROUTE_NEXT;
	return UWSGI_ROUTE_BREAK;

error:
	close(fd);
	return UWSGI_ROUTE_BREAK;
	
end:
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_redis(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_redis;
        ur->data = args;
        ur->data_len = strlen(args);
	struct uwsgi_router_redis_conf *urrc = uwsgi_calloc(sizeof(struct uwsgi_router_redis_conf));
                if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "addr", &urrc->addr,
                        "key", &urrc->key,
                        "content_type", &urrc->content_type,
                        "no_offload", &urrc->no_offload,
                        NULL)) {
			uwsgi_log("invalid route syntax: %s\n", args);
		exit(1);
        }

	if (!urrc->key || !urrc->addr) {
		uwsgi_log("invalid route syntax: you need to specify a redis address and key pattern\n");
		return -1;
	}

	urrc->key_len = strlen(urrc->key);
	urrc->addr_len = strlen(urrc->addr);

        if (!urrc->content_type) urrc->content_type = "text/html";
        urrc->content_type_len = strlen(urrc->content_type);

        ur->data2 = urrc;
	return 0;
}

static int uwsgi_router_redis_continue(struct uwsgi_route *ur, char *args) {
	uwsgi_router_redis(ur, args);
	ur->custom = 1;
	return 0;
}

static int uwsgi_router_redis_store(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_redis_store;
        ur->data = args;
        ur->data_len = strlen(args);
	struct uwsgi_router_redis_conf *urrc = uwsgi_calloc(sizeof(struct uwsgi_router_redis_conf));
        if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
			"addr", &urrc->addr,
                        "key", &urrc->key,
                        "expires", &urrc->expires, NULL)) {
                        uwsgi_log("invalid redisstore route syntax: %s\n", args);
			return -1;
                }

		if (!urrc->key || !urrc->addr) {
                        uwsgi_log("invalid redisstore route syntax: you need to specify an address and a key\n");
			return -1;
                }

		urrc->key_len = strlen(urrc->key);
		urrc->addr_len = strlen(urrc->addr);

                if (!urrc->expires) urrc->expires = "0";

        ur->data2 = urrc;
        return 0;
}


static void router_redis_register() {
	uwsgi_register_router("redis", uwsgi_router_redis);
	uwsgi_register_router("redis-continue", uwsgi_router_redis_continue);
	uwsgi_register_router("redisstore", uwsgi_router_redis_store);
        uwsgi_register_router("redis-store", uwsgi_router_redis_store);
}

#endif

struct uwsgi_plugin router_redis_plugin = {
	.name = "router_redis",
#ifdef UWSGI_ROUTING
	.on_load = router_redis_register,
#endif
};
