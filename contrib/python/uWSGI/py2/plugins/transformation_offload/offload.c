#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#if defined(UWSGI_ROUTING)

/*

	offload transformation

	each chunk is buffered to the transformation custom buffer

	if the buffer grows higher than the specified limit, the response will be buffered to disk

	on final the memory (or the tmpfile) will be offloaded

	even if we are talking about buffering, this is a streaming transformation
	as we need to hold control on memory usage

	"offload" MUST BE the last transformation in the chain. Hellish things will happen otherwise...

*/

static int transform_offload(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {

	if (ut->is_final) {
		struct uwsgi_transformation *orig_ut = (struct uwsgi_transformation *) ut->data;
		// sendfile offload
		if (orig_ut->fd > -1) {
			if (!uwsgi_offload_request_sendfile_do(wsgi_req, orig_ut->fd, 0, orig_ut->len)) {
				// the fd will be closed by the offload engine
				orig_ut->fd = -1;
                        	wsgi_req->via = UWSGI_VIA_OFFLOAD;
                        	wsgi_req->response_size += orig_ut->len;
                        	return 0;
                	}
			// fallback to non-offloaded write
			if (uwsgi_simple_sendfile(wsgi_req, orig_ut->fd, 0, orig_ut->len)) {
				return -1;
			}
			wsgi_req->response_size += orig_ut->len;
			return 0;
		}
		// memory offload
		if (orig_ut->ub) {
			if (!uwsgi_offload_request_memory_do(wsgi_req, orig_ut->ub->buf, orig_ut->ub->pos)) {
				// memory will be freed by the offload engine
				orig_ut->ub->buf = NULL;
				wsgi_req->via = UWSGI_VIA_OFFLOAD;
				wsgi_req->response_size += orig_ut->ub->pos;
				return 0;
			}
			// fallback to non-offloaded write
			if (uwsgi_simple_write(wsgi_req, orig_ut->ub->buf, orig_ut->ub->pos)) {
				return -1;
			}
			wsgi_req->response_size += orig_ut->ub->pos;
			return -1;
		}
		return 0;
	}

	// check if we need to start buffering to disk
	if (ut->fd == -1 && ut->len + ut->chunk->pos > ut->custom64) {
		ut->fd = uwsgi_tmpfd();
		if (ut->fd < 0) return -1; 
		// save already buffered data
		if (ut->ub) {
			ssize_t wlen = write(ut->fd, ut->ub->buf, ut->ub->pos);
			if (wlen != (ssize_t) ut->ub->pos) {
                        	uwsgi_req_error("transform_offload/write()");
                        	return -1;
                	}
		}
	}
	
	// if fd > -1, append to file
	if (ut->fd > -1) {
		ssize_t wlen = write(ut->fd, ut->chunk->buf, ut->chunk->pos);
		if (wlen != (ssize_t) ut->chunk->pos) {
			uwsgi_req_error("transform_offload/write()");
			return -1;
		}
		ut->len += wlen;
		goto done;
	}

	// buffer to memory
	if (!ut->ub) {
		ut->ub = uwsgi_buffer_new(ut->chunk->pos);
	}

	// append the chunk to the custom buffer
	if (uwsgi_buffer_append(ut->ub, ut->chunk->buf, ut->chunk->pos)) return -1;
	ut->len += ut->chunk->pos;
done:
	// reset the chunk !!!
	ut->chunk->pos = 0;
	return 0;
}

static int uwsgi_routing_func_offload(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	if (!wsgi_req->socket->can_offload) {
		uwsgi_log("unable to use the offload transformation without offload threads !!!\n");
		return UWSGI_ROUTE_BREAK;
	}
	struct uwsgi_transformation *ut = uwsgi_add_transformation(wsgi_req, transform_offload, NULL);
	ut->can_stream = 1;
	ut->custom64 = ur->custom;
	// add a "final" transformation to add the trailing chunk
	ut = uwsgi_add_transformation(wsgi_req, transform_offload, ut);
	ut->is_final = 1;
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_offload(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_offload;
	if (args[0] == 0) {
		// 1 MB limit
		ur->custom = 1024 * 1024;
	}
	else {
		ur->custom = strtoul(args, NULL, 10);
	}
	return 0;
}

static void router_offload_register(void) {
	uwsgi_register_router("offload", uwsgi_router_offload);
}

struct uwsgi_plugin transformation_offload_plugin = {
	.name = "transformation_offload",
	.on_load = router_offload_register,
};
#else
struct uwsgi_plugin transformation_offload_plugin = {
	.name = "transformation_offload",
};
#endif
