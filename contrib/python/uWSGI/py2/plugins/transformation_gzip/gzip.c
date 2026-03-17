#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#if defined(UWSGI_ROUTING) && defined(UWSGI_ZLIB)

/*

	gzip transformations add content-encoding to your headers and changes the final size !!!

	remember to fix the content_length (or use chunked encoding) !!!

*/

struct uwsgi_transformation_gzip {
	z_stream z;
	uint32_t crc32;
	size_t len;
	uint8_t header;
};

extern char gzheader[];

static int transform_gzip(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {
	struct uwsgi_transformation_gzip *utgz = (struct uwsgi_transformation_gzip *) ut->data;
	struct uwsgi_buffer *ub = ut->chunk;

	if (ut->is_final) {
		if (utgz->len > 0 && uwsgi_gzip_fix(&utgz->z, utgz->crc32, ub, utgz->len)) {
			free(utgz);
			return -1;
		}
		free(utgz);
		return 0;
	}

	// TODO: Temporary fix for a memory leak, replace with a proper one after it's available:
	// https://github.com/unbit/uwsgi/issues/1749
	// if (ub->pos == 0) {
	// 	// Don't try to compress empty responses.
	// 	return 0;
	// }

	size_t dlen = 0;
	char *gzipped = uwsgi_gzip_chunk(&utgz->z, &utgz->crc32, ub->buf, ub->pos, &dlen);
	if (!gzipped) return -1;
	utgz->len += ub->pos;
	uwsgi_buffer_map(ub, gzipped, dlen);
	if (!utgz->header) {
		// do not check for errors !!!
		uwsgi_response_add_header(wsgi_req, "Content-Encoding", 16, "gzip", 4);
		utgz->header = 1;
		if (uwsgi_buffer_insert(ub, 0, gzheader, 10)) {
			return -1;
		}
	}

	return 0;
}

static int uwsgi_routing_func_gzip(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	struct uwsgi_transformation_gzip *utgz = uwsgi_calloc(sizeof(struct uwsgi_transformation_gzip));
	if (uwsgi_gzip_prepare(&utgz->z, NULL, 0, &utgz->crc32)) {
		free(utgz);
		return UWSGI_ROUTE_BREAK;
	}
	struct uwsgi_transformation *ut = uwsgi_add_transformation(wsgi_req, transform_gzip, utgz);
	ut->can_stream = 1;
	// this is the trasformation clearing the memory
	ut = uwsgi_add_transformation(wsgi_req, transform_gzip, utgz);
	ut->is_final = 1;
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_gzip(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_gzip;
	return 0;
}

static void router_gzip_register(void) {
	uwsgi_register_router("gzip", uwsgi_router_gzip);
}

struct uwsgi_plugin transformation_gzip_plugin = {
	.name = "transformation_gzip",
	.on_load = router_gzip_register,
};
#else
struct uwsgi_plugin transformation_gzip_plugin = {
	.name = "transformation_gzip",
};
#endif
