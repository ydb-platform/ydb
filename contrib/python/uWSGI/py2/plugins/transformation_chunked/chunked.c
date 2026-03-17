#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#if defined(UWSGI_ROUTING)

/*

	transfer-encoding is added to the headers

*/

static int transform_chunked(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {
	struct uwsgi_buffer *ub = ut->chunk;

	if (ut->is_final) {
		if (uwsgi_buffer_append(ub, "0\r\n\r\n", 5)) return -1;
		return 0;
	}

	if (ut->round == 1) {
		// do not check for errors !!!
        	uwsgi_response_add_header(wsgi_req, "Transfer-Encoding", 17, "chunked", 7);
	}

	if (ub->pos > 0) {
		if (uwsgi_buffer_insert_chunked(ub, 0, ub->pos)) return -1;
		if (uwsgi_buffer_append(ub, "\r\n", 2)) return -1;
	}

	return 0;
}

static int uwsgi_routing_func_chunked(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	struct uwsgi_transformation *ut = uwsgi_add_transformation(wsgi_req, transform_chunked, NULL);
	ut->can_stream = 1;
	// add a "final" transformation to add the trailing chunk
	ut = uwsgi_add_transformation(wsgi_req, transform_chunked, NULL);
	ut->is_final = 1;
	return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_chunked(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_chunked;
	return 0;
}

static void router_chunked_register(void) {
	uwsgi_register_router("chunked", uwsgi_router_chunked);
}

struct uwsgi_plugin transformation_chunked_plugin = {
	.name = "transformation_chunked",
	.on_load = router_chunked_register,
};
#else
struct uwsgi_plugin transformation_chunked_plugin = {
	.name = "transformation_chunked",
};
#endif
