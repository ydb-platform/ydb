#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

static int uwsgi_routing_func_redirect(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	struct uwsgi_buffer *ub = NULL;

	if (uwsgi_response_prepare_headers(wsgi_req, "302 Found", 9)) goto end;
	
	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data, ur->data_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	if (uwsgi_response_add_header(wsgi_req, "Location", 8, ub->buf, ub->pos)) goto end;
	// no need to check the ret value
	uwsgi_response_write_body_do(wsgi_req, "Moved", 5);
end:
	if (ub)
		uwsgi_buffer_destroy(ub);
	return UWSGI_ROUTE_BREAK;
}

static int uwsgi_routing_func_redirect_permanent(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	struct uwsgi_buffer *ub = NULL;

        if (uwsgi_response_prepare_headers(wsgi_req, "301 Moved Permanently", 21)) goto end;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data, ur->data_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        if (uwsgi_response_add_header(wsgi_req, "Location", 8, ub->buf, ub->pos)) goto end;

        // no need to check the ret value
        uwsgi_response_write_body_do(wsgi_req, "Moved Permanently", 17);
end:
	if (ub)
                uwsgi_buffer_destroy(ub);
        return UWSGI_ROUTE_BREAK;
}



static int uwsgi_router_redirect(struct uwsgi_route *ur, char *args) {

	ur->func = uwsgi_routing_func_redirect;
	ur->data = args;
	ur->data_len = strlen(args);
	return 0;
}

static int uwsgi_router_redirect_permanent(struct uwsgi_route *ur, char *args) {

        ur->func = uwsgi_routing_func_redirect_permanent;
        ur->data = args;
        ur->data_len = strlen(args);
        return 0;
}



static void router_redirect_register(void) {

	uwsgi_register_router("redirect", uwsgi_router_redirect);
	uwsgi_register_router("redirect-302", uwsgi_router_redirect);
	uwsgi_register_router("redirect-permanent", uwsgi_router_redirect_permanent);
	uwsgi_register_router("redirect-301", uwsgi_router_redirect_permanent);
}

struct uwsgi_plugin router_redirect_plugin = {

	.name = "router_redirect",
	.on_load = router_redirect_register,
};
#else
struct uwsgi_plugin router_redirect_plugin = {
	.name = "router_redirect",
};
#endif
