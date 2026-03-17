#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

// apply templating
static int transform_template(struct wsgi_request *wsgi_req, struct uwsgi_transformation *ut) {

	struct uwsgi_route *ur = (struct uwsgi_route *) ut->data;

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ut->chunk->buf, ut->chunk->pos);
        if (!ub) return -1;
        uwsgi_buffer_map(ut->chunk, ub->buf, ub->pos);
        ub->buf = NULL;
        uwsgi_buffer_destroy(ub);
        return 0;
}
static int uwsgi_router_template_func(struct wsgi_request *wsgi_req, struct uwsgi_route *route) {
        uwsgi_add_transformation(wsgi_req, transform_template, route);
        return UWSGI_ROUTE_NEXT;
}
static int uwsgi_router_template(struct uwsgi_route *ur, char *arg) {
        ur->func = uwsgi_router_template_func;
        return 0;
}

static void transformation_template_register(void) {
        uwsgi_register_router("template", uwsgi_router_template);
}

struct uwsgi_plugin transformation_template_plugin = {
        .name = "transformation_template",
        .on_load = transformation_template_register,
};

#else
struct uwsgi_plugin transformation_template_plugin = {
        .name = "transformation_template",
};
#endif
