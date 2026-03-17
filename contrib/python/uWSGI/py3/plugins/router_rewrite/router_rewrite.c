#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>
#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

static int uwsgi_routing_func_rewrite(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	char *tmp_qs = NULL;

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data, ur->data_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

	uint16_t path_info_len = ub->pos;
	uint16_t query_string_len = 0;
	
	char *query_string = memchr(ub->buf, '?', ub->pos);
	if (query_string) {
		path_info_len = query_string - ub->buf;
		query_string++;
		query_string_len = ub->pos - (path_info_len + 1);
		if (wsgi_req->query_string_len > 0) {
			tmp_qs = uwsgi_concat4n(query_string, query_string_len, "&", 1, wsgi_req->query_string, wsgi_req->query_string_len, "", 0);
			query_string = tmp_qs;
			query_string_len = strlen(query_string);
		}	
	}
	// over engineering, could be required in the future...
	else {
		if (wsgi_req->query_string_len > 0) {
			query_string = wsgi_req->query_string;
			query_string_len = wsgi_req->query_string_len;
		}
		else {
			query_string = "";
		}
	}

	char *path_info = uwsgi_malloc(path_info_len);
	http_url_decode(ub->buf, &path_info_len, path_info);

	char *ptr = uwsgi_req_append(wsgi_req, "PATH_INFO", 9, path_info, path_info_len);
        if (!ptr) goto clear;

	free(path_info);
	path_info = NULL;

	// set new path_info
	wsgi_req->path_info = ptr;
	wsgi_req->path_info_len = path_info_len;

	ptr = uwsgi_req_append(wsgi_req, "QUERY_STRING", 12, query_string, query_string_len);
	if (!ptr) goto clear;

	// set new query_string
	wsgi_req->query_string = ptr;
	wsgi_req->query_string_len = query_string_len;

	uwsgi_buffer_destroy(ub);
	if (tmp_qs) free(tmp_qs);
	if (ur->custom)
		return UWSGI_ROUTE_CONTINUE;
	return UWSGI_ROUTE_NEXT;

clear:
	uwsgi_buffer_destroy(ub);
	if (tmp_qs) free(tmp_qs);
	if (path_info) free(path_info);
	return UWSGI_ROUTE_BREAK;
}

static int uwsgi_router_rewrite(struct uwsgi_route *ur, char *args) {

        ur->func = uwsgi_routing_func_rewrite;
        ur->data = args;
        ur->data_len = strlen(args);
        return 0;
}


static int uwsgi_router_rewrite_last(struct uwsgi_route *ur, char *args) {

        ur->func = uwsgi_routing_func_rewrite;
        ur->data = args;
        ur->data_len = strlen(args);
	ur->custom = 1;
        return 0;
}


static void router_rewrite_register(void) {
	uwsgi_register_router("rewrite", uwsgi_router_rewrite);
	uwsgi_register_router("rewrite-last", uwsgi_router_rewrite_last);
}

struct uwsgi_plugin router_rewrite_plugin = {
	.name = "router_rewrite",
	.on_load = router_rewrite_register,
};
#else
struct uwsgi_plugin router_rewrite_plugin = {
	.name = "router_rewrite",
};
#endif
