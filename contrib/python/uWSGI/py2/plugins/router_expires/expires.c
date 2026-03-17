#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

extern struct uwsgi_server uwsgi;

/*

	by Unbit

	syntax:

	route = /^foobar1(.*)/ expires:filename=foo$1poo,value=30
	route = /^foobar1(.*)/ expires:unix=${time[unix]},value=30

*/

struct uwsgi_router_expires_conf {

	char *filename;
	size_t filename_len;

	char *unixt;
	size_t unixt_len;

	char *value;
	size_t value_len;
};

static int uwsgi_routing_func_expires(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_expires_conf *urec = (struct uwsgi_router_expires_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	uint64_t expires = 0;

	if (urec->filename) {

        	struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urec->filename, urec->filename_len);
        	if (!ub) return UWSGI_ROUTE_BREAK;

		struct stat st;
		if (stat(ub->buf, &st)) {
			uwsgi_buffer_destroy(ub);
			return UWSGI_ROUTE_BREAK;
		}

		expires = st.st_mtime;
		uwsgi_buffer_destroy(ub);

	}
	else if (urec->unixt) {
		struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urec->unixt, urec->unixt_len);
                if (!ub) return UWSGI_ROUTE_BREAK;

		expires = strtoul(ub->buf, NULL, 10);
		uwsgi_buffer_destroy(ub);
	}

	if (urec->value) {
		struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urec->value, urec->value_len);
		if (!ub) return UWSGI_ROUTE_BREAK;

		expires += atoi(ub->buf);
        	uwsgi_buffer_destroy(ub);
	}

	char expires_str[7 + 2 + 31];
        int len = uwsgi_http_date((time_t) expires, expires_str + 9);
        if (!len) return UWSGI_ROUTE_BREAK;

	memcpy(expires_str, "Expires: ", 9);

	uwsgi_additional_header_add(wsgi_req, expires_str, 9 + len);

        return UWSGI_ROUTE_NEXT;
}

static int uwsgi_router_expires(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_expires;
        ur->data = args;
        ur->data_len = strlen(args);
        struct uwsgi_router_expires_conf *urec = uwsgi_calloc(sizeof(struct uwsgi_router_expires_conf));
                if (uwsgi_kvlist_parse(ur->data, ur->data_len, ',', '=',
                        "filename", &urec->filename,
                        "file", &urec->filename,
                        "unix", &urec->unixt,
                        "value", &urec->value,
                        NULL)) {
                        uwsgi_log("invalid route syntax: %s\n", args);
                        exit(1);
                }

		if (urec->filename) {
			urec->filename_len = strlen(urec->filename);
		}

		if (urec->unixt) {
                        urec->unixt_len = strlen(urec->unixt);
                }

		if (urec->value) {
                        urec->value_len = strlen(urec->value);
                }

                ur->data2 = urec;
        return 0;
}

static void router_expires_register() {
	uwsgi_register_router("expires", uwsgi_router_expires);
}

struct uwsgi_plugin router_expires_plugin = {
	.name = "router_expires",
	.on_load = router_expires_register,
};

#else
struct uwsgi_plugin router_expires_plugin = {
	.name = "router_expires",
};
#endif
