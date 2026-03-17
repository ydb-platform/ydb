#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

struct uwsgi_router_metric_conf {

	// the name of the metric
	char *name;
	size_t name_len;

	// the value for the metric
	char *value;
	size_t value_len;

	int (*func)(char *, char *, int64_t);
};

// metricinc/metricdec/metricmul/metricdiv/metricset
static int uwsgi_routing_func_metricmath(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        struct uwsgi_router_metric_conf *urmc = (struct uwsgi_router_metric_conf *) ur->data2;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urmc->name, urmc->name_len);
        if (!ub) return UWSGI_ROUTE_BREAK;

        struct uwsgi_buffer *ub_val = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, urmc->value, urmc->value_len);
        if (!ub_val) {
               	uwsgi_buffer_destroy(ub);
               	return UWSGI_ROUTE_BREAK;
        }
	int64_t num = strtol(ub_val->buf, NULL, 10);
        uwsgi_buffer_destroy(ub_val);

        if (urmc->func(ub->buf, NULL, num)) {
                uwsgi_buffer_destroy(ub);
                return UWSGI_ROUTE_BREAK;
        }
        uwsgi_buffer_destroy(ub);
        return UWSGI_ROUTE_NEXT;
}


static struct uwsgi_router_metric_conf *uwsgi_router_metricmath(struct uwsgi_route *ur, char *args) {
	ur->func = uwsgi_routing_func_metricmath;
	ur->data = args;
        ur->data_len = strlen(args);

	struct uwsgi_router_metric_conf *urmc = uwsgi_calloc(sizeof(struct uwsgi_router_metric_conf));
	char *comma = strchr(args, ',');
	if (comma) {
		urmc->value = comma+1;
		urmc->value_len = strlen(urmc->value);
		*comma = 0;
	}
	else {
		urmc->value = "1";
		urmc->value_len = 1;
	}

	urmc->name = args;
	urmc->name_len = strlen(args);
	if (comma) *comma = ',';

        ur->data2 = urmc;
        return urmc;
}

static int uwsgi_router_metricinc(struct uwsgi_route *ur, char *args) {
	struct uwsgi_router_metric_conf *urmc = uwsgi_router_metricmath(ur, args);
	if (!urmc) return -1;
	urmc->func = uwsgi_metric_inc;
	return 0;
}

static int uwsgi_router_metricdec(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_metric_conf *urmc = uwsgi_router_metricmath(ur, args);
        if (!urmc) return -1;
	urmc->func = uwsgi_metric_dec;
        return 0;
}

static int uwsgi_router_metricmul(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_metric_conf *urmc = uwsgi_router_metricmath(ur, args);
        if (!urmc) return -1;
	urmc->func = uwsgi_metric_mul;
        return 0;
}

static int uwsgi_router_metricdiv(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_metric_conf *urmc = uwsgi_router_metricmath(ur, args);
        if (!urmc) return -1;
	urmc->func = uwsgi_metric_div;
        return 0;
}


static int uwsgi_router_metricset(struct uwsgi_route *ur, char *args) {
        struct uwsgi_router_metric_conf *urmc = uwsgi_router_metricmath(ur, args);
        if (!urmc) return -1;
	urmc->func = uwsgi_metric_set;
        return 0;
}

static char *uwsgi_route_var_metric(struct wsgi_request *wsgi_req, char *key, uint16_t keylen, uint16_t *vallen) {
        int64_t metric = uwsgi_metric_getn(key, keylen, NULL, 0);
        char *ret = uwsgi_64bit2str(metric);
        *vallen = strlen(ret);
        return ret;
}

static void router_metrics_register() {
	uwsgi_register_router("metricinc", uwsgi_router_metricinc);
	uwsgi_register_router("metricdec", uwsgi_router_metricdec);
	uwsgi_register_router("metricmul", uwsgi_router_metricmul);
	uwsgi_register_router("metricdiv", uwsgi_router_metricdiv);
	uwsgi_register_router("metricset", uwsgi_router_metricset);

        struct uwsgi_route_var *urv = uwsgi_register_route_var("metric", uwsgi_route_var_metric);
        urv->need_free = 1;

}

struct uwsgi_plugin router_metrics_plugin = {
	.name = "router_metrics",
	.on_load = router_metrics_register,
};

#else
struct uwsgi_plugin router_metrics_plugin = {
	.name = "router_metrics",
};
#endif
