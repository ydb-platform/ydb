#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	uWSGI metrics subsystem

	a metric is a node in a linked list reachable via a numeric id (OID, in SNMP way) or a simple string:

	uwsgi.worker.1.requests
	uwsgi.custom.foo.bar

	the oid representation:

		1.3.6.1.4.1.35156.17 = iso.org.dod.internet.private.enterprise.unbit.uwsgi
		1.3.6.1.4.1.35156.17.3.1.1 = iso.org.dod.internet.private.enterprise.unbit.uwsgi.worker.1.requests
		1.3.6.1.4.1.35156.17.3.1.1 = iso.org.dod.internet.private.enterprise.unbit.uwsgi.worker.1.requests
		1.3.6.1.4.1.35156.17.3.1.2.1.1 = iso.org.dod.internet.private.enterprise.unbit.uwsgi.worker.1.core.1.requests
		...

	each metric is a collected value with a specific frequency
	metrics are meant for numeric values signed 64 bit, but they can be exposed as:

	gauge
	counter
	absolute

	metrics are managed by a dedicated thread (in the master) holding a linked list of all the items. For few metrics it is a good (read: simple) approach,
	but you can cache lookups in a uWSGI cache for really big list. (TODO)

	struct uwsgi_metric *um = uwsgi_register_metric("worker.1.requests", "3.1.1", UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[1].requests, 0, NULL);
	prototype: struct uwsgi_metric *uwsgi_register_metric(char *name, char *oid, uint8_t value_type, char *collector, void *ptr, uint32_t freq, void *custom);

	value_type = UWSGI_METRIC_COUNTER/UWSGI_METRIC_GAUGE/UWSGI_METRIC_ABSOLUTE
	collect_way = "ptr" -> get from a pointer / UWSGI_METRIC_FUNC -> get from a func with the prototype int64_t func(struct uwsgi_metric *); / UWSGI_METRIC_FILE -> get the value from a file, ptr is the filename

	For some metric (or all ?) you may want to hold a value even after a server reload. For such a reason you can specify a directory on wich the server (on startup/restart) will look for
	a file named like the metric and will read the initial value from it. It may look an old-fashioned and quite inefficient way, but it is the most versatile for a sysadmin (allowing him/her
	to even modify the values manually)

	When registering a metric with the same name of an already registered one, the new one will overwrite the previous one. This allows plugins writer to override default behaviours

	Applications are allowed to update metrics (but they cannot register new ones), with simple api funcs:

	uwsgi.metric_set("worker.1.requests", N)
	uwsgi.metric_inc("worker.1.requests", N=1)
	uwsgi.metric_dec("worker.1.requests", N=1)
	uwsgi.metric_mul("worker.1.requests", N=1)
	uwsgi.metric_div("worker.1.requests", N=1)

	and obviously they can get values:

	uwsgi.metric_get("worker.1.requests")

	Updating metrics from your app MUST BE ATOMIC, for such a reason a uWSGI rwlock is initialized on startup and used for each operation (simple reading from a metric does not require locking)

	Metrics can be updated from the internal routing subsystem too:

		route-if = equal:${REQUEST_URI};/foobar metricinc:foobar.test 2

	and can be accessed as ${metric[foobar.test]}

	The stats server exports the metrics list in the "metrics" attribute (obviously some info could be redundant)

*/


int64_t uwsgi_metric_collector_file(struct uwsgi_metric *metric) {
	char *filename = metric->arg1;
	if (!filename) return 0;
	int split_pos = metric->arg1n;

	char buf[4096];
	int64_t ret = 0;
	int fd = open(filename, O_RDONLY);
	if (fd < 0) {
		uwsgi_error_open(filename);
		return 0;
	}

	ssize_t rlen = read(fd, buf, 4096);
	if (rlen <= 0) goto end;

	char *ptr = buf;
	ssize_t i;
	int pos = 0;
	int found = 0;
	for(i=0;i<rlen;i++) {
		if (!found) {
			if (buf[i] == ' ' || buf[i] == '\t' || buf[i] == '\r' || buf[i] == 0 || buf[i] == '\n') {
				if (pos == split_pos) goto found;
				found = 1;
			}
		}
		else {
			if (!(buf[i] == ' ' || buf[i] == '\t' || buf[i] == '\r' || buf[i] == 0 || buf[i] == '\n')) {
				found = 0;
				ptr = buf + i;
				pos++;
			}
		}
	}

	if (pos == split_pos) goto found;
	goto end;
found:
	ret = strtoll(ptr, NULL, 10);
end:
	close(fd);
	return ret;

}



/*

	allowed chars for metrics name

	0-9
	a-z
	A-Z
	.
	-
	_

*/

static int uwsgi_validate_metric_name(char *buf) {
	size_t len = strlen(buf);
	size_t i;
	for(i=0;i<len;i++) {
		if ( !(
			(buf[i] >= '0' && buf[i] <= '9') ||
			(buf[i] >= 'a' && buf[i] <= 'z') ||
			(buf[i] >= 'A' && buf[i] <= 'Z') ||
			buf[i] == '.' || buf[i] == '-' || buf[i] == '_'
		)) {

			return 0;
		
		}
	}	

	return 1;
}

/*

	allowed chars for metrics oid

	0-9
	.

	oids can be null
*/

static int uwsgi_validate_metric_oid(char *buf) {
	if (!buf) return 1;
        size_t len = strlen(buf);
        size_t i;
        for(i=0;i<len;i++) {
                if ( !(
                        (buf[i] >= '0' && buf[i] <= '9') ||
                        buf[i] == '.'
                )) {
                
                        return 0;
                
                }
        }

        return 1;
}

void uwsgi_metric_append(struct uwsgi_metric *um) {
	struct uwsgi_metric *old_metric=NULL,*metric=uwsgi.metrics;
	while(metric) {
		old_metric = metric;
                metric = metric->next;
	}

	if (old_metric) {
                       old_metric->next = um;
        }
        else {
        	uwsgi.metrics = um;
        }

        uwsgi.metrics_cnt++;
}

struct uwsgi_metric_collector *uwsgi_metric_collector_by_name(char *name) {
	if (!name) return NULL;
	struct uwsgi_metric_collector *umc = uwsgi.metric_collectors;
	while(umc) {
		if (!strcmp(name, umc->name)) return umc;
		umc = umc->next;
	}
	uwsgi_log("unable to find metric collector \"%s\"\n", name);
	exit(1);
}

struct uwsgi_metric *uwsgi_register_metric_do(char *name, char *oid, uint8_t value_type, char *collector, void *ptr, uint32_t freq, void *custom, int do_not_push) {
	if (!uwsgi.has_metrics) return NULL;
	struct uwsgi_metric *old_metric=NULL,*metric=uwsgi.metrics;

	if (!uwsgi_validate_metric_name(name)) {
		uwsgi_log("invalid metric name: %s\n", name);
		exit(1);
	}

	if (!uwsgi_validate_metric_oid(oid)) {
		uwsgi_log("invalid metric oid: %s\n", oid);
		exit(1);
	}

	while(metric) {
		if (!strcmp(metric->name, name)) {
			goto found;
		}
		old_metric = metric;
		metric = metric->next;
	}

	metric = uwsgi_calloc(sizeof(struct uwsgi_metric));
	// always make a copy of the name (so we can use stack for building strings)
	metric->name = uwsgi_str(name);
	metric->name_len = strlen(metric->name);

	if (!do_not_push) {
		if (old_metric) {
			old_metric->next = metric;
		}
		else {
			uwsgi.metrics = metric;
		}

		uwsgi.metrics_cnt++;
	}

found:
	metric->oid = oid;
	if (metric->oid) {
		metric->oid_len = strlen(oid);
		metric->oid = uwsgi_str(oid);
		char *p, *ctx = NULL;
		char *oid_tmp = uwsgi_str(metric->oid);
		// slower but we save lot of memory
		struct uwsgi_buffer *ub = uwsgi_buffer_new(1);
                uwsgi_foreach_token(oid_tmp, ".", p, ctx) {
			uint64_t l = strtoull(p, NULL, 10);	
			if (uwsgi_base128(ub, l, 1)) {
				uwsgi_log("unable to encode oid %s to asn/ber\n", metric->oid);
				exit(1);
			}
		}
		metric->asn = ub->buf;
		metric->asn_len = ub->pos;
		ub->buf = NULL;
		uwsgi_buffer_destroy(ub);
		free(oid_tmp);
	}
	metric->type = value_type;
	metric->collector = uwsgi_metric_collector_by_name(collector);
	metric->ptr = ptr;
	metric->freq = freq;
	if (!metric->freq) metric->freq = 1;
	metric->custom = custom;

	if (uwsgi.metrics_dir) {
		char *filename = uwsgi_concat3(uwsgi.metrics_dir, "/", name);
		int fd = open(filename, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP);
		if (fd < 0) {
			uwsgi_error_open(filename);
			exit(1);
		}
		// fill the file
		if (lseek(fd, uwsgi.page_size-1, SEEK_SET) < 0) {
			uwsgi_error("uwsgi_register_metric()/lseek()");
			uwsgi_log("unable to register metric: %s\n", name);
			exit(1);
		}
		if (write(fd, "\0", 1) != 1) {
			uwsgi_error("uwsgi_register_metric()/write()");
			uwsgi_log("unable to register metric: %s\n", name);
			exit(1);
		}
		metric->map = mmap(NULL, uwsgi.page_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		if (metric->map == MAP_FAILED) {
			uwsgi_error("uwsgi_register_metric()/mmap()");
			uwsgi_log("unable to register metric: %s\n", name);
			exit(1);
		}
		
		// we can now safely close the file descriptor and update the file from memory
		close(fd);
		free(filename);
	}

	return metric;
}

struct uwsgi_metric *uwsgi_register_metric(char *name, char *oid, uint8_t value_type, char *collector, void *ptr, uint32_t freq, void *custom) {
	return uwsgi_register_metric_do(name, oid, value_type, collector, ptr, freq, custom, 0);
}

struct uwsgi_metric *uwsgi_register_keyval_metric(char *arg) {
	char *m_name = NULL;
	char *m_oid = NULL;
	char *m_type = NULL;
	char *m_collector = NULL;
	char *m_freq = NULL;
	char *m_arg1 = NULL;
	char *m_arg2 = NULL;
	char *m_arg3 = NULL;
	char *m_arg1n = NULL;
	char *m_arg2n = NULL;
	char *m_arg3n = NULL;
	char *m_initial_value = NULL;
	char *m_children = NULL;
	char *m_alias = NULL;
	char *m_reset_after_push = NULL;

	if (!strchr(arg, '=')) {
		m_name = uwsgi_str(arg);
	}
	else if (uwsgi_kvlist_parse(arg, strlen(arg), ',', '=',
		"name", &m_name,
		"oid", &m_oid,
		"type", &m_type,
		"initial_value", &m_initial_value,
		"collector", &m_collector,
		"freq", &m_freq,
		"arg1", &m_arg1,
		"arg2", &m_arg2,
		"arg3", &m_arg3,
		"arg1n", &m_arg1n,
		"arg2n", &m_arg2n,
		"arg3n", &m_arg3n,
		"children", &m_children,
		"alias", &m_alias,
		"reset_after_push", &m_reset_after_push,
		NULL)) {
		uwsgi_log("invalid metric keyval syntax: %s\n", arg);
		exit(1);
	}

	if (!m_name) {
		uwsgi_log("you need to specify a metric name: %s\n", arg);
		exit(1);
	}

	uint8_t type = UWSGI_METRIC_COUNTER;
	char *collector = NULL;
	uint32_t freq = 0;
	int64_t initial_value = 0;

	if (m_type) {
		if (!strcmp(m_type, "gauge")) {
			type = UWSGI_METRIC_GAUGE;
		}
		else if (!strcmp(m_type, "absolute")) {
			type = UWSGI_METRIC_ABSOLUTE;
		}
		else if (!strcmp(m_type, "alias")) {
			type = UWSGI_METRIC_ALIAS;
		}
	}

	if (m_collector) {
		collector = m_collector;	
	}

	if (m_freq) freq = strtoul(m_freq, NULL, 10);

	
	if (m_initial_value) {
		initial_value = strtoll(m_initial_value, NULL, 10);		
	}

	struct uwsgi_metric* um =  uwsgi_register_metric(m_name, m_oid, type, collector, NULL, freq, NULL);
	um->initial_value = initial_value;

	if (m_reset_after_push){
		um->reset_after_push = 1;
	}

	if (m_children) {
		char *p, *ctx = NULL;
        	uwsgi_foreach_token(m_children, ";", p, ctx) {
			struct uwsgi_metric *child = uwsgi_metric_find_by_name(p);
			if (!child) {
				uwsgi_log("unable to find metric \"%s\"\n", p);
				exit(1);
			}
			uwsgi_metric_add_child(um, child);
                }
        }

	if (m_alias) {
		struct uwsgi_metric *alias = uwsgi_metric_find_by_name(m_alias);
		if (!alias) {
			uwsgi_log("unable to find metric \"%s\"\n", m_alias);
                        exit(1);
		}
		um->ptr = (void *) alias;
	}

	um->arg1 = m_arg1;
	um->arg2 = m_arg2;
	um->arg3 = m_arg3;

	if (m_arg1n) um->arg1n = strtoll(m_arg1n, NULL, 10);
	if (m_arg2n) um->arg2n = strtoll(m_arg2n, NULL, 10);
	if (m_arg3n) um->arg3n = strtoll(m_arg3n, NULL, 10);

	free(m_name);
	if (m_oid) free(m_oid);
	if (m_type) free(m_type);
	if (m_collector) free(m_collector);
	if (m_freq) free(m_freq);
	/*
	DO NOT FREE THEM
	if (m_arg1) free(m_arg1);
	if (m_arg2) free(m_arg2);
	if (m_arg3) free(m_arg3);
	*/
	if (m_arg1n) free(m_arg1n);
	if (m_arg2n) free(m_arg2n);
	if (m_arg3n) free(m_arg3n);
	if (m_initial_value) free(m_initial_value);
	if (m_children) free(m_children);
	if (m_alias) free(m_alias);
	if (m_reset_after_push) free(m_reset_after_push);
	return um;
}

static void *uwsgi_metrics_loop(void *arg) {

	// block signals on this thread
        sigset_t smask;
        sigfillset(&smask);
#ifndef UWSGI_DEBUG
        sigdelset(&smask, SIGSEGV);
#endif
        pthread_sigmask(SIG_BLOCK, &smask, NULL);

	for(;;) {
		struct uwsgi_metric *metric = uwsgi.metrics;
		// every second scan the whole metrics tree
		time_t now = uwsgi_now();
		while(metric) {
			if (!metric->last_update) {
				metric->last_update = now;
			}
			else {
				if ((uint32_t) (now - metric->last_update) < metric->freq) goto next;
			}
			uwsgi_wlock(uwsgi.metrics_lock);
			int64_t value = *metric->value;
			// gather the new value based on the type of collection strategy
			if (metric->collector) {
				*metric->value = metric->initial_value + metric->collector->func(metric);
			}
			int64_t new_value = *metric->value;
			uwsgi_rwunlock(uwsgi.metrics_lock);

			metric->last_update = now;

			if (uwsgi.metrics_dir && metric->map) {
				if (value != new_value) {
					int ret = snprintf(metric->map, uwsgi.page_size, "%lld\n", (long long) new_value);
					if (ret > 0 && ret < uwsgi.page_size) {
						memset(metric->map+ret, 0, 4096-ret);
					}
				}
			}

			// thresholds;
			struct uwsgi_metric_threshold *umt = metric->thresholds;
			while(umt) {
				if (new_value >= umt->value) {
					if (umt->reset) {
						uwsgi_wlock(uwsgi.metrics_lock);
						*metric->value = umt->reset_value;
						uwsgi_rwunlock(uwsgi.metrics_lock);
					}

					if (umt->alarm) {
						if (umt->last_alarm + umt->rate <= now) {
							if (umt->msg) {
								uwsgi_alarm_trigger(umt->alarm, umt->msg, umt->msg_len);
							}
							else {
								uwsgi_alarm_trigger(umt->alarm, metric->name, metric->name_len);
							}
							umt->last_alarm = now;
						}
					}
				}
				umt = umt->next;
			}
next:
			metric = metric->next;
		}
		sleep(1);
	}

	return NULL;
	
}

void uwsgi_metrics_start_collector() {
	if (!uwsgi.has_metrics) return;
	pthread_t t;
        pthread_create(&t, NULL, uwsgi_metrics_loop, NULL);
	uwsgi_log("metrics collector thread started\n");
}

struct uwsgi_metric *uwsgi_metric_find_by_name(char *name) {
	struct uwsgi_metric *um = uwsgi.metrics;
	while(um) {
		if (!strcmp(um->name, name)) {
			return um;
		}	
		um = um->next;
	}

	return NULL;
}

struct uwsgi_metric *uwsgi_metric_find_by_namen(char *name, size_t len) {
        struct uwsgi_metric *um = uwsgi.metrics;
        while(um) {
                if (!uwsgi_strncmp(um->name, um->name_len, name, len)) {
                        return um;
                }
                um = um->next;
        }

        return NULL;
}

struct uwsgi_metric_child *uwsgi_metric_add_child(struct uwsgi_metric *parent, struct uwsgi_metric *child) {
	struct uwsgi_metric_child *umc = parent->children, *old_umc = NULL;
	while(umc) {
		old_umc = umc;
		umc = umc->next;
	}

	umc = uwsgi_calloc(sizeof(struct uwsgi_metric_child));
	umc->um = child;
	if (old_umc) {
		old_umc->next = umc;
	}
	else {
		parent->children = umc;
	}
	return umc;
}

struct uwsgi_metric *uwsgi_metric_find_by_oid(char *oid) {
        struct uwsgi_metric *um = uwsgi.metrics;
        while(um) {
                if (um->oid && !strcmp(um->oid, oid)) {
                        return um;
                }
                um = um->next;
        }

        return NULL;
}

struct uwsgi_metric *uwsgi_metric_find_by_oidn(char *oid, size_t len) {
        struct uwsgi_metric *um = uwsgi.metrics;
        while(um) {
                if (um->oid && !uwsgi_strncmp(um->oid, um->oid_len, oid, len)) {
                        return um;
                }
                um = um->next;
        }

        return NULL;
}

struct uwsgi_metric *uwsgi_metric_find_by_asn(char *asn, size_t len) {
        struct uwsgi_metric *um = uwsgi.metrics;
        while(um) {
                if (um->oid && um->asn && !uwsgi_strncmp(um->asn, um->asn_len, asn, len)) {
                        return um;
                }
                um = um->next;
        }

        return NULL;
}


/*

	api functions

	metric_set
	metric_inc
	metric_dec
	metric_mul
	metric_div

*/

#define um_op struct uwsgi_metric *um = NULL;\
	if (!uwsgi.has_metrics) return -1;\
	if (name) {\
                um = uwsgi_metric_find_by_name(name);\
        }\
        else if (oid) {\
                um = uwsgi_metric_find_by_oid(oid);\
        }\
        if (!um) return -1;\
	if (um->collector || um->type == UWSGI_METRIC_ALIAS) return -1;\
	uwsgi_wlock(uwsgi.metrics_lock)

int uwsgi_metric_set(char *name, char *oid, int64_t value) {
	um_op;
	*um->value = value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int uwsgi_metric_inc(char *name, char *oid, int64_t value) {
        um_op;
	*um->value += value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int uwsgi_metric_dec(char *name, char *oid, int64_t value) {
        um_op;
	*um->value -= value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int uwsgi_metric_mul(char *name, char *oid, int64_t value) {
        um_op;
	*um->value *= value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int uwsgi_metric_div(char *name, char *oid, int64_t value) {
	// avoid division by zero
	if (value == 0) return -1;
        um_op;
	*um->value /= value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int64_t uwsgi_metric_get(char *name, char *oid) {
	if (!uwsgi.has_metrics) return 0;
	int64_t ret = 0;
	struct uwsgi_metric *um = NULL;
	if (name) {
		um = uwsgi_metric_find_by_name(name);
	}
	else if (oid) {
		um = uwsgi_metric_find_by_oid(oid);
	}
	if (!um) return 0;

	// now (in rlocked context) we get the value from
	// the map
	uwsgi_rlock(uwsgi.metrics_lock);
	ret = *um->value;
	// unlock
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return ret;
}

int64_t uwsgi_metric_getn(char *name, size_t nlen, char *oid, size_t olen) {
        if (!uwsgi.has_metrics) return 0;
        int64_t ret = 0;
        struct uwsgi_metric *um = NULL;
        if (name) {
                um = uwsgi_metric_find_by_namen(name, nlen);
        }
        else if (oid) {
                um = uwsgi_metric_find_by_oidn(oid, olen);
        }
        if (!um) return 0;

        // now (in rlocked context) we get the value from
        // the map
        uwsgi_rlock(uwsgi.metrics_lock);
        ret = *um->value;
        // unlock
        uwsgi_rwunlock(uwsgi.metrics_lock);
        return ret;
}

int uwsgi_metric_set_max(char *name, char *oid, int64_t value) {
	um_op;
	if (value > *um->value)
	        *um->value = value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

int uwsgi_metric_set_min(char *name, char *oid, int64_t value) {
	um_op;
	if ((value > um->initial_value || 0) && value < *um->value)
		*um->value = value;
	uwsgi_rwunlock(uwsgi.metrics_lock);
	return 0;
}

#define uwsgi_metric_name(f, n) ret = snprintf(buf, 4096, f, n); if (ret <= 1 || ret >= 4096) { uwsgi_log("unable to register metric name %s\n", f); exit(1);}
#define uwsgi_metric_name2(f, n, n2) ret = snprintf(buf, 4096, f, n, n2); if (ret <= 1 || ret >= 4096) { uwsgi_log("unable to register metric name %s\n", f); exit(1);}

#define uwsgi_metric_oid(f, n) ret = snprintf(buf2, 4096, f, n); if (ret <= 1 || ret >= 4096) { uwsgi_log("unable to register metric oid %s\n", f); exit(1);}
#define uwsgi_metric_oid2(f, n, n2) ret = snprintf(buf2, 4096, f, n, n2); if (ret <= 1 || ret >= 4096) { uwsgi_log("unable to register metric oid %s\n", f); exit(1);}

void uwsgi_setup_metrics() {

	if (!uwsgi.has_metrics) return;

	char buf[4096];
	char buf2[4096];

	// create the main rwlock
	uwsgi.metrics_lock = uwsgi_rwlock_init("metrics");
	
	// get realpath of the storage dir
	if (uwsgi.metrics_dir) {
		char *dir = uwsgi_expand_path(uwsgi.metrics_dir, strlen(uwsgi.metrics_dir), NULL);
		if (!dir) {
			uwsgi_error("uwsgi_setup_metrics()/uwsgi_expand_path()");
			exit(1);
		}
		uwsgi.metrics_dir = dir;
	}

	// the 'core' namespace
	uwsgi_register_metric("core.routed_signals", "5.1", UWSGI_METRIC_COUNTER, "ptr", &uwsgi.shared->routed_signals, 0, NULL);
	uwsgi_register_metric("core.unrouted_signals", "5.2", UWSGI_METRIC_COUNTER, "ptr", &uwsgi.shared->unrouted_signals, 0, NULL);

	uwsgi_register_metric("core.busy_workers", "5.3", UWSGI_METRIC_GAUGE, "ptr", &uwsgi.shared->busy_workers, 0, NULL);
	uwsgi_register_metric("core.idle_workers", "5.4", UWSGI_METRIC_GAUGE, "ptr", &uwsgi.shared->idle_workers, 0, NULL);
	uwsgi_register_metric("core.overloaded", "5.5", UWSGI_METRIC_COUNTER, "ptr", &uwsgi.shared->overloaded, 0, NULL);

	// parents are appended only at the end
	struct uwsgi_metric *total_tx = uwsgi_register_metric_do("core.total_tx", "5.100", UWSGI_METRIC_COUNTER, "sum", NULL, 0, NULL, 1);
	struct uwsgi_metric *total_rss = uwsgi_register_metric_do("core.total_rss", "5.101", UWSGI_METRIC_GAUGE, "sum", NULL, 0, NULL, 1);
	struct uwsgi_metric *total_vsz = uwsgi_register_metric_do("core.total_vsz", "5.102", UWSGI_METRIC_GAUGE, "sum", NULL, 0, NULL, 1);
	struct uwsgi_metric *total_avg_rt = uwsgi_register_metric_do("core.avg_response_time", "5.103", UWSGI_METRIC_GAUGE, "avg", NULL, 0, NULL, 1);
	struct uwsgi_metric *total_running_time = uwsgi_register_metric_do("core.total_running_time", "5.104", UWSGI_METRIC_COUNTER, "sum", NULL, 0, NULL, 1);

	int ret;

	// create the 'worker' namespace
	int i;
	for(i=0;i<=uwsgi.numproc;i++) {

		uwsgi_metric_name("worker.%d.requests", i) ; uwsgi_metric_oid("3.%d.1", i);
		uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].requests, 0, NULL);

		uwsgi_metric_name("worker.%d.delta_requests", i) ; uwsgi_metric_oid("3.%d.2", i);
		uwsgi_register_metric(buf, buf2, UWSGI_METRIC_ABSOLUTE, "ptr", &uwsgi.workers[i].delta_requests, 0, NULL);

		uwsgi_metric_name("worker.%d.failed_requests", i) ; uwsgi_metric_oid("3.%d.13", i);
		uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].failed_requests, 0, NULL);

		uwsgi_metric_name("worker.%d.respawns", i) ; uwsgi_metric_oid("3.%d.14", i);
		uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].respawn_count, 0, NULL);

		uwsgi_metric_name("worker.%d.avg_response_time", i) ; uwsgi_metric_oid("3.%d.8", i);
		struct uwsgi_metric *avg_rt = uwsgi_register_metric(buf, buf2, UWSGI_METRIC_GAUGE, "ptr", &uwsgi.workers[i].avg_response_time, 0, NULL);
		if (i > 0) uwsgi_metric_add_child(total_avg_rt, avg_rt);

		uwsgi_metric_name("worker.%d.total_tx", i) ; uwsgi_metric_oid("3.%d.9", i);
		struct uwsgi_metric *tx = uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].tx, 0, NULL);
		if (i > 0) uwsgi_metric_add_child(total_tx, tx);

		uwsgi_metric_name("worker.%d.rss_size", i) ; uwsgi_metric_oid("3.%d.11", i);
		struct uwsgi_metric *rss = uwsgi_register_metric(buf, buf2, UWSGI_METRIC_GAUGE, "ptr", &uwsgi.workers[i].rss_size, 0, NULL);
		if (i > 0) uwsgi_metric_add_child(total_rss, rss);

		uwsgi_metric_name("worker.%d.vsz_size", i) ; uwsgi_metric_oid("3.%d.12", i);
		struct uwsgi_metric *vsz = uwsgi_register_metric(buf, buf2, UWSGI_METRIC_GAUGE, "ptr", &uwsgi.workers[i].vsz_size, 0, NULL);
		if (i > 0) uwsgi_metric_add_child(total_vsz, vsz);

		uwsgi_metric_name("worker.%d.running_time", i) ; uwsgi_metric_oid("3.%d.13", i);
		struct uwsgi_metric *running_time = uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].running_time, 0, NULL);
		if (i > 0) uwsgi_metric_add_child(total_running_time, running_time);

		// skip core metrics for worker 0
		if (i == 0) continue;

		if (uwsgi.metrics_no_cores) continue;

		int j;
		for(j=0;j<uwsgi.cores;j++) {
			uwsgi_metric_name2("worker.%d.core.%d.requests", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.1", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].requests, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.write_errors", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.3", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].write_errors, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.routed_requests", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.4", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].routed_requests, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.static_requests", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.5", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].static_requests, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.offloaded_requests", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.6", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].offloaded_requests, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.exceptions", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.7", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].exceptions, 0, NULL);

			uwsgi_metric_name2("worker.%d.core.%d.read_errors", i, j) ; uwsgi_metric_oid2("3.%d.2.%d.8", i, j);
			uwsgi_register_metric(buf, buf2, UWSGI_METRIC_COUNTER, "ptr", &uwsgi.workers[i].cores[j].read_errors, 0, NULL);

		}
	}

	// append parents
	uwsgi_metric_append(total_tx);
	uwsgi_metric_append(total_rss);
	uwsgi_metric_append(total_vsz);
	uwsgi_metric_append(total_avg_rt);
	uwsgi_metric_append(total_running_time);

	// sockets
	struct uwsgi_socket *uwsgi_sock = uwsgi.sockets;
	int pos = 0;
	while(uwsgi_sock) {
		uwsgi_metric_name("socket.%d.listen_queue", pos) ; uwsgi_metric_oid("7.%d.1", pos);
                uwsgi_register_metric(buf, buf2, UWSGI_METRIC_GAUGE, "ptr", &uwsgi_sock->queue, 0, NULL);
		pos++;
		uwsgi_sock = uwsgi_sock->next;
	}

	// create aliases
	uwsgi_register_metric("rss_size", NULL, UWSGI_METRIC_ALIAS, NULL, total_rss, 0, NULL);
	uwsgi_register_metric("vsz_size", NULL, UWSGI_METRIC_ALIAS, NULL, total_vsz, 0, NULL);

	// create custom/user-defined metrics
	struct uwsgi_string_list *usl;
	uwsgi_foreach(usl, uwsgi.additional_metrics) {
		struct uwsgi_metric *um = uwsgi_register_keyval_metric(usl->value);
		if (um) {
			uwsgi_log("added custom metric: %s\n", um->name);
		}
	}

	// allocate shared memory
	int64_t *values = uwsgi_calloc_shared(sizeof(int64_t) * uwsgi.metrics_cnt);
	pos = 0;

	struct uwsgi_metric *metric = uwsgi.metrics;
	while(metric) {
		metric->value = &values[pos];
		pos++;
		metric = metric->next;
	}

	// remap aliases
	metric = uwsgi.metrics;
        while(metric) {
		if (metric->type == UWSGI_METRIC_ALIAS) {
			struct uwsgi_metric *alias = (struct uwsgi_metric *) metric->ptr;
			if (!alias) {
				uwsgi_log("metric alias \"%s\" requires a mapping !!!\n", metric->name);
				exit(1);
			}
			metric->value = alias->value;
			metric->oid = alias->oid;
		}
		if (metric->initial_value) {
			*metric->value = metric->initial_value;
		}
		metric = metric->next;
	}

	// setup thresholds
	uwsgi_foreach(usl, uwsgi.metrics_threshold) {
		char *m_key = NULL;
		char *m_value = NULL;
		char *m_alarm = NULL;
		char *m_rate = NULL;
		char *m_reset = NULL;
		char *m_msg = NULL;
		if (uwsgi_kvlist_parse(usl->value, usl->len, ',', '=',
                	"key", &m_key,
                	"value", &m_value,
                	"alarm", &m_alarm,
                	"rate", &m_rate,
                	"msg", &m_msg,
                	"reset", &m_reset,
                	NULL)) {
                		uwsgi_log("invalid metric threshold keyval syntax: %s\n", usl->value);
                		exit(1);
		}

		if (!m_key || !m_value) {
			uwsgi_log("metric's threshold requires a key and a value: %s\n", usl->value);
			exit(1);
		}

		struct uwsgi_metric *um = uwsgi_metric_find_by_name(m_key);
		if (!um) {
			uwsgi_log("unable to find metric %s\n", m_key);
			exit(1);
		}

		struct uwsgi_metric_threshold *umt = uwsgi_calloc(sizeof(struct uwsgi_metric_threshold));
		umt->value = strtoll(m_value, NULL, 10);
		if (m_reset) {
			umt->reset = 1;
			umt->reset_value = strtoll(m_reset, NULL, 10);
		}

		if (m_rate) {
			umt->rate = (int32_t) atoi(m_rate);
		}

		umt->alarm = m_alarm;

		if (m_msg) {
			umt->msg = m_msg;
			umt->msg_len = strlen(m_msg);
		}

		free(m_key);
		free(m_value);
		if (m_rate) free(m_rate);
		if (m_reset) free(m_reset);

		if (um->thresholds) {
			struct uwsgi_metric_threshold *umt_list = um->thresholds;
			while(umt_list) {
				if (!umt_list->next) {
					umt_list->next = umt;
					break;
				}
				umt_list = umt_list->next;
			}
		}
		else {
			um->thresholds = umt;
		}

		uwsgi_log("added threshold for metric %s (value: %lld)\n", um->name, umt->value);
	}

	uwsgi_log("initialized %llu metrics\n", uwsgi.metrics_cnt);

	if (uwsgi.metrics_dir) {
		uwsgi_log("memory allocated for metrics storage: %llu bytes (%llu MB)\n", uwsgi.metrics_cnt * uwsgi.page_size, (uwsgi.metrics_cnt * uwsgi.page_size)/1024/1024);
		if (uwsgi.metrics_dir_restore) {
			metric = uwsgi.metrics;
        		while(metric) {
				if (metric->map) {
					metric->initial_value = strtoll(metric->map, NULL, 10);
				}
				metric = metric->next;
			}
		}
	}
}

struct uwsgi_metric_collector *uwsgi_register_metric_collector(char *name, int64_t (*func)(struct uwsgi_metric *)) {
	struct uwsgi_metric_collector *collector = uwsgi.metric_collectors, *old_collector = NULL;

	while(collector) {
		if (!strcmp(collector->name, name)) goto found;
		old_collector = collector;
		collector = collector->next;
	}

	collector = uwsgi_calloc(sizeof(struct uwsgi_metric_collector));
	collector->name = name;
	if (old_collector) {
		old_collector->next = collector;
	}
	else {
		uwsgi.metric_collectors = collector;
	}
found:
	collector->func = func;

	return collector;
}

static int64_t uwsgi_metric_collector_ptr(struct uwsgi_metric *um) {
	return *um->ptr;
}

static int64_t uwsgi_metric_collector_sum(struct uwsgi_metric *um) {
	int64_t total = 0;
        struct uwsgi_metric_child *umc = um->children;
        while(umc) {
                struct uwsgi_metric *c = umc->um;
                total += *c->value;
                umc = umc->next;
        }

        return total;
}

static int64_t uwsgi_metric_collector_accumulator(struct uwsgi_metric *um) {
        int64_t total = *um->value;
        struct uwsgi_metric_child *umc = um->children;
        while(umc) {
                struct uwsgi_metric *c = umc->um;
                total += *c->value;
                umc = umc->next;
        }

        return total;
}

static int64_t uwsgi_metric_collector_multiplier(struct uwsgi_metric *um) {
        int64_t total = 0;
        struct uwsgi_metric_child *umc = um->children;
        while(umc) {
                struct uwsgi_metric *c = umc->um;
                total += *c->value;
                umc = umc->next;
        }

        return total * um->arg1n;
}

static int64_t uwsgi_metric_collector_adder(struct uwsgi_metric *um) {
        int64_t total = 0;
        struct uwsgi_metric_child *umc = um->children;
        while(umc) {
                struct uwsgi_metric *c = umc->um;
                total += *c->value;
                umc = umc->next;
        }

        return total + um->arg1n;
}

static int64_t uwsgi_metric_collector_avg(struct uwsgi_metric *um) {
        int64_t total = 0;
	int64_t count = 0;
        struct uwsgi_metric_child *umc = um->children;
        while(umc) {
                struct uwsgi_metric *c = umc->um;
                total += *c->value;
		count++;
                umc = umc->next;
        }

	if (count == 0) return 0;

        return total/count;
}

static int64_t uwsgi_metric_collector_func(struct uwsgi_metric *um) {
	if (!um->arg1) return 0;
	int64_t (*func)(struct uwsgi_metric *) = (int64_t (*)(struct uwsgi_metric *)) um->custom;
	if (!func) {
		func = dlsym(RTLD_DEFAULT, um->arg1);
		um->custom = func;
	}
	if (!func) return 0;
        return func(um);
}

void uwsgi_metrics_collectors_setup() {
	uwsgi_register_metric_collector("ptr", uwsgi_metric_collector_ptr);
	uwsgi_register_metric_collector("file", uwsgi_metric_collector_file);
	uwsgi_register_metric_collector("sum", uwsgi_metric_collector_sum);
	uwsgi_register_metric_collector("accumulator", uwsgi_metric_collector_accumulator);
	uwsgi_register_metric_collector("adder", uwsgi_metric_collector_adder);
	uwsgi_register_metric_collector("multiplier", uwsgi_metric_collector_multiplier);
	uwsgi_register_metric_collector("avg", uwsgi_metric_collector_avg);
	uwsgi_register_metric_collector("func", uwsgi_metric_collector_func);
}
