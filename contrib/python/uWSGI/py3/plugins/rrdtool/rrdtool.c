#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

static struct uwsgi_rrdtool {
	void *lib;
	char *lib_name;

	int (*create)(int, char **);
	int (*update)(int, char **);

	int freq;

	char *update_area;
	struct uwsgi_string_list *directory;

	struct uwsgi_stats_pusher *pusher;
} u_rrd;

static struct uwsgi_option rrdtool_options[] = {
	{"rrdtool", required_argument, 0, "store rrd files in the specified directory", uwsgi_opt_add_string_list, &u_rrd.directory, UWSGI_OPT_MASTER|UWSGI_OPT_METRICS},
	{"rrdtool-freq", required_argument, 0, "set collect frequency", uwsgi_opt_set_int, &u_rrd.freq, 0},
	{"rrdtool-lib", required_argument, 0, "set the name of rrd library (default: librrd.so)", uwsgi_opt_set_str, &u_rrd.lib_name, 0},
	{0, 0, 0, 0, 0, 0, 0},

};


static int rrdtool_init() {

	if (!u_rrd.lib_name) {
		u_rrd.lib_name = "librrd.so";
	}

	u_rrd.lib = dlopen(u_rrd.lib_name, RTLD_LAZY);
	if (!u_rrd.lib) return -1;

	u_rrd.create = dlsym(u_rrd.lib, "rrd_create");
	if (!u_rrd.create) {
		dlclose(u_rrd.lib);
		return -1;
	}

	u_rrd.update = dlsym(u_rrd.lib, "rrd_update");
	if (!u_rrd.update) {
		dlclose(u_rrd.lib);
		return -1;
	}

	uwsgi_log_initial("*** RRDtool library available at %p ***\n", u_rrd.lib);

	return 0;
}

/*

	create .rrd files if needed

*/
static void rrdtool_post_init() {

	if (!u_rrd.create) return;

	// do not waste time if no --rrdtool option is defined
	if (!u_rrd.directory) return;

	if (!u_rrd.freq) u_rrd.freq = 300;

	char *argv[7];
	argv[0] = "create";


	// create RRA
	argv[3] = "RRA:AVERAGE:0.5:1:288" ;
	argv[4] = "RRA:AVERAGE:0.5:12:168" ;
	argv[5] = "RRA:AVERAGE:0.5:288:31" ;
	argv[6] = "RRA:AVERAGE:0.5:2016:52";

	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, u_rrd.directory) {
		char *dir = uwsgi_expand_path(usl->value, strlen(usl->value), NULL);
                if (!dir) {
                        uwsgi_error("rrdtool_post_init()/uwsgi_expand_path()");
                        exit(1);
                }
		struct uwsgi_metric *um = uwsgi.metrics;
		// here locking is useless, but maybe in the future we could move this part
		// somewhere else
		int created = 0;
		uwsgi_rlock(uwsgi.metrics_lock);
		while(um) {
			char *filename = uwsgi_concat4(dir, "/", um->name, ".rrd");
			if (!uwsgi_file_exists(filename)) {
				argv[1] = filename;
				if (um->type == UWSGI_METRIC_GAUGE) {
					argv[2] = "DS:metric:GAUGE:600:0:U";
				}
				else {
					argv[2] = "DS:metric:DERIVE:600:0:U";
				}
				if (u_rrd.create(7, argv)) {
					uwsgi_log("unable to create rrd file for metric \"%s\"\n", um->name);
					uwsgi_error("rrd_create()");
					exit(1);
				}
				created++;
			}
			free(filename);
			um = um->next;
		}
		uwsgi_rwunlock(uwsgi.metrics_lock);
	
		uwsgi_log("created %d new rrd files in %s\n", created, dir);

		struct uwsgi_stats_pusher_instance *uspi = uwsgi_stats_pusher_add(u_rrd.pusher, NULL);
        	uspi->freq = u_rrd.freq;
		uspi->data = dir;
        	// no need to generate the json
        	uspi->raw=1;
	}

}

static void rrdtool_push(struct uwsgi_stats_pusher_instance *uspi, time_t now, char *json, size_t json_len) {

	if (!u_rrd.update) return ;

	// standard stats pusher
	if (!uspi->data) {
		if (!uspi->arg) {
			uwsgi_log("invalid rrdtool stats pusher syntax\n");
			exit(1);
		}
		uspi->data = uwsgi_expand_path(uspi->arg, strlen(uspi->arg), NULL);
		if (!uspi->data) {
			uwsgi_error("rrdtool_push()/uwsgi_expand_path()");
                        exit(1);
		}
		if (!u_rrd.freq) u_rrd.freq = 300;
		uspi->freq = u_rrd.freq;
	}

	// 1k will be more than enough
	char buf[1024];
	char *argv[3];
	argv[0] = "update";

	struct uwsgi_metric *um = uwsgi.metrics;
	while(um) {
		uwsgi_rlock(uwsgi.metrics_lock);
		int ret = snprintf(buf, 1024, "N:%lld", (long long) (*um->value));
		uwsgi_rwunlock(uwsgi.metrics_lock);
		if (um->reset_after_push){
			uwsgi_wlock(uwsgi.metrics_lock);
			*um->value = um->initial_value;
			uwsgi_rwunlock(uwsgi.metrics_lock);
		}
		if (ret < 3 || ret >= 1024) {
			uwsgi_log("unable to update rrdtool metric for %s\n", um->name);
			goto next;
		}		
		char *filename = uwsgi_concat4(uspi->data, "/", um->name, ".rrd");
		argv[1] = filename;
                argv[2] = buf;
                if (u_rrd.update(3, argv)) {
                	uwsgi_log_verbose("ERROR: rrd_update(\"%s\", \"%s\")\n", argv[1], argv[2]);
                }
		free(filename);
next:
		um = um->next;
	}

}

static void rrdtool_register() {
	u_rrd.pusher = uwsgi_register_stats_pusher("rrdtool", rrdtool_push);
	u_rrd.pusher->raw = 1;
}

struct uwsgi_plugin rrdtool_plugin = {

	.name = "rrdtool",
	.options = rrdtool_options,
	
	.on_load = rrdtool_register,

	// this is the best phase to create rrd files (if needed)
	.preinit_apps = rrdtool_post_init,
	// here we get pointers to rrdtool functions
	.init = rrdtool_init,
};
