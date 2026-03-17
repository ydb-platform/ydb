#include <contrib/python/uWSGI/py3/config.h>
#include "../../uwsgi.h"

#include <syslog.h>

extern struct uwsgi_server uwsgi;

struct uwsgi_syslog_facility {
	const char *name;
	int facility;
};

struct uwsgi_syslog_facility usf[] = {

	{ "auth",       LOG_AUTH,       },
#ifdef LOG_AUTHPRIV
        { "authpriv",   LOG_AUTHPRIV,   },
#endif
        { "cron",       LOG_CRON,       },
        { "daemon",     LOG_DAEMON,     },
#ifdef LOG_FTP
        { "ftp",        LOG_FTP,        },
#endif
#ifdef LOG_INSTALL
        { "install",    LOG_INSTALL     },
#endif
        { "kern",       LOG_KERN,       },
        { "lpr",        LOG_LPR,        },
        { "mail",       LOG_MAIL,       },
#ifdef INTERNAL_MARK
        { "mark",       INTERNAL_MARK,  },      /* INTERNAL */
#endif
#ifdef LOG_NETINFO
        { "netinfo",    LOG_NETINFO,    },
#endif
#ifdef LOG_RAS
        { "ras",        LOG_RAS         },
#endif
#ifdef LOG_REMOTEAUTH
        { "remoteauth", LOG_REMOTEAUTH  },
#endif
        { "news",       LOG_NEWS,       },
        { "security",   LOG_AUTH        },      /* DEPRECATED */
        { "syslog",     LOG_SYSLOG,     },
        { "user",       LOG_USER,       },
        { "uucp",       LOG_UUCP,       },
        { "local0",     LOG_LOCAL0,     },
        { "local1",     LOG_LOCAL1,     },
        { "local2",     LOG_LOCAL2,     },
        { "local3",     LOG_LOCAL3,     },
        { "local4",     LOG_LOCAL4,     },
        { "local5",     LOG_LOCAL5,     },
        { "local6",     LOG_LOCAL6,     },
        { "local7",     LOG_LOCAL7,     },
#ifdef LOG_LAUNCHD
        { "launchd",    LOG_LAUNCHD     },
#endif
        { NULL,         -1,             }
};

ssize_t uwsgi_syslog_logger(struct uwsgi_logger *ul, char *message, size_t len) {

	char *syslog_opts;
	int facility = LOG_DAEMON;

	if (!ul->configured) {

        	setlinebuf(stderr);

        	if (ul->arg == NULL) {
                	syslog_opts = "uwsgi";
        	}
		else {
			char *colon = strchr(ul->arg, ',');
			if (colon) {
				struct uwsgi_syslog_facility *fn = usf;
				while(fn->name) {
					if (!strcmp(fn->name, colon+1)) {
						facility = fn->facility;
						break;
					}
					fn++;
				}
				syslog_opts = uwsgi_str(ul->arg);
				syslog_opts[colon-ul->arg] = 0;
			}
			else {
				syslog_opts = ul->arg;
			}
		}

        	openlog(syslog_opts, 0, facility);

		ul->configured = 1;
	}

#ifdef __APPLE__
	syslog(LOG_NOTICE, "%.*s", (int) len, message);
#else
	syslog(LOG_INFO, "%.*s", (int) len, message);
#endif
	return 0;

}

void uwsgi_syslog_register() {
	uwsgi_register_logger("syslog", uwsgi_syslog_logger);
}

struct uwsgi_plugin syslog_plugin = {

        .name = "syslog",
        .on_load = uwsgi_syslog_register,

};

