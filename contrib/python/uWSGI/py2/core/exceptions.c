#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	Exceptions management

	generally exceptions are printed in the logs, but if you enable
	an exception manager they will be stored in a (relatively big) uwsgi packet
	with the following structure.

	"vars" -> keyval of request vars
	"backtrace" -> list of backtrace lines. Each line is a list of 5 elements filename,line,function,text,custom
	"unix" -> seconds since the epoch
	"class" -> the exception class
	"msg" -> a text message mapped to the extension
	"repr" -> a text message mapped to the extension in language-specific gergo
	"wid" -> worker id
	"core" -> the core generating the exception
	"pid" -> pid of the worker
	"node" -> hostname

	Other vars can be added, but you cannot be sure they will be used by exceptions handler.

	The exception-uwsgi packet is passed "as is" to the exception handler

	Exceptions hooks:
	
		a request plugin can export that hooks:
	
		struct uwsgi_buffer *backtrace(struct wsgi_request *);
		struct uwsgi_buffer *exception_class(struct wsgi_request *);
		struct uwsgi_buffer *exception_msg(struct wsgi_request *);
		struct uwsgi_buffer *exception_repr(struct wsgi_request *);
		void exception_log(struct wsgi_request *);

		Remember to reset the exception status (if possibile) after each call

	Exceptions catcher:

		This is a special development-mode in which exceptions are printed
		to the HTTP client.

*/

struct uwsgi_buffer *uwsgi_exception_handler_object(struct wsgi_request *wsgi_req) {
	struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);
	if (uwsgi_buffer_append_keyval(ub, "vars", 4, wsgi_req->buffer,wsgi_req->uh->pktsize)) goto error;
	if (uwsgi.p[wsgi_req->uh->modifier1]->backtrace) {
                struct uwsgi_buffer *bt = uwsgi.p[wsgi_req->uh->modifier1]->backtrace(wsgi_req);
		if (bt) {
			if (uwsgi_buffer_append_keyval(ub, "backtrace", 9, bt->buf, bt->pos)) {
				uwsgi_buffer_destroy(bt);
				goto error;
			} 
			uwsgi_buffer_destroy(bt);
		}
	}
	
	if (uwsgi.p[wsgi_req->uh->modifier1]->exception_class) {
		struct uwsgi_buffer *ec = uwsgi.p[wsgi_req->uh->modifier1]->exception_class(wsgi_req);
		if (ec) {
			if (uwsgi_buffer_append_keyval(ub, "class", 5, ec->buf, ec->pos)) {
				uwsgi_buffer_destroy(ec);
				goto error;
			}
			uwsgi_buffer_destroy(ec);
		}
	}

	if (uwsgi.p[wsgi_req->uh->modifier1]->exception_msg) {
                struct uwsgi_buffer *em = uwsgi.p[wsgi_req->uh->modifier1]->exception_msg(wsgi_req);
                if (em) {
                        if (uwsgi_buffer_append_keyval(ub, "msg", 3, em->buf, em->pos)) {
                                uwsgi_buffer_destroy(em);
                                goto error;
                        }
                        uwsgi_buffer_destroy(em);
                }
        }

	if (uwsgi.p[wsgi_req->uh->modifier1]->exception_repr) {
                struct uwsgi_buffer *er = uwsgi.p[wsgi_req->uh->modifier1]->exception_repr(wsgi_req);
                if (er) {
                        if (uwsgi_buffer_append_keyval(ub, "repr", 4, er->buf, er->pos)) {
                                uwsgi_buffer_destroy(er);
                                goto error;
                        }
                        uwsgi_buffer_destroy(er);
                }
        }

	if (uwsgi_buffer_append_keynum(ub, "unix", 4, uwsgi_now())) goto error;
	if (uwsgi_buffer_append_keynum(ub, "wid", 3, uwsgi.mywid)) goto error;
	if (uwsgi_buffer_append_keynum(ub, "pid", 3, uwsgi.mypid)) goto error;
	if (uwsgi_buffer_append_keynum(ub, "core", 4, wsgi_req->async_id)) goto error;
	if (uwsgi_buffer_append_keyval(ub, "node", 4, uwsgi.hostname, uwsgi.hostname_len)) goto error;

	return ub;
	
error:
	uwsgi_buffer_destroy(ub);
	return NULL;
}

static void append_vars_to_ubuf(char *key, uint16_t keylen, char *val, uint16_t vallen, void *data) {
	struct uwsgi_buffer *ub = (struct uwsgi_buffer *) data;

	if (uwsgi_buffer_append(ub, key, keylen)) return;
	if (uwsgi_buffer_append(ub, " = ", 3)) return;
	if (uwsgi_buffer_append(ub, val, vallen)) return;
	if (uwsgi_buffer_append(ub, "\n", 1)) return;
}

static void append_backtrace_to_ubuf(uint16_t pos, char *value, uint16_t len, void *data) {
        struct uwsgi_buffer *ub = (struct uwsgi_buffer *) data;

	uint16_t item = 0;
	if (pos > 0) {
		item = pos % 5;
	}

	switch(item) {
		// filename
		case 0:
			if (uwsgi_buffer_append(ub, "filename: \"", 11)) return;
			if (uwsgi_buffer_append(ub, value, len)) return;
			if (uwsgi_buffer_append(ub, "\" ", 2)) return;
			break;
		// lineno
		case 1:
			if (uwsgi_buffer_append(ub, "line: ", 6)) return;
			if (uwsgi_buffer_append(ub, value, len)) return;
			if (uwsgi_buffer_append(ub, " ", 1)) return;
			break;
		// function
		case 2:
			if (uwsgi_buffer_append(ub, "function: \"", 11)) return;
			if (uwsgi_buffer_append(ub, value, len)) return;
			if (uwsgi_buffer_append(ub, "\" ", 2)) return;
			break;
		// text
		case 3:
			if (len > 0) {
				if (uwsgi_buffer_append(ub, "text/code: \"", 12)) return;
				if (uwsgi_buffer_append(ub, value, len)) return;
				if (uwsgi_buffer_append(ub, "\" ", 2)) return;
			}
			break;
		// custom
		case 4:
			if (len > 0) {
				if (uwsgi_buffer_append(ub, "custom: \"", 9)) return;
                        	if (uwsgi_buffer_append(ub, value, len)) return;
                        	if (uwsgi_buffer_append(ub, "\" ", 2)) return;
			}
			if (uwsgi_buffer_append(ub, "\n", 1)) return;
			break;
		default:
			break;
	}

}


int uwsgi_exceptions_catch(struct wsgi_request *wsgi_req) {

	if (uwsgi_response_prepare_headers(wsgi_req, "500 Internal Server Error", 25)) {
		return -1;
	}

	if (uwsgi_response_add_content_type(wsgi_req, "text/plain", 10)) {
		return -1;
	}

	struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);
	if (uwsgi_buffer_append(ub, "uWSGI exceptions catcher for \"", 30)) goto error;
	if (uwsgi_buffer_append(ub, wsgi_req->method, wsgi_req->method_len)) goto error;
	if (uwsgi_buffer_append(ub, " ", 1)) goto error;
	if (uwsgi_buffer_append(ub, wsgi_req->uri, wsgi_req->uri_len)) goto error;
	if (uwsgi_buffer_append(ub, "\" (request plugin: \"", 20)) goto error;
	if (uwsgi_buffer_append(ub, (char *) uwsgi.p[wsgi_req->uh->modifier1]->name, strlen(uwsgi.p[wsgi_req->uh->modifier1]->name))) goto error;
	if (uwsgi_buffer_append(ub, "\", modifier1: ", 14 )) goto error;
	if (uwsgi_buffer_num64(ub, wsgi_req->uh->modifier1)) goto error;
	if (uwsgi_buffer_append(ub, ")\n\n", 3)) goto error;

	if (uwsgi_buffer_append(ub, "Exception: ", 11)) goto error;
                
        if (uwsgi.p[wsgi_req->uh->modifier1]->exception_repr) {
                struct uwsgi_buffer *ub_exc_repr = uwsgi.p[wsgi_req->uh->modifier1]->exception_repr(wsgi_req);
                if (ub_exc_repr) {
                        if (uwsgi_buffer_append(ub, ub_exc_repr->buf, ub_exc_repr->pos)) {
                                uwsgi_buffer_destroy(ub_exc_repr);
                                goto error;
                        }
                        uwsgi_buffer_destroy(ub_exc_repr);
                }
                else {
                        goto notavail3;
                }
        }
        else {
notavail3:
                if (uwsgi_buffer_append(ub, "-Not available-", 15)) goto error;
        }

        if (uwsgi_buffer_append(ub, "\n\n", 2)) goto error;

	if (uwsgi_buffer_append(ub, "Exception class: ", 17)) goto error;

	if (uwsgi.p[wsgi_req->uh->modifier1]->exception_class) {
		struct uwsgi_buffer *ub_exc_class = uwsgi.p[wsgi_req->uh->modifier1]->exception_class(wsgi_req);
		if (ub_exc_class) {
			if (uwsgi_buffer_append(ub, ub_exc_class->buf, ub_exc_class->pos)) {
				uwsgi_buffer_destroy(ub_exc_class);
				goto error;
			}
			uwsgi_buffer_destroy(ub_exc_class);
		}
		else {
			goto notavail;
		}
	}
	else {
notavail:
		if (uwsgi_buffer_append(ub, "-Not available-", 15)) goto error;
	}

	if (uwsgi_buffer_append(ub, "\n\n", 2)) goto error;

	if (uwsgi_buffer_append(ub, "Exception message: ", 19)) goto error;

        if (uwsgi.p[wsgi_req->uh->modifier1]->exception_msg) {
                struct uwsgi_buffer *ub_exc_msg = uwsgi.p[wsgi_req->uh->modifier1]->exception_msg(wsgi_req);
                if (ub_exc_msg) {
                        if (uwsgi_buffer_append(ub, ub_exc_msg->buf, ub_exc_msg->pos)) {
                                uwsgi_buffer_destroy(ub_exc_msg);
                                goto error;
                        }
                        uwsgi_buffer_destroy(ub_exc_msg);
                }
                else {
                        goto notavail2;
                }
        }
        else {
notavail2:
                if (uwsgi_buffer_append(ub, "-Not available-", 15)) goto error;
        }

	if (uwsgi_buffer_append(ub, "\n\n", 2)) goto error;

	if (uwsgi_buffer_append(ub, "Backtrace:\n", 11)) goto error;

        if (uwsgi.p[wsgi_req->uh->modifier1]->backtrace) {
                struct uwsgi_buffer *ub_exc_bt = uwsgi.p[wsgi_req->uh->modifier1]->backtrace(wsgi_req);
                if (ub_exc_bt) {
			struct uwsgi_buffer *parsed_bt = uwsgi_buffer_new(4096);
			if (uwsgi_hooked_parse_array(ub_exc_bt->buf, ub_exc_bt->pos, append_backtrace_to_ubuf, parsed_bt)) {
				uwsgi_buffer_destroy(ub_exc_bt);
				uwsgi_buffer_destroy(parsed_bt);
                                goto error;
			}
			uwsgi_buffer_destroy(ub_exc_bt);
                        if (uwsgi_buffer_append(ub, parsed_bt->buf, parsed_bt->pos)) {
                                uwsgi_buffer_destroy(parsed_bt);
                                goto error;
                        }
                        uwsgi_buffer_destroy(parsed_bt);
                }
                else {
                        goto notavail4;
                }
        }
        else {
notavail4:
                if (uwsgi_buffer_append(ub, "-Not available-", 15)) goto error;
        }

        if (uwsgi_buffer_append(ub, "\n\n", 2)) goto error;

	if (uwsgi_hooked_parse(wsgi_req->buffer, wsgi_req->uh->pktsize, append_vars_to_ubuf, ub)) {
		goto error;
	}

	if (uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos)) {
		goto error;
	}

	uwsgi_buffer_destroy(ub);
	return 0;

error:
	uwsgi_buffer_destroy(ub);
	return -1;

}

static void uwsgi_exception_run_handlers(struct uwsgi_buffer *ub) {
	struct uwsgi_string_list *usl = uwsgi.exception_handlers_instance;
	struct iovec iov[2];
        iov[1].iov_base = ub->buf;
        iov[1].iov_len = ub->pos;
	while(usl) {
		struct uwsgi_exception_handler_instance *uehi = (struct uwsgi_exception_handler_instance *) usl->custom_ptr;
        	iov[0].iov_base = &uehi;
        	iov[0].iov_len = sizeof(long);
        	// now send the message to the exception handler thread
        	if (writev(uwsgi.exception_handler_thread->pipe[0], iov, 2) != (ssize_t) (ub->pos+sizeof(long))) {
                	uwsgi_error("[uwsgi-exception-handler-error] uwsgi_exception_run_handlers()/writev()");
        	}
		usl = usl->next;
	}
}

void uwsgi_manage_exception(struct wsgi_request *wsgi_req,int catch) {

	int do_exit = uwsgi.reload_on_exception;

	if (!wsgi_req) goto log2;

	if (do_exit) goto check_catch;

	if (uwsgi.exception_handlers_instance) {
		struct uwsgi_buffer *ehi = uwsgi_exception_handler_object(wsgi_req);
		if (ehi) {
			uwsgi_exception_run_handlers(ehi);
			uwsgi_buffer_destroy(ehi);
		}
	}

	uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].exceptions++;
	uwsgi_apps[wsgi_req->app_id].exceptions++;

	if (uwsgi.reload_on_exception_type && uwsgi.p[wsgi_req->uh->modifier1]->exception_class) {
		struct uwsgi_buffer *ub = uwsgi.p[wsgi_req->uh->modifier1]->exception_msg(wsgi_req);
		if (ub) {
			struct uwsgi_string_list *usl = uwsgi.reload_on_exception_type;
			while (usl) {
				if (!uwsgi_strncmp(usl->value, usl->len, ub->buf, ub->len)) {
					do_exit = 1;
					uwsgi_buffer_destroy(ub);
					goto check_catch;
				}
				usl = usl->next;
			}
			uwsgi_buffer_destroy(ub);
		}
	}

	if (uwsgi.reload_on_exception_value && uwsgi.p[wsgi_req->uh->modifier1]->exception_msg) {
                struct uwsgi_buffer *ub = uwsgi.p[wsgi_req->uh->modifier1]->exception_msg(wsgi_req);
		if (ub) {
			struct uwsgi_string_list *usl = uwsgi.reload_on_exception_value;
                        while (usl) {
                                if (!uwsgi_strncmp(usl->value, usl->len, ub->buf, ub->len)) {
                                        do_exit = 1;
                                        uwsgi_buffer_destroy(ub);
                                        goto check_catch;
                                }
                                usl = usl->next;
                        }
			uwsgi_buffer_destroy(ub);
		}
        }

        if (uwsgi.reload_on_exception_repr && uwsgi.p[wsgi_req->uh->modifier1]->exception_repr) {
                struct uwsgi_buffer *ub = uwsgi.p[wsgi_req->uh->modifier1]->exception_msg(wsgi_req);
		if (ub) {
			struct uwsgi_string_list *usl = uwsgi.reload_on_exception_repr;
                        while (usl) {
                                if (!uwsgi_strncmp(usl->value, usl->len, ub->buf, ub->len)) {
                                        do_exit = 1;
                                        uwsgi_buffer_destroy(ub);
                                        goto check_catch;
                                }
                                usl = usl->next;
                        }
			uwsgi_buffer_destroy(ub);
		}
        }

check_catch:
	if (catch && wsgi_req) {
		if (uwsgi_exceptions_catch(wsgi_req)) {
			// for now, just goto, new features could be added
			goto log;		
		}
	}

log:
	if (uwsgi.p[wsgi_req->uh->modifier1]->exception_log) {
		uwsgi.p[wsgi_req->uh->modifier1]->exception_log(wsgi_req);
	}
	
log2:
	if (do_exit) {
		exit(UWSGI_EXCEPTION_CODE);		
	}
	
}

struct uwsgi_exception_handler *uwsgi_register_exception_handler(char *name, int (*func)(struct uwsgi_exception_handler_instance *, char *, size_t)) {
	struct uwsgi_exception_handler *old_ueh = NULL, *ueh = uwsgi.exception_handlers;
	while(ueh) {
		if (!strcmp(name, ueh->name)) {
			return NULL;
		}
		old_ueh = ueh;
		ueh = ueh->next;
	}

	ueh = uwsgi_calloc(sizeof(struct uwsgi_exception_handler));
	ueh->name = name;
	ueh->func = func;

	if (old_ueh) {
		old_ueh->next = ueh;
	}
	else {
		uwsgi.exception_handlers = ueh;
	}

	return ueh;
}

struct uwsgi_exception_handler *uwsgi_exception_handler_by_name(char *name) {
	struct uwsgi_exception_handler *ueh = uwsgi.exception_handlers;
	while(ueh) {
		if (!strcmp(name, ueh->name)) {
			return ueh;
		}
		ueh = ueh->next;
	}
	return NULL;
}

static void uwsgi_exception_handler_thread_loop(struct uwsgi_thread *ut) {
        char *buf = uwsgi_malloc(uwsgi.exception_handler_msg_size + sizeof(long));
        for (;;) {
                int interesting_fd = -1;
                int ret = event_queue_wait(ut->queue, -1, &interesting_fd);
                if (ret > 0) {
                        ssize_t len = read(ut->pipe[1], buf, uwsgi.exception_handler_msg_size + sizeof(long));
                        if (len > (ssize_t)(sizeof(long) + 1)) {
                                size_t msg_size = len - sizeof(long);
                                char *msg = buf + sizeof(long);
                                long ptr = 0;
                                memcpy(&ptr, buf, sizeof(long));
                                struct uwsgi_exception_handler_instance *uehi = (struct uwsgi_exception_handler_instance *) ptr;
                                if (!uehi)
					break;
				if (uehi->handler->func(uehi, msg, msg_size)) {
                        		uwsgi_log("[uwsgi-exception] error running the handler \"%s\" args: \"%s\"\n", uehi->handler->name, uehi->arg ? uehi->arg : "");
                		}
                        }
                }
        }
	free(buf);
}

void uwsgi_exception_setup_handlers() {

	struct uwsgi_string_list *usl = uwsgi.exception_handlers_instance;
	while(usl) {
		// do not free handler !!!
		char *handler = uwsgi_str(usl->value);
		char *colon = strchr(handler, ':');
		if (colon) {
			*colon = 0;
		}
		struct uwsgi_exception_handler *ueh = uwsgi_exception_handler_by_name(handler);
		if (!ueh) {
			uwsgi_log("unable to find exception handler: %s\n", handler);
			exit(1);
		}

		struct uwsgi_exception_handler_instance *uehi = uwsgi_calloc(sizeof(struct uwsgi_exception_handler_instance));
		uehi->handler = ueh;
		if (colon) {
			uehi->arg = colon+1;
		}
		usl->custom_ptr = uehi;
		usl = usl->next;
	}
}

void uwsgi_exceptions_handler_thread_start() {
	if (!uwsgi.exception_handlers_instance) return;
	// start the exception_handler_thread
        uwsgi.exception_handler_thread = uwsgi_thread_new(uwsgi_exception_handler_thread_loop);
        if (!uwsgi.exception_handler_thread) {
                uwsgi_log("unable to spawn exception handler thread\n");
                exit(1);
        }

}
