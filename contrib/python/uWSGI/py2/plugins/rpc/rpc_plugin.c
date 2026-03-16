#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_XML_LIBXML2
#error #include <libxml/parser.h>
#error #include <libxml/tree.h>
#endif

extern struct uwsgi_server uwsgi;

/*

	rpc-HTTP interface.

	modifier2 changes the parser behaviours:

	0 -> return uwsgi header + rpc response
	1 -> return raw rpc response
	2 -> split PATH_INFO to get func name and args and return as HTTP response with content_type as application/binary or  Accept request header (if different from *)
	3 -> set xmlrpc wrapper (requires libxml2)
	4 -> set jsonrpc wrapper (requires libjansson)
	5 -> used in uwsgi response to signal the response is a uwsgi dictionary followed by the body (the dictionary must contains a CONTENT_LENGTH key)

*/

#ifdef UWSGI_XML_LIBXML2
// raise error on non-string args
static int uwsgi_rpc_xmlrpc_string(xmlNode *element, char **argv, uint16_t *argvs, uint8_t *argc) {
	
	xmlNode *node;
	for (node = element->children; node; node = node->next) {
		if (node->type == XML_ELEMENT_NODE) {
			if (strcmp((char *) node->name, "string")) {
				return -1;
			}
			if (!node->children) return -1;
			if (!node->children->content) return -1;
			*argc+=1;
			argv[*argc] = (char *) node->children->content;
			argvs[*argc] = strlen(argv[*argc]);
		}
	}

	return 0;
}
static int uwsgi_rpc_xmlrpc_value(xmlNode *element, char **argv, uint16_t *argvs, uint8_t *argc) {
	xmlNode *node;
        for (node = element->children; node; node = node->next) {
                if (node->type == XML_ELEMENT_NODE) {
                        if (!strcmp((char *) node->name, "value")) {
				if (uwsgi_rpc_xmlrpc_string(node, argv, argvs, argc)) {
					return -1;
				}
			}
		}
	}
	return 0;
}

static int uwsgi_rpc_xmlrpc_args(xmlNode *element, char **argv, uint16_t *argvs, uint8_t *argc) {
	xmlNode *node;
        for (node = element->children; node; node = node->next) {
                if (node->type == XML_ELEMENT_NODE) {
                        if (!strcmp((char *) node->name, "param")) {
				if (uwsgi_rpc_xmlrpc_value(node, argv, argvs, argc)) {
					return -1;
				}
                        }
                }
        }

	return 0;
}

static int uwsgi_rpc_xmlrpc(struct wsgi_request *wsgi_req, xmlDoc *doc, char **argv, uint16_t *argvs, uint8_t *argc, char **response_buf) {
	char *method = NULL;
	xmlNode *element = xmlDocGetRootElement(doc);
        if (!element) return -1;

        if (strcmp((char *) element->name, "methodCall")) return -1;

	*argc = 0;
	xmlNode *node;
        for (node = element->children; node; node = node->next) {
                if (node->type == XML_ELEMENT_NODE) {
                	if (!strcmp((char *) node->name, "methodName") && node->children) {
				method = (char *) node->children->content;
			}
			else if (!strcmp((char *) node->name, "params")) {
				if (uwsgi_rpc_xmlrpc_args(node, argv, argvs, argc)) return -1;	
			}
                }
        }

	if (!method) return -1;
	uint64_t content_len = uwsgi_rpc(method, *argc, argv+1, argvs+1, response_buf);

	if (!*response_buf) return -1;
	// add final NULL byte
	char *tmp_buf = realloc(*response_buf, content_len + 1);
	if (!tmp_buf) return -1;
	*response_buf = tmp_buf;
	*response_buf[content_len] = 0;

	xmlDoc *rdoc = xmlNewDoc(BAD_CAST "1.0");
        xmlNode *m_response = xmlNewNode(NULL, BAD_CAST "methodResponse");
        xmlDocSetRootElement(rdoc, m_response);
	xmlNode *x_params = xmlNewChild(m_response, NULL, BAD_CAST "params", NULL);
	xmlNode *x_param = xmlNewChild(x_params, NULL, BAD_CAST "param", NULL);
	xmlNode *x_value = xmlNewChild(x_param, NULL, BAD_CAST "value", NULL);
        xmlNewTextChild(x_value, NULL, BAD_CAST "string", BAD_CAST response_buf);
	
	xmlChar *xmlbuf;
        int xlen = 0;
        xmlDocDumpFormatMemory(rdoc, &xmlbuf, &xlen, 1);
	if (uwsgi_response_prepare_headers(wsgi_req,"200 OK", 6)) {
		xmlFreeDoc(rdoc);
		xmlFree(xmlbuf);
		return -1;
	}
        uwsgi_response_add_content_length(wsgi_req, xlen);
        uwsgi_response_add_content_type(wsgi_req, "application/xml", 15);
        uwsgi_response_write_body_do(wsgi_req, (char *) xmlbuf, xlen);

	xmlFreeDoc(rdoc);
	xmlFree(xmlbuf);

	return 0;
}
#endif

static int uwsgi_rpc_request(struct wsgi_request *wsgi_req) {

	// this is the list of args
	char *argv[UMAX8];
	// this is the size of each argument
	uint16_t argvs[UMAX8];
	// maximum number of supported arguments
	uint8_t argc = 0xff;
	// response output
	char *response_buf = NULL;
	// response size
	size_t content_len = 0;

	/* Standard RPC request */
        if (!wsgi_req->uh->pktsize) {
                uwsgi_log("Empty RPC request. skip.\n");
                return -1;
        }

	if (wsgi_req->uh->modifier2 == 2) {
		if (uwsgi_parse_vars(wsgi_req)) {
                	uwsgi_log("Invalid RPC request. skip.\n");
                	return -1;
		}

		if (wsgi_req->path_info_len == 0) {
			uwsgi_500(wsgi_req);
			return UWSGI_OK;
		}

		char *args = NULL;
		if (wsgi_req->path_info[0] == '/') {
			args = uwsgi_concat2n(wsgi_req->path_info+1, wsgi_req->path_info_len-1, "", 0);
		}
		else {
			args = uwsgi_concat2n(wsgi_req->path_info, wsgi_req->path_info_len, "", 0);
		}

		argc = 0;
		char *ctx = NULL;
		argv[0] = strtok_r(args, "/", &ctx);
		if (!argv[0]) {
			free(args);
			uwsgi_500(wsgi_req);
			return UWSGI_OK;
		}
		char *p = strtok_r(NULL, "/", &ctx);
		while(p) {
			argc++;
			argv[argc] = p;
			argvs[argc] = strlen(p);
			p = strtok_r(NULL, "/", &ctx);
		}
		
		content_len = uwsgi_rpc(argv[0], argc, argv+1, argvs+1, &response_buf);
		free(args);

		if (!content_len) {
			if (response_buf) free(response_buf);
			uwsgi_404(wsgi_req);
			return UWSGI_OK;
		}
		if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) {if (response_buf) free(response_buf); return -1;}
		if (uwsgi_response_add_content_length(wsgi_req, content_len)) {if (response_buf) free(response_buf); return -1;}
		uint16_t ctype_len = 0;
		char *ctype = uwsgi_get_var(wsgi_req, "HTTP_ACCEPT", 11, &ctype_len);
		if (!ctype || !uwsgi_strncmp(ctype, ctype_len, "*/*" ,3) || !uwsgi_strncmp(ctype, ctype_len, "*", 1)) {
			if (uwsgi_response_add_content_type(wsgi_req, "application/binary", 18)) {if (response_buf) free(response_buf); return -1;}
		}
		else {
			if (uwsgi_response_add_content_type(wsgi_req, ctype, ctype_len)) {if (response_buf) free(response_buf); return -1;}
		}
		goto sendbody;
	}

#ifdef UWSGI_XML_LIBXML2
	if (wsgi_req->uh->modifier2 == 3) {
		if (wsgi_req->post_cl == 0) {
			uwsgi_500(wsgi_req);
			return UWSGI_OK;
		}
		ssize_t body_len = 0;
                char *body = uwsgi_request_body_read(wsgi_req, wsgi_req->post_cl, &body_len);	
		xmlDoc *doc = xmlReadMemory(body, body_len, NULL, NULL, 0);
                if (!doc) {
			uwsgi_500(wsgi_req);
			return UWSGI_OK;
		}
                int ret = uwsgi_rpc_xmlrpc(wsgi_req, doc, argv, argvs, &argc, &response_buf);
                xmlFreeDoc(doc);
		if (ret) {
			uwsgi_500(wsgi_req);
		}
		if (response_buf) free(response_buf);
		return UWSGI_OK;
	}
#endif

	if (uwsgi_parse_array(wsgi_req->buffer, wsgi_req->uh->pktsize, argv, argvs, &argc)) {
                uwsgi_log("Invalid RPC request. skip.\n");
                return -1;
	}

	// call the function (output will be in wsgi_req->buffer)
	content_len = uwsgi_rpc(argv[0], argc-1, argv+1, argvs+1, &response_buf);
	if (!response_buf) return -1;

	// using modifier2 we may want a raw output
	if (wsgi_req->uh->modifier2 == 0) {
		if (content_len > 0xffff) {
			// signal dictionary
			wsgi_req->uh->modifier2 = 5;
			struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
			if (uwsgi_buffer_append_keynum(ub, "CONTENT_LENGTH", 14 , content_len)) {
				uwsgi_buffer_destroy(ub);
				free(response_buf);	
				return -1;	
			}
			// fix uwsgi header
			wsgi_req->uh->pktsize = ub->pos;
			if (uwsgi_response_write_body_do(wsgi_req, (char *) wsgi_req->uh, 4)) {
				uwsgi_buffer_destroy(ub);
				free(response_buf);	
				return -1;
			}
			if (uwsgi_response_write_body_do(wsgi_req, ub->buf, ub->pos)) {
				uwsgi_buffer_destroy(ub);
				free(response_buf);	
                        	return -1;
                	}
			uwsgi_buffer_destroy(ub);
		}
		else {
			wsgi_req->uh->pktsize = content_len;
			if (uwsgi_response_write_body_do(wsgi_req, (char *) wsgi_req->uh, 4)) {
				free(response_buf);	
				return -1;
			}
		}

	}


sendbody:
	// write the response
	uwsgi_response_write_body_do(wsgi_req, response_buf, content_len);
	free(response_buf);	
	return UWSGI_OK;
}

#ifdef UWSGI_ROUTING
static int uwsgi_routing_func_rpc(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	int ret = -1;
	// this is the list of args
        char *argv[UMAX8];
        // this is the size of each argument
        uint16_t argvs[UMAX8];
	// this is a placeholder for tmp uwsgi_buffers
	struct uwsgi_buffer *ubs[UMAX8];

	char **r_argv = (char **) ur->data2;
	uint16_t *r_argvs = (uint16_t *) ur->data3;

	char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

	uint64_t i, last_translated = 0;
	for(i=0;i<ur->custom;i++) {
		ubs[i] = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, r_argv[i], r_argvs[i]);
		if (!ubs[i]) goto end;
		argv[i] = ubs[i]->buf;
		argvs[i] = ubs[i]->pos;
		last_translated = i;
	}

	// ok we now need to check it it is a local call or a remote one
	char *func = uwsgi_str(ur->data);
	char *remote = NULL;
	char *at = strchr(func, '@');
	if (at) {
		*at = 0;
		remote = at+1;
	}
	uint64_t size;
	char *response = uwsgi_do_rpc(remote, func, ur->custom, argv, argvs, &size);
	free(func);
	if (!response) goto end;

	ret = UWSGI_ROUTE_BREAK;

	if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) {free(response) ; goto end;}
        if (uwsgi_response_add_content_length(wsgi_req, size)) {free(response) ; goto end;}
	uwsgi_response_write_body_do(wsgi_req, response, size);
	free(response);

end:
	for(i=0;i<last_translated;i++) {
		uwsgi_buffer_destroy(ubs[i]);
	}
	return ret;
}

static int uwsgi_routing_func_rpc_blob(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
        int ret = -1;
        // this is the list of args
        char *argv[UMAX8];
        // this is the size of each argument
        uint16_t argvs[UMAX8];
        // this is a placeholder for tmp uwsgi_buffers
        struct uwsgi_buffer *ubs[UMAX8];

        char **r_argv = (char **) ur->data2;
        uint16_t *r_argvs = (uint16_t *) ur->data3;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        uint64_t i, last_translated = 0;
        for(i=0;i<ur->custom;i++) {
                ubs[i] = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, r_argv[i], r_argvs[i]);
                if (!ubs[i]) goto end;
                argv[i] = ubs[i]->buf;
                argvs[i] = ubs[i]->pos;
                last_translated = i;
        }

        // ok we now need to check it it is a local call or a remote one
        char *func = uwsgi_str(ur->data);
        char *remote = NULL;
        char *at = strchr(func, '@');
        if (at) {
                *at = 0;
                remote = at+1;
        }
        uint64_t size;
        char *response = uwsgi_do_rpc(remote, func, ur->custom, argv, argvs, &size);
        free(func);
        if (!response) goto end;

        ret = UWSGI_ROUTE_NEXT;

	// optimization
	if (!wsgi_req->headers_sent) {
        	if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) {free(response) ; goto end;}
        	if (uwsgi_response_add_connection_close(wsgi_req)) {free(response) ; goto end;}
	}
        uwsgi_response_write_body_do(wsgi_req, response, size);
        free(response);

end:
        for(i=0;i<last_translated;i++) {
                uwsgi_buffer_destroy(ubs[i]);
        }
        return ret;
}

static int uwsgi_routing_func_rpc_raw(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
	char *response = NULL;
        int ret = -1;
        // this is the list of args
        char *argv[UMAX8];
        // this is the size of each argument
        uint16_t argvs[UMAX8];
        // this is a placeholder for tmp uwsgi_buffers
        struct uwsgi_buffer *ubs[UMAX8];

        char **r_argv = (char **) ur->data2;
        uint16_t *r_argvs = (uint16_t *) ur->data3;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        uint64_t i, last_translated = 0;
        for(i=0;i<ur->custom;i++) {
                ubs[i] = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, r_argv[i], r_argvs[i]);
                if (!ubs[i]) goto end;
                argv[i] = ubs[i]->buf;
                argvs[i] = ubs[i]->pos;
                last_translated = i;
        }

        // ok we now need to check it it is a local call or a remote one
        char *func = uwsgi_str(ur->data);
        char *remote = NULL;
        char *at = strchr(func, '@');
        if (at) {
                *at = 0;
                remote = at+1;
        }
        uint64_t size;
        response = uwsgi_do_rpc(remote, func, ur->custom, argv, argvs, &size);
        free(func);
        if (!response) goto end;

        ret = UWSGI_ROUTE_NEXT;
	if (size == 0) goto end;

	ret = uwsgi_blob_to_response(wsgi_req, response, size);
	if (ret == 0) {
		ret = UWSGI_ROUTE_BREAK;
	}

end:
	free(response);

	for(i=0;i<last_translated;i++) {
		uwsgi_buffer_destroy(ubs[i]);
	}
	return ret;
}

static int uwsgi_routing_func_rpc_var(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
        char *response = NULL;
        int ret = -1;
        // this is the list of args
        char *argv[UMAX8];
        // this is the size of each argument
        uint16_t argvs[UMAX8];
        // this is a placeholder for tmp uwsgi_buffers
        struct uwsgi_buffer *ubs[UMAX8];

        char **r_argv = (char **) ur->data2;
        uint16_t *r_argvs = (uint16_t *) ur->data3;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        uint64_t i, last_translated = 0;
        for(i=0;i<ur->custom;i++) {
                ubs[i] = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, r_argv[i], r_argvs[i]);
                if (!ubs[i]) goto end;
                argv[i] = ubs[i]->buf;
                argvs[i] = ubs[i]->pos;
                last_translated = i;
        }

        // ok we now need to check it it is a local call or a remote one
        char *func = uwsgi_str(ur->data);
        char *remote = NULL;
        char *at = strchr(func, '@');
        if (at) {
                *at = 0;
                remote = at+1;
        }
        uint64_t size;
        response = uwsgi_do_rpc(remote, func, ur->custom, argv, argvs, &size);
        free(func);

        ret = UWSGI_ROUTE_BREAK;

	if (!uwsgi_req_append(wsgi_req, ur->data4, ur->data4_len, response, size)) {
		goto end;
	}
	ret = UWSGI_ROUTE_NEXT;
end:
	if (response)
		free(response);

        for(i=0;i<last_translated;i++) {
                uwsgi_buffer_destroy(ubs[i]);
        }
        return ret;
}



// "next" || "continue" || "break(.*)" || "goon" || "goto .+"
static int uwsgi_routing_func_rpc_ret(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {
        int ret = -1;
        // this is the list of args
        char *argv[UMAX8];
        // this is the size of each argument
        uint16_t argvs[UMAX8];
        // this is a placeholder for tmp uwsgi_buffers
        struct uwsgi_buffer *ubs[UMAX8];

        char **r_argv = (char **) ur->data2;
        uint16_t *r_argvs = (uint16_t *) ur->data3;

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *)  (((char *)(wsgi_req))+ur->subject_len);

        uint64_t i, last_translated = 0;
        for(i=0;i<ur->custom;i++) {
                ubs[i] = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, r_argv[i], r_argvs[i]);
                if (!ubs[i]) goto end;
                argv[i] = ubs[i]->buf;
                argvs[i] = ubs[i]->pos;
                last_translated = i;
        }

        // ok we now need to check it it is a local call or a remote one
        char *func = uwsgi_str(ur->data);
        char *remote = NULL;
        char *at = strchr(func, '@');
        if (at) {
                *at = 0;
                remote = at+1;
        }
        uint64_t size;
        char *response = uwsgi_do_rpc(remote, func, ur->custom, argv, argvs, &size);
        free(func);
        if (!response) goto end;

        ret = UWSGI_ROUTE_CONTINUE;
	if (!uwsgi_strncmp(response, size, "next", 4 )) {
        	ret = UWSGI_ROUTE_NEXT;
	}
	else if (!uwsgi_strncmp(response, size, "continue", 8 )) {
        	ret = UWSGI_ROUTE_CONTINUE;
	}
	else if (!uwsgi_starts_with(response, size, "break", 5 )) {
        	ret = UWSGI_ROUTE_BREAK;
		if (size > 6) {
			if (uwsgi_response_prepare_headers(wsgi_req, response+6, size-6)) goto end0;
                	if (uwsgi_response_add_connection_close(wsgi_req)) goto end0;
                	if (uwsgi_response_add_content_type(wsgi_req, "text/plain", 10)) goto end0;
                	// no need to check for return value
                	uwsgi_response_write_headers_do(wsgi_req);
		}
	}
	else if (!uwsgi_starts_with(response, size, "goto ", 5)) {
		ret = UWSGI_ROUTE_BREAK;
		if (size > 5) {
		 	// find the label
        		struct uwsgi_route *routes = uwsgi.routes;
        		while(routes) {
                		if (!routes->label) goto next;
                		if (!uwsgi_strncmp(routes->label, routes->label_len, response+5, size-5)) {
					ret = UWSGI_ROUTE_NEXT;
                        		wsgi_req->route_goto = routes->pos;
                        		goto found;
                		}
next:
               			routes = routes->next;
        		}
			goto end0;	
found:
        		if (wsgi_req->route_goto <= wsgi_req->route_pc) {
                		wsgi_req->route_goto = 0;
                		uwsgi_log("[uwsgi-route] ERROR \"goto\" instruction can only jump forward (check your label !!!)\n");
				ret = UWSGI_ROUTE_BREAK;
        		}
		}
	}

end0:
        free(response);

end:
        for(i=0;i<last_translated;i++) {
                uwsgi_buffer_destroy(ubs[i]);
        }
        return ret;
}


/*
	ur->data = the func name
        ur->custom = the number of arguments
	ur->data2 = the pointer to the args
	ur->data3 = the pointer to the args sizes
	ur->data4 = func specific
*/
static int uwsgi_router_rpc_base(struct uwsgi_route *ur, char *args) {
	ur->custom = 0;
	ur->data2 = uwsgi_calloc(sizeof(char *) * UMAX8);
	ur->data3 = uwsgi_calloc(sizeof(uint16_t) * UMAX8);
	char *p, *ctx = NULL;
	uwsgi_foreach_token(args, " ", p, ctx) {
		if (!ur->data) {
			ur->data = p;
		}
		else {
			if (ur->custom >= UMAX8) {
				uwsgi_log("unable to register route: maximum number of rpc args reached\n");
				free(ur->data2);
				free(ur->data3);
				return -1;
			}
			char **argv = (char **) ur->data2;
			uint16_t *argvs = (uint16_t *) ur->data3;
			argv[ur->custom] = p;
			argvs[ur->custom] = strlen(p);
			ur->custom++;	
		}
	}

	if (!ur->data) {
		uwsgi_log("unable to register route: you need to specify an rpc function\n");
		free(ur->data2);
		free(ur->data3);
		return -1;
	}
	return 0;
}

static int uwsgi_router_rpc(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_rpc;
	return uwsgi_router_rpc_base(ur, args);
}

static int uwsgi_router_rpc_ret(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_rpc_ret;
	return uwsgi_router_rpc_base(ur, args);
}

static int uwsgi_router_rpc_blob(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_rpc_blob;
        return uwsgi_router_rpc_base(ur, args);
}

static int uwsgi_router_rpc_raw(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_rpc_raw;
        return uwsgi_router_rpc_base(ur, args);
}

static int uwsgi_router_rpc_var(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_rpc_var;
	char *space = strchr(args, ' ');
	if (!space) return -1;
	ur->data4 = args;
	ur->data4_len = space - args;
        return uwsgi_router_rpc_base(ur, space+1);
}

static void router_rpc_register() {
        uwsgi_register_router("call", uwsgi_router_rpc);
        uwsgi_register_router("rpc", uwsgi_router_rpc);
        uwsgi_register_router("rpcret", uwsgi_router_rpc_ret);
        uwsgi_register_router("rpcblob", uwsgi_router_rpc_blob);
        uwsgi_register_router("rpcnext", uwsgi_router_rpc_blob);
        uwsgi_register_router("rpcraw", uwsgi_router_rpc_raw);
        uwsgi_register_router("rpcvar", uwsgi_router_rpc_var);
}
#endif

struct uwsgi_plugin rpc_plugin = {

	.name = "rpc",
	.modifier1 = 173,
	
	.request = uwsgi_rpc_request,
#ifdef UWSGI_ROUTING
	.on_load = router_rpc_register,
#endif
};
