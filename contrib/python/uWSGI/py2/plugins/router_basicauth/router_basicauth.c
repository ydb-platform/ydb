#include <contrib/python/uWSGI/py2/config.h>
#include <uwsgi.h>

#ifdef UWSGI_ROUTING

// TODO: Add more crypt_r supported platfroms here
#if defined(__linux__) && defined(__GLIBC__)
#include <crypt.h>
#elif defined(__CYGWIN__)
#include <crypt.h>
pthread_mutex_t ur_basicauth_crypt_mutex;
#else
pthread_mutex_t ur_basicauth_crypt_mutex;
#endif

extern struct uwsgi_server uwsgi;

static char *htpasswd_check_sha1(char *pwd) {
#ifdef UWSGI_SSL
	char sha1[20];
	uwsgi_sha1(pwd, strlen(pwd), sha1);

	size_t len = 0;
	char *b64 = uwsgi_base64_encode(sha1, 20, &len);
	if (!b64) return NULL;

	// we add a new line for being fgets-friendly
	char *crypted = uwsgi_concat3n("{SHA}", 5, b64, len, "\n", 1);
	free(b64);
	return crypted;

#else
	uwsgi_log("*** WARNING, rebuild uWSGI with SSL support for htpasswd sha1 feature ***\n");
	return NULL;
#endif
}

static uint16_t htpasswd_check(char *filename, char *auth) {

	char line[1024];

	char *colon = strchr(auth, ':');
	if (!colon) return 0;

	FILE *htpasswd = fopen(filename, "r");
	if (!htpasswd) {
		return 0;
	}
	while(fgets(line, 1024, htpasswd)) {
		char *crypted = NULL;
		int need_free = 0;
		char *colon2 = strchr(line, ':');
		if (!colon2) break;	

		char *cpwd = colon2+1;
		size_t clen = strlen(cpwd);

		// now we check which algo to use
		// {SHA} ?
		if (!uwsgi_starts_with(cpwd, clen, "{SHA}", 5)) {
			crypted = htpasswd_check_sha1(colon+1);
			if (crypted) need_free = 1;
			goto check;
		}


		if (clen < 13) break;

		if (clen > 13) cpwd[13] = 0;

#if defined(__linux__) && defined(__GLIBC__)
		struct crypt_data cd;
		memset(&cd, 0, sizeof(struct crypt_data));
    /* work around glibc-2.2.5 bug,
     * has been fixed at some time in glibc-2.3.X */
#if (__GLIBC__ == 2) && \
    (defined(__GLIBC_MINOR__) && __GLIBC_MINOR__ >= 2 && __GLIBC_MINOR__ < 4)
		// we do as nginx here
		cd.current_salt[0] = ~cpwd[0];
#endif
		crypted = crypt_r( colon+1, cpwd, &cd);
#else
		if (uwsgi.threads > 1) pthread_mutex_lock(&ur_basicauth_crypt_mutex);
		crypted = crypt( colon+1, cpwd);
		if (uwsgi.threads > 1) pthread_mutex_unlock(&ur_basicauth_crypt_mutex);
#endif
check:
		if (!crypted) continue;

		if (!strcmp( crypted, cpwd )) {
			if (!uwsgi_strncmp(auth, colon-auth, line, colon2-line)) {
				fclose(htpasswd);
				if (need_free) free(crypted);
				return colon-auth;
			}
		}

		if (need_free) free(crypted);
	}
	
	fclose(htpasswd);

	return 0;
}

static int uwsgi_routing_func_basicauth(struct wsgi_request *wsgi_req, struct uwsgi_route *ur) {

	// skip if already authenticated
	if (wsgi_req->remote_user_len > 0) {
		return UWSGI_ROUTE_NEXT;
	}


	if (wsgi_req->authorization_len > 7 && ur->data2_len > 0) {
		if (strncmp(wsgi_req->authorization, "Basic ", 6))
			goto forbidden;

		size_t auth_len = 0;
		char *auth = uwsgi_base64_decode(wsgi_req->authorization+6, wsgi_req->authorization_len-6, &auth_len);
		if (auth) {
			if (!ur->custom) {
				// check htpasswd-like file
				uint16_t ulen = htpasswd_check(ur->data2, auth);
				if (ulen > 0) {
					wsgi_req->remote_user = uwsgi_req_append(wsgi_req, "REMOTE_USER", 11, auth, ulen); 
					if (!wsgi_req->remote_user) {
						free(auth);
						goto forbidden;
					}
					wsgi_req->remote_user_len = ulen;
				}
				else if (ur->data3_len == 0) {
					free(auth);
					goto forbidden;
				}
			}
			else {
				if (!uwsgi_strncmp(auth, auth_len, ur->data2, ur->data2_len)) {
					wsgi_req->remote_user = uwsgi_req_append(wsgi_req, "REMOTE_USER", 11, auth, ur->custom); 
					if (!wsgi_req->remote_user) {
						free(auth);
						goto forbidden;
					}
					wsgi_req->remote_user_len = ur->custom;
				}
				else if (ur->data3_len == 0) {
					free(auth);
					goto forbidden;
				}
			}
			free(auth);
			return UWSGI_ROUTE_NEXT;
		}
	}

forbidden:
	if (uwsgi_response_prepare_headers(wsgi_req, "401 Authorization Required", 26)) goto end;
	char *realm = uwsgi_concat3n("Basic realm=\"", 13, ur->data, ur->data_len, "\"", 1);
	// no need to check for errors
	uwsgi_response_add_header(wsgi_req, "WWW-Authenticate", 16, realm, 13 + ur->data_len + 1);
	free(realm);
	uwsgi_response_write_body_do(wsgi_req, "Unauthorized", 12);
end:
	return UWSGI_ROUTE_BREAK;
}

#ifndef __linux__
static void router_basicauth_init_lock() {
	pthread_mutex_init(&ur_basicauth_crypt_mutex, NULL);
}
#endif

static int uwsgi_router_basicauth(struct uwsgi_route *ur, char *args) {

	ur->func = uwsgi_routing_func_basicauth;

	char *comma = strchr(args, ',');
	if (!comma) {
		uwsgi_log("invalid route syntax: %s\n", args);
                exit(1);
	}

	*comma = 0;

	char *colon = strchr(comma+1, ':');
	// is an htpasswd-like file ?
	if (!colon) {
		ur->custom = 0;
	}
	else {
		ur->custom = colon-(comma+1);
	}

	ur->data = args;
	ur->data_len = strlen(args);

	ur->data2 = comma+1;
	ur->data2_len = strlen(ur->data2);

	return 0;
}

static int uwsgi_router_basicauth_next(struct uwsgi_route *ur, char *args) {
	ur->data3_len = 1;
	return uwsgi_router_basicauth(ur, args);
}

static void router_basicauth_register(void) {
	uwsgi_register_router("basicauth", uwsgi_router_basicauth);
	uwsgi_register_router("basicauth-next", uwsgi_router_basicauth_next);
}

struct uwsgi_plugin router_basicauth_plugin = {

	.name = "router_basicauth",
	.on_load = router_basicauth_register,
#ifndef __linux__
	.enable_threads = router_basicauth_init_lock,
#endif
};
#else
struct uwsgi_plugin router_basicauth_plugin = {
	.name = "router_basicauth",
};
#endif
