#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

/*

	cookie management functions (mainly used by the internal routing subsystem)

*/

static char *check_cookie(char *cookie, uint16_t cookie_len, char *key, uint16_t keylen, uint16_t *vallen) {
	uint16_t orig_cookie_len = cookie_len-1;
	// first lstrip white spaces
	char *ptr = cookie;
	uint16_t i;
	for(i=0;i<cookie_len;i++) {
		if (isspace((int)ptr[i])) {
			cookie++;
			cookie_len--;
		}
		else {
			break;
		}
	}

	// then rstrip (skipping the first char...)
	for(i=orig_cookie_len;i>0;i--) {
		if (isspace((int)ptr[i])) {
			cookie_len--;
		}
		else {
			break;
		}
	}

	// now search for the first equal sign
	char *equal = memchr(cookie, '=', cookie_len);
	if (!equal) return NULL;
	
	if (uwsgi_strncmp(key, keylen, cookie, equal-cookie)) {
		return NULL;
	}

	cookie_len -= (equal-cookie)+1;
	if (cookie_len == 0) return NULL;

	*vallen = cookie_len;
	return equal+1;
}

char *uwsgi_get_cookie(struct wsgi_request *wsgi_req, char *key, uint16_t keylen, uint16_t *vallen) {
	uint16_t i;

	char *cookie = wsgi_req->cookie;
	uint16_t cookie_len = 0;
	char *ptr = wsgi_req->cookie;
	//start splitting by ;
	for(i=0;i<wsgi_req->cookie_len;i++) {
		if (!cookie) {
			cookie = ptr + i;
		}
		if (ptr[i] == ';') {
			char *value = check_cookie(cookie, cookie_len, key, keylen, vallen);
			if (value) {
				return value;
			}
			cookie_len = 0;
			cookie = NULL;
		}
		else {
			cookie_len++;
		}
	}

	if (cookie_len > 0) {
		char *value = check_cookie(cookie, cookie_len, key, keylen, vallen);
		if (value) {
                	return value;
                }
	}

	return NULL;
}
