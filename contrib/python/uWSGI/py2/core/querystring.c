#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

/*

	QUERY_STRING management functions (mainly used by the internal routing subsystem)

*/

static char *check_qs(char *qs, uint16_t qs_len, char *key, uint16_t keylen, uint16_t *vallen) {
	// search for the equal sign
	char *equal = memchr(qs, '=', qs_len);
	if (!equal) return NULL;
	
	if (uwsgi_strncmp(key, keylen, qs, equal-qs)) {
		return NULL;
	}

	qs_len -= (equal-qs)+1;
	if (qs_len == 0) return NULL;

	*vallen = qs_len;
	return equal+1;
}

char *uwsgi_get_qs(struct wsgi_request *wsgi_req, char *key, uint16_t keylen, uint16_t *vallen) {
	uint16_t i;

	char *qs = wsgi_req->query_string;
	uint16_t qs_len = 0;
	char *ptr = wsgi_req->query_string;
	//start splitting by ;
	for(i=0;i<wsgi_req->query_string_len;i++) {
		if (!qs) {
			qs = ptr + i;
		}
		if (ptr[i] == '&') {
			char *value = check_qs(qs, qs_len, key, keylen, vallen);
			if (value) {
				return value;
			}
			qs_len = 0;
			qs = NULL;
		}
		else {
			qs_len++;
		}
	}

	if (qs_len > 0) {
		char *value = check_qs(qs, qs_len, key, keylen, vallen);
		if (value) {
                	return value;
                }
	}

	return NULL;
}
