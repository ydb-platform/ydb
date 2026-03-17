#include <contrib/python/uWSGI/py2/config.h>
#include "common.h"

extern struct uwsgi_http uhttp;

#ifdef UWSGI_ZLIB
static char gzheader[10] = { 0x1f, 0x8b, Z_DEFLATED, 0, 0, 0, 0, 0, 0, 3 };
#endif

int http_response_parse(struct http_session *hr, struct uwsgi_buffer *ub, size_t len) {

        size_t i;
        size_t next = 0;

	char *buf = ub->buf;

        int found = 0;
        // protocol
        for(i=0;i<len;i++) {
                if (buf[i] == ' ') {
			if (hr->session.can_keepalive && uwsgi_strncmp("HTTP/1.1", 8, buf, i)) {
				goto end;
			}
                        if (i+1 >= len) return -1;;
                        next = i+1;
                        found = 1;
                        break;
                }
        }

        if (!found) goto end;

        // status
        found = 0;
        for(i=next;i<len;i++) {
                if (buf[i] == '\r' || buf[i] == '\n') {
			// status ready
                        if (i+1 >= len) return -1;
                        next = i + 1;
                        found = 1;
                        break;
                }
        }

        if (!found) goto end;

        char *key = NULL;

        // find first header position
        for(i=next;i<len;i++) {
                if (buf[i] != '\r' && buf[i] != '\n') {
                        key = buf + i;
                        next = i;
                        break;
                }
        }

	if (!key) goto end;

        uint32_t h_len = 0;

	int has_size = 0;
	int has_connection = 0;

        for(i=next;i<len;i++) {
                if (key) {
                        if (buf[i] == '\r' || buf[i] == '\n') {
                                char *colon = memchr(key, ':', h_len);
                                if (!colon) return -1;
                                // security check
                                if (colon+2 >= buf+len) return -1;
#ifdef UWSGI_ZLIB
				if (hr->session.can_keepalive || (uhttp.auto_gzip && hr->can_gzip)) {
#else
				if (hr->session.can_keepalive) {
#endif
					if (!uwsgi_strnicmp(key, colon-key, "Connection", 10)) {
						has_connection = 1;
						if (!uwsgi_strnicmp(colon+2, h_len-((colon-key)+2), "close", 5)) {
							goto end;
						}
					}
					else if (!uwsgi_strnicmp(key, colon-key, "Trailers", 8)) {
						goto end;
					}
					else if (!uwsgi_strnicmp(key, colon-key, "Content-Length", 14)) {
						has_size = 1;
					}
					else if (!uwsgi_strnicmp(key, colon-key, "Transfer-Encoding", 17)) {
						has_size = 1;
					}
				}
#ifdef UWSGI_ZLIB
				if (uhttp.auto_gzip && hr->can_gzip) {
					if (!uwsgi_strnicmp(key, colon-key, "Content-Encoding", 16)) {
						hr->can_gzip = 0;
					}
					else if (!uwsgi_strnicmp(key, colon-key, "uWSGI-Encoding", 14)) {
						if (!uwsgi_strnicmp(colon+2, h_len-((colon-key)+2), "gzip", 4)) {
							hr->has_gzip = 1;
                                                }
					}
				}
#endif
                                key = NULL;
                                h_len = 0;
                        }
                        else {
                                h_len++;
                        }
                }
                else {
                        if (buf[i] != '\r' && buf[i] != '\n') {
                                key = buf+i;
                                h_len = 1;
                        }
                }
        }

	if (!has_size) {
#ifdef UWSGI_ZLIB
		if (hr->has_gzip) {
			hr->force_gzip = 1;
			if (uwsgi_deflate_init(&hr->z, NULL, 0)) {
				hr->force_gzip = 0;
				goto end;
			}
			hr->gzip_crc32 = 0;
			uwsgi_crc32(&hr->gzip_crc32, NULL, 0);
			hr->gzip_size = 0;
			char cr = buf[len-2];
                        char nl = buf[len-1];
                        if (cr == '\r' && nl == '\n') {
                        	if (uwsgi_buffer_insert(ub, len-2, "Transfer-Encoding: chunked\r\n", 28)) return -1;
                        	if (uwsgi_buffer_insert(ub, len-2+28, "Content-Encoding: gzip\r\n", 24)) return -1;
                                size_t remains = ub->pos - (len+28+24);
                                if (remains > 0) {
					size_t zlen = 0;
					char *ptr = ub->buf + (ub->pos - remains);
					char *gzipped = uwsgi_deflate(&hr->z, ptr, remains, &zlen);
					if (!gzipped) return -1;
					uwsgi_crc32(&hr->gzip_crc32, ptr, remains);
					hr->gzip_size += remains;
					ub->pos = len + 28 + 24;
					if (uwsgi_buffer_append_chunked(ub, zlen + 10)) {free(gzipped); return -1;}
					if (uwsgi_buffer_append(ub, gzheader, 10)) {free(gzipped); return -1;}
					if (uwsgi_buffer_append(ub, gzipped, zlen)) {free(gzipped); return -1;}
					free(gzipped);
                                        if (uwsgi_buffer_append(ub, "\r\n", 2)) return -1;
                                }
				else {
					if (uwsgi_buffer_append_chunked(ub, 10)) return -1;
					if (uwsgi_buffer_append(ub, gzheader, 10)) return -1;
					if (uwsgi_buffer_append(ub, "\r\n", 2)) return -1;
					
				}
			}
		} else
#endif
		if (hr->session.can_keepalive) {
			if (uhttp.auto_chunked) {
				char cr = buf[len-2];
				char nl = buf[len-1];
				if (cr == '\r' && nl == '\n') {
					if (uwsgi_buffer_insert(ub, len-2, "Transfer-Encoding: chunked\r\n", 28)) return -1;
					size_t remains = ub->pos - (len+28);
					if (remains > 0) {
						if (uwsgi_buffer_insert_chunked(ub, len + 28, remains)) return -1;
						if (uwsgi_buffer_append(ub, "\r\n", 2)) return -1;
					}
					hr->force_chunked = 1;
					return 0;
				}
			}
			// avoid iOS making mess...
			if (!has_connection) {
				if (uwsgi_buffer_insert(ub, len-2, "Connection: close\r\n", 19)) return -1;
			}
			hr->session.can_keepalive = 0;
		}
	}
        return 0;

end:
	hr->session.can_keepalive = 0;
        return 0;
}

