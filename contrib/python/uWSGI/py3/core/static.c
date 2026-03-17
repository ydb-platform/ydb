#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

int uwsgi_static_want_gzip(struct wsgi_request *wsgi_req, char *filename, size_t *filename_len, struct stat *st) {
	char can_gzip = 0, can_br = 0;

	// check for filename size
	if (*filename_len + 4 > PATH_MAX) return 0;
	// check for supported encodings
	can_br = uwsgi_contains_n(wsgi_req->encoding, wsgi_req->encoding_len, "br", 2);
	can_gzip = uwsgi_contains_n(wsgi_req->encoding, wsgi_req->encoding_len, "gzip", 4);

	if(!can_br && !can_gzip)
		return 0;

	// check for 'all'
	if (uwsgi.static_gzip_all) goto gzip;

	// check for dirs/prefix
	struct uwsgi_string_list *usl = uwsgi.static_gzip_dir;
	while(usl) {
		if (!uwsgi_starts_with(filename, *filename_len, usl->value, usl->len)) {
			goto gzip;
		}
		usl = usl->next;
	} 

	// check for ext/suffix
	usl = uwsgi.static_gzip_ext;
	while(usl) {
		if (!uwsgi_strncmp(filename + (*filename_len - usl->len), usl->len, usl->value, usl->len)) {
			goto gzip;
		}
		usl = usl->next;
	}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	// check for regexp
	struct uwsgi_regexp_list *url = uwsgi.static_gzip;
	while(url) {
		if (uwsgi_regexp_match(url->pattern, filename, *filename_len) >= 0) {
			goto gzip;
		}
		url = url->next;
	}
#endif
	return 0;

gzip:
	if (can_br) {
		memcpy(filename + *filename_len, ".br\0", 4);
		*filename_len += 3;
		if (!stat(filename, st)) return 2;
		*filename_len -= 3;
		filename[*filename_len] = 0;
	}

	if (can_gzip) {
		memcpy(filename + *filename_len, ".gz\0", 4);
		*filename_len += 3;
		if (!stat(filename, st)) return 1;
		*filename_len -= 3;
		filename[*filename_len] = 0;
	}

	return 0;
}

int uwsgi_http_date(time_t t, char *dst) {

        static char *week[] = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
        static char *months[] = {
                "Jan", "Feb", "Mar", "Apr",
                "May", "Jun", "Jul", "Aug",
                "Sep", "Oct", "Nov", "Dec"
        };

        struct tm *hdtm = gmtime(&t);

        int ret = snprintf(dst, 31, "%s, %02d %s %4d %02d:%02d:%02d GMT",
		week[hdtm->tm_wday], hdtm->tm_mday, months[hdtm->tm_mon], hdtm->tm_year + 1900, hdtm->tm_hour, hdtm->tm_min, hdtm->tm_sec);
	if (ret <= 0 || ret > 31) {
		return 0;
	}
	return ret;
}

// only RFC 1123 is supported
static time_t parse_http_date(char *date, uint16_t len) {

        struct tm hdtm;

        if (len != 29 && date[3] != ',')
                return 0;

        hdtm.tm_mday = uwsgi_str2_num(date + 5);

        switch (date[8]) {
        case 'J':
                if (date[9] == 'a') {
                        hdtm.tm_mon = 0;
                        break;
                }

                if (date[9] == 'u') {
                        if (date[10] == 'n') {
                                hdtm.tm_mon = 5;
                                break;
                        }

                        if (date[10] == 'l') {
                                hdtm.tm_mon = 6;
                                break;
                        }

                        return 0;
                }

                return 0;

        case 'F':
                hdtm.tm_mon = 1;
                break;

        case 'M':
                if (date[9] != 'a')
                        return 0;

                if (date[10] == 'r') {
                        hdtm.tm_mon = 2;
                        break;
                }

                if (date[10] == 'y') {
                        hdtm.tm_mon = 4;
                        break;
                }

                return 0;

        case 'A':
                if (date[10] == 'r') {
                        hdtm.tm_mon = 3;
                        break;
                }
                if (date[10] == 'g') {
                        hdtm.tm_mon = 7;
                        break;
                }
                return 0;

        case 'S':
                hdtm.tm_mon = 8;
                break;

        case 'O':
                hdtm.tm_mon = 9;
                break;

        case 'N':
                hdtm.tm_mon = 10;
		break;

        case 'D':
                hdtm.tm_mon = 11;
                break;
        default:
                return 0;
        }

        hdtm.tm_year = uwsgi_str4_num(date + 12) - 1900;

        hdtm.tm_hour = uwsgi_str2_num(date + 17);
        hdtm.tm_min = uwsgi_str2_num(date + 20);
        hdtm.tm_sec = uwsgi_str2_num(date + 23);

        return timegm(&hdtm);

}



int uwsgi_add_expires_type(struct wsgi_request *wsgi_req, char *mime_type, int mime_type_len, struct stat *st) {

	struct uwsgi_dyn_dict *udd = uwsgi.static_expires_type;
	time_t now = wsgi_req->start_of_request / 1000000;
	// 30+1
	char expires[31];

	while (udd) {
		if (!uwsgi_strncmp(udd->key, udd->keylen, mime_type, mime_type_len)) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(now + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	udd = uwsgi.static_expires_type_mtime;
	while (udd) {
		if (!uwsgi_strncmp(udd->key, udd->keylen, mime_type, mime_type_len)) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(st->st_mtime + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	return 0;
}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
int uwsgi_add_expires(struct wsgi_request *wsgi_req, char *filename, int filename_len, struct stat *st) {

	struct uwsgi_dyn_dict *udd = uwsgi.static_expires;
	time_t now = wsgi_req->start_of_request / 1000000;
	// 30+1
	char expires[31];

	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, filename, filename_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(now + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	udd = uwsgi.static_expires_mtime;
	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, filename, filename_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(st->st_mtime + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	return 0;
}

int uwsgi_add_expires_path_info(struct wsgi_request *wsgi_req, struct stat *st) {

	struct uwsgi_dyn_dict *udd = uwsgi.static_expires_path_info;
	time_t now = wsgi_req->start_of_request / 1000000;
	// 30+1
	char expires[31];

	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, wsgi_req->path_info, wsgi_req->path_info_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(now + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	udd = uwsgi.static_expires_path_info_mtime;
	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, wsgi_req->path_info, wsgi_req->path_info_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(st->st_mtime + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	return 0;
}

int uwsgi_add_expires_uri(struct wsgi_request *wsgi_req, struct stat *st) {

	struct uwsgi_dyn_dict *udd = uwsgi.static_expires_uri;
	time_t now = wsgi_req->start_of_request / 1000000;
	// 30+1
	char expires[31];

	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, wsgi_req->uri, wsgi_req->uri_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(now + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	udd = uwsgi.static_expires_uri_mtime;
	while (udd) {
		if (uwsgi_regexp_match(udd->pattern, wsgi_req->uri, wsgi_req->uri_len) >= 0) {
			int delta = uwsgi_str_num(udd->value, udd->vallen);
			int size = uwsgi_http_date(st->st_mtime + delta, expires);
			if (size > 0) {
				if (uwsgi_response_add_header(wsgi_req, "Expires", 7, expires, size)) return -1;
			}
			return 0;
		}
		udd = udd->next;
	}

	return 0;
}



#endif


char *uwsgi_get_mime_type(char *name, int namelen, size_t *size) {

	int i;
	int count = 0;
	char *ext = NULL;
	for (i = namelen - 1; i >= 0; i--) {
		if (!isalnum((int) name[i])) {
			if (name[i] == '.') {
				ext = name + (namelen - count);
				break;
			}
		}
		count++;
	}

	if (!ext)
		return NULL;


	if (uwsgi.threads > 1)
                pthread_mutex_lock(&uwsgi.lock_static);

	struct uwsgi_dyn_dict *udd = uwsgi.mimetypes;
	while (udd) {
		if (!uwsgi_strncmp(ext, count, udd->key, udd->keylen)) {
			udd->hits++;
			// auto optimization
			if (udd->prev) {
				if (udd->hits > udd->prev->hits) {
					struct uwsgi_dyn_dict *udd_parent = udd->prev->prev, *udd_prev = udd->prev;
					if (udd_parent) {
						udd_parent->next = udd;
					}

					if (udd->next) {
						udd->next->prev = udd_prev;
					}

					udd_prev->prev = udd;
					udd_prev->next = udd->next;

					udd->prev = udd_parent;
					udd->next = udd_prev;

					if (udd->prev == NULL) {
						uwsgi.mimetypes = udd;
					}
				}
			}
			*size = udd->vallen;
			if (uwsgi.threads > 1)
                		pthread_mutex_unlock(&uwsgi.lock_static);
			return udd->value;
		}
		udd = udd->next;
	}

	if (uwsgi.threads > 1)
        	pthread_mutex_unlock(&uwsgi.lock_static);

	return NULL;
}

ssize_t uwsgi_append_static_path(char *dir, size_t dir_len, char *file, size_t file_len) {


	size_t len = dir_len;

	if (len + 1 + file_len > PATH_MAX) {
		return -1;
	}

	if (dir[len - 1] == '/') {
		memcpy(dir + len, file, file_len);
		dir[len + file_len] = 0;
		len += file_len;
	}
	else {
		dir[len] = '/';
		memcpy(dir + len + 1, file, file_len);
		dir[len + 1 + file_len] = 0;
		len += 1 + file_len;
	}

	return len;
}

static int uwsgi_static_stat(struct wsgi_request *wsgi_req, char *filename, size_t *filename_len, struct stat *st, struct uwsgi_string_list **index) {

	int ret = stat(filename, st);
	// if non-existant return -1
	if (ret < 0)
		return -1;

	if (S_ISREG(st->st_mode))
		return 0;

	// check for index
	if (S_ISDIR(st->st_mode)) {
		struct uwsgi_string_list *usl = uwsgi.static_index;
		while (usl) {
			ssize_t new_len = uwsgi_append_static_path(filename, *filename_len, usl->value, usl->len);
			if (new_len >= 0) {
#ifdef UWSGI_DEBUG
				uwsgi_log("checking for %s\n", filename);
#endif
				if (uwsgi_is_file2(filename, st)) {
					*index = usl;
					*filename_len = new_len;
					return 0;
				}
				// reset to original name
				filename[*filename_len] = 0;
			}
			usl = usl->next;
		}
	}

	return -1;
}

void uwsgi_request_fix_range_for_size(struct wsgi_request *wsgi_req, int64_t size) {
	uwsgi_fix_range_for_size(&wsgi_req->range_parsed,
			&wsgi_req->range_from, &wsgi_req->range_to, size);
}

int uwsgi_real_file_serve(struct wsgi_request *wsgi_req, char *real_filename, size_t real_filename_len, struct stat *st) {

	size_t mime_type_size = 0;
	char http_last_modified[49];
	int use_gzip = 0;

	char *mime_type = uwsgi_get_mime_type(real_filename, real_filename_len, &mime_type_size);

	// here we need to choose if we want the gzip variant;
	use_gzip = uwsgi_static_want_gzip(wsgi_req, real_filename, &real_filename_len, st);

	if (wsgi_req->if_modified_since_len) {
		time_t ims = parse_http_date(wsgi_req->if_modified_since, wsgi_req->if_modified_since_len);
		if (st->st_mtime <= ims) {
			if (uwsgi_response_prepare_headers(wsgi_req, "304 Not Modified", 16))
				return -1;
			return uwsgi_response_write_headers_do(wsgi_req);
		}
	}
#ifdef UWSGI_DEBUG
	uwsgi_log("[uwsgi-fileserve] file %s found\n", real_filename);
#endif

	// static file - don't update avg_rt after request
	wsgi_req->do_not_account_avg_rt = 1;

	int64_t fsize = (int64_t)st->st_size;
	uwsgi_request_fix_range_for_size(wsgi_req, fsize);
	switch (wsgi_req->range_parsed) {
	case UWSGI_RANGE_INVALID:
		if (uwsgi_response_prepare_headers(wsgi_req,
					"416 Requested Range Not Satisfiable", 35))
			return -1;
		if (uwsgi_response_add_content_range(wsgi_req, -1, -1, st->st_size)) return -1;
		return 0;
	case UWSGI_RANGE_VALID:
		{
			time_t when = 0;
			if (wsgi_req->if_range != NULL) {
				when = parse_http_date(wsgi_req->if_range, wsgi_req->if_range_len);
				// an ETag will result in when == 0
			}

			if (when < st->st_mtime) {
				fsize = wsgi_req->range_to - wsgi_req->range_from + 1;
				if (uwsgi_response_prepare_headers(wsgi_req, "206 Partial Content", 19)) return -1;
				break;
			}
		}
		/* fallthrough */
	default: /* UWSGI_RANGE_NOT_PARSED */
		if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) return -1;
	}

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
	uwsgi_add_expires(wsgi_req, real_filename, real_filename_len, st);
	uwsgi_add_expires_path_info(wsgi_req, st);
	uwsgi_add_expires_uri(wsgi_req, st);
#endif

	if (use_gzip == 1) {
		if (uwsgi_response_add_header(wsgi_req, "Content-Encoding", 16, "gzip", 4)) return -1;
	} else if (use_gzip == 2) {
	    if (uwsgi_response_add_header(wsgi_req, "Content-Encoding", 16, "br", 2)) return -1;
	}

	// Content-Type (if available)
	if (mime_type_size > 0 && mime_type) {
		if (uwsgi_response_add_content_type(wsgi_req, mime_type, mime_type_size)) return -1;
		// check for content-type related headers
		uwsgi_add_expires_type(wsgi_req, mime_type, mime_type_size, st);
	}

	// increase static requests counter
	uwsgi.workers[uwsgi.mywid].cores[wsgi_req->async_id].static_requests++;

	// nginx
	if (uwsgi.file_serve_mode == 1) {
		if (uwsgi_response_add_header(wsgi_req, "X-Accel-Redirect", 16, real_filename, real_filename_len)) return -1;
		// this is the final header (\r\n added)
		int size = uwsgi_http_date(st->st_mtime, http_last_modified);
		if (uwsgi_response_add_header(wsgi_req, "Last-Modified", 13, http_last_modified, size)) return -1;
	}
	// apache
	else if (uwsgi.file_serve_mode == 2) {
		if (uwsgi_response_add_header(wsgi_req, "X-Sendfile", 10, real_filename, real_filename_len)) return -1;
		// this is the final header (\r\n added)
		int size = uwsgi_http_date(st->st_mtime, http_last_modified);
		if (uwsgi_response_add_header(wsgi_req, "Last-Modified", 13, http_last_modified, size)) return -1;
	}
	// raw
	else {
		// set Content-Length (to fsize NOT st->st_size)
		if (uwsgi_response_add_content_length(wsgi_req, fsize)) return -1;
		if (wsgi_req->range_parsed == UWSGI_RANGE_VALID) {
			// here use the original size !!!
			if (uwsgi_response_add_content_range(wsgi_req, wsgi_req->range_from, wsgi_req->range_to, st->st_size)) return -1;
		}
		int size = uwsgi_http_date(st->st_mtime, http_last_modified);
		if (uwsgi_response_add_header(wsgi_req, "Last-Modified", 13, http_last_modified, size)) return -1;

		// if it is a HEAD request just skip transfer
		if (!uwsgi_strncmp(wsgi_req->method, wsgi_req->method_len, "HEAD", 4)) {
			wsgi_req->status = 200;
			return 0;
		}

		// Ok, the file must be transferred from uWSGI
		// offloading will be automatically managed
		int fd = open(real_filename, O_RDONLY);
		if (fd < 0) return -1;
		// fd will be closed in the following function
		uwsgi_response_sendfile_do(wsgi_req, fd, wsgi_req->range_from, fsize);
	}

	wsgi_req->status = 200;
	return 0;
}


int uwsgi_file_serve(struct wsgi_request *wsgi_req, char *document_root, uint16_t document_root_len, char *path_info, uint16_t path_info_len, int is_a_file) {

	struct stat st;
	char real_filename[PATH_MAX + 1];
	size_t real_filename_len = 0;
	char *filename = NULL;
	size_t filename_len = 0;

	struct uwsgi_string_list *index = NULL;

	if (!is_a_file) {
		filename = uwsgi_concat3n(document_root, document_root_len, "/", 1, path_info, path_info_len);
		filename_len = document_root_len + 1 + path_info_len;
	}
	else {
		filename = uwsgi_concat2n(document_root, document_root_len, "", 0);
		filename_len = document_root_len;
	}

#ifdef UWSGI_DEBUG
	uwsgi_log("[uwsgi-fileserve] checking for %s\n", filename);
#endif

	if (uwsgi.static_cache_paths) {
		uwsgi_rlock(uwsgi.static_cache_paths->lock);
		uint64_t item_len;
		char *item = uwsgi_cache_get2(uwsgi.static_cache_paths, filename, filename_len, &item_len);
		if (item && item_len > 0 && item_len <= PATH_MAX) {
			memcpy(real_filename, item, item_len);
			real_filename_len = item_len;
			real_filename[real_filename_len] = 0;
			uwsgi_rwunlock(uwsgi.static_cache_paths->lock);
			goto found;
		}
		uwsgi_rwunlock(uwsgi.static_cache_paths->lock);
	}

	if (!realpath(filename, real_filename)) {
#ifdef UWSGI_DEBUG
		uwsgi_log("[uwsgi-fileserve] unable to get realpath() of the static file\n");
#endif
		free(filename);
		return -1;
	}
	real_filename_len = strlen(real_filename);

	if (uwsgi.static_cache_paths) {
		uwsgi_wlock(uwsgi.static_cache_paths->lock);
		uwsgi_cache_set2(uwsgi.static_cache_paths, filename, filename_len, real_filename, real_filename_len, uwsgi.use_static_cache_paths, UWSGI_CACHE_FLAG_UPDATE);
		uwsgi_rwunlock(uwsgi.static_cache_paths->lock);
	}

found:
	free(filename);

	if (uwsgi_starts_with(real_filename, real_filename_len, document_root, document_root_len)) {
		struct uwsgi_string_list *safe = uwsgi.static_safe;
		while(safe) {
			if (!uwsgi_starts_with(real_filename, real_filename_len, safe->value, safe->len)) {
				goto safe;
			}		
			safe = safe->next;
		}
		uwsgi_log("[uwsgi-fileserve] security error: %s is not under %.*s or a safe path\n", real_filename, document_root_len, document_root);
		return -1;
	}

safe:

	if (!uwsgi_static_stat(wsgi_req, real_filename, &real_filename_len, &st, &index)) {

		if (index) {
			// if we are here the PATH_INFO need to be changed
			if (uwsgi_req_append_path_info_with_index(wsgi_req, index->value, index->len)) {
                        	return -1;
                        }
		}

		// skip methods other than GET and HEAD
        	if (uwsgi_strncmp(wsgi_req->method, wsgi_req->method_len, "GET", 3) && uwsgi_strncmp(wsgi_req->method, wsgi_req->method_len, "HEAD", 4)) {
			return -1;
        	}

		// check for skippable ext
		struct uwsgi_string_list *sse = uwsgi.static_skip_ext;
		while (sse) {
			if (real_filename_len >= sse->len) {
				if (!uwsgi_strncmp(real_filename + (real_filename_len - sse->len), sse->len, sse->value, sse->len)) {
					return -1;
				}
			}
			sse = sse->next;
		}

#ifdef UWSGI_ROUTING
		// before sending the file, we need to check if some rule applies
		if (!wsgi_req->is_routing && uwsgi_apply_routes_do(uwsgi.routes, wsgi_req, NULL, 0) == UWSGI_ROUTE_BREAK) {
			return 0;
		}
		wsgi_req->routes_applied = 1;
#endif

		return uwsgi_real_file_serve(wsgi_req, real_filename, real_filename_len, &st);
	}

	return -1;

}
