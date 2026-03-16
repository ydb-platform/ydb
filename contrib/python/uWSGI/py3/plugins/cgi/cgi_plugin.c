#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

#define kill_on_error if (!uc.do_not_kill_on_error) { if (kill(cgi_pid, SIGKILL)) uwsgi_error("kill()");}

struct uwsgi_cgi {
	struct uwsgi_dyn_dict *mountpoint;
	struct uwsgi_dyn_dict *helpers;
	size_t buffer_size;
	int timeout;
	struct uwsgi_string_list *index;
	struct uwsgi_string_list *allowed_ext;
	struct uwsgi_string_list *unset;
	struct uwsgi_string_list *loadlib;
	struct uwsgi_string_list *cgi_safe;
	int optimize;
	int from_docroot;
	int has_mountpoints;
	struct uwsgi_dyn_dict *default_cgi;
	int path_info;
	int do_not_kill_on_error;
	int async_max_attempts;
	int close_stdin_on_eof;
} uc ;

static void uwsgi_opt_add_cgi(char *opt, char *value, void *foobar) {

	char *val = strchr(value, '=');
        if (!val) {
        	uwsgi_dyn_dict_new(&uc.mountpoint, value, strlen(value), NULL, 0);
        }
        else {
        	uwsgi_dyn_dict_new(&uc.mountpoint, value, val-value, val+1, strlen(val+1));
        }

}

static void uwsgi_opt_add_cgi_maphelper(char *opt, char *value, void *foobar) {
	char *val = strchr(value, '=');
        if (!val) {
        	uwsgi_log("invalid CGI helper syntax, must be ext=command\n");
                exit(1);
        }
        uwsgi_dyn_dict_new(&uc.helpers, value, val-value, val+1, strlen(val+1));
}

struct uwsgi_option uwsgi_cgi_options[] = {

        {"cgi", required_argument, 0, "add a cgi mountpoint/directory/script", uwsgi_opt_add_cgi, NULL, 0},

        {"cgi-map-helper", required_argument, 0, "add a cgi map-helper", uwsgi_opt_add_cgi_maphelper, NULL, 0},
        {"cgi-helper", required_argument, 0, "add a cgi map-helper", uwsgi_opt_add_cgi_maphelper, NULL, 0},

        {"cgi-from-docroot", no_argument, 0, "blindly enable cgi in DOCUMENT_ROOT", uwsgi_opt_true, &uc.from_docroot, 0},

        {"cgi-buffer-size", required_argument, 0, "set cgi buffer size", uwsgi_opt_set_64bit, &uc.buffer_size, 0},
        {"cgi-timeout", required_argument, 0, "set cgi script timeout", uwsgi_opt_set_int, &uc.timeout, 0},

        {"cgi-index", required_argument, 0, "add a cgi index file", uwsgi_opt_add_string_list, &uc.index, 0},
        {"cgi-allowed-ext", required_argument, 0, "cgi allowed extension", uwsgi_opt_add_string_list, &uc.allowed_ext, 0},

        {"cgi-unset", required_argument, 0, "unset specified environment variables", uwsgi_opt_add_string_list, &uc.unset, 0},

        {"cgi-loadlib", required_argument, 0, "load a cgi shared library/optimizer", uwsgi_opt_add_string_list, &uc.loadlib, 0},
        {"cgi-optimize", no_argument, 0, "enable cgi realpath() optimizer", uwsgi_opt_true, &uc.optimize, 0},
        {"cgi-optimized", no_argument, 0, "enable cgi realpath() optimizer", uwsgi_opt_true, &uc.optimize, 0},

        {"cgi-path-info", no_argument, 0, "disable PATH_INFO management in cgi scripts", uwsgi_opt_true, &uc.path_info, 0},

        {"cgi-do-not-kill-on-error", no_argument, 0, "do not send SIGKILL to cgi script on errors", uwsgi_opt_true, &uc.do_not_kill_on_error, 0},
        {"cgi-async-max-attempts", no_argument, 0, "max waitpid() attempts in cgi async mode (default 10)", uwsgi_opt_set_int, &uc.async_max_attempts, 0},

        {"cgi-close-stdin-on-eof", no_argument, 0, "close STDIN on input EOF", uwsgi_opt_true, &uc.close_stdin_on_eof, 0},

        {"cgi-safe", required_argument, 0, "skip security checks if the cgi file is under the specified path", uwsgi_opt_add_string_list, &uc.cgi_safe, 0},

        {0, 0, 0, 0, 0, 0, 0},

};

static void uwsgi_cgi_apps() {

	struct uwsgi_dyn_dict *udd = uc.mountpoint;
	struct stat st;

	while(udd) {
		if (udd->vallen) {
			if (uc.optimize) {
				udd->value = realpath(udd->value, NULL);	
				if (!udd->value) {
					uwsgi_log("unable to find CGI path %.*s\n", udd->vallen, udd->value);
					exit(1);
				}
				udd->vallen = strlen(udd->value);
				udd->status = 1;
				if (stat(udd->value, &st)) {
					uwsgi_error("stat()");
					uwsgi_log("something horrible happened during CGI initialization\n");
					exit(1);
				}

				if (!S_ISDIR(st.st_mode)) {
					udd->status = 2;
				}
			}
			uc.has_mountpoints = 1;
			uwsgi_log("initialized CGI mountpoint: %.*s = %.*s\n", udd->keylen, udd->key, udd->vallen, udd->value);
		}
		else {
			if (uc.optimize) {
				udd->key = realpath(udd->key, NULL);
				if (!udd->key) {
                                        uwsgi_log("unable to find CGI path %.*s\n", udd->keylen, udd->key);
                                        exit(1);
                                }
                                udd->keylen = strlen(udd->key);
				udd->status = 1;

				if (stat(udd->key, &st)) {
                                        uwsgi_error("stat()");  
                                        uwsgi_log("something horrible happened during CGI initialization\n");
                                        exit(1);
                                }

                                if (!S_ISDIR(st.st_mode)) {
                                        udd->status = 2;
                                }

			}
			uwsgi_log("initialized CGI path: %.*s\n", udd->keylen, udd->key);
			uc.default_cgi = udd;
		}
		udd = udd->next;
	}

}

static int uwsgi_cgi_init(){

	void (*cgi_sym)(void);

	if (!uc.buffer_size) uc.buffer_size = 65536;
	if (!uc.timeout) uc.timeout = 60;

	struct uwsgi_string_list *ll = uc.loadlib;
	while(ll) {
		char *colon = strchr(ll->value, ':');
		if (!colon) {
			uwsgi_log("invalid cgi-loadlib syntax, must be in the form lib:func\n");
			exit(1);
		}
		*colon = 0;
		void *cgi_lib = dlopen(ll->value, RTLD_NOW | RTLD_GLOBAL);
		if (!cgi_lib) {
			uwsgi_log( "cgi-loadlib: %s\n", dlerror());
			exit(1);
		}

		cgi_sym = dlsym(cgi_lib, colon+1);
		if (!cgi_sym) {
			uwsgi_log("unknown symbol %s in lib %s\n", colon+1, ll->value);
			exit(1);
		}

		cgi_sym();
		uwsgi_log("[cgi-loadlib] loaded symbol %s from %s\n", colon+1, ll->value);

		*colon = ':';
		ll = ll->next;
	}

	return 0;	

}

static char *uwsgi_cgi_get_helper(char *filename) {

	struct uwsgi_dyn_dict *helpers = uc.helpers;
	size_t len = strlen(filename);

	while(helpers) {
		if (len >= (size_t) helpers->keylen) {
			if (!uwsgi_strncmp((filename+len)-helpers->keylen, helpers->keylen, helpers->key, helpers->keylen)) {
				return helpers->value;
			}
		}
		helpers = helpers->next;
	}

	return NULL;
	
}

/*
start reading each line until Status or Location are found
-1 error
0 not found
1 found
*/
static int uwsgi_cgi_check_status(struct wsgi_request *wsgi_req, char *buf, size_t len) {
	char *key = buf, *value = NULL;
	size_t header_size = 0;
	size_t i;

	for(i=0;i<len;i++) {
       		// end of a line
                if (buf[i] == '\n') {
                	// end of headers
                        if (key == NULL) {
                        	// Default status
#ifdef UWSGI_DEBUG
				uwsgi_log("setting default Status header\n");
#endif
				if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) return -1;
				return 1;
			}
                        // invalid header
                        else if (value == NULL) return -1;
                        header_size = (buf+i) - key;
                        // security check
                        if (buf+i > buf) {
				// remove \r
                        	if ((buf[i-1]) == '\r') {
                                	header_size--;
                                }
                        }

			// enough space for Status ?
                        if (header_size >= 11) {
                        	// "Status: NNN"
                                if (!strncasecmp("Status: ", key, 8)) {
#ifdef UWSGI_DEBUG
                                	uwsgi_log("found Status header: %.*s\n", header_size, key);
#endif
                                        if (uwsgi_response_prepare_headers(wsgi_req, key+8, header_size - 8)) return -1;
					return 1;
                                }
                                // Location: X
                                if (!strncasecmp("Location: ", key, 10)) {
#ifdef UWSGI_DEBUG
                                	uwsgi_log("found Location header: %.*s\n", header_size, key);
#endif
                                        if (uwsgi_response_prepare_headers(wsgi_req, "302 Found", 9)) return -1;
					return 1;
                                }
			}

                        key = NULL;
                        value = NULL;
		}
                else if (buf[i] == ':') {
			value = buf+i;
                }
                else if (buf[i] != '\r') {
                	if (key == NULL) key = buf + i;
                }

	}

	// no Status/Location found
	return 0;

}

static int uwsgi_cgi_parse(struct wsgi_request *wsgi_req, int fd, char *buf, size_t blen) {

	size_t i;
	size_t header_size = 0;
	int status_sent = 0;
	size_t remains = blen;
	char *ptr = buf;
	size_t len = 0;

	while(remains > 0) {
		ssize_t rlen = uwsgi_read_true_nb(fd, ptr, remains, uc.timeout);
		if (rlen < 0) {
			if (!errno) return 1;
			return -1;
		}
		// timed out
		if (rlen == 0) return -1;
		remains -= rlen;
		len += rlen;
		ptr += rlen;

		// Search for Status/Location headers
		if (!status_sent) {
			status_sent = uwsgi_cgi_check_status(wsgi_req, buf, len); 
			if (status_sent < 0) return -1;
			// need more data ?
			if (status_sent == 0) continue;
		}

		// send headers
		char *key = buf;
		char *value = NULL;

		for(i=0;i<len;i++) {
			// end of a line
			if (buf[i] == '\n') {
				// end of headers
				if (key == NULL) {
					i++;
					goto send_body;
				}
				// invalid header
				else if (value == NULL) {
					return -1;
				}
				header_size = (buf+i) - key;
				// security check
				if (buf+i > buf) {
					if ((buf[i-1]) == '\r') {
						header_size--;
					}
				}

#ifdef UWSGI_DEBUG
				uwsgi_log("found CGI header: %.*s\n", header_size, key);
#endif

				// Ignore "Status: NNN" header
				if (header_size >= 11) {
					if (!strncasecmp("Status: ", key, 8)) {
						key = NULL;
						value = NULL;
						continue;
					}
				}

				uwsgi_response_add_header(wsgi_req, NULL, 0, key, header_size);
				key = NULL;
				value = NULL;
			}
			else if (buf[i] == ':') {
				value = buf+i;
			}
			else if (buf[i] != '\r') {
				if (key == NULL) {
					key = buf + i;
				}
			}
		}
	}

	return -1;

send_body:

	if (len-i > 0) {
		uwsgi_response_write_body_do(wsgi_req, buf+i, len-i);
	}

	return 0;	
}

static char *uwsgi_cgi_get_docroot(char *path_info, uint16_t path_info_len, int *need_free, int *is_a_file, int *discard_base, char **script_name) {

	struct uwsgi_dyn_dict *udd = uc.mountpoint, *choosen_udd = NULL;
	int best_found = 0;
	struct stat st;
	char *path = NULL;

	if (uc.has_mountpoints) {
		while(udd) {
			if (udd->vallen) {
				if (!uwsgi_starts_with(path_info, path_info_len, udd->key, udd->keylen) && udd->keylen > best_found) {
					best_found = udd->keylen ;
					choosen_udd = udd;
					path = udd->value;
					*script_name = udd->key;
					*discard_base = udd->keylen;
					if (udd->key[udd->keylen-1] == '/') {
						*discard_base = *discard_base-1;
					}
				}
			}
			udd = udd->next;
		}
	}

	if (choosen_udd == NULL) {
		choosen_udd = uc.default_cgi;
		if (!choosen_udd) return NULL;
		path = choosen_udd->key;
	}

	if (choosen_udd->status == 0) {
		char *tmp_udd = uwsgi_malloc(PATH_MAX+1);
		if (!realpath(path, tmp_udd)) {
			free(tmp_udd);
			return NULL;
		}

		if (stat(tmp_udd, &st)) {
			uwsgi_error("stat()");
			free(tmp_udd);
			return NULL;
		}

		if (!S_ISDIR(st.st_mode)) {
			*is_a_file = 1;
		}

		*need_free = 1;
		return tmp_udd;
	}

	if (choosen_udd->status == 2)
		*is_a_file = 1;
	return path;
}

static int uwsgi_cgi_walk(struct wsgi_request *wsgi_req, char *full_path, char *docroot, size_t docroot_len, int discard_base, char **path_info) {

	// and now start walking...
        uint16_t i;
        char *ptr = wsgi_req->path_info+discard_base;
        char *dst = full_path+docroot_len;
        char *part = ptr;
        int part_size = 0;
	struct stat st;

	if (wsgi_req->path_info_len == 0) return 0;

        if (ptr[0] == '/') part_size++;

        for(i=0;i<wsgi_req->path_info_len-discard_base;i++) {
        	if (ptr[i] == '/') {
                	memcpy(dst, part, part_size-1);
                        *(dst+part_size-1) = 0;

                        if (stat(full_path, &st)) {
                        	uwsgi_404(wsgi_req);
                                return -1;
                        }


			// not a directory, stop walking
                        if (!S_ISDIR(st.st_mode)) {
				if (i < (wsgi_req->path_info_len-discard_base)-1) {
                        		*path_info = ptr + i;
				}

				return 0;
                        }


			// check for buffer overflow !!!
                        *(dst+part_size-1) = '/';
                        *(dst+part_size) = 0;

                        dst += part_size ;
                        part_size = 0;
                        part = ptr + i + 1;
         	}

                part_size++;
	}

	if (part < wsgi_req->path_info+wsgi_req->path_info_len) {
		memcpy(dst, part, part_size-1);
		*(dst+part_size-1) = 0;
	}

	return 0;


}

static int uwsgi_cgi_run(struct wsgi_request *, char *, size_t, char *, char *, char *, char *, int, int);

static int uwsgi_cgi_request(struct wsgi_request *wsgi_req) {

	char full_path[PATH_MAX];
	char tmp_path[PATH_MAX];
	struct stat cgi_stat;
	int need_free = 0;
	int is_a_file = 0;
	int discard_base = 0;
	size_t docroot_len = 0;
	size_t full_path_len = 0;
	char *helper = NULL;
	char *path_info = NULL;
	char *script_name = NULL;

	/* Standard CGI request */
	if (!wsgi_req->uh->pktsize) {
		uwsgi_log("Empty CGI request. skip.\n");
		return -1;
	}


	if (uwsgi_parse_vars(wsgi_req)) {
		return -1;
	}

	char *docroot = NULL;

	// check for file availability (and 'runnability')
	if (uc.from_docroot) {
		docroot = wsgi_req->document_root;	
		docroot_len = wsgi_req->document_root_len;	
	}
	else {
		docroot = uwsgi_cgi_get_docroot(wsgi_req->path_info, wsgi_req->path_info_len, &need_free, &is_a_file, &discard_base, &script_name);
		if (docroot)
			docroot_len = strlen(docroot);
	}

	if (docroot == NULL || docroot_len == 0) {
		uwsgi_404(wsgi_req);
		return UWSGI_OK;
	}

	memcpy(full_path, docroot, docroot_len);

	if (!is_a_file) {

		*(full_path+docroot_len) = '/';
		*(full_path+docroot_len+1) = 0;

		if (uwsgi_cgi_walk(wsgi_req, full_path, docroot, docroot_len, discard_base, &path_info)) {
			if (need_free)
				free(docroot);
			return UWSGI_OK;
		}

		if (realpath(full_path, tmp_path) == NULL) {
			if (need_free)
				free(docroot);
			uwsgi_404(wsgi_req);
			return UWSGI_OK;
		}

		full_path_len = strlen(tmp_path);
		// add +1 to copy the null byte
		memcpy(full_path, tmp_path, full_path_len+1);

		struct uwsgi_string_list *safe = uc.cgi_safe;
		while(safe) {
			if (!uwsgi_starts_with(full_path, full_path_len, safe->value, safe->len))
				break;
			safe = safe->next;
		}

		if (!safe) {
			if (uwsgi_starts_with(full_path, full_path_len, docroot, docroot_len)) {
				uwsgi_log("CGI security error: %s is not under %s\n", full_path, docroot);
				if (need_free)
					free(docroot);
				return -1;
			}
		}

	}
	else {
		*(full_path+docroot_len) = 0;
		path_info = wsgi_req->path_info+discard_base;
	}

	if (stat(full_path, &cgi_stat)) {
		uwsgi_404(wsgi_req);
		if (need_free)
			free(docroot);
		return UWSGI_OK;
	}

	if (S_ISDIR(cgi_stat.st_mode)) {

		// add / to directories
		if (wsgi_req->path_info_len == 0 || (wsgi_req->path_info_len > 0 && wsgi_req->path_info[wsgi_req->path_info_len-1] != '/')) {
			uwsgi_redirect_to_slash(wsgi_req);
			if (need_free)
                        	free(docroot);
                	return UWSGI_OK;
		}
		struct uwsgi_string_list *ci = uc.index;
		full_path[full_path_len] = '/';
		full_path_len++;
		int found = 0;
		while(ci) {
			if (full_path_len + ci->len + 1 < PATH_MAX) {
				// add + 1 to ensure null byte
				memcpy(full_path+full_path_len, ci->value, ci->len + 1);
				if (!access(full_path, R_OK)) {
					
					found = 1;
					break;
				}
			}
			ci = ci->next;
		}

		if (!found) {
			uwsgi_404(wsgi_req);
			if (need_free)
				free(docroot);
			return UWSGI_OK;
		}

	}

	full_path_len = strlen(full_path);

	int cgi_allowed = 1;
	struct uwsgi_string_list *allowed = uc.allowed_ext;
	while(allowed) {
		cgi_allowed = 0;
		if (full_path_len >= allowed->len) {
			if (!uwsgi_strncmp(full_path+(full_path_len-allowed->len), allowed->len, allowed->value, allowed->len)) {
				cgi_allowed = 1;
				break;
			}
		}
		allowed = allowed->next;
	}

	if (!cgi_allowed) {
		uwsgi_403(wsgi_req);
		if (need_free)
			free(docroot);
		return UWSGI_OK;
	}

	// get the helper
	if (!is_a_file) {
		helper = uwsgi_cgi_get_helper(full_path);

		if (helper == NULL) {
			if (access(full_path, X_OK)) {
				uwsgi_error("access()");
				uwsgi_403(wsgi_req);
                		if (need_free)
                        		free(docroot);
				return UWSGI_OK;
			}
		}
	}

	int ret = uwsgi_cgi_run(wsgi_req, docroot, docroot_len, full_path, helper, path_info, script_name, is_a_file, discard_base);
	if (need_free) free(docroot);
	return ret;
}

static int uwsgi_cgi_run(struct wsgi_request *wsgi_req, char *docroot, size_t docroot_len, char *full_path, char *helper, char *path_info, char *script_name, int is_a_file, int discard_base) {

	int cgi_pipe[2];
	int post_pipe[2];
	int nargs = 0;
	int waitpid_status;
	int i;
	char **argv;

	char *command = full_path;
	int stdin_closed = 0;

	if (is_a_file) {
                command = docroot;
        }

	if (pipe(cgi_pipe)) {
		uwsgi_error("uwsgi_cgi_run()/pipe()");
		return UWSGI_OK;
	}

	if (pipe(post_pipe)) {
		close(cgi_pipe[0]);
		close(cgi_pipe[1]);
		uwsgi_error("uwsgi_cgi_run()/pipe()");
		return UWSGI_OK;
	}

	pid_t cgi_pid = fork();

	if (cgi_pid < 0) {
		uwsgi_error("uwsgi_cgi_run()/fork()");
		close(cgi_pipe[0]);
		close(cgi_pipe[1]);
		close(post_pipe[0]);
		close(post_pipe[1]);
		return UWSGI_OK;
	}

	if (cgi_pid > 0) {

		close(cgi_pipe[1]);
		close(post_pipe[0]);

		uwsgi_socket_nb(cgi_pipe[0]);
		uwsgi_socket_nb(post_pipe[1]);

		// ok start sending post data...
		size_t remains = wsgi_req->post_cl;
		while(remains > 0) {
                	ssize_t rlen = 0;
                	char *buf = uwsgi_request_body_read(wsgi_req, 8192, &rlen);
                	if (!buf) {
				goto clear2;
                	}
                	if (buf == uwsgi.empty) break;
                	// write data to the node
                	if (uwsgi_write_true_nb(post_pipe[1], buf, rlen, uc.timeout)) {
				goto clear2;
                	}
                	remains -= rlen;
        	}

		if (uc.close_stdin_on_eof) {
			close(post_pipe[1]);
			stdin_closed = 1;
		}

		// wait for data
		char *buf = uwsgi_malloc(uc.buffer_size);

		int completed = uwsgi_cgi_parse(wsgi_req, cgi_pipe[0], buf, uc.buffer_size);
		if (completed < 0) {
			uwsgi_log("invalid CGI response !!!\n");
			kill_on_error
			goto clear;
		}

		while (!completed) {
			ssize_t rlen = uwsgi_read_true_nb(cgi_pipe[0], buf, uc.buffer_size, uc.timeout);
			if (rlen > 0) {
				if (uwsgi_response_write_body_do(wsgi_req, buf, rlen)) {
					kill_on_error
					goto clear;
				}
			}
			else if (rlen == 0) {
				uwsgi_log("CGI timeout !!!\n");
				kill_on_error
				goto clear;
			}
			else {
				if (errno) {
					uwsgi_req_error("error reading CGI response\n");
					kill_on_error
				}
				goto clear;
			}
		}

clear:
		free(buf);
clear2:
		close(cgi_pipe[0]);
		if (!stdin_closed)
			close(post_pipe[1]);

		// now wait for process exit/death
		// in async mode we need a trick...
		if (uwsgi.async > 1) {
			pid_t diedpid = waitpid(cgi_pid, &waitpid_status, WNOHANG);
			if (diedpid < 0) {
                               	uwsgi_error("waitpid()");
                        }
			else if (diedpid == 0) {
				// pass the pid of the cgi to async_plagued (the after request hook will clear the process)
				wsgi_req->async_plagued = (int) cgi_pid;
			}
		}
		else {
			if (waitpid(cgi_pid, &waitpid_status, 0) < 0) {
				uwsgi_error("waitpid()");
			}
		}

		return UWSGI_OK;
	}

	// now map wsgi_req->poll.fd (or async_post) to 0 & cgi_pipe[1] to 1
	dup2(post_pipe[0], 0);
	close(post_pipe[0]);

	dup2(cgi_pipe[1],1);
	close(cgi_pipe[1]);

	// close all the fd > 2
	DIR *dirp = opendir("/proc/self/fd");
	if (dirp == NULL)
		dirp = opendir("/dev/fd");
	if (dirp != NULL) {
		struct dirent *dent;
		while ((dent = readdir(dirp)) != NULL) {
			int fd = atoi(dent->d_name);
			if ((fd > 2) && fd != dirfd(dirp))
				close(fd);
		}
		closedir(dirp);
	} else {
		for(i=3;i<(int)uwsgi.max_fd;i++) {
			close(i);
		}
	}

	// fill cgi env
	for(i=0;i<wsgi_req->var_cnt;i+=2) {
		// no need to free the putenv() memory
		if (putenv(uwsgi_concat3n(wsgi_req->hvec[i].iov_base, wsgi_req->hvec[i].iov_len, "=", 1, wsgi_req->hvec[i+1].iov_base, wsgi_req->hvec[i+1].iov_len))) {
			uwsgi_error("putenv()");
		}
	}


	if (setenv("GATEWAY_INTERFACE", "CGI/1.1", 0)) {
		uwsgi_error("setenv()");
	}

	if (setenv("SERVER_SOFTWARE", uwsgi_concat2("uWSGI/", UWSGI_VERSION), 0)) {
		uwsgi_error("setenv()");
	}

	// for newer php
	if (setenv("REDIRECT_STATUS", "200", 0)) {
		uwsgi_error("setenv()");
	}



	if (path_info) {

		size_t pi_len = wsgi_req->path_info_len - (path_info - wsgi_req->path_info);

		if (setenv("PATH_INFO", uwsgi_concat2n(path_info, pi_len, "", 0), 1)) {
			uwsgi_error("setenv()");
		}

		if (wsgi_req->document_root_len > 0) {
			if (setenv("PATH_TRANSLATED", uwsgi_concat3n(wsgi_req->document_root, wsgi_req->document_root_len, path_info, pi_len, "", 0) , 1)) {
				uwsgi_error("setenv()");
			}
		}
		else {
			if (setenv("PATH_TRANSLATED", uwsgi_concat3n(docroot, docroot_len, path_info, pi_len, "", 0) , 1)) {
				uwsgi_error("setenv()");
			}
		}

	}
	else {
		unsetenv("PATH_INFO");
		unsetenv("PATH_TRANSLATED");
	}

	if (is_a_file) {
		if (setenv("DOCUMENT_ROOT", uwsgi.cwd, 0)) {
			uwsgi_error("setenv()");
		}

		if (setenv("SCRIPT_FILENAME", docroot, 0)) {
			uwsgi_error("setenv()");
		}

		if (script_name && discard_base > 1) {
			if (setenv("SCRIPT_NAME", uwsgi_concat2n(script_name, discard_base, "", 0), 1)) {
				uwsgi_error("setenv()");
			}
		}
	}
	else {
		if (setenv("DOCUMENT_ROOT", docroot, 0)) {
			uwsgi_error("setenv()");
		}

		if (setenv("SCRIPT_FILENAME", full_path, 0)) {
			uwsgi_error("setenv()");
		}

		if (setenv("SCRIPT_NAME", uwsgi_concat2n(wsgi_req->path_info, discard_base, full_path+docroot_len, strlen(full_path+docroot_len)), 1)) {
			uwsgi_error("setenv()");
		}

		char *base = uwsgi_get_last_char(full_path, '/');
		if (base) {
			// a little trick :P
			*base = 0;
			if (chdir(full_path)) {
                                uwsgi_error("chdir()");
                        }
			*base = '/';
		}
		else if (docroot_len > 0) {
			if (chdir(docroot)) {
				uwsgi_error("chdir()");
			}
		}
	}

	struct uwsgi_string_list *drop_env = uc.unset;
	while(drop_env) {
		unsetenv(drop_env->value);
		drop_env = drop_env->next;
	}

	argv = uwsgi_malloc(sizeof(char *) * 3);

	// check if we need to parse indexed QUERY_STRING
	if (wsgi_req->query_string_len > 0) {
		if (!memchr(wsgi_req->query_string, '=', wsgi_req->query_string_len)) {
			nargs = 1;
			for(i=0;i<wsgi_req->query_string_len;i++) {
				if (wsgi_req->query_string[i] == '+')
					nargs++;
			}

			
			// reallocate argv and qs
			argv = uwsgi_malloc(sizeof(char *) * (3+nargs));
			char *qs = uwsgi_concat2n(wsgi_req->query_string, wsgi_req->query_string_len, "", 0);
			// set the start position of args in argv
			i = 1;
			if (helper) i = 2;
			char *p, *ctx = NULL;
			uwsgi_foreach_token(qs, "+", p, ctx) {
				// create a copy for the url_decoded string
				char *arg_copy = uwsgi_str(p);
				uint16_t arg_copy_len = strlen(p);
				http_url_decode(p, &arg_copy_len, arg_copy);
				// and a final copy for shell escaped arg
				argv[i] = uwsgi_malloc( (arg_copy_len * 2) +1);
				escape_shell_arg(arg_copy, arg_copy_len, argv[i]);	
				i++;
			}
			free(qs);
		}
	}

	if (helper) {
		if (!uwsgi_starts_with(helper, strlen(helper), "sym://", 6)) {
			void (*cgi_func)(char *) = dlsym(RTLD_DEFAULT, helper+6);
			if (cgi_func) {
				cgi_func(command);
			}
			else {
				uwsgi_log("unable to find symbol %s\n", helper+6);
			}
			exit(0);	
		}
		argv[0] = helper;
		argv[1] = command;
		argv[nargs+2] = NULL;
	}
	else {
		argv[0] = command;
		argv[nargs+1] = NULL;
	}

	if (execvp(argv[0], argv)) {
		uwsgi_error("uwsgi_cgi_run()/execvp()");
	}

	// never here
	exit(1);
}


static void uwsgi_cgi_after_request(struct wsgi_request *wsgi_req) {
	if (wsgi_req->async_plagued > 0) {
		int waitpid_status;
		pid_t cgi_pid = (pid_t) wsgi_req->async_plagued;
		int max_attempts = uc.async_max_attempts;
                        if (!max_attempts) max_attempts = 10;
                        while(max_attempts) {
                                pid_t diedpid = waitpid(cgi_pid, &waitpid_status, WNOHANG);
                                if (diedpid < 0) {
                                        uwsgi_error("waitpid()");
                                        break;
                                }
                                else if (diedpid == 0) {
                                        int ret = uwsgi.wait_milliseconds_hook(1000);
                                        if (ret < 0) {
                                                kill_on_error
						if (waitpid(cgi_pid, &waitpid_status, 0) < 0) {
                                			uwsgi_error("waitpid()");
                        			}
                                        }
                                }
                                else {
                                        break;
                                }
                                max_attempts--;
                        }
                        if (max_attempts == 0) {
                                kill_on_error
				if (waitpid(cgi_pid, &waitpid_status, 0) < 0) {
                                	uwsgi_error("waitpid()");
                        	}
                        }
	}

	log_request(wsgi_req);
}

#ifdef UWSGI_ROUTING
static int uwsgi_routing_func_cgi(struct wsgi_request *wsgi_req, struct uwsgi_route *ur){

        char **subject = (char **) (((char *)(wsgi_req))+ur->subject);
        uint16_t *subject_len = (uint16_t *) (((char *)(wsgi_req))+ur->subject_len);

        struct uwsgi_buffer *ub_command = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data, ur->data_len);
        if (!ub_command) return UWSGI_ROUTE_BREAK;
	struct uwsgi_buffer *ub_helper = NULL;
	if (ur->data2_len) {
		ub_helper = uwsgi_routing_translate(wsgi_req, ur, *subject, *subject_len, ur->data2, ur->data2_len);
		if (!ub_helper) {
			uwsgi_buffer_destroy(ub_command);
        		return UWSGI_ROUTE_BREAK;
		}
	}
	else {
		if (!uwsgi_is_file(ub_command->buf)) {
			uwsgi_404(wsgi_req);
			uwsgi_buffer_destroy(ub_command);
			return UWSGI_ROUTE_BREAK;
		}

		if (access(ub_command->buf, X_OK)) {
			uwsgi_403(wsgi_req);
			uwsgi_buffer_destroy(ub_command);
			return UWSGI_ROUTE_BREAK;
		}
	}
	// we need a NULL suffix-ed copy of the docroot
	char *docroot = uwsgi_concat2n(wsgi_req->document_root, wsgi_req->document_root_len, "", 0);
        uwsgi_cgi_run(wsgi_req, wsgi_req->document_root, wsgi_req->document_root_len, ub_command->buf, ub_helper ? ub_helper->buf : NULL, NULL, NULL, 0, 0 );
	free(docroot);
        uwsgi_buffer_destroy(ub_command);
	if (ub_helper) uwsgi_buffer_destroy(ub_helper);
        return UWSGI_ROUTE_BREAK;
}

static int uwsgi_router_cgi_helper(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_cgi;
	char *space = strchr(args, ' ');
	if (!space) {
		uwsgi_log("invalid cgihelper syntax, must be \"cgihelper:helper command\"\n");
		return -1;
	}
	*space = 0;
        ur->data = space+1;
        ur->data_len = strlen(space+1);
	ur->data2 = args;
	ur->data2_len = strlen(args);
        return 0;
}
static int uwsgi_router_cgi(struct uwsgi_route *ur, char *args) {
        ur->func = uwsgi_routing_func_cgi;
	ur->data = args;
	ur->data_len = strlen(args);
        return 0;
}
static void uwsgi_cgi_register_router() {
        uwsgi_register_router("cgi", uwsgi_router_cgi);
        uwsgi_register_router("cgihelper", uwsgi_router_cgi_helper);
}
#endif

struct uwsgi_plugin cgi_plugin = {

	.name = "cgi",
	.modifier1 = 9,
	.init = uwsgi_cgi_init,
	.init_apps = uwsgi_cgi_apps,
	.options = uwsgi_cgi_options,
	.request = uwsgi_cgi_request,
	.after_request = uwsgi_cgi_after_request,
#ifdef UWSGI_ROUTING
        .on_load = uwsgi_cgi_register_router,
#endif

};
