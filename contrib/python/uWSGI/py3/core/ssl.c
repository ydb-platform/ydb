#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

extern struct uwsgi_server uwsgi;
/*

ssl additional datas are retrieved via indexes.

You can create an index with SSL_CTX_get_ex_new_index and
set data in it with SSL_CTX_set_ex_data

*/

void uwsgi_ssl_init(void) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        OPENSSL_config(NULL);
#endif
        SSL_library_init();
        SSL_load_error_strings();
        OpenSSL_add_all_algorithms();
        uwsgi.ssl_initialized = 1;
}

#if OPENSSL_VERSION_NUMBER < 0x10100000L
void uwsgi_ssl_info_cb(const SSL *ssl, int where, int ret) {
        if (where & SSL_CB_HANDSHAKE_DONE) {
                if (ssl->s3) {
                        ssl->s3->flags |= SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS;
                }
        }
}
#endif

int uwsgi_ssl_verify_callback(int ok, X509_STORE_CTX * x509_store) {
        if (!ok && uwsgi.ssl_verbose) {
                char buf[256];
                X509 *err_cert;
                int depth;
                int err;
                depth = X509_STORE_CTX_get_error_depth(x509_store);
                err_cert = X509_STORE_CTX_get_current_cert(x509_store);
                X509_NAME_oneline(X509_get_subject_name(err_cert), buf, 256);
                err = X509_STORE_CTX_get_error(x509_store);
                uwsgi_log("[uwsgi-ssl] client certificate verify error: num=%d:%s:depth=%d:%s\n", err, X509_verify_cert_error_string(err), depth, buf);
        }
        return ok;
}

#ifdef UWSGI_SSL_SESSION_CACHE
int uwsgi_ssl_session_new_cb(SSL *ssl, SSL_SESSION *sess) {
        char session_blob[4096];
        int len = i2d_SSL_SESSION(sess, NULL);
        if (len > 4096) {
                if (uwsgi.ssl_verbose) {
                        uwsgi_log("[uwsgi-ssl] unable to store session of size %d\n", len);
                }
                return 0;
        }

        unsigned char *p = (unsigned char *) session_blob;
        i2d_SSL_SESSION(sess, &p);

        // ok let's write the value to the cache
        uwsgi_wlock(uwsgi.ssl_sessions_cache->lock);
        if (uwsgi_cache_set2(uwsgi.ssl_sessions_cache, (char *) sess->session_id, sess->session_id_length, session_blob, len, uwsgi.ssl_sessions_timeout, 0)) {
                if (uwsgi.ssl_verbose) {
                        uwsgi_log("[uwsgi-ssl] unable to store session of size %d in the cache\n", len);
                }
        }
        uwsgi_rwunlock(uwsgi.ssl_sessions_cache->lock);
        return 0;
}

SSL_SESSION *uwsgi_ssl_session_get_cb(SSL *ssl, unsigned char *key, int keylen, int *copy) {

        uint64_t valsize = 0;

        *copy = 0;
        uwsgi_rlock(uwsgi.ssl_sessions_cache->lock);
        char *value = uwsgi_cache_get2(uwsgi.ssl_sessions_cache, (char *)key, keylen, &valsize);
        if (!value) {
                uwsgi_rwunlock(uwsgi.ssl_sessions_cache->lock);
                if (uwsgi.ssl_verbose) {
                        uwsgi_log("[uwsgi-ssl] cache miss\n");
                }
                return NULL;
        }
#if (OPENSSL_VERSION_NUMBER >= 0x0090800fL)
        SSL_SESSION *sess = d2i_SSL_SESSION(NULL, (const unsigned char **)&value, valsize);
#else
        SSL_SESSION *sess = d2i_SSL_SESSION(NULL, (unsigned char **)&value, valsize);
#endif
        uwsgi_rwunlock(uwsgi.ssl_sessions_cache->lock);
        return sess;
}

void uwsgi_ssl_session_remove_cb(SSL_CTX *ctx, SSL_SESSION *sess) {
        uwsgi_wlock(uwsgi.ssl_sessions_cache->lock);
        if (uwsgi_cache_del2(uwsgi.ssl_sessions_cache, (char *) sess->session_id, sess->session_id_length, 0, 0)) {
                if (uwsgi.ssl_verbose) {
                        uwsgi_log("[uwsgi-ssl] error removing cache item\n");
                }
        }
        uwsgi_rwunlock(uwsgi.ssl_sessions_cache->lock);
}
#endif

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
static int uwsgi_sni_cb(SSL *ssl, int *ad, void *arg) {
        const char *servername = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
        if (!servername) return SSL_TLSEXT_ERR_NOACK;
        size_t servername_len = strlen(servername);
	// reduce DOS attempts
	int count = 5;

	while(count > 0) {
        	struct uwsgi_string_list *usl = uwsgi.sni;
        	while(usl) {
                	if (!uwsgi_strncmp(usl->value, usl->len, (char *)servername, servername_len)) {
                        	SSL_set_SSL_CTX(ssl, (SSL_CTX *) usl->custom_ptr);
				// the following steps are taken from nginx
				SSL_set_verify(ssl, SSL_CTX_get_verify_mode((SSL_CTX *)usl->custom_ptr),
				SSL_CTX_get_verify_callback((SSL_CTX *)usl->custom_ptr));
				SSL_set_verify_depth(ssl, SSL_CTX_get_verify_depth((SSL_CTX *)usl->custom_ptr));	
#ifdef SSL_CTRL_CLEAR_OPTIONS
				SSL_clear_options(ssl, SSL_get_options(ssl) & ~SSL_CTX_get_options((SSL_CTX *)usl->custom_ptr));
#endif
				SSL_set_options(ssl, SSL_CTX_get_options((SSL_CTX *)usl->custom_ptr));
                        	return SSL_TLSEXT_ERR_OK;
                	}
                	usl = usl->next;
        	}
		if (!uwsgi.subscription_dotsplit) break;
		char *next = memchr(servername+1, '.', servername_len-1);
		if (next) {
			servername_len -= next - servername;
			servername = next;
			count--;
			continue;
		}
		break;
	}

	if (uwsgi.subscription_dotsplit) goto end;

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
        struct uwsgi_regexp_list *url = uwsgi.sni_regexp;
        while(url) {
                if (uwsgi_regexp_match(url->pattern, (char *)servername, servername_len) >= 0) {
                        SSL_set_SSL_CTX(ssl, url->custom_ptr);
                        return SSL_TLSEXT_ERR_OK;
                }
                url = url->next;
        }
#endif

	if (uwsgi.sni_dir) {
		size_t sni_dir_len = strlen(uwsgi.sni_dir);
		char *sni_dir_cert = uwsgi_concat4n(uwsgi.sni_dir, sni_dir_len, "/", 1, (char *) servername, servername_len, ".crt", 4);
		char *sni_dir_key = uwsgi_concat4n(uwsgi.sni_dir, sni_dir_len, "/", 1, (char *) servername, servername_len, ".key", 4);
		char *sni_dir_client_ca = uwsgi_concat4n(uwsgi.sni_dir, sni_dir_len, "/", 1, (char *) servername, servername_len, ".ca", 3);
		if (uwsgi_file_exists(sni_dir_cert) && uwsgi_file_exists(sni_dir_key)) {
			char *client_ca = NULL;
			if (uwsgi_file_exists(sni_dir_client_ca)) {
				client_ca = sni_dir_client_ca;
			}
			struct uwsgi_string_list *usl = uwsgi_ssl_add_sni_item(uwsgi_str((char *)servername), sni_dir_cert, sni_dir_key, uwsgi.sni_dir_ciphers, client_ca);
			if (!usl) goto done;
			free(sni_dir_cert);
			free(sni_dir_key);
			free(sni_dir_client_ca);
			SSL_set_SSL_CTX(ssl, usl->custom_ptr);
			return SSL_TLSEXT_ERR_OK;
		}
done:
		free(sni_dir_cert);
		free(sni_dir_key);
		free(sni_dir_client_ca);
	}
end:
        return SSL_TLSEXT_ERR_NOACK;
}
#endif

char *uwsgi_write_pem_to_file(char *name, char *buf, size_t len, char *ext) {
	if (!uwsgi.ssl_tmp_dir) return NULL;
	char *filename = uwsgi_concat4(uwsgi.ssl_tmp_dir, "/", name, ext);
	int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR);
	if (fd < 0) {
		uwsgi_error_open(filename);
		free(filename);
		return NULL;
	}

	if (write(fd, buf, len) != (ssize_t) len) {
		uwsgi_log("unable to write pem data in file %s\n", filename);
		uwsgi_error("uwsgi_write_pem_to_file()/write()");
		free(filename);
		close(fd);
                return NULL;
	}

	close(fd);
	return filename;
}

SSL_CTX *uwsgi_ssl_new_server_context(char *name, char *crt, char *key, char *ciphers, char *client_ca) {

	int crt_need_free = 0;
	int key_need_free = 0;
	int client_ca_need_free = 0;

        SSL_CTX *ctx = SSL_CTX_new(SSLv23_server_method());
        if (!ctx) {
                uwsgi_log("[uwsgi-ssl] unable to initialize context \"%s\"\n", name);
                return NULL;
        }

        // this part is taken from nginx and stud, removing unneeded functionality
        // stud (for me) has made the best choice on choosing DH approach

        long ssloptions = SSL_OP_NO_SSLv2 | SSL_OP_ALL | SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION;
// disable compression (if possibile)
#ifdef SSL_OP_NO_COMPRESSION
        ssloptions |= SSL_OP_NO_COMPRESSION;
#endif

	if (!uwsgi.sslv3) {
		ssloptions |= SSL_OP_NO_SSLv3;
	}

	if (!uwsgi.tlsv1) {
		ssloptions |= SSL_OP_NO_TLSv1;
	}

// release/reuse buffers as soon as possibile
#ifdef SSL_MODE_RELEASE_BUFFERS
        SSL_CTX_set_mode(ctx, SSL_MODE_RELEASE_BUFFERS);
#endif
	
	/*
		we need to support both file-based certs and stream based
		as dealing with openssl memory bio for keys is really overkill,
		we store them in a tmp directory
	*/

	if (uwsgi.ssl_tmp_dir && !uwsgi_starts_with(crt, strlen(crt), "-----BEGIN ", 11)) {
		crt = uwsgi_write_pem_to_file(name, crt, strlen(crt), ".crt");
		if (!crt) {
			SSL_CTX_free(ctx);
                	return NULL;
		}	
		crt_need_free = 1;	
	}

        if (SSL_CTX_use_certificate_chain_file(ctx, crt) <= 0) {
               	uwsgi_log("[uwsgi-ssl] unable to assign certificate %s for context \"%s\"\n", crt, name);
               	SSL_CTX_free(ctx);
		if (crt_need_free) free(crt);
               	return NULL;
        }

// this part is based from stud
        BIO *bio = BIO_new_file(crt, "r");
        if (bio) {
                DH *dh = PEM_read_bio_DHparams(bio, NULL, NULL, NULL);
                BIO_free(bio);
                if (dh) {
                        SSL_CTX_set_options(ctx, SSL_OP_SINGLE_DH_USE);
                        SSL_CTX_set_tmp_dh(ctx, dh);
                        DH_free(dh);
                }
        }
#if OPENSSL_VERSION_NUMBER >= 0x0090800fL
#ifndef OPENSSL_NO_ECDH
#ifdef NID_X9_62_prime256v1
        EC_KEY *ecdh = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
        if (ecdh) {
                SSL_CTX_set_options(ctx, SSL_OP_SINGLE_ECDH_USE);
                SSL_CTX_set_tmp_ecdh(ctx, ecdh);
                EC_KEY_free(ecdh);
        }
#endif
#endif
#endif

	if (crt_need_free) free(crt);

	if (uwsgi.ssl_tmp_dir && !uwsgi_starts_with(key, strlen(key), "-----BEGIN ", 11)) {
                key = uwsgi_write_pem_to_file(name, key, strlen(key), ".key");
                if (!key) {
                        SSL_CTX_free(ctx);
                        return NULL;
                }
                key_need_free = 1;
        }

        if (SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) <= 0) {
                uwsgi_log("[uwsgi-ssl] unable to assign key %s for context \"%s\"\n", key, name);
                SSL_CTX_free(ctx);
		if (key_need_free) free(key);
                return NULL;
        }

	if (key_need_free) free(key);

	// if ciphers are specified, prefer server ciphers
        if (ciphers && strlen(ciphers) > 0) {
                if (SSL_CTX_set_cipher_list(ctx, ciphers) == 0) {
                        uwsgi_log("[uwsgi-ssl] unable to set requested ciphers (%s) for context \"%s\"\n", ciphers, name);
                        SSL_CTX_free(ctx);
                        return NULL;
                }

                ssloptions |= SSL_OP_CIPHER_SERVER_PREFERENCE;
        }

        // set session context (if possibile), this is required for client certificate authentication
        if (name) {
                SSL_CTX_set_session_id_context(ctx, (unsigned char *) name, strlen(name));
        }

        if (client_ca) {
                if (client_ca[0] == '!') {
                        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, uwsgi_ssl_verify_callback);
                        client_ca++;
                }
                else {
                        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, uwsgi_ssl_verify_callback);
                }
                SSL_CTX_set_verify_depth(ctx, uwsgi.ssl_verify_depth);

		if (uwsgi.ssl_tmp_dir && !uwsgi_starts_with(client_ca, strlen(client_ca), "-----BEGIN ", 11)) {
			if (!name) {
                        	SSL_CTX_free(ctx);
                        	return NULL;
			}
                	client_ca = uwsgi_write_pem_to_file(name, client_ca, strlen(client_ca), ".ca");
                	if (!client_ca) {
                        	SSL_CTX_free(ctx);
                        	return NULL;
                	}
                	client_ca_need_free = 1;
        	}

                if (SSL_CTX_load_verify_locations(ctx, client_ca, NULL) == 0) {
                        uwsgi_log("[uwsgi-ssl] unable to set ssl verify locations (%s) for context \"%s\"\n", client_ca, name);
                        SSL_CTX_free(ctx);
			if (client_ca_need_free) free(client_ca);
                        return NULL;
                }
                STACK_OF(X509_NAME) * list = SSL_load_client_CA_file(client_ca);
                if (!list) {
                        uwsgi_log("unable to load client CA certificate (%s) for context \"%s\"\n", client_ca, name);
                        SSL_CTX_free(ctx);
			if (client_ca_need_free) free(client_ca);
                        return NULL;
                }

                SSL_CTX_set_client_CA_list(ctx, list);

		if (client_ca_need_free) free(client_ca);
        }


#if OPENSSL_VERSION_NUMBER < 0x10100000L
        SSL_CTX_set_info_callback(ctx, uwsgi_ssl_info_cb);
#endif
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
        SSL_CTX_set_tlsext_servername_callback(ctx, uwsgi_sni_cb);
#endif

        // disable session caching by default
        SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);

#ifdef UWSGI_SSL_SESSION_CACHE
	if (uwsgi.ssl_sessions_use_cache) {

		// we need to early initialize locking and caching
		uwsgi_setup_locking();
		uwsgi_cache_create_all();

		uwsgi.ssl_sessions_cache = uwsgi_cache_by_name(uwsgi.ssl_sessions_use_cache);
		if (!uwsgi.ssl_sessions_cache) {
			// check for default cache
			if (!strcmp(uwsgi.ssl_sessions_use_cache, "true") && uwsgi.caches) {
				uwsgi.ssl_sessions_cache = uwsgi.caches;
			}
			else {
				uwsgi_log("unable to find cache \"%s\"\n", uwsgi.ssl_sessions_use_cache ? uwsgi.ssl_sessions_use_cache : "default");
				exit(1);
			}
		}

                if (!uwsgi.ssl_sessions_cache->max_items) {
                        uwsgi_log("you have to enable uWSGI cache to use it as SSL session store !!!\n");
                        exit(1);
                }

                if (uwsgi.ssl_sessions_cache->blocksize < 4096) {
                        uwsgi_log("cache blocksize for SSL session store must be at least 4096 bytes\n");
                        exit(1);
                }

                SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_SERVER|
                        SSL_SESS_CACHE_NO_INTERNAL|
                        SSL_SESS_CACHE_NO_AUTO_CLEAR);

#ifdef SSL_OP_NO_TICKET
                ssloptions |= SSL_OP_NO_TICKET;
#endif

                // just for fun
                SSL_CTX_sess_set_cache_size(ctx, 0);

                // set the callback for ssl sessions
                SSL_CTX_sess_set_new_cb(ctx, uwsgi_ssl_session_new_cb);
                SSL_CTX_sess_set_get_cb(ctx, uwsgi_ssl_session_get_cb);
                SSL_CTX_sess_set_remove_cb(ctx, uwsgi_ssl_session_remove_cb);
        }
#endif

        SSL_CTX_set_timeout(ctx, uwsgi.ssl_sessions_timeout);

	struct uwsgi_string_list *usl = NULL;
	uwsgi_foreach(usl, uwsgi.ssl_options) {
		ssloptions |= atoi(usl->value);
	}

        SSL_CTX_set_options(ctx, ssloptions);


        return ctx;
}


char *uwsgi_rsa_sign(char *algo_key, char *message, size_t message_len, unsigned int *s_len) {

        // openssl could not be initialized
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        *s_len = 0;
        EVP_PKEY *pk = NULL;

        char *algo = uwsgi_str(algo_key);
        char *colon = strchr(algo, ':');
        if (!colon) {
                uwsgi_log("invalid RSA signature syntax, must be: <digest>:<pemfile>\n");
                free(algo);
                return NULL;
        }

        *colon = 0;
        char *keyfile = colon + 1;
        char *signature = NULL;

        FILE *kf = fopen(keyfile, "r");
        if (!kf) {
                uwsgi_error_open(keyfile);
                free(algo);
                return NULL;
        }

        if (PEM_read_PrivateKey(kf, &pk, NULL, NULL) == 0) {
                uwsgi_log("unable to load private key: %s\n", keyfile);
                free(algo);
                fclose(kf);
                return NULL;
        }

        fclose(kf);

        EVP_MD_CTX *ctx = EVP_MD_CTX_create();
        if (!ctx) {
                free(algo);
                EVP_PKEY_free(pk);
                return NULL;
        }

        const EVP_MD *md = EVP_get_digestbyname(algo);
        if (!md) {
                uwsgi_log("unknown digest algo: %s\n", algo);
                free(algo);
                EVP_PKEY_free(pk);
                EVP_MD_CTX_destroy(ctx);
                return NULL;
        }

        *s_len = EVP_PKEY_size(pk);
        signature = uwsgi_malloc(*s_len);

	if (EVP_SignInit_ex(ctx, md, NULL) == 0) {
                ERR_print_errors_fp(stderr);
                free(signature);
                signature = NULL;
                *s_len = 0;
                goto clear;
        }

        if (EVP_SignUpdate(ctx, message, message_len) == 0) {
                ERR_print_errors_fp(stderr);
                free(signature);
                signature = NULL;
                *s_len = 0;
                goto clear;
        }


        if (EVP_SignFinal(ctx, (unsigned char *) signature, s_len, pk) == 0) {
                ERR_print_errors_fp(stderr);
                free(signature);
                signature = NULL;
                *s_len = 0;
                goto clear;
        }

clear:
        free(algo);
        EVP_PKEY_free(pk);
        EVP_MD_CTX_destroy(ctx);
        return signature;

}

char *uwsgi_sanitize_cert_filename(char *base, char *key, uint16_t keylen) {
        uint16_t i;
        char *filename = uwsgi_concat4n(base, strlen(base), "/", 1, key, keylen, ".pem\0", 5);

        for (i = strlen(base) + 1; i < (strlen(base) + 1) + keylen; i++) {
                if (filename[i] >= '0' && filename[i] <= '9')
                        continue;
                if (filename[i] >= 'A' && filename[i] <= 'Z')
                        continue;
                if (filename[i] >= 'a' && filename[i] <= 'z')
                        continue;
                if (filename[i] == '.')
                        continue;
                if (filename[i] == '-')
                        continue;
                if (filename[i] == '_')
                        continue;
                filename[i] = '_';
        }

        return filename;
}

char *uwsgi_ssl_rand(size_t len) {
	unsigned char *buf = uwsgi_calloc(len);
	if (RAND_bytes(buf, len) <= 0) {
		free(buf);
		return NULL;
	}
	return (char *) buf;
}

char *uwsgi_sha1(char *src, size_t len, char *dst) {
        SHA_CTX sha;
        SHA1_Init(&sha);
        SHA1_Update(&sha, src, len);
        SHA1_Final((unsigned char *)dst, &sha);
	return dst;
}

char *uwsgi_md5(char *src, size_t len, char *dst) {
	MD5_CTX md5;
	MD5_Init(&md5);
	MD5_Update(&md5, src, len);
	MD5_Final((unsigned char *)dst, &md5);
	return dst;
}

char *uwsgi_sha1_2n(char *s1, size_t len1, char *s2, size_t len2, char *dst) {
        SHA_CTX sha;
        SHA1_Init(&sha);
        SHA1_Update(&sha, s1, len1);
        SHA1_Update(&sha, s2, len2);
        SHA1_Final((unsigned char *)dst, &sha);
        return dst;
}

void uwsgi_opt_sni(char *opt, char *value, void *foobar) {
        char *client_ca = NULL;
        char *v = uwsgi_str(value);

        char *space = strchr(v, ' ');
        if (!space) {
                uwsgi_log("invalid %s syntax, must be sni_key<space>crt,key[,ciphers,client_ca]\n", opt);
                exit(1);
        }
        *space = 0;
        char *crt = space+1;
        char *key = strchr(crt, ',');
        if (!key) {
                uwsgi_log("invalid %s syntax, must be sni_key<space>crt,key[,ciphers,client_ca]\n", opt);
                exit(1);
        }
        *key = '\0'; key++;

        char *ciphers = strchr(key, ',');
        if (ciphers) {
                *ciphers = '\0'; ciphers++;
                client_ca = strchr(ciphers, ',');
                if (client_ca) {
                        *client_ca = '\0'; client_ca++;
                }
        }

        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        SSL_CTX *ctx = uwsgi_ssl_new_server_context(v, crt, key, ciphers, client_ca);
        if (!ctx) {
                uwsgi_log("[uwsgi-ssl] DANGER unable to initialize context for \"%s\"\n", v);
                free(v);
                return;
        }

#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
        if (!strcmp(opt, "sni-regexp")) {
                struct uwsgi_regexp_list *url = uwsgi_regexp_new_list(&uwsgi.sni_regexp, v);
                url->custom_ptr = ctx;
        }
        else {
#endif
                struct uwsgi_string_list *usl = uwsgi_string_new_list(&uwsgi.sni, v);
                usl->custom_ptr = ctx;
#if defined(UWSGI_PCRE) || defined(UWSGI_PCRE2)
        }
#endif

}

struct uwsgi_string_list *uwsgi_ssl_add_sni_item(char *name, char *crt, char *key, char *ciphers, char *client_ca) {
	if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }
	SSL_CTX *ctx = uwsgi_ssl_new_server_context(name, crt, key, ciphers, client_ca);
        if (!ctx) {
                uwsgi_log("[uwsgi-ssl] DANGER unable to initialize context for \"%s\"\n", name);
		free(name);
		return NULL;
	}

	struct uwsgi_string_list *usl = uwsgi_string_new_list(&uwsgi.sni, name);
	usl->custom_ptr = ctx;
	// mark it as dynamic
	usl->custom = 1;
	uwsgi_log_verbose("[uwsgi-sni for pid %d] added SSL context for %s\n", (int) getpid(), name);
	return usl;
}

void uwsgi_ssl_del_sni_item(char *name, uint16_t name_len) {
	struct uwsgi_string_list *usl = NULL, *last_sni = NULL, *sni_item = NULL;
	uwsgi_foreach(usl, uwsgi.sni) {
		if (!uwsgi_strncmp(usl->value, usl->len, name, name_len) && usl->custom) {
			sni_item = usl;
			break;
		}
		last_sni = usl;
	}

	if (!sni_item) return;

	if (last_sni) {
		last_sni->next = sni_item->next;
	}
	else {
		uwsgi.sni = sni_item->next;
	}

	// we are free to destroy it as no more clients are using it
	SSL_CTX_free((SSL_CTX *) sni_item->custom_ptr);
	free(sni_item->value);
	free(sni_item);	

	uwsgi_log_verbose("[uwsgi-sni for pid %d] destroyed SSL context for %s\n",(int) getpid(), name);
}
