#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI HTTPS router

*/

#include "common.h"

#ifdef UWSGI_SSL

extern struct uwsgi_http uhttp;

// taken from nginx
static void hr_ssl_clear_errors() {
	while (ERR_peek_error()) {
		(void) ERR_get_error();
	}
	ERR_clear_error();
}

void uwsgi_opt_https(char *opt, char *value, void *cr) {
        struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;
        char *client_ca = NULL;

        // build socket, certificate and key file
        char *sock = uwsgi_str(value);
        char *crt = strchr(sock, ',');
        if (!crt) {
                uwsgi_log("invalid https syntax must be socket,crt,key\n");
                exit(1);
        }
        *crt = '\0'; crt++;
        char *key = strchr(crt, ',');
        if (!key) {
                uwsgi_log("invalid https syntax must be socket,crt,key\n");
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

        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(sock, ucr->name);
        // ok we have the socket, initialize ssl if required
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        // initialize ssl context
	char *name = uhttp.https_session_context;
	if (!name) {
		name = uwsgi_concat3(ucr->short_name, "-", ugs->name);
	}

        ugs->ctx = uwsgi_ssl_new_server_context(name, crt, key, ciphers, client_ca);
	if (!ugs->ctx) {
		exit(1);
	}
        // set the ssl mode
        ugs->mode = UWSGI_HTTP_SSL;

        ucr->has_sockets++;
}

void uwsgi_opt_https2(char *opt, char *value, void *cr) {
        struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;

	char *s2_addr = NULL;
	char *s2_cert = NULL;
	char *s2_key = NULL;
	char *s2_ciphers = NULL;
	char *s2_clientca = NULL;
	char *s2_spdy = NULL;

	if (uwsgi_kvlist_parse(value, strlen(value), ',', '=',
                        "addr", &s2_addr,
                        "cert", &s2_cert,
                        "crt", &s2_cert,
                        "key", &s2_key,
                        "ciphers", &s2_ciphers,
                        "clientca", &s2_clientca,
                        "client_ca", &s2_clientca,
                        "spdy", &s2_spdy,
                	NULL)) {
		uwsgi_log("error parsing --https2 option\n");
		exit(1);
        }

	if (!s2_addr || !s2_cert || !s2_key) {
		uwsgi_log("--https2 option needs addr, cert and key items\n");
		exit(1);
	}

        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(s2_addr, ucr->name);
        // ok we have the socket, initialize ssl if required
        if (!uwsgi.ssl_initialized) {
                uwsgi_ssl_init();
        }

        // initialize ssl context
        char *name = uhttp.https_session_context;
        if (!name) {
                name = uwsgi_concat3(ucr->short_name, "-", ugs->name);
        }

#ifdef UWSGI_SPDY
	if (s2_spdy) {
        	uhttp.spdy_index = SSL_CTX_get_ex_new_index(0, NULL, NULL, NULL, NULL);
		uhttp.spdy3_settings = uwsgi_buffer_new(uwsgi.page_size);
		if (uwsgi_buffer_append(uhttp.spdy3_settings, "\x80\x03\x00\x04\x01", 5)) goto spdyerror;
		if (uwsgi_buffer_u24be(uhttp.spdy3_settings, (8 * 2) + 4)) goto spdyerror;
		if (uwsgi_buffer_u32be(uhttp.spdy3_settings, 2)) goto spdyerror;

		// SETTINGS_ROUND_TRIP_TIME
		if (uwsgi_buffer_append(uhttp.spdy3_settings, "\x01\x00\x00\x03", 4)) goto spdyerror;
		if (uwsgi_buffer_u32be(uhttp.spdy3_settings, 30 * 1000)) goto spdyerror;
		// SETTINGS_INITIAL_WINDOW_SIZE
		if (uwsgi_buffer_append(uhttp.spdy3_settings, "\x01\x00\x00\x07", 4)) goto spdyerror;
		if (uwsgi_buffer_u32be(uhttp.spdy3_settings, 8192)) goto spdyerror;

		uhttp.spdy3_settings_size = uhttp.spdy3_settings->pos;
	}
#endif

        ugs->ctx = uwsgi_ssl_new_server_context(name, s2_cert, s2_key, s2_ciphers, s2_clientca);
        if (!ugs->ctx) {
                exit(1);
        }
#ifdef UWSGI_SPDY
	if (s2_spdy) {
        	SSL_CTX_set_info_callback(ugs->ctx, uwsgi_spdy_info_cb);
        	SSL_CTX_set_next_protos_advertised_cb(ugs->ctx, uwsgi_spdy_npn, NULL);
	}
#endif
        // set the ssl mode
        ugs->mode = UWSGI_HTTP_SSL;

        ucr->has_sockets++;

	return;

#ifdef UWSGI_SPDY
spdyerror:
	uwsgi_log("unable to initialize SPDY settings buffers\n");
	exit(1);
#endif
}



void uwsgi_opt_http_to_https(char *opt, char *value, void *cr) {
        struct uwsgi_corerouter *ucr = (struct uwsgi_corerouter *) cr;

        char *sock = uwsgi_str(value);
        char *port = strchr(sock, ',');
        if (port) {
                *port = '\0';
                port++;
        }

        struct uwsgi_gateway_socket *ugs = uwsgi_new_gateway_socket(sock, ucr->name);

        // set context to the port
        ugs->ctx = port;
        // force SSL mode
        ugs->mode = UWSGI_HTTP_FORCE_SSL;

        ucr->has_sockets++;
}

int hr_https_add_vars(struct http_session *hr, struct corerouter_peer *peer, struct uwsgi_buffer *out) {
// HTTPS (adapted from nginx)
        if (hr->session.ugs->mode == UWSGI_HTTP_SSL) {
                if (uwsgi_buffer_append_keyval(out, "HTTPS", 5, "on", 2)) return -1;
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
			const char *servername = SSL_get_servername(hr->ssl, TLSEXT_NAMETYPE_host_name);
                        if (servername && strlen(servername) <= 0xff) {
				peer->key_len = strlen(servername);
                        	memcpy(peer->key, servername, peer->key_len) ;
                        }
#endif
                hr->ssl_client_cert = SSL_get_peer_certificate(hr->ssl);
                if (hr->ssl_client_cert) {
                        int client_cert_len;
                        unsigned char *client_cert_der = NULL;
                        client_cert_len = i2d_X509(hr->ssl_client_cert, &client_cert_der);
                        if (client_cert_len < 0) return -1;
			int ret = uwsgi_buffer_append_keyval(out, "HTTPS_CLIENT_CERTIFICATE", 24, (char*)client_cert_der, client_cert_len);
			OPENSSL_free(client_cert_der);
			if (ret) return -1;

                        X509_NAME *name = X509_get_subject_name(hr->ssl_client_cert);
                        if (name) {
                                hr->ssl_client_dn = X509_NAME_oneline(name, NULL, 0);
                                if (uwsgi_buffer_append_keyval(out, "HTTPS_DN", 8, hr->ssl_client_dn, strlen(hr->ssl_client_dn))) return -1;
                        }
                        if (uhttp.https_export_cert) {
                        hr->ssl_bio = BIO_new(BIO_s_mem());
                        if (hr->ssl_bio) {
                                if (PEM_write_bio_X509(hr->ssl_bio, hr->ssl_client_cert) > 0) {
                                        size_t cc_len = BIO_pending(hr->ssl_bio);
                                        hr->ssl_cc = uwsgi_malloc(cc_len);
                                        BIO_read(hr->ssl_bio, hr->ssl_cc, cc_len);
                                        if (uwsgi_buffer_append_keyval(out, "HTTPS_CC", 8, hr->ssl_cc, cc_len)) return -1;
                                }
                        }
                        }
                }
        }
        else if (hr->session.ugs->mode == UWSGI_HTTP_FORCE_SSL) {
                hr->force_https = 1;
        }

	return 0;
}

void hr_session_ssl_close(struct corerouter_session *cs) {
	hr_session_close(cs);
	struct http_session *hr = (struct http_session *) cs;
	if (hr->ssl_client_dn) {
                OPENSSL_free(hr->ssl_client_dn);
        }

        if (hr->ssl_cc) {
                free(hr->ssl_cc);
        }

        if (hr->ssl_bio) {
                BIO_free(hr->ssl_bio);
        }

        if (hr->ssl_client_cert) {
                X509_free(hr->ssl_client_cert);
        }

#ifdef UWSGI_SPDY
	if (hr->spdy_ping) {
		uwsgi_buffer_destroy(hr->spdy_ping);
	}
	if (hr->spdy) {
		deflateEnd(&hr->spdy_z_in);
		deflateEnd(&hr->spdy_z_out);
	}
#endif

	// clear the errors (otherwise they could be propagated)
	hr_ssl_clear_errors();
        SSL_free(hr->ssl);
}

int hr_force_https(struct corerouter_peer *peer) {
	struct corerouter_session *cs = peer->session;
        struct http_session *hr = (struct http_session *) cs;

	if (uwsgi_buffer_append(peer->in, "HTTP/1.1 301 Moved Permanently\r\nLocation: https://", 50)) return -1;              

        char *colon = memchr(peer->key, ':', peer->key_len);
        if (colon) {
        	if (uwsgi_buffer_append(peer->in, peer->key, colon-peer->key)) return -1;
        }
        else {
        	if (uwsgi_buffer_append(peer->in, peer->key, peer->key_len)) return -1;
        }

        if (cs->ugs->ctx) {
        	if (uwsgi_buffer_append(peer->in, ":", 1)) return -1;
        	if (uwsgi_buffer_append(peer->in,cs->ugs->ctx, strlen(cs->ugs->ctx))) return -1;
        }
        if (uwsgi_buffer_append(peer->in, hr->request_uri, hr->request_uri_len)) return -1;
        if (uwsgi_buffer_append(peer->in, "\r\n\r\n", 4)) return -1;

	hr->session.wait_full_write = 1;
        peer->session->main_peer->out = peer->in;
        peer->session->main_peer->out_pos = 0;
        cr_write_to_main(peer, hr->func_write);
        return 0;
}

ssize_t hr_ssl_write(struct corerouter_peer *main_peer) {
        struct corerouter_session *cs = main_peer->session;
        struct http_session *hr = (struct http_session *) cs;

	hr_ssl_clear_errors();

        int ret = SSL_write(hr->ssl, main_peer->out->buf + main_peer->out_pos, main_peer->out->pos - main_peer->out_pos);
        if (ret > 0) {
                main_peer->out_pos += ret;
                if (main_peer->out->pos == main_peer->out_pos) {
			// reset the buffer (if needed)
			main_peer->out->pos = 0;
			if (main_peer->session->wait_full_write) {
                        	main_peer->session->wait_full_write = 0;
                        	return 0;
                	}
			if (main_peer->session->connect_peer_after_write) {
                        	cr_connect(main_peer->session->connect_peer_after_write, hr_instance_connected);
                        	main_peer->session->connect_peer_after_write = NULL;
                        	return ret;
                	}
                        cr_reset_hooks(main_peer);
#ifdef UWSGI_SPDY
			if (hr->spdy) {
				return spdy_parse(main_peer);
			}
#endif
                }
                return ret;
        }

        int err = SSL_get_error(hr->ssl, ret);

	if (err == SSL_ERROR_ZERO_RETURN || err == 0) return 0;

        if (err == SSL_ERROR_WANT_READ) {
                cr_reset_hooks_and_read(main_peer, hr_ssl_write);
                return 1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
                cr_write_to_main(main_peer, hr_ssl_write);
                return 1;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_cr_error(main_peer, "hr_ssl_write()");
        }

        else if (err == SSL_ERROR_SSL && uwsgi.ssl_verbose) {
                ERR_print_errors_fp(stderr);
        }

        return -1;
}

ssize_t hr_ssl_read(struct corerouter_peer *main_peer) {
        struct corerouter_session *cs = main_peer->session;
        struct http_session *hr = (struct http_session *) cs;

	hr_ssl_clear_errors();

        // try to always leave 4k available
        if (uwsgi_buffer_ensure(main_peer->in, uwsgi.page_size)) return -1;
        int ret = SSL_read(hr->ssl, main_peer->in->buf + main_peer->in->pos, main_peer->in->len - main_peer->in->pos);
        if (ret > 0) {
                // fix the buffer
                main_peer->in->pos += ret;
                // check for pending data
                int ret2 = SSL_pending(hr->ssl);
                if (ret2 > 0) {
                        if (uwsgi_buffer_fix(main_peer->in, main_peer->in->len + ret2 )) {
                                uwsgi_cr_log(main_peer, "cannot fix the buffer to %d\n", main_peer->in->len + ret2);
                                return -1;
                        }
                        if (SSL_read(hr->ssl, main_peer->in->buf + main_peer->in->pos, ret2) != ret2) {
                                uwsgi_cr_log(main_peer, "SSL_read() on %d bytes of pending data failed\n", ret2);
                                return -1;
                        }
                        // fix the buffer
                        main_peer->in->pos += ret2;
                }
#ifdef UWSGI_SPDY
                if (hr->spdy) {
                        //uwsgi_log("RUNNING THE SPDY PARSER FOR %d bytes\n", main_peer->in->pos);
                        return spdy_parse(main_peer);
                }
#endif
                return http_parse(main_peer);
        }

        int err = SSL_get_error(hr->ssl, ret);

	if (err == SSL_ERROR_ZERO_RETURN || err == 0) return 0;

        if (err == SSL_ERROR_WANT_READ) {
                cr_reset_hooks_and_read(main_peer, hr_ssl_read);
                return 1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
                cr_write_to_main(main_peer, hr_ssl_read);
                return 1;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_cr_error(main_peer, "hr_ssl_read()");
        }

        else if (err == SSL_ERROR_SSL && uwsgi.ssl_verbose) {
                ERR_print_errors_fp(stderr);
        }

        return -1;
}

ssize_t hr_ssl_shutdown(struct corerouter_peer *peer) {
	// ensure no hooks are set
	if (uwsgi_cr_set_hooks(peer, NULL, NULL)) return -1;

	struct corerouter_session *cs = peer->session;
        struct http_session *hr = (struct http_session *) cs;	

	hr_ssl_clear_errors();

	int ret = SSL_shutdown(hr->ssl);
	int err = 0;

	if (ret != 1 && ERR_peek_error()) {
		err = SSL_get_error(hr->ssl, ret);
	}

	// no error, close the connection
	if (ret == 1 || err == 0 || err == SSL_ERROR_ZERO_RETURN) return 0;

	if (err == SSL_ERROR_WANT_READ) {
		if (uwsgi_cr_set_hooks(peer, hr_ssl_shutdown, NULL)) return -1;
                return 1;
        }

        else if (err == SSL_ERROR_WANT_WRITE) {
		if (uwsgi_cr_set_hooks(peer, NULL, hr_ssl_shutdown)) return -1;
                return 1;
        }

        else if (err == SSL_ERROR_SYSCALL) {
		if (errno != 0)
                	uwsgi_cr_error(peer, "hr_ssl_shutdown()");
        }

        else if (err == SSL_ERROR_SSL && uwsgi.ssl_verbose) {
                ERR_print_errors_fp(stderr);
        }

        return -1;
}

void hr_setup_ssl(struct http_session *hr, struct uwsgi_gateway_socket *ugs) {
 	hr->ssl = SSL_new(ugs->ctx);
        SSL_set_fd(hr->ssl, hr->session.main_peer->fd);
        SSL_set_accept_state(hr->ssl);
#ifdef UWSGI_SPDY
        SSL_set_ex_data(hr->ssl, uhttp.spdy_index, hr);
#endif
        uwsgi_cr_set_hooks(hr->session.main_peer, hr_ssl_read, NULL);
	hr->session.main_peer->flush = hr_ssl_shutdown;
        hr->session.close = hr_session_ssl_close;
	hr->func_write = hr_ssl_write;
}

#endif
