#include "../../uwsgi.h"

extern struct uwsgi_server uwsgi;

#include "../corerouter/cr.h"

#ifdef UWSGI_SSL
#if !defined(OPENSSL_NO_NEXTPROTONEG) && defined(OPENSSL_NPN_UNSUPPORTED)
#ifdef UWSGI_ZLIB
#define UWSGI_SPDY
#endif
#endif
#endif

struct uwsgi_http {

        struct uwsgi_corerouter cr;

        uint8_t modifier1;
        uint8_t modifier2;
        struct uwsgi_string_list *http_vars;
        uint64_t manage_expect;

        int raw_body;
        int keepalive;
        int auto_chunked;
        int auto_gzip;

        int websockets;
#ifdef UWSGI_SSL
        char *https_session_context;
        int https_export_cert;
#endif

        struct uwsgi_string_list *stud_prefix;

#ifdef UWSGI_SPDY
        int spdy_index;
	struct uwsgi_buffer *spdy3_settings;
	size_t spdy3_settings_size;
#endif

	int server_name_as_http_host;

	int headers_timeout;
	int connect_timeout;
	int manage_source;
	int enable_proxy_protocol;
	int chunked_input;
	int manage_rtsp;
}; 

struct http_session {

        struct corerouter_session session;

	// used for http parser
        int rnrn;
	size_t headers_size;
	size_t remains;
	size_t content_length;

	int raw_body;

        char *port;
        int port_len;

        char *request_uri;
        uint16_t request_uri_len;

        char *path_info;
        uint16_t path_info_len;

	int force_chunked;

	int websockets;
	char *websocket_key;
	uint16_t websocket_key_len;

        size_t received_body;

#ifdef UWSGI_SSL
        SSL *ssl;
        X509 *ssl_client_cert;
        char *ssl_client_dn;
        BIO *ssl_bio;
        char *ssl_cc;
        int force_https;
        struct uwsgi_buffer *force_ssl_buf;
#endif

#ifdef UWSGI_SPDY
        int spdy;
        int spdy_initialized;
	int spdy_phase;
	uint32_t spdy_need;

        z_stream spdy_z_in;
        z_stream spdy_z_out;

        uint8_t spdy_frame_type;

        uint16_t spdy_control_version;
        uint16_t spdy_control_type;
        uint8_t spdy_control_flags;
        uint32_t spdy_control_length;

	uint32_t spdy_data_stream_id;
        uint8_t spdy_data_flags;
        uint32_t spdy_data_length;

	struct uwsgi_buffer *spdy_ping;

	uint32_t spdy_update_window;

        ssize_t (*spdy_hook)(struct corerouter_peer *);
#endif

#ifdef UWSGI_ZLIB
	int can_gzip;
	int has_gzip;
	int force_gzip;
	uint32_t gzip_crc32;
	uint32_t gzip_size;
	z_stream z;
#endif

        int send_expect_100;

        // 1 (family) + 4/16 (addr)
        char stud_prefix[17];
        size_t stud_prefix_remains;
        size_t stud_prefix_pos;

	struct uwsgi_buffer *last_chunked;

	ssize_t (*func_write)(struct corerouter_peer *);

	int is_rtsp;

};


#ifdef UWSGI_SSL

#define UWSGI_HTTP_NOSSL        0
#define UWSGI_HTTP_SSL          1
#define UWSGI_HTTP_FORCE_SSL    2

void uwsgi_opt_https(char *, char *, void *);
void uwsgi_opt_https2(char *, char *, void *);
void uwsgi_opt_http_to_https(char *, char *, void *);

ssize_t hr_recv_http_ssl(struct corerouter_peer *);
ssize_t hr_read_ssl_body(struct corerouter_peer *);
ssize_t hr_write_ssl_response(struct corerouter_peer *);

int hr_force_https(struct corerouter_peer *);

void hr_session_ssl_close(struct corerouter_session *);

ssize_t hr_ssl_read(struct corerouter_peer *);
ssize_t hr_ssl_write(struct corerouter_peer *);

int hr_https_add_vars(struct http_session *, struct corerouter_peer *, struct uwsgi_buffer *);
void hr_setup_ssl(struct http_session *, struct uwsgi_gateway_socket *);

#endif

#ifdef UWSGI_SPDY
int uwsgi_spdy_npn(SSL *ssl, const unsigned char **, unsigned int *, void *);
void uwsgi_spdy_info_cb(SSL const *, int, int);
ssize_t hr_recv_spdy_control_frame(struct corerouter_peer *);
ssize_t spdy_parse(struct corerouter_peer *);
void spdy_window_update(char *, uint32_t, uint32_t);
#endif

ssize_t hs_http_manage(struct corerouter_peer *, ssize_t);

ssize_t hr_instance_connected(struct corerouter_peer *);
ssize_t hr_instance_write(struct corerouter_peer *);

ssize_t hr_instance_read_response(struct corerouter_peer *);
ssize_t hr_read_body(struct corerouter_peer *);
ssize_t hr_write_body(struct corerouter_peer *);

void hr_session_close(struct corerouter_session *);
ssize_t http_parse(struct corerouter_peer *);

int http_response_parse(struct http_session *, struct uwsgi_buffer *, size_t);
