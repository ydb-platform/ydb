#include <contrib/python/uWSGI/py3/config.h>
#include <stdlib.h>
#include <uwsgi.h>

/*

 This is a stats pusher plugin for sending metrics in json format to several types of targets: file (appending),
 udp socket, http socket (by POST request)

 --stats-push json:URL

 where URL has one of the formats: file://file_path, udp://<host|IP>:<port>, http://<host|IP>:<port>

 Examples:

 --stats-push json:file:///my/uwagi/app.stats

 --stats-push json:udp://127.0.0.1:8125

 --stats-push json:http://[::1]:9125/my_handle

 --stats-push json:http://my.favorite.balancer.net:9145/my/handle

*/

extern struct uwsgi_server uwsgi;

enum UWSGI_STATS_PUSHER_TARGET {
    UWSGI_STATS_PUSHER_FILE,
    UWSGI_STATS_PUSHER_UDP,
    UWSGI_STATS_PUSHER_HTTP
};
enum UWSGI_STATS_PUSHER_PROTO {
    UWSGI_STATS_PUSHER_UNIX,
    UWSGI_STATS_PUSHER_IP,
    UWSGI_STATS_PUSHER_IP6
};
enum CONSTEXPR {
    MAX_RESPONSE_LEN = 4096,
    MAX_REQUEST_HEADER_LEN = 1024
};

struct uwsgi_stats_pusher_json_conf {
    int fd;
    enum UWSGI_STATS_PUSHER_TARGET type;
    enum UWSGI_STATS_PUSHER_PROTO proto;
    union uwsgi_sockaddr addr;
    socklen_t addr_len;
    char *prefix;
    uint16_t prefix_len;
    char *url;
    char *freq;
    char *separator;
    int atomic_reset;
};

int send_data(int fd, char *msg, size_t msg_len) {
    int sent = 0, bytes;
    do {
        bytes = write(fd, msg + sent, msg_len - sent);
        if (bytes < 0) {
            uwsgi_error("uwsgi_stats_pusher_json() -> send_data() -> HTTP write()")
        }
        if (bytes <= 0)
            break;
        sent += bytes;
    } while (sent < msg_len);
    return (sent == 0 && bytes < 0) ? -1 : sent;
}

static void stats_pusher_json(struct uwsgi_stats_pusher_instance *uspi, time_t now, char *json, size_t json_len) {
    struct uwsgi_stats_pusher_json_conf *uspic = (struct uwsgi_stats_pusher_json_conf *) uspi->data;
    if (!uspi->configured) {
        char *colon = NULL, *atomic_reset = NULL;
        uspic = uwsgi_calloc(sizeof(struct uwsgi_stats_pusher_json_conf));
        if (uspi->arg) {
            if (uwsgi_kvlist_parse(uspi->arg, strlen(uspi->arg), ',', '=',
                    "url",       &uspic->url,               // URL for pushing metrics' data: <proto>://<host|IP|file_path>[:<port>][/<uri>]
                    "separator", &uspic->separator,         // target file records separator
                    "freq",      &uspic->freq,              // frequency of pushing metrics' data
                    "atomic_reset", &atomic_reset, NULL)) { // atomic reset of all metrics at once
                free(uspi);
                return;
            }
            uspic->atomic_reset = atomic_reset ? 1 : 0;
        }
        if (uspic->url) {
            if (!strncmp(uspic->url, "file://", 7)) {
                uspic->type = UWSGI_STATS_PUSHER_FILE;
                uspic->fd = open(uspic->url + 7, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP);
            } else if (!strncmp(uspic->url, "udp://", 6)) {
                colon = strrchr(uspic->url + 6, ':');
                if (!colon) {
                    uwsgi_log("invalid UDP socket address %s\n", uspic->url);
                    free(uspic);
                    return;
                }
                *colon = 0;
                uspic->type = UWSGI_STATS_PUSHER_UDP;
                char *colon2 = strchr(uspic->url + 6, ':');
                if (colon2) {
                    uspic->proto = UWSGI_STATS_PUSHER_IP6;
                    uspic->addr_len = socket_to_in_addr6(uspic->url + 6, colon, 0, &uspic->addr.sa_in6);
                    uspic->fd = socket(AF_INET6, SOCK_DGRAM, 0);
                } else {
                    uspic->proto = UWSGI_STATS_PUSHER_IP;
                    uspic->addr_len = socket_to_in_addr(uspic->url + 6, colon, 0, &uspic->addr.sa_in);
                    uspic->fd = socket(AF_INET, SOCK_DGRAM, 0);
                }
                if (uspic->fd < 0) {
                    uwsgi_error("stats_pusher_json() -> UDP socket()")
                    *colon = ':';
                    free(uspic);
                    return;
                }
                uwsgi_socket_nb(uspic->fd);
            } else if (!strncmp(uspic->url, "http://", 7)) {
                colon = strrchr(uspic->url + 7, ':');
                if (!colon) {
                    uwsgi_log("invalid HTTP socket address %s\n", uspic->url);
                    free(uspic);
                    return;
                }
                *colon = 0;
                uspic->type = UWSGI_STATS_PUSHER_HTTP;
                char *colon2 = strchr(uspic->url + 7, ':');
                if (colon2) {
                    uspic->proto = UWSGI_STATS_PUSHER_IP6;
                    uspic->addr_len = socket_to_in_addr6(uspic->url + 7, colon, 0, &uspic->addr.sa_in6);
                } else {
                    uspic->proto = UWSGI_STATS_PUSHER_IP;
                    uspic->addr_len = socket_to_in_addr(uspic->url + 7, colon, 0, &uspic->addr.sa_in);
                }
            }
        } else {
            uspic->type = UWSGI_STATS_PUSHER_FILE;
            uspic->url = "uwsgi.stats";
        }
        if (!uspic->separator)
            uspic->separator = "\n\n";
        if (colon) *colon = ':';
        uspi->freq = uspic->freq ? atoi(uspic->freq) : 1;
        uspi->configured = 1;
        uspi->data = uspic;
    }
    if (uspic->fd < 0) {
        uwsgi_error_open(uspic->url)
        return;
    }
    struct uwsgi_metric *um = uwsgi.metrics;
    if (uspic->atomic_reset)
        uwsgi_wlock(uwsgi.metrics_lock);
    while(um) { // reset metrics if needed
        if (um->reset_after_push){
            if (!uspic->atomic_reset)
                uwsgi_wlock(uwsgi.metrics_lock);
            *um->value = um->initial_value;
            if (!uspic->atomic_reset)
                uwsgi_rwunlock(uwsgi.metrics_lock);
        }
        um = um->next;
    }
    if (uspic->atomic_reset)
        uwsgi_rwunlock(uwsgi.metrics_lock);
    if (uspic->type == UWSGI_STATS_PUSHER_FILE) {
        uwsgi_rlock(uwsgi.metrics_lock);
        ssize_t rlen = write(uspic->fd, json, json_len);
        if (rlen != (ssize_t) json_len) {
            uwsgi_error("uwsgi_stats_pusher_json() -> write()\n")
        }
        rlen = write(uspic->fd, uspic->separator, strlen(uspic->separator));
        if (rlen != (ssize_t) strlen(uspic->separator)) {
            uwsgi_error("uwsgi_stats_pusher_json() -> write()\n")
        }
        uwsgi_rwunlock(uwsgi.metrics_lock);
    } else {
        struct sockaddr *sa = NULL;
        if (uspic->proto == UWSGI_STATS_PUSHER_IP)
            sa = (struct sockaddr *) &uspic->addr.sa_in;
        else if (uspic->proto == UWSGI_STATS_PUSHER_IP6)
            sa = (struct sockaddr *) &uspic->addr.sa_in6;
        if (uspic->type == UWSGI_STATS_PUSHER_UDP) {
            struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
            ub->pos = 0;    // reset the buffer
            if (uwsgi_buffer_append_json(ub, json, json_len)) {
                uwsgi_error("uwsgi_stats_pusher_json() -> uwsgi_buffer_append_json()\n")
            } else {
                uwsgi_rlock(uwsgi.metrics_lock);
                if (sendto(uspic->fd, ub->buf, ub->pos, 0, sa, uspic->addr_len) < 0)
                    uwsgi_error("uwsgi_stats_pusher_json() -> UDP sendto()")
                uwsgi_rwunlock(uwsgi.metrics_lock);
            }
        } else if (uspic->type == UWSGI_STATS_PUSHER_HTTP) {
            if (uspic->proto == UWSGI_STATS_PUSHER_IP6)
                uspic->fd = socket(AF_INET6, SOCK_STREAM, 0);
            else if (uspic->proto == UWSGI_STATS_PUSHER_IP)
                uspic->fd = socket(AF_INET, SOCK_STREAM, 0);
            if (uspic->fd < 0) {
                uwsgi_error("stats_pusher_json() -> HTTP socket()")
                free(uspic);
                return;
            }
            int conn_err = connect(uspic->fd, sa, uspic->addr_len);
            if (conn_err < 0 && errno != EISCONN) {
                uwsgi_log("uwsgi_stats_pusher_json() -> HTTP connect(), error: %d\n", errno);
            } else {
                int bytes, sent, received, total;
                char header[MAX_REQUEST_HEADER_LEN], response[MAX_RESPONSE_LEN];
                char *message_fmt = "POST /%s HTTP/1.0\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n";
                char *slash = strchr(uspic->url + 7, '/');
                json[json_len] = 0;
                sprintf(header, message_fmt, (slash ? (slash + 1) : ""), json_len);
                total = strlen(header);
                uwsgi_rlock(uwsgi.metrics_lock);
                sent = send_data(uspic->fd, header, total);
                if (sent <= 0) {
                    conn_err = close(uspic->fd);
                    if (conn_err < 0)
                        uwsgi_log("uwsgi_stats_pusher_json close: errno=%s", errno);
                    if (uspic->proto == UWSGI_STATS_PUSHER_IP6)
                        uspic->fd = socket(AF_INET6, SOCK_STREAM, 0);
                    else if (uspic->proto == UWSGI_STATS_PUSHER_IP)
                        uspic->fd = socket(AF_INET, SOCK_STREAM, 0);
                    conn_err = connect(uspic->fd, sa, uspic->addr_len);
                    if (conn_err < 0 && errno != EISCONN) {
                        uwsgi_error("uwsgi_stats_pusher_json() -> HTTP connect()");
                    } else
                        send_data(uspic->fd, header, strlen(header));
                }
                bytes = send_data(uspic->fd, json, json_len);
                uwsgi_rwunlock(uwsgi.metrics_lock);
                if (bytes >= 0) { /* receive the response */
                    memset(response, 0, sizeof(response));
                    total = sizeof(response) - 1;
                    received = 0;
                    do {
                        bytes = read(uspic->fd, response + received, total - received);
                        if (bytes < 0)
                            uwsgi_error("uwsgi_stats_pusher_json() -> HTTP read()")
                        if (bytes <= 0)
                            break;
                        received += bytes;
                    } while (received < total);
                }
            }
            close(uspic->fd);
        }
    }
}

static void stats_pusher_json_init(void) {
	uwsgi_register_stats_pusher("json", stats_pusher_json);
}

struct uwsgi_plugin stats_pusher_json_plugin = {
        .name = "stats_pusher_json",
        .on_load = stats_pusher_json_init,
};

