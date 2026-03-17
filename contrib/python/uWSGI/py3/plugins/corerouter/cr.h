#define COREROUTER_STATUS_FREE 0
#define COREROUTER_STATUS_CONNECTING 1
#define COREROUTER_STATUS_RECV_HDR 2
#define COREROUTER_STATUS_RESPONSE 3

#define cr_add_timeout(u, x) uwsgi_add_rb_timer(u->timeouts, uwsgi_now()+x->current_timeout, x)
#define cr_add_timeout_fast(u, x, t) uwsgi_add_rb_timer(u->timeouts, t+x->current_timeout, x)
#define cr_del_timeout(u, x) uwsgi_del_rb_timer(u->timeouts, x->timeout); free(x->timeout);

#define uwsgi_cr_error(x, y) uwsgi_log("[uwsgi-%s key: %.*s client_addr: %s client_port: %s] %s: %s [%s line %d]\n", x->session->corerouter->short_name, (x == x->session->main_peer) ? (x->session->peers ? x->session->peers->key_len: 0) : x->key_len, (x == x->session->main_peer) ? (x->session->peers ? x->session->peers->key: "") : x->key, x->session->client_address, x->session->client_port, y, strerror(errno), __FILE__, __LINE__)
#define uwsgi_cr_log(x, y, ...) uwsgi_log("[uwsgi-%s key: %.*s client_addr: %s client_port: %s]" y, x->session->corerouter->short_name,  (x == x->session->main_peer) ? (x->session->peers ? x->session->peers->key_len: 0) : x->key_len, (x == x->session->main_peer) ? (x->session->peers ? x->session->peers->key: "") : x->key, x->session->client_address, x->session->client_port, __VA_ARGS__)

#define cr_try_again if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS) {\
                     	errno = EINPROGRESS;\
                     	return -1;\
                     }

#define cr_write(peer, f) write(peer->fd, peer->out->buf + peer->out_pos, peer->out->pos - peer->out_pos);\
	if (len < 0) {\
                cr_try_again;\
                uwsgi_cr_error(peer, f);\
                return -1;\
        }\
	if (peer != peer->session->main_peer && peer->un) peer->un->rx+=len;\
        peer->out_pos += len;

#define cr_write_buf(peer, ubuf, f) write(peer->fd, ubuf->buf + ubuf##_pos, ubuf->pos - ubuf##_pos);\
        if (len < 0) {\
                cr_try_again;\
                uwsgi_cr_error(peer, f);\
                return -1;\
        }\
	if (peer != peer->session->main_peer && peer->un) peer->un->rx+=len;\
        ubuf##_pos += len;

#define cr_write_complete(peer) peer->out_pos == peer->out->pos

#define cr_write_complete_buf(peer, buf) buf##_pos == buf->pos

#define cr_connect(peer, f) peer->fd = uwsgi_connectn(peer->instance_address, peer->instance_address_len, 0, 1);\
        if (peer->fd < 0) {\
                peer->failed = 1;\
                peer->soopt = errno;\
                return -1;\
        }\
        peer->session->corerouter->cr_table[peer->fd] = peer;\
        peer->connecting = 1;\
	cr_write_to_backend(peer, f);

#define cr_read(peer, f) read(peer->fd, peer->in->buf + peer->in->pos, peer->in->len - peer->in->pos);\
	if (len < 0) {\
                cr_try_again;\
                uwsgi_cr_error(peer, f);\
                return -1;\
        }\
	if (peer != peer->session->main_peer && peer->un) peer->un->tx+=len;\
        peer->in->pos += len;\

#define cr_read_exact(peer, l, f) read(peer->fd, peer->in->buf + peer->in->pos, (l - peer->in->pos));\
        if (len < 0) {\
                cr_try_again;\
                uwsgi_cr_error(peer, f);\
                return -1;\
        }\
	if (peer != peer->session->main_peer && peer->un) peer->un->tx+=len;\
        peer->in->pos += len;\

#define cr_reset_hooks(peer) if(!peer->session->main_peer->disabled) {\
			if (uwsgi_cr_set_hooks(peer->session->main_peer, peer->session->main_peer->last_hook_read, NULL)) return -1;\
		}\
		else {\
			if (uwsgi_cr_set_hooks(peer->session->main_peer, NULL, NULL)) return -1;\
		}\
		struct corerouter_peer *peers = peer->session->peers;\
                while(peers) {\
                        if (uwsgi_cr_set_hooks(peers, peers->last_hook_read, NULL)) return -1;\
                        peers = peers->next;\
                }

#define cr_reset_hooks_and_read(peer, f) if (uwsgi_cr_set_hooks(peer->session->main_peer, peer->session->main_peer->last_hook_read, NULL)) return -1;\
		peer->last_hook_read = f;\
                struct corerouter_peer *peers = peer->session->peers;\
                while(peers) {\
                        if (uwsgi_cr_set_hooks(peers, peers->last_hook_read, NULL)) return -1;\
                        peers = peers->next;\
                }

#define cr_write_to_main(peer, f) if (uwsgi_cr_set_hooks(peer->session->main_peer, NULL, f)) return -1;\
		struct corerouter_peer *peers = peer->session->peers;\
                while(peers) {\
                        if (uwsgi_cr_set_hooks(peers, NULL, NULL)) return -1;\
                        peers = peers->next;\
                }

#define cr_write_to_backend(peer, f) if (uwsgi_cr_set_hooks(peer->session->main_peer, NULL, NULL)) return -1;\
		if (uwsgi_cr_set_hooks(peer, NULL, f)) return -1;\
                struct corerouter_peer *peers = peer->session->peers;\
                while(peers) {\
			if (peers != peer) {\
                        	if (uwsgi_cr_set_hooks(peers, NULL, NULL)) return -1;\
			}\
                        peers = peers->next;\
                }

#define cr_peer_connected(peer, f) socklen_t solen = sizeof(int);\
        if (getsockopt(peer->fd, SOL_SOCKET, SO_ERROR, (void *) (&peer->soopt), &solen) < 0) {\
                uwsgi_cr_error(peer, f "/getsockopt()");\
                peer->failed = 1;\
                return -1;\
        }\
        if (peer->soopt) {\
                peer->failed = 1;\
                return -1;\
        }\
	peer->connecting = 0;\
	peer->can_retry = 0;\
        if (peer->static_node) peer->static_node->custom2++;\
        if (peer->un) {\
		peer->un->requests++;\
		peer->un->last_requests++;\
	}\


struct corerouter_session;

// a peer is a connection to a socket (a client or a backend) and can be monitored for events.
struct corerouter_peer {
	// the file descriptor 
	int fd;
	// the session
	struct corerouter_session *session;

	// if set do not wait for events
	int disabled;

	// hook to run on a read event
	ssize_t (*hook_read)(struct corerouter_peer *);
	ssize_t (*last_hook_read)(struct corerouter_peer *);
	// hook to run on a write event
	ssize_t (*hook_write)(struct corerouter_peer *);
	ssize_t (*last_hook_write)(struct corerouter_peer *);

	// has the peer failed ?
	int failed;
	// is the peer connecting ?
	int connecting;
	// is there a connection error ?
        int soopt;
	// has the peer timed out ?
        int timed_out;
	// the timeout rb_tree
        struct uwsgi_rb_timer *timeout;

	// each peer can map to a different instance
        char *tmp_socket_name;
        char *instance_address;
        uint64_t instance_address_len;

	// backend info
        struct uwsgi_subscribe_node *un;
        struct uwsgi_string_list *static_node;

	// incoming data 
        struct uwsgi_buffer *in;
	// data to send
        struct uwsgi_buffer *out;
	// amount of sent data (partial write management)
	size_t out_pos;
	int out_need_free;

	// stream id (could have various use)
	uint32_t sid;

	// internal parser status
	int r_parser_status;

	// can retry ?
	int can_retry;
	// how many retries ?
	uint16_t retries;

	// parsed key
        char key[0xff];
        uint8_t key_len;

	uint8_t modifier1;
	uint8_t modifier2;

	struct corerouter_peer *prev;
	struct corerouter_peer *next;

	int current_timeout;

	ssize_t (*flush)(struct corerouter_peer *);

	int is_flushing;
	int is_buffering;
        int buffering_fd;
};

struct uwsgi_corerouter {

	char *name;
	char *short_name;
	size_t session_size;

	int (*alloc_session)(struct uwsgi_corerouter *, struct uwsgi_gateway_socket *, struct corerouter_session *, struct sockaddr *, socklen_t);
	int (*mapper)(struct uwsgi_corerouter *, struct corerouter_peer *);

        int has_sockets;
	int has_backends;
        int has_subscription_sockets;

        int processes;
        int quiet;

        struct uwsgi_rbtree *timeouts;

        char *use_cache;
	struct uwsgi_cache *cache;
        int nevents;

	int max_retries;

	char *magic_table[256];

        int queue;

        char *pattern;
        int pattern_len;

        char *base;
        int base_len;

        size_t post_buffering;
        char *pb_base_dir;

        struct uwsgi_string_list *static_nodes;
        struct uwsgi_string_list *current_static_node;
        int static_node_gracetime;

        char *stats_server;
        int cr_stats_server;

        int use_socket;
        int socket_num;
        struct uwsgi_socket *to_socket;

        struct uwsgi_subscribe_slot **subscriptions;

        struct uwsgi_string_list *fallback;

        int socket_timeout;

        uint8_t code_string_modifier1;
        char *code_string_code;
        char *code_string_function;

        struct uwsgi_rb_timer *subscriptions_check;

        int cheap;
        int i_am_cheap;

        int tolerance;
        int harakiri;

        struct corerouter_peer **cr_table;

	int interesting_fd;

	uint64_t active_sessions;

	uid_t uid;
	gid_t gid;

        struct uwsgi_string_list *resubscribe;
        char *resubscribe_bind;

	size_t buffer_size;
	int fallback_on_no_key;

	char *fallback_key;
	int fallback_key_len;
};

// a session is started when a client connect to the router
struct corerouter_session {

	// corerouter related to this session
	struct uwsgi_corerouter *corerouter;
	// gateway socket related to this session
	struct uwsgi_gateway_socket *ugs;

	// the list of fallback instances
        struct uwsgi_string_list *fallback;

	// store the client address
	struct sockaddr_un addr;
        socklen_t addr_len;

	void (*close)(struct corerouter_session *);
	int (*retry)(struct corerouter_peer *);

	// leave the main peer alive
	int can_keepalive;
	// destroy the main peer after the last full write
	int wait_full_write;

	// this is the peer of the client
	struct corerouter_peer *main_peer;
	// this is the linked list of backends
	struct corerouter_peer *peers;

	// connect after the next successfull write
	struct corerouter_peer *connect_peer_after_write;

	union uwsgi_sockaddr client_sockaddr;
#ifdef AF_INET6
	char client_address[INET6_ADDRSTRLEN];
#else
	char client_address[INET_ADDRSTRLEN];
#endif

	// use 11 bytes to be snprintf friendly
	char client_port[11];
};

void uwsgi_opt_corerouter(char *, char *, void *);
void uwsgi_opt_undeferred_corerouter(char *, char *, void *);
void uwsgi_opt_corerouter_use_socket(char *, char *, void *);
void uwsgi_opt_corerouter_use_base(char *, char *, void *);
void uwsgi_opt_corerouter_use_pattern(char *, char *, void *);
void uwsgi_opt_corerouter_zerg(char *, char *, void *);
void uwsgi_opt_corerouter_cs(char *, char *, void *);
void uwsgi_opt_corerouter_ss(char *, char *, void *);
void uwsgi_opt_corerouter_fallback_key(char *, char *, void *);


void corerouter_manage_subscription(char *, uint16_t, char *, uint16_t, void *);

void *uwsgi_corerouter_setup_event_queue(struct uwsgi_corerouter *, int);
void uwsgi_corerouter_manage_subscription(struct uwsgi_corerouter *, int id, struct uwsgi_gateway_socket *);
void uwsgi_corerouter_manage_internal_subscription(struct uwsgi_corerouter *, int);
void uwsgi_corerouter_setup_sockets(struct uwsgi_corerouter *);

int uwsgi_corerouter_init(struct uwsgi_corerouter *);

struct corerouter_session *corerouter_alloc_session(struct uwsgi_corerouter *, struct uwsgi_gateway_socket *, int, struct sockaddr *, socklen_t);
void corerouter_close_session(struct uwsgi_corerouter *, struct corerouter_session *);

int uwsgi_cr_map_use_void(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_cache(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_pattern(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_cluster(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_subscription(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_subscription_dotsplit(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_base(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_cs(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_to(struct uwsgi_corerouter *, struct corerouter_peer *);
int uwsgi_cr_map_use_static_nodes(struct uwsgi_corerouter *, struct corerouter_peer *);

int uwsgi_corerouter_has_backends(struct uwsgi_corerouter *);

int uwsgi_cr_set_hooks(struct corerouter_peer *, ssize_t (*)(struct corerouter_peer *), ssize_t (*)(struct corerouter_peer *));
struct corerouter_peer *uwsgi_cr_peer_add(struct corerouter_session *);
struct corerouter_peer *uwsgi_cr_peer_find_by_sid(struct corerouter_session *, uint32_t);
void corerouter_close_peer(struct uwsgi_corerouter *, struct corerouter_peer *);
struct uwsgi_rb_timer *corerouter_reset_timeout(struct uwsgi_corerouter *, struct corerouter_peer *);
