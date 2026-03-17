#include <contrib/python/uWSGI/py3/config.h>
#include <uwsgi.h>

extern struct uwsgi_server uwsgi;

struct uwsgi_mongodb_header {
	int32_t len;
	int32_t request_id;
	int32_t response_id;
	int32_t opcode;
};

struct uwsgi_mongodb_state {
	int fd;
	char *address;
	int32_t base_len;
	struct uwsgi_mongodb_header header;
	int32_t flags;
	char *collection;


	int32_t bson_base_len;
	int32_t bson_len;

	int64_t ts;

	int32_t bson_node_len;
	char *bson_node;
	int32_t bson_msg_len;
	struct iovec iovec[13];
};

static ssize_t uwsgi_mongodb_logger(struct uwsgi_logger *ul, char *message, size_t len) {
	
	struct uwsgi_mongodb_state *ums = NULL;

	if (!ul->configured) {

		ul->data = uwsgi_calloc(sizeof(struct uwsgi_mongodb_state));
		ums = (struct uwsgi_mongodb_state *) ul->data;

		// full default
        	if (ul->arg == NULL) {
			ums->address = uwsgi_str("127.0.0.1:27017");
			ums->collection = "uwsgi.logs";
			ums->bson_node = uwsgi.hostname;
			ums->bson_node_len = uwsgi.hostname_len;
			goto done;
		}

		ums->address = uwsgi_str(ul->arg);
		char *collection = strchr(ums->address, ',');
		// default to uwsgi.logs
		if (!collection) {
			ums->collection = "uwsgi.logs";
			ums->bson_node = uwsgi.hostname;
			ums->bson_node_len = uwsgi.hostname_len;
			goto done;
		}
		*collection = 0;
		ums->collection = collection+1;
	
		char *node = strchr(ums->collection, ',');
		// default to hostname
		if (!node) {
			ums->bson_node = uwsgi.hostname;
			ums->bson_node_len = uwsgi.hostname_len;
			goto done;
		}	
		*node = 0;
		ums->bson_node = node+1;
		ums->bson_node_len = strlen(ums->bson_node)+1;

done:
		ums->fd = -1;
		// header
		ums->iovec[0].iov_base = &ums->header;
		ums->iovec[0].iov_len = sizeof(struct uwsgi_mongodb_header);
		// OPCODE INSERT
		ums->header.opcode = 2002; 
		// flags
		ums->iovec[1].iov_base = &ums->flags;
		ums->iovec[1].iov_len = sizeof(int32_t);
		// collection name
		ums->iovec[2].iov_base = ums->collection;
		ums->iovec[2].iov_len = strlen(ums->collection)+1;
		// BSON len
		ums->iovec[3].iov_base = &ums->bson_len;
		ums->iovec[3].iov_len = sizeof(int32_t);
		// BSON node
		ums->iovec[4].iov_base = "\x02node\0";
		ums->iovec[4].iov_len = 6;
		ums->iovec[5].iov_base = &ums->bson_node_len;
		ums->iovec[5].iov_len = sizeof(int32_t);
		ums->iovec[6].iov_base = ums->bson_node;
		ums->iovec[6].iov_len = ums->bson_node_len;

		// BSON timestamp (ts)
		ums->iovec[7].iov_base = "\x09ts\0";
		ums->iovec[7].iov_len = 4;
		ums->iovec[8].iov_base = &ums->ts;
                ums->iovec[8].iov_len = sizeof(int64_t);


		// BSON msg
		ums->iovec[9].iov_base = "\2msg\0";
		ums->iovec[9].iov_len = 5;
		ums->iovec[10].iov_base = &ums->bson_msg_len;
                ums->iovec[10].iov_len = sizeof(int32_t);
		// iov 11 is reset at each cycle
		// ...
		// BSON end (msg_zero + bson_zero);
		ums->iovec[12].iov_base = "\0\0";	
		ums->iovec[12].iov_len = 2;	

		ums->bson_base_len = ums->iovec[3].iov_len + ums->iovec[4].iov_len + ums->iovec[5].iov_len + ums->iovec[6].iov_len +
					ums->iovec[7].iov_len + ums->iovec[8].iov_len + ums->iovec[9].iov_len + ums->iovec[10].iov_len +
					ums->iovec[12].iov_len;

		ums->base_len = ums->iovec[0].iov_len + ums->iovec[1].iov_len + ums->iovec[2].iov_len + ums->bson_base_len;
		
		ul->configured = 1;
	}

	ums = (struct uwsgi_mongodb_state *) ul->data;

	if (ums->fd == -1) {
		ums->fd = uwsgi_connect(ums->address, uwsgi.socket_timeout, 0);
	}

	if (ums->fd == -1) return -1;

	// fix the packet
	ums->bson_msg_len = len+1;
	ums->bson_len = ums->bson_base_len + len;
	ums->header.len = ums->base_len + len;
	ums->header.request_id++;

	// get milliseconds time
	ums->ts = uwsgi_micros()/1000;

	ums->iovec[11].iov_base = message;
	ums->iovec[11].iov_len = len;
	
	ssize_t ret = writev(ums->fd, ums->iovec, 13);
	if (ret <= 0) {
		close(ums->fd);
		ums->fd = -1;
		return -1;
	}

	return ret;
}

static void uwsgi_mongodblog_register() {
	uwsgi_register_logger("mongodblog", uwsgi_mongodb_logger);
}

struct uwsgi_plugin mongodblog_plugin = {

        .name = "mongodblog",
        .on_load = uwsgi_mongodblog_register,

};

