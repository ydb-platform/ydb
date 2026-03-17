#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

struct uwsgi_buffer *uwsgi_buffer_new(size_t len) {
#ifdef UWSGI_DEBUG_BUFFER
	uwsgi_log("[uwsgi-buffer] allocating a new buffer of %llu\n", (unsigned long long) len);
#endif
	struct uwsgi_buffer *ub = uwsgi_calloc(sizeof(struct uwsgi_buffer));

	if (len) {
		ub->buf = uwsgi_malloc(len);
		ub->len = len;
	}
	return ub;

}

int uwsgi_buffer_fix(struct uwsgi_buffer *ub, size_t len) {
	if (ub->limit > 0 && len > ub->limit)
		return -1;
	if (ub->len < len) {
		char *new_buf = realloc(ub->buf, len);
		if (!new_buf) {
			uwsgi_error("uwsgi_buffer_fix()");
			return -1;
		}
		ub->buf = new_buf;
		ub->len = len;
	}
	return 0;
}

int uwsgi_buffer_ensure(struct uwsgi_buffer *ub, size_t len) {
	size_t remains = ub->len - ub->pos;
	if (remains < len) {
		size_t new_len = ub->len + (len - remains);
		if (ub->limit > 0 && new_len > ub->limit) {
			new_len = ub->limit;
			if (new_len == ub->len)
				return -1;
		}
		char *new_buf = realloc(ub->buf, new_len);
		if (!new_buf) {
			uwsgi_error("uwsgi_buffer_ensure()");
			return -1;
		}
		ub->buf = new_buf;
		ub->len = new_len;
	}
	return 0;
}

int uwsgi_buffer_insert(struct uwsgi_buffer *ub, size_t pos, char *buf, size_t len) {
	size_t to_move = (ub->pos-pos);
	if (uwsgi_buffer_ensure(ub, len)) return -1;
	memmove(ub->buf+pos+len, ub->buf+pos, to_move);
	memcpy(ub->buf+pos, buf, len);
	ub->pos += len;
	return 0;
}

int uwsgi_buffer_insert_chunked(struct uwsgi_buffer *ub, size_t pos, size_t len) {
	// 0xFFFFFFFFFFFFFFFF\r\n\0
	char chunked[19];
	int ret = snprintf(chunked, 19, "%X\r\n", (unsigned int) len);
        if (ret <= 0 || ret >= 19) {
                return -1;
        }
	return uwsgi_buffer_insert(ub, pos, chunked, ret);
}

int uwsgi_buffer_append_chunked(struct uwsgi_buffer *ub, size_t len) {
        // 0xFFFFFFFFFFFFFFFF\r\n\0
        char chunked[19];
        int ret = snprintf(chunked, 19, "%X\r\n", (unsigned int) len);
        if (ret <= 0 || ret >= 19) {
                return -1;
        }
        return uwsgi_buffer_append(ub, chunked, ret);
}


int uwsgi_buffer_decapitate(struct uwsgi_buffer *ub, size_t len) {
	if (len > ub->pos) return -1;
	memmove(ub->buf, ub->buf + len, ub->pos-len);
	ub->pos = ub->pos-len;
	return 0;
}

int uwsgi_buffer_byte(struct uwsgi_buffer *ub, char byte) {
	return uwsgi_buffer_append(ub, &byte, 1);
}

int uwsgi_buffer_u8(struct uwsgi_buffer *ub, uint8_t u8) {
	return uwsgi_buffer_append(ub, (char *) &u8, 1);
}

int uwsgi_buffer_append(struct uwsgi_buffer *ub, char *buf, size_t len) {

	size_t remains = ub->len - ub->pos;

	if (len > remains) {
		size_t chunk_size = UMAX(len, (size_t) uwsgi.page_size);
		if (ub->limit > 0 && ub->len + chunk_size > ub->limit) {
			// retry with another minimal size
			if (len < (size_t) uwsgi.page_size) {
				chunk_size = len;
			}
			if (ub->len + chunk_size > ub->limit)
				return -1;
		}
		char *new_buf = realloc(ub->buf, ub->len + chunk_size);
		if (!new_buf) {
			uwsgi_error("uwsgi_buffer_append()");
			return -1;
		}
		ub->buf = new_buf;
		ub->len += chunk_size;
	}

	memcpy(ub->buf + ub->pos, buf, len);
	ub->pos += len;
	return 0;
}

int uwsgi_buffer_append_json(struct uwsgi_buffer *ub, char *buf, size_t len) {
	// need to escape \ and "
	size_t i;
	for(i=0;i<len;i++) {
		if (buf[i] == '\t') {
			if (uwsgi_buffer_append(ub, "\\t", 2)) return -1;
		}
		else if (buf[i] == '\n') {
			if (uwsgi_buffer_append(ub, "\\n", 2)) return -1;
		}
		else if (buf[i] == '\r') {
			if (uwsgi_buffer_append(ub, "\\r", 2)) return -1;
		}
		else if (buf[i] == '"') {
			if (uwsgi_buffer_append(ub, "\\\"", 2)) return -1;
		}
		else if (buf[i] == '\\') {
			if (uwsgi_buffer_append(ub, "\\\\", 2)) return -1;
		}
		else {
			if (uwsgi_buffer_append(ub, buf+i, 1)) return -1;
		}
	}
	return 0;
}

int uwsgi_buffer_u16le(struct uwsgi_buffer *ub, uint16_t num) {
	uint8_t buf[2];
	buf[0] = (uint8_t) (num & 0xff);
        buf[1] = (uint8_t) ((num >> 8) & 0xff);
	return uwsgi_buffer_append(ub, (char *) buf, 2);
}

int uwsgi_buffer_u16be(struct uwsgi_buffer *ub, uint16_t num) {
        uint8_t buf[2];
        buf[1] = (uint8_t) (num & 0xff);
        buf[0] = (uint8_t) ((num >> 8) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 2);
}

int uwsgi_buffer_u24be(struct uwsgi_buffer *ub, uint32_t num) {
        uint8_t buf[3];
        buf[2] = (uint8_t) (num & 0xff);
        buf[1] = (uint8_t) ((num >> 8) & 0xff);
        buf[0] = (uint8_t) ((num >> 16) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 3);
}

int uwsgi_buffer_u32be(struct uwsgi_buffer *ub, uint32_t num) {
        uint8_t buf[4];
        buf[3] = (uint8_t) (num & 0xff);
        buf[2] = (uint8_t) ((num >> 8) & 0xff);
        buf[1] = (uint8_t) ((num >> 16) & 0xff);
        buf[0] = (uint8_t) ((num >> 24) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 4);
}

int uwsgi_buffer_f32be(struct uwsgi_buffer *ub, float num) {
        uint8_t buf[4];
	uint32_t *pnum = (uint32_t *) &num;
        buf[3] = (uint8_t) (*pnum & 0xff);
        buf[2] = (uint8_t) ((*pnum >> 8) & 0xff);
        buf[1] = (uint8_t) ((*pnum >> 16) & 0xff);
        buf[0] = (uint8_t) ((*pnum >> 24) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 4);
}

int uwsgi_buffer_u32le(struct uwsgi_buffer *ub, uint32_t num) {
        uint8_t buf[4];
        buf[0] = (uint8_t) (num & 0xff);
        buf[1] = (uint8_t) ((num >> 8) & 0xff);
        buf[2] = (uint8_t) ((num >> 16) & 0xff);
        buf[3] = (uint8_t) ((num >> 24) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 4);
}

int uwsgi_buffer_u64be(struct uwsgi_buffer *ub, uint64_t num) {
        uint8_t buf[8];
        buf[7] = (uint8_t) (num & 0xff);
        buf[6] = (uint8_t) ((num >> 8) & 0xff);
        buf[5] = (uint8_t) ((num >> 16) & 0xff);
        buf[4] = (uint8_t) ((num >> 24) & 0xff);
        buf[3] = (uint8_t) ((num >> 32) & 0xff);
        buf[2] = (uint8_t) ((num >> 40) & 0xff);
        buf[1] = (uint8_t) ((num >> 48) & 0xff);
        buf[0] = (uint8_t) ((num >> 56) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 8);
}

int uwsgi_buffer_u64le(struct uwsgi_buffer *ub, uint64_t num) {
	uint8_t buf[8];
        buf[0] = (uint8_t) (num & 0xff);
        buf[1] = (uint8_t) ((num >> 8) & 0xff);
        buf[2] = (uint8_t) ((num >> 16) & 0xff);
        buf[3] = (uint8_t) ((num >> 24) & 0xff);
	buf[4] = (uint8_t) ((num >> 32) & 0xff);
        buf[5] = (uint8_t) ((num >> 40) & 0xff);
        buf[6] = (uint8_t) ((num >> 48) & 0xff);
        buf[7] = (uint8_t) ((num >> 56) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 8);
}


int uwsgi_buffer_f64be(struct uwsgi_buffer *ub, double num) {
        uint8_t buf[8];
	uint64_t *pnum = (uint64_t *) &num;
        buf[7] = (uint8_t) (*pnum & 0xff);
        buf[6] = (uint8_t) ((*pnum >> 8) & 0xff);
        buf[5] = (uint8_t) ((*pnum >> 16) & 0xff);
        buf[4] = (uint8_t) ((*pnum >> 24) & 0xff);
        buf[3] = (uint8_t) ((*pnum >> 32) & 0xff);
        buf[2] = (uint8_t) ((*pnum >> 40) & 0xff);
        buf[1] = (uint8_t) ((*pnum >> 48) & 0xff);
        buf[0] = (uint8_t) ((*pnum >> 56) & 0xff);
        return uwsgi_buffer_append(ub, (char *) buf, 8);
}

int uwsgi_buffer_append_ipv4(struct uwsgi_buffer *ub, void *addr) {
	char ip[INET_ADDRSTRLEN];
	if (!inet_ntop(AF_INET, addr, ip, INET_ADDRSTRLEN)) {
		uwsgi_error("uwsgi_buffer_append_ipv4() -> inet_ntop()");
		return -1;
	}
	return uwsgi_buffer_append(ub, ip, strlen(ip));
}


int uwsgi_buffer_num64(struct uwsgi_buffer *ub, int64_t num) {
	char buf[sizeof(UMAX64_STR)+1];
	int ret = snprintf(buf, sizeof(UMAX64_STR)+1, "%lld", (long long) num);
	if (ret <= 0 || ret >= (int) (sizeof(UMAX64_STR)+1)) {
		return -1;
	}
	return uwsgi_buffer_append(ub, buf, ret);
}

int uwsgi_buffer_append_keyval(struct uwsgi_buffer *ub, char *key, uint16_t keylen, char *val, uint16_t vallen) {
	if (uwsgi_buffer_u16le(ub, keylen)) return -1;
	if (uwsgi_buffer_append(ub, key, keylen)) return -1;
	if (uwsgi_buffer_u16le(ub, vallen)) return -1;
	return uwsgi_buffer_append(ub, val, vallen);
}

int uwsgi_buffer_append_keyval32(struct uwsgi_buffer *ub, char *key, uint32_t keylen, char *val, uint32_t vallen) {
        if (uwsgi_buffer_u32be(ub, keylen)) return -1;
        if (uwsgi_buffer_append(ub, key, keylen)) return -1;
        if (uwsgi_buffer_u32be(ub, vallen)) return -1;
        return uwsgi_buffer_append(ub, val, vallen);
}

int uwsgi_buffer_append_keynum(struct uwsgi_buffer *ub, char *key, uint16_t keylen, int64_t num) {
	char buf[sizeof(UMAX64_STR)+1];
        int ret = snprintf(buf, (sizeof(UMAX64_STR)+1), "%lld", (long long) num);
        if (ret <= 0 || ret >= (int) (sizeof(UMAX64_STR)+1)) {
                return -1;
        }
	if (uwsgi_buffer_u16le(ub, keylen)) return -1;
	if (uwsgi_buffer_append(ub, key, keylen)) return -1;
	if (uwsgi_buffer_u16le(ub, ret)) return -1;
	return uwsgi_buffer_append(ub, buf, ret);
}

int uwsgi_buffer_append_valnum(struct uwsgi_buffer *ub, int64_t num) {
        char buf[sizeof(UMAX64_STR)+1];
        int ret = snprintf(buf, (sizeof(UMAX64_STR)+1), "%lld", (long long) num);
        if (ret <= 0 || ret >= (int) (sizeof(UMAX64_STR)+1)) {
                return -1;
        }
        if (uwsgi_buffer_u16le(ub, ret)) return -1;
        return uwsgi_buffer_append(ub, buf, ret);
}


int uwsgi_buffer_append_keyipv4(struct uwsgi_buffer *ub, char *key, uint16_t keylen, void *addr) {
        if (uwsgi_buffer_u16le(ub, keylen)) return -1;
        if (uwsgi_buffer_append(ub, key, keylen)) return -1;
        if (uwsgi_buffer_u16le(ub, 15)) return -1;
	char *ptr = ub->buf + (ub->pos - 2);
        if (uwsgi_buffer_append_ipv4(ub, addr)) return -1;
	// fix the size
	*ptr = ((ub->buf+ub->pos) - (ptr+2));
	return 0;
}

int uwsgi_buffer_append_base64(struct uwsgi_buffer *ub, char *s, size_t len) {
	size_t b64_len = 0;
	char *b64 = uwsgi_base64_encode(s, len, &b64_len);
	if (!b64) return -1;
	int ret = uwsgi_buffer_append(ub, b64, b64_len);
	free(b64);
	return ret;
}


void uwsgi_buffer_destroy(struct uwsgi_buffer *ub) {
#ifdef UWSGI_DEBUG_BUFFER
	uwsgi_log("[uwsgi-buffer] destroying buffer of %llu bytes\n", (unsigned long long) ub->len);
	if (ub->freed) {
		uwsgi_log("[uwsgi-buffer][BUG] buffer at %p already destroyed !!!\n", ub);
	}
	ub->freed = 1;
#endif
	if (ub->buf)
		free(ub->buf);
	free(ub);
}

ssize_t uwsgi_buffer_write_simple(struct wsgi_request *wsgi_req, struct uwsgi_buffer *ub) {
	size_t remains = ub->pos;
	while(remains) {
		ssize_t len = write(wsgi_req->fd, ub->buf + (ub->pos - remains), remains);
		if (len <= 0) {
			return len;
		}
		remains -= len;
	}

	return ub->pos;
}

int uwsgi_buffer_send(struct uwsgi_buffer *ub, int fd) {
	size_t remains = ub->pos;
	char *ptr = ub->buf;

	while (remains > 0) {
		int ret = uwsgi_waitfd_write(fd, uwsgi.socket_timeout);
		if (ret > 0) {
			ssize_t len = write(fd, ptr, remains);
			if (len > 0) {
				ptr += len;
				remains -= len;
			}
			else if (len == 0) {
				return -1;
			}
			else {
				uwsgi_error("uwsgi_buffer_send()");
				return -1;
			}
		}
		else if (ret == 0) {
			uwsgi_log("timeout while sending buffer !!!\n");
			return -1;
		}
		else {
			return -1;
		}
	}

	return 0;
}

int uwsgi_buffer_set_uh(struct uwsgi_buffer *ub, uint8_t modifier1, uint8_t modifier2) {
	if (ub->pos < 4) return -1;
	ub->buf[0] = modifier1;
	ub->buf[1] = (uint8_t) ((ub->pos - 4) & 0xff);
	ub->buf[2] = (uint8_t) (((ub->pos - 4) >> 8) & 0xff);
	ub->buf[3] = modifier2;
	return 0;
}

struct uwsgi_buffer *uwsgi_buffer_from_file(char *filename) {
	struct stat st;
        int fd = open(filename, O_RDONLY);
        if (fd < 0) {
		return NULL;
        }

        if (fstat(fd, &st)) {
                close(fd);
		return NULL;
        }

        struct uwsgi_buffer *ub = uwsgi_buffer_new(st.st_size);

        ssize_t len = read(fd, ub->buf, st.st_size);
	close(fd);
        if (len != st.st_size) {
		uwsgi_buffer_destroy(ub);
		return NULL;
        }

	ub->pos = len;
	return ub;
}

void uwsgi_buffer_map(struct uwsgi_buffer *ub, char *buf, size_t len) {
	if (ub->buf) {
		free(ub->buf);
	}
	ub->buf = buf;
	ub->pos = len;
	ub->len = len;
}
