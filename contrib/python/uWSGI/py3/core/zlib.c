#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

char gzheader[10] = { 0x1f, 0x8b, Z_DEFLATED, 0, 0, 0, 0, 0, 0, 3 };

extern struct uwsgi_server uwsgi;

char *uwsgi_gzip_chunk(z_stream *z, uint32_t *crc32, char *buf, size_t len, size_t *dlen) {
	uwsgi_crc32(crc32, buf, len);
	return uwsgi_deflate(z, buf, len, dlen);
}

struct uwsgi_buffer *uwsgi_gzip(char *buf, size_t len) {
	z_stream z;
	struct uwsgi_buffer *ub = NULL;
	uint32_t gzip_crc32 = 0;
	char *gzipped = NULL;
	char *gzipped0 = NULL;
	size_t dlen = 0;
	size_t dlen0 = 0;

	uwsgi_crc32(&gzip_crc32, NULL, 0);
	if (uwsgi_deflate_init(&z, NULL, 0)) return NULL; 
	uwsgi_crc32(&gzip_crc32, buf, len);

	gzipped = uwsgi_deflate(&z, buf, len, &dlen);
	if (!gzipped) goto end;
	gzipped0 = uwsgi_deflate(&z, NULL, 0, &dlen0);
	if (!gzipped0) goto end;

	ub = uwsgi_buffer_new(10 + dlen + dlen0 + 8);
	if (uwsgi_buffer_append(ub, gzheader, 10)) goto end;
	if (uwsgi_buffer_append(ub, gzipped, dlen)) goto end;
	if (uwsgi_buffer_append(ub, gzipped0, dlen0)) goto end;
	if (uwsgi_buffer_u32le(ub, gzip_crc32)) goto end;
        if (uwsgi_buffer_u32le(ub, len)) goto end;
end:
	if (gzipped) free(gzipped);
	if (gzipped0) free(gzipped0);
	deflateEnd(&z);
	return ub;
}

struct uwsgi_buffer *uwsgi_zlib_decompress(char *buf, size_t len) {
        z_stream z;

	z.zalloc = Z_NULL;
        z.zfree = Z_NULL;
        z.opaque = Z_NULL;
        if (inflateInit(&z) != Z_OK) {
		return NULL;
        }

        struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
	unsigned char out[8192];

	z.next_in = (unsigned char *)buf;
	z.avail_in = len;
	z.next_out = out;

	do {
		z.avail_out = 8192;
		z.next_out = out;
		int ret = inflate(&z, Z_NO_FLUSH);
		if (ret == Z_STREAM_ERROR || ret == Z_NEED_DICT || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
			uwsgi_buffer_destroy(ub);
			ub = NULL;
			goto end;			
		}
		if (uwsgi_buffer_append(ub, (char *)out, 8192 - z.avail_out)) {
			uwsgi_buffer_destroy(ub);
			ub = NULL;
			goto end;			
		}
	}
	while (z.avail_out == 0);
end:
        inflateEnd(&z);
        return ub;
}


int uwsgi_deflate_init(z_stream *z, char *dict, size_t dict_len) {
        z->zalloc = Z_NULL;
        z->zfree = Z_NULL;
        z->opaque = Z_NULL;
        if (deflateInit2(z, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY) != Z_OK) {
	//if (deflateInit(z, Z_DEFAULT_COMPRESSION)) {
		return -1;
	}
	if (dict && dict_len) {
                if (deflateSetDictionary(z, (Bytef *) dict, dict_len) != Z_OK) {
                        return -1;
                }
	}
	return 0;
}

int uwsgi_inflate_init(z_stream *z, char *dict, size_t dict_len) {
        z->zalloc = Z_NULL;
        z->zfree = Z_NULL;
        z->opaque = Z_NULL;
        if (inflateInit2(z, 16+MAX_WBITS) != Z_OK) {
                return -1;
        }
        if (dict && dict_len) {
                if (inflateSetDictionary(z, (Bytef *) dict, dict_len) != Z_OK) {
                        return -1;
                }
        }
        return 0;
}

int uwsgi_gzip_prepare(z_stream *z, char *dict, size_t dict_len, uint32_t *crc32) {
	uwsgi_crc32(crc32, NULL, 0);
	if (uwsgi_deflate_init(z, NULL, 0)) return -1;
	return 0;
}

// fix and free a gzip stream
int uwsgi_gzip_fix(z_stream *z, uint32_t crc32, struct uwsgi_buffer *ub, size_t len) {
	size_t dlen0 = 0;
	char *gzipped0 = uwsgi_deflate(z, NULL, 0, &dlen0);
        if (!gzipped0) goto end;
	if (uwsgi_buffer_append(ub, gzipped0, dlen0)) goto end;
	free(gzipped0);
	if (uwsgi_buffer_u32le(ub, crc32)) goto end2;
        if (uwsgi_buffer_u32le(ub, len)) goto end2;
	deflateEnd(z);
	return 0;
end:
	if (gzipped0) free(gzipped0);
end2:
	deflateEnd(z);
	return -1;
}


char *uwsgi_deflate(z_stream *z, char *buf, size_t len, size_t *dlen) {

	// calculate the amount of bytes needed for output (+30 should be enough)
        Bytef *dbuf = uwsgi_malloc(len+30);
	z->avail_in = len;
	z->next_in = (Bytef *) buf;
	z->avail_out = len+30;
	z->next_out = dbuf;

	if (len > 0) {
		if (deflate(z, Z_SYNC_FLUSH) != Z_OK) {
			free(dbuf);
			return NULL;
		}
	}
	else {
        	if (deflate(z, Z_FINISH) != Z_STREAM_END) {
                	free(dbuf);
                	return NULL;
		}
		deflateEnd(z);
        }

        *dlen = (z->next_out - dbuf);
        return (char *) dbuf;
}

void uwsgi_crc32(uint32_t *ctx, char *buf, size_t len) {
	if (!buf) {
		*ctx = crc32(*ctx, Z_NULL, 0);
	}
	else {
		*ctx = crc32(*ctx, (const Bytef *) buf, len);
	}
}
