#include <contrib/python/uWSGI/py3/config.h>
/*

   uWSGI SPDY3 router

*/

#include "common.h"

#ifdef UWSGI_SPDY

extern struct uwsgi_http uhttp;

#include "spdy3.h"

static uint8_t spdy_h_read_control(uint8_t *header) {
	return header[0] >> 7;
}

static uint16_t spdy_h_read_version(uint8_t *header) {
	uint16_t ret = 0;
	uint8_t *ptr = (uint8_t *) &ret;
	ptr[0] = header[0] & 0x7f;
	ptr[1] = header[1];
	return ntohs(ret);
}

static uint16_t spdy_h_read_type(uint8_t *header) {
        uint16_t ret = 0;
        uint8_t *ptr = (uint8_t *) &ret;
        ptr[0] = header[2];
        ptr[1] = header[3];
        return ntohs(ret);
}


static uint8_t spdy_h_read_flags(uint8_t *header) {
	return header[4];
}

static uint32_t spdy_h_read_length(uint8_t *header) {
        uint32_t ret = 0;
        uint8_t *ptr = (uint8_t *) &ret;
        ptr[1] = header[5];
        ptr[2] = header[6];
        ptr[3] = header[7];
        return ntohl(ret);
}

static uint32_t spdy_stream_id(uint8_t *body) {
	uint32_t ret = 0;
	uint8_t *ptr = (uint8_t *) &ret;
	ptr[0] = body[0] & 0x7f;
	ptr[1] = body[1];
	ptr[2] = body[2];
	ptr[3] = body[3];
	return ntohl(ret);
}

/*
static uint32_t spdy_associated_stream_id(uint8_t *body) {
        uint32_t ret = 0;
        uint8_t *ptr = (uint8_t *) &ret;
        ptr[0] = body[4] & 0x7f;
        ptr[1] = body[5];
        ptr[2] = body[6];
        ptr[3] = body[7];
        return ntohl(ret);
}
*/

static char *spdy_translate(char *buf, uint32_t len, uint16_t *d_len) {
	uint32_t i;
	if (len == 0) return NULL;
	
	if (buf[0] == ':') {

		if (!uwsgi_strncmp(buf+1, len-1, "method", 6)) {
			*d_len = 14;
			return uwsgi_str("REQUEST_METHOD");
		}

		if (!uwsgi_strncmp(buf+1, len-1, "path", 4)) {
			*d_len = 11;
			return uwsgi_str("REQUEST_URI");
		}

		if (!uwsgi_strncmp(buf+1, len-1, "version", 7)) {
			*d_len = 15;
			return uwsgi_str("SERVER_PROTOCOL");
		}

		if (!uwsgi_strncmp(buf+1, len-1, "host", 4)) {
			*d_len = 9;
			return uwsgi_str("HTTP_HOST");
		}

		if (!uwsgi_strncmp(buf+1, len-1, "scheme", 6)) {
			*d_len = 12;
			return uwsgi_str("UWSGI_SCHEME");
		}

		return NULL;
	}
	
	if (!uwsgi_strncmp(buf, len, "content-length", 14)) {
		*d_len = 14;
		return uwsgi_str("CONTENT_LENGTH");
	}

	if (!uwsgi_strncmp(buf, len, "content-type", 12)) {
		*d_len = 12;
		return uwsgi_str("CONTENT_TYPE");
	}

	char *buf2 = uwsgi_malloc(len + 5);
	memcpy(buf2, "HTTP_", 5);
	char *ptr = buf2+5;
	for(i=0;i<len;i++) {
		if (buf[i] == '-') {
			*ptr++= '_';
		}
		else {
			*ptr++= toupper((int) buf[i]);
		}
	}

	*d_len = len+5;
	return buf2;
	
}

struct uwsgi_buffer *spdy_http_to_spdy(char *buf, size_t len, uint32_t *hh) {
	size_t i;
	size_t next = 0;
	struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
	// leave space for the number of items
	ub->pos+=4;

	int found = 0;
	// :version
	for(i=0;i<len;i++) {
		if (buf[i] == ' ') {
			if (uwsgi_buffer_append_keyval32(ub, ":version", 8, buf, i)) goto end;
			if (i+1 >= len) goto end;
			next = i+1;
			found = 1;
			break;
		}
	}		

	if (!found) goto end;

	// :status
	found = 0;
	for(i=next;i<len;i++) {
		if (buf[i] == '\r' || buf[i] == '\n') {
			if (uwsgi_buffer_append_keyval32(ub, ":status", 7, buf + next, i-next)) goto end;
			if (i+1 >= len) goto end;
			next = i + 1;
			found = 1;
			break;
		}
	}

	if (!found) goto end;

	*hh = 2;

	char *key = NULL;

	// find first header position
	for(i=next;i<len;i++) {
		if (buf[i] != '\r' && buf[i] != '\n') {
			key = buf + i; 
                        next = i;
			break;
		}
	}

	if (!key) return ub;

	uint32_t h_len = 0;
	// merge header values and ensure keys are all lowercase
	struct uwsgi_string_list *hr=NULL, *usl=NULL;
	char *line_value;
	size_t line_key_len, line_value_len;
	for(i=next;i<len;i++) {
		if (key) {
			if (buf[i] == '\r' || buf[i] == '\n') {
				char *colon = memchr(key, ':', h_len);
				if (!colon) goto end;
				// security check
				if (colon+2 >= buf+len) goto end;
				// tolower !!!
				size_t j;
				for(j=0;j<h_len;j++) {
					// don't lowercase values, only keys
					if (key[j] == ':') break;
					key[j] = tolower((int) key[j]);
				}
				line_key_len = colon - key;
				key[line_key_len] = 0;
				line_value_len = h_len - 2 - line_key_len;
				line_value = uwsgi_strncopy(colon+2, line_value_len);
				if (hr) {
					// check if we already store values for this key
					usl = uwsgi_string_list_has_item(hr, key, line_key_len);
					if (usl) {
						// we have this key, append new value
						char *oldval = usl->custom_ptr;
						usl->custom_ptr = uwsgi_concat3n(usl->custom_ptr, usl->custom, "\0", 1, line_value, line_value_len);
						usl->custom = usl->custom + 1 + line_value_len;
						free(oldval);
					}
					else {
						// this key is new
						usl = uwsgi_string_new_list(&hr, key);
						usl->custom_ptr = line_value;
						usl->custom = line_value_len;
					}
				}
				else {
					// this is first key
					usl = uwsgi_string_new_list(&hr, key);
					usl->custom_ptr = line_value;
					usl->custom = line_value_len;
				}
				key = NULL;
				h_len = 0;
			}
			else {
				h_len++;
			}
		}
		else {
			if (buf[i] != '\r' && buf[i] != '\n') {
				key = buf+i;
				h_len = 1;
			}
		}
	}

	// append all merged header lines to buffer and free memory
	struct uwsgi_string_list *ohr;
	while (hr) {
		if (uwsgi_buffer_append_keyval32(ub, hr->value, hr->len, hr->custom_ptr, hr->custom)) goto end;
		*hh+=1;
		ohr = hr;
		hr = hr->next;
		free(ohr->custom_ptr);
		free(ohr);
	}

	return ub;

end:
	uwsgi_buffer_destroy(ub);
	return NULL;
}

char *spdy_deflate_http_headers(struct http_session *, struct uwsgi_buffer *, size_t *);

// be sure to have at least 8 free bytes
static void spdy_data_header(char *buf, uint32_t len, uint32_t stream_id) {
	// stream id
        buf[3] = (uint8_t) (stream_id & 0xff);
        buf[2] = (uint8_t) ((stream_id >> 8) & 0xff);
        buf[1] = (uint8_t) ((stream_id >> 16) & 0xff);
        buf[0] = (uint8_t) ((stream_id >> 24) & 0xff);

	// FIN
	if (len == 0) {
		buf[4] = 1;
	}	

	// length
	buf[7] = (uint8_t) (len & 0xff);
	buf[6] = (uint8_t) ((len >> 8) & 0xff);
	buf[5] = (uint8_t) ((len >> 16) & 0xff);

}


// be sure to have at least 12 free bytes
static void spdy_reply_header(char *buf, uint32_t len, uint32_t stream_id) {
	buf[0] = 0x80;
	buf[1] = 0x03;
	buf[2] = 0;
	buf[3] = 0x02;

	// flags
	buf[4] = 0;

	// length
	buf[7] = (uint8_t) (len & 0xff);
	buf[6] = (uint8_t) ((len >> 8) & 0xff);
	buf[5] = (uint8_t) ((len >> 16) & 0xff);

	// stream id
	buf[11] = (uint8_t) (stream_id & 0xff);
	buf[10] = (uint8_t) ((stream_id >> 8) & 0xff);
	buf[9] = (uint8_t) ((stream_id >> 16) & 0xff);
	buf[8] = (uint8_t) ((stream_id >> 24) & 0xff);

}

// be sure to have at least 16 free bytes
void spdy_window_update(char *buf, uint32_t stream_id, uint32_t wsize) {
        buf[0] = 0x80;
        buf[1] = 0x03;
        buf[2] = 0;
        buf[3] = 0x09;

        // flags
        buf[4] = 0;

	uint32_t len = 8;

        // length
        buf[7] = (uint8_t) (len & 0xff);
        buf[6] = (uint8_t) ((len >> 8) & 0xff);
        buf[5] = (uint8_t) ((len >> 16) & 0xff);

        // stream id
        buf[11] = (uint8_t) (stream_id & 0xff);
        buf[10] = (uint8_t) ((stream_id >> 8) & 0xff);
        buf[9] = (uint8_t) ((stream_id >> 16) & 0xff);
        buf[8] = (uint8_t) ((stream_id >> 24) & 0xff);

        buf[15] = (uint8_t) (wsize & 0xff);
        buf[14] = (uint8_t) ((wsize >> 8) & 0xff);
        buf[13] = (uint8_t) ((wsize >> 16) & 0xff);
        buf[12] = (uint8_t) ((wsize >> 24) & 0xff);

}


static ssize_t http_parse_to_spdy(struct corerouter_peer *peer) {
	size_t i;
	struct uwsgi_buffer *ub = peer->in;
	struct uwsgi_buffer *out = peer->out;

	// reset the out buf (it will hold the spdy frame)
	out->pos = 0;
	peer->session->main_peer->out_pos = 0;

	// end of the stream
	if (peer->r_parser_status == 5) {
		return -1;
	}

	// send DATA frame
	if (peer->r_parser_status == 4) {
		spdy_data_header(out->buf, ub->pos, peer->sid);
		out->pos = 8;
		// no need to call append on empty values
		if (ub->pos > 0) {
			if (uwsgi_buffer_append(out, ub->buf, ub->pos)) return -1;
		}
		else {
			peer->r_parser_status = 5;
		}
		// reset the input buffer
		ub->pos = 0;
		return 1;
	}

	// try to send REPLY frame
	for(i=0;i<ub->pos;i++) {
		char c = ub->buf[i];
		if (c == '\r' && (peer->r_parser_status == 0 || peer->r_parser_status == 2)) {
                        peer->r_parser_status++;
                }
                else if (c == '\r') {
                        peer->r_parser_status = 1;
                }
                else if (c == '\n' && peer->r_parser_status == 1) {
                        peer->r_parser_status = 2;
                }
		// parsing done
                else if (c == '\n' && peer->r_parser_status == 3) {
			peer->r_parser_status = 4;
			uint32_t hh = 0;
			struct uwsgi_buffer *h_buf = spdy_http_to_spdy(ub->buf, i, &hh);
			if (!h_buf) return -1;
			// put the number of headers on front of the buffer
			h_buf->buf[3] = (uint8_t) (hh & 0xff);
        		h_buf->buf[2] = (uint8_t) ((hh >> 8) & 0xff);
        		h_buf->buf[1] = (uint8_t) ((hh >> 16) & 0xff);
        		h_buf->buf[0] = (uint8_t) ((hh >> 24) & 0xff);

			// ok now we need to deflate the buffer
			size_t cb_len = 0;
			char *compressed_buf = spdy_deflate_http_headers((struct http_session *) peer->session, h_buf, &cb_len);
			uwsgi_buffer_destroy(h_buf);
			if (!compressed_buf) {
				return -1;
			}
			// ok now we need an additional buffer
			spdy_reply_header(out->buf, 4+cb_len, peer->sid);
			out->pos = 12;
			if (uwsgi_buffer_append(out, compressed_buf, cb_len)) {
				free(compressed_buf);
				return -1;
			}
			free(compressed_buf);

			// any push request ???
			// after all of the pushed requests are sent, we have to come back here...
			// remains ?
			if (ub->pos-i > 1) {
				uint32_t remains = ub->pos-(i+1);
				if (uwsgi_buffer_append(out, "\0\0\0\0\0\0\0\0", 8)) {
					return -1;
				}		
				spdy_data_header(out->buf + (out->pos - 8), remains, peer->sid);
				if (uwsgi_buffer_append(out, ub->buf+i+1, remains)) {
					return -1;
				}		
			}
			// reset the input buffer
			ub->pos = 0;
			return 1;
		}
		else {
			peer->r_parser_status = 0;
		}
	}

	return 0;

}

ssize_t hr_instance_read_to_spdy(struct corerouter_peer *peer) {
	ssize_t len = cr_read(peer, "hr_instance_read_to_spdy()");

	// do not check for empty packet, as 0 will trigger a data frame
	len = http_parse_to_spdy(peer);
	if (len > 0) goto parsed;
	if (len < 0) {
		if (peer->r_parser_status == 5) return 0;
		return -1;
	}
	// need more data
	return 1;

parsed:
	peer->session->main_peer->out = peer->out;
	peer->session->main_peer->out_pos = 0;
        cr_write_to_main(peer, hr_ssl_write);
        return 1;
}

char *spdy_deflate_http_headers(struct http_session *hr, struct uwsgi_buffer *h_buf, size_t *dlen) {
	// calculate the amount of bytes needed for output (+30 should be enough)
	Bytef *dbuf = uwsgi_malloc(h_buf->pos+30);
	z_stream *z = &hr->spdy_z_out;
z->avail_in = h_buf->pos; z->next_in = (Bytef *) h_buf->buf; z->avail_out = h_buf->pos+30; z->next_out = dbuf; 
	if (deflate(z, Z_SYNC_FLUSH) != Z_OK) {
		free(dbuf);
		return NULL;
	}

	*dlen = z->next_out - dbuf;
	return (char *) dbuf;	
}

static ssize_t spdy_inflate_http_headers(struct http_session *hr) {

	Bytef zbuf[4096];

	uint8_t *src = (uint8_t *) hr->session.main_peer->in->buf;

	hr->spdy_z_in.avail_in = hr->spdy_control_length - 10;
	hr->spdy_z_in.next_in = src + 10;

	struct uwsgi_buffer *ub = uwsgi_buffer_new(4096);

	while(hr->spdy_z_in.avail_in > 0) {
		hr->spdy_z_in.avail_out = 4096;
		hr->spdy_z_in.next_out = zbuf;

		int ret = inflate(&hr->spdy_z_in, Z_NO_FLUSH);
		if (ret == Z_NEED_DICT) {
			inflateSetDictionary(&hr->spdy_z_in, (Bytef *) SPDY_dictionary_txt, sizeof(SPDY_dictionary_txt));
			ret = inflate(&hr->spdy_z_in, Z_NO_FLUSH);
		}
		if (ret != Z_OK) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
		size_t zlen = hr->spdy_z_in.next_out-zbuf;
		if (uwsgi_buffer_append(ub, (char *) zbuf, zlen)) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
	}

	if (ub->pos < 4) {
		uwsgi_buffer_destroy(ub);
		return -1;
	}

	uint32_t headers_num = uwsgi_be32(ub->buf);
	uint32_t i, watermark = ub->pos, pos = 4;

	struct corerouter_peer *new_peer = uwsgi_cr_peer_add(&hr->session);
	new_peer->last_hook_read = hr_instance_read_to_spdy;
	new_peer->out = uwsgi_buffer_new(uwsgi.page_size);
	// this will avoid the buffer being destroyed on the first instance write
	new_peer->out_need_free = 2;
	// leave space for uwsgi header
	new_peer->out->pos = 4;
	new_peer->sid = hr->spdy_data_stream_id;

	// leave space for header
	for(i=0;i<headers_num;i++) {
		// key
		if (pos + 4 > watermark) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
		uint32_t k_len = uwsgi_be32( ub->buf + pos);
		pos += 4;
		if (pos + k_len > watermark) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
		char *k = ub->buf + pos;
		pos += k_len;	

		// value
		if (pos + 4 > watermark) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
                uint32_t v_len = uwsgi_be32( ub->buf + pos);
                pos += 4;
                if (pos + v_len > watermark) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}
                char *v = ub->buf + pos;
                pos += v_len;	

		uint16_t nk_len = 0;
		char *cgi_name = spdy_translate(k, k_len, &nk_len);
		if (!cgi_name) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}

		if (uwsgi_buffer_append_keyval(new_peer->out, cgi_name, nk_len, v, v_len)) {
			uwsgi_buffer_destroy(ub);
			return -1;
		}

		if (!uwsgi_strncmp(cgi_name, nk_len, "HTTP_HOST", 9)) {
			if (v_len <= 0xff) {
				memcpy(new_peer->key, new_peer->out->buf + (new_peer->out->pos - v_len), v_len);
				new_peer->key_len = v_len;
			}
		}
		else if (!uwsgi_strncmp(cgi_name, nk_len, "REQUEST_URI", 11)) {
			char *path_info = new_peer->out->buf + (new_peer->out->pos - v_len);
			uint16_t path_info_len = v_len;
			char *query_string = memchr(path_info, '?', v_len);
			if (query_string) {
				query_string++;
				path_info_len = (query_string - path_info) -1;
				uint16_t query_string_len = v_len - (path_info_len + 1);
				if (uwsgi_buffer_append_keyval(new_peer->out, "QUERY_STRING", 12, query_string, query_string_len)) {
					free(cgi_name);
					uwsgi_buffer_destroy(ub);
					return -1;
				}
			}
			if (uwsgi_buffer_append_keyval(new_peer->out, "PATH_INFO", 9, path_info, path_info_len)) {
				free(cgi_name);
				uwsgi_buffer_destroy(ub);
				return -1;
			}
		}
		free(cgi_name);
	}
	uwsgi_buffer_destroy(ub);

	// find the backend node
	if (new_peer->key_len == 0) return -1;

	if (uwsgi_buffer_append_keyval(new_peer->out, "HTTPS", 5, "on", 2)) return -1;
	if (uwsgi_buffer_append_keyval(new_peer->out, "SPDY", 4, "on", 2)) return -1;
	if (uwsgi_buffer_append_keynum(new_peer->out, "SPDY.version", 12, 3)) return -1;
	if (uwsgi_buffer_append_keynum(new_peer->out, "SPDY.stream", 11, new_peer->sid)) return -1;


        struct uwsgi_corerouter *ucr = hr->session.corerouter;

        // get instance name
	if (ucr->mapper(ucr, new_peer )) return -1;

        if (new_peer->instance_address_len == 0) {
                return -1; 
        }

	uint16_t pktsize = new_peer->out->pos-4;
        // fix modifiers
        new_peer->out->buf[0] = new_peer->session->main_peer->modifier1;
        new_peer->out->buf[3] = new_peer->session->main_peer->modifier2;
        // fix pktsize
        new_peer->out->buf[1] = (uint8_t) (pktsize & 0xff);
        new_peer->out->buf[2] = (uint8_t) ((pktsize >> 8) & 0xff);

	new_peer->can_retry = 1;

	cr_connect(new_peer, hr_instance_connected);

	return 1;
}

ssize_t spdy_manage_settings(struct http_session *hs) {
	uwsgi_log("settings received !!!\n");
	return 1;
}

ssize_t spdy_manage_syn_stream(struct http_session *hr) {
	uint8_t *buf = (uint8_t *) hr->session.main_peer->in->buf;
	hr->spdy_data_stream_id = spdy_stream_id(buf);
	//uwsgi_log("SYN_STREAM received %u !!!\n", hr->spdy_data_stream_id) ;
	//uwsgi_log("associated stream %u\n", spdy_associated_stream_id(buf));
	return spdy_inflate_http_headers(hr);
}

ssize_t spdy_manage_rst_stream(struct http_session *hr) {
        uint8_t *buf = (uint8_t *) hr->session.main_peer->in->buf;
        hr->spdy_data_stream_id = spdy_stream_id(buf);
        //uwsgi_log("RST_STREAM received %u !!!\n", hr->spdy_data_stream_id) ;
	struct corerouter_peer *peer = uwsgi_cr_peer_find_by_sid(&hr->session, hr->spdy_data_stream_id);
	if (peer) {
		corerouter_close_peer(hr->session.corerouter, peer);
	}
        return 0;
}

ssize_t spdy_manage_ping(struct http_session *hr) {
	if (!hr->spdy_ping) {
		hr->spdy_ping = uwsgi_buffer_new(12);
	}
	hr->spdy_ping->pos = 0;
	if (uwsgi_buffer_append(hr->spdy_ping,  hr->session.main_peer->in->buf, 12)) return -1;
	hr->session.main_peer->out = hr->spdy_ping;
	hr->session.main_peer->out_pos = 0;
	cr_write_to_main(hr->session.main_peer, hr_ssl_write);
	//uwsgi_log("PONG\n");
	return 1;
}


/*

	read from ssl peer.

	This must be able to efficiently manage both http and spdy packets

	when the first chunk is received the spdy field of the session is checked.

	If it is a spdy packet, the SPDY parser will run...

*/



#define UWSGI_SPDY_PHASE_HEADER 0
#define UWSGI_SPDY_PHASE_CONTROL 1
#define UWSGI_SPDY_PHASE_DATA 2

ssize_t spdy_parse(struct corerouter_peer *main_peer) {
	struct corerouter_session *cs = main_peer->session;
        struct http_session *hr = (struct http_session *) cs;

	ssize_t ret = -1;

	if (!hr->spdy_initialized) {
	    	hr->spdy_z_in.zalloc = Z_NULL;
            	hr->spdy_z_in.zfree = Z_NULL;
            	hr->spdy_z_in.opaque = Z_NULL;
            	if (inflateInit(&hr->spdy_z_in) != Z_OK) {
            		return -1;
            	}
            	hr->spdy_z_out.zalloc = Z_NULL;
            	hr->spdy_z_out.zfree = Z_NULL;
            	hr->spdy_z_out.opaque = Z_NULL;
            	if (deflateInit(&hr->spdy_z_out, Z_DEFAULT_COMPRESSION) != Z_OK) {
            		return -1;
            	}
		if (deflateSetDictionary(&hr->spdy_z_out, (Bytef *) SPDY_dictionary_txt, sizeof(SPDY_dictionary_txt)) != Z_OK) {
            		return -1;
            	}
		cs->can_keepalive = 1;
		hr->spdy_initialized = 1;

		hr->spdy_phase = UWSGI_SPDY_PHASE_HEADER;
		hr->spdy_need = 8;

		main_peer->out = uhttp.spdy3_settings;
		main_peer->out->pos = uhttp.spdy3_settings_size;
		main_peer->out_pos = 0;
		cr_write_to_main(main_peer, hr_ssl_write);
		return 1;
	}

	for(;;) {
		size_t len = main_peer->in->pos;
		if (len == 0) {
			return 1;
		}
		uint8_t *buf = (uint8_t *) main_peer->in->buf;
		//uwsgi_log("%d bytes available\n", len);
		switch(hr->spdy_phase) {
			case UWSGI_SPDY_PHASE_HEADER:
				if (len >= hr->spdy_need) {
					hr->spdy_frame_type = spdy_h_read_control(buf);
					if (hr->spdy_frame_type) {
						hr->spdy_control_version = spdy_h_read_version(buf);
						hr->spdy_control_type =  spdy_h_read_type(buf);
						hr->spdy_control_flags = spdy_h_read_flags(buf);
						hr->spdy_control_length = spdy_h_read_length(buf);
						hr->spdy_phase = UWSGI_SPDY_PHASE_CONTROL;
						hr->spdy_need = hr->spdy_control_length;
						//uwsgi_log("now i need %llu bytes for type %u\n", (unsigned long long) hr->spdy_need, hr->spdy_control_type);
					}
					else {
						hr->spdy_phase = UWSGI_SPDY_PHASE_DATA;
						hr->spdy_data_stream_id = spdy_stream_id(buf);
						hr->spdy_control_length = spdy_h_read_length(buf);
						hr->spdy_need = hr->spdy_control_length;
						//uwsgi_log("need %llu bytes for stream_id %lu\n", (unsigned long long) hr->spdy_need, hr->spdy_data_stream_id);
					}
					if (uwsgi_buffer_decapitate(main_peer->in, 8)) return -1;
					continue;
				}
				return 1;
			case UWSGI_SPDY_PHASE_CONTROL:
				if (len >= hr->spdy_need) {
					switch(hr->spdy_control_type) {
						// SYN_STREAM
						case 1:
							ret = spdy_manage_syn_stream(hr);
							if (ret == 0) goto goon;
							if (ret < 0) return -1;
							goto newframe;
						// RST_STREAM
						case 3:
							ret = spdy_manage_rst_stream(hr);
							if (ret == 0) goto goon;
							if (ret < 0) return -1;
							goto newframe;
						case 4:
							//uwsgi_log("settings request...\n");
							break;
						case 6:
							ret = spdy_manage_ping(hr);
                                                        if (ret == 0) goto goon;
							if (ret < 0) return -1;
                                                        goto newframe;
							break;
						case 7:
							// uwsgi_log("GO AWAY...\n");
							break;
						case 9:
							// uwsgi_log("window update...\n");
							break;
						default:
							uwsgi_log("i do not know how to manage type %u\n", hr->spdy_control_type);
							break;
					}
goon:
					hr->spdy_phase = UWSGI_SPDY_PHASE_HEADER;
					hr->spdy_need = 8;
					if (uwsgi_buffer_decapitate(main_peer->in, hr->spdy_control_length)) return -1;
					continue;
				}
				return 1;
			case UWSGI_SPDY_PHASE_DATA:
				if (len >= hr->spdy_need) {
					struct corerouter_peer *peer = uwsgi_cr_peer_find_by_sid(&hr->session, hr->spdy_data_stream_id);
					if (!peer) {
						return -1;
					}

					peer->out->pos = 0;
					if (uwsgi_buffer_append(peer->out, main_peer->in->buf, hr->spdy_need)) return -1;
                			peer->out_pos = 0;
					hr->spdy_update_window = hr->spdy_data_stream_id;
                			cr_write_to_backend(peer, hr_instance_write);
					ret = 1;
					goto newframe;
				}
				return 1;
			default:
				return -1;
		}
	}

	return -1;

newframe:
	hr->spdy_phase = UWSGI_SPDY_PHASE_HEADER;
        hr->spdy_need = 8;
        if (uwsgi_buffer_decapitate(main_peer->in, hr->spdy_control_length)) return -1;
	return ret;
	
}


int uwsgi_spdy_npn(SSL *ssl, const unsigned char **data, unsigned int *len, void *arg) {
        //*data = (const unsigned char *) "\x06spdy/3\x06spdy/2\x08http/1.1\x08http/1.0";
        *data = (const unsigned char *) "\x06spdy/3\x08http/1.1\x08http/1.0";
        *len = strlen((const char *) *data);
        return SSL_TLSEXT_ERR_OK;
}

void uwsgi_spdy_info_cb(SSL const *ssl, int where, int ret) {
        if (where & SSL_CB_HANDSHAKE_DONE) {
                const unsigned char * proto = NULL;
                unsigned len = 0;
                SSL_get0_next_proto_negotiated(ssl, &proto, &len);
                if (len == 6) {
			if (!memcmp(proto, "spdy/3", 6)) {
				//uwsgi_log("SPDY 3 !!!\n");
                        	struct http_session *hr = SSL_get_ex_data(ssl, uhttp.spdy_index);
				hr->spdy = 3;
				//hr->spdy_hook = hr_recv_spdy_control_frame;
                	}
			else if (!memcmp(proto, "spdy/2", 6)) {
				//uwsgi_log("SPDY 2 !!!\n");
                        	struct http_session *hr = SSL_get_ex_data(ssl, uhttp.spdy_index);
				hr->spdy = 2;
				//uwsgi_log("SPDY/2\n");
				//hr->spdy_hook = hr_recv_spdy_control_frame;
			}
		}
#if OPENSSL_VERSION_NUMBER < 0x10100000L
                if (ssl->s3) {
                        ssl->s3->flags |= SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS;
                }
#endif
        }
}

#endif
