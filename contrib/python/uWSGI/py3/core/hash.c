#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

// Bernstein classic hash (this is not static as it is used by other areas)
uint32_t djb33x_hash(char *key, uint64_t keylen) {

        register uint32_t hash = 5381;
        uint64_t i;

        for (i = 0; i < keylen; i++) {
                hash = ((hash << 5) + hash) ^ key[i];
        }

        return hash;
}

// Murmur2 hash Copyright (C) Austin Appleby
// adapted from nginx
static uint32_t murmur2_hash(char *key, uint64_t keylen) {

	uint32_t  h, k;
	uint8_t *ukey = (uint8_t *) key;
	h = 0 ^ keylen;
	while (keylen >= 4) {
        	k  = ukey[0];
        	k |= ukey[1] << 8;
        	k |= ukey[2] << 16;
        	k |= ukey[3] << 24;

        	k *= 0x5bd1e995;
        	k ^= k >> 24;
        	k *= 0x5bd1e995;

        	h *= 0x5bd1e995;
        	h ^= k;

        	ukey += 4;
        	keylen -= 4;
    	}

	switch (keylen) {
		case 3:
        		h ^= key[2] << 16;
			/* fallthrough */
    		case 2:
        		h ^= key[1] << 8;
			/* fallthrough */
    		case 1:
        		h ^= key[0];
        		h *= 0x5bd1e995;
    	}

	h ^= h >> 13;
	h *= 0x5bd1e995;
	h ^= h >> 15;

	return h;
}

static uint32_t random_hash(char *key, uint64_t keylen) {
	return (uint32_t) rand();
}

/*
	not atomic, avoid its use in multithreaded modes
*/
static uint32_t rr_hash(char *key, uint64_t keylen) {
	static uint32_t rr = 0;
	uint32_t max_value = uwsgi_str_num(key, keylen);
	uint32_t ret = rr;
	rr++;
	if (rr > max_value) {
		rr = 0;
	}
	return ret;
}

struct uwsgi_hash_algo *uwsgi_hash_algo_get(char *name) {
	struct uwsgi_hash_algo *uha = uwsgi.hash_algos;
	while(uha) {
		if (!strcmp(name, uha->name)) {
			return uha;
		}
		uha = uha->next;
	}
	return NULL;
}
void uwsgi_hash_algo_register(char *name, uint32_t (*func)(char *, uint64_t)) {

	struct uwsgi_hash_algo *old_uha = NULL, *uha = uwsgi.hash_algos;
	while(uha) {
		if (!strcmp(uha->name, name)) return;
		old_uha = uha;
		uha = uha->next;
	} 

	uha = uwsgi_calloc(sizeof(struct uwsgi_hash_algo));
	uha->name = name;
	uha->func = func;
	if (old_uha) {
		old_uha->next = uha;
	}
	else {
		uwsgi.hash_algos = uha;
	}
}

void uwsgi_hash_algo_register_all() {
	uwsgi_hash_algo_register("djb33x", djb33x_hash);
	uwsgi_hash_algo_register("murmur2", murmur2_hash);
	uwsgi_hash_algo_register("random", random_hash);
	uwsgi_hash_algo_register("rand", random_hash);
	uwsgi_hash_algo_register("rr", rr_hash);
}
