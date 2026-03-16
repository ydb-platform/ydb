#include <contrib/python/uWSGI/py3/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

void uwsgi_init_queue() {
	if (!uwsgi.queue_blocksize)
		uwsgi.queue_blocksize = 8192;

	if ((uwsgi.queue_blocksize * uwsgi.queue_size) % uwsgi.page_size != 0) {
		uwsgi_log("invalid queue size/blocksize %llu: must be a multiple of memory page size (%d bytes)\n", (unsigned long long) uwsgi.queue_blocksize, uwsgi.page_size);
		exit(1);
	}



	if (uwsgi.queue_store) {
		uwsgi.queue_filesize = uwsgi.queue_blocksize * uwsgi.queue_size + 16;
		int queue_fd;
		struct stat qst;

		if (stat(uwsgi.queue_store, &qst)) {
			uwsgi_log("creating a new queue store file: %s\n", uwsgi.queue_store);
			queue_fd = open(uwsgi.queue_store, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
			if (queue_fd >= 0) {
				// fill the queue store
				if (ftruncate(queue_fd, uwsgi.queue_filesize)) {
					uwsgi_log("ftruncate()");
					exit(1);
				}
			}
		}
		else {
			if ((size_t) qst.st_size != uwsgi.queue_filesize || !S_ISREG(qst.st_mode)) {
				uwsgi_log("invalid queue store file. Please remove it or fix queue blocksize/items to match its size\n");
				exit(1);
			}
			queue_fd = open(uwsgi.queue_store, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
			uwsgi_log("recovered queue from backing store file: %s\n", uwsgi.queue_store);
		}

		if (queue_fd < 0) {
			uwsgi_error_open(uwsgi.queue_store);
			exit(1);
		}
		uwsgi.queue = mmap(NULL, uwsgi.queue_filesize, PROT_READ | PROT_WRITE, MAP_SHARED, queue_fd, 0);

		// fix header
		uwsgi.queue_header = uwsgi.queue;
		uwsgi.queue += 16;
		close(queue_fd);
	}
	else {
		uwsgi.queue = mmap(NULL, (uwsgi.queue_blocksize * uwsgi.queue_size) + 16, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
		// fix header
		uwsgi.queue_header = uwsgi.queue;
		uwsgi.queue += 16;
		uwsgi.queue_header->pos = 0;
		uwsgi.queue_header->pull_pos = 0;
	}
	if (uwsgi.queue == MAP_FAILED) {
		uwsgi_error("mmap()");
		exit(1);
	}



	uwsgi.queue_lock = uwsgi_rwlock_init("queue");

	uwsgi_log("*** Queue subsystem initialized: %luMB preallocated ***\n", (uwsgi.queue_blocksize * uwsgi.queue_size) / (1024 * 1024));
}

char *uwsgi_queue_get(uint64_t index, uint64_t * size) {

	struct uwsgi_queue_item *uqi;
	char *ptr = (char *) uwsgi.queue;

	if (index >= uwsgi.queue_size)
		return NULL;

	ptr = ptr + (uwsgi.queue_blocksize * index);

	uqi = (struct uwsgi_queue_item *) ptr;

	*size = uqi->size;

	return ptr + sizeof(struct uwsgi_queue_item);

}


char *uwsgi_queue_pop(uint64_t * size) {

	struct uwsgi_queue_item *uqi;
	char *ptr = (char *) uwsgi.queue;

	if (uwsgi.queue_header->pos == 0) {
		uwsgi.queue_header->pos = uwsgi.queue_size - 1;
	}
	else {
		uwsgi.queue_header->pos--;
	}

	ptr = ptr + (uwsgi.queue_blocksize * uwsgi.queue_header->pos);
	uqi = (struct uwsgi_queue_item *) ptr;

	if (!uqi->size)
		return NULL;

	*size = uqi->size;
	// remove item
	uqi->size = 0;

	return ptr + sizeof(struct uwsgi_queue_item);
}


char *uwsgi_queue_pull(uint64_t * size) {

	struct uwsgi_queue_item *uqi;
	char *ptr = (char *) uwsgi.queue;

	ptr = ptr + (uwsgi.queue_blocksize * uwsgi.queue_header->pull_pos);
	uqi = (struct uwsgi_queue_item *) ptr;

	if (!uqi->size)
		return NULL;

	*size = uqi->size;

	uwsgi.queue_header->pull_pos++;

	if (uwsgi.queue_header->pull_pos >= uwsgi.queue_size)
		uwsgi.queue_header->pull_pos = 0;

	// remove item
	uqi->size = 0;

	return ptr + sizeof(struct uwsgi_queue_item);

}

int uwsgi_queue_push(char *message, uint64_t size) {

	struct uwsgi_queue_item *uqi;
	char *ptr = (char *) uwsgi.queue;

	if (size > uwsgi.queue_blocksize - sizeof(struct uwsgi_queue_item))
		return 0;

	if (!size)
		return 0;

	ptr = ptr + (uwsgi.queue_blocksize * uwsgi.queue_header->pos);
	uqi = (struct uwsgi_queue_item *) ptr;

	ptr += sizeof(struct uwsgi_queue_item);

	uqi->size = size;
	uqi->ts = uwsgi_now();
	memcpy(ptr, message, size);

	uwsgi.queue_header->pos++;

	if (uwsgi.queue_header->pos >= uwsgi.queue_size)
		uwsgi.queue_header->pos = 0;

	return 1;
}

int uwsgi_queue_set(uint64_t pos, char *message, uint64_t size) {

	struct uwsgi_queue_item *uqi;
	char *ptr = (char *) uwsgi.queue;

	if (size > uwsgi.queue_blocksize + sizeof(struct uwsgi_queue_item))
		return 0;

	if (!size)
		return 0;

	if (pos >= uwsgi.queue_size)
		return 0;

	ptr = ptr + (uwsgi.queue_blocksize * pos);
	uqi = (struct uwsgi_queue_item *) ptr;

	ptr += sizeof(struct uwsgi_queue_item);

	uqi->size = size;
	uqi->ts = uwsgi_now();
	memcpy(ptr, message, size);

	return 1;
}
