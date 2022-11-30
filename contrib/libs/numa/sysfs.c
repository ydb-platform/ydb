/* Utility functions for reading sysfs values */
#define _GNU_SOURCE 1
#include <stdio.h>
#include <sys/fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include "numa.h"
#include "numaint.h"

#define SYSFS_BLOCK 4096

hidden char *sysfs_read(char *name)
{
	char *buf;
	int n;
	int fd;

	buf = malloc(SYSFS_BLOCK);
	if (!buf)
		return NULL;
	fd = open(name, O_RDONLY);
	n = read(fd, buf, SYSFS_BLOCK - 1);
	close(fd);
	if (n <= 0) {
		free(buf);
		return NULL;
	}
	buf[n] = 0;
	return buf;
}

hidden int sysfs_node_read(struct bitmask *mask, char *fmt, ...)
{
	int n, ret = 0;
	va_list ap;
	char *p, *fn, *m, *end;
	int num;

	va_start(ap, fmt);
	n = vasprintf(&fn, fmt, ap);
	va_end(ap);
	if (n < 0)
		return -1;
	p = sysfs_read(fn);
	free(fn);
	if (!p)
		return -1;

	m = p;
	do {
		num = strtol(m, &end, 0);
		if (m == end) {
			ret = -1;
			goto out;
		}
		if (num < 0) {
			ret = -2;
			goto out;
		}
		if (num >= numa_num_task_nodes()) {
			ret = -1;
			goto out;
		}
		numa_bitmask_setbit(mask, num);

		/* Continuation not supported by kernel yet. */
		m = end;
		while (isspace(*m) || *m == ',')
			m++;
	} while (isdigit(*m));
out:
	free(p);
	return ret;
}
