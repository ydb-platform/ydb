/* Discover distances
   Copyright (C) 2005 Andi Kleen, SuSE Labs.

   libnuma is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; version
   2.1.

   libnuma is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should find a copy of v2.1 of the GNU Lesser General Public License
   somewhere on your Linux system; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

   All calls are undefined when numa_available returns an error. */
#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "numa.h"
#include "numaint.h"

static int distance_numnodes;
static int *distance_table;

static void parse_numbers(char *s, int *iptr)
{
	int i, d, j;
	char *end;
	int maxnode = numa_max_node();
	int numnodes = 0;

	for (i = 0; i <= maxnode; i++)
		if (numa_bitmask_isbitset(numa_nodes_ptr, i))
			numnodes++;

	for (i = 0, j = 0; i <= maxnode; i++, j++) {
		d = strtoul(s, &end, 0);
		/* Skip unavailable nodes */
		while (j<=maxnode && !numa_bitmask_isbitset(numa_nodes_ptr, j))
			j++;
		if (s == end)
			break;
		*(iptr+j) = d;
		s = end;
	}
}

static int read_distance_table(void)
{
	int nd, len;
	char *line = NULL;
	size_t linelen = 0;
	int maxnode = numa_max_node() + 1;
	int *table = NULL;
	int err = -1;

	for (nd = 0;; nd++) {
		char fn[100];
		FILE *dfh;
		sprintf(fn, "/sys/devices/system/node/node%d/distance", nd);
		dfh = fopen(fn, "r");
		if (!dfh) {
			if (errno == ENOENT)
				err = 0;
			if (!err && nd<maxnode)
				continue;
			else
				break;
		}
		len = getdelim(&line, &linelen, '\n', dfh);
		fclose(dfh);
		if (len <= 0)
			break;

		if (!table) {
			table = calloc(maxnode * maxnode, sizeof(int));
			if (!table) {
				errno = ENOMEM;
				break;
			}
		}

		parse_numbers(line, table + nd * maxnode);
	}
	free(line);
	if (err)  {
		numa_warn(W_distance,
			  "Cannot parse distance information in sysfs: %s",
			  strerror(errno));
		free(table);
		return err;
	}
	/* Update the global table pointer.  Race window here with
	   other threads, but in the worst case we leak one distance
	   array one time, which is tolerable. This avoids a
	   dependency on pthreads. */
	if (distance_table) {
		free(table);
		return 0;
	}
	distance_numnodes = maxnode;
	distance_table = table;
	return 0;
}

int numa_distance(int a, int b)
{
	if (!distance_table) {
		int err = read_distance_table();
		if ((err < 0) || (!distance_table))
			return 0;
	}
	if ((unsigned)a >= distance_numnodes || (unsigned)b >= distance_numnodes)
		return 0;
	return distance_table[a * distance_numnodes + b];
}
