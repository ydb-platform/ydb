#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2014 6WIND S.A.
 */

/* This file manages the list of devices and their arguments, as given
 * by the user at startup
 */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <rte_bus.h>
#include <rte_class.h>
#include <rte_compat.h>
#include <rte_dev.h>
#include <rte_devargs.h>
#include <rte_errno.h>
#include <rte_kvargs.h>
#include <rte_log.h>
#include <rte_tailq.h>
#include "eal_private.h"

/** user device double-linked queue type definition */
TAILQ_HEAD(rte_devargs_list, rte_devargs);

/** Global list of user devices */
static struct rte_devargs_list devargs_list =
	TAILQ_HEAD_INITIALIZER(devargs_list);

static size_t
devargs_layer_count(const char *s)
{
	size_t i = s ? 1 : 0;

	while (s != NULL && s[0] != '\0') {
		i += s[0] == '/';
		s++;
	}
	return i;
}

int
rte_devargs_layers_parse(struct rte_devargs *devargs,
			 const char *devstr)
{
	struct {
		const char *key;
		const char *str;
		struct rte_kvargs *kvlist;
	} layers[] = {
		{ "bus=",    NULL, NULL, },
		{ "class=",  NULL, NULL, },
		{ "driver=", NULL, NULL, },
	};
	struct rte_kvargs_pair *kv = NULL;
	struct rte_class *cls = NULL;
	struct rte_bus *bus = NULL;
	const char *s = devstr;
	size_t nblayer;
	size_t i = 0;
	int ret = 0;

	/* Split each sub-lists. */
	nblayer = devargs_layer_count(devstr);
	if (nblayer > RTE_DIM(layers)) {
		RTE_LOG(ERR, EAL, "Invalid format: too many layers (%zu)\n",
			nblayer);
		ret = -E2BIG;
		goto get_out;
	}

	/* If the devargs points the devstr
	 * as source data, then it should not allocate
	 * anything and keep referring only to it.
	 */
	if (devargs->data != devstr) {
		devargs->data = strdup(devstr);
		if (devargs->data == NULL) {
			RTE_LOG(ERR, EAL, "OOM\n");
			ret = -ENOMEM;
			goto get_out;
		}
		s = devargs->data;
	}

	while (s != NULL) {
		if (i >= RTE_DIM(layers)) {
			RTE_LOG(ERR, EAL, "Unrecognized layer %s\n", s);
			ret = -EINVAL;
			goto get_out;
		}
		/*
		 * The last layer is free-form.
		 * The "driver" key is not required (but accepted).
		 */
		if (strncmp(layers[i].key, s, strlen(layers[i].key)) &&
				i != RTE_DIM(layers) - 1)
			goto next_layer;
		layers[i].str = s;
		layers[i].kvlist = rte_kvargs_parse_delim(s, NULL, "/");
		if (layers[i].kvlist == NULL) {
			RTE_LOG(ERR, EAL, "Could not parse %s\n", s);
			ret = -EINVAL;
			goto get_out;
		}
		s = strchr(s, '/');
		if (s != NULL)
			s++;
next_layer:
		i++;
	}

	/* Parse each sub-list. */
	for (i = 0; i < RTE_DIM(layers); i++) {
		if (layers[i].kvlist == NULL)
			continue;
		kv = &layers[i].kvlist->pairs[0];
		if (strcmp(kv->key, "bus") == 0) {
			bus = rte_bus_find_by_name(kv->value);
			if (bus == NULL) {
				RTE_LOG(ERR, EAL, "Could not find bus \"%s\"\n",
					kv->value);
				ret = -EFAULT;
				goto get_out;
			}
		} else if (strcmp(kv->key, "class") == 0) {
			cls = rte_class_find_by_name(kv->value);
			if (cls == NULL) {
				RTE_LOG(ERR, EAL, "Could not find class \"%s\"\n",
					kv->value);
				ret = -EFAULT;
				goto get_out;
			}
		} else if (strcmp(kv->key, "driver") == 0) {
			/* Ignore */
			continue;
		}
	}

	/* Fill devargs fields. */
	devargs->bus_str = layers[0].str;
	devargs->cls_str = layers[1].str;
	devargs->drv_str = layers[2].str;
	devargs->bus = bus;
	devargs->cls = cls;

	/* If we own the data, clean up a bit
	 * the several layers string, to ease
	 * their parsing afterward.
	 */
	if (devargs->data != devstr) {
		char *s = (void *)(intptr_t)(devargs->data);

		while ((s = strchr(s, '/'))) {
			*s = '\0';
			s++;
		}
	}

get_out:
	for (i = 0; i < RTE_DIM(layers); i++) {
		if (layers[i].kvlist)
			rte_kvargs_free(layers[i].kvlist);
	}
	if (ret != 0)
		rte_errno = -ret;
	return ret;
}

static int
bus_name_cmp(const struct rte_bus *bus, const void *name)
{
	return strncmp(bus->name, name, strlen(bus->name));
}

int
rte_devargs_parse(struct rte_devargs *da, const char *dev)
{
	struct rte_bus *bus = NULL;
	const char *devname;
	const size_t maxlen = sizeof(da->name);
	size_t i;

	if (da == NULL)
		return -EINVAL;

	/* Retrieve eventual bus info */
	do {
		devname = dev;
		bus = rte_bus_find(bus, bus_name_cmp, dev);
		if (bus == NULL)
			break;
		devname = dev + strlen(bus->name) + 1;
		if (rte_bus_find_by_device_name(devname) == bus)
			break;
	} while (1);
	/* Store device name */
	i = 0;
	while (devname[i] != '\0' && devname[i] != ',') {
		da->name[i] = devname[i];
		i++;
		if (i == maxlen) {
			RTE_LOG(WARNING, EAL, "Parsing \"%s\": device name should be shorter than %zu\n",
				dev, maxlen);
			da->name[i - 1] = '\0';
			return -EINVAL;
		}
	}
	da->name[i] = '\0';
	if (bus == NULL) {
		bus = rte_bus_find_by_device_name(da->name);
		if (bus == NULL) {
			RTE_LOG(ERR, EAL, "failed to parse device \"%s\"\n",
				da->name);
			return -EFAULT;
		}
	}
	da->bus = bus;
	/* Parse eventual device arguments */
	if (devname[i] == ',')
		da->args = strdup(&devname[i + 1]);
	else
		da->args = strdup("");
	if (da->args == NULL) {
		RTE_LOG(ERR, EAL, "not enough memory to parse arguments\n");
		return -ENOMEM;
	}
	return 0;
}

int
rte_devargs_parsef(struct rte_devargs *da, const char *format, ...)
{
	va_list ap;
	size_t len;
	char *dev;
	int ret;

	if (da == NULL)
		return -EINVAL;

	va_start(ap, format);
	len = vsnprintf(NULL, 0, format, ap);
	va_end(ap);

	dev = calloc(1, len + 1);
	if (dev == NULL) {
		RTE_LOG(ERR, EAL, "not enough memory to parse device\n");
		return -ENOMEM;
	}

	va_start(ap, format);
	vsnprintf(dev, len + 1, format, ap);
	va_end(ap);

	ret = rte_devargs_parse(da, dev);

	free(dev);
	return ret;
}

int
rte_devargs_insert(struct rte_devargs **da)
{
	struct rte_devargs *listed_da;
	void *tmp;

	if (*da == NULL || (*da)->bus == NULL)
		return -1;

	TAILQ_FOREACH_SAFE(listed_da, &devargs_list, next, tmp) {
		if (listed_da == *da)
			/* devargs already in the list */
			return 0;
		if (strcmp(listed_da->bus->name, (*da)->bus->name) == 0 &&
				strcmp(listed_da->name, (*da)->name) == 0) {
			/* device already in devargs list, must be updated */
			listed_da->type = (*da)->type;
			listed_da->policy = (*da)->policy;
			free(listed_da->args);
			listed_da->args = (*da)->args;
			listed_da->bus = (*da)->bus;
			listed_da->cls = (*da)->cls;
			listed_da->bus_str = (*da)->bus_str;
			listed_da->cls_str = (*da)->cls_str;
			listed_da->data = (*da)->data;
			/* replace provided devargs with found one */
			free(*da);
			*da = listed_da;
			return 0;
		}
	}
	/* new device in the list */
	TAILQ_INSERT_TAIL(&devargs_list, *da, next);
	return 0;
}

/* store in allowed list parameter for later parsing */
int
rte_devargs_add(enum rte_devtype devtype, const char *devargs_str)
{
	struct rte_devargs *devargs = NULL;
	struct rte_bus *bus = NULL;
	const char *dev = devargs_str;

	/* use calloc instead of rte_zmalloc as it's called early at init */
	devargs = calloc(1, sizeof(*devargs));
	if (devargs == NULL)
		goto fail;

	if (rte_devargs_parse(devargs, dev))
		goto fail;
	devargs->type = devtype;
	bus = devargs->bus;
	if (devargs->type == RTE_DEVTYPE_BLOCKED)
		devargs->policy = RTE_DEV_BLOCKED;
	if (bus->conf.scan_mode == RTE_BUS_SCAN_UNDEFINED) {
		if (devargs->policy == RTE_DEV_ALLOWED)
			bus->conf.scan_mode = RTE_BUS_SCAN_ALLOWLIST;
		else if (devargs->policy == RTE_DEV_BLOCKED)
			bus->conf.scan_mode = RTE_BUS_SCAN_BLOCKLIST;
	}
	TAILQ_INSERT_TAIL(&devargs_list, devargs, next);
	return 0;

fail:
	if (devargs) {
		free(devargs->args);
		free(devargs);
	}

	return -1;
}

int
rte_devargs_remove(struct rte_devargs *devargs)
{
	struct rte_devargs *d;
	void *tmp;

	if (devargs == NULL || devargs->bus == NULL)
		return -1;

	TAILQ_FOREACH_SAFE(d, &devargs_list, next, tmp) {
		if (strcmp(d->bus->name, devargs->bus->name) == 0 &&
		    strcmp(d->name, devargs->name) == 0) {
			TAILQ_REMOVE(&devargs_list, d, next);
			free(d->args);
			free(d);
			return 0;
		}
	}
	return 1;
}

/* count the number of devices of a specified type */
unsigned int
rte_devargs_type_count(enum rte_devtype devtype)
{
	struct rte_devargs *devargs;
	unsigned int count = 0;

	TAILQ_FOREACH(devargs, &devargs_list, next) {
		if (devargs->type != devtype)
			continue;
		count++;
	}
	return count;
}

/* dump the user devices on the console */
void
rte_devargs_dump(FILE *f)
{
	struct rte_devargs *devargs;

	fprintf(f, "User device list:\n");
	TAILQ_FOREACH(devargs, &devargs_list, next) {
		fprintf(f, "  [%s]: %s %s\n",
			(devargs->bus ? devargs->bus->name : "??"),
			devargs->name, devargs->args);
	}
}

/* bus-aware rte_devargs iterator. */
struct rte_devargs *
rte_devargs_next(const char *busname, const struct rte_devargs *start)
{
	struct rte_devargs *da;

	if (start != NULL)
		da = TAILQ_NEXT(start, next);
	else
		da = TAILQ_FIRST(&devargs_list);
	while (da != NULL) {
		if (busname == NULL ||
		    (strcmp(busname, da->bus->name) == 0))
			return da;
		da = TAILQ_NEXT(da, next);
	}
	return NULL;
}
