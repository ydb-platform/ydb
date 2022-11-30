#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Intel Corporation
 */

#undef RTE_USE_LIBBSD
#include <rte_string_fns.h>

#include "telemetry_data.h"

int
rte_tel_data_start_array(struct rte_tel_data *d, enum rte_tel_value_type type)
{
	enum tel_container_types array_types[] = {
			RTE_TEL_ARRAY_STRING, /* RTE_TEL_STRING_VAL = 0 */
			RTE_TEL_ARRAY_INT,    /* RTE_TEL_INT_VAL = 1 */
			RTE_TEL_ARRAY_U64,    /* RTE_TEL_u64_VAL = 2 */
			RTE_TEL_ARRAY_CONTAINER, /* RTE_TEL_CONTAINER = 3 */
	};
	d->type = array_types[type];
	d->data_len = 0;
	return 0;
}

int
rte_tel_data_start_dict(struct rte_tel_data *d)
{
	d->type = RTE_TEL_DICT;
	d->data_len = 0;
	return 0;
}

int
rte_tel_data_string(struct rte_tel_data *d, const char *str)
{
	d->type = RTE_TEL_STRING;
	d->data_len = strlcpy(d->data.str, str, sizeof(d->data.str));
	if (d->data_len >= RTE_TEL_MAX_SINGLE_STRING_LEN) {
		d->data_len = RTE_TEL_MAX_SINGLE_STRING_LEN - 1;
		return E2BIG; /* not necessarily and error, just truncation */
	}
	return 0;
}

int
rte_tel_data_add_array_string(struct rte_tel_data *d, const char *str)
{
	if (d->type != RTE_TEL_ARRAY_STRING)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_ARRAY_ENTRIES)
		return -ENOSPC;
	const size_t bytes = strlcpy(d->data.array[d->data_len++].sval,
			str, RTE_TEL_MAX_STRING_LEN);
	return bytes < RTE_TEL_MAX_STRING_LEN ? 0 : E2BIG;
}

int
rte_tel_data_add_array_int(struct rte_tel_data *d, int x)
{
	if (d->type != RTE_TEL_ARRAY_INT)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_ARRAY_ENTRIES)
		return -ENOSPC;
	d->data.array[d->data_len++].ival = x;
	return 0;
}

int
rte_tel_data_add_array_u64(struct rte_tel_data *d, uint64_t x)
{
	if (d->type != RTE_TEL_ARRAY_U64)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_ARRAY_ENTRIES)
		return -ENOSPC;
	d->data.array[d->data_len++].u64val = x;
	return 0;
}

int
rte_tel_data_add_array_container(struct rte_tel_data *d,
		struct rte_tel_data *val, int keep)
{
	if (d->type != RTE_TEL_ARRAY_CONTAINER ||
			(val->type != RTE_TEL_ARRAY_U64
			&& val->type != RTE_TEL_ARRAY_INT
			&& val->type != RTE_TEL_ARRAY_STRING))
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_ARRAY_ENTRIES)
		return -ENOSPC;

	d->data.array[d->data_len].container.data = val;
	d->data.array[d->data_len++].container.keep = !!keep;
	return 0;
}

int
rte_tel_data_add_dict_string(struct rte_tel_data *d, const char *name,
		const char *val)
{
	struct tel_dict_entry *e = &d->data.dict[d->data_len];
	size_t nbytes, vbytes;

	if (d->type != RTE_TEL_DICT)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_DICT_ENTRIES)
		return -ENOSPC;

	d->data_len++;
	e->type = RTE_TEL_STRING_VAL;
	vbytes = strlcpy(e->value.sval, val, RTE_TEL_MAX_STRING_LEN);
	nbytes = strlcpy(e->name, name, RTE_TEL_MAX_STRING_LEN);
	if (vbytes >= RTE_TEL_MAX_STRING_LEN ||
			nbytes >= RTE_TEL_MAX_STRING_LEN)
		return E2BIG;
	return 0;
}

int
rte_tel_data_add_dict_int(struct rte_tel_data *d, const char *name, int val)
{
	struct tel_dict_entry *e = &d->data.dict[d->data_len];
	if (d->type != RTE_TEL_DICT)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_DICT_ENTRIES)
		return -ENOSPC;

	d->data_len++;
	e->type = RTE_TEL_INT_VAL;
	e->value.ival = val;
	const size_t bytes = strlcpy(e->name, name, RTE_TEL_MAX_STRING_LEN);
	return bytes < RTE_TEL_MAX_STRING_LEN ? 0 : E2BIG;
}

int
rte_tel_data_add_dict_u64(struct rte_tel_data *d,
		const char *name, uint64_t val)
{
	struct tel_dict_entry *e = &d->data.dict[d->data_len];
	if (d->type != RTE_TEL_DICT)
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_DICT_ENTRIES)
		return -ENOSPC;

	d->data_len++;
	e->type = RTE_TEL_U64_VAL;
	e->value.u64val = val;
	const size_t bytes = strlcpy(e->name, name, RTE_TEL_MAX_STRING_LEN);
	return bytes < RTE_TEL_MAX_STRING_LEN ? 0 : E2BIG;
}

int
rte_tel_data_add_dict_container(struct rte_tel_data *d, const char *name,
		struct rte_tel_data *val, int keep)
{
	struct tel_dict_entry *e = &d->data.dict[d->data_len];

	if (d->type != RTE_TEL_DICT || (val->type != RTE_TEL_ARRAY_U64
			&& val->type != RTE_TEL_ARRAY_INT
			&& val->type != RTE_TEL_ARRAY_STRING))
		return -EINVAL;
	if (d->data_len >= RTE_TEL_MAX_DICT_ENTRIES)
		return -ENOSPC;

	d->data_len++;
	e->type = RTE_TEL_CONTAINER;
	e->value.container.data = val;
	e->value.container.keep = !!keep;
	const size_t bytes = strlcpy(e->name, name, RTE_TEL_MAX_STRING_LEN);
	return bytes < RTE_TEL_MAX_STRING_LEN ? 0 : E2BIG;
}

struct rte_tel_data *
rte_tel_data_alloc(void)
{
	return malloc(sizeof(struct rte_tel_data));
}

void
rte_tel_data_free(struct rte_tel_data *data)
{
	free(data);
}
