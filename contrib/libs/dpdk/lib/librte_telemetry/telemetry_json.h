/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Intel Corporation
 */

#ifndef _RTE_TELEMETRY_JSON_H_
#define _RTE_TELEMETRY_JSON_H_

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <rte_common.h>

/**
 * @file
 * Internal Telemetry Utility functions
 *
 * This file contains small inline functions to make it easier for applications
 * to build up valid JSON responses to telemetry requests.
 *
 ***/

/**
 * @internal
 * Copies a value into a buffer if the buffer has enough available space.
 * Nothing written to buffer if an overflow ocurs.
 * This function is not for use for values larger than 1k.
 */
__rte_format_printf(3, 4)
static inline int
__json_snprintf(char *buf, const int len, const char *format, ...)
{
	char tmp[1024];
	va_list ap;
	int ret;

	va_start(ap, format);
	ret = vsnprintf(tmp, sizeof(tmp), format, ap);
	va_end(ap);
	if (ret > 0 && ret < (int)sizeof(tmp) && ret < len) {
		strcpy(buf, tmp);
		return ret;
	}
	return 0; /* nothing written or modified */
}

/* Copies an empty array into the provided buffer. */
static inline int
rte_tel_json_empty_array(char *buf, const int len, const int used)
{
	return used + __json_snprintf(buf + used, len - used, "[]");
}

/* Copies an empty object into the provided buffer. */
static inline int
rte_tel_json_empty_obj(char *buf, const int len, const int used)
{
	return used + __json_snprintf(buf + used, len - used, "{}");
}

/* Copies a string into the provided buffer, in JSON format. */
static inline int
rte_tel_json_str(char *buf, const int len, const int used, const char *str)
{
	return used + __json_snprintf(buf + used, len - used, "\"%s\"", str);
}

/* Appends a string into the JSON array in the provided buffer. */
static inline int
rte_tel_json_add_array_string(char *buf, const int len, const int used,
		const char *str)
{
	int ret, end = used - 1; /* strip off final delimiter */
	if (used <= 2) /* assume empty, since minimum is '[]' */
		return __json_snprintf(buf, len, "[\"%s\"]", str);

	ret = __json_snprintf(buf + end, len - end, ",\"%s\"]", str);
	return ret == 0 ? used : end + ret;
}

/* Appends an integer into the JSON array in the provided buffer. */
static inline int
rte_tel_json_add_array_int(char *buf, const int len, const int used, int val)
{
	int ret, end = used - 1; /* strip off final delimiter */
	if (used <= 2) /* assume empty, since minimum is '[]' */
		return __json_snprintf(buf, len, "[%d]", val);

	ret = __json_snprintf(buf + end, len - end, ",%d]", val);
	return ret == 0 ? used : end + ret;
}

/* Appends a uint64_t into the JSON array in the provided buffer. */
static inline int
rte_tel_json_add_array_u64(char *buf, const int len, const int used,
		uint64_t val)
{
	int ret, end = used - 1; /* strip off final delimiter */
	if (used <= 2) /* assume empty, since minimum is '[]' */
		return __json_snprintf(buf, len, "[%"PRIu64"]", val);

	ret = __json_snprintf(buf + end, len - end, ",%"PRIu64"]", val);
	return ret == 0 ? used : end + ret;
}

/*
 * Add a new element with raw JSON value to the JSON array stored in the
 * provided buffer.
 */
static inline int
rte_tel_json_add_array_json(char *buf, const int len, const int used,
		const char *str)
{
	int ret, end = used - 1; /* strip off final delimiter */
	if (used <= 2) /* assume empty, since minimum is '[]' */
		return __json_snprintf(buf, len, "[%s]", str);

	ret = __json_snprintf(buf + end, len - end, ",%s]", str);
	return ret == 0 ? used : end + ret;
}

/**
 * Add a new element with uint64_t value to the JSON object stored in the
 * provided buffer.
 */
static inline int
rte_tel_json_add_obj_u64(char *buf, const int len, const int used,
		const char *name, uint64_t val)
{
	int ret, end = used - 1;
	if (used <= 2) /* assume empty, since minimum is '{}' */
		return __json_snprintf(buf, len, "{\"%s\":%"PRIu64"}", name,
				val);

	ret = __json_snprintf(buf + end, len - end, ",\"%s\":%"PRIu64"}",
			name, val);
	return ret == 0 ? used : end + ret;
}

/**
 * Add a new element with int value to the JSON object stored in the
 * provided buffer.
 */
static inline int
rte_tel_json_add_obj_int(char *buf, const int len, const int used,
		const char *name, int val)
{
	int ret, end = used - 1;
	if (used <= 2) /* assume empty, since minimum is '{}' */
		return __json_snprintf(buf, len, "{\"%s\":%d}", name,
				val);

	ret = __json_snprintf(buf + end, len - end, ",\"%s\":%d}",
			name, val);
	return ret == 0 ? used : end + ret;
}

/**
 * Add a new element with string value to the JSON object stored in the
 * provided buffer.
 */
static inline int
rte_tel_json_add_obj_str(char *buf, const int len, const int used,
		const char *name, const char *val)
{
	int ret, end = used - 1;
	if (used <= 2) /* assume empty, since minimum is '{}' */
		return __json_snprintf(buf, len, "{\"%s\":\"%s\"}", name, val);

	ret = __json_snprintf(buf + end, len - end, ",\"%s\":\"%s\"}",
			name, val);
	return ret == 0 ? used : end + ret;
}

/**
 * Add a new element with raw JSON value to the JSON object stored in the
 * provided buffer.
 */
static inline int
rte_tel_json_add_obj_json(char *buf, const int len, const int used,
		const char *name, const char *val)
{
	int ret, end = used - 1;
	if (used <= 2) /* assume empty, since minimum is '{}' */
		return __json_snprintf(buf, len, "{\"%s\":%s}", name, val);

	ret = __json_snprintf(buf + end, len - end, ",\"%s\":%s}",
			name, val);
	return ret == 0 ? used : end + ret;
}

#endif /*_RTE_TELEMETRY_JSON_H_*/
