#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <regex.h>
#include <fnmatch.h>

#include <rte_eal.h>
#include <rte_log.h>
#include <rte_per_lcore.h>

#include "eal_private.h"

struct rte_log_dynamic_type {
	const char *name;
	uint32_t loglevel;
};

/** The rte_log structure. */
static struct rte_logs {
	uint32_t type;  /**< Bitfield with enabled logs. */
	uint32_t level; /**< Log level. */
	FILE *file;     /**< Output file set by rte_openlog_stream, or NULL. */
	size_t dynamic_types_len;
	struct rte_log_dynamic_type *dynamic_types;
} rte_logs = {
	.type = ~0,
	.level = RTE_LOG_DEBUG,
};

struct rte_eal_opt_loglevel {
	/** Next list entry */
	TAILQ_ENTRY(rte_eal_opt_loglevel) next;
	/** Compiled regular expression obtained from the option */
	regex_t re_match;
	/** Globbing pattern option */
	char *pattern;
	/** Log level value obtained from the option */
	uint32_t level;
};

TAILQ_HEAD(rte_eal_opt_loglevel_list, rte_eal_opt_loglevel);

/** List of valid EAL log level options */
static struct rte_eal_opt_loglevel_list opt_loglevel_list =
	TAILQ_HEAD_INITIALIZER(opt_loglevel_list);

/* Stream to use for logging if rte_logs.file is NULL */
static FILE *default_log_stream;

/**
 * This global structure stores some information about the message
 * that is currently being processed by one lcore
 */
struct log_cur_msg {
	uint32_t loglevel; /**< log level - see rte_log.h */
	uint32_t logtype;  /**< log type  - see rte_log.h */
};

 /* per core log */
static RTE_DEFINE_PER_LCORE(struct log_cur_msg, log_cur_msg);

/* default logs */

/* Change the stream that will be used by logging system */
int
rte_openlog_stream(FILE *f)
{
	rte_logs.file = f;
	return 0;
}

FILE *
rte_log_get_stream(void)
{
	FILE *f = rte_logs.file;

	if (f == NULL) {
		/*
		 * Grab the current value of stderr here, rather than
		 * just initializing default_log_stream to stderr. This
		 * ensures that we will always use the current value
		 * of stderr, even if the application closes and
		 * reopens it.
		 */
		return default_log_stream ? : stderr;
	}
	return f;
}

/* Set global log level */
void
rte_log_set_global_level(uint32_t level)
{
	rte_logs.level = (uint32_t)level;
}

/* Get global log level */
uint32_t
rte_log_get_global_level(void)
{
	return rte_logs.level;
}

int
rte_log_get_level(uint32_t type)
{
	if (type >= rte_logs.dynamic_types_len)
		return -1;

	return rte_logs.dynamic_types[type].loglevel;
}

bool
rte_log_can_log(uint32_t logtype, uint32_t level)
{
	int log_level;

	if (level > rte_log_get_global_level())
		return false;

	log_level = rte_log_get_level(logtype);
	if (log_level < 0)
		return false;

	if (level > (uint32_t)log_level)
		return false;

	return true;
}

int
rte_log_set_level(uint32_t type, uint32_t level)
{
	if (type >= rte_logs.dynamic_types_len)
		return -1;
	if (level > RTE_LOG_DEBUG)
		return -1;

	rte_logs.dynamic_types[type].loglevel = level;

	return 0;
}

/* set log level by regular expression */
int
rte_log_set_level_regexp(const char *regex, uint32_t level)
{
	regex_t r;
	size_t i;

	if (level > RTE_LOG_DEBUG)
		return -1;

	if (regcomp(&r, regex, 0) != 0)
		return -1;

	for (i = 0; i < rte_logs.dynamic_types_len; i++) {
		if (rte_logs.dynamic_types[i].name == NULL)
			continue;
		if (regexec(&r, rte_logs.dynamic_types[i].name, 0,
				NULL, 0) == 0)
			rte_logs.dynamic_types[i].loglevel = level;
	}

	regfree(&r);

	return 0;
}

/*
 * Save the type string and the loglevel for later dynamic
 * logtypes which may register later.
 */
static int rte_log_save_level(int priority,
			      const char *regex, const char *pattern)
{
	struct rte_eal_opt_loglevel *opt_ll = NULL;

	opt_ll = malloc(sizeof(*opt_ll));
	if (opt_ll == NULL)
		goto fail;

	opt_ll->level = priority;

	if (regex) {
		opt_ll->pattern = NULL;
		if (regcomp(&opt_ll->re_match, regex, 0) != 0)
			goto fail;
	} else if (pattern) {
		opt_ll->pattern = strdup(pattern);
		if (opt_ll->pattern == NULL)
			goto fail;
	} else
		goto fail;

	TAILQ_INSERT_HEAD(&opt_loglevel_list, opt_ll, next);
	return 0;
fail:
	free(opt_ll);
	return -1;
}

int rte_log_save_regexp(const char *regex, int tmp)
{
	return rte_log_save_level(tmp, regex, NULL);
}

/* set log level based on globbing pattern */
int
rte_log_set_level_pattern(const char *pattern, uint32_t level)
{
	size_t i;

	if (level > RTE_LOG_DEBUG)
		return -1;

	for (i = 0; i < rte_logs.dynamic_types_len; i++) {
		if (rte_logs.dynamic_types[i].name == NULL)
			continue;

		if (fnmatch(pattern, rte_logs.dynamic_types[i].name, 0) == 0)
			rte_logs.dynamic_types[i].loglevel = level;
	}

	return 0;
}

int rte_log_save_pattern(const char *pattern, int priority)
{
	return rte_log_save_level(priority, NULL, pattern);
}

/* get the current loglevel for the message being processed */
int rte_log_cur_msg_loglevel(void)
{
	return RTE_PER_LCORE(log_cur_msg).loglevel;
}

/* get the current logtype for the message being processed */
int rte_log_cur_msg_logtype(void)
{
	return RTE_PER_LCORE(log_cur_msg).logtype;
}

static int
rte_log_lookup(const char *name)
{
	size_t i;

	for (i = 0; i < rte_logs.dynamic_types_len; i++) {
		if (rte_logs.dynamic_types[i].name == NULL)
			continue;
		if (strcmp(name, rte_logs.dynamic_types[i].name) == 0)
			return i;
	}

	return -1;
}

/* register an extended log type, assuming table is large enough, and id
 * is not yet registered.
 */
static int
__rte_log_register(const char *name, int id)
{
	char *dup_name = strdup(name);

	if (dup_name == NULL)
		return -ENOMEM;

	rte_logs.dynamic_types[id].name = dup_name;
	rte_logs.dynamic_types[id].loglevel = RTE_LOG_INFO;

	return id;
}

/* register an extended log type */
int
rte_log_register(const char *name)
{
	struct rte_log_dynamic_type *new_dynamic_types;
	int id, ret;

	id = rte_log_lookup(name);
	if (id >= 0)
		return id;

	new_dynamic_types = realloc(rte_logs.dynamic_types,
		sizeof(struct rte_log_dynamic_type) *
		(rte_logs.dynamic_types_len + 1));
	if (new_dynamic_types == NULL)
		return -ENOMEM;
	rte_logs.dynamic_types = new_dynamic_types;

	ret = __rte_log_register(name, rte_logs.dynamic_types_len);
	if (ret < 0)
		return ret;

	rte_logs.dynamic_types_len++;

	return ret;
}

/* Register an extended log type and try to pick its level from EAL options */
int
rte_log_register_type_and_pick_level(const char *name, uint32_t level_def)
{
	struct rte_eal_opt_loglevel *opt_ll;
	uint32_t level = level_def;
	int type;

	type = rte_log_register(name);
	if (type < 0)
		return type;

	TAILQ_FOREACH(opt_ll, &opt_loglevel_list, next) {
		if (opt_ll->level > RTE_LOG_DEBUG)
			continue;

		if (opt_ll->pattern) {
			if (fnmatch(opt_ll->pattern, name, 0) == 0)
				level = opt_ll->level;
		} else {
			if (regexec(&opt_ll->re_match, name, 0, NULL, 0) == 0)
				level = opt_ll->level;
		}
	}

	rte_logs.dynamic_types[type].loglevel = level;

	return type;
}

struct logtype {
	uint32_t log_id;
	const char *logtype;
};

static const struct logtype logtype_strings[] = {
	{RTE_LOGTYPE_EAL,        "lib.eal"},
	{RTE_LOGTYPE_MALLOC,     "lib.malloc"},
	{RTE_LOGTYPE_RING,       "lib.ring"},
	{RTE_LOGTYPE_MEMPOOL,    "lib.mempool"},
	{RTE_LOGTYPE_TIMER,      "lib.timer"},
	{RTE_LOGTYPE_PMD,        "pmd"},
	{RTE_LOGTYPE_HASH,       "lib.hash"},
	{RTE_LOGTYPE_LPM,        "lib.lpm"},
	{RTE_LOGTYPE_KNI,        "lib.kni"},
	{RTE_LOGTYPE_ACL,        "lib.acl"},
	{RTE_LOGTYPE_POWER,      "lib.power"},
	{RTE_LOGTYPE_METER,      "lib.meter"},
	{RTE_LOGTYPE_SCHED,      "lib.sched"},
	{RTE_LOGTYPE_PORT,       "lib.port"},
	{RTE_LOGTYPE_TABLE,      "lib.table"},
	{RTE_LOGTYPE_PIPELINE,   "lib.pipeline"},
	{RTE_LOGTYPE_MBUF,       "lib.mbuf"},
	{RTE_LOGTYPE_CRYPTODEV,  "lib.cryptodev"},
	{RTE_LOGTYPE_EFD,        "lib.efd"},
	{RTE_LOGTYPE_EVENTDEV,   "lib.eventdev"},
	{RTE_LOGTYPE_GSO,        "lib.gso"},
	{RTE_LOGTYPE_USER1,      "user1"},
	{RTE_LOGTYPE_USER2,      "user2"},
	{RTE_LOGTYPE_USER3,      "user3"},
	{RTE_LOGTYPE_USER4,      "user4"},
	{RTE_LOGTYPE_USER5,      "user5"},
	{RTE_LOGTYPE_USER6,      "user6"},
	{RTE_LOGTYPE_USER7,      "user7"},
	{RTE_LOGTYPE_USER8,      "user8"}
};

/* Logging should be first initializer (before drivers and bus) */
RTE_INIT_PRIO(rte_log_init, LOG)
{
	uint32_t i;

	rte_log_set_global_level(RTE_LOG_DEBUG);

	rte_logs.dynamic_types = calloc(RTE_LOGTYPE_FIRST_EXT_ID,
		sizeof(struct rte_log_dynamic_type));
	if (rte_logs.dynamic_types == NULL)
		return;

	/* register legacy log types */
	for (i = 0; i < RTE_DIM(logtype_strings); i++)
		__rte_log_register(logtype_strings[i].logtype,
				logtype_strings[i].log_id);

	rte_logs.dynamic_types_len = RTE_LOGTYPE_FIRST_EXT_ID;
}

static const char *
loglevel_to_string(uint32_t level)
{
	switch (level) {
	case 0: return "disabled";
	case RTE_LOG_EMERG: return "emerg";
	case RTE_LOG_ALERT: return "alert";
	case RTE_LOG_CRIT: return "critical";
	case RTE_LOG_ERR: return "error";
	case RTE_LOG_WARNING: return "warning";
	case RTE_LOG_NOTICE: return "notice";
	case RTE_LOG_INFO: return "info";
	case RTE_LOG_DEBUG: return "debug";
	default: return "unknown";
	}
}

/* dump global level and registered log types */
void
rte_log_dump(FILE *f)
{
	size_t i;

	fprintf(f, "global log level is %s\n",
		loglevel_to_string(rte_log_get_global_level()));

	for (i = 0; i < rte_logs.dynamic_types_len; i++) {
		if (rte_logs.dynamic_types[i].name == NULL)
			continue;
		fprintf(f, "id %zu: %s, level is %s\n",
			i, rte_logs.dynamic_types[i].name,
			loglevel_to_string(rte_logs.dynamic_types[i].loglevel));
	}
}

/*
 * Generates a log message The message will be sent in the stream
 * defined by the previous call to rte_openlog_stream().
 */
int
rte_vlog(uint32_t level, uint32_t logtype, const char *format, va_list ap)
{
	FILE *f = rte_log_get_stream();
	int ret;

	if (logtype >= rte_logs.dynamic_types_len)
		return -1;
	if (!rte_log_can_log(logtype, level))
		return 0;

	/* save loglevel and logtype in a global per-lcore variable */
	RTE_PER_LCORE(log_cur_msg).loglevel = level;
	RTE_PER_LCORE(log_cur_msg).logtype = logtype;

	ret = vfprintf(f, format, ap);
	fflush(f);
	return ret;
}

/*
 * Generates a log message The message will be sent in the stream
 * defined by the previous call to rte_openlog_stream().
 * No need to check level here, done by rte_vlog().
 */
int
rte_log(uint32_t level, uint32_t logtype, const char *format, ...)
{
	va_list ap;
	int ret;

	va_start(ap, format);
	ret = rte_vlog(level, logtype, format, ap);
	va_end(ap);
	return ret;
}

/*
 * Called by environment-specific initialization functions.
 */
void
eal_log_set_default(FILE *default_log)
{
	default_log_stream = default_log;

#if RTE_LOG_DP_LEVEL >= RTE_LOG_DEBUG
	RTE_LOG(NOTICE, EAL,
		"Debug dataplane logs available - lower performance\n");
#endif
}
