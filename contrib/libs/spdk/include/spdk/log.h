/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file
 * Logging interfaces
 */

#ifndef SPDK_LOG_H
#define SPDK_LOG_H

#include "spdk/stdinc.h"
#include "spdk/queue.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * for passing user-provided log call
 *
 * \param level Log level threshold.
 * \param file Name of the current source file.
 * \param line Current source file line.
 * \param func Current source function name.
 * \param format Format string to the message.
 * \param args Additional arguments for format string.
 */
typedef void logfunc(int level, const char *file, const int line,
		     const char *func, const char *format, va_list args);

/**
 * Initialize the logging module. Messages prior
 * to this call will be dropped.
 */
void spdk_log_open(logfunc *logf);

/**
 * Close the currently active log. Messages after this call
 * will be dropped.
 */
void spdk_log_close(void);

/**
 * Enable or disable timestamps
 */
void spdk_log_enable_timestamps(bool value);

enum spdk_log_level {
	/** All messages will be suppressed. */
	SPDK_LOG_DISABLED = -1,
	SPDK_LOG_ERROR,
	SPDK_LOG_WARN,
	SPDK_LOG_NOTICE,
	SPDK_LOG_INFO,
	SPDK_LOG_DEBUG,
};

/**
 * Set the log level threshold to log messages. Messages with a higher
 * level than this are ignored.
 *
 * \param level Log level threshold to set to log messages.
 */
void spdk_log_set_level(enum spdk_log_level level);

/**
 * Get the current log level threshold.
 *
 * \return the current log level threshold.
 */
enum spdk_log_level spdk_log_get_level(void);

/**
 * Set the current log level threshold for printing to stderr.
 * Messages with a level less than or equal to this level
 * are also printed to stderr. You can use \c SPDK_LOG_DISABLED to completely
 * suppress log printing.
 *
 * \param level Log level threshold for printing to stderr.
 */
void spdk_log_set_print_level(enum spdk_log_level level);

/**
 * Get the current log level print threshold.
 *
 * \return the current log level print threshold.
 */
enum spdk_log_level spdk_log_get_print_level(void);

#ifdef DEBUG
#define SPDK_DEBUGLOG_FLAG_ENABLED(name) spdk_log_get_flag(name)
#else
#define SPDK_DEBUGLOG_FLAG_ENABLED(name) false
#endif

#define SPDK_NOTICELOG(...) \
	spdk_log(SPDK_LOG_NOTICE, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define SPDK_WARNLOG(...) \
	spdk_log(SPDK_LOG_WARN, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define SPDK_ERRLOG(...) \
	spdk_log(SPDK_LOG_ERROR, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define SPDK_PRINTF(...) \
	spdk_log(SPDK_LOG_NOTICE, NULL, -1, NULL, __VA_ARGS__)
#define SPDK_INFOLOG(FLAG, ...)									\
	do {											\
		extern struct spdk_log_flag SPDK_LOG_##FLAG;					\
		if (SPDK_LOG_##FLAG.enabled) {							\
			spdk_log(SPDK_LOG_INFO, __FILE__, __LINE__, __func__, __VA_ARGS__);	\
		}										\
	} while (0)

#ifdef DEBUG
#define SPDK_DEBUGLOG(FLAG, ...)								\
	do {											\
		extern struct spdk_log_flag SPDK_LOG_##FLAG;					\
		if (SPDK_LOG_##FLAG.enabled) {							\
			spdk_log(SPDK_LOG_DEBUG, __FILE__, __LINE__, __func__, __VA_ARGS__);	\
		}										\
	} while (0)

#define SPDK_LOGDUMP(FLAG, LABEL, BUF, LEN)				\
	do {								\
		extern struct spdk_log_flag SPDK_LOG_##FLAG;		\
		if (SPDK_LOG_##FLAG.enabled) {				\
			spdk_log_dump(stderr, (LABEL), (BUF), (LEN));	\
		}							\
	} while (0)

#else
#define SPDK_DEBUGLOG(...) do { } while (0)
#define SPDK_LOGDUMP(...) do { } while (0)
#endif

/**
 * Write messages to the log file. If \c level is set to \c SPDK_LOG_DISABLED,
 * this log message won't be written.
 *
 * \param level Log level threshold.
 * \param file Name of the current source file.
 * \param line Current source line number.
 * \param func Current source function name.
 * \param format Format string to the message.
 */
void spdk_log(enum spdk_log_level level, const char *file, const int line, const char *func,
	      const char *format, ...) __attribute__((__format__(__printf__, 5, 6)));

/**
 * Same as spdk_log except that instead of being called with variable number of
 * arguments it is called with an argument list as defined in stdarg.h
 *
 * \param level Log level threshold.
 * \param file Name of the current source file.
 * \param line Current source line number.
 * \param func Current source function name.
 * \param format Format string to the message.
 * \param ap printf arguments
 */
void spdk_vlog(enum spdk_log_level level, const char *file, const int line, const char *func,
	       const char *format, va_list ap);

/**
 * Log the contents of a raw buffer to a file.
 *
 * \param fp File to hold the log.
 * \param label Label to print to the file.
 * \param buf Buffer that holds the log information.
 * \param len Length of buffer to dump.
 */
void spdk_log_dump(FILE *fp, const char *label, const void *buf, size_t len);

struct spdk_log_flag {
	TAILQ_ENTRY(spdk_log_flag) tailq;
	const char *name;
	bool enabled;
};

/**
 * Register a log flag.
 *
 * \param name Name of the log flag.
 * \param flag Log flag to be added.
 */
void spdk_log_register_flag(const char *name, struct spdk_log_flag *flag);

#define SPDK_LOG_REGISTER_COMPONENT(FLAG) \
struct spdk_log_flag SPDK_LOG_##FLAG = { \
	.enabled = false, \
	.name = #FLAG, \
}; \
__attribute__((constructor)) static void register_flag_##FLAG(void) \
{ \
	spdk_log_register_flag(#FLAG, &SPDK_LOG_##FLAG); \
}

/**
 * Get the first registered log flag.
 *
 * \return The first registered log flag.
 */
struct spdk_log_flag *spdk_log_get_first_flag(void);

/**
 * Get the next registered log flag.
 *
 * \param flag The current log flag.
 *
 * \return The next registered log flag.
 */
struct spdk_log_flag *spdk_log_get_next_flag(struct spdk_log_flag *flag);

/**
 * Check whether the log flag exists and is enabled.
 *
 * \return true if enabled, or false otherwise.
 */
bool spdk_log_get_flag(const char *flag);

/**
 * Enable the log flag.
 *
 * \param flag Log flag to be enabled.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_log_set_flag(const char *flag);

/**
 * Clear a log flag.
 *
 * \param flag Log flag to clear.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_log_clear_flag(const char *flag);

/**
 * Show all the log flags and their usage.
 *
 * \param f File to hold all the flags' information.
 * \param log_arg Command line option to set/enable the log flag.
 */
void spdk_log_usage(FILE *f, const char *log_arg);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_LOG_H */
