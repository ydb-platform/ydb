#include <contrib/libs/spdk/ndebug.h>
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

#include "spdk/stdinc.h"

#include "spdk/log.h"

static const char *const spdk_level_names[] = {
	[SPDK_LOG_ERROR]	= "ERROR",
	[SPDK_LOG_WARN]		= "WARNING",
	[SPDK_LOG_NOTICE]	= "NOTICE",
	[SPDK_LOG_INFO]		= "INFO",
	[SPDK_LOG_DEBUG]	= "DEBUG",
};

#define MAX_TMPBUF 1024

static logfunc *g_log = NULL;
static bool g_log_timestamps = true;

extern enum spdk_log_level g_spdk_log_level;
extern enum spdk_log_level g_spdk_log_print_level;

void
spdk_log_open(logfunc *logf)
{
	if (logf) {
		g_log = logf;
	} else {
		openlog("spdk", LOG_PID, LOG_LOCAL7);
	}
}

void
spdk_log_close(void)
{
	if (!g_log) {
		closelog();
	}
}

void
spdk_log_enable_timestamps(bool value)
{
	g_log_timestamps = value;
}

static void
get_timestamp_prefix(char *buf, int buf_size)
{
	struct tm *info;
	char date[24];
	struct timespec ts;
	long usec;

	if (!g_log_timestamps) {
		buf[0] = '\0';
		return;
	}

	clock_gettime(CLOCK_REALTIME, &ts);
	info = localtime(&ts.tv_sec);
	usec = ts.tv_nsec / 1000;
	if (info == NULL) {
		snprintf(buf, buf_size, "[%s.%06ld] ", "unknown date", usec);
		return;
	}

	strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", info);
	snprintf(buf, buf_size, "[%s.%06ld] ", date, usec);
}

void
spdk_log(enum spdk_log_level level, const char *file, const int line, const char *func,
	 const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	spdk_vlog(level, file, line, func, format, ap);
	va_end(ap);
}

void
spdk_vlog(enum spdk_log_level level, const char *file, const int line, const char *func,
	  const char *format, va_list ap)
{
	int severity = LOG_INFO;
	char buf[MAX_TMPBUF];
	char timestamp[64];

	if (g_log) {
		g_log(level, file, line, func, format, ap);
		return;
	}

	if (level > g_spdk_log_print_level && level > g_spdk_log_level) {
		return;
	}

	switch (level) {
	case SPDK_LOG_ERROR:
		severity = LOG_ERR;
		break;
	case SPDK_LOG_WARN:
		severity = LOG_WARNING;
		break;
	case SPDK_LOG_NOTICE:
		severity = LOG_NOTICE;
		break;
	case SPDK_LOG_INFO:
	case SPDK_LOG_DEBUG:
		severity = LOG_INFO;
		break;
	case SPDK_LOG_DISABLED:
		return;
	}

	vsnprintf(buf, sizeof(buf), format, ap);

	if (level <= g_spdk_log_print_level) {
		get_timestamp_prefix(timestamp, sizeof(timestamp));
		if (file) {
			fprintf(stderr, "%s%s:%4d:%s: *%s*: %s", timestamp, file, line, func, spdk_level_names[level], buf);
		} else {
			fprintf(stderr, "%s%s", timestamp, buf);
		}
	}

	if (level <= g_spdk_log_level) {
		if (file) {
			syslog(severity, "%s:%4d:%s: *%s*: %s", file, line, func, spdk_level_names[level], buf);
		} else {
			syslog(severity, "%s", buf);
		}
	}
}

static void
fdump(FILE *fp, const char *label, const uint8_t *buf, size_t len)
{
	char tmpbuf[MAX_TMPBUF];
	char buf16[16 + 1];
	size_t total;
	unsigned int idx;

	fprintf(fp, "%s\n", label);

	memset(buf16, 0, sizeof buf16);
	total = 0;
	for (idx = 0; idx < len; idx++) {
		if (idx != 0 && idx % 16 == 0) {
			snprintf(tmpbuf + total, sizeof tmpbuf - total,
				 " %s", buf16);
			memset(buf16, 0, sizeof buf16);
			fprintf(fp, "%s\n", tmpbuf);
			total = 0;
		}
		if (idx % 16 == 0) {
			total += snprintf(tmpbuf + total, sizeof tmpbuf - total,
					  "%08x ", idx);
		}
		if (idx % 8 == 0) {
			total += snprintf(tmpbuf + total, sizeof tmpbuf - total,
					  "%s", " ");
		}
		total += snprintf(tmpbuf + total, sizeof tmpbuf - total,
				  "%2.2x ", buf[idx] & 0xff);
		buf16[idx % 16] = isprint(buf[idx]) ? buf[idx] : '.';
	}
	for (; idx % 16 != 0; idx++) {
		if (idx == 8) {
			total += snprintf(tmpbuf + total, sizeof tmpbuf - total,
					  " ");
		}

		total += snprintf(tmpbuf + total, sizeof tmpbuf - total, "   ");
	}
	snprintf(tmpbuf + total, sizeof tmpbuf - total, "  %s", buf16);
	fprintf(fp, "%s\n", tmpbuf);
	fflush(fp);
}

void
spdk_log_dump(FILE *fp, const char *label, const void *buf, size_t len)
{
	fdump(fp, label, buf, len);
}
