#pragma once

#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "vhost/server.h"

/* Normally we pass LOG_VERBOSITY from make */
#ifndef LOG_VERBOSITY
#   define LOG_VERBOSITY LOG_INFO
#endif

/* Log function for tests */
static const char *const log_level_str[] = {
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
};

__attribute__((format(printf, 2, 3)))
static inline void vhd_log_stderr(enum LogLevel level, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    if (level <= LOG_VERBOSITY) {
        char timestr[64];
        struct timeval tv;

        gettimeofday(&tv, NULL);
        strftime(timestr, sizeof(timestr), "%F %T", localtime(&tv.tv_sec));
        fprintf(stderr, "%s.%03ld [%8s] ", timestr, tv.tv_usec / 1000,
                log_level_str[level]);
        vfprintf(stderr, fmt, args);
        fprintf(stderr, "\n");
    }
    va_end(args);
}
