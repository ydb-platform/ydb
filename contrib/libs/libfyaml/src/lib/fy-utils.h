/*
 * fy-utils.h - internal utilities header file
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */

#ifndef FY_UTILS_H
#define FY_UTILS_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdbool.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#include <termios.h>
#endif

#if defined(__APPLE__) && (_POSIX_C_SOURCE < 200809L)
FILE *open_memstream(char **ptr, size_t *sizeloc);
#endif

int fy_get_pagesize();

#if defined(_MSC_VER)
int vasprintf(char **strp, const char *fmt, va_list ap);
int asprintf(char **strp, const char *fmt, ...);
#endif

int fy_tag_handle_length(const char *data, size_t len);
bool fy_tag_uri_is_valid(const char *data, size_t len);
int fy_tag_uri_length(const char *data, size_t len);

struct fy_tag_scan_info {
	int total_length;
	int handle_length;
	int uri_length;
	int prefix_length;
	int suffix_length;
};

int fy_tag_scan(const char *data, size_t len, struct fy_tag_scan_info *info);

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(x) ((sizeof(x)/sizeof((x)[0])))
#endif

#if !defined(NDEBUG) && (defined(__GNUC__) && __GNUC__ >= 4)
#define FY_ALWAYS_INLINE __attribute__((always_inline))
#else
#define FY_ALWAYS_INLINE /* nothing */
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define FY_UNUSED __attribute__((unused))
#else
#define FY_UNUSED /* nothing */
#endif

int fy_term_set_raw(int fd, struct termios *oldt);
int fy_term_restore(int fd, const struct termios *oldt);
ssize_t fy_term_write(int fd, const void *data, size_t count);
int fy_term_safe_write(int fd, const void *data, size_t count);
ssize_t fy_term_read(int fd, void *data, size_t count, int timeout_us);
ssize_t fy_term_read_escape(int fd, void *buf, size_t count);

/* the raw methods require the terminal to be in raw mode */
int fy_term_query_size_raw(int fd, int *rows, int *cols);

/* the non raw methods will set the terminal to raw and then restore */
int fy_term_query_size(int fd, int *rows, int *cols);

#endif
