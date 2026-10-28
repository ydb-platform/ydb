/*
 * Manually maintained Windows configuration for gdk-pixbuf.
 * Do not remove — this file is preserved across yamaker re-imports via keep_paths.
 */

#pragma once

#define GETTEXT_PACKAGE "gdk-pixbuf"

#define HAVE_BIND_TEXTDOMAIN_CODESET 1

#define HAVE_LRINT 1

#define HAVE_PROGRESSIVE_JPEG

#define HAVE_ROUND 1

/* HAVE_SETRLIMIT — not available on Windows */
/* HAVE_SIGSETJMP — not available on Windows; io-jpeg.c falls back to jmp_buf */
/* HAVE_SYS_RESOURCE_H — not available on Windows */
/* HAVE_SYS_TIME_H — not available on Windows */
/* HAVE_UNISTD_H — not available on Windows */
/* OS_LINUX — not defined on Windows */
/* USE_GMODULE — not used on Windows; builtin loaders only */
