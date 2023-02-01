/*
 * fy-dump.h - dumps for various internal structures
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_DUMP_H
#define FY_DUMP_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-diag.h"

struct fy_parser;
struct fy_token;
struct fy_token_list;
struct fy_simple_key;
struct fy_simple_key_list;
struct fy_input_cfg;

extern const char *fy_token_type_txt[];

char *fy_token_dump_format(struct fy_token *fyt, char *buf, size_t bufsz);
char *fy_token_list_dump_format(struct fy_token_list *fytl,
		struct fy_token *fyt_highlight, char *buf, size_t bufsz);

char *fy_simple_key_dump_format(struct fy_parser *fyp, struct fy_simple_key *fysk, char *buf, size_t bufsz);
char *fy_simple_key_list_dump_format(struct fy_parser *fyp, struct fy_simple_key_list *fyskl,
		struct fy_simple_key *fysk_highlight, char *buf, size_t bufsz);

#ifdef FY_DEVMODE

void fyp_debug_dump_token_list(struct fy_parser *fyp, struct fy_token_list *fytl,
			       struct fy_token *fyt_highlight, const char *banner);
void fyp_debug_dump_token(struct fy_parser *fyp, struct fy_token *fyt, const char *banner);

void fyp_debug_dump_simple_key_list(struct fy_parser *fyp, struct fy_simple_key_list *fyskl,
				    struct fy_simple_key *fysk_highlight, const char *banner);
void fyp_debug_dump_simple_key(struct fy_parser *fyp, struct fy_simple_key *fysk, const char *banner);

void fyp_debug_dump_input(struct fy_parser *fyp, const struct fy_input_cfg *fyic,
			  const char *banner);

#else

static inline void
fyp_debug_dump_token_list(struct fy_parser *fyp, struct fy_token_list *fytl,
			  struct fy_token *fyt_highlight, const char *banner)
{
	/* nothing */
}

static inline void
fyp_debug_dump_token(struct fy_parser *fyp, struct fy_token *fyt, const char *banner)
{
	/* nothing */
}

static inline void
fyp_debug_dump_simple_key_list(struct fy_parser *fyp, struct fy_simple_key_list *fyskl,
			       struct fy_simple_key *fysk_highlight, const char *banner)
{
	/* nothing */
}

static inline void
fyp_debug_dump_simple_key(struct fy_parser *fyp, struct fy_simple_key *fysk, const char *banner)
{
	/* nothing */
}

static inline void
fy_debug_dump_input(struct fy_parser *fyp, const struct fy_input_cfg *fyic,
		    const char *banner)
{
	/* nothing */
}

#endif

#endif
