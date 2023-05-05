/*
 * fy-composer.h - YAML composer
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_COMPOSER_H
#define FY_COMPOSER_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdbool.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-typelist.h"

#include "fy-emit-accum.h"
#include "fy-path.h"

struct fy_composer;
struct fy_token;
struct fy_diag;
struct fy_event;
struct fy_eventp;
struct fy_document_builder;

struct fy_composer_ops {
	/* single process event callback */
	enum fy_composer_return (*process_event)(struct fy_composer *fyc, struct fy_path *path, struct fy_event *fye);
	struct fy_document_builder *(*create_document_builder)(struct fy_composer *fyc);
};

struct fy_composer_cfg {
	const struct fy_composer_ops *ops;
	void *userdata;
	struct fy_diag *diag;
};

struct fy_composer {
	struct fy_composer_cfg cfg;
	struct fy_path_list paths;
};

struct fy_composer *fy_composer_create(struct fy_composer_cfg *cfg);
void fy_composer_destroy(struct fy_composer *fyc);
int fy_composer_process_event(struct fy_composer *fyc, struct fy_event *fye);

struct fy_composer_cfg *fy_composer_get_cfg(struct fy_composer *fyc);
void *fy_composer_get_cfg_userdata(struct fy_composer *fyc);
struct fy_diag *fy_composer_get_diag(struct fy_composer *fyc);

#endif
