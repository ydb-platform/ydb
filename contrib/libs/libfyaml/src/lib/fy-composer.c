/*
 * fy-composer.c - Composer support
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"

#include "fy-utils.h"

struct fy_composer *
fy_composer_create(struct fy_composer_cfg *cfg)
{
	struct fy_composer *fyc;
	struct fy_path *fypp;

	/* verify configuration and mandatory ops */
	if (!cfg || !cfg->ops ||
	    !cfg->ops->process_event)
		return NULL;

	fyc = malloc(sizeof(*fyc));
	if (!fyc)
		return NULL;
	memset(fyc, 0, sizeof(*fyc));
	fyc->cfg = *cfg;

	fy_path_list_init(&fyc->paths);
	fypp = fy_path_create();
	if (!fypp)
		goto err_no_path;
	fy_path_list_add_tail(&fyc->paths, fypp);

	return fyc;

err_no_path:
	free(fyc);
	return NULL;
}

void fy_composer_destroy(struct fy_composer *fyc)
{
	struct fy_path *fypp;

	if (!fyc)
		return;

	fy_diag_unref(fyc->cfg.diag);
	while ((fypp = fy_path_list_pop(&fyc->paths)) != NULL)
		fy_path_destroy(fypp);
	free(fyc);
}

static enum fy_composer_return
fy_composer_process_event_private(struct fy_composer *fyc, struct fy_event *fye, struct fy_path *fypp)
{
	const struct fy_composer_ops *ops;
	struct fy_eventp *fyep;
	struct fy_path_component *fypc, *fypc_last;
	struct fy_path *fyppt;
	struct fy_document *fyd;
	bool is_collection, is_map, is_start, is_end;
	int rc = 0;
	enum fy_composer_return ret;
	bool stop_req = false;

	assert(fyc);
	assert(fye);
	assert(fypp);

	fyep = fy_container_of(fye, struct fy_eventp, e);

	ops = fyc->cfg.ops;
	assert(ops);

	rc = 0;

	switch (fye->type) {
	case FYET_MAPPING_START:
		is_collection = true;
		is_start = true;
		is_end = false;
		is_map = true;
		break;

	case FYET_MAPPING_END:
		is_collection = true;
		is_start = false;
		is_end = true;
		is_map = true;
		break;

	case FYET_SEQUENCE_START:
		is_collection = true;
		is_start = true;
		is_end = false;
		is_map = false;
		break;

	case FYET_SEQUENCE_END:
		is_collection = true;
		is_start = false;
		is_end = true;
		is_map = false;
		break;

	case FYET_SCALAR:
		is_collection = false;
		is_start = true;
		is_end = true;
		is_map = false;
		break;

	case FYET_ALIAS:
		is_collection = false;
		is_start = true;
		is_end = true;
		is_map = false;
		break;

	case FYET_STREAM_START:
	case FYET_STREAM_END:
	case FYET_DOCUMENT_START:
	case FYET_DOCUMENT_END:
		return ops->process_event(fyc, fypp, fye);

	default:
		return FYCR_OK_CONTINUE;
	}

	fypc_last = fy_path_component_list_tail(&fypp->components);

	if (fy_path_component_is_mapping(fypc_last) && fypc_last->map.accumulating_complex_key) {

		/* get the next one */
		fyppt = fy_path_next(&fyc->paths, fypp);
		assert(fyppt);
		assert(fyppt != fypp);
		assert(fyppt->parent == fypp);

		/* and pass along */
		ret = fy_composer_process_event_private(fyc, fye, fyppt);
		if (!fy_composer_return_is_ok(ret)) {
			/* XXX TODO handle skip */
			return ret;
		}
		if (!stop_req)
			stop_req = ret == FYCR_OK_STOP;

		rc = fy_document_builder_process_event(fypp->fydb, fyep);
		if (rc == 0)
			return FYCR_OK_CONTINUE;
		fyc_error_check(fyc, rc > 0, err_out,
				"fy_document_builder_process_event() failed\n");

		/* get the document */
		fyd = fy_document_builder_take_document(fypp->fydb);
		fyc_error_check(fyc, fyd, err_out,
				"fy_document_builder_take_document() failed\n");

		fypc_last->map.is_complex_key = true;
		fypc_last->map.accumulating_complex_key = false;
		fypc_last->map.complex_key = fyd;
		fypc_last->map.has_key = true;
		fypc_last->map.await_key = false;
		fypc_last->map.complex_key_complete = true;
		fypc_last->map.root = false;

		fyppt = fy_path_list_pop_tail(&fyc->paths);
		assert(fyppt);

		fy_path_destroy(fyppt);

		fyc_error_check(fyc, rc >= 0, err_out,
				"fy_path_component_build_text() failed\n");

		return !stop_req ? FYCR_OK_CONTINUE : FYCR_OK_STOP;
	}

	/* start of something on a mapping */
	if (is_start && fy_path_component_is_mapping(fypc_last) && fypc_last->map.await_key && is_collection) {

		/* the configuration must support a document builder for complex keys */
		FYC_TOKEN_ERROR_CHECK(fyc, fy_event_get_token(fye), FYEM_DOC,
				ops->create_document_builder, err_out,
				"composer configuration does not support complex keys");

		/* call out for creating the document builder */
		fypp->fydb = ops->create_document_builder(fyc);
		fyc_error_check(fyc, fypp->fydb, err_out,
				"ops->create_document_builder() failed\n");

		/* and pass the current event; must return 0 since we know it's a collection start */
		rc = fy_document_builder_process_event(fypp->fydb, fyep);
		fyc_error_check(fyc, !rc, err_out,
				"fy_document_builder_process_event() failed\n");

		fypc_last->map.is_complex_key = true;
		fypc_last->map.accumulating_complex_key = true;
		fypc_last->map.complex_key = NULL;
		fypc_last->map.complex_key_complete = false;

		/* create new path */
		fyppt = fy_path_create();
		fyc_error_check(fyc, fyppt, err_out,
				"fy_path_create() failed\n");

		/* append it to the end */
		fyppt->parent = fypp;
		fy_path_list_add_tail(&fyc->paths, fyppt);

		/* and pass along */
		ret = fy_composer_process_event_private(fyc, fye, fyppt);
		if (!fy_composer_return_is_ok(ret)) {
			/* XXX TODO handle skip */
			return ret;
		}
		if (!stop_req)
			stop_req = ret == FYCR_OK_STOP;

		return !stop_req ? FYCR_OK_CONTINUE : FYCR_OK_STOP;
	}

	if (is_start && fy_path_component_is_sequence(fypc_last)) {	/* start in a sequence */

		if (fypc_last->seq.idx < 0)
			fypc_last->seq.idx = 0;
		else
			fypc_last->seq.idx++;
	}

	if (is_collection && is_start) {

		/* collection start */
		if (is_map) {
			fypc = fy_path_component_create_mapping(fypp);
			fyc_error_check(fyc, fypc, err_out,
					"fy_path_component_create_mapping() failed\n");
		} else {
			fypc = fy_path_component_create_sequence(fypp);
			fyc_error_check(fyc, fypc, err_out,
					"fy_path_component_create_sequence() failed\n");
		}

		/* append to the tail */
		fy_path_component_list_add_tail(&fypp->components, fypc);

	} else if (is_collection && is_end) {

		/* collection end */
		assert(fypc_last);
		fy_path_component_clear_state(fypc_last);

	} else if (!is_collection && fy_path_component_is_mapping(fypc_last) && fypc_last->map.await_key) {

		fypc_last->map.is_complex_key = false;
		fypc_last->map.scalar.tag = fy_token_ref(fy_event_get_tag_token(fye));
		fypc_last->map.scalar.key = fy_token_ref(fy_event_get_token(fye));
		fypc_last->map.has_key = true;
		fypc_last->map.root = false;

	}

	/* process the event */
	ret = ops->process_event(fyc, fypp, fye);
	if (!fy_composer_return_is_ok(ret)) {
		/* XXX TODO handle skip */
		return ret;
	}
	if (!stop_req)
		stop_req = ret == FYCR_OK_STOP;

	if (is_collection && is_end) {
		/* for the end of a collection, pop the last component */
		fypc = fy_path_component_list_pop_tail(&fypp->components);
		assert(fypc);

		assert(fypc == fypc_last);

		fy_path_component_recycle(fypp, fypc);

		/* and get the new last */
		fypc_last = fy_path_component_list_tail(&fypp->components);
	}

	/* at the end of something */
	if (is_end && fy_path_component_is_mapping(fypc_last)) {
		if (!fypc_last->map.await_key) {
			fy_path_component_clear_state(fypc_last);
			fypc_last->map.await_key = true;
		} else
			fypc_last->map.await_key = false;
	}

	return !stop_req ? FYCR_OK_CONTINUE : FYCR_OK_STOP;

err_out:
	return FYCR_ERROR;
}

enum fy_composer_return
fy_composer_process_event(struct fy_composer *fyc, struct fy_event *fye)
{
	struct fy_path *fypp;
	int rc;

	if (!fyc || !fye)
		return -1;

	/* start at the head */
	fypp = fy_path_list_head(&fyc->paths);

	/* no top? something's very out of order */
	if (!fypp)
		return -1;

	rc = fy_composer_process_event_private(fyc, fye, fypp);

	return rc;
}

struct fy_composer_cfg *fy_composer_get_cfg(struct fy_composer *fyc)
{
	if (!fyc)
		return NULL;
	return &fyc->cfg;
}

void *fy_composer_get_cfg_userdata(struct fy_composer *fyc)
{
	if (!fyc)
		return NULL;
	return fyc->cfg.userdata;
}

struct fy_diag *fy_composer_get_diag(struct fy_composer *fyc)
{
	if (!fyc)
		return NULL;
	return fyc->cfg.diag;
}
