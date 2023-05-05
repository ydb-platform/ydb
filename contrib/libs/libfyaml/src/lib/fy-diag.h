/*
 * fy-diag.h - diagnostics
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_DIAG_H
#define FY_DIAG_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-token.h"

#if !defined(NDEBUG) && defined(HAVE_DEVMODE) && HAVE_DEVMODE
#define FY_DEVMODE
#else
#undef FY_DEVMODE
#endif

/* error flags (above 0x100 is library specific) */
#define FYEF_SOURCE	0x0001
#define FYEF_POSITION	0x0002
#define FYEF_TYPE	0x0004
#define FYEF_USERSTART	0x0100

#define FYDF_LEVEL_SHIFT	0
#define FYDF_LEVEL_MASK		(0x0f << FYDF_LEVEL_SHIFT)
#define FYDF_LEVEL(x)		(((unsigned int)(x) << FYDF_LEVEL_SHIFT) & FYDF_LEVEL_MASK)
#define FYDF_DEBUG		FYDF_LEVEL(FYET_DEBUG)
#define FYDF_INFO		FYDF_LEVEL(FYET_INFO)
#define FYDF_NOTICE		FYDF_LEVEL(FYET_NOTICE)
#define FYDF_WARNING		FYDF_LEVEL(FYET_WARNING)
#define FYDF_ERROR		FYDF_LEVEL(FYET_ERROR)

#define FYDF_MODULE_SHIFT	4
#define FYDF_MODULE_MASK	(0x0f << FYDF_MODULE_SHIFT)
#define FYDF_MODULE(x)		(((unsigned int)(x) << FYDF_MODULE_SHIFT) & FYDF_MODULE_MASK)
#define FYDF_ATOM		FYDF_MODULE(FYEM_ATOM)
#define FYDF_SCANNER		FYDF_MODULE(FYEM_SCANNER)
#define FYDF_PARSER		FYDF_MODULE(FYEM_PARSER)
#define FYDF_TREE		FYDF_MODULE(FYEM_TREE)
#define FYDF_BUILDER		FYDF_MODULE(FYEM_BUILDER)
#define FYDF_INTERNAL		FYDF_MODULE(FYEM_INTERNAL)
#define FYDF_SYSTEM		FYDF_MODULE(FYEM_SYSTEM)
#define FYDF_MODULE_USER_MASK	7
#define FYDF_MODULE_USER(x)	FYDF_MODULE(8 + ((x) & FYDF_MODULE_USER_MASK))

struct fy_diag_term_info {
	int rows;
	int columns;
};

struct fy_diag_report_ctx {
	enum fy_error_type type;
	enum fy_error_module module;
	struct fy_token *fyt;
	bool has_override;
	const char *override_file;
	int override_line;
	int override_column;
};

FY_TYPE_FWD_DECL_LIST(diag_errorp);
struct fy_diag_errorp {
	struct fy_list_head node;
	char *space;
	struct fy_diag_error e;
};
FY_TYPE_DECL_LIST(diag_errorp);

struct fy_diag {
	struct fy_diag_cfg cfg;
	int refs;
	bool on_error : 1;
	bool destroyed : 1;
	bool collect_errors : 1;
	bool terminal_probed : 1;
	struct fy_diag_term_info term_info;
	struct fy_diag_errorp_list errors;
};

void fy_diag_free(struct fy_diag *diag);

void fy_diag_vreport(struct fy_diag *diag,
		     const struct fy_diag_report_ctx *fydrc,
		     const char *fmt, va_list ap);
void fy_diag_report(struct fy_diag *diag,
		    const struct fy_diag_report_ctx *fydrc,
		    const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE
#define __FY_DEBUG_UNUSED__	/* nothing */
#else
#define __FY_DEBUG_UNUSED__	FY_ATTRIBUTE(__unused__)
#endif

/* parser diagnostics */

struct fy_parser;

void fy_diag_cfg_from_parser_flags(struct fy_diag_cfg *cfg, enum fy_parse_cfg_flags pflags);

int fy_parser_vdiag(struct fy_parser *fyp, unsigned int flags,
		    const char *file, int line, const char *func,
		    const char *fmt, va_list ap);

int fy_parser_diag(struct fy_parser *fyp, unsigned int flags,
		   const char *file, int line, const char *func,
		   const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 6, 7));

void fy_diag_error_atom_display(struct fy_diag *diag, enum fy_error_type type,
				 struct fy_atom *atom);
void fy_diag_error_token_display(struct fy_diag *diag, enum fy_error_type type,
				 struct fy_token *fyt);

void fy_parser_diag_vreport(struct fy_parser *fyp,
			    const struct fy_diag_report_ctx *fydrc,
			    const char *fmt, va_list ap);
void fy_parser_diag_report(struct fy_parser *fyp,
			   const struct fy_diag_report_ctx *fydrc,
			   const char *fmt, ...)
		FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE

#define fyp_debug(_fyp, _module, _fmt, ...) \
	fy_parser_diag((_fyp), FYET_DEBUG | FYDF_MODULE(_module), \
			__FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#else

#define fyp_debug(_fyp, _module, _fmt, ...) \
	do { } while(0)

#endif

#define fyp_info(_fyp, _fmt, ...) \
	fy_parser_diag((_fyp), FYET_INFO, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyp_notice(_fyp, _fmt, ...) \
	fy_parser_diag((_fyp), FYET_NOTICE, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyp_warning(_fyp, _fmt, ...) \
	fy_parser_diag((_fyp), FYET_WARNING, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyp_error(_fyp, _fmt, ...) \
	fy_parser_diag((_fyp), FYET_ERROR, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#define fyp_scan_debug(_fyp, _fmt, ...) \
	fyp_debug((_fyp), FYEM_SCAN, (_fmt) , ## __VA_ARGS__)
#define fyp_parse_debug(_fyp, _fmt, ...) \
	fyp_debug((_fyp), FYEM_PARSE, (_fmt) , ## __VA_ARGS__)
#define fyp_doc_debug(_fyp, _fmt, ...) \
	fyp_debug((_fyp), FYEM_DOC, (_fmt) , ## __VA_ARGS__)

#define fyp_error_check(_fyp, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			fyp_error((_fyp), _fmt, ## __VA_ARGS__); \
			goto _label ; \
		} \
	} while(0)

#define _FYP_TOKEN_DIAG(_fyp, _fyt, _type, _module, _fmt, ...) \
	do { \
		struct fy_diag_report_ctx _drc; \
		memset(&_drc, 0, sizeof(_drc)); \
		_drc.type = (_type); \
		_drc.module = (_module); \
		_drc.fyt = (_fyt); \
		fy_parser_diag_report((_fyp), &_drc, (_fmt) , ## __VA_ARGS__); \
	} while(0)

#define FYP_TOKEN_DIAG(_fyp, _fyt, _type, _module, _fmt, ...) \
	_FYP_TOKEN_DIAG(_fyp, fy_token_ref(_fyt), _type, _module, _fmt, ## __VA_ARGS__)

#define FYP_PARSE_DIAG(_fyp, _adv, _cnt, _type, _module, _fmt, ...) \
	_FYP_TOKEN_DIAG(_fyp, \
		fy_token_create(FYTT_INPUT_MARKER, \
			fy_fill_atom_at((_fyp), (_adv), (_cnt), \
			FY_ALLOCA(sizeof(struct fy_atom)))), \
		_type, _module, _fmt, ## __VA_ARGS__)

#define FYP_MARK_DIAG(_fyp, _sm, _em, _type, _module, _fmt, ...) \
	_FYP_TOKEN_DIAG(_fyp, \
		fy_token_create(FYTT_INPUT_MARKER, \
			fy_fill_atom_mark(((_fyp)), (_sm), (_em), \
				FY_ALLOCA(sizeof(struct fy_atom)))), \
		_type, _module, _fmt, ## __VA_ARGS__)

#define FYP_NODE_DIAG(_fyp, _fyn, _type, _module, _fmt, ...) \
	_FYP_TOKEN_DIAG(_fyp, fy_node_token(_fyn), _type, _module, _fmt, ## __VA_ARGS__)

#define FYP_TOKEN_ERROR(_fyp, _fyt, _module, _fmt, ...) \
	FYP_TOKEN_DIAG(_fyp, _fyt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYP_PARSE_ERROR(_fyp, _adv, _cnt, _module, _fmt, ...) \
	FYP_PARSE_DIAG(_fyp, _adv, _cnt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYP_MARK_ERROR(_fyp, _sm, _em, _module, _fmt, ...) \
	FYP_MARK_DIAG(_fyp, _sm, _em, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYP_NODE_ERROR(_fyp, _fyn, _module, _fmt, ...) \
	FYP_NODE_DIAG(_fyp, _fyn, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYP_TOKEN_ERROR_CHECK(_fyp, _fyt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYP_TOKEN_ERROR(_fyp, _fyt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYP_PARSE_ERROR_CHECK(_fyp, _adv, _cnt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYP_PARSE_ERROR(_fyp, _adv, _cnt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYP_MARK_ERROR_CHECK(_fyp, _sm, _em, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYP_MARK_ERROR(_fyp, _sm, _em, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYP_NODE_ERROR_CHECK(_fyp, _fyn, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYP_NODE_ERROR(_fyp, _fyn, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYP_TOKEN_WARNING(_fyp, _fyt, _module, _fmt, ...) \
	FYP_TOKEN_DIAG(_fyp, _fyt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYP_PARSE_WARNING(_fyp, _adv, _cnt, _module, _fmt, ...) \
	FYP_PARSE_DIAG(_fyp, _adv, _cnt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYP_MARK_WARNING(_fyp, _sm, _em, _module, _fmt, ...) \
	FYP_MARK_DIAG(_fyp, _sm, _em, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYP_NODE_WARNING(_fyp, _fyn, _type, _module, _fmt, ...) \
	FYP_NODE_DIAG(_fyp, _fyn, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

/* reader diagnostics */

struct fy_reader;

int fy_reader_vdiag(struct fy_reader *fyr, unsigned int flags,
		    const char *file, int line, const char *func,
		    const char *fmt, va_list ap);

int fy_reader_diag(struct fy_reader *fyr, unsigned int flags,
		   const char *file, int line, const char *func,
		   const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 6, 7));

void fy_reader_diag_vreport(struct fy_reader *fyr,
			    const struct fy_diag_report_ctx *fydrc,
			    const char *fmt, va_list ap);
void fy_reader_diag_report(struct fy_reader *fyr,
			   const struct fy_diag_report_ctx *fydrc,
			   const char *fmt, ...)
		FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE

#define fyr_debug(_fyr, _fmt, ...) \
	fy_reader_diag((_fyr), FYET_DEBUG, \
			__FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#else

#define fyr_debug(_fyr, _fmt, ...) \
	do { } while(0)

#endif

#define fyr_info(_fyr, _fmt, ...) \
	fy_reader_diag((_fyr), FYET_INFO, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyr_notice(_fyr, _fmt, ...) \
	fy_reader_diag((_fyr), FYET_NOTICE, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyr_warning(_fyr, _fmt, ...) \
	fy_reader_diag((_fyr), FYET_WARNING, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyr_error(_fyr, _fmt, ...) \
	fy_reader_diag((_fyr), FYET_ERROR, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#define fyr_error_check(_fyr, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			fyr_error((_fyr), _fmt, ## __VA_ARGS__); \
			goto _label ; \
		} \
	} while(0)

#define _FYR_TOKEN_DIAG(_fyr, _fyt, _type, _module, _fmt, ...) \
	do { \
		struct fy_diag_report_ctx _drc; \
		memset(&_drc, 0, sizeof(_drc)); \
		_drc.type = (_type); \
		_drc.module = (_module); \
		_drc.fyt = (_fyt); \
		fy_reader_diag_report((_fyr), &_drc, (_fmt) , ## __VA_ARGS__); \
	} while(0)

#define FYR_TOKEN_DIAG(_fyr, _fyt, _type, _module, _fmt, ...) \
	_FYR_TOKEN_DIAG(_fyr, fy_token_ref(_fyt), _type, _module, _fmt, ## __VA_ARGS__)

#define FYR_PARSE_DIAG(_fyr, _adv, _cnt, _type, _module, _fmt, ...) \
	_FYR_TOKEN_DIAG(_fyr, \
		fy_token_create(FYTT_INPUT_MARKER, \
			fy_reader_fill_atom_at((_fyr), (_adv), (_cnt), \
			FY_ALLOCA(sizeof(struct fy_atom)))), \
		_type, _module, _fmt, ## __VA_ARGS__)

#define FYR_MARK_DIAG(_fyr, _sm, _em, _type, _module, _fmt, ...) \
	_FYR_TOKEN_DIAG(_fyr, \
		fy_token_create(FYTT_INPUT_MARKER, \
			fy_reader_fill_atom_mark(((_fyr)), (_sm), (_em), \
				FY_ALLOCA(sizeof(struct fy_atom)))), \
		_type, _module, _fmt, ## __VA_ARGS__)

#define FYR_NODE_DIAG(_fyr, _fyn, _type, _module, _fmt, ...) \
	_FYR_TOKEN_DIAG(_fyr, fy_node_token(_fyn), _type, _module, _fmt, ## __VA_ARGS__)

#define FYR_TOKEN_ERROR(_fyr, _fyt, _module, _fmt, ...) \
	FYR_TOKEN_DIAG(_fyr, _fyt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYR_PARSE_ERROR(_fyr, _adv, _cnt, _module, _fmt, ...) \
	FYR_PARSE_DIAG(_fyr, _adv, _cnt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYR_MARK_ERROR(_fyr, _sm, _em, _module, _fmt, ...) \
	FYR_MARK_DIAG(_fyr, _sm, _em, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYR_NODE_ERROR(_fyr, _fyn, _module, _fmt, ...) \
	FYR_NODE_DIAG(_fyr, _fyn, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYR_TOKEN_ERROR_CHECK(_fyr, _fyt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYR_TOKEN_ERROR(_fyr, _fyt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYR_PARSE_ERROR_CHECK(_fyr, _adv, _cnt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYR_PARSE_ERROR(_fyr, _adv, _cnt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYR_MARK_ERROR_CHECK(_fyr, _sm, _em, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYR_MARK_ERROR(_fyr, _sm, _em, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYR_NODE_ERROR_CHECK(_fyr, _fyn, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYR_NODE_ERROR(_fyr, _fyn, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYR_TOKEN_WARNING(_fyr, _fyt, _module, _fmt, ...) \
	FYR_TOKEN_DIAG(_fyr, _fyt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYR_PARSE_WARNING(_fyr, _adv, _cnt, _module, _fmt, ...) \
	FYR_PARSE_DIAG(_fyr, _adv, _cnt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYR_MARK_WARNING(_fyr, _sm, _em, _module, _fmt, ...) \
	FYR_MARK_DIAG(_fyr, _sm, _em, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYR_NODE_WARNING(_fyr, _fyn, _type, _module, _fmt, ...) \
	FYR_NODE_DIAG(_fyr, _fyn, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

/* doc */
struct fy_document;

int fy_document_vdiag(struct fy_document *fyd, unsigned int flags,
		      const char *file, int line, const char *func,
		      const char *fmt, va_list ap);

int fy_document_diag(struct fy_document *fyd, unsigned int flags,
		     const char *file, int line, const char *func,
		     const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 6, 7));

void fy_document_diag_vreport(struct fy_document *fyd,
			      const struct fy_diag_report_ctx *fydrc,
			      const char *fmt, va_list ap);
void fy_document_diag_report(struct fy_document *fyd,
			     const struct fy_diag_report_ctx *fydrc,
			     const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE

#define fyd_debug(_fyd, _module, _fmt, ...) \
	fy_document_diag((_fyd), FYET_DEBUG | FYDF_MODULE(_module), \
			__FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#else

#define fyd_debug(_fyd, _module, _fmt, ...) \
	do { } while(0)

#endif

#define fyd_info(_fyd, _fmt, ...) \
	fy_document_diag((_fyd), FYET_INFO, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyd_notice(_fyd, _fmt, ...) \
	fy_document_diag((_fyd), FYET_NOTICE, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyd_warning(_fyd, _fmt, ...) \
	fy_document_diag((_fyd), FYET_WARNING, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyd_error(_fyd, _fmt, ...) \
	fy_document_diag((_fyd), FYET_ERROR, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#define fyd_doc_debug(_fyd, _fmt, ...) \
	fyd_debug((_fyd), FYEM_DOC, (_fmt) , ## __VA_ARGS__)

#define fyd_error_check(_fyd, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			fyd_error((_fyd), _fmt, ## __VA_ARGS__); \
			goto _label ; \
		} \
	} while(0)

#define _FYD_TOKEN_DIAG(_fyd, _fyt, _type, _module, _fmt, ...) \
	do { \
		struct fy_diag_report_ctx _drc; \
		memset(&_drc, 0, sizeof(_drc)); \
		_drc.type = (_type); \
		_drc.module = (_module); \
		_drc.fyt = (_fyt); \
		fy_document_diag_report((_fyd), &_drc, (_fmt) , ## __VA_ARGS__); \
	} while(0)

#define FYD_TOKEN_DIAG(_fyd, _fyt, _type, _module, _fmt, ...) \
	_FYD_TOKEN_DIAG(_fyd, fy_token_ref(_fyt), _type, _module, _fmt, ## __VA_ARGS__)

#define FYD_NODE_DIAG(_fyd, _fyn, _type, _module, _fmt, ...) \
	_FYD_TOKEN_DIAG(_fyd, fy_node_token(_fyn), _type, _module, _fmt, ## __VA_ARGS__)

#define FYD_TOKEN_ERROR(_fyd, _fyt, _module, _fmt, ...) \
	FYD_TOKEN_DIAG(_fyd, _fyt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYD_NODE_ERROR(_fyd, _fyn, _module, _fmt, ...) \
	FYD_NODE_DIAG(_fyd, _fyn, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYD_TOKEN_ERROR_CHECK(_fyd, _fyt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYD_TOKEN_ERROR(_fyd, _fyt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYD_NODE_ERROR_CHECK(_fyd, _fyn, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYD_NODE_ERROR(_fyd, _fyn, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYD_TOKEN_WARNING(_fyd, _fyt, _module, _fmt, ...) \
	FYD_TOKEN_DIAG(_fyd, _fyt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

#define FYD_NODE_WARNING(_fyd, _fyn, _type, _module, _fmt, ...) \
	FYD_NODE_DIAG(_fyd, _fyn, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

/* composer */
struct fy_composer;

int fy_composer_vdiag(struct fy_composer *fyc, unsigned int flags,
		      const char *file, int line, const char *func,
		      const char *fmt, va_list ap);

int fy_composer_diag(struct fy_composer *fyc, unsigned int flags,
		     const char *file, int line, const char *func,
		     const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 6, 7));

void fy_composer_diag_vreport(struct fy_composer *fyc,
			      const struct fy_diag_report_ctx *fydrc,
			      const char *fmt, va_list ap);
void fy_composer_diag_report(struct fy_composer *fyc,
			     const struct fy_diag_report_ctx *fydrc,
			     const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE

#define fyc_debug(_fyc, _module, _fmt, ...) \
	fy_composer_diag((_fyc), FYET_DEBUG | FYDF_MODULE(_module), \
			__FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#else

#define fyc_debug(_fyc, _module, _fmt, ...) \
	do { } while(0)

#endif

#define fyc_info(_fyc, _fmt, ...) \
	fy_composer_diag((_fyc), FYET_INFO, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyc_notice(_fyc, _fmt, ...) \
	fy_composer_diag((_fyc), FYET_NOTICE, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyc_warning(_fyc, _fmt, ...) \
	fy_composer_diag((_fyc), FYET_WARNING, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fyc_error(_fyc, _fmt, ...) \
	fy_composer_diag((_fyc), FYET_ERROR, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#define fyc_error_check(_fyc, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			fyc_error((_fyc), _fmt, ## __VA_ARGS__); \
			goto _label ; \
		} \
	} while(0)

#define _FYC_TOKEN_DIAG(_fyc, _fyt, _type, _module, _fmt, ...) \
	do { \
		struct fy_diag_report_ctx _drc; \
		memset(&_drc, 0, sizeof(_drc)); \
		_drc.type = (_type); \
		_drc.module = (_module); \
		_drc.fyt = (_fyt); \
		fy_composer_diag_report((_fyc), &_drc, (_fmt) , ## __VA_ARGS__); \
	} while(0)

#define FYC_TOKEN_DIAG(_fyc, _fyt, _type, _module, _fmt, ...) \
	_FYC_TOKEN_DIAG(_fyc, fy_token_ref(_fyt), _type, _module, _fmt, ## __VA_ARGS__)

#define FYC_NODE_DIAG(_fyc, _fyn, _type, _module, _fmt, ...) \
	_FYC_TOKEN_DIAG(_fyc, fy_node_token(_fyn), _type, _module, _fmt, ## __VA_ARGS__)

#define FYC_TOKEN_ERROR(_fyc, _fyt, _module, _fmt, ...) \
	FYC_TOKEN_DIAG(_fyc, _fyt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYC_TOKEN_ERROR_CHECK(_fyc, _fyt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYC_TOKEN_ERROR(_fyc, _fyt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYC_TOKEN_WARNING(_fyc, _fyt, _module, _fmt, ...) \
	FYC_TOKEN_DIAG(_fyc, _fyt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

/* document builder */
struct fy_document_builder;

int fy_document_builder_vdiag(struct fy_document_builder *fydb, unsigned int flags,
			      const char *file, int line, const char *func,
			      const char *fmt, va_list ap);

int fy_document_builder_diag(struct fy_document_builder *fydb, unsigned int flags,
			     const char *file, int line, const char *func,
			     const char *fmt, ...)
			FY_ATTRIBUTE(format(printf, 6, 7));

void fy_document_builder_diag_vreport(struct fy_document_builder *fydb,
				      const struct fy_diag_report_ctx *fydrc,
				      const char *fmt, va_list ap);
void fy_document_builder_diag_report(struct fy_document_builder *fydb,
				     const struct fy_diag_report_ctx *fydrc,
				     const char *fmt, ...)
				FY_ATTRIBUTE(format(printf, 3, 4));

#ifdef FY_DEVMODE

#define fydb_debug(_fydb, _module, _fmt, ...) \
	fy_document_builder_diag((_fydb), FYET_DEBUG | FYDF_MODULE(_module), \
			__FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#else

#define fydb_debug(_fydb, _module, _fmt, ...) \
	do { } while(0)

#endif

#define fydb_info(_fydb, _fmt, ...) \
	fy_document_builder_diag((_fydb), FYET_INFO, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fydb_notice(_fydb, _fmt, ...) \
	fy_document_builder_diag((_fydb), FYET_NOTICE, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fydb_warning(_fydb, _fmt, ...) \
	fy_document_builder_diag((_fydb), FYET_WARNING, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)
#define fydb_error(_fydb, _fmt, ...) \
	fy_document_builder_diag((_fydb), FYET_ERROR, __FILE__, __LINE__, __func__, \
			(_fmt) , ## __VA_ARGS__)

#define fydb_error_check(_fydb, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			fydb_error((_fydb), _fmt, ## __VA_ARGS__); \
			goto _label ; \
		} \
	} while(0)

#define _FYDB_TOKEN_DIAG(_fydb, _fyt, _type, _module, _fmt, ...) \
	do { \
		struct fy_diag_report_ctx _drc; \
		memset(&_drc, 0, sizeof(_drc)); \
		_drc.type = (_type); \
		_drc.module = (_module); \
		_drc.fyt = (_fyt); \
		fy_document_builder_diag_report((_fydb), &_drc, (_fmt) , ## __VA_ARGS__); \
	} while(0)

#define FYDB_TOKEN_DIAG(_fydb, _fyt, _type, _module, _fmt, ...) \
	_FYDB_TOKEN_DIAG(_fydb, fy_token_ref(_fyt), _type, _module, _fmt, ## __VA_ARGS__)

#define FYDB_NODE_DIAG(_fydb, _fyn, _type, _module, _fmt, ...) \
	_FYDB_TOKEN_DIAG(_fydb, fy_node_token(_fyn), _type, _module, _fmt, ## __VA_ARGS__)

#define FYDB_TOKEN_ERROR(_fydb, _fyt, _module, _fmt, ...) \
	FYDB_TOKEN_DIAG(_fydb, _fyt, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYDB_NODE_ERROR(_fydb, _fyn, _module, _fmt, ...) \
	FYDB_NODE_DIAG(_fydb, _fyn, FYET_ERROR, _module, _fmt, ## __VA_ARGS__)

#define FYDB_TOKEN_ERROR_CHECK(_fydb, _fyt, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYDB_TOKEN_ERROR(_fydb, _fyt, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYDB_NODE_ERROR_CHECK(_fydb, _fyn, _module, _cond, _label, _fmt, ...) \
	do { \
		if (!(_cond)) { \
			FYDB_NODE_ERROR(_fydb, _fyn, _module, _fmt, ## __VA_ARGS__); \
			goto _label; \
		} \
	} while(0)

#define FYDB_TOKEN_WARNING(_fydb, _fyt, _module, _fmt, ...) \
	FYDB_TOKEN_DIAG(_fydb, _fyt, FYET_WARNING, _module, _fmt, ## __VA_ARGS__)

/* alloca formatted print methods */
#define alloca_vsprintf(_res, _fmt, _ap) \
	do { \
		const char *__fmt = (_fmt); \
		va_list _ap_orig; \
		int _size; \
		int _sizew __FY_DEBUG_UNUSED__; \
		char *_buf = NULL, *_s; \
		\
		va_copy(_ap_orig, (_ap)); \
		_size = vsnprintf(NULL, 0, __fmt, _ap_orig); \
		va_end(_ap_orig); \
		if (_size != -1) { \
			_buf = FY_ALLOCA(_size + 1); \
			_sizew = vsnprintf(_buf, _size + 1, __fmt, _ap); \
			assert(_size == _sizew); \
			_s = _buf + strlen(_buf); \
			while (_s > _buf && _s[-1] == '\n') \
				*--_s = '\0'; \
		} \
		*(_res) = _buf; \
	} while(false)

#define alloca_sprintf(_res, _fmt, ...) \
	do { \
		const char *__fmt = (_fmt); \
		int _size; \
		int _sizew __FY_DEBUG_UNUSED__; \
		char *_buf = NULL, *_s; \
		\
		_size = snprintf(NULL, 0, __fmt, ## __VA_ARGS__); \
		if (_size != -1) { \
			_buf = FY_ALLOCA(_size + 1); \
			_sizew = snprintf(_buf, _size + 1, __fmt, __VA_ARGS__); \
			assert(_size == _sizew); \
			_s = _buf + strlen(_buf); \
			while (_s > _buf && _s[-1] == '\n') \
				*--_s = '\0'; \
		} \
		*(_res) = _buf; \
	} while(false)

#endif
