/*
 * libfyaml.h - Main header file of the public interface
 *
 * Copyright (c) 2019-2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 */

#ifndef LIBFYAML_H
#define LIBFYAML_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#define FY_ALLOCA(x) alloca(x)
#elif defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#define FY_ALLOCA(x) _alloca(x)
#endif

/* opaque types for the user */
struct fy_token;
struct fy_document_state;
struct fy_parser;
struct fy_emitter;
struct fy_document;
struct fy_node;
struct fy_node_pair;
struct fy_anchor;
struct fy_node_mapping_sort_ctx;
struct fy_token_iter;
struct fy_diag;
struct fy_path_parser;
struct fy_path_expr;
struct fy_path_exec;
struct fy_path_component;
struct fy_path;
struct fy_document_iterator;


#ifndef FY_BIT
#define FY_BIT(x) (1U << (x))
#endif

/* NULL terminated string length specifier */
#define FY_NT	((size_t)-1)

#if defined(__GNUC__) && __GNUC__ >= 4
#define FY_ATTRIBUTE(attr) __attribute ((attr))
#define FY_EXPORT __attribute__ ((visibility ("default")))
#define FY_DEPRECATED __attribute__ ((deprecated))
#else
#define FY_ATTRIBUTE(attr) /* nothing */
#define FY_EXPORT /* nothing */
#define FY_DEPRECATED /* nothing */
#endif

/* make a copy of an allocated string and return it on stack
 * note that this is a convenience, and should not be called
 * in a loop. The string is always '\0' terminated.
 * If the _str pointer is NULL, then NULL will be returned
 */
#ifndef FY_ALLOCA_COPY_FREE
#define FY_ALLOCA_COPY_FREE(_str, _len, _res)			\
	do {							\
		char *__res, *__str = (char*)(_str);		\
	        size_t __len = (size_t)(_len);			\
		__res = NULL;					\
								\
		if (__str) {					\
			if (__len == FY_NT)			\
				__len = strlen(__str);		\
			__res = FY_ALLOCA(__len + 1);		\
			memcpy(__res, __str, __len);		\
			(__res[__len]) = '\0';			\
			free(__str);				\
		}						\
	        *(_res) = __res;				\
	} while(false)
#endif

/* same as above but when _str == NULL return "" */
#ifndef FY_ALLOCA_COPY_FREE_NO_NULL
#define FY_ALLOCA_COPY_FREE_NO_NULL(_str, _len, _res)		\
	do {							\
		FY_ALLOCA_COPY_FREE(_str, _len, _res);  	\
		if (!*(_res))					\
			*(_res) = "";				\
	} while(false)
#endif

/**
 * DOC: libfyaml public API
 *
 */

/**
 * struct fy_version - The YAML version
 *
 * @major: Major version number
 * @minor: Major version number
 *
 * The parser fills it according to the \%YAML directive
 * found in the document.
 */
struct fy_version {
	int major;
	int minor;
};

/* Build a fy_version * from the given major and minor */
#define fy_version_make(_maj, _min) (&(struct fy_version){ (_maj), (_min) })

/**
 * fy_version_compare() - Compare two yaml versions
 *
 * Compare the versions
 *
 * @va: The first version, if NULL use default version
 * @vb: The second version, if NULL use default version
 *
 * Returns:
 * 0 if versions are equal, > 0 if version va is higher than vb
 * < 0 if version va is lower than vb
 */
int
fy_version_compare(const struct fy_version *va, const struct fy_version *vb)
	FY_EXPORT;

/**
 * fy_version_default() - Get the default version of the library
 *
 * Return the default version of the library, i.e. the highest
 * stable version that is supported.
 *
 * Returns:
 * The default YAML version of this library
 */
const struct fy_version *
fy_version_default(void)
	FY_EXPORT;

/**
 * fy_version_is_supported() - Check if supported version
 *
 * Check if the given YAML version is supported.
 *
 * @vers: The version to check, NULL means default version.
 *
 * Returns:
 * true if version supported, false otherwise.
 */
bool
fy_version_is_supported(const struct fy_version *vers)
	FY_EXPORT;

/**
 * fy_version_supported_iterate() - Iterate over the supported YAML versions
 *
 * This method iterates over the supported YAML versions of this ibrary.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @prevp: The previous version iterator
 *
 * Returns:
 * The next node in sequence or NULL at the end of the sequence.
 */
const struct fy_version *
fy_version_supported_iterate(void **prevp)
	FY_EXPORT;

/**
 * struct fy_tag - The YAML tag structure.
 *
 * @handle: Handle of the tag (i.e. `"!!"` )
 * @prefix: The prefix of the tag (i.e. `"tag:yaml.org,2002:"`
 *
 * The parser fills it according to the \%TAG directives
 * encountered during parsing.
 */
struct fy_tag {
	const char *handle;
	const char *prefix;
};

/**
 * struct fy_mark - marker holding information about a location
 *		    in a &struct fy_input
 *
 * @input_pos: Position of the mark (from the start of the current input)
 * @line: Line position (0 index based)
 * @column: Column position (0 index based)
 */
struct fy_mark {
	size_t input_pos;
	int line;
	int column;
};

/**
 * enum fy_error_type - The supported diagnostic/error types
 *
 * @FYET_DEBUG: Debug level (disabled if library is not compiled in debug mode)
 * @FYET_INFO: Informational level
 * @FYET_NOTICE: Notice level
 * @FYET_WARNING: Warning level
 * @FYET_ERROR: Error level - error reporting is using this level
 * @FYET_MAX: Non inclusive maximum fy_error_type value
 *
 */
enum fy_error_type {
	FYET_DEBUG,
	FYET_INFO,
	FYET_NOTICE,
	FYET_WARNING,
	FYET_ERROR,
	FYET_MAX,
};

/**
 * enum fy_error_module - Module which generated the diagnostic/error
 *
 * @FYEM_UNKNOWN: Unknown, default if not specific
 * @FYEM_ATOM: Atom module, used by atom chunking
 * @FYEM_SCAN: Scanner module
 * @FYEM_PARSE: Parser module
 * @FYEM_DOC: Document module
 * @FYEM_BUILD: Build document module (after tree is constructed)
 * @FYEM_INTERNAL: Internal error/diagnostic module
 * @FYEM_SYSTEM: System error/diagnostic module
 * @FYEM_MAX: Non inclusive maximum fy_error_module value
 */
enum fy_error_module {
	FYEM_UNKNOWN,
	FYEM_ATOM,
	FYEM_SCAN,
	FYEM_PARSE,
	FYEM_DOC,
	FYEM_BUILD,
	FYEM_INTERNAL,
	FYEM_SYSTEM,
	FYEM_MAX,
};

/* Shift amount of the default version */
#define FYPCF_DEFAULT_VERSION_SHIFT	9
/* Mask of the default version */
#define FYPCF_DEFAULT_VERSION_MASK	((1U << 5) - 1)
/* Build a default version */
#define FYPCF_DEFAULT_VERSION(x)	(((unsigned int)(x) & FYPCF_DEFAULT_VERSION_MASK) << FYPCF_DEFAULT_VERSION_SHIFT)

/* Shift amount of the JSON input mode */
#define FYPCF_JSON_SHIFT		16
/* Mask of the JSON input mode */
#define FYPCF_JSON_MASK			((1U << 2) - 1)
/* Build a JSON input mode option */
#define FYPCF_JSON(x)			(((unsigned int)(x) & FYPCF_JSON_MASK) << FYPCF_JSON_SHIFT)

/* guaranteed minimum depth limit for generated document */
/* the actual limit is larger depending on the platform */
#define FYPCF_GUARANTEED_MINIMUM_DEPTH_LIMIT	64

/**
 * enum fy_parse_cfg_flags - Parse configuration flags
 *
 * These flags control the operation of the parse and the debugging
 * output/error reporting via filling in the &fy_parse_cfg->flags member.
 *
 * @FYPCF_QUIET: Quiet, do not output any information messages
 * @FYPCF_COLLECT_DIAG: Collect diagnostic/error messages
 * @FYPCF_RESOLVE_DOCUMENT: When producing documents, automatically resolve them
 * @FYPCF_DISABLE_MMAP_OPT: Disable mmap optimization
 * @FYPCF_DISABLE_RECYCLING: Disable recycling optimization
 * @FYPCF_PARSE_COMMENTS: Enable parsing of comments (experimental)
 * @FYPCF_DISABLE_DEPTH_LIMIT: Disable depth limit check, use with enlarged stack
 * @FYPCF_DISABLE_ACCELERATORS: Disable use of access accelerators (saves memory)
 * @FYPCF_DISABLE_BUFFERING: Disable use of buffering where possible
 * @FYPCF_DEFAULT_VERSION_AUTO: Automatically use the most recent version the library supports
 * @FYPCF_DEFAULT_VERSION_1_1: Default version is YAML 1.1
 * @FYPCF_DEFAULT_VERSION_1_2: Default version is YAML 1.2
 * @FYPCF_DEFAULT_VERSION_1_3: Default version is YAML 1.3 (experimental)
 * @FYPCF_SLOPPY_FLOW_INDENTATION: Allow sloppy indentation in flow mode
 * @FYPCF_PREFER_RECURSIVE: Prefer recursive algorighms instead of iterative whenever possible
 * @FYPCF_JSON_AUTO: Automatically enable JSON mode (when extension is .json)
 * @FYPCF_JSON_NONE: Never enable JSON input mode
 * @FYPCF_JSON_FORCE: Force JSON mode always
 * @FYPCF_YPATH_ALIASES: Enable YPATH aliases mode
 * @FYPCF_ALLOW_DUPLICATE_KEYS: Allow duplicate keys on mappings
 */
enum fy_parse_cfg_flags {
	FYPCF_QUIET			= FY_BIT(0),
	FYPCF_COLLECT_DIAG		= FY_BIT(1),
	FYPCF_RESOLVE_DOCUMENT		= FY_BIT(2),
	FYPCF_DISABLE_MMAP_OPT		= FY_BIT(3),
	FYPCF_DISABLE_RECYCLING		= FY_BIT(4),
	FYPCF_PARSE_COMMENTS		= FY_BIT(5),
	FYPCF_DISABLE_DEPTH_LIMIT	= FY_BIT(6),
	FYPCF_DISABLE_ACCELERATORS	= FY_BIT(7),
	FYPCF_DISABLE_BUFFERING		= FY_BIT(8),
	FYPCF_DEFAULT_VERSION_AUTO	= FYPCF_DEFAULT_VERSION(0),
	FYPCF_DEFAULT_VERSION_1_1	= FYPCF_DEFAULT_VERSION(1),
	FYPCF_DEFAULT_VERSION_1_2	= FYPCF_DEFAULT_VERSION(2),
	FYPCF_DEFAULT_VERSION_1_3	= FYPCF_DEFAULT_VERSION(3),
	FYPCF_SLOPPY_FLOW_INDENTATION	= FY_BIT(14),
	FYPCF_PREFER_RECURSIVE		= FY_BIT(15),
	FYPCF_JSON_AUTO			= FYPCF_JSON(0),
	FYPCF_JSON_NONE			= FYPCF_JSON(1),
	FYPCF_JSON_FORCE		= FYPCF_JSON(2),
	FYPCF_YPATH_ALIASES		= FY_BIT(18),
	FYPCF_ALLOW_DUPLICATE_KEYS	= FY_BIT(19),
};

#define FYPCF_DEFAULT_PARSE	(0)

#define FYPCF_DEFAULT_DOC	(FYPCF_QUIET | FYPCF_DEFAULT_PARSE)

/*
 * The FYPCF_DEBUG and FYPCF_COLOR flags have been removed, however
 * to help with backwards compatibility we will define them as 0
 * so that code can continue to compile.
 *
 * You will need to eventualy modify the code if you actually depended
 * on the old behaviour.
 */
#define FYPCF_MODULE_SHIFT		0
#define FYPCF_MODULE_MASK		0
#define FYPCF_DEBUG_LEVEL_SHIFT		0
#define FYPCF_DEBUG_LEVEL_MASK		0
#define FYPCF_DEBUG_LEVEL(x)		0
#define FYPCF_DEBUG_DIAG_SHIFT		0
#define FYPCF_DEBUG_DIAG_MASK		0
#define FYPCF_DEBUG_DIAG_ALL		0
#define FYPCF_DEBUG_DIAG_DEFAULT	0
#define FYPCF_DEBUG_UNKNOWN		0
#define FYPCF_DEBUG_ATOM		0
#define FYPCF_DEBUG_SCAN		0
#define FYPCF_DEBUG_PARSE		0
#define FYPCF_DEBUG_DOC			0
#define FYPCF_DEBUG_BUILD		0
#define FYPCF_DEBUG_INTERNAL		0
#define FYPCF_DEBUG_SYSTEM		0
#define FYPCF_DEBUG_LEVEL_DEBUG		0
#define FYPCF_DEBUG_LEVEL_INFO		0
#define FYPCF_DEBUG_LEVEL_NOTICE	0
#define FYPCF_DEBUG_LEVEL_WARNING	0
#define FYPCF_DEBUG_LEVEL_ERROR		0
#define FYPCF_DEBUG_DIAG_SOURCE		0
#define FYPCF_DEBUG_DIAG_POSITION	0
#define FYPCF_DEBUG_DIAG_TYPE		0
#define FYPCF_DEBUG_DIAG_MODULE		0
#define FYPCF_DEBUG_ALL			0
#define FYPCF_DEBUG_DEFAULT		0
#define FYPCF_COLOR_SHIFT		0
#define FYPCF_COLOR_MASK		0
#define FYPCF_COLOR(x)			0
#define FYPCF_COLOR_AUTO		0
#define FYPCF_COLOR_NONE		0
#define FYPCF_COLOR_FORCE		0

/**
 * struct fy_parse_cfg - parser configuration structure.
 *
 * Argument to the fy_parser_create() method which
 * perform parsing of YAML files.
 *
 * @search_path: Search path when accessing files, seperate with ':'
 * @flags: Configuration flags
 * @userdata: Opaque user data pointer
 * @diag: Optional diagnostic interface to use
 */
struct fy_parse_cfg {
	const char *search_path;
	enum fy_parse_cfg_flags flags;
	void *userdata;
	struct fy_diag *diag;
};

/**
 * enum fy_event_type - Event types
 *
 * @FYET_NONE: No event
 * @FYET_STREAM_START: Stream start event
 * @FYET_STREAM_END: Stream end event
 * @FYET_DOCUMENT_START: Document start event
 * @FYET_DOCUMENT_END: Document end event
 * @FYET_MAPPING_START: YAML mapping start event
 * @FYET_MAPPING_END: YAML mapping end event
 * @FYET_SEQUENCE_START: YAML sequence start event
 * @FYET_SEQUENCE_END: YAML sequence end event
 * @FYET_SCALAR: YAML scalar event
 * @FYET_ALIAS: YAML alias event
 */
enum fy_event_type {
	FYET_NONE,
	FYET_STREAM_START,
	FYET_STREAM_END,
	FYET_DOCUMENT_START,
	FYET_DOCUMENT_END,
	FYET_MAPPING_START,
	FYET_MAPPING_END,
	FYET_SEQUENCE_START,
	FYET_SEQUENCE_END,
	FYET_SCALAR,
	FYET_ALIAS,
};

/**
 * fy_event_type_get_text() - Return text of an event type
 *
 * @type: The event type to get text from
 *
 * Returns:
 * A pointer to a text, i.e for FYET_SCALAR "=VAL".
 */
const char *
fy_event_type_get_text(enum fy_event_type type)
	FY_EXPORT;

/**
 * enum fy_scalar_style - Scalar styles supported by the parser/emitter
 *
 * @FYSS_ANY: Any scalar style, not generated by the parser.
 * 	      Lets the emitter to choose
 * @FYSS_PLAIN: Plain scalar style
 * @FYSS_SINGLE_QUOTED: Single quoted style
 * @FYSS_DOUBLE_QUOTED: Double quoted style
 * @FYSS_LITERAL: YAML literal block style
 * @FYSS_FOLDED: YAML folded block style
 * @FYSS_MAX: marks end of scalar styles
 */
enum fy_scalar_style {
	FYSS_ANY = -1,
	FYSS_PLAIN,
	FYSS_SINGLE_QUOTED,
	FYSS_DOUBLE_QUOTED,
	FYSS_LITERAL,
	FYSS_FOLDED,
	FYSS_MAX,
};

/**
 * struct fy_event_stream_start_data - stream start event data
 *
 * @stream_start: The token that started the stream
 */
struct fy_event_stream_start_data {
	struct fy_token *stream_start;
};

/*
 * struct fy_event_stream_end_data - stream end event data
 *
 * @stream_end: The token that ended the stream
 */
struct fy_event_stream_end_data {
	struct fy_token *stream_end;
};

/*
 * struct fy_event_document_start_data - doument start event data
 *
 * @document_start: The token that started the document, or NULL if
 * 		    the document was implicitly started.
 * @document_state: The state of the document (i.e. information about
 * 		    the YAML version and configured tags)
 * @implicit: True if the document started implicitly
 */
struct fy_event_document_start_data {
	struct fy_token *document_start;
	struct fy_document_state *document_state;
	bool implicit;
};

/*
 * struct fy_event_document_end_data - doument end event data
 *
 * @document_end: The token that ended the document, or NULL if the
 * 	          document was implicitly ended
 * @implicit: True if the document ended implicitly
 */
struct fy_event_document_end_data {
	struct fy_token *document_end;
	bool implicit;
};

/*
 * struct fy_event_alias_data - alias event data
 *
 * @anchor: The anchor token definining this alias.
 */
struct fy_event_alias_data {
	struct fy_token *anchor;
};

/*
 * struct fy_event_scalar_data - scalar event data
 *
 * @.anchor: anchor token or NULL
 * @tag: tag token or NULL
 * @value: scalar value token (cannot be NULL)
 * @tag_implicit: true if the tag was implicit or explicit
 */
struct fy_event_scalar_data {
	struct fy_token *anchor;
	struct fy_token *tag;
	struct fy_token *value;
	bool tag_implicit;
};

/*
 * struct fy_event_sequence_start_data - sequence start event data
 *
 * @anchor: anchor token or NULL
 * @tag: tag token or NULL
 * @sequence_start: sequence start value token or NULL if the sequence
 * 		    was started implicitly
 */
struct fy_event_sequence_start_data {
	struct fy_token *anchor;
	struct fy_token *tag;
	struct fy_token *sequence_start;
};

/*
 * struct fy_event_sequence_end_data - sequence end event data
 *
 * @sequence_end: The token that ended the sequence, or NULL if
 * 		  the sequence was implicitly ended
 */
struct fy_event_sequence_end_data {
	struct fy_token *sequence_end;
};

/*
 * struct fy_event_mapping_start_data - mapping start event data
 *
 * @anchor: anchor token or NULL
 * @tag: tag token or NULL
 * @mapping_start: mapping start value token or NULL if the mapping
 * 		    was started implicitly
 */
struct fy_event_mapping_start_data {
	struct fy_token *anchor;
	struct fy_token *tag;
	struct fy_token *mapping_start;
};

/*
 * struct fy_event_mapping_end_data - mapping end event data
 *
 * @mapping_end: The token that ended the mapping, or NULL if
 * 		  the mapping was implicitly ended
 */
struct fy_event_mapping_end_data {
	struct fy_token *mapping_end;
};

/**
 * struct fy_event - Event generated by the parser
 *
 * This structure is generated by the parser by each call
 * to fy_parser_parse() and release by fy_parser_event_free()
 *
 * @type: Type of the event, see &enum fy_event_type
 *
 * @stream_start: Stream start information, it is valid when
 *                &fy_event->type is &enum FYET_STREAM_START
 * @stream_end: Stream end information, it is valid when
 *                &fy_event->type is &enum FYET_STREAM_END
 * @document_start: Document start information, it is valid when
 *                  &fy_event->type is &enum FYET_DOCUMENT_START
 * @document_end: Document end information, it is valid when
 *                &fy_event->type is &enum FYET_DOCUMENT_END
 * @alias: Alias information, it is valid when
 *         &fy_event->type is &enum FYET_ALIAS
 * @scalar: Scalar information, it is valid when
 *          &fy_event->type is &enum FYET_SCALAR
 * @sequence_start: Sequence start information, it is valid when
 *                  &fy_event->type is &enum FYET_SEQUENCE_START
 * @sequence_end: Sequence end information, it is valid when
 *                &fy_event->type is &enum FYET_SEQUENCE_END
 * @mapping_start: Mapping start information, it is valid when
 *                 &fy_event->type is &enum FYET_MAPPING_START
 * @mapping_end: Mapping end information, it is valid when
 *               &fy_event->type is &enum FYET_MAPPING_END
 */
struct fy_event {
	enum fy_event_type type;
	/* anonymous union */
	union {
		struct fy_event_stream_start_data stream_start;
		struct fy_event_stream_end_data stream_end;
		struct fy_event_document_start_data document_start;
		struct fy_event_document_end_data document_end;
		struct fy_event_alias_data alias;
		struct fy_event_scalar_data scalar;
		struct fy_event_sequence_start_data sequence_start;
		struct fy_event_sequence_end_data sequence_end;
		struct fy_event_mapping_start_data mapping_start;
		struct fy_event_mapping_end_data mapping_end;
	};
};

/**
 * fy_event_data() - Get a pointer to the event data
 *
 * Some languages *cough*golang*cough* really don't like
 * unions, and anonymous unions in particular.
 *
 * You should not have to use this in other language bindings.
 *
 * @fye: The event
 *
 * Returns:
 * A pointer to the event data structure, or NULL if the
 * event is invalid
 */
static inline void *
fy_event_data(struct fy_event *fye)
{
	if (!fye)
		return NULL;

	/* note that the unions should all be laid out
	 * at the same address, but play it straight and
	 * hope the optimizer will figure this is all
	 * the same...
	 */
	switch (fye->type) {
	case FYET_STREAM_START:
		return &fye->stream_start;
	case FYET_STREAM_END:
		return &fye->stream_end;
	case FYET_DOCUMENT_START:
		return &fye->document_start;
	case FYET_DOCUMENT_END:
		return &fye->document_end;
	case FYET_ALIAS:
		return &fye->alias;
	case FYET_SCALAR:
		return &fye->scalar;
	case FYET_SEQUENCE_START:
		return &fye->sequence_start;
	case FYET_SEQUENCE_END:
		return &fye->sequence_end;
	case FYET_MAPPING_START:
		return &fye->mapping_start;
	case FYET_MAPPING_END:
		return &fye->mapping_end;
	default:
		break;
	}

	return NULL;
}

/**
 * fy_library_version() - Return the library version string
 *
 * Returns:
 * A pointer to a version string of the form
 * <MAJOR>.<MINOR>[[.<PATCH>][-EXTRA-VERSION-INFO]]
 */
const char *
fy_library_version(void)
	FY_EXPORT;

/**
 * fy_string_to_error_type() - Return the error type from a string
 *
 * @str: The string to convert to an error type
 *
 * Returns:
 * The error type if greater or equal to zero, FYET_MAX otherwise
 */
enum fy_error_type fy_string_to_error_type(const char *str)
	FY_EXPORT;

/**
 * fy_error_type_to_string() - Convert an error type to string
 *
 * @type: The error type to convert
 *
 * Returns:
 * The string value of the error type or the empty string "" on error
 */
const char *fy_error_type_to_string(enum fy_error_type type)
	FY_EXPORT;

/**
 * fy_string_to_error_module() - Return the error module from a string
 *
 * @str: The string to convert to an error module
 *
 * Returns:
 * The error type if greater or equal to zero, FYEM_MAX otherwise
 */
enum fy_error_module fy_string_to_error_module(const char *str)
	FY_EXPORT;

/**
 * fy_error_module_to_string() - Convert an error module to string
 *
 * @module: The error module to convert
 *
 * Returns:
 * The string value of the error module or the empty string "" on error
 */
const char *fy_error_module_to_string(enum fy_error_module module)
	FY_EXPORT;

/**
 * fy_event_is_implicit() - Check whether the given event is an implicit one
 *
 * @fye: A pointer to a &struct fy_event to check.
 *
 * Returns:
 * true if the event is an implicit one.
 */
bool
fy_event_is_implicit(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_document_event_is_implicit() - Check whether the given document event is an implicit one
 *
 * @fye: A pointer to a &struct fy_event to check.
 *       It must be either a document start or document end event.
 *
 * Returns:
 * true if the event is an implicit one.
 */
bool
fy_document_event_is_implicit(const struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_parser_create() - Create a parser.
 *
 * Creates a parser with its configuration @cfg
 * The parser may be destroyed by a corresponding call to
 * fy_parser_destroy().
 *
 * @cfg: The configuration for the parser
 *
 * Returns:
 * A pointer to the parser or NULL in case of an error.
 */
struct fy_parser *
fy_parser_create(const struct fy_parse_cfg *cfg)
	FY_EXPORT;

/**
 * fy_parser_destroy() - Destroy the given parser
 *
 * Destroy a parser created earlier via fy_parser_create().
 *
 * @fyp: The parser to destroy
 */
void
fy_parser_destroy(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parser_get_cfg() - Get the configuration of a parser
 *
 * @fyp: The parser
 *
 * Returns:
 * The configuration of the parser
 */
const struct fy_parse_cfg *
fy_parser_get_cfg(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parser_get_diag() - Get the diagnostic object of a parser
 *
 * Return a pointer to the diagnostic object of a parser object.
 * Note that the returned diag object has a reference taken so
 * you should fy_diag_unref() it when you're done with it.
 *
 * @fyp: The parser to get the diagnostic object
 *
 * Returns:
 * A pointer to a ref'ed diagnostic object or NULL in case of an
 * error.
 */
struct fy_diag *
fy_parser_get_diag(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parser_set_diag() - Set the diagnostic object of a parser
 *
 * Replace the parser's current diagnostic object with the one
 * given as an argument. The previous diagnostic object will be
 * unref'ed (and freed if its reference gets to 0).
 * Also note that the diag argument shall take a reference too.
 *
 * @fyp: The parser to replace the diagnostic object
 * @diag: The parser's new diagnostic object, NULL for default
 *
 * Returns:
 * 0 if everything OK, -1 otherwise
 */
int
fy_parser_set_diag(struct fy_parser *fyp, struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_parser_reset() - Reset a parser completely
 *
 * Completely reset a parser, including after an error
 * that caused a parser error to be emitted.
 *
 * @fyp: The parser to reset
 *
 * Returns:
 * 0 if the reset was successful, -1 otherwise
 */
int
fy_parser_reset(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parser_set_input_file() - Set the parser to process the given file
 *
 * Point the parser to the given @file for processing. The file
 * is located by honoring the search path of the configuration set
 * by the earlier call to fy_parser_create().
 * While the parser is in use the file will must be available.
 *
 * @fyp: The parser
 * @file: The file to parse.
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_parser_set_input_file(struct fy_parser *fyp, const char *file)
	FY_EXPORT;

/**
 * fy_parser_set_string() - Set the parser to process the given string.
 *
 * Point the parser to the given (NULL terminated) string. Note that
 * while the parser is active the string must not go out of scope.
 *
 * @fyp: The parser
 * @str: The YAML string to parse.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_parser_set_string(struct fy_parser *fyp, const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_parser_set_malloc_string() - Set the parser to process the given malloced string.
 *
 * Point the parser to the given (possible NULL terminated) string. Note that
 * the string is expected to be allocated via malloc(3) and ownership is transferred
 * to the created input. When the input is free'ed the memory will be automatically
 * freed.
 *
 * In case of an error the string is not freed.
 *
 * @fyp: The parser
 * @str: The YAML string to parse (allocated).
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_parser_set_malloc_string(struct fy_parser *fyp, char *str, size_t len)
	FY_EXPORT;

/**
 * fy_parser_set_input_fp() - Set the parser to process the given file
 *
 * Point the parser to use @fp for processing.
 *
 * @fyp: The parser
 * @name: The label of the stream
 * @fp: The FILE pointer, it must be open in read mode.
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_parser_set_input_fp(struct fy_parser *fyp, const char *name, FILE *fp)
	FY_EXPORT;

/**
 * fy_parser_set_input_callback() - Set the parser to process via a callback
 *
 * Point the parser to use a callback for input.
 *
 * @fyp: The parser
 * @user: The user data pointer
 * @callback: The callback method to request data with
 *
 * Returns:
 * zero on success, -1 on error
 */
int fy_parser_set_input_callback(struct fy_parser *fyp, void *user,
		ssize_t (*callback)(void *user, void *buf, size_t count))
	FY_EXPORT;

/**
 * fy_parser_set_input_db() - Set the parser to process the given file descriptor
 *
 * Point the parser to use @fd for processing.
 *
 * @fyp: The parser
 * @fd: The file descriptor to use
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_parser_set_input_fd(struct fy_parser *fyp, int fd)
	FY_EXPORT;

/**
 * fy_parser_parse() - Parse and return the next event.
 *
 * Each call to fy_parser_parse() returns the next event from
 * the configured input of the parser, or NULL at the end of
 * the stream. The returned event must be released via
 * a matching call to fy_parser_event_free().
 *
 * @fyp: The parser
 *
 * Returns:
 * The next event in the stream or NULL.
 */
struct fy_event *
fy_parser_parse(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parser_event_free() - Free an event
 *
 * Free a previously returned event from fy_parser_parse().
 *
 * @fyp: The parser
 * @fye: The event to free (may be NULL)
 */
void
fy_parser_event_free(struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_parser_get_stream_error() - Check the parser for stream errors
 *
 * @fyp: The parser
 *
 * Returns:
 * true in case of a stream error, false otherwise.
 */
bool
fy_parser_get_stream_error(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_token_scalar_style() - Get the style of a scalar token
 *
 * @fyt: The scalar token to get it's style. Note that a NULL
 *       token is a &enum FYSS_PLAIN.
 *
 * Returns:
 * The scalar style of the token, or FYSS_PLAIN on each other case
 */
enum fy_scalar_style
fy_token_scalar_style(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_scalar_is_null() - Test whether the scalar is null (content)
 *
 * @fyt: The scalar token to check for NULLity.
 *  
 * Note that this is different than null of the YAML type system.
 * It is null as in null content. It is also different than an
 * empty scalar.
 *
 * Returns:
 * true if is a null scalar, false otherwise
 */
bool
fy_token_scalar_is_null(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_get_text() - Get text (and length of it) of a token
 *
 * This method will return a pointer to the text of a token
 * along with the length of it. Note that this text is *not*
 * NULL terminated. If you need a NULL terminated pointer
 * use fy_token_get_text0().
 *
 * In case that the token is 'simple' enough (i.e. a plain scalar)
 * or something similar the returned pointer is a direct pointer
 * to the space of the input that contains the token.
 *
 * That means that the pointer is *not* guaranteed to be valid
 * after the parser is destroyed.
 *
 * If the token is 'complex' enough, then space shall be allocated,
 * filled and returned.
 *
 * Note that the concept of 'simple' and 'complex' is vague, and
 * that's on purpose.
 *
 * @fyt: The token out of which the text pointer will be returned.
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the text representation of the token, while
 * @lenp will be assigned the character length of said representation.
 * NULL in case of an error.
 */
const char *
fy_token_get_text(struct fy_token *fyt, size_t *lenp)
	FY_EXPORT;

/**
 * fy_token_get_text0() - Get zero terminated text of a token
 *
 * This method will return a pointer to the text of a token
 * which is zero terminated. It will allocate memory to hold
 * it in the token structure so try to use fy_token_get_text()
 * instead if possible.
 *
 * @fyt: The token out of which the text pointer will be returned.
 *
 * Returns:
 * A pointer to a zero terminated text representation of the token.
 * NULL in case of an error.
 */
const char *
fy_token_get_text0(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_get_text_length() - Get length of the text of a token
 *
 * This method will return the length of the text representation
 * of a token.
 *
 * @fyt: The token
 *
 * Returns:
 * The size of the text representation of a token, -1 in case of an error.
 * Note that the NULL token will return a length of zero.
 */
size_t
fy_token_get_text_length(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_get_utf8_length() - Get length of the text of a token
 *
 * This method will return the length of the text representation
 * of a token as a utf8 string.
 *
 * @fyt: The token
 *
 * Returns:
 * The size of the utf8 text representation of a token, -1 in case of an error.
 * Note that the NULL token will return a length of zero.
 */
size_t
fy_token_get_utf8_length(struct fy_token *fyt)
	FY_EXPORT;

/**
 * enum fy_comment_placement - Comment placement relative to token
 *
 * @fycp_top: Comment on top of token
 * @fycp_right: Comment to the right of the token
 * @fycp_bottom: Comment to the bottom of the token
 */
enum fy_comment_placement {
	fycp_top,
	fycp_right,
	fycp_bottom
};
#define fycp_max (fycp_bottom + 1)

/**
 * fy_token_get_comment() - Get zero terminated comment of a token
 *
 * @fyt: The token out of which the comment text will be returned.
 * @buf: The buffer to be filled with the contents of the token
 * @maxsz: The maximum size of the comment buffer
 * @which: The comment placement
 *
 * Returns:
 * A pointer to a zero terminated text representation of the token comment.
 * NULL in case of an error or if the token has no comment.
 */
const char *
fy_token_get_comment(struct fy_token *fyt, char *buf, size_t maxsz,
		     enum fy_comment_placement which)
	FY_EXPORT;

/**
 * struct fy_iter_chunk - An iteration chunk
 *
 * @str: Pointer to the start of the chunk
 * @len: The size of the chunk
 *
 * The iterator produces a stream of chunks which
 * cover the whole object.
 */
struct fy_iter_chunk {
	const char *str;
	size_t len;
};

/**
 * fy_token_iter_create() - Create a token iterator
 *
 * Create an iterator for operating on the given token, or
 * a generic iterator for use with fy_token_iter_start().
 * The iterator must be destroyed with a matching call to
 * fy_token_iter_destroy().
 *
 * @fyt: The token to iterate, or NULL.
 *
 * Returns:
 * A pointer to the newly created iterator, or NULL in case of
 * an error.
 */
struct fy_token_iter *
fy_token_iter_create(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_iter_destroy() - Destroy the iterator
 *
 * Destroy the iterator created by fy_token_iter_create().
 *
 * @iter: The iterator to destroy.
 */
void
fy_token_iter_destroy(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_start() - Start iterating over the contents of a token
 *
 * Prepare an iterator for operating on the given token.
 * The iterator must be created via a previous call to fy_token_iter_create()
 * for user level API access.
 *
 * @fyt: The token to iterate over
 * @iter: The iterator to prepare.
 */
void
fy_token_iter_start(struct fy_token *fyt, struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_finish() - Stop iterating over the contents of a token
 *
 * Stop the iteration operation.
 *
 * @iter: The iterator.
 */
void
fy_token_iter_finish(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_peek_chunk() - Peek at the next iterator chunk
 *
 * Peek at the next iterator chunk
 *
 * @iter: The iterator.
 *
 * Returns:
 * A pointer to the next iterator chunk, or NULL in case there's
 * no other.
 */
const struct fy_iter_chunk *
fy_token_iter_peek_chunk(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_chunk_next() - Get next iterator chunk
 *
 * Get the next iterator chunk in sequence,
 *
 * @iter: The iterator.
 * @curr: The current chunk, or NULL for the first one.
 * @errp: Pointer to an error return value or NULL
 *
 * Returns:
 * A pointer to the next iterator chunk, or NULL in case there's
 * no other. When the return value is NULL, the errp variable
 * will be filled with 0 for normal end, or -1 in case of an error.
 */
const struct fy_iter_chunk *
fy_token_iter_chunk_next(struct fy_token_iter *iter,
			 const struct fy_iter_chunk *curr, int *errp)
	FY_EXPORT;

/**
 * fy_token_iter_advance() - Advance the iterator position
 *
 * Advance the read pointer of the iterator.
 * Note that mixing calls of this with any call of fy_token_iter_ungetc() /
 * fy_token_iter_utf8_unget() in a single iterator sequence leads
 * to undefined behavior.
 *
 * @iter: The iterator.
 * @len: Number of bytes to advance the iterator position
 */
void
fy_token_iter_advance(struct fy_token_iter *iter, size_t len)
	FY_EXPORT;

/**
 * fy_token_iter_read() - Read a block from an iterator
 *
 * Read a block from an iterator. Note than mixing calls of this
 * and any of the ungetc methods leads to undefined behavior.
 *
 * @iter: The iterator.
 * @buf: Pointer to a block of memory to receive the data. Must be at
 *       least count bytes long.
 * @count: Amount of bytes to read.
 *
 * Returns:
 * The amount of data read, or -1 in case of an error.
 */
ssize_t
fy_token_iter_read(struct fy_token_iter *iter, void *buf, size_t count)
	FY_EXPORT;

/**
 * fy_token_iter_getc() - Get a single character from an iterator
 *
 * Reads a single character from an iterator. If the iterator is
 * finished, it will return -1. If any calls to ungetc have pushed
 * a character in the iterator it shall return that.
 *
 * @iter: The iterator.
 *
 * Returns:
 * The next character in the iterator, or -1 in case of an error, or
 * end of stream.
 */
int
fy_token_iter_getc(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_ungetc() - Ungets a single character from an iterator
 *
 * Pushes back a single character to an iterator stream. It will be
 * returned in subsequent calls of fy_token_iter_getc(). Currently
 * only a single character is allowed to be pushed back, and any
 * further calls to ungetc will return an error.
 *
 * @iter: The iterator.
 * @c: The character to push back, or -1 to reset the pushback buffer.
 *
 * Returns:
 * The pushed back character given as argument, or -1 in case of an error.
 * If the pushed back character was -1, then 0 will be returned.
 */
int
fy_token_iter_ungetc(struct fy_token_iter *iter, int c)
	FY_EXPORT;

/**
 * fy_token_iter_peekc() - Peeks at single character from an iterator
 *
 * Peeks at the next character to get from an iterator. If the iterator is
 * finished, it will return -1. If any calls to ungetc have pushed
 * a character in the iterator it shall return that. The character is not
 * removed from the iterator stream.
 *
 * @iter: The iterator.
 *
 * Returns:
 * The next character in the iterator, or -1 in case of an error, or end
 * of stream.
 */
int
fy_token_iter_peekc(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_utf8_get() - Get a single utf8 character from an iterator
 *
 * Reads a single utf8 character from an iterator. If the iterator is
 * finished, it will return -1. If any calls to ungetc have pushed
 * a character in the iterator it shall return that.
 *
 * @iter: The iterator.
 *
 * Returns:
 * The next utf8 character in the iterator, or -1 in case of an error, or end
 * of stream.
 */
int
fy_token_iter_utf8_get(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_token_iter_utf8_unget() - Ungets a single utf8 character from an iterator
 *
 * Pushes back a single utf8 character to an iterator stream. It will be
 * returned in subsequent calls of fy_token_iter_utf8_getc(). Currently
 * only a single character is allowed to be pushed back, and any
 * further calls to ungetc will return an error.
 *
 * @iter: The iterator.
 * @c: The character to push back, or -1 to reset the pushback buffer.
 *
 * Returns:
 * The pushed back utf8 character given as argument, or -1 in case of an error.
 * If the pushed back utf8 character was -1, then 0 will be returned.
 */
int
fy_token_iter_utf8_unget(struct fy_token_iter *iter, int c)
	FY_EXPORT;

/**
 * fy_token_iter_utf8_peek() - Peeks at single utf8 character from an iterator
 *
 * Peeks at the next utf8 character to get from an iterator. If the iterator is
 * finished, it will return -1. If any calls to ungetc have pushed
 * a character in the iterator it shall return that. The character is not
 * removed from the iterator stream.
 *
 * @iter: The iterator.
 *
 * Returns:
 * The next utf8 character in the iterator, or -1 in case of an error, or end
 * of stream.
 */
int
fy_token_iter_utf8_peek(struct fy_token_iter *iter)
	FY_EXPORT;

/**
 * fy_parse_load_document() - Parse the next document from the parser stream
 *
 * This method performs parsing on a parser stream and returns the next
 * document. This means that for a compound document with multiple
 * documents, each call will return the next document.
 *
 * @fyp: The parser
 *
 * Returns:
 * The next document from the parser stream.
 */
struct fy_document *
fy_parse_load_document(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_parse_document_destroy() - Destroy a document created by fy_parse_load_document()
 *
 * @fyp: The parser
 * @fyd: The document to destroy
 */
void
fy_parse_document_destroy(struct fy_parser *fyp, struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_resolve() - Resolve anchors and merge keys
 *
 * This method performs resolution of the given document,
 * by replacing references to anchors with their contents
 * and handling merge keys (<<)
 *
 * @fyd: The document to resolve
 *
 * Returns:
 * zero on success, -1 on error
 */
int
fy_document_resolve(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_has_directives() - Document directive check
 *
 * Checks whether the given document has any directives, i.e.
 * %TAG or %VERSION.
 *
 * @fyd: The document to check for directives existence
 *
 * Returns:
 * true if directives exist, false if not
 */
bool
fy_document_has_directives(const struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_has_explicit_document_start() - Explicit document start check
 *
 * Checks whether the given document has an explicit document start marker,
 * i.e. ---
 *
 * @fyd: The document to check for explicit start marker
 *
 * Returns:
 * true if document has an explicit document start marker, false if not
 */
bool
fy_document_has_explicit_document_start(const struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_has_explicit_document_end() - Explicit document end check
 *
 * Checks whether the given document has an explicit document end marker,
 * i.e. ...
 *
 * @fyd: The document to check for explicit end marker
 *
 * Returns:
 * true if document has an explicit document end marker, false if not
 */
bool
fy_document_has_explicit_document_end(const struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_node_document() - Retreive the document the node belong to
 *
 * Returns the document of the node; note that while the node may not
 * be reachable via a path expression, it may still be member of a
 * document.
 *
 * @fyn: The node to retreive it's document
 *
 * Returns:
 * The document of the node, or NULL in case of an error, or
 * when the node has no document attached.
 */
struct fy_document *
fy_node_document(struct fy_node *fyn)
	FY_EXPORT;

/*
 * enum fy_emitter_write_type - Type of the emitted output
 *
 * Describes the kind of emitted output, which makes it
 * possible to colorize the output, or do some other content related
 * filtering.
 *
 * @fyewt_document_indicator: Output chunk is a document indicator
 * @fyewt_tag_directive: Output chunk is a tag directive
 * @fyewt_version_directive: Output chunk is a version directive
 * @fyewt_indent: Output chunk is a document indicator
 * @fyewt_indicator: Output chunk is an indicator
 * @fyewt_whitespace: Output chunk is white space
 * @fyewt_plain_scalar: Output chunk is a plain scalar
 * @fyewt_single_quoted_scalar: Output chunk is a single quoted scalar
 * @fyewt_double_quoted_scalar: Output chunk is a double quoted scalar
 * @fyewt_literal_scalar: Output chunk is a literal block scalar
 * @fyewt_folded_scalar: Output chunk is a folded block scalar
 * @fyewt_anchor: Output chunk is an anchor
 * @fyewt_tag: Output chunk is a tag
 * @fyewt_linebreak: Output chunk is a linebreak
 * @fyewt_alias: Output chunk is an alias
 * @fyewt_terminating_zero: Output chunk is a terminating zero
 * @fyewt_plain_scalar_key: Output chunk is an plain scalar key
 * @fyewt_single_quoted_scalar_key: Output chunk is an single quoted scalar key
 * @fyewt_double_quoted_scalar_key: Output chunk is an double quoted scalar key
 * @fyewt_comment: Output chunk is a comment
 *
 */
enum fy_emitter_write_type {
	fyewt_document_indicator,
	fyewt_tag_directive,
	fyewt_version_directive,
	fyewt_indent,
	fyewt_indicator,
	fyewt_whitespace,
	fyewt_plain_scalar,
	fyewt_single_quoted_scalar,
	fyewt_double_quoted_scalar,
	fyewt_literal_scalar,
	fyewt_folded_scalar,
	fyewt_anchor,
	fyewt_tag,
	fyewt_linebreak,
	fyewt_alias,
	fyewt_terminating_zero,
	fyewt_plain_scalar_key,
	fyewt_single_quoted_scalar_key,
	fyewt_double_quoted_scalar_key,
	fyewt_comment,
};

#define FYECF_INDENT_SHIFT	8
#define FYECF_INDENT_MASK	0xf
#define FYECF_INDENT(x)	(((x) & FYECF_INDENT_MASK) << FYECF_INDENT_SHIFT)

#define FYECF_WIDTH_SHIFT	12
#define FYECF_WIDTH_MASK	0xff
#define FYECF_WIDTH(x)		(((x) & FYECF_WIDTH_MASK) << FYECF_WIDTH_SHIFT)

#define FYECF_MODE_SHIFT	20
#define FYECF_MODE_MASK		0xf
#define FYECF_MODE(x)		(((x) & FYECF_MODE_MASK) << FYECF_MODE_SHIFT)

#define FYECF_DOC_START_MARK_SHIFT	24
#define FYECF_DOC_START_MARK_MASK	0x3
#define FYECF_DOC_START_MARK(x)		(((x) & FYECF_DOC_START_MARK_MASK) << FYECF_DOC_START_MARK_SHIFT)

#define FYECF_DOC_END_MARK_SHIFT	26
#define FYECF_DOC_END_MARK_MASK		0x3
#define FYECF_DOC_END_MARK(x)		(((x) & FYECF_DOC_END_MARK_MASK) << FYECF_DOC_END_MARK_SHIFT)

#define FYECF_VERSION_DIR_SHIFT		28
#define FYECF_VERSION_DIR_MASK		0x3
#define FYECF_VERSION_DIR(x)		(((x) & FYECF_VERSION_DIR_MASK) << FYECF_VERSION_DIR_SHIFT)

#define FYECF_TAG_DIR_SHIFT		30
#define FYECF_TAG_DIR_MASK		0x3
#define FYECF_TAG_DIR(x)		(((unsigned int)(x) & FYECF_TAG_DIR_MASK) << FYECF_TAG_DIR_SHIFT)

/**
 * enum fy_emitter_cfg_flags - Emitter configuration flags
 *
 * These flags control the operation of the emitter
 *
 * @FYECF_SORT_KEYS: Sort key when emitting
 * @FYECF_OUTPUT_COMMENTS: Output comments (experimental)
 * @FYECF_STRIP_LABELS: Strip labels when emitting
 * @FYECF_STRIP_TAGS: Strip tags when emitting
 * @FYECF_STRIP_DOC: Strip document tags and markers when emitting
 * @FYECF_NO_ENDING_NEWLINE: Do not output ending new line (useful for single line mode)
 * @FYECF_STRIP_EMPTY_KV: Remove all keys with empty values from the output (not available in streaming mode)
 * @FYECF_INDENT_DEFAULT: Default emit output indent
 * @FYECF_INDENT_1: Output indent is 1
 * @FYECF_INDENT_2: Output indent is 2
 * @FYECF_INDENT_3: Output indent is 3
 * @FYECF_INDENT_4: Output indent is 4
 * @FYECF_INDENT_5: Output indent is 5
 * @FYECF_INDENT_6: Output indent is 6
 * @FYECF_INDENT_7: Output indent is 7
 * @FYECF_INDENT_8: Output indent is 8
 * @FYECF_INDENT_9: Output indent is 9
 * @FYECF_WIDTH_DEFAULT: Default emit output width
 * @FYECF_WIDTH_80: Output width is 80
 * @FYECF_WIDTH_132: Output width is 132
 * @FYECF_WIDTH_INF: Output width is infinite
 * @FYECF_MODE_ORIGINAL: Emit using the same flow mode as the original
 * @FYECF_MODE_BLOCK: Emit using only the block mode
 * @FYECF_MODE_FLOW: Emit using only the flow mode
 * @FYECF_MODE_FLOW_ONELINE: Emit using only the flow mode (in one line)
 * @FYECF_MODE_JSON: Emit using JSON mode (non type preserving)
 * @FYECF_MODE_JSON_TP: Emit using JSON mode (type preserving)
 * @FYECF_MODE_JSON_ONELINE: Emit using JSON mode (non type preserving, one line)
 * @FYECF_MODE_DEJSON: Emit YAML trying to pretify JSON
 * @FYECF_MODE_PRETTY: Emit YAML that tries to look good
 * @FYECF_MODE_MANUAL: Emit YAML respecting all manual style hints (reformats if needed)
 * @FYECF_DOC_START_MARK_AUTO: Automatically generate document start markers if required
 * @FYECF_DOC_START_MARK_OFF: Do not generate document start markers
 * @FYECF_DOC_START_MARK_ON: Always generate document start markers
 * @FYECF_DOC_END_MARK_AUTO: Automatically generate document end markers if required
 * @FYECF_DOC_END_MARK_OFF: Do not generate document end markers
 * @FYECF_DOC_END_MARK_ON: Always generate document end markers
 * @FYECF_VERSION_DIR_AUTO: Automatically generate version directive
 * @FYECF_VERSION_DIR_OFF: Never generate version directive
 * @FYECF_VERSION_DIR_ON: Always generate version directive
 * @FYECF_TAG_DIR_AUTO: Automatically generate tag directives
 * @FYECF_TAG_DIR_OFF: Never generate tag directives
 * @FYECF_TAG_DIR_ON: Always generate tag directives
 * @FYECF_DEFAULT: The default emitter configuration
 */
enum fy_emitter_cfg_flags {
	FYECF_SORT_KEYS			= FY_BIT(0),
	FYECF_OUTPUT_COMMENTS		= FY_BIT(1),
	FYECF_STRIP_LABELS		= FY_BIT(2),
	FYECF_STRIP_TAGS		= FY_BIT(3),
	FYECF_STRIP_DOC			= FY_BIT(4),
	FYECF_NO_ENDING_NEWLINE		= FY_BIT(5),
	FYECF_STRIP_EMPTY_KV		= FY_BIT(6),
	FYECF_INDENT_DEFAULT		= FYECF_INDENT(0),
	FYECF_INDENT_1			= FYECF_INDENT(1),
	FYECF_INDENT_2			= FYECF_INDENT(2),
	FYECF_INDENT_3			= FYECF_INDENT(3),
	FYECF_INDENT_4			= FYECF_INDENT(4),
	FYECF_INDENT_5			= FYECF_INDENT(5),
	FYECF_INDENT_6			= FYECF_INDENT(6),
	FYECF_INDENT_7			= FYECF_INDENT(7),
	FYECF_INDENT_8			= FYECF_INDENT(8),
	FYECF_INDENT_9			= FYECF_INDENT(9),
	FYECF_WIDTH_DEFAULT		= FYECF_WIDTH(80),
	FYECF_WIDTH_80			= FYECF_WIDTH(80),
	FYECF_WIDTH_132			= FYECF_WIDTH(132),
	FYECF_WIDTH_INF			= FYECF_WIDTH(255),
	FYECF_MODE_ORIGINAL		= FYECF_MODE(0),
	FYECF_MODE_BLOCK		= FYECF_MODE(1),
	FYECF_MODE_FLOW			= FYECF_MODE(2),
	FYECF_MODE_FLOW_ONELINE 	= FYECF_MODE(3),
	FYECF_MODE_JSON			= FYECF_MODE(4),
	FYECF_MODE_JSON_TP		= FYECF_MODE(5),
	FYECF_MODE_JSON_ONELINE 	= FYECF_MODE(6),
	FYECF_MODE_DEJSON 		= FYECF_MODE(7),
	FYECF_MODE_PRETTY 		= FYECF_MODE(8),
	FYECF_MODE_MANUAL 		= FYECF_MODE(9),
	FYECF_DOC_START_MARK_AUTO	= FYECF_DOC_START_MARK(0),
	FYECF_DOC_START_MARK_OFF	= FYECF_DOC_START_MARK(1),
	FYECF_DOC_START_MARK_ON		= FYECF_DOC_START_MARK(2),
	FYECF_DOC_END_MARK_AUTO		= FYECF_DOC_END_MARK(0),
	FYECF_DOC_END_MARK_OFF		= FYECF_DOC_END_MARK(1),
	FYECF_DOC_END_MARK_ON		= FYECF_DOC_END_MARK(2),
	FYECF_VERSION_DIR_AUTO		= FYECF_VERSION_DIR(0),
	FYECF_VERSION_DIR_OFF		= FYECF_VERSION_DIR(1),
	FYECF_VERSION_DIR_ON		= FYECF_VERSION_DIR(2),
	FYECF_TAG_DIR_AUTO		= FYECF_TAG_DIR(0),
	FYECF_TAG_DIR_OFF		= FYECF_TAG_DIR(1),
	FYECF_TAG_DIR_ON		= FYECF_TAG_DIR(2),

	FYECF_DEFAULT			= FYECF_WIDTH_INF |
					  FYECF_MODE_ORIGINAL |
					  FYECF_INDENT_DEFAULT,
};

/**
 * struct fy_emitter_cfg - emitter configuration structure.
 *
 * Argument to the fy_emitter_create() method which
 * is the way to convert a runtime document structure back to YAML.
 *
 * @flags: Configuration flags
 * @output: Pointer to the method that will perform output.
 * @userdata: Opaque user data pointer
 * @diag: Diagnostic interface
 */
struct fy_emitter_cfg {
	enum fy_emitter_cfg_flags flags;
	int (*output)(struct fy_emitter *emit, enum fy_emitter_write_type type,
		      const char *str, int len, void *userdata);
	void *userdata;
	struct fy_diag *diag;
};

/**
 * fy_emitter_create() - Create an emitter
 *
 * Creates an emitter using the supplied configuration
 *
 * @cfg: The emitter configuration
 *
 * Returns:
 * The newly created emitter or NULL on error.
 */
struct fy_emitter *
fy_emitter_create(const struct fy_emitter_cfg *cfg)
	FY_EXPORT;

/**
 * fy_emitter_destroy() - Destroy an emitter
 *
 * Destroy an emitter previously created by fy_emitter_create()
 *
 * @emit: The emitter to destroy
 */
void
fy_emitter_destroy(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emitter_get_cfg() - Get the configuration of an emitter
 *
 * @emit: The emitter
 *
 * Returns:
 * The configuration of the emitter
 */
const struct fy_emitter_cfg *
fy_emitter_get_cfg(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emitter_get_diag() - Get the diagnostic object of an emitter
 *
 * Return a pointer to the diagnostic object of an emitter object.
 * Note that the returned diag object has a reference taken so
 * you should fy_diag_unref() it when you're done with it.
 *
 * @emit: The emitter to get the diagnostic object
 *
 * Returns:
 * A pointer to a ref'ed diagnostic object or NULL in case of an
 * error.
 */
struct fy_diag *
fy_emitter_get_diag(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emitter_set_diag() - Set the diagnostic object of an emitter
 *
 * Replace the emitters's current diagnostic object with the one
 * given as an argument. The previous diagnostic object will be
 * unref'ed (and freed if its reference gets to 0).
 * Also note that the diag argument shall take a reference too.
 *
 * @emit: The emitter to replace the diagnostic object
 * @diag: The emitter's new diagnostic object, NULL for default
 *
 * Returns:
 * 0 if everything OK, -1 otherwise
 */
int
fy_emitter_set_diag(struct fy_emitter *emit, struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_emitter_set_finalizer() - Set emitter finalizer
 *
 * Set a method callback to be called when the emitter
 * is disposed of. If finalizer is NULL, then the method
 * is removed.
 *
 * @emit: The emitter to replace the diagnostic object
 * @finalizer: The finalizer callback
 */
void
fy_emitter_set_finalizer(struct fy_emitter *emit,
		void (*finalizer)(struct fy_emitter *emit))
	FY_EXPORT;

/**
 * struct fy_emitter_default_output_data - emitter default output configuration
 *
 * This is the argument to the default output method of the emitter.
 *
 * @fp: File where the output is directed to
 * @colorize: Use ANSI color sequences to colorize the output
 * @visible: Make whitespace visible (requires a UTF8 capable terminal)
 */
struct fy_emitter_default_output_data {
	FILE *fp;
	bool colorize;
	bool visible;
};

/**
 * fy_emitter_default_output() - The default colorizing output method
 *
 * This is the default colorizing output method.
 * Will be used when the output field of the emitter configuration is NULL.
 *
 * @fye: The emitter
 * @type: Type of the emitted output
 * @str: Pointer to the string to output
 * @len: Length of the string
 * @userdata: Must point to a fy_emitter_default_output_data structure
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emitter_default_output(struct fy_emitter *fye, enum fy_emitter_write_type type,
			  const char *str, int len, void *userdata)
	FY_EXPORT;

/**
 * fy_document_default_emit_to_fp() - Emit a document to a file, using defaults
 *
 * Simple one shot emitter to a file, using the default emitter output.
 * The output will be colorized if the the file points to a tty.
 *
 * @fyd: The document to emit
 * @fp: The file where the output is sent
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_default_emit_to_fp(struct fy_document *fyd, FILE *fp)
	FY_EXPORT;

/**
 * fy_emit_event() - Queue (and possibly emit) an event
 *
 * Queue and output using the emitter. This is the streaming
 * output method which does not require creating a document.
 *
 * @emit: The emitter to use
 * @fye: The event to queue for emission
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_event(struct fy_emitter *emit, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_emit_event_from_parser() - Queue (and possibly emit) an event
 * 			  	 generated by the parser.
 *
 * Queue and output using the emitter. This is the streaming
 * output method which does not require creating a document.
 * Similar to fy_emit_event() but it is more efficient.
 *
 * @emit: The emitter to use
 * @fyp: The parser that generated the event
 * @fye: The event to queue for emission
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_event_from_parser(struct fy_emitter *emit, struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_emit_document() - Emit the document using the emitter
 *
 * Emits a document in YAML format using the emitter.
 *
 * @emit: The emitter
 * @fyd: The document to emit
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document(struct fy_emitter *emit, struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_emit_document_start() - Emit document start using the emitter
 *
 * Emits a document start using the emitter. This is used in case
 * you need finer control over the emitting output.
 *
 * @emit: The emitter
 * @fyd: The document to use for emitting it's start
 * @fyn: The root (or NULL for using the document's root)
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document_start(struct fy_emitter *emit, struct fy_document *fyd,
		       struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_emit_document_end() - Emit document end using the emitter
 *
 * Emits a document end using the emitter. This is used in case
 * you need finer control over the emitting output.
 *
 * @emit: The emitter
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document_end(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emit_node() - Emit a single node using the emitter
 *
 * Emits a single node using the emitter. This is used in case
 * you need finer control over the emitting output.
 *
 * @emit: The emitter
 * @fyn: The node to emit
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_node(struct fy_emitter *emit, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_emit_root_node() - Emit a single root node using the emitter
 *
 * Emits a single root node using the emitter. This is used in case
 * you need finer control over the emitting output.
 *
 * @emit: The emitter
 * @fyn: The root node to emit
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_root_node(struct fy_emitter *emit, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_emit_explicit_document_end() - Emit an explicit document end
 *
 * Emits an explicit document end, i.e. ... . Use this if you
 * you need finer control over the emitting output.
 *
 * @emit: The emitter
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_explicit_document_end(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emit_document_to_fp() - Emit a document to an file pointer
 *
 * Emits a document from the root to the given file pointer.
 *
 * @fyd: The document to emit
 * @flags: The emitter flags to use
 * @fp: The file pointer to output to
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document_to_fp(struct fy_document *fyd,
		       enum fy_emitter_cfg_flags flags, FILE *fp)
	FY_EXPORT;

/**
 * fy_emit_document_to_file() - Emit a document to file
 *
 * Emits a document from the root to the given file.
 * The file will be fopen'ed using a "wa" mode.
 *
 * @fyd: The document to emit
 * @flags: The emitter flags to use
 * @filename: The filename to output to, or NULL for stdout
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document_to_file(struct fy_document *fyd,
			 enum fy_emitter_cfg_flags flags,
			 const char *filename)
	FY_EXPORT;

/**
 * fy_emit_document_to_fd() - Emit a document to a file descriptor
 *
 * Emits a document from the root to the given file descriptor
 *
 * @fyd: The document to emit
 * @flags: The emitter flags to use
 * @fd: The file descriptor to output to
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_emit_document_to_fd(struct fy_document *fyd,
		       enum fy_emitter_cfg_flags flags, int fd)
	FY_EXPORT;

/**
 * fy_emit_document_to_buffer() - Emit a document to a buffer
 *
 * Emits an document from the root to the given buffer.
 * If the document does not fit, an error will be returned.
 *
 * @fyd: The document to emit
 * @flags: The emitter flags to use
 * @buf: Pointer to the buffer area to fill
 * @size: Size of the buffer
 *
 * Returns:
 * A positive number, which is the size of the emitted document
 * on the buffer on success, -1 on error
 */
int
fy_emit_document_to_buffer(struct fy_document *fyd,
			   enum fy_emitter_cfg_flags flags,
			   char *buf, size_t size)
	FY_EXPORT;

/**
 * fy_emit_document_to_string() - Emit a document to an allocated string
 *
 * Emits an document from the root to a string which will be dynamically
 * allocated.
 *
 * @fyd: The document to emit
 * @flags: The emitter flags to use
 *
 * Returns:
 * A pointer to the allocated string, or NULL in case of an error
 */
char *
fy_emit_document_to_string(struct fy_document *fyd,
			   enum fy_emitter_cfg_flags flags)
	FY_EXPORT;

#define fy_emit_document_to_string_alloca(_fyd, _flags, _res) \
	FY_ALLOCA_COPY_FREE(fy_emit_document_to_string((_fyd), (_flags)), FY_NT, (_res))

/**
 * fy_emit_node_to_buffer() - Emit a node (recursively) to a buffer
 *
 * Emits a node recursively to the given buffer.
 * If the document does not fit, an error will be returned.
 *
 * @fyn: The node to emit
 * @flags: The emitter flags to use
 * @buf: Pointer to the buffer area to fill
 * @size: Size of the buffer
 *
 * Returns:
 * A positive number, which is the size of the emitted node
 * on the buffer on success, -1 on error
 */
int
fy_emit_node_to_buffer(struct fy_node *fyn, enum fy_emitter_cfg_flags flags,
		       char *buf, size_t size)
	FY_EXPORT;

/**
 * fy_emit_node_to_string() - Emit a node to an allocated string
 *
 * Emits a node recursively to a string which will be dynamically
 * allocated.
 *
 * @fyn: The node to emit
 * @flags: The emitter flags to use
 *
 * Returns:
 * A pointer to the allocated string, or NULL in case of an error
 */
char *
fy_emit_node_to_string(struct fy_node *fyn, enum fy_emitter_cfg_flags flags)
	FY_EXPORT;

#define fy_emit_node_to_string_alloca(_fyn, _flags, _res) \
	FY_ALLOCA_COPY_FREE(fy_emit_node_to_string((_fyn), (_flags)), FY_NT, (_res))

/**
 * fy_emit_to_buffer() - Create an emitter for buffer output.
 *
 * Creates a special purpose emitter for buffer output.
 * Calls to fy_emit_event() populate the buffer.
 * The contents are retreived by a call to fy_emit_to_buffer_collect()
 *
 * @flags: The emitter flags to use
 * @buf: Pointer to the buffer area to fill
 * @size: Size of the buffer
 *
 * Returns:
 * The newly created emitter or NULL on error.
 */
struct fy_emitter *
fy_emit_to_buffer(enum fy_emitter_cfg_flags flags, char *buf, size_t size)
	FY_EXPORT;

/**
 * fy_emit_to_buffer_collect() - Collect the buffer emitter output
 *
 * Collects the output of the emitter which was created by
 * fy_emit_to_buffer() and populated using fy_emit_event() calls.
 * The NULL terminated returned buffer is the same as the one used in the
 * fy_emit_to_buffer() call and the sizep argument will be filled with
 * the size of the buffer.
 *
 * @emit: The emitter
 * @sizep: Pointer to the size to be filled
 *
 * Returns:
 * The buffer or NULL in case of an error.
 */
char *
fy_emit_to_buffer_collect(struct fy_emitter *emit, size_t *sizep)
	FY_EXPORT;

/**
 * fy_emit_to_string() - Create an emitter to create a dynamically
 *                       allocated string.
 *
 * Creates a special purpose emitter for output to a dynamically
 * allocated string.
 * Calls to fy_emit_event() populate the buffer.
 * The contents are retreived by a call to fy_emit_to_string_collect()
 *
 * @flags: The emitter flags to use
 *
 * Returns:
 * The newly created emitter or NULL on error.
 */
struct fy_emitter *
fy_emit_to_string(enum fy_emitter_cfg_flags flags)
	FY_EXPORT;

/**
 * fy_emit_to_string_collect() - Collect the string emitter output
 *
 * Collects the output of the emitter which was created by
 * fy_emit_to_string() and populated using fy_emit_event() calls.
 * The NULL terminated returned buffer is dynamically allocated
 * and must be freed via a call to free().
 * The sizep argument will be filled with the size of the buffer.
 *
 * @emit: The emitter
 * @sizep: Pointer to the size to be filled
 *
 * Returns:
 * The dynamically allocated string or NULL in case of an error.
 */
char *
fy_emit_to_string_collect(struct fy_emitter *emit, size_t *sizep)
	FY_EXPORT;

/**
 * fy_node_copy() - Copy a node, associating the new node with the given document
 *
 * Make a deep copy of a node, associating the copy with the given document.
 * Note that no content copying takes place as the contents of the nodes
 * are reference counted. This means that the operation is relatively inexpensive.
 *
 * Note that the copy includes all anchors contained in the subtree of the
 * source, so this call will register them with the document.
 *
 * @fyd: The document which the resulting node will be associated with
 * @fyn_from: The source node to recursively copy
 *
 * Returns:
 * The copied node on success, NULL on error
 */
struct fy_node *
fy_node_copy(struct fy_document *fyd, struct fy_node *fyn_from)
	FY_EXPORT;

/**
 * fy_document_clone() - Clones a document
 *
 * Clone a document, by making a deep copy of the source.
 * Note that no content copying takes place as the contents of the nodes
 * are reference counted. This means that the operation is relatively inexpensive.
 *
 * @fydsrc: The source document to clone
 *
 * Returns:
 * The newly created clone document, or NULL in case of an error
 */
struct fy_document *
fy_document_clone(struct fy_document *fydsrc)
	FY_EXPORT;

/**
 * fy_node_insert() - Insert a node to the given node
 *
 * Insert a node to another node. If @fyn_from is NULL then this
 * operation will delete the @fyn_to node.
 *
 * The operation varies according to the types of the arguments:
 *
 * from: scalar
 *
 * to: another-scalar -> scalar
 * to: { key: value } -> scalar
 * to: [ seq0, seq1 ] -> scalar
 *
 * from: [ seq2 ]
 * to: scalar -> [ seq2 ]
 * to: { key: value } -> [ seq2 ]
 * to: [ seq0, seq1 ] -> [ seq0, seq1, sec2 ]
 *
 * from: { another-key: another-value }
 * to: scalar -> { another-key: another-value }
 * to: { key: value } -> { key: value, another-key: another-value }
 * to: [ seq0, seq1 ] -> { another-key: another-value }
 *
 * from: { key: another-value }
 * to: scalar -> { key: another-value }
 * to: { key: value } -> { key: another-value }
 * to: [ seq0, seq1 ] -> { key: another-value }
 *
 * The rules are:
 * - If target node changes type, source ovewrites target.
 * - If source or target node are scalars, source it overwrites target.
 * - If target and source are sequences the items of the source sequence
 *   are appended to the target sequence.
 * - If target and source are maps the key, value pairs of the source
 *   are appended to the target map. If the target map contains a
 *   key-value pair that is present in the source map, it is overwriten
 *   by it.
 *
 * @fyn_to: The target node
 * @fyn_from: The source node
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_insert(struct fy_node *fyn_to, struct fy_node *fyn_from)
	FY_EXPORT;

/**
 * fy_document_insert_at() - Insert a node to the given path in the document
 *
 * Insert a node to a given point in the document. If @fyn is NULL then this
 * operation will delete the target node.
 *
 * Please see fy_node_insert for details of operation.
 *
 * Note that in any case the fyn node will be unref'ed.
 * So if the operation fails, and the reference is 0
 * the node will be freed. If you want it to stick around
 * take a reference before.
 *
 * @fyd: The document
 * @path: The path where the insert operation will target
 * @pathlen: The length of the path (or -1 if '\0' terminated)
 * @fyn: The source node
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_insert_at(struct fy_document *fyd,
		      const char *path, size_t pathlen,
		      struct fy_node *fyn)
	FY_EXPORT;

/**
 * enum fy_node_type - Node type
 *
 * Each node may be one of the following types
 *
 * @FYNT_SCALAR: Node is a scalar
 * @FYNT_SEQUENCE: Node is a sequence
 * @FYNT_MAPPING: Node is a mapping
 */
enum fy_node_type {
	FYNT_SCALAR,
	FYNT_SEQUENCE,
	FYNT_MAPPING,
};

/**
 * enum fy_node_style - Node style
 *
 * A node may contain a hint of how it should be
 * rendered, encoded as a style.
 *
 * @FYNS_ANY: No hint, let the emitter decide
 * @FYNS_FLOW: Prefer flow style (for sequence/mappings)
 * @FYNS_BLOCK: Prefer block style (for sequence/mappings)
 * @FYNS_PLAIN: Plain style preferred
 * @FYNS_SINGLE_QUOTED: Single quoted style preferred
 * @FYNS_DOUBLE_QUOTED: Double quoted style preferred
 * @FYNS_LITERAL: Literal style preferred (valid in block context)
 * @FYNS_FOLDED: Folded style preferred (valid in block context)
 * @FYNS_ALIAS: It's an alias
 */
enum fy_node_style {
	FYNS_ANY = -1,
	FYNS_FLOW,
	FYNS_BLOCK,
	FYNS_PLAIN,
	FYNS_SINGLE_QUOTED,
	FYNS_DOUBLE_QUOTED,
	FYNS_LITERAL,
	FYNS_FOLDED,
	FYNS_ALIAS,
};

/* maximum depth is 256 */
#define FYNWF_MAXDEPTH_SHIFT	4
#define FYNWF_MAXDEPTH_MASK	0xff
#define FYNWF_MAXDEPTH(x)	(((x) & FYNWF_MAXDEPTH_MASK) << FYNWF_MAXDEPTH_SHIFT)
#define FYNWF_MARKER_SHIFT	12
#define FYNWF_MARKER_MASK	0x1f
#define FYNWF_MARKER(x)		(((x) & FYNWF_MARKER_MASK) << FYNWF_MARKER_SHIFT)
#define FYNWF_PTR_SHIFT		16
#define FYNWF_PTR_MASK		0x03
#define FYNWF_PTR(x)		(((x) & FYNWF_PTR_MASK) << FYNWF_PTR_SHIFT)

/**
 * enum fy_node_walk_flags - Node walk flags
 *
 * @FYNWF_DONT_FOLLOW: Don't follow aliases during pathwalk
 * @FYNWF_FOLLOW: Follow aliases during pathwalk
 * @FYNWF_PTR_YAML: YAML pointer path walks
 * @FYNWF_PTR_JSON: JSON pointer path walks
 * @FYNWF_PTR_RELJSON: Relative JSON pointer path walks
 * @FYNWF_PTR_YPATH: YPATH pointer path walks
 * @FYNWF_URI_ENCODED: The path is URI encoded
 * @FYNWF_MAXDEPTH_DEFAULT: Max follow depth is automatically determined
 * @FYNWF_MARKER_DEFAULT: Default marker to use when scanning
 * @FYNWF_PTR_DEFAULT: Default path type
 */
enum fy_node_walk_flags {
	FYNWF_DONT_FOLLOW = 0,
	FYNWF_FOLLOW = FY_BIT(0),
	FYNWF_PTR_YAML = FYNWF_PTR(0),
	FYNWF_PTR_JSON = FYNWF_PTR(1),
	FYNWF_PTR_RELJSON = FYNWF_PTR(2),
	FYNWF_PTR_YPATH = FYNWF_PTR(3),
	FYNWF_URI_ENCODED = FY_BIT(18),
	FYNWF_MAXDEPTH_DEFAULT = FYNWF_MAXDEPTH(0),
	FYNWF_MARKER_DEFAULT = FYNWF_MARKER(0),
	FYNWF_PTR_DEFAULT = FYNWF_PTR(0),
};

/* the maximum user marker */
#define FYNWF_MAX_USER_MARKER	24

/**
 * fy_node_style_from_scalar_style() - Convert from scalar to node style
 *
 * Convert a scalar style to a node style.
 *
 * @sstyle: The input scalar style
 *
 * Returns:
 * The matching node style
 */
static inline enum fy_node_style
fy_node_style_from_scalar_style(enum fy_scalar_style sstyle)
{
	if (sstyle == FYSS_ANY)
		return FYNS_ANY;
	return (enum fy_node_style)(FYNS_PLAIN + (sstyle - FYSS_PLAIN));
}

/**
 * typedef fy_node_mapping_sort_fn - Mapping sorting method function
 *
 * @fynp_a: The first node_pair used in the comparison
 * @fynp_b: The second node_pair used in the comparison
 * @arg: The opaque user provided pointer to the sort operation
 *
 * Returns:
 * <0 if @fynp_a is less than @fynp_b
 * 0 if @fynp_a is equal to fynp_b
 * >0 if @fynp_a is greater than @fynp_b
 */
typedef int (*fy_node_mapping_sort_fn)(const struct fy_node_pair *fynp_a,
				       const struct fy_node_pair *fynp_b,
				       void *arg);

/**
 * typedef fy_node_scalar_compare_fn - Node compare method function for scalars
 *
 * @fyn_a: The first scalar node used in the comparison
 * @fyn_b: The second scalar node used in the comparison
 * @arg: The opaque user provided pointer to the compare operation
 *
 * Returns:
 * <0 if @fyn_a is less than @fyn_b
 * 0 if @fyn_a is equal to fyn_b
 * >0 if @fyn_a is greater than @fyn_b
 */
typedef int (*fy_node_scalar_compare_fn)(struct fy_node *fyn_a,
					 struct fy_node *fyn_b,
					 void *arg);

/**
 * fy_node_compare() - Compare two nodes for equality
 *
 * Compare two nodes for equality.
 * The comparison is 'deep', i.e. it recurses in subnodes,
 * and orders the keys of maps using default libc strcmp
 * ordering. For scalar the comparison is performed after
 * any escaping so it's a true content comparison.
 *
 * @fyn1: The first node to use in the comparison
 * @fyn2: The second node to use in the comparison
 *
 * Returns:
 * true if the nodes contain the same content, false otherwise
 */
bool
fy_node_compare(struct fy_node *fyn1, struct fy_node *fyn2)
	FY_EXPORT;

/**
 * fy_node_compare_user() - Compare two nodes for equality using
 * 			    user supplied sort and scalar compare methods
 *
 * Compare two nodes for equality using user supplied sot and scalar
 * compare methods.
 * The comparison is 'deep', i.e. it recurses in subnodes,
 * and orders the keys of maps using the supplied mapping sort method for
 * ordering. For scalars the comparison is performed using the supplied
 * scalar node compare methods.
 *
 * @fyn1: The first node to use in the comparison
 * @fyn2: The second node to use in the comparison
 * @sort_fn: The method to use for sorting maps, or NULL for the default
 * @sort_fn_arg: The extra user supplied argument to the @sort_fn
 * @cmp_fn: The method to use for comparing scalars, or NULL for the default
 * @cmp_fn_arg: The extra user supplied argument to the @cmp_fn
 *
 * Returns:
 * true if the nodes contain the same content, false otherwise
 */
bool
fy_node_compare_user(struct fy_node *fyn1, struct fy_node *fyn2,
		     fy_node_mapping_sort_fn sort_fn, void *sort_fn_arg,
		     fy_node_scalar_compare_fn cmp_fn, void *cmp_fn_arg)
	FY_EXPORT;

/**
 * fy_node_compare_string() - Compare a node for equality with a YAML string
 *
 * Compare a node for equality with a YAML string.
 * The comparison is performed using fy_node_compare() with the
 * first node supplied as an argument and the second being generated
 * by calling fy_document_build_from_string with the YAML string.
 *
 * @fyn: The node to use in the comparison
 * @str: The YAML string to compare against
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * true if the node and the string are equal.
 */
bool
fy_node_compare_string(struct fy_node *fyn, const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_node_compare_token() - Compare a node for equality against a token
 *
 * Compare a node for equality with a token.
 * Both the node and the tokens must be a scalars.
 *
 * @fyn: The node to use in the comparison
 * @fyt: The scalar token
 *
 * Returns:
 * true if the node and the token are equal.
 */
bool
fy_node_compare_token(struct fy_node *fyn, struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_node_compare_text() - Compare a node for equality with a raw C text
 *
 * Compare a node for equality with a raw C string.
 * The node must be a scalar.
 *
 * @fyn: The node to use in the comparison
 * @text: The raw C text to compare against
 * @len: The length of the text (or -1 if '\0' terminated)
 *
 * Returns:
 * true if the node and the text are equal.
 */
bool
fy_node_compare_text(struct fy_node *fyn, const char *text, size_t len)
	FY_EXPORT;

/**
 * fy_document_create() - Create an empty document
 *
 * Create an empty document using the provided parser configuration.
 * If NULL use the default parse configuration.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 *
 * Returns:
 * The created empty document, or NULL on error.
 */
struct fy_document *
fy_document_create(const struct fy_parse_cfg *cfg)
	FY_EXPORT;

/**
 * fy_document_destroy() - Destroy a document previously created via
 *                         fy_document_create()
 *
 * Destroy a document (along with all children documents)
 *
 * @fyd: The document to destroy
 *
 */
void
fy_document_destroy(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_get_cfg() - Get the configuration of a document
 *
 * @fyd: The document
 *
 * Returns:
 * The configuration of the document
 */
const struct fy_parse_cfg *
fy_document_get_cfg(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_get_diag() - Get the diagnostic object of a document
 *
 * Return a pointer to the diagnostic object of a document object.
 * Note that the returned diag object has a reference taken so
 * you should fy_diag_unref() it when you're done with it.
 *
 * @fyd: The document to get the diagnostic object
 *
 * Returns:
 * A pointer to a ref'ed diagnostic object or NULL in case of an
 * error.
 */
struct fy_diag *
fy_document_get_diag(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_set_diag() - Set the diagnostic object of a document
 *
 * Replace the documents's current diagnostic object with the one
 * given as an argument. The previous diagnostic object will be
 * unref'ed (and freed if its reference gets to 0).
 * Also note that the diag argument shall take a reference too.
 *
 * @fyd: The document to replace the diagnostic object
 * @diag: The document's new diagnostic object, NULL for default
 *
 * Returns:
 * 0 if everything OK, -1 otherwise
 */
int
fy_document_set_diag(struct fy_document *fyd, struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_document_set_parent() - Make a document a child of another
 *
 * Set the parent of @fyd_child document to be @fyd
 *
 * @fyd: The parent document
 * @fyd_child: The child document
 *
 * Returns:
 * 0 if all OK, -1 on error.
 */
int
fy_document_set_parent(struct fy_document *fyd, struct fy_document *fyd_child)
	FY_EXPORT;

/**
 * fy_document_build_from_string() - Create a document using the provided YAML source.
 *
 * Create a document parsing the provided string as a YAML source.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @str: The YAML source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_build_from_string(const struct fy_parse_cfg *cfg,
			      const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_document_build_from_malloc_string() - Create a document using the provided YAML source which was malloced.
 *
 * Create a document parsing the provided string as a YAML source. The string is expected to have been
 * allocated by malloc(3) and when the document is destroyed it will be automatically freed.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @str: The YAML source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_build_from_malloc_string(const struct fy_parse_cfg *cfg,
				     char *str, size_t len)
	FY_EXPORT;
/**
 * fy_document_build_from_file() - Create a document parsing the given file
 *
 * Create a document parsing the provided file as a YAML source.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @file: The name of the file to parse
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_build_from_file(const struct fy_parse_cfg *cfg, const char *file)
	FY_EXPORT;

/**
 * fy_document_build_from_fp() - Create a document parsing the given file pointer
 *
 * Create a document parsing the provided file pointer as a YAML source.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @fp: The file pointer
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_build_from_fp(const struct fy_parse_cfg *cfg, FILE *fp)
	FY_EXPORT;

/**
 * fy_document_vbuildf() - Create a document using the provided YAML via vprintf formatting
 *
 * Create a document parsing the provided string as a YAML source. The string
 * is created by applying vprintf formatting.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @fmt: The format string creating the YAML source to use.
 * @ap: The va_list argument pointer
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_vbuildf(const struct fy_parse_cfg *cfg,
		    const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_document_buildf() - Create a document using the provided YAML source via printf formatting
 *
 * Create a document parsing the provided string as a YAML source. The string
 * is created by applying printf formatting.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @fmt: The format string creating the YAML source to use.
 * @...: The printf arguments
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_document_buildf(const struct fy_parse_cfg *cfg, const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 2, 3))
	FY_EXPORT;

/**
 * fy_flow_document_build_from_string() - Create a document using the provided YAML source.
 *
 * Create a document parsing the provided string as a YAML source.
 *
 * The document is a flow document, i.e. does not contain any block content
 * and is usually laid out in a single line.
 *
 * Example of flow documents:
 *
 * plain-scalar
 * "double-quoted-scalar"
 * 'single-quoted-scalar'
 * { foo: bar }
 * [ 0, 1, 2 ]
 *
 * A flow document is important because parsing stops at the end
 * of it, and so can be placed in other non-yaml content.
 *
 * @cfg: The parse configuration to use or NULL for the default.
 * @str: The YAML source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 * @consumed: A pointer to the consumed amount
 *
 * Returns:
 * The created document, or NULL on error.
 */
struct fy_document *
fy_flow_document_build_from_string(const struct fy_parse_cfg *cfg,
				   const char *str, size_t len, size_t *consumed)
	FY_EXPORT;

/**
 * fy_document_root() - Return the root node of the document
 *
 * Returns the root of the document. If the document is empty
 * NULL will be returned instead.
 *
 * @fyd: The document
 *
 * Returns:
 * The root of the document, or NULL if the document is empty.
 */
struct fy_node *
fy_document_root(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_set_root() - Set the root of the document
 *
 * Set the root of a document. If the document was not empty
 * the old root will be freed. If @fyn is NULL then the
 * document is set to empty.
 *
 * @fyd: The document
 * @fyn: The new root of the document.
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_set_root(struct fy_document *fyd, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_type() - Get the node type
 *
 * Retrieve the node type. It is one of FYNT_SCALAR, FYNT_SEQUENCE
 * or FYNT_MAPPING. A NULL node argument is a FYNT_SCALAR.
 *
 * @fyn: The node
 *
 * Returns:
 * The node type
 */
enum fy_node_type
fy_node_get_type(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_style() - Get the node style
 *
 * Retrieve the node rendering style.
 * If the node is NULL then the style is FYNS_PLAIN.
 *
 * @fyn: The node
 *
 * Returns:
 * The node style
 */
enum fy_node_style
fy_node_get_style(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_set_style() - Set the node style
 *
 * Set the node rendering style.
 * If current node style is alias it won't be changed
 * to save document structure
 *
 * @fyn: The node
 * @style: The node style
 */
void
fy_node_set_style(struct fy_node *fyn, enum fy_node_style style)
       FY_EXPORT;

/**
 * fy_node_is_scalar() - Check whether the node is a scalar
 *
 * Convenience method for checking whether a node is a scalar.
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is a scalar, false otherwise
 */
static inline bool
fy_node_is_scalar(struct fy_node *fyn)
{
	return fy_node_get_type(fyn) == FYNT_SCALAR;
}

/**
 * fy_node_is_sequence() - Check whether the node is a sequence
 *
 * Convenience method for checking whether a node is a sequence.
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is a sequence, false otherwise
 */
static inline bool
fy_node_is_sequence(struct fy_node *fyn)
{
	return fy_node_get_type(fyn) == FYNT_SEQUENCE;
}

/**
 * fy_node_is_mapping() - Check whether the node is a mapping
 *
 * Convenience method for checking whether a node is a mapping.
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is a mapping, false otherwise
 */
static inline bool
fy_node_is_mapping(struct fy_node *fyn)
{
	return fy_node_get_type(fyn) == FYNT_MAPPING;
}

/**
 * fy_node_is_alias() - Check whether the node is an alias
 *
 * Convenience method for checking whether a node is an alias.
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is an alias, false otherwise
 */
static inline bool
fy_node_is_alias(struct fy_node *fyn)
{
	return fy_node_get_type(fyn) == FYNT_SCALAR &&
	       fy_node_get_style(fyn) == FYNS_ALIAS;
}

/**
 * fy_node_is_null() - Check whether the node is a NULL
 *
 * Convenience method for checking whether a node is a NULL scalar..
 * Note that a NULL node argument returns true...
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is a NULL scalar, false otherwise
 */
bool
fy_node_is_null(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_is_attached() - Check whether the node is attached
 *
 * Checks whether a node is attached in a document structure.
 * An attached node may not be freed, before being detached.
 * Note that there is no method that explicitly detaches
 * a node, since this is handled internaly by the sequence
 * and mapping removal methods.
 *
 * @fyn: The node
 *
 * Returns:
 * true if the node is attached, false otherwise
 */
bool
fy_node_is_attached(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_tag_token() - Gets the tag token of a node (if it exists)
 *
 * Gets the tag token of a node, if it exists
 *
 * @fyn: The node which has the tag token to be returned
 *
 * Returns:
 * The tag token of the given node, or NULL if the tag does not
 * exist.
 */
struct fy_token *
fy_node_get_tag_token(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_scalar_token() - Gets the scalar token of a node (if it exists)
 *
 * Gets the scalar token of a node, if it exists and the node is a valid scalar
 * node. Note that aliases are scalars, so if this call is issued on an alias
 * node the return shall be of an alias token.
 *
 * @fyn: The node which has the scalar token to be returned
 *
 * Returns:
 * The scalar token of the given node, or NULL if the node is not a scalar.
 */
struct fy_token *
fy_node_get_scalar_token(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_resolve_alias() - Resolve an alias node
 *
 * Resolve an alias node, following any subsequent aliases until
 * a non alias node has been found. This call performs cycle detection
 * and excessive redirections checks so it's safe to call in any
 * context.
 *
 * @fyn: The alias node to be resolved
 *
 * Returns:
 * The resolved alias node, or NULL if either fyn is not an alias, or
 * resolution fails due to a graph cycle.
 */
struct fy_node *
fy_node_resolve_alias(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_dereference() - Dereference a single alias node
 *
 * Dereference an alias node. This is different than resolution
 * in that will only perform a single alias follow call and
 * it will fail if the input is not an alias.
 * This call performs cycle detection
 * and excessive redirections checks so it's safe to call in any
 * context.
 *
 * @fyn: The alias node to be dereferenced
 *
 * Returns:
 * The dereferenced alias node, or NULL if either fyn is not an alias, or
 * resolution fails due to a graph cycle.
 */
struct fy_node *
fy_node_dereference(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_free() - Free a node
 *
 * Recursively frees the given node releasing the memory it uses, removing
 * any anchors on the document it contains, and releasing references
 * on the tokens it contains.
 *
 * This method will return an error if the node is attached, or
 * if not NULL it is not a member of a document.
 *
 * @fyn: The node to free
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_free(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_build_from_string() - Create a node using the provided YAML source.
 *
 * Create a node parsing the provided string as a YAML source. The
 * node created will be associated with the provided document.
 *
 * @fyd: The document
 * @str: The YAML source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_build_from_string(struct fy_document *fyd,
			  const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_node_build_from_malloc_string() - Create a node using the provided YAML source which was malloced.
 *
 * Create a node parsing the provided string as a YAML source. The
 * node created will be associated with the provided document. The string is expected to have been
 * allocated by malloc(3) and when the document is destroyed it will be automatically freed.
 *
 * @fyd: The document
 * @str: The YAML source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_build_from_malloc_string(struct fy_document *fyd,
				 char *str, size_t len)
	FY_EXPORT;


/**
 * fy_node_build_from_file() - Create a node using the provided YAML file.
 *
 * Create a node parsing the provided file as a YAML source. The
 * node created will be associated with the provided document.
 *
 * @fyd: The document
 * @file: The name of the file to parse
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_build_from_file(struct fy_document *fyd, const char *file)
	FY_EXPORT;

/**
 * fy_node_build_from_fp() - Create a node using the provided file pointer.
 *
 * Create a node parsing the provided file pointer as a YAML source. The
 * node created will be associated with the provided document.
 *
 * @fyd: The document
 * @fp: The file pointer
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_build_from_fp(struct fy_document *fyd, FILE *fp)
	FY_EXPORT;

/**
 * fy_node_vbuildf() - Create a node using the provided YAML source via vprintf formatting
 *
 * Create a node parsing the resulting string as a YAML source. The string
 * is created by applying vprintf formatting.
 *
 * @fyd: The document
 * @fmt: The format string creating the YAML source to use.
 * @ap: The va_list argument pointer
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_vbuildf(struct fy_document *fyd, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_node_buildf() - Create a node using the provided YAML source via printf formatting
 *
 * Create a node parsing the resulting string as a YAML source. The string
 * is created by applying printf formatting.
 *
 * @fyd: The document
 * @fmt: The format string creating the YAML source to use.
 * @...: The printf arguments
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_buildf(struct fy_document *fyd, const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 2, 3))
	FY_EXPORT;

/**
 * fy_node_by_path() - Retrieve a node using the provided path spec.
 *
 * This method will retrieve a node relative to the given node using
 * the provided path spec.
 *
 * Path specs are comprised of keys seperated by slashes '/'.
 * Keys are either regular YAML expressions in flow format for traversing
 * mappings, or indexes in brackets for traversing sequences.
 * Path specs may start with '/' which is silently ignored.
 *
 * A few examples will make this clear
 *
 * fyn = { foo: bar } - fy_node_by_path(fyn, "/foo") -> bar
 * fyn = [ foo, bar ] - fy_node_by_path(fyn, "1") -> bar
 * fyn = { { foo: bar }: baz } - fy_node_by_path(fyn, "{foo: bar}") -> baz
 * fyn = [ foo, { bar: baz } } - fy_node_by_path(fyn, "1/bar") -> baz
 *
 * Note that the special characters /{}[] are not escaped in plain style,
 * so you will not be able to use them as path traversal keys.
 * In that case you can easily use either the single, or double quoted forms:
 *
 * fyn = { foo/bar: baz } - fy_node_by_path(fyn, "'foo/bar'") -> baz
 *
 * @fyn: The node to use as start of the traversal operation
 * @path: The path spec to use in the traversal operation
 * @len: The length of the path (or -1 if '\0' terminated)
 * @flags: The extra path walk flags
 *
 * Returns:
 * The retrieved node, or NULL if not possible to be found.
 */
struct fy_node *
fy_node_by_path(struct fy_node *fyn, const char *path, size_t len,
		enum fy_node_walk_flags flags)
	FY_EXPORT;

/**
 * fy_node_get_path() - Get the path of this node
 *
 * Retrieve the given node's path address relative to the document root.
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn: The node
 *
 * Returns:
 * The node's address, or NULL if fyn is the root.
 */
char *
fy_node_get_path(struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_path_alloca(_fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_path((_fyn)), FY_NT, (_res))

/**
 * fy_node_get_parent() - Get the parent node of a node
 *
 * Get the parent node of a node. The parent of a document's root
 * is NULL, and so is the parent of the root of a key node's of a mapping.
 * This is because the nodes of a key may not be addressed using a
 * path expression.
 *
 * @fyn: The node
 *
 * Returns:
 * The node's parent, or NULL if fyn is the root, or the root of a key mapping.
 */
struct fy_node *
fy_node_get_parent(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_document_parent() - Get the document parent node of a node
 *
 * Get the document parent node of a node. The document parent differs
 * than the regular parent in that a key's root node of a mapping is not
 * NULL, rather it points to the actual node parent.
 *
 * @fyn: The node
 *
 * Returns:
 * The node's document parent, or NULL if fyn is the root
 */
struct fy_node *
fy_node_get_document_parent(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_parent_address() - Get the path address of this node's parent
 *
 * Retrieve the given node's parent path address
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn: The node
 *
 * Returns:
 * The parent's address, or NULL if fyn is the root.
 */
char *
fy_node_get_parent_address(struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_parent_address_alloca(_fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_parent_address((_fyn)), FY_NT, (_res))

/**
 * fy_node_get_path_relative_to() - Get a path address of a node
 *                                  relative to one of it's parents
 *
 * Retrieve the given node's path address relative to an arbitrary
 * parent in the tree.
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn_parent: The node parent/grandparent...
 * @fyn: The node
 *
 * Returns:
 * The relative address from the parent to the node
 */
char *
fy_node_get_path_relative_to(struct fy_node *fyn_parent, struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_path_relative_to_alloca(_fynp, _fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_path_relative_to((_fynp), (_fyn)), FY_NT, (_res))

/**
 * fy_node_get_short_path() - Get a path address of a node in the shortest
 *                            path possible
 *
 * Retrieve the given node's short path address relative to the
 * closest anchor (either on this node, or it's parent).
 * If no such parent is found then returns the absolute path
 * from the start of the document.
 *
 *   --- &foo
 *   foo: &bar
 *       bar
 *   baz
 *
 * - The short path of /foo is \*foo
 * - The short path of /foo/bar is \*bar
 * - The short path of /baz is \*foo/baz
 *
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn: The node
 *
 * Returns:
 * The shortest path describing the node
 */
char *
fy_node_get_short_path(struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_short_path_alloca(_fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_short_path((_fyn)), FY_NT, (_res))

/**
 * fy_node_get_reference() - Get a textual reference to a node
 *
 * Retrieve the given node's textual reference. If the node
 * contains an anchor the expression that references the anchor
 * will be returned, otherwise an absolute path reference relative
 * to the root of the document will be returned.
 *
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn: The node
 *
 * Returns:
 * The node's text reference.
 */
char *
fy_node_get_reference(struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_reference_alloca(_fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_reference((_fyn)), FY_NT, (_res))

/**
 * fy_node_create_reference() - Create an alias reference node
 *
 * Create an alias node reference. If the node
 * contains an anchor the expression that references the alias will
 * use the anchor, otherwise an absolute path reference relative
 * to the root of the document will be created.
 *
 * @fyn: The node
 *
 * Returns:
 * An alias node referencing the argument node
 */
struct fy_node *
fy_node_create_reference(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_relative_reference() - Get a textual reference to a node
 * 				      relative to a base node.
 *
 * Retrieve the given node's textual reference as generated using
 * another parent (or grand parent) as a base.
 * If the node contains an anchor the expression that references the anchor
 * will be returned.
 * If the base node contains an anchor the reference will be relative to it
 * otherwise an absolute path reference will be returned.
 *
 * The address is dynamically allocated and should be freed when
 * you're done with it.
 *
 * @fyn_base: The base node
 * @fyn: The node
 *
 * Returns:
 * The node's text reference.
 */
char *
fy_node_get_relative_reference(struct fy_node *fyn_base, struct fy_node *fyn)
	FY_EXPORT;

#define fy_node_get_relative_reference_alloca(_fynb, _fyn, _res) \
	FY_ALLOCA_COPY_FREE_NO_NULL(fy_node_get_relative_reference((_fynb), (_fyn)), FY_NT, (_res))

/**
 * fy_node_create_relative_reference() - Create an alias reference node
 *
 * Create a relative alias node reference using
 * another parent (or grand parent) as a base.
 * If the node contains an anchor the alias will reference the anchor.
 * If the base node contains an anchor the alias will be relative to it
 * otherwise an absolute path reference will be created.
 *
 * @fyn_base: The base node
 * @fyn: The node
 *
 * Returns:
 * An alias node referencing the argument node relative to the base
 */
struct fy_node *
fy_node_create_relative_reference(struct fy_node *fyn_base, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_create_scalar() - Create a scalar node.
 *
 * Create a scalar node using the provided memory area as input.
 * The input is expected to be regular utf8 encoded. It may contain
 * escaped characters in which case the style of the scalar will be
 * set to double quoted.
 *
 * Note that the data are not copied, merely a reference is taken, so
 * it must be available while the node is in use.
 *
 * @fyd: The document which the resulting node will be associated with
 * @data: Pointer to the data area
 * @size: Size of the data area, or (size_t)-1 for '\0' terminated data.
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_scalar(struct fy_document *fyd,
		      const char *data, size_t size)
	FY_EXPORT;

/**
 * fy_node_create_scalar_copy() - Create a scalar node copying the data.
 *
 * Create a scalar node using the provided memory area as input.
 * The input is expected to be regular utf8 encoded. It may contain
 * escaped characters in which case the style of the scalar will be
 * set to double quoted.
 *
 * A copy of the data will be made, so it is safe to free the data
 * after the call.
 *
 * @fyd: The document which the resulting node will be associated with
 * @data: Pointer to the data area
 * @size: Size of the data area, or (size_t)-1 for '\0' terminated data.
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_scalar_copy(struct fy_document *fyd,
			   const char *data, size_t size)
	FY_EXPORT;

/**
 * fy_node_create_vscalarf() - vprintf interface for creating scalars
 *
 * Create a scalar node using a vprintf interface.
 * The input is expected to be regular utf8 encoded. It may contain
 * escaped characters in which case the style of the scalar will be
 * set to double quoted.
 *
 * @fyd: The document which the resulting node will be associated with
 * @fmt: The printf based format string
 * @ap: The va_list containing the arguments
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_vscalarf(struct fy_document *fyd, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_node_create_scalarf() - printf interface for creating scalars
 *
 * Create a scalar node using a printf interface.
 * The input is expected to be regular utf8 encoded. It may contain
 * escaped characters in which case the style of the scalar will be
 * set to double quoted.
 *
 * @fyd: The document which the resulting node will be associated with
 * @fmt: The printf based format string
 * @...: The arguments
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_scalarf(struct fy_document *fyd, const char *fmt, ...)
	FY_EXPORT
	FY_ATTRIBUTE(format(printf, 2, 3));

/**
 * fy_node_create_sequence() - Create an empty sequence node.
 *
 * Create an empty sequence node associated with the given document.
 *
 * @fyd: The document which the resulting node will be associated with
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_sequence(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_node_create_mapping() - Create an empty mapping node.
 *
 * Create an empty mapping node associated with the given document.
 *
 * @fyd: The document which the resulting node will be associated with
 *
 * Returns:
 * The created node, or NULL on error.
 */
struct fy_node *
fy_node_create_mapping(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_node_set_tag() - Set the tag of node
 *
 * Manually set the tag of a node. The tag must be a valid one for
 * the document the node belongs to.
 *
 * Note that the data are not copied, merely a reference is taken, so
 * it must be available while the node is in use.
 *
 * If the node already contains a tag it will be overwriten.
 *
 * @fyn: The node to set it's tag.
 * @data: Pointer to the tag data.
 * @len: Size of the tag data, or (size_t)-1 for '\0' terminated.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_set_tag(struct fy_node *fyn, const char *data, size_t len)
	FY_EXPORT;

/**
 * fy_node_remove_tag() - Remove the tag of node
 *
 * Remove the tag of a node.
 *
 * @fyn: The node to remove it's tag.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_remove_tag(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_tag() - Get the tag of the node
 *
 * This method will return a pointer to the text of a tag
 * along with the length of it. Note that this text is *not*
 * NULL terminated.
 *
 * @fyn: The node
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the tag of the node, while @lenp will be assigned the
 * length of said tag.
 * A NULL will be returned in case of an error.
 */
const char *
fy_node_get_tag(struct fy_node *fyn, size_t *lenp)
	FY_EXPORT;

/**
 * fy_node_get_scalar() - Get the scalar content of the node
 *
 * This method will return a pointer to the text of the scalar
 * content of a node along with the length of it.
 * Note that this pointer is *not* NULL terminated.
 *
 * @fyn: The scalar node
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the scalar content of the node, while @lenp will be assigned the
 * length of said content.
 * A NULL will be returned in case of an error, i.e. the node is not
 * a scalar.
 */
const char *
fy_node_get_scalar(struct fy_node *fyn, size_t *lenp)
	FY_EXPORT;

/**
 * fy_node_get_scalar0() - Get the scalar content of the node
 *
 * This method will return a pointer to the text of the scalar
 * content of a node as a null terminated string.
 * Note that this call will allocate memory to hold the null terminated
 * string so if possible use fy_node_get_scalar()
 *
 * @fyn: The scalar node
 *
 * Returns:
 * A pointer to the scalar content of the node or NULL in returned in case of an error.
 */
const char *
fy_node_get_scalar0(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_scalar_length() - Get the length of the scalar content
 *
 * This method will return the size of the scalar content of the node.
 * If the node is not a scalar it will return 0.
 *
 * @fyn: The scalar node
 *
 * Returns:
 * The size of the scalar content, or 0 if node is not scalar.
 */
size_t
fy_node_get_scalar_length(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_scalar_utf8_length() - Get the length of the scalar content
 * 				      in utf8 characters
 *
 * This method will return the size of the scalar content of the node in
 * utf8 characters.
 * If the node is not a scalar it will return 0.
 *
 * @fyn: The scalar node
 *
 * Returns:
 * The size of the scalar content in utf8 characters, or 0 if node is not scalar.
 */
size_t
fy_node_get_scalar_utf8_length(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_iterate() - Iterate over a sequence node
 *
 * This method iterates over all the item nodes in the sequence node.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fyn: The sequence node
 * @prevp: The previous sequence iterator
 *
 * Returns:
 * The next node in sequence or NULL at the end of the sequence.
 */
struct fy_node *
fy_node_sequence_iterate(struct fy_node *fyn, void **prevp)
	FY_EXPORT;

/**
 * fy_node_sequence_reverse_iterate() - Iterate over a sequence node in reverse
 *
 * This method iterates in reverse over all the item nodes in the sequence node.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fyn: The sequence node
 * @prevp: The previous sequence iterator
 *
 * Returns:
 * The next node in reverse sequence or NULL at the end of the sequence.
 */
struct fy_node *
fy_node_sequence_reverse_iterate(struct fy_node *fyn, void **prevp)
	FY_EXPORT;

/**
 * fy_node_sequence_item_count() - Return the item count of the sequence
 *
 * Get the item count of the sequence.
 *
 * @fyn: The sequence node
 *
 * Returns:
 * The count of items in the sequence or -1 in case of an error.
 */
int
fy_node_sequence_item_count(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_is_empty() - Check whether the sequence is empty
 *
 * Check whether the sequence contains items.
 *
 * @fyn: The sequence node
 *
 * Returns:
 * true if the node is a sequence containing items, false otherwise
 */
bool
fy_node_sequence_is_empty(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_get_by_index() - Return a sequence item by index
 *
 * Retrieve a node in the sequence using it's index. If index
 * is positive or zero the count is from the start of the sequence,
 * while if negative from the end. I.e. -1 returns the last item
 * in the sequence.
 *
 * @fyn: The sequence node
 * @index: The index of the node to retrieve.
 *         - >= 0 counting from the start
 *         - < 0 counting from the end
 *
 * Returns:
 * The node at the specified index or NULL if no such item exist.
 */
struct fy_node *
fy_node_sequence_get_by_index(struct fy_node *fyn, int index)
	FY_EXPORT;

/**
 * fy_node_sequence_append() - Append a node item to a sequence
 *
 * Append a node item to a sequence.
 *
 * @fyn_seq: The sequence node
 * @fyn: The node item to append
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_sequence_append(struct fy_node *fyn_seq, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_prepend() - Append a node item to a sequence
 *
 * Prepend a node item to a sequence.
 *
 * @fyn_seq: The sequence node
 * @fyn: The node item to prepend
 *
 * Returns:
 * 0 on success, -1 on error
 */
int fy_node_sequence_prepend(struct fy_node *fyn_seq, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_insert_before() - Insert a node item before another
 *
 * Insert a node item before another in the sequence.
 *
 * @fyn_seq: The sequence node
 * @fyn_mark: The node item which the node will be inserted before.
 * @fyn: The node item to insert in the sequence.
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_sequence_insert_before(struct fy_node *fyn_seq,
			       struct fy_node *fyn_mark, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_insert_after() - Insert a node item after another
 *
 * Insert a node item after another in the sequence.
 *
 * @fyn_seq: The sequence node
 * @fyn_mark: The node item which the node will be inserted after.
 * @fyn: The node item to insert in the sequence.
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_sequence_insert_after(struct fy_node *fyn_seq,
			      struct fy_node *fyn_mark, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_remove() - Remove an item from a sequence
 *
 * Remove a node item from a sequence and return it.
 *
 * @fyn_seq: The sequence node
 * @fyn: The node item to remove from the sequence.
 *
 * Returns:
 * The removed node item fyn, or NULL in case of an error.
 */
struct fy_node *
fy_node_sequence_remove(struct fy_node *fyn_seq, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_iterate() - Iterate over a mapping node
 *
 * This method iterates over all the node pairs in the mapping node.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * Note that while a mapping is an unordered collection of key/values
 * the order of which they are created is important for presentation
 * purposes.
 *
 * @fyn: The mapping node
 * @prevp: The previous sequence iterator
 *
 * Returns:
 * The next node pair in the mapping or NULL at the end of the mapping.
 */
struct fy_node_pair *
fy_node_mapping_iterate(struct fy_node *fyn, void **prevp)
	FY_EXPORT;

/**
 * fy_node_mapping_reverse_iterate() - Iterate over a mapping node in reverse
 *
 * This method iterates in reverse over all the node pairs in the mapping node.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * Note that while a mapping is an unordered collection of key/values
 * the order of which they are created is important for presentation
 * purposes.
 *
 * @fyn: The mapping node
 * @prevp: The previous sequence iterator
 *
 * Returns:
 * The next node pair in reverse sequence in the mapping or NULL at the end of the mapping.
 */
struct fy_node_pair *
fy_node_mapping_reverse_iterate(struct fy_node *fyn, void **prevp)
	FY_EXPORT;

/**
 * fy_node_mapping_item_count() - Return the node pair count of the mapping
 *
 * Get the count of the node pairs in the mapping.
 *
 * @fyn: The mapping node
 *
 * Returns:
 * The count of node pairs in the mapping or -1 in case of an error.
 */
int
fy_node_mapping_item_count(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_is_empty() - Check whether the mapping is empty
 *
 * Check whether the mapping contains any node pairs.
 *
 * @fyn: The mapping node
 *
 * Returns:
 * true if the node is a mapping containing node pairs, false otherwise
 */
bool
fy_node_mapping_is_empty(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_get_by_index() - Return a node pair by index
 *
 * Retrieve a node pair in the mapping using its index. If index
 * is positive or zero the count is from the start of the sequence,
 * while if negative from the end. I.e. -1 returns the last node pair
 * in the mapping.
 *
 * @fyn: The mapping node
 * @index: The index of the node pair to retrieve.
 *         - >= 0 counting from the start
 *         - < 0 counting from the end
 *
 * Returns:
 * The node pair at the specified index or NULL if no such item exist.
 */
struct fy_node_pair *
fy_node_mapping_get_by_index(struct fy_node *fyn, int index)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_pair_by_string() - Lookup a node pair in mapping by string
 *
 * This method will return the node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using fy_node_compare()
 *
 * @fyn: The mapping node
 * @key: The YAML source to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The value matching the given key, or NULL if not found.
 */
struct fy_node_pair *
fy_node_mapping_lookup_pair_by_string(struct fy_node *fyn,
				      const char *key, size_t len)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_by_string() - Lookup a node value in mapping by string
 *
 * This method will return the value of node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using fy_node_compare()
 *
 * @fyn: The mapping node
 * @key: The YAML source to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The value matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_by_string(struct fy_node *fyn,
				 const char *key, size_t len)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_value_by_string() - Lookup a node value in mapping by string
 *
 * This method will return the value of node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using fy_node_compare()
 *
 * It is synonymous with fy_node_mapping_lookup_by_string().
 *
 * @fyn: The mapping node
 * @key: The YAML source to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The value matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_value_by_string(struct fy_node *fyn,
				       const char *key, size_t len)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_key_by_string() - Lookup a node key in mapping by string
 *
 * This method will return the key of node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using fy_node_compare()
 *
 * @fyn: The mapping node
 * @key: The YAML source to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The key matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_key_by_string(struct fy_node *fyn,
				     const char *key, size_t len)
	FY_EXPORT;


/**
 * fy_node_mapping_lookup_pair_by_simple_key() - Lookup a node pair in mapping by simple string
 *
 * This method will return the node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using by comparing the strings for identity.
 *
 * @fyn: The mapping node
 * @key: The string to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The node pair matching the given key, or NULL if not found.
 */
struct fy_node_pair *
fy_node_mapping_lookup_pair_by_simple_key(struct fy_node *fyn,
					  const char *key, size_t len)
	FY_EXPORT;
/**
 * fy_node_mapping_lookup_value_by_simple_key() - Lookup a node value in mapping by simple string
 *
 * This method will return the value of node pair that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using by comparing the strings for identity.
 *
 * @fyn: The mapping node
 * @key: The string to use as key
 * @len: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The value matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_value_by_simple_key(struct fy_node *fyn,
					   const char *key, size_t len)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_pair_by_null_key() - Lookup a node pair in mapping that has a NULL key
 *
 * This method will return the node pair that has a NULL key.
 * Note this method is not using the mapping accelerator
 * and arguably NULL keys should not exist. Alas...
 *
 * @fyn: The mapping node
 *
 * Returns:
 * The node pair with a NULL key, NULL otherwise
 */
struct fy_node_pair *
fy_node_mapping_lookup_pair_by_null_key(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_value_by_null_key() - Lookup a node value with a NULL key.
 *
 * Return the value of a node pair that has a NULL key.
 *
 * @fyn: The mapping node
 *
 * Returns:
 * The value matching the null key, NULL otherwise.
 * Note that the value may be NULL too, but for that pathological case
 * use the node pair method instead.
 */
struct fy_node *
fy_node_mapping_lookup_value_by_null_key(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_scalar_by_simple_key() - Lookup a scalar in mapping by simple string
 *
 * This method will return the scalar contents that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using by comparing the strings for identity.
 *
 * @fyn: The mapping node
 * @lenp: Pointer to a variable that will hold the returned length
 * @key: The string to use as key
 * @keylen: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The scalar contents matching the given key, or NULL if not found.
 */
const char *
fy_node_mapping_lookup_scalar_by_simple_key(struct fy_node *fyn, size_t *lenp,
					    const char *key, size_t keylen)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_scalar0_by_simple_key() - Lookup a scalar in mapping by simple string
 * 						    returning a '\0' terminated scalar
 *
 * This method will return the NUL terminated scalar contents that contains the same key
 * from the YAML node created from the @key argument. The comparison of the
 * node is using by comparing the strings for identity.
 *
 * @fyn: The mapping node
 * @key: The string to use as key
 * @keylen: The length of the key (or -1 if '\0' terminated)
 *
 * Returns:
 * The NUL terminated scalar contents matching the given key, or NULL if not found.
 */
const char *
fy_node_mapping_lookup_scalar0_by_simple_key(struct fy_node *fyn,
					     const char *key, size_t keylen)
	FY_EXPORT;

/**
 * fy_node_mapping_lookup_pair() - Lookup a node pair matching the provided key
 *
 * This method will return the node pair that matches the provided @fyn_key
 *
 * @fyn: The mapping node
 * @fyn_key: The node to use as key
 *
 * Returns:
 * The node pair matching the given key, or NULL if not found.
 */
struct fy_node_pair *
fy_node_mapping_lookup_pair(struct fy_node *fyn, struct fy_node *fyn_key)
	FY_EXPORT;


/**
 * fy_node_mapping_lookup_value_by_key() - Lookup a node pair's value matching the provided key
 *
 * This method will return the node pair that matches the provided @fyn_key
 * The key may be collection and a content match check is performed recursively
 * in order to find the right key.
 *
 * @fyn: The mapping node
 * @fyn_key: The node to use as key
 *
 * Returns:
 * The node value matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_value_by_key(struct fy_node *fyn, struct fy_node *fyn_key);

/**
 * fy_node_mapping_lookup_key_by_key() - Lookup a node pair's key matching the provided key
 *
 * This method will return the node pair that matches the provided @fyn_key
 * The key may be collection and a content match check is performed recursively
 * in order to find the right key.
 *
 * @fyn: The mapping node
 * @fyn_key: The node to use as key
 *
 * Returns:
 * The node key matching the given key, or NULL if not found.
 */
struct fy_node *
fy_node_mapping_lookup_key_by_key(struct fy_node *fyn, struct fy_node *fyn_key);

/**
 * fy_node_mapping_get_pair_index() - Return the node pair index in the mapping
 *
 * This method will return the node pair index in the mapping of the given
 * node pair argument.
 *
 * @fyn: The mapping node
 * @fynp: The node pair
 *
 * Returns:
 * The index of the node pair in the mapping or -1 in case of an error.
 */
int
fy_node_mapping_get_pair_index(struct fy_node *fyn,
			       const struct fy_node_pair *fynp)
	FY_EXPORT;

/**
 * fy_node_pair_key() - Return the key of a node pair
 *
 * This method will return the node pair's key.
 * Note that this may be NULL, which is returned also in case
 * the node pair argument is NULL, so you should protect against
 * such a case.
 *
 * @fynp: The node pair
 *
 * Returns:
 * The node pair key
 */
struct fy_node *
fy_node_pair_key(struct fy_node_pair *fynp)
	FY_EXPORT;

/**
 * fy_node_pair_value() - Return the value of a node pair
 *
 * This method will return the node pair's value.
 * Note that this may be NULL, which is returned also in case
 * the node pair argument is NULL, so you should protect against
 * such a case.
 *
 * @fynp: The node pair
 *
 * Returns:
 * The node pair value
 */
struct fy_node *
fy_node_pair_value(struct fy_node_pair *fynp)
	FY_EXPORT;

/**
 * fy_node_pair_set_key() - Sets the key of a node pair
 *
 * This method will set the key part of the node pair.
 * It will ovewrite any previous key.
 *
 * Note that no checks for duplicate keys are going to be
 * performed.
 *
 * @fynp: The node pair
 * @fyn: The key node
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_pair_set_key(struct fy_node_pair *fynp, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_pair_set_value() - Sets the value of a node pair
 *
 * This method will set the value part of the node pair.
 * It will ovewrite any previous value.
 *
 * @fynp: The node pair
 * @fyn: The value node
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_pair_set_value(struct fy_node_pair *fynp, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_mapping_append() - Append a node item to a mapping
 *
 * Append a node pair to a mapping.
 *
 * @fyn_map: The mapping node
 * @fyn_key: The node pair's key
 * @fyn_value: The node pair's value
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_mapping_append(struct fy_node *fyn_map,
		       struct fy_node *fyn_key, struct fy_node *fyn_value)
	FY_EXPORT;

/**
 * fy_node_mapping_prepend() - Prepend a node item to a mapping
 *
 * Prepend a node pair to a mapping.
 *
 * @fyn_map: The mapping node
 * @fyn_key: The node pair's key
 * @fyn_value: The node pair's value
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_mapping_prepend(struct fy_node *fyn_map,
			struct fy_node *fyn_key, struct fy_node *fyn_value)
	FY_EXPORT;

/**
 * fy_node_mapping_remove() - Remove a node pair from a mapping
 *
 * Remove node pair from a mapping.
 *
 * @fyn_map: The mapping node
 * @fynp: The node pair to remove
 *
 * Returns:
 * 0 on success, -1 on failure.
 */
int
fy_node_mapping_remove(struct fy_node *fyn_map, struct fy_node_pair *fynp)
	FY_EXPORT;

/**
 * fy_node_mapping_remove_by_key() - Remove a node pair from a mapping returning the value
 *
 * Remove node pair from a mapping using the supplied key.
 *
 * @fyn_map: The mapping node
 * @fyn_key: The node pair's key
 *
 * Returns:
 * The value part of removed node pair, or NULL in case of an error.
 */
struct fy_node *
fy_node_mapping_remove_by_key(struct fy_node *fyn_map, struct fy_node *fyn_key)
	FY_EXPORT;

/**
 * fy_node_sort() - Recursively sort node
 *
 * Recursively sort all mappings of the given node, using the given
 * comparison method (if NULL use the default one).
 *
 * @fyn: The node to sort
 * @key_cmp: The comparison method
 * @arg: An opaque user pointer for the comparison method
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_sort(struct fy_node *fyn, fy_node_mapping_sort_fn key_cmp, void *arg)
	FY_EXPORT;

/**
 * fy_node_vscanf() - Retrieve data via vscanf
 *
 * This method easily retrieves data using a familiar vscanf interface.
 * The format string is a regular scanf format string with the following format.
 *
 *   "pathspec %opt pathspec %opt..."
 *
 * Each pathspec is separated with space from the scanf option
 *
 * For example:
 * fyn = { foo: 3 } -> fy_node_scanf(fyn, "/foo %d", &var) -> var = 3
 *
 *
 * @fyn: The node to use as a pathspec root
 * @fmt: The scanf based format string
 * @ap: The va_list containing the arguments
 *
 * Returns:
 * The number of scanned arguments, or -1 on error.
 */
int fy_node_vscanf(struct fy_node *fyn, const char *fmt, va_list ap);

/**
 * fy_node_scanf() - Retrieve data via scanf
 *
 * This method easily retrieves data using a familiar vscanf interface.
 * The format string is a regular scanf format string with the following format.
 *
 *   "pathspec %opt pathspec %opt..."
 *
 * Each pathspec is separated with space from the scanf option
 *
 * For example:
 * fyn = { foo: 3 } -> fy_node_scanf(fyn, "/foo %d", &var) -> var = 3
 *
 *
 * @fyn: The node to use as a pathspec root
 * @fmt: The scanf based format string
 * @...: The arguments
 *
 * Returns:
 * The number of scanned arguments, or -1 on error.
 */
int
fy_node_scanf(struct fy_node *fyn, const char *fmt, ...)
	FY_ATTRIBUTE(format(scanf, 2, 3))
	FY_EXPORT;

/**
 * fy_document_vscanf() - Retrieve data via vscanf relative to document root
 *
 * This method easily retrieves data using a familiar vscanf interface.
 * The format string is a regular scanf format string with the following format.
 *
 *   "pathspec %opt pathspec %opt..."
 *
 * Each pathspec is separated with space from the scanf option
 *
 * For example:
 * fyd = { foo: 3 } -> fy_document_scanf(fyd, "/foo %d", &var) -> var = 3
 *
 *
 * @fyd: The document
 * @fmt: The scanf based format string
 * @ap: The va_list containing the arguments
 *
 * Returns:
 * The number of scanned arguments, or -1 on error.
 */
int
fy_document_vscanf(struct fy_document *fyd, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_document_scanf() - Retrieve data via scanf relative to document root
 *
 * This method easily retrieves data using a familiar vscanf interface.
 * The format string is a regular scanf format string with the following format.
 *
 *   "pathspec %opt pathspec %opt..."
 *
 * Each pathspec is separated with space from the scanf option
 *
 * For example:
 * fyn = { foo: 3 } -> fy_node_scanf(fyd, "/foo %d", &var) -> var = 3
 *
 *
 * @fyd: The document
 * @fmt: The scanf based format string
 * @...: The arguments
 *
 * Returns:
 * The number of scanned arguments, or -1 on error.
 */
int
fy_document_scanf(struct fy_document *fyd, const char *fmt, ...)
	FY_ATTRIBUTE(format(scanf, 2, 3))
	FY_EXPORT;

/**
 * fy_document_tag_directive_iterate() - Iterate over a document's tag directives
 *
 * This method iterates over all the documents tag directives.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fyd: The document
 * @prevp: The previous state of the iterator
 *
 * Returns:
 * The next tag directive token in the document or NULL at the end of them.
 */
struct fy_token *
fy_document_tag_directive_iterate(struct fy_document *fyd, void **prevp)
	FY_EXPORT;

/**
 * fy_document_tag_directive_lookup() - Retreive a document's tag directive
 *
 * Retreives the matching tag directive token of the document matching the handle.
 *
 * @fyd: The document
 * @handle: The handle to look for
 *
 * Returns:
 * The tag directive token with the given handle or NULL if not found
 */
struct fy_token *
fy_document_tag_directive_lookup(struct fy_document *fyd, const char *handle)
	FY_EXPORT;

/**
 * fy_tag_directive_token_handle() - Get a tag directive handle
 *
 * Retreives the tag directives token handle value. Only valid on
 * tag directive tokens.
 *
 * @fyt: The tag directive token
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the tag directive's handle, while @lenp will be assigned the
 * length of said handle.
 * A NULL will be returned in case of an error.
 */
const char *
fy_tag_directive_token_handle(struct fy_token *fyt, size_t *lenp)
	FY_EXPORT;

/**
 * fy_tag_directive_token_prefix() - Get a tag directive prefix
 *
 * Retreives the tag directives token prefix value. Only valid on
 * tag directive tokens.
 *
 * @fyt: The tag directive token
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the tag directive's prefix, while @lenp will be assigned the
 * length of said prefix.
 * A NULL will be returned in case of an error.
 */
const char *
fy_tag_directive_token_prefix(struct fy_token *fyt, size_t *lenp)
	FY_EXPORT;

/**
 * fy_document_tag_directive_add() - Add a tag directive to a document
 *
 * Add tag directive to the document.
 *
 * @fyd: The document
 * @handle: The handle of the tag directive
 * @prefix: The prefix of the tag directive
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_tag_directive_add(struct fy_document *fyd,
			      const char *handle, const char *prefix)
	FY_EXPORT;

/**
 * fy_document_tag_directive_remove() - Remove a tag directive
 *
 * Remove a tag directive from a document.
 * Note that removal is prohibited if any node is still using this tag directive.
 *
 * @fyd: The document
 * @handle: The handle of the tag directive to remove.
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_tag_directive_remove(struct fy_document *fyd, const char *handle)
	FY_EXPORT;

/**
 * fy_document_lookup_anchor() - Lookup an anchor
 *
 * Lookup for an anchor having the name provided
 *
 * @fyd: The document
 * @anchor: The anchor to look for
 * @len: The length of the anchor (or -1 if '\0' terminated)
 *
 * Returns:
 * The anchor if found, NULL otherwise
 */
struct fy_anchor *
fy_document_lookup_anchor(struct fy_document *fyd,
			  const char *anchor, size_t len)
	FY_EXPORT;

/**
 * fy_document_lookup_anchor_by_token() - Lookup an anchor by token text
 *
 * Lookup for an anchor having the name provided from the text of the token
 *
 * @fyd: The document
 * @anchor: The token contains the anchor text to look for
 *
 * Returns:
 * The anchor if found, NULL otherwise
 */
struct fy_anchor *
fy_document_lookup_anchor_by_token(struct fy_document *fyd,
				   struct fy_token *anchor)
	FY_EXPORT;

/**
 * fy_document_lookup_anchor_by_node() - Lookup an anchor by node
 *
 * Lookup for an anchor located in the given node
 *
 * @fyd: The document
 * @fyn: The node
 *
 * Returns:
 * The anchor if found, NULL otherwise
 */
struct fy_anchor *
fy_document_lookup_anchor_by_node(struct fy_document *fyd,
				  struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_anchor_get_text() - Get the text of an anchor
 *
 * This method will return a pointer to the text of an anchor
 * along with the length of it. Note that this text is *not*
 * NULL terminated.
 *
 * @fya: The anchor
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the text of the anchor, while @lenp will be assigned the
 * length of said anchor.
 * A NULL will be returned in case of an error.
 */
const char *
fy_anchor_get_text(struct fy_anchor *fya, size_t *lenp)
	FY_EXPORT;

/**
 * fy_anchor_node() - Get the node of an anchor
 *
 * This method returns the node associated with the anchor.
 *
 * @fya: The anchor
 *
 * Returns:
 * The node of the anchor, or NULL in case of an error.
 */
struct fy_node *
fy_anchor_node(struct fy_anchor *fya)
	FY_EXPORT;

/**
 * fy_document_anchor_iterate() - Iterate over a document's anchors
 *
 * This method iterates over all the documents anchors.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fyd: The document
 * @prevp: The previous state of the iterator
 *
 * Returns:
 * The next anchor in the document or NULL at the end of them.
 */
struct fy_anchor *
fy_document_anchor_iterate(struct fy_document *fyd, void **prevp)
	FY_EXPORT;

/**
 * fy_document_set_anchor() - Place an anchor
 *
 * Places an anchor to the node with the give text name.
 *
 * Note that the data are not copied, merely a reference is taken, so
 * it must be available while the node is in use.
 *
 * Also not that this method is deprecated; use fy_node_set_anchor()
 * instead.
 *
 * @fyd: The document
 * @fyn: The node to set the anchor to
 * @text: Pointer to the anchor text
 * @len: Size of the anchor text, or (size_t)-1 for '\0' terminated.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_document_set_anchor(struct fy_document *fyd, struct fy_node *fyn,
		       const char *text, size_t len)
	FY_EXPORT FY_DEPRECATED;

/**
 * fy_node_set_anchor() - Place an anchor to the node
 *
 * Places an anchor to the node with the give text name.
 *
 * Note that the data are not copied, merely a reference is taken, so
 * it must be available while the node is in use.
 *
 * This is similar to fy_document_set_anchor() with the document set
 * to the document of the @fyn node.
 *
 * @fyn: The node to set the anchor to
 * @text: Pointer to the anchor text
 * @len: Size of the anchor text, or (size_t)-1 for '\0' terminated.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_set_anchor(struct fy_node *fyn, const char *text, size_t len)
	FY_EXPORT;

/**
 * fy_node_set_anchor_copy() - Place an anchor to the node copying the text
 *
 * Places an anchor to the node with the give text name, which
 * will be copied, so it's safe to dispose the text after the call.
 *
 * @fyn: The node to set the anchor to
 * @text: Pointer to the anchor text
 * @len: Size of the anchor text, or (size_t)-1 for '\0' terminated.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_set_anchor_copy(struct fy_node *fyn, const char *text, size_t len)
	FY_EXPORT;

/**
 * fy_node_set_vanchorf() - Place an anchor to the node using a vprintf interface.
 *
 * Places an anchor to the node with the give text name as created
 * via vprintf'ing the arguments.
 *
 * @fyn: The node to set the anchor to
 * @fmt: Pointer to the anchor format string
 * @ap: The argument list.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_set_vanchorf(struct fy_node *fyn, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_node_set_anchorf() - Place an anchor to the node using a printf interface.
 *
 * Places an anchor to the node with the give text name as created
 * via printf'ing the arguments.
 *
 * @fyn: The node to set the anchor to
 * @fmt: Pointer to the anchor format string
 * @...: The extra arguments.
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_set_anchorf(struct fy_node *fyn, const char *fmt, ...)
	FY_EXPORT FY_ATTRIBUTE(format(printf, 2, 3));

/**
 * fy_node_remove_anchor() - Remove an anchor
 *
 * Remove an anchor for the given node (if it exists)
 *
 * @fyn: The node to remove anchors from
 *
 * Returns:
 * 0 on success, -1 on error.
 */
int
fy_node_remove_anchor(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_anchor() - Get the anchor of a node
 *
 * Retrieve the anchor of the given node (if it exists)
 *
 * @fyn: The node
 *
 * Returns:
 * The anchor if there's one at the node, or NULL otherwise
 */
struct fy_anchor *
fy_node_get_anchor(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_nearest_anchor() - Get the nearest anchor of the node
 *
 * Retrieve the anchor of the nearest parent of the given node or
 * the given node if it has one.
 *
 * @fyn: The node
 *
 * Returns:
 * The nearest anchor if there's one, or NULL otherwise
 */
struct fy_anchor *
fy_node_get_nearest_anchor(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_get_nearest_child_of() - Get the nearest node which is a
 * 				    child of the base
 *
 * Retrieve the nearest node which is a child of fyn_base starting
 * at fyn and working upwards.
 *
 * @fyn_base: The base node
 * @fyn: The node to start from
 *
 * Returns:
 * The nearest child of the base if there's one, or NULL otherwise
 */
struct fy_node *
fy_node_get_nearest_child_of(struct fy_node *fyn_base, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_create_alias() - Create an alias node
 *
 * Create an alias on the given document
 *
 * Note that the data are not copied, merely a reference is taken, so
 * it must be available while the node is in use.
 *
 * @fyd: The document
 * @alias: The alias text
 * @len: The length of the alias (or -1 if '\0' terminated)
 *
 * Returns:
 * The created alias node, or NULL in case of an error
 */
struct fy_node *
fy_node_create_alias(struct fy_document *fyd,
		     const char *alias, size_t len)
	FY_EXPORT;

/**
 * fy_node_create_alias_copy() - Create an alias node copying the data
 *
 * Create an alias on the given document
 *
 * A copy of the data will be made, so it is safe to free the data
 * after the call.
 *
 * @fyd: The document
 * @alias: The alias text
 * @len: The length of the alias (or -1 if '\0' terminated)
 *
 * Returns:
 * The created alias node, or NULL in case of an error
 */
struct fy_node *
fy_node_create_alias_copy(struct fy_document *fyd,
			  const char *alias, size_t len)
	FY_EXPORT;

/**
 * fy_node_get_meta() - Get the meta pointer of a node
 *
 * Return the meta pointer of a node.
 *
 * @fyn: The node to get meta data from
 *
 * Returns:
 * The stored meta data pointer
 */
void *
fy_node_get_meta(struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_set_meta() - Set the meta pointer of a node
 *
 * Set the meta pointer of a node. If @meta is NULL
 * then clear the meta data.
 *
 * @fyn: The node to set meta data
 * @meta: The meta data pointer
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_set_meta(struct fy_node *fyn, void *meta)
	FY_EXPORT;

/**
 * fy_node_clear_meta() - Clear the meta data of a node
 *
 * Clears the meta data of a node.
 *
 * @fyn: The node to clear meta data
 */
void
fy_node_clear_meta(struct fy_node *fyn)
	FY_EXPORT;

/**
 * typedef fy_node_meta_clear_fn - Meta data clear method
 *
 * This is the callback called when meta data are cleared.
 *
 * @fyn: The node which the meta data is being cleared
 * @meta: The meta data of the node assigned via fy_node_set_meta()
 * @user: The user pointer of fy_document_register_meta()
 *
 */
typedef void (*fy_node_meta_clear_fn)(struct fy_node *fyn, void *meta, void *user);

/**
 * fy_document_register_meta() - Register a meta cleanup hook
 *
 * Register a meta data cleanup hook, to be called when
 * the node is freed via a final call to fy_node_free().
 * The hook is active for all nodes belonging to the document.
 *
 * @fyd: The document which the hook is registered to
 * @clear_fn: The clear hook method
 * @user: Opaque user provided pointer to the clear method
 *
 * Returns:
 * 0 on success, -1 if another hook is already registered.
 */
int
fy_document_register_meta(struct fy_document *fyd,
			  fy_node_meta_clear_fn clear_fn, void *user)
	FY_EXPORT;

/**
 * fy_document_unregister_meta() - Unregister a meta cleanup hook
 *
 * Unregister the currently active meta cleanup hook.
 * The previous cleanup hook will be called for every node in
 * the document.
 *
 * @fyd: The document to unregister it's meta cleanup hook.
 */
void
fy_document_unregister_meta(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_get_userdata() - Get the userdata pointer of a document
 *
 * Return the userdata pointer of a document.
 *
 * @fyn: The document to get userdata from
 *
 * Returns:
 * The stored userdata pointer
 */
void *
fy_document_get_userdata(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_set_userdata() - Set the userdata pointer of a document
 *
 * Set the userdata pointer of a document. If @userdata is NULL
 * then clear the userdata.
 *
 * @fyd: The document to set userdata
 * @userdata: The userdata pointer
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_set_userdata(struct fy_document *fyd, void *userdata)
	FY_EXPORT;

/**
 * typedef fy_document_on_destroy_fn - Userdata clear method
 *
 * This is the callback called just before document is destroyed.
 *
 * @fyd: The document which will be destroyed
 * @userdata: The userdata pointer of a document
 *
 */
typedef void (*fy_document_on_destroy_fn)(struct fy_document *fyd, void *userdata);

/**
 * fy_document_register_on_destroy() - Register an on_destroy hook
 *
 * Register an on_destroy hook, to be called when
 * the document is freed via a final call to fy_document_destroy().
 *
 * @fyd: The document which the hook is registered to
 * @on_destroy_fn: The on_destroy hook method
 *
 * Returns:
 * 0 on success, -1 if another hook is already registered.
 */
int
fy_document_register_on_destroy(struct fy_document *fyd,
				fy_document_on_destroy_fn on_destroy_fn)
	FY_EXPORT;

/**
 * fy_document_unregister_on_destroy() - Unregister an on_destroy hook
 *
 * Unregister the currently active on_destroy hook.
 *
 * @fyd: The document to unregister it's on_destroy hook.
 */
void
fy_document_unregister_on_destroy(struct fy_document *fyd)
	FY_EXPORT;


/**
 * fy_node_set_marker() - Set a marker of a node
 *
 * Sets the marker of the given node, while returning
 * the previous state of the marker. Note that the
 * markers use the same space as the node follow markers.
 *
 * @fyn: The node
 * @marker: The marker to set
 *
 * Returns:
 * The previous value of the marker
 */
bool
fy_node_set_marker(struct fy_node *fyn, unsigned int marker)
	FY_EXPORT;

/**
 * fy_node_clear_marker() - Clear a marker of a node
 *
 * Clears the marker of the given node, while returning
 * the previous state of the marker. Note that the
 * markers use the same space as the node follow markers.
 *
 * @fyn: The node
 * @marker: The marker to clear
 *
 * Returns:
 * The previous value of the marker
 */
bool
fy_node_clear_marker(struct fy_node *fyn, unsigned int marker)
	FY_EXPORT;

/**
 * fy_node_is_marker_set() - Checks whether a marker is set
 *
 * Check the state of the given marker.
 *
 * @fyn: The node
 * @marker: The marker index (must be less that FYNWF_MAX_USER_MARKER)
 *
 * Returns:
 * The value of the marker (invalid markers return false)
 */
bool
fy_node_is_marker_set(struct fy_node *fyn, unsigned int marker)
	FY_EXPORT;

/**
 * fy_node_vreport() - Report about a node vprintf style
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document.
 *
 * @fyn: The node
 * @type: The error type
 * @fmt: The printf format string
 * @ap: The argument list
 */
void
fy_node_vreport(struct fy_node *fyn, enum fy_error_type type,
		const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_node_report() - Report about a node printf style
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document.
 *
 * @fyn: The node
 * @type: The error type
 * @fmt: The printf format string
 * @...: The extra arguments.
 */
void
fy_node_report(struct fy_node *fyn, enum fy_error_type type,
	       const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 3, 4))
	FY_EXPORT;

/**
 * fy_node_override_vreport() - Report about a node vprintf style,
 * 				overriding file, line and column info
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document. This method will use the overrides provided in order
 * to massage the reporting information.
 * If @file is NULL, no file location will be reported.
 * If either @line or @column is negative no location will be reported.
 *
 * @fyn: The node
 * @type: The error type
 * @file: The file override
 * @line: The line override
 * @column: The column override
 * @fmt: The printf format string
 * @ap: The argument list
 */
void
fy_node_override_vreport(struct fy_node *fyn, enum fy_error_type type,
			 const char *file, int line, int column,
			 const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_node_override_report() - Report about a node printf style,
 * 				overriding file, line and column info
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document. This method will use the overrides provided in order
 * to massage the reporting information.
 * If @file is NULL, no file location will be reported.
 * If either @line or @column is negative no location will be reported.
 *
 * @fyn: The node
 * @type: The error type
 * @file: The file override
 * @line: The line override
 * @column: The column override
 * @fmt: The printf format string
 * @...: The extra arguments.
 */
void
fy_node_override_report(struct fy_node *fyn, enum fy_error_type type,
			const char *file, int line, int column,
			const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 6, 7))
	FY_EXPORT;

typedef void (*fy_diag_output_fn)(struct fy_diag *diag, void *user,
				  const char *buf, size_t len);

/**
 * struct fy_diag_cfg - The diagnostics configuration
 *
 * @fp: File descriptor of the error output
 * @output_fn: Callback to use when fp is NULL
 * @user: User pointer to pass to the output_fn
 * @level: The minimum debugging level
 * @module_mask: A bitmask of the enabled modules
 * @colorize: true if output should be colorized using ANSI sequences
 * @show_source: true if source location should be displayed
 * @show_position: true if position should be displayed
 * @show_type: true if the type should be displayed
 * @show_module: true if the module should be displayed
 * @source_width: Width of the source field
 * @position_width: Width of the position field
 * @type_width: Width of the type field
 * @module_width: Width of the module field
 *
 * This structure contains the configuration of the
 * diagnostic object.
 */
struct fy_diag_cfg {
	FILE *fp;
	fy_diag_output_fn output_fn;
	void *user;
	enum fy_error_type level;
	unsigned int module_mask;
	bool colorize : 1;
	bool show_source : 1;
	bool show_position : 1;
	bool show_type : 1;
	bool show_module : 1;
	int source_width;
	int position_width;
	int type_width;
	int module_width;
};

/**
 * struct fy_diag_error - A collected diagnostic error
 *
 * @type: Error type
 * @module: The module
 * @fyt: The token (may be NULL)
 * @msg: The message to be output alongside
 * @file: The file which contained the input
 * @line: The line at the error
 * @column: The column at the error
 *
 * This structure contains information about an error
 * being collected by the diagnostic object.
 */
struct fy_diag_error {
	enum fy_error_type type;
	enum fy_error_module module;
	struct fy_token *fyt;
	const char *msg;
	const char *file;
	int line;
	int column;
};

/**
 * fy_diag_create() - Create a diagnostic object
 *
 * Creates a diagnostic object using the provided configuration.
 *
 * @cfg: The configuration for the diagnostic object (NULL is default)
 *
 * Returns:
 * A pointer to the diagnostic object or NULL in case of an error.
 */
struct fy_diag *
fy_diag_create(const struct fy_diag_cfg *cfg)
	FY_EXPORT;

/**
 * fy_diag_destroy() - Destroy a diagnostic object
 *
 * Destroy a diagnostic object; note that the actual
 * destruction only occurs when no more references to the
 * object are present. However no output will be generated
 * after this call.
 *
 * @diag: The diagnostic object to destroy
 */
void
fy_diag_destroy(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_get_cfg() - Get a diagnostic object's configuration
 *
 * Return the current configuration of a diagnostic object
 *
 * @diag: The diagnostic object
 *
 * Returns:
 * A const pointer to the diagnostic object configuration, or NULL
 * in case where diag is NULL
 */
const struct fy_diag_cfg *
fy_diag_get_cfg(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_set_cfg() - Set a diagnostic object's configuration
 *
 * Replace the current diagnostic configuration with the given
 * configuration passed as an argument.
 *
 * @diag: The diagnostic object
 * @cfg: The diagnostic configuration
 */
void
fy_diag_set_cfg(struct fy_diag *diag, const struct fy_diag_cfg *cfg)
	FY_EXPORT;

/**
 * fy_diag_set_level() - Set a diagnostic object's debug error level
 *
 * @diag: The diagnostic object
 * @level: The debugging level to set
 */
void
fy_diag_set_level(struct fy_diag *diag, enum fy_error_type level);

/**
 * fy_diag_set_colorize() - Set a diagnostic object's colorize option
 *
 * @diag: The diagnostic object
 * @colorize: The colorize option
 */
void
fy_diag_set_colorize(struct fy_diag *diag, bool colorize);

/**
 * fy_diag_ref() - Increment that reference counter of a diagnostic object
 *
 * Increment the reference.
 *
 * @diag: The diagnostic object to reference
 *
 * Returns:
 * Always returns the @diag argument
 */
struct fy_diag *
fy_diag_ref(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_unref() - Take away a ref from a diagnostic object
 *
 * Take away a reference, if it gets to 0, the diagnostic object
 * is freed.
 *
 * @diag: The diagnostic object to unref
 */
void
fy_diag_unref(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_got_error() - Test whether an error level diagnostic
 *                       has been produced
 *
 * Tests whether an error diagnostic has been produced.
 *
 * @diag: The diagnostic object
 *
 * Returns:
 * true if an error has been produced, false otherwise
 */
bool
fy_diag_got_error(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_reset_error() - Reset the error flag of
 * 			   the diagnostic object
 *
 * Clears the error flag which was set by an output
 * of an error level diagnostic
 *
 * @diag: The diagnostic object
 */
void
fy_diag_reset_error(struct fy_diag *diag)
	FY_EXPORT;

/**
 * fy_diag_set_collect_errors() - Collect errors instead of outputting
 *
 * Set the collect errors mode. When true instead of outputting to
 * the diagnostic interface, errors are collected for later
 * retrieval.
 *
 * @diag: The diagnostic object
 * @collect_errors: The collect errors mode
 */
void
fy_diag_set_collect_errors(struct fy_diag *diag, bool collect_errors)
	FY_EXPORT;

/**
 * fy_diag_cfg_default() - Fill in the configuration structure
 * 			   with defaults
 *
 * Fills the configuration structure with defaults. The default
 * always associates the file descriptor to stderr.
 *
 * @cfg: The diagnostic configuration structure
 */
void
fy_diag_cfg_default(struct fy_diag_cfg *cfg)
	FY_EXPORT;

/**
 * fy_diag_cfg_from_parser_flags() - Fill partial diagnostic config
 * 				     structure from parser config flags
 *
 * Fills in part of the configuration structure using parser flags.
 * Since the parser flags do not contain debugging flags anymore this
 * method is deprecated.
 *
 * @cfg: The diagnostic configuration structure
 * @pflags: The parser flags
 */
void
fy_diag_cfg_from_parser_flags(struct fy_diag_cfg *cfg,
			      enum fy_parse_cfg_flags pflags)
	FY_EXPORT FY_DEPRECATED;

/**
 * fy_diag_vprintf() - vprintf raw interface to diagnostics
 *
 * Raw output to the diagnostic object using a standard
 * vprintf interface. Note that this is the lowest level
 * interface, and as such is not recommended for use, since
 * no formatting or coloring will take place.
 *
 * @diag: The diagnostic object to use
 * @fmt: The vprintf format string
 * @ap: The arguments
 *
 * Returns:
 * The number of characters output, or -1 in case of an error
 * Note that 0 shall be returned if the diagnostic object has
 * been destroyed but not yet freed.
 */
int
fy_diag_vprintf(struct fy_diag *diag, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_diag_printf() - printf raw interface to diagnostics
 *
 * Raw output to the diagnostic object using a standard
 * printf interface. Note that this is the lowest level
 * interface, and as such is not recommended for use, since
 * no formatting or coloring will take place.
 *
 * @diag: The diagnostic object to use
 * @fmt: The printf format string
 * @...: The arguments
 *
 * Returns:
 * The number of characters output, or -1 in case of an error
 * Note that 0 shall be returned if the diagnostic object has
 * been destroyed but not yet freed.
 */
int
fy_diag_printf(struct fy_diag *diag, const char *fmt, ...)
	FY_EXPORT
	FY_ATTRIBUTE(format(printf, 2, 3));

/**
 * struct fy_diag_ctx - The diagnostics context
 *
 * @level: The level of the diagnostic
 * @module: The module of the diagnostic
 * @source_func: The source function
 * @source_file: The source file
 * @source_line: The source line
 * @file: The file that caused the error
 * @line: The line where the diagnostic occured
 * @column: The column where the diagnostic occured
 *
 * This structure contains the diagnostic context
 */
struct fy_diag_ctx {
	enum fy_error_type level;
	enum fy_error_module module;
	const char *source_func;
	const char *source_file;
	int source_line;
	const char *file;
	int line;
	int column;
};

/**
 * fy_vdiag() - context aware diagnostic output like vprintf
 *
 * Context aware output to the diagnostic object using a standard
 * vprintf interface.
 *
 * @diag: The diagnostic object to use
 * @fydc: The diagnostic context
 * @fmt: The vprintf format string
 * @ap: The arguments
 *
 * Returns:
 * The number of characters output, or -1 in case of an error
 * Note that 0 shall be returned if the diagnostic object has
 * been destroyed but not yet freed.
 */
int
fy_vdiag(struct fy_diag *diag, const struct fy_diag_ctx *fydc,
	 const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_diagf() - context aware diagnostic output like printf
 *
 * Context aware output to the diagnostic object using a standard
 * printf interface.
 *
 * @diag: The diagnostic object to use
 * @fydc: The diagnostic context
 * @fmt: The vprintf format string
 *
 * Returns:
 * The number of characters output, or -1 in case of an error
 * Note that 0 shall be returned if the diagnostic object has
 * been destroyed but not yet freed.
 */
int
fy_diagf(struct fy_diag *diag, const struct fy_diag_ctx *fydc,
	 const char *fmt, ...)
	FY_EXPORT
	FY_ATTRIBUTE(format(printf, 3, 4));

int fy_diag_diag(struct fy_diag *diag, enum fy_error_type level, const char* fmt, ...);

#ifndef NDEBUG

#define fy_debug(_diag, _fmt, ...) \
	fy_diag_diag((_diag), FYET_DEBUG, (_fmt) , ## __VA_ARGS__)
#else

#define fy_debug(_diag, _fmt, ...) \
	do { } while(0)

#endif

#define fy_info(_diag, _fmt, ...) \
	fy_diag_diag((_diag), FYET_INFO, (_fmt) , ## __VA_ARGS__)
#define fy_notice(_diag, _fmt, ...) \
	fy_diag_diag((_diag), FYET_NOTICE, (_fmt) , ## __VA_ARGS__)
#define fy_warning(_diag, _fmt, ...) \
	fy_diag_diag((_diag), FYET_WARNING, (_fmt) , ## __VA_ARGS__)
#define fy_error(_diag, _fmt, ...) \
	fy_diag_diag((_diag), FYET_ERROR, (_fmt) , ## __VA_ARGS__)

/**
 * fy_diag_node_vreport() - Report about a node vprintf style using
 *                          the given diagnostic object
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document.
 *
 * @diag: The diag object
 * @fyn: The node
 * @type: The error type
 * @fmt: The printf format string
 * @ap: The argument list
 */
void
fy_diag_node_vreport(struct fy_diag *diag, struct fy_node *fyn,
		     enum fy_error_type type, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_diag_node_report() - Report about a node printf style using
 *                          the given diagnostic object
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document.
 *
 * @diag: The diag object
 * @fyn: The node
 * @type: The error type
 * @fmt: The printf format string
 * @...: The extra arguments.
 */
void
fy_diag_node_report(struct fy_diag *diag, struct fy_node *fyn,
		    enum fy_error_type type, const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 4, 5))
	FY_EXPORT;

/**
 * fy_diag_node_override_vreport() - Report about a node vprintf style,
 * 				     overriding file, line and column info using
 * 				     the given diagnostic object
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document. This method will use the overrides provided in order
 * to massage the reporting information.
 * If @file is NULL, no file location will be reported.
 * If either @line or @column is negative no location will be reported.
 *
 * @diag: The diag object
 * @fyn: The node
 * @type: The error type
 * @file: The file override
 * @line: The line override
 * @column: The column override
 * @fmt: The printf format string
 * @ap: The argument list
 */
void
fy_diag_node_override_vreport(struct fy_diag *diag, struct fy_node *fyn,
			      enum fy_error_type type, const char *file,
			      int line, int column, const char *fmt, va_list ap)
	FY_EXPORT;

/**
 * fy_diag_node_override_report() - Report about a node printf style,
 * 				    overriding file, line and column info using
 * 				    the given diagnostic object
 *
 * Output a report about the given node via the specific
 * error type, and using the reporting configuration of the node's
 * document. This method will use the overrides provided in order
 * to massage the reporting information.
 * If @file is NULL, no file location will be reported.
 * If either @line or @column is negative no location will be reported.
 *
 * @diag: The diag object
 * @fyn: The node
 * @type: The error type
 * @file: The file override
 * @line: The line override
 * @column: The column override
 * @fmt: The printf format string
 * @...: The extra arguments.
 */
void
fy_diag_node_override_report(struct fy_diag *diag, struct fy_node *fyn,
			     enum fy_error_type type, const char *file,
			     int line, int column, const char *fmt, ...)
	FY_ATTRIBUTE(format(printf, 7, 8))
	FY_EXPORT;

/**
 * fy_diag_errors_iterate() - Iterate over the errors of a diagnostic object
 *
 * This method iterates over all the errors collected on the diagnostic object.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @diag: The diagnostic object
 * @prevp: The previous result iterator
 *
 * Returns:
 * The next errors or NULL when there are not any more.
 */
struct fy_diag_error *
fy_diag_errors_iterate(struct fy_diag *diag, void **prevp)
	FY_EXPORT;

/**
 * enum fy_path_parse_cfg_flags - Path parse configuration flags
 *
 * These flags control the operation of the path parse
 *
 * @FYPPCF_QUIET: Quiet, do not output any information messages
 * @FYPPCF_DISABLE_RECYCLING: Disable recycling optimization
 * @FYPPCF_DISABLE_ACCELERATORS: Disable use of access accelerators (saves memory)
 */
enum fy_path_parse_cfg_flags {
	FYPPCF_QUIET			= FY_BIT(0),
	FYPPCF_DISABLE_RECYCLING	= FY_BIT(1),
	FYPPCF_DISABLE_ACCELERATORS	= FY_BIT(2),
};

/**
 * struct fy_path_parse_cfg - path parser configuration structure.
 *
 * Argument to the fy_path_parser_create() method which
 * performs parsing of a ypath expression
 *
 * @flags: Configuration flags
 * @userdata: Opaque user data pointer
 * @diag: Optional diagnostic interface to use
 */
struct fy_path_parse_cfg {
	enum fy_path_parse_cfg_flags flags;
	void *userdata;
	struct fy_diag *diag;
};

/**
 * fy_path_parser_create() - Create a ypath parser.
 *
 * Creates a path parser with its configuration @cfg
 * The path parser may be destroyed by a corresponding call to
 * fy_path_parser_destroy().
 *
 * @cfg: The configuration for the path parser
 *
 * Returns:
 * A pointer to the path parser or NULL in case of an error.
 */
struct fy_path_parser *
fy_path_parser_create(const struct fy_path_parse_cfg *cfg)
	FY_EXPORT;

/**
 * fy_path_parser_destroy() - Destroy the given path parser
 *
 * Destroy a path parser created earlier via fy_path_parser_create().
 *
 * @fypp: The path parser to destroy
 */
void
fy_path_parser_destroy(struct fy_path_parser *fypp)
	FY_EXPORT;

/**
 * fy_path_parser_reset() - Reset a path parser completely
 *
 * Completely reset a path parser, including after an error
 * that caused a parser error to be emitted.
 *
 * @fypp: The path parser to reset
 *
 * Returns:
 * 0 if the reset was successful, -1 otherwise
 */
int
fy_path_parser_reset(struct fy_path_parser *fypp)
	FY_EXPORT;

/**
 * fy_path_parse_expr_from_string() - Parse an expression from a given string
 *
 * Create a path expression from a string using the provided path parser.
 *
 * @fypp: The path parser to use
 * @str: The ypath source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created path expression or NULL on error.
 */
struct fy_path_expr *
fy_path_parse_expr_from_string(struct fy_path_parser *fypp,
			       const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_path_expr_build_from_string() - Parse an expression from a given string
 *
 * Create a path expression from a string using the provided path parser
 * configuration.
 *
 * @pcfg: The path parser configuration to use, or NULL for default
 * @str: The ypath source to use.
 * @len: The length of the string (or -1 if '\0' terminated)
 *
 * Returns:
 * The created path expression or NULL on error.
 */
struct fy_path_expr *
fy_path_expr_build_from_string(const struct fy_path_parse_cfg *pcfg,
			       const char *str, size_t len)
	FY_EXPORT;

/**
 * fy_path_expr_free() - Free a path expression
 *
 * Free a previously returned expression from any of the path parser
 * methods like fy_path_expr_build_from_string()
 *
 * @expr: The expression to free (may be NULL)
 */
void
fy_path_expr_free(struct fy_path_expr *expr)
	FY_EXPORT;

/**
 * fy_path_expr_dump() - Dump the contents of a path expression to
 * 			 the diagnostic object
 *
 * Dumps the expression using the provided error level.
 *
 * @expr: The expression to dump
 * @diag: The diagnostic object to use
 * @errlevel: The error level which the diagnostic will use
 * @level: The nest level; should be set to 0
 * @banner: The banner to display on level 0
 */
void
fy_path_expr_dump(struct fy_path_expr *expr, struct fy_diag *diag,
		  enum fy_error_type errlevel, int level, const char *banner)
	FY_EXPORT;

/**
 * fy_path_expr_to_document() - Converts the path expression to a YAML document
 *
 * Converts the expression to a YAML document which is useful for
 * understanding what the expression evaluates to.
 *
 * @expr: The expression to convert to a document
 *
 * Returns:
 * The document of the expression or NULL on error.
 */
struct fy_document *
fy_path_expr_to_document(struct fy_path_expr *expr)
	FY_EXPORT;

/**
 * enum fy_path_exec_cfg_flags - Path executor configuration flags
 *
 * These flags control the operation of the path expression executor
 *
 * @FYPXCF_QUIET: Quiet, do not output any information messages
 * @FYPXCF_DISABLE_RECYCLING: Disable recycling optimization
 * @FYPXCF_DISABLE_ACCELERATORS: Disable use of access accelerators (saves memory)
 */
enum fy_path_exec_cfg_flags {
	FYPXCF_QUIET			= FY_BIT(0),
	FYPXCF_DISABLE_RECYCLING	= FY_BIT(1),
	FYPXCF_DISABLE_ACCELERATORS	= FY_BIT(2),
};

/**
 * struct fy_path_exec_cfg - path expression executor configuration structure.
 *
 * Argument to the fy_path_exec_create() method which
 * performs execution of a ypath expression
 *
 * @flags: Configuration flags
 * @userdata: Opaque user data pointer
 * @diag: Optional diagnostic interface to use
 */
struct fy_path_exec_cfg {
	enum fy_path_exec_cfg_flags flags;
	void *userdata;
	struct fy_diag *diag;
};

/**
 * fy_path_exec_create() - Create a ypath expression executor.
 *
 * Creates a ypath expression parser with its configuration @cfg
 * The executor may be destroyed by a corresponding call to
 * fy_path_exec_destroy().
 *
 * @xcfg: The configuration for the executor
 *
 * Returns:
 * A pointer to the executor or NULL in case of an error.
 */
struct fy_path_exec *
fy_path_exec_create(const struct fy_path_exec_cfg *xcfg)
	FY_EXPORT;

/**
 * fy_path_exec_destroy() - Destroy the given path expression executor
 *
 * Destroy ane executor  created earlier via fy_path_exec_create().
 *
 * @fypx: The path parser to destroy
 */
void
fy_path_exec_destroy(struct fy_path_exec *fypx)
	FY_EXPORT;

/**
 * fy_path_exec_reset() - Reset an executor
 *
 * Completely reset an executor without releasing it.
 *
 * @fypx: The executor to reset
 *
 * Returns:
 * 0 if the reset was successful, -1 otherwise
 */
int
fy_path_exec_reset(struct fy_path_exec *fypx)
	FY_EXPORT;

/**
 * fy_path_exec_execute() - Execute a path expression starting at
 *                          the given start node
 *
 * Execute the expression starting at fyn_start. If execution
 * is successful the results are available via fy_path_exec_results_iterate()
 *
 * Note that it is illegal to modify the state of the document that the
 * results reside between this call and the results collection.
 *
 * @fypx: The executor to use
 * @expr: The expression to use
 * @fyn_start: The node on which the expression will begin.
 *
 * Returns:
 * 0 if the execution was successful, -1 otherwise
 *
 * Note that the execution may be successful but no results were
 * produced, in which case the iterator will return NULL.
 */
int
fy_path_exec_execute(struct fy_path_exec *fypx, struct fy_path_expr *expr,
		     struct fy_node *fyn_start)
	FY_EXPORT;

/**
 * fy_path_exec_results_iterate() - Iterate over the results of execution
 *
 * This method iterates over all the results in the executor.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fypx: The executor
 * @prevp: The previous result iterator
 *
 * Returns:
 * The next node in the result set or NULL at the end of the results.
 */
struct fy_node *
fy_path_exec_results_iterate(struct fy_path_exec *fypx, void **prevp)
	FY_EXPORT;

/*
 * Helper methods for binding implementers
 * Note that users of the library do not need to know these details.
 * However bindings that were developed against libyaml expect these
 * to be exported, so provide a shim here
 */

/**
 * enum fy_token_type - Token types
 *
 * The available token types that the tokenizer produces.
 *
 * @FYTT_NONE: No token
 * @FYTT_STREAM_START: Stream start
 * @FYTT_STREAM_END: Stream end
 * @FYTT_VERSION_DIRECTIVE: Version directive
 * @FYTT_TAG_DIRECTIVE: Tag directive
 * @FYTT_DOCUMENT_START: Document start
 * @FYTT_DOCUMENT_END: Document end
 * @FYTT_BLOCK_SEQUENCE_START: Start of a block sequence
 * @FYTT_BLOCK_MAPPING_START: Start of a block mapping
 * @FYTT_BLOCK_END: End of a block mapping or a sequence
 * @FYTT_FLOW_SEQUENCE_START: Start of a flow sequence
 * @FYTT_FLOW_SEQUENCE_END: End of a flow sequence
 * @FYTT_FLOW_MAPPING_START: Start of a flow mapping
 * @FYTT_FLOW_MAPPING_END: End of a flow mapping
 * @FYTT_BLOCK_ENTRY: A block entry
 * @FYTT_FLOW_ENTRY: A flow entry
 * @FYTT_KEY: A key of a mapping
 * @FYTT_VALUE: A value of a mapping
 * @FYTT_ALIAS: An alias
 * @FYTT_ANCHOR: An anchor
 * @FYTT_TAG: A tag
 * @FYTT_SCALAR: A scalar
 * @FYTT_INPUT_MARKER: Internal input marker token
 * @FYTT_PE_SLASH: A slash
 * @FYTT_PE_ROOT: A root
 * @FYTT_PE_THIS: A this
 * @FYTT_PE_PARENT: A parent
 * @FYTT_PE_MAP_KEY: A map key
 * @FYTT_PE_SEQ_INDEX: A sequence index
 * @FYTT_PE_SEQ_SLICE: A sequence slice
 * @FYTT_PE_SCALAR_FILTER: A scalar filter
 * @FYTT_PE_COLLECTION_FILTER: A collection filter
 * @FYTT_PE_SEQ_FILTER: A sequence filter
 * @FYTT_PE_MAP_FILTER: A mapping filter
 * @FYTT_PE_UNIQUE_FILTER: Filters out duplicates
 * @FYTT_PE_EVERY_CHILD: Every child
 * @FYTT_PE_EVERY_CHILD_R: Every child recursive
 * @FYTT_PE_ALIAS: An alias
 * @FYTT_PE_SIBLING: A sibling marker
 * @FYTT_PE_COMMA: A comma
 * @FYTT_PE_BARBAR: A ||
 * @FYTT_PE_AMPAMP: A &&
 * @FYTT_PE_LPAREN: A left parenthesis
 * @FYTT_PE_RPAREN: A right parenthesis
 * @FYTT_PE_EQEQ: Equality operator
 * @FYTT_PE_NOTEQ: Non-equality operator
 * @FYTT_PE_LT: Less than operator
 * @FYTT_PE_GT: Greater than operator
 * @FYTT_PE_LTE: Less or equal than operator
 * @FYTT_PE_GTE: Greater or equal than operator
 * @FYTT_SE_PLUS: Plus operator
 * @FYTT_SE_MINUS: Minus operator
 * @FYTT_SE_MULT: Multiply operator
 * @FYTT_SE_DIV: Divide operator
 * @FYTT_PE_METHOD: Path expression method (chained)
 * @FYTT_SE_METHOD: Scalar expression method (non chained)
 */
enum fy_token_type {
	/* non-content token types */
	FYTT_NONE,
	FYTT_STREAM_START,
	FYTT_STREAM_END,
	FYTT_VERSION_DIRECTIVE,
	FYTT_TAG_DIRECTIVE,
	FYTT_DOCUMENT_START,
	FYTT_DOCUMENT_END,
	/* content token types */
	FYTT_BLOCK_SEQUENCE_START,
	FYTT_BLOCK_MAPPING_START,
	FYTT_BLOCK_END,
	FYTT_FLOW_SEQUENCE_START,
	FYTT_FLOW_SEQUENCE_END,
	FYTT_FLOW_MAPPING_START,
	FYTT_FLOW_MAPPING_END,
	FYTT_BLOCK_ENTRY,
	FYTT_FLOW_ENTRY,
	FYTT_KEY,
	FYTT_VALUE,
	FYTT_ALIAS,
	FYTT_ANCHOR,
	FYTT_TAG,
	FYTT_SCALAR,

	/* special error reporting */
	FYTT_INPUT_MARKER,

	/* path expression tokens */
	FYTT_PE_SLASH,
	FYTT_PE_ROOT,
	FYTT_PE_THIS,
	FYTT_PE_PARENT,
	FYTT_PE_MAP_KEY,
	FYTT_PE_SEQ_INDEX,
	FYTT_PE_SEQ_SLICE,
	FYTT_PE_SCALAR_FILTER,
	FYTT_PE_COLLECTION_FILTER,
	FYTT_PE_SEQ_FILTER,
	FYTT_PE_MAP_FILTER,
	FYTT_PE_UNIQUE_FILTER,
	FYTT_PE_EVERY_CHILD,
	FYTT_PE_EVERY_CHILD_R,
	FYTT_PE_ALIAS,
	FYTT_PE_SIBLING,
	FYTT_PE_COMMA,
	FYTT_PE_BARBAR,
	FYTT_PE_AMPAMP,
	FYTT_PE_LPAREN,
	FYTT_PE_RPAREN,

	/* comparison operators */
	FYTT_PE_EQEQ,		/* == */
	FYTT_PE_NOTEQ,		/* != */
	FYTT_PE_LT,		/* <  */
	FYTT_PE_GT,		/* >  */
	FYTT_PE_LTE,		/* <= */
	FYTT_PE_GTE,		/* >= */

	/* scalar expression tokens */
	FYTT_SE_PLUS,
	FYTT_SE_MINUS,
	FYTT_SE_MULT,
	FYTT_SE_DIV,

	FYTT_PE_METHOD,		/* path expr method (chained) */
	FYTT_SE_METHOD,		/* scalar expr method (non chained) */
};

/* The number of token types available */
#define FYTT_COUNT	(FYTT_SE_METHOD+1)

/**
 * fy_token_type_is_valid() - Check token type validity
 *
 * Check if argument token type is a valid one.
 *
 * @type: The token type
 *
 * Returns:
 * true if the token type is valid, false otherwise
 */
static inline bool
fy_token_type_is_valid(enum fy_token_type type)
{
	return type >= FYTT_NONE && type < FYTT_COUNT;
}

/**
 * fy_token_type_is_yaml() - Check if token type is valid for YAML
 *
 * Check if argument token type is a valid YAML one.
 *
 * @type: The token type
 *
 * Returns:
 * true if the token type is a valid YAML one, false otherwise
 */
static inline bool
fy_token_type_is_yaml(enum fy_token_type type)
{
	return type >= FYTT_STREAM_START && type <= FYTT_SCALAR;
}

/**
 * fy_token_type_is_content() - Check if token type is
 *                              valid for YAML content
 *
 * Check if argument token type is a valid YAML content one.
 *
 * @type: The token type
 *
 * Returns:
 * true if the token type is a valid YAML content one, false otherwise
 */
static inline bool
fy_token_type_is_content(enum fy_token_type type)
{
	return type >= FYTT_BLOCK_SEQUENCE_START && type <= FYTT_SCALAR;
}

/**
 * fy_token_type_is_path_expr() - Check if token type is
 *                                valid for a YPATH expression
 *
 * Check if argument token type is a valid YPATH parse expression token
 *
 * @type: The token type
 *
 * Returns:
 * true if the token type is a valid YPATH one, false otherwise
 */
static inline bool
fy_token_type_is_path_expr(enum fy_token_type type)
{
	return type >= FYTT_PE_SLASH && type <= FYTT_PE_GTE;
}

/**
 * fy_token_type_is_scalar_expr() - Check if token type is
 *                                  valid for a YPATH scalar expression
 *
 * Check if argument token type is a valid YPATH parse scalar expression token
 *
 * @type: The token type
 *
 * Returns:
 * true if the token type is a valid YPATH scalar one, false otherwise
 */
static inline bool
fy_token_type_is_scalar_expr(enum fy_token_type type)
{
	return type >= FYTT_SE_PLUS && type <= FYTT_SE_DIV;
}

/**
 * fy_token_get_type() - Return the token's type
 *
 * Return the token's type; if NULL then FYTT_NONE is returned
 *
 * @fyt: The token
 *
 * Returns:
 * The token's type; FYTT_NONE if not a valid token (or NULL)
 */
enum fy_token_type
fy_token_get_type(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_start_mark() - Get token's start marker
 *
 * Return the token's start marker if it exists. Note
 * it is permissable for some token types to have no
 * start marker because they are without content.
 *
 * @fyt: The token to get its start marker
 *
 * Returns:
 * The token's start marker, NULL if not available.
 */
const struct fy_mark *
fy_token_start_mark(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_token_end_mark() - Get token's end marker
 *
 * Return the token's end marker if it exists. Note
 * it is permissable for some token types to have no
 * end marker because they are without content.
 *
 * @fyt: The token to get its end marker
 *
 * Returns:
 * The token's end marker, NULL if not available.
 */
const struct fy_mark *
fy_token_end_mark(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_scan() - Low level access to the scanner
 *
 * Return the next scanner token. Note this is a very
 * low level interface, intended for users that want/need
 * to implement their own YAML parser. The returned
 * token is expected to be disposed using fy_scan_token_free()
 *
 * @fyp: The parser to get the next token from.
 *
 * Returns:
 * The next token, or NULL if no more tokens are available.
 */
struct fy_token *
fy_scan(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_scan_token_free() - Free the token returned by fy_scan()
 *
 * Free the token returned by fy_scan().
 *
 * @fyp: The parser of which the token was returned by fy_scan()
 * @fyt: The token to free
 */
void
fy_scan_token_free(struct fy_parser *fyp, struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_directive_token_prefix0() - Get the prefix contained in the
 * 				      tag directive token as zero terminated
 * 				      string
 *
 * Retrieve the tag directive's prefix contents as a zero terminated string.
 * It is similar to fy_tag_directive_token_prefix(), with the difference
 * that the returned string is zero terminated and memory may be allocated
 * to hold it associated with the token.
 *
 * @fyt: The tag directive token out of which the prefix pointer
 *       will be returned.
 *
 * Returns:
 * A pointer to the zero terminated text representation of the prefix token.
 * NULL in case of an error.
 */
const char *
fy_tag_directive_token_prefix0(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_directive_token_handle0() - Get the handle contained in the
 * 				      tag directive token as zero terminated
 * 				      string
 *
 * Retrieve the tag directive's handle contents as a zero terminated string.
 * It is similar to fy_tag_directive_token_handle(), with the difference
 * that the returned string is zero terminated and memory may be allocated
 * to hold it associated with the token.
 *
 * @fyt: The tag directive token out of which the handle pointer
 *       will be returned.
 *
 * Returns:
 * A pointer to the zero terminated text representation of the handle token.
 * NULL in case of an error.
 */
const char *
fy_tag_directive_token_handle0(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_token_handle() - Get the handle contained in the
 * 			   tag token
 *
 * Retrieve the tag handle contents. Will fail if
 * token is not a tag token, or if a memory error happens.
 *
 * @fyt: The tag token out of which the handle pointer
 *       will be returned.
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the text representation of the handle token, while
 * @lenp will be assigned the character length of said representation.
 * NULL in case of an error.
 */
const char *
fy_tag_token_handle(struct fy_token *fyt, size_t *lenp)
	FY_EXPORT;

/**
 * fy_tag_token_suffix() - Get the suffix contained in the
 * 			   tag token
 *
 * Retrieve the tag suffix contents. Will fail if
 * token is not a tag token, or if a memory error happens.
 *
 * @fyt: The tag token out of which the suffix pointer
 *       will be returned.
 * @lenp: Pointer to a variable that will hold the returned length
 *
 * Returns:
 * A pointer to the text representation of the suffix token, while
 * @lenp will be assigned the character length of said representation.
 * NULL in case of an error.
 */
const char *
fy_tag_token_suffix(struct fy_token *fyt, size_t *lenp)
	FY_EXPORT;

/**
 * fy_tag_token_handle0() - Get the handle contained in the
 * 			    tag token as zero terminated string
 *
 * Retrieve the tag handle contents as a zero terminated string.
 * It is similar to fy_tag_token_handle(), with the difference
 * that the returned string is zero terminated and memory may be allocated
 * to hold it associated with the token.
 *
 * @fyt: The tag token out of which the handle pointer will be returned.
 *
 * Returns:
 * A pointer to the zero terminated text representation of the handle token.
 * NULL in case of an error.
 */
const char *
fy_tag_token_handle0(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_token_suffix0() - Get the suffix contained in the
 * 			    tag token as zero terminated string
 *
 * Retrieve the tag suffix contents as a zero terminated string.
 * It is similar to fy_tag_token_suffix(), with the difference
 * that the returned string is zero terminated and memory may be allocated
 * to hold it associated with the token.
 *
 * @fyt: The tag token out of which the suffix pointer will be returned.
 *
 * Returns:
 * A pointer to the zero terminated text representation of the suffix token.
 * NULL in case of an error.
 */
const char *
fy_tag_token_suffix0(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_version_directive_token_version() - Return the version of a version
 * 					  directive token
 *
 * Retrieve the version stored in a version directive token.
 *
 * @fyt: The version directive token
 *
 * Returns:
 * A pointer to the version stored in the version directive token, or
 * NULL in case of an error, or the token not being a version directive token.
 */
const struct fy_version *
fy_version_directive_token_version(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_scalar_token_get_style() - Return the style of a scalar token
 *
 * Retrieve the style of a scalar token.
 *
 * @fyt: The scalar token
 *
 * Returns:
 * The scalar style of the token, or FYSS_ANY for an error
 */
enum fy_scalar_style
fy_scalar_token_get_style(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_token_tag() - Get tag of a tag token
 *
 * Retrieve the tag of a tag token.
 *
 * @fyt: The tag token
 *
 * Returns:
 * A pointer to the tag or NULL in case of an error
 */
const struct fy_tag *
fy_tag_token_tag(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_tag_directive_token_tag() - Get tag of a tag directive token
 *
 * Retrieve the tag of a tag directive token.
 *
 * @fyt: The tag directive token
 *
 * Returns:
 * A pointer to the tag or NULL in case of an error
 */
const struct fy_tag *
fy_tag_directive_token_tag(struct fy_token *fyt)
	FY_EXPORT;

/**
 * fy_event_get_token() - Return the main token of an event
 *
 * Retrieve the main token (i.e. not the tag or the anchor) of
 * an event. It may be NULL in case of an implicit event.
 *
 * @fye: The event to get its main token
 *
 * Returns:
 * The main token if it exists, NULL otherwise or in case of an error
 */
struct fy_token *
fy_event_get_token(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_event_get_anchor_token() - Return the anchor token of an event
 *
 * Retrieve the anchor token if it exists. Only valid for
 * mapping/sequence start and scalar events.
 *
 * @fye: The event to get its anchor token
 *
 * Returns:
 * The anchor token if it exists, NULL otherwise or in case of an error
 */
struct fy_token *
fy_event_get_anchor_token(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_event_get_tag_token() - Return the tag token of an event
 *
 * Retrieve the tag token if it exists. Only valid for
 * mapping/sequence start and scalar events.
 *
 * @fye: The event to get its tag token
 *
 * Returns:
 * The tag token if it exists, NULL otherwise or in case of an error
 */
struct fy_token *
fy_event_get_tag_token(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_event_start_mark() - Get event's start marker
 *
 * Return the event's start marker if it exists. The
 * start marker is the one of the event's main token.
 *
 * @fye: The event to get its start marker
 *
 * Returns:
 * The event's start marker, NULL if not available.
 */
const struct fy_mark *
fy_event_start_mark(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_event_end_mark() - Get event's end marker
 *
 * Return the event's end marker if it exists. The
 * end marker is the one of the event's main token.
 *
 * @fye: The event to get its end marker
 *
 * Returns:
 * The event's end marker, NULL if not available.
 */
const struct fy_mark *
fy_event_end_mark(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_event_get_node_style() - Get the node style of an event
 *
 * Return the node style (FYNS_*) of an event. May return
 * FYNS_ANY if the event is implicit.
 * For collection start events the only possible values is
 * FYNS_ANY, FYNS_FLOW, FYNS_BLOCK.
 * A scalar event may return any.
 *
 * @fye: The event to get it's node style
 *
 * Returns:
 * The event's end marker, NULL if not available.
 */
enum fy_node_style
fy_event_get_node_style(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_document_start_event_version() - Return the version of a document
 * 				       start event
 *
 * Retrieve the version stored in a document start event
 *
 * @fye: The document start event
 *
 * Returns:
 * A pointer to the version, or NULL in case of an error, or the event
 * not being a document start event.
 */
const struct fy_version *
fy_document_start_event_version(struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_document_state_version() - Return the version of a document state
 * 				 object
 *
 * Retrieve the version stored in a document state object
 *
 * @fyds: The document state object
 *
 * Returns:
 * A pointer to the version, or NULL in case of an error
 */
const struct fy_version *
fy_document_state_version(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_start_mark() - Get document state's start mark
 *
 * Return the document state's start mark (if it exists).
 * Note that purely synthetic documents do not contain one
 *
 * @fyds: The document state object
 *
 * Returns:
 * The document's start marker, NULL if not available.
 */
const struct fy_mark *
fy_document_state_start_mark(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_end_mark() - Get document state's end mark
 *
 * Return the document state's end mark (if it exists).
 * Note that purely synthetic documents do not contain one
 *
 * @fyds: The document state object
 *
 * Returns:
 * The document's end marker, NULL if not available.
 */
const struct fy_mark *
fy_document_state_end_mark(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_version_explicit() - Version explicit?
 *
 * Find out if a document state object's version was explicitly
 * set in the document.
 * Note that for synthetic documents this method returns false.
 *
 * @fyds: The document state object
 *
 * Returns:
 * true if version was set explicitly, false otherwise
 */
bool
fy_document_state_version_explicit(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_tags_explicit() - Tags explicit?
 *
 * Find out if a document state object's tags were explicitly
 * set in the document.
 * Note that for synthetic documents this method returns false.
 *
 * @fyds: The document state object
 *
 * Returns:
 * true if document had tags set explicitly, false otherwise
 */
bool
fy_document_state_tags_explicit(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_start_implicit() - Started implicitly?
 *
 * Find out if a document state object's document was
 * started implicitly.
 * Note that for synthetic documents this method returns false.
 *
 * @fyds: The document state object
 *
 * Returns:
 * true if document was started implicitly, false otherwise
 */
bool
fy_document_state_start_implicit(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_end_implicit() - Started implicitly?
 *
 * Find out if a document state object's document was
 * ended implicitly.
 * Note that for synthetic documents this method returns false.
 *
 * @fyds: The document state object
 *
 * Returns:
 * true if document was ended implicitly, false otherwise
 */
bool
fy_document_state_end_implicit(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_json_mode() - Input was JSON?
 *
 * Find out if a document state object's document was
 * created by a JSON input.
 * Note that for synthetic documents this method returns false.
 *
 * @fyds: The document state object
 *
 * Returns:
 * true if document was created in JSON mode, false otherwise
 */
bool
fy_document_state_json_mode(struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_state_tag_directive_iterate() - Iterate over the tag
 * 					       directives of a document state
 * 					       object
 *
 * This method iterates over all the tag directives nodes in the document state
 * object.
 * The start of the iteration is signalled by a NULL in \*prevp.
 *
 * @fyds: The document state
 * @prevp: The previous iterator
 *
 * Returns:
 * The next tag or NULL at the end of the iteration sequence.
 */
const struct fy_tag *
fy_document_state_tag_directive_iterate(struct fy_document_state *fyds, void **prevp)
	FY_EXPORT;

/**
 * fy_document_state_tag_is_default() - Test whether the given tag is a default one
 *
 * Test whether a tag is a default (i.e. impliciticly set)
 *
 * @fyds: The document state
 * @tag: The tag to check
 *
 * Returns:
 * true in case that the tag is a default one, false otherwise
 */
bool
fy_document_state_tag_is_default(struct fy_document_state *fyds, const struct fy_tag *tag)
	FY_EXPORT;

/**
 * fy_parser_get_document_state() - Get the document state of a parser object
 *
 * Retrieve the document state object of a parser. Note that this is only
 * valid during parsing.
 *
 * @fyp: The parser
 *
 * Returns:
 * The active document state object of the parser, NULL otherwise
 */
struct fy_document_state *
fy_parser_get_document_state(struct fy_parser *fyp)
	FY_EXPORT;

/**
 * fy_document_get_document_state() - Get the document state of a document
 *
 * Retrieve the document state object of a document.
 *
 * @fyd: The document
 *
 * Returns:
 * The document state object of the document, NULL otherwise
 */
struct fy_document_state *
fy_document_get_document_state(struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_set_document_state() - Set the document state of a document
 *
 * Set the document state of a document
 *
 * @fyd: The document
 * @fyds: The document state to use, or NULL for default
 *
 * Returns:
 * 0 if set operation was successful, -1 otherwise
 */
int
fy_document_set_document_state(struct fy_document *fyd, struct fy_document_state *fyds)
	FY_EXPORT;

/**
 * fy_document_create_from_event() - Create an empty document using the event
 *
 * Create an empty document using the FYET_DOCUMENT_START event generated
 * by the parser.
 *
 * @fyp: The parser
 * @fye: The event
 *
 * Returns:
 * The created empty document, or NULL on error.
 */
struct fy_document *
fy_document_create_from_event(struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_document_update_from_event() - Update the document with the event
 *
 * Update the document using the FYET_DOCUMENT_END event generated
 * by the parser.
 *
 * @fyd: The document
 * @fyp: The parser
 * @fye: The event
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_document_update_from_event(struct fy_document *fyd, struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_node_create_from_event() - Create a node using the event
 *
 * Create a new node using the supplied event.
 * Allowed events are FYET_SCALAR, FYET_ALIAS, FYET_MAPPING_START & FYET_SEQUENCE_START
 *
 * @fyd: The document
 * @fyp: The parser
 * @fye: The event
 *
 * Returns:
 * The newly created node, or NULL on error
 */
struct fy_node *
fy_node_create_from_event(struct fy_document *fyd, struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_node_update_from_event() - Update a node using the event
 *
 * Update information of node created using fy_node_create_from_event()
 * Allowed events are FYET_MAPPING_END & FYET_SEQUENCE_END
 *
 * @fyn: The node
 * @fyp: The parser
 * @fye: The event
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_update_from_event(struct fy_node *fyn, struct fy_parser *fyp, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_node_pair_create_with_key() - Create a new node pair and set it's key
 *
 * Create a new node pair using the supplied fyn_parent mapping and fyn node as
 * a key. Note that the nodes _must_ have been created by fy_node_create_from_event
 * and they are not interchangeable with other node pair methods.
 *
 * The node pair will be added to the fyn_parent mapping with a subsequent call
 * to fy_node_pair_update_with_value().
 *
 * @fyd: The document
 * @fyn_parent: The mapping
 * @fyn: The node pair key
 *
 * Returns:
 * The newly created node pair, or NULL on error
 */
struct fy_node_pair *
fy_node_pair_create_with_key(struct fy_document *fyd, struct fy_node *fyn_parent, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_pair_update_with_value() - Update a node pair with a value and add it to the parent mapping
 *
 * Update the node pair with the given value and add it to the parent mapping.
 * Note that the fyn node _must_ have been created by fy_node_create_from_event
 * and the node pair created by fy_node_pair_create_with_key().
 * Do not intermix other node pair manipulation methods.
 *
 * @fynp: The node pair
 * @fyn: The node pair value
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_pair_update_with_value(struct fy_node_pair *fynp, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_node_sequence_add_item() - Add an item node to a sequence node
 *
 * Add an item to the end of the sequence node fyn_parent.
 * Note that the fyn_parent and fyn nodes _must_ have been created by
 * fy_node_create_from_event.
 * Do not intermix other sequence node manipulation methods.
 *
 * @fyn_parent: The parent sequence node
 * @fyn: The node pair item
 *
 * Returns:
 * 0 on success, -1 on error
 */
int
fy_node_sequence_add_item(struct fy_node *fyn_parent, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_emitter_get_document_state() - Get the document state of an emitter  object
 *
 * Retrieve the document state object of an emitter. Note that this is only
 * valid during emitting.
 *
 * @emit: The emitter
 *
 * Returns:
 * The active document state object of the emitter, NULL otherwise
 */
struct fy_document_state *
fy_emitter_get_document_state(struct fy_emitter *emit)
	FY_EXPORT;

/**
 * fy_emit_event_create() - Create an emit event.
 *
 * Create an emit event to pass to fy_emit_event()
 * The extra arguments differ according to the event to be created
 *
 * FYET_STREAM_START:
 * 	- None
 *
 * FYET_STREAM_END:
 * 	- None
 *
 * FYET_DOCUMENT_START:
 * 	- int implicit
 * 		true if document start should be marked implicit
 * 		false if document start should not be marked implicit
 * 	- const struct fy_version \*vers
 * 		Pointer to version to use for the document, or NULL for default
 * 	- const struct fy_tag \* const \*tags
 * 		Pointer to a NULL terminated array of tag pointers (like argv)
 * 		NULL if no extra tags are to be used
 *
 * FYET_DOCUMENT_END:
 * 	- int implicit
 * 		true if document end should be marked implicit
 * 		false if document end should not be marked implicit
 *
 * FYET_MAPPING_START:
 * 	- enum fy_node_style style
 * 		Style of the mapping (one of FYNS_ANY, FYNS_BLOCK or FYNS_FLOW)
 *	- const char \*anchor
 *		Anchor to place at the mapping, or NULL for none
 *	- const char \*tag
 *		Tag to place at the mapping, or NULL for none
 *
 * FYET_MAPPING_END:
 * 	- None
 *
 * FYET_SEQUENCE_START:
 * 	- enum fy_node_style style
 * 		Style of the sequence (one of FYNS_ANY, FYNS_BLOCK or FYNS_FLOW)
 *	- const char \*anchor
 *		Anchor to place at the sequence, or NULL for none
 *	- const char \*tag
 *		Tag to place at the sequence, or NULL for none
 *
 * FYET_SEQUENCE_END:
 * 	- None
 *
 * FYET_SCALAR:
 * 	- enum fy_scalar_style style
 * 		Style of the scalar, any valid FYSS_* value
 * 		Note that certain styles may not be used according to the
 * 		contents of the data
 *	- const char \*value
 *		Pointer to the scalar contents.
 *	- size_t len
 *		Length of the scalar contents, of FY_NT (-1) for strlen(value)
 *	- const char \*anchor
 *		Anchor to place at the scalar, or NULL for none
 *	- const char \*tag
 *		Tag to place at the scalar, or NULL for none
 *
 * FYET_ALIAS:
 *	- const char \*value
 *		Pointer to the alias.
 *
 * @emit: The emitter
 * @type: The event type to create
 *
 * Returns:
 * The created event or NULL in case of an error
 */
struct fy_event *
fy_emit_event_create(struct fy_emitter *emit, enum fy_event_type type, ...)
	FY_EXPORT;

/**
 * fy_emit_event_vcreate() - Create an emit event using varargs.
 *
 * Create an emit event to pass to fy_emit_event()
 * The varargs analogous to fy_emit_event_create().
 *
 * @emit: The emitter
 * @type: The event type to create
 * @ap: The variable argument list pointer.
 *
 * Returns:
 * The created event or NULL in case of an error
 */
struct fy_event *
fy_emit_event_vcreate(struct fy_emitter *emit, enum fy_event_type type, va_list ap)
	FY_EXPORT;

/**
 * fy_emit_event_free() - Free an event created via fy_emit_event_create()
 *
 * Free an event previously created via fy_emit_event_create(). Note
 * that usually you don't have to call this method since if you pass
 * the event to fy_emit_event() it shall be disposed properly.
 * Only use is error recovery and cleanup.
 *
 * @emit: The emitter
 * @fye: The event to free
 */
void
fy_emit_event_free(struct fy_emitter *emit, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_parse_event_create() - Create an emit event.
 *
 * See fy_emit_event_create()...
 *
 * @fyp: The parser
 * @type: The event type to create
 *
 * Returns:
 * The created event or NULL in case of an error
 */
struct fy_event *
fy_parse_event_create(struct fy_parser *fyp, enum fy_event_type type, ...)
	FY_EXPORT;

/**
 * fy_parse_event_vcreate() - Create an emit event using varargs.
 *
 * Create an emit event to pass to fy_emit_event()
 * The varargs analogous to fy_parse_event_create().
 *
 * @fyp: The parser
 * @type: The event type to create
 * @ap: The variable argument list pointer.
 *
 * Returns:
 * The created event or NULL in case of an error
 */
struct fy_event *
fy_parse_event_vcreate(struct fy_parser *fyp, enum fy_event_type type, va_list ap)
	FY_EXPORT;

/**
 * enum fy_composer_return - The returns of the composer callback
 *
 * @FYCR_OK_CONTINUE: continue processing, event processed
 * @FYCR_OK_STOP: stop processing, event processed
 * @FYCR_OK_START_SKIP: start skip object(s), event processed
 * @FYCR_OK_STOP_SKIP: stop skipping of objects, event processed
 * @FYCR_ERROR: error, stop processing
 */
enum fy_composer_return {
	FYCR_OK_CONTINUE = 0,
	FYCR_OK_STOP = 1,
	FYCR_OK_START_SKIP = 2,
	FYCR_OK_STOP_SKIP = 3,
	FYCR_ERROR = -1,
};

/**
 * fy_composer_return_is_ok() - Check if the return code is OK
 *
 * Convenience method for checking if it's OK to continue
 *
 * @ret: the composer return to check
 *
 * Returns:
 * true if non error or skip condition
 */
static inline bool
fy_composer_return_is_ok(enum fy_composer_return ret)
{
	return ret == FYCR_OK_CONTINUE || ret == FYCR_OK_STOP;
}

/**
 * typedef fy_parse_composer_cb - composer callback method
 *
 * This method is called by the fy_parse_compose() method
 * when an event must be processed.
 *
 * @fyp: The parser
 * @fye: The event
 * @path: The path that the parser is processing
 * @userdata: The user data of the fy_parse_compose() method
 *
 * Returns:
 * fy_composer_return code telling the parser what to do
 */
typedef enum fy_composer_return
(*fy_parse_composer_cb)(struct fy_parser *fyp, struct fy_event *fye,
			struct fy_path *path, void *userdata);

/**
 * fy_parse_compose() - Parse using a compose callback
 *
 * Alternative parsing method using a composer callback.
 *
 * The parser will construct a path argument that is used
 * by the callback to make intelligent decisions about
 * creating a document and/or DOM.
 *
 * @fyp: The parser
 * @cb: The callback that will be called
 * @userdata: user pointer to pass to the callback
 *
 * Returns:
 * 0 if no error occured
 * -1 on error
 */
int
fy_parse_compose(struct fy_parser *fyp, fy_parse_composer_cb cb,
		 void *userdata)
	FY_EXPORT;

/**
 * fy_path_component_is_mapping() - Check if the component is a mapping
 *
 * @fypc: The path component to check
 *
 * Returns:
 * true if the path component is a mapping, false otherwise
 */
bool
fy_path_component_is_mapping(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_is_sequence() - Check if the component is a sequence
 *
 * @fypc: The path component to check
 *
 * Returns:
 * true if the path component is a sequence, false otherwise
 */
bool
fy_path_component_is_sequence(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_sequence_get_index() - Get the index of sequence path component
 *
 * @fypc: The sequence path component to return it's index value
 *
 * Returns:
 * >= 0 the sequence index
 * -1 if the component is either not in the proper mode, or not a sequence
 */
int
fy_path_component_sequence_get_index(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_mapping_get_scalar_key() - Get the scalar key of a mapping
 *
 * @fypc: The mapping path component to return it's scalar key
 *
 * Returns:
 * a non NULL scalar or alias token if the mapping contains a scalar key
 * NULL in case of an error, or if the component has a complex key
 */
struct fy_token *
fy_path_component_mapping_get_scalar_key(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_mapping_get_scalar_key_tag() - Get the scalar key's tag of a mapping
 *
 * @fypc: The mapping path component to return it's scalar key's tag
 *
 * Returns:
 * a non NULL tag token if the mapping contains a scalar key with a tag or
 * NULL in case of an error, or if the component has a complex key
 */
struct fy_token *
fy_path_component_mapping_get_scalar_key_tag(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_mapping_get_complex_key() - Get the complex key of a mapping
 *
 * @fypc: The mapping path component to return it's complex key
 *
 * Returns:
 * a non NULL document if the mapping contains a complex key
 * NULL in case of an error, or if the component has a simple key
 */
struct fy_document *
fy_path_component_mapping_get_complex_key(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_get_text() - Get the textual representation of a path component
 *
 * Given a path component, return a malloc'ed string which contains
 * the textual representation of it.
 *
 * Note that this method will only return fully completed components and not
 * ones that are in the building process.
 *
 * @fypc: The path component to get it's textual representation
 *
 * Returns:
 * The textual representation of the path component, NULL on error, or
 * if the component has not been completely built during the composition
 * of a complex key.
 * The string must be free'ed using free.
 */
char *
fy_path_component_get_text(struct fy_path_component *fypc)
	FY_EXPORT;

#define fy_path_component_get_text_alloca(_fypc, _res) \
	FY_ALLOCA_COPY_FREE(fy_path_component_get_text((_fypc)), FY_NT, (_res))
/**
 * fy_path_depth() - Get the depth of a path
 *
 * @fypp: The path to query
 *
 * Returns:
 * The depth of the path, or -1 on error
 */
int
fy_path_depth(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_parent() - Get the parent of a path
 *
 * Paths may contain parents when traversing complex keys.
 * This method returns the immediate parent.
 *
 * @fypp: The path to return it's parent
 *
 * Returns:
 * The path parent or NULL on error, if it doesn't exist
 */
struct fy_path *
fy_path_parent(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_get_text() - Get the textual representation of a path
 *
 * Given a path, return a malloc'ed string which contains
 * the textual representation of it.
 *
 * Note that during processing, complex key paths are simply
 * indicative and not to be used for addressing.
 *
 * @fypp: The path to get it's textual representation
 *
 * Returns:
 * The textual representation of the path, NULL on error.
 * The string must be free'ed using free.
 */
char *
fy_path_get_text(struct fy_path *fypp)
	FY_EXPORT;

#define fy_path_get_text_alloca(_fypp, _res) \
	FY_ALLOCA_COPY_FREE(fy_path_get_text((_fypp)), FY_NT, (_res))

/**
 * fy_path_in_root() - Check if the path is in the root of the document
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within the root of the document
 */
bool
fy_path_in_root(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_in_mapping() - Check if the path is in a mapping
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within a mapping
 */
bool
fy_path_in_mapping(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_in_sequence() - Check if the path is in a sequence
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within a sequence
 */
bool
fy_path_in_sequence(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_in_mapping_key() - Check if the path is in a mapping key state
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within a mapping key state
 */
bool
fy_path_in_mapping_key(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_in_mapping_value() - Check if the path is in a mapping value state
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within a mapping value state
 */
bool
fy_path_in_mapping_value(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_in_collection_root() - Check if the path is in a collection root
 *
 * A collection root state is when the path points to a sequence or mapping
 * but the state does not allow setting keys, values or adding items.
 *
 * This occurs on MAPPING/SEQUENCE START/END events.
 *
 * @fypp: The path
 *
 * Returns:
 * true if the path is located within a collectin root state
 */
bool
fy_path_in_collection_root(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_get_root_user_data() - Return the userdata associated with the path root
 *
 * @fypp: The path
 *
 * Returns:
 * The user data associated with the root of the path, or NULL if no path
 */
void *
fy_path_get_root_user_data(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_set_root_user_data() - Set the user data associated with the root
 *
 * Note, no error condition if not a path
 *
 * @fypp: The path
 * @data: The data to set as root data
 */
void
fy_path_set_root_user_data(struct fy_path *fypp, void *data)
	FY_EXPORT;

/**
 * fy_path_component_get_mapping_user_data() - Return the userdata associated with the mapping
 *
 * @fypc: The path component
 *
 * Returns:
 * The user data associated with the mapping, or NULL if not a mapping or the user data are NULL
 */
void *
fy_path_component_get_mapping_user_data(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_get_mapping_key_user_data() - Return the userdata associated with the mapping key
 *
 * @fypc: The path component
 *
 * Returns:
 * The user data associated with the mapping key, or NULL if not a mapping or the user data are NULL
 */
void *
fy_path_component_get_mapping_key_user_data(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_get_sequence_user_data() - Return the userdata associated with the sequence
 *
 * @fypc: The path component
 *
 * Returns:
 * The user data associated with the sequence, or NULL if not a sequence or the user data are NULL
 */
void *
fy_path_component_get_sequence_user_data(struct fy_path_component *fypc)
	FY_EXPORT;

/**
 * fy_path_component_set_mapping_user_data() - Set the user data associated with a mapping
 *
 * Note, no error condition if not a mapping
 *
 * @fypc: The path component
 * @data: The data to set as mapping data
 */
void
fy_path_component_set_mapping_user_data(struct fy_path_component *fypc, void *data)
	FY_EXPORT;

/**
 * fy_path_component_set_mapping_key_user_data() - Set the user data associated with a mapping key
 *
 * Note, no error condition if not in a mapping key state
 *
 * @fypc: The path component
 * @data: The data to set as mapping key data
 */
void
fy_path_component_set_mapping_key_user_data(struct fy_path_component *fypc, void *data)
	FY_EXPORT;

/**
 * fy_path_component_set_sequence_user_data() - Set the user data associated with a sequence
 *
 * Note, no error condition if not a sequence
 *
 * @fypc: The path component
 * @data: The data to set as sequence data
 */
void
fy_path_component_set_sequence_user_data(struct fy_path_component *fypc, void *data)
	FY_EXPORT;

/**
 * fy_path_last_component() - Get the very last component of a path
 *
 * Returns the last component of a path.
 *
 * @fypp: The path
 *
 * Returns:
 * The last path component (which may be a collection root component), or NULL
 * if it does not exist
 */
struct fy_path_component *
fy_path_last_component(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_path_last_not_collection_root_component() - Get the last non collection root component of a path
 *
 * Returns the last non collection root component of a path. This may not be the
 * last component that is returned by fy_path_last_component().
 *
 * The difference is present on MAPPING/SEQUENCE START/END events where the
 * last component is present but not usuable as a object parent.
 *
 * @fypp: The path
 *
 * Returns:
 * The last non collection root component, or NULL if it does not exist
 */
struct fy_path_component *
fy_path_last_not_collection_root_component(struct fy_path *fypp)
	FY_EXPORT;

/**
 * fy_document_iterator_create() - Create a document iterator
 *
 * Creates a document iterator, that can trawl through a document
 * without using recursion.
 *
 * Returns:
 * The newly created document iterator or NULL on error
 */
struct fy_document_iterator *
fy_document_iterator_create(void)
	FY_EXPORT;

/**
 * fy_document_iterator_destroy() - Destroy the given document iterator
 *
 * Destroy a document iterator created earlier via fy_document_iterator_create().
 *
 * @fydi: The document iterator to destroy
 */
void
fy_document_iterator_destroy(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_event_free() - Free an event that was created by a document iterator
 *
 * Free (possibly recycling) an event that was created by a document iterator.
 *
 * @fydi: The document iterator that created the event
 * @fye: The event
 */
void
fy_document_iterator_event_free(struct fy_document_iterator *fydi, struct fy_event *fye)
	FY_EXPORT;

/**
 * fy_document_iterator_stream_start() - Create a stream start event using the iterator
 *
 * Creates a stream start event on the document iterator and advances the internal state
 * of it accordingly.
 *
 * @fydi: The document iterator to create the event
 *
 * Returns:
 * The newly created stream start event, or NULL on error.
 */
struct fy_event *
fy_document_iterator_stream_start(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_stream_end() - Create a stream end event using the iterator
 *
 * Creates a stream end event on the document iterator and advances the internal state
 * of it accordingly.
 *
 * @fydi: The document iterator to create the event
 *
 * Returns:
 * The newly created stream end event, or NULL on error.
 */
struct fy_event *
fy_document_iterator_stream_end(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_document_start() - Create a document start event using the iterator
 *
 * Creates a document start event on the document iterator and advances the internal state
 * of it accordingly. The document must not be released until an error, cleanup or a call
 * to fy_document_iterator_document_end().
 *
 * @fydi: The document iterator to create the event
 * @fyd: The document containing the document state that is used in the event
 *
 * Returns:
 * The newly created document start event, or NULL on error.
 */
struct fy_event *
fy_document_iterator_document_start(struct fy_document_iterator *fydi, struct fy_document *fyd)
	FY_EXPORT;

/**
 * fy_document_iterator_document_end() - Create a document end event using the iterator
 *
 * Creates a document end event on the document iterator and advances the internal state
 * of it accordingly. The document that was used earlier in the call of
 * fy_document_iterator_document_start() can now be released.
 *
 * @fydi: The document iterator to create the event
 *
 * Returns:
 * The newly created document end event, or NULL on error.
 */
struct fy_event *
fy_document_iterator_document_end(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_body_next() - Create document body events until the end
 *
 * Creates the next document body, depth first until the end of the document.
 * The events are created depth first and are in same exact sequence that the
 * original events that created the document.
 *
 * That means that the finite event stream that generated the document is losslesly
 * preserved in such a way that the document tree representation is functionally
 * equivalent.
 *
 * Repeated calls to this function will generate a stream of SCALAR, ALIAS, SEQUENCE
 * START, SEQUENCE END, MAPPING START and MAPPING END events, returning NULL at the
 * end of the body event stream.
 *
 * @fydi: The document iterator to create the event
 *
 * Returns:
 * The newly created document body event or NULL at an error, or an end of the
 * event stream. Use fy_document_iterator_get_error() to check if an error occured.
 */
struct fy_event *
fy_document_iterator_body_next(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_node_start() - Start a document node iteration run using a starting point
 *
 * Starts an iteration run starting at the given node.
 *
 * @fydi: The document iterator to run with
 * @fyn: The iterator root for the iteration
 */
void
fy_document_iterator_node_start(struct fy_document_iterator *fydi, struct fy_node *fyn)
	FY_EXPORT;

/**
 * fy_document_iterator_node_next() - Return the next node in the iteration sequence
 *
 * Returns a pointer to the next node iterating using as a start the node given
 * at fy_document_iterator_node_start(). The first node returned will be that,
 * followed by all the remaing nodes in the subtree.
 *
 * @fydi: The document iterator to use for the iteration
 *
 * Returns:
 * The next node in the iteration sequence or NULL at the end, or if an error occured.
 */
struct fy_node *
fy_document_iterator_node_next(struct fy_document_iterator *fydi)
	FY_EXPORT;

/**
 * fy_document_iterator_get_error() - Get the error state of the document iterator
 *
 * Returns the error state of the iterator. If it's in error state, return true
 * and reset the iterator to the state just after creation.
 *
 * @fydi: The document iterator to use for checking it's error state.
 *
 * Returns:
 * true if it was in an error state, false otherwise.
 */
bool
fy_document_iterator_get_error(struct fy_document_iterator *fydi)
	FY_EXPORT;

#ifdef __cplusplus
}
#endif

#endif
