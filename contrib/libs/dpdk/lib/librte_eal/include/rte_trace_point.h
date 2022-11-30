/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#ifndef _RTE_TRACE_POINT_H_
#define _RTE_TRACE_POINT_H_

/**
 * @file
 *
 * RTE Tracepoint API
 *
 * This file provides the tracepoint API to RTE applications.
 *
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdio.h>

#include <rte_branch_prediction.h>
#include <rte_common.h>
#include <rte_compat.h>
#include <rte_cycles.h>
#include <rte_per_lcore.h>
#include <rte_string_fns.h>
#include <rte_uuid.h>

/** The tracepoint object. */
typedef uint64_t rte_trace_point_t;

/**
 * Macro to define the tracepoint arguments in RTE_TRACE_POINT macro.

 * @see RTE_TRACE_POINT, RTE_TRACE_POINT_FP
 */
#define RTE_TRACE_POINT_ARGS

#ifndef _RTE_TRACE_POINT_REGISTER_H_
#define _RTE_TRACE_POINT_ATTRS __rte_always_inline
#else
/* Registration functions are NOT ubsan friendly */
#define _RTE_TRACE_POINT_ATTRS __rte_always_inline __attribute__((no_sanitize("undefined")))
#endif /* _RTE_TRACE_POINT_REGISTER_H_ */

/** @internal Helper macro to support RTE_TRACE_POINT and RTE_TRACE_POINT_FP */
#define __RTE_TRACE_POINT(_mode, _tp, _args, ...) \
extern rte_trace_point_t __##_tp; \
static void _RTE_TRACE_POINT_ATTRS \
_tp _args \
{ \
	__rte_trace_point_emit_header_##_mode(&__##_tp); \
	__VA_ARGS__ \
}

/**
 * Create a tracepoint.
 *
 * A tracepoint is defined by specifying:
 * - its input arguments: they are the C function style parameters to define
 *   the arguments of tracepoint function. These input arguments are embedded
 *   using the RTE_TRACE_POINT_ARGS macro.
 * - its output event fields: they are the sources of event fields that form
 *   the payload of any event that the execution of the tracepoint macro emits
 *   for this particular tracepoint. The application uses
 *   rte_trace_point_emit_* macros to emit the output event fields.
 *
 * @param tp
 *   Tracepoint object. Before using the tracepoint, an application needs to
 *   define the tracepoint using RTE_TRACE_POINT_REGISTER macro.
 * @param args
 *   C function style input arguments to define the arguments to tracepoint
 *   function.
 * @param ...
 *   Define the payload of trace function. The payload will be formed using
 *   rte_trace_point_emit_* macros. Use ";" delimiter between two payloads.
 *
 * @see RTE_TRACE_POINT_ARGS, RTE_TRACE_POINT_REGISTER, rte_trace_point_emit_*
 */
#define RTE_TRACE_POINT(tp, args, ...) \
	__RTE_TRACE_POINT(generic, tp, args, __VA_ARGS__)

/**
 * Create a tracepoint for fast path.
 *
 * Similar to RTE_TRACE_POINT, except that it is removed at compilation time
 * unless the RTE_ENABLE_TRACE_FP configuration parameter is set.
 *
 * @param tp
 *   Tracepoint object. Before using the tracepoint, an application needs to
 *   define the tracepoint using RTE_TRACE_POINT_REGISTER macro.
 * @param args
 *   C function style input arguments to define the arguments to tracepoint.
 *   function.
 * @param ...
 *   Define the payload of trace function. The payload will be formed using
 *   rte_trace_point_emit_* macros, Use ";" delimiter between two payloads.
 *
 * @see RTE_TRACE_POINT
 */
#define RTE_TRACE_POINT_FP(tp, args, ...) \
	__RTE_TRACE_POINT(fp, tp, args, __VA_ARGS__)

#ifdef __DOXYGEN__

/**
 * Register a tracepoint.
 *
 * @param trace
 *   The tracepoint object created using RTE_TRACE_POINT_REGISTER.
 * @param name
 *   The name of the tracepoint object.
 * @return
 *   - 0: Successfully registered the tracepoint.
 *   - <0: Failure to register the tracepoint.
 */
#define RTE_TRACE_POINT_REGISTER(trace, name)

/** Tracepoint function payload for uint64_t datatype */
#define rte_trace_point_emit_u64(val)
/** Tracepoint function payload for int64_t datatype */
#define rte_trace_point_emit_i64(val)
/** Tracepoint function payload for uint32_t datatype */
#define rte_trace_point_emit_u32(val)
/** Tracepoint function payload for int32_t datatype */
#define rte_trace_point_emit_i32(val)
/** Tracepoint function payload for uint16_t datatype */
#define rte_trace_point_emit_u16(val)
/** Tracepoint function payload for int16_t datatype */
#define rte_trace_point_emit_i16(val)
/** Tracepoint function payload for uint8_t datatype */
#define rte_trace_point_emit_u8(val)
/** Tracepoint function payload for int8_t datatype */
#define rte_trace_point_emit_i8(val)
/** Tracepoint function payload for int datatype */
#define rte_trace_point_emit_int(val)
/** Tracepoint function payload for long datatype */
#define rte_trace_point_emit_long(val)
/** Tracepoint function payload for size_t datatype */
#define rte_trace_point_emit_size_t(val)
/** Tracepoint function payload for float datatype */
#define rte_trace_point_emit_float(val)
/** Tracepoint function payload for double datatype */
#define rte_trace_point_emit_double(val)
/** Tracepoint function payload for pointer datatype */
#define rte_trace_point_emit_ptr(val)
/** Tracepoint function payload for string datatype */
#define rte_trace_point_emit_string(val)

#endif /* __DOXYGEN__ */

/** @internal Macro to define maximum emit length of string datatype. */
#define __RTE_TRACE_EMIT_STRING_LEN_MAX 32
/** @internal Macro to define event header size. */
#define __RTE_TRACE_EVENT_HEADER_SZ sizeof(uint64_t)

/**
 * Enable recording events of the given tracepoint in the trace buffer.
 *
 * @param tp
 *   The tracepoint object to enable.
 * @return
 *   - 0: Success.
 *   - (-ERANGE): Trace object is not registered.
 */
__rte_experimental
int rte_trace_point_enable(rte_trace_point_t *tp);

/**
 * Disable recording events of the given tracepoint in the trace buffer.
 *
 * @param tp
 *   The tracepoint object to disable.
 * @return
 *   - 0: Success.
 *   - (-ERANGE): Trace object is not registered.
 */
__rte_experimental
int rte_trace_point_disable(rte_trace_point_t *tp);

/**
 * Test if recording events from the given tracepoint is enabled.
 *
 * @param tp
 *    The tracepoint object.
 * @return
 *    true if tracepoint is enabled, false otherwise.
 */
__rte_experimental
bool rte_trace_point_is_enabled(rte_trace_point_t *tp);

/**
 * Lookup a tracepoint object from its name.
 *
 * @param name
 *   The name of the tracepoint.
 * @return
 *   The tracepoint object or NULL if not found.
 */
__rte_experimental
rte_trace_point_t *rte_trace_point_lookup(const char *name);

/**
 * @internal
 *
 * Test if the tracepoint fast path compile-time option is enabled.
 *
 * @return
 *   true if tracepoint fast path enabled, false otherwise.
 */
__rte_experimental
static __rte_always_inline bool
__rte_trace_point_fp_is_enabled(void)
{
#ifdef RTE_ENABLE_TRACE_FP
	return true;
#else
	return false;
#endif
}

/**
 * @internal
 *
 * Allocate trace memory buffer per thread.
 *
 */
__rte_experimental
void __rte_trace_mem_per_thread_alloc(void);

/**
 * @internal
 *
 * Helper function to emit field.
 *
 * @param sz
 *   The tracepoint size.
 * @param field
 *   The name of the trace event.
 * @param type
 *   The datatype of the trace event as string.
 * @return
 *   - 0: Success.
 *   - <0: Failure.
 */
__rte_experimental
void __rte_trace_point_emit_field(size_t sz, const char *field,
	const char *type);

/**
 * @internal
 *
 * Helper function to register a dynamic tracepoint.
 * Use RTE_TRACE_POINT_REGISTER macro for tracepoint registration.
 *
 * @param trace
 *   The tracepoint object created using RTE_TRACE_POINT_REGISTER.
 * @param name
 *   The name of the tracepoint object.
 * @param register_fn
 *   Trace registration function.
 * @return
 *   - 0: Successfully registered the tracepoint.
 *   - <0: Failure to register the tracepoint.
 */
__rte_experimental
int __rte_trace_point_register(rte_trace_point_t *trace, const char *name,
	void (*register_fn)(void));

#ifndef __DOXYGEN__

#ifndef _RTE_TRACE_POINT_REGISTER_H_
#ifdef ALLOW_EXPERIMENTAL_API

#define __RTE_TRACE_EVENT_HEADER_ID_SHIFT (48)

#define __RTE_TRACE_FIELD_SIZE_SHIFT 0
#define __RTE_TRACE_FIELD_SIZE_MASK (0xffffULL << __RTE_TRACE_FIELD_SIZE_SHIFT)
#define __RTE_TRACE_FIELD_ID_SHIFT (16)
#define __RTE_TRACE_FIELD_ID_MASK (0xffffULL << __RTE_TRACE_FIELD_ID_SHIFT)
#define __RTE_TRACE_FIELD_ENABLE_MASK (1ULL << 63)
#define __RTE_TRACE_FIELD_ENABLE_DISCARD (1ULL << 62)

struct __rte_trace_stream_header {
	uint32_t magic;
	rte_uuid_t uuid;
	uint32_t lcore_id;
	char thread_name[__RTE_TRACE_EMIT_STRING_LEN_MAX];
} __rte_packed;

struct __rte_trace_header {
	uint32_t offset;
	uint32_t len;
	struct __rte_trace_stream_header stream_header;
	uint8_t mem[];
};

RTE_DECLARE_PER_LCORE(void *, trace_mem);

static __rte_always_inline void *
__rte_trace_mem_get(uint64_t in)
{
	struct __rte_trace_header *trace =
		(struct __rte_trace_header *)(RTE_PER_LCORE(trace_mem));
	const uint16_t sz = in & __RTE_TRACE_FIELD_SIZE_MASK;

	/* Trace memory is not initialized for this thread */
	if (unlikely(trace == NULL)) {
		__rte_trace_mem_per_thread_alloc();
		trace = (struct __rte_trace_header *)(RTE_PER_LCORE(trace_mem));
		if (unlikely(trace == NULL))
			return NULL;
	}
	/* Check the wrap around case */
	uint32_t offset = trace->offset;
	if (unlikely((offset + sz) >= trace->len)) {
		/* Disable the trace event if it in DISCARD mode */
		if (unlikely(in & __RTE_TRACE_FIELD_ENABLE_DISCARD))
			return NULL;

		offset = 0;
	}
	/* Align to event header size */
	offset = RTE_ALIGN_CEIL(offset, __RTE_TRACE_EVENT_HEADER_SZ);
	void *mem = RTE_PTR_ADD(&trace->mem[0], offset);
	offset += sz;
	trace->offset = offset;

	return mem;
}

static __rte_always_inline void *
__rte_trace_point_emit_ev_header(void *mem, uint64_t in)
{
	uint64_t val;

	/* Event header [63:0] = id [63:48] | timestamp [47:0] */
	val = rte_get_tsc_cycles() &
		~(0xffffULL << __RTE_TRACE_EVENT_HEADER_ID_SHIFT);
	val |= ((in & __RTE_TRACE_FIELD_ID_MASK) <<
		(__RTE_TRACE_EVENT_HEADER_ID_SHIFT -
		 __RTE_TRACE_FIELD_ID_SHIFT));

	*(uint64_t *)mem = val;
	return RTE_PTR_ADD(mem, __RTE_TRACE_EVENT_HEADER_SZ);
}

#define __rte_trace_point_emit_header_generic(t) \
void *mem; \
do { \
	const uint64_t val = __atomic_load_n(t, __ATOMIC_ACQUIRE); \
	if (likely(!(val & __RTE_TRACE_FIELD_ENABLE_MASK))) \
		return; \
	mem = __rte_trace_mem_get(val); \
	if (unlikely(mem == NULL)) \
		return; \
	mem = __rte_trace_point_emit_ev_header(mem, val); \
} while (0)

#define __rte_trace_point_emit_header_fp(t) \
	if (!__rte_trace_point_fp_is_enabled()) \
		return; \
	__rte_trace_point_emit_header_generic(t)

#define __rte_trace_point_emit(in, type) \
do { \
	memcpy(mem, &(in), sizeof(in)); \
	mem = RTE_PTR_ADD(mem, sizeof(in)); \
} while (0)

#define rte_trace_point_emit_string(in) \
do { \
	if (unlikely(in == NULL)) \
		return; \
	rte_strscpy(mem, in, __RTE_TRACE_EMIT_STRING_LEN_MAX); \
	mem = RTE_PTR_ADD(mem, __RTE_TRACE_EMIT_STRING_LEN_MAX); \
} while (0)

#else

#define __rte_trace_point_emit_header_generic(t) RTE_SET_USED(t)
#define __rte_trace_point_emit_header_fp(t) RTE_SET_USED(t)
#define __rte_trace_point_emit(in, type) RTE_SET_USED(in)
#define rte_trace_point_emit_string(in) RTE_SET_USED(in)

#endif /* ALLOW_EXPERIMENTAL_API */
#endif /* _RTE_TRACE_POINT_REGISTER_H_ */

#define rte_trace_point_emit_u64(in) __rte_trace_point_emit(in, uint64_t)
#define rte_trace_point_emit_i64(in) __rte_trace_point_emit(in, int64_t)
#define rte_trace_point_emit_u32(in) __rte_trace_point_emit(in, uint32_t)
#define rte_trace_point_emit_i32(in) __rte_trace_point_emit(in, int32_t)
#define rte_trace_point_emit_u16(in) __rte_trace_point_emit(in, uint16_t)
#define rte_trace_point_emit_i16(in) __rte_trace_point_emit(in, int16_t)
#define rte_trace_point_emit_u8(in) __rte_trace_point_emit(in, uint8_t)
#define rte_trace_point_emit_i8(in) __rte_trace_point_emit(in, int8_t)
#define rte_trace_point_emit_int(in) __rte_trace_point_emit(in, int32_t)
#define rte_trace_point_emit_long(in) __rte_trace_point_emit(in, long)
#define rte_trace_point_emit_size_t(in) __rte_trace_point_emit(in, size_t)
#define rte_trace_point_emit_float(in) __rte_trace_point_emit(in, float)
#define rte_trace_point_emit_double(in) __rte_trace_point_emit(in, double)
#define rte_trace_point_emit_ptr(in) __rte_trace_point_emit(in, uintptr_t)

#endif /* __DOXYGEN__ */

#ifdef __cplusplus
}
#endif

#endif /* _RTE_TRACE_POINT_H_ */
