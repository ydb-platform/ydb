/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#ifndef _RTE_TRACE_POINT_REGISTER_H_
#define _RTE_TRACE_POINT_REGISTER_H_

#ifdef _RTE_TRACE_POINT_H_
#error for registration, include this file first before <rte_trace_point.h>
#endif

#include <rte_per_lcore.h>
#include <rte_trace_point.h>

RTE_DECLARE_PER_LCORE(volatile int, trace_point_sz);

#define RTE_TRACE_POINT_REGISTER(trace, name) \
rte_trace_point_t __attribute__((section("__rte_trace_point"))) __##trace; \
RTE_INIT(trace##_init) \
{ \
	__rte_trace_point_register(&__##trace, RTE_STR(name), \
		(void (*)(void)) trace); \
}

#define __rte_trace_point_emit_header_generic(t) \
	RTE_PER_LCORE(trace_point_sz) = __RTE_TRACE_EVENT_HEADER_SZ

#define __rte_trace_point_emit_header_fp(t) \
	__rte_trace_point_emit_header_generic(t)

#define __rte_trace_point_emit(in, type) \
do { \
	RTE_BUILD_BUG_ON(sizeof(type) != sizeof(typeof(in))); \
	__rte_trace_point_emit_field(sizeof(type), RTE_STR(in), \
		RTE_STR(type)); \
} while (0)

#define rte_trace_point_emit_string(in) \
do { \
	RTE_SET_USED(in); \
	__rte_trace_point_emit_field(__RTE_TRACE_EMIT_STRING_LEN_MAX, \
		RTE_STR(in)"[32]", "string_bounded_t"); \
} while (0)

#endif /* _RTE_TRACE_POINT_REGISTER_H_ */
