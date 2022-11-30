/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_PER_LCORE_H_
#define _RTE_PER_LCORE_H_

/**
 * @file
 *
 * Per-lcore variables in RTE
 *
 * This file defines an API for instantiating per-lcore "global
 * variables" that are environment-specific. Note that in all
 * environments, a "shared variable" is the default when you use a
 * global variable.
 *
 * Parts of this are execution environment specific.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

/**
 * Macro to define a per lcore variable "var" of type "type", don't
 * use keywords like "static" or "volatile" in type, just prefix the
 * whole macro.
 */
#define RTE_DEFINE_PER_LCORE(type, name)			\
	__thread __typeof__(type) per_lcore_##name

/**
 * Macro to declare an extern per lcore variable "var" of type "type"
 */
#define RTE_DECLARE_PER_LCORE(type, name)			\
	extern __thread __typeof__(type) per_lcore_##name

/**
 * Read/write the per-lcore variable value
 */
#define RTE_PER_LCORE(name) (per_lcore_##name)

#ifdef __cplusplus
}
#endif

#endif /* _RTE_PER_LCORE_H_ */
