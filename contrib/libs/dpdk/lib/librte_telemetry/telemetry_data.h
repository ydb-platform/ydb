/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Intel Corporation
 */

#ifndef _TELEMETRY_DATA_H_
#define _TELEMETRY_DATA_H_

#include <inttypes.h>
#include "rte_telemetry.h"

enum tel_container_types {
	RTE_TEL_NULL,	      /** null, used as error value */
	RTE_TEL_STRING,	      /** basic string type, no included data */
	RTE_TEL_DICT,	      /** name-value pairs, of individual value type */
	RTE_TEL_ARRAY_STRING, /** array of string values only */
	RTE_TEL_ARRAY_INT,    /** array of signed, 32-bit int values */
	RTE_TEL_ARRAY_U64,    /** array of unsigned 64-bit int values */
	RTE_TEL_ARRAY_CONTAINER, /** array of container structs */
};

struct container {
	struct rte_tel_data *data;
	int keep;
};

/* each type here must have an equivalent enum in the value types enum in
 * telemetry.h and an array type defined above, and have appropriate
 * type assignment in the RTE_TEL_data_start_array() function
 */
union tel_value {
	char sval[RTE_TEL_MAX_STRING_LEN];
	int ival;
	uint64_t u64val;
	struct container container;
};

struct tel_dict_entry {
	char name[RTE_TEL_MAX_STRING_LEN];
	enum rte_tel_value_type type;
	union tel_value value;
};

struct rte_tel_data {
	enum tel_container_types type;
	unsigned int data_len; /* for array or object, how many items */
	union {
		char str[RTE_TEL_MAX_SINGLE_STRING_LEN];
		struct tel_dict_entry dict[RTE_TEL_MAX_DICT_ENTRIES];
		union tel_value array[RTE_TEL_MAX_ARRAY_ENTRIES];
	} data; /* data container */
};

#endif
