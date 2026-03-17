/*
 * Copyright (c) 2013-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer listed
 *   in this license in the documentation and/or other materials
 *   provided with the distribution.
 *
 * - Neither the name of the copyright holders nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * The copyright holders provide no reassurances that the source code
 * provided does not infringe any patent, copyright, or any other
 * intellectual property rights of third parties.  The copyright holders
 * disclaim any liability to any recipient for claims brought against
 * recipient by any third party for infringement of that parties
 * intellectual property rights.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * $HEADER$
 */

#ifndef PMIx_EXTEND_H
#define PMIx_EXTEND_H

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

/* expose some functions that are resolved in the
 * PMIx library, but part of a header that
 * includes internal functions - we don't
 * want to expose the entire header here. These
 * back the associated macros included in the
 * PMIx Standard
 */
void pmix_value_load(pmix_value_t *v, const void *data, pmix_data_type_t type);

pmix_status_t pmix_value_unload(pmix_value_t *kv, void **data, size_t *sz);

pmix_status_t pmix_value_xfer(pmix_value_t *kv, const pmix_value_t *src);

pmix_status_t pmix_argv_append_nosize(char ***argv, const char *arg);

pmix_status_t pmix_argv_prepend_nosize(char ***argv, const char *arg);

pmix_status_t pmix_argv_append_unique_nosize(char ***argv, const char *arg, bool overwrite);

void pmix_argv_free(char **argv);

char **pmix_argv_split(const char *src_string, int delimiter);

int pmix_argv_count(char **argv);

char *pmix_argv_join(char **argv, int delimiter);

char **pmix_argv_copy(char **argv);

pmix_status_t pmix_setenv(const char *name, const char *value,
                          bool overwrite, char ***env);


/* the following are a set of legacy macros not included in the
 * PMIx Standard, but used in some codes (e.g., the Slurm plugin).
 * These should be considered "deprecated" and will be removed
 * in the next major release of the PRI */
#define PMIX_VAL_FIELD_int(x)       ((x)->data.integer)
#define PMIX_VAL_FIELD_uint32_t(x)  ((x)->data.uint32)
#define PMIX_VAL_FIELD_uint16_t(x)  ((x)->data.uint16)
#define PMIX_VAL_FIELD_string(x)    ((x)->data.string)
#define PMIX_VAL_FIELD_float(x)     ((x)->data.fval)
#define PMIX_VAL_FIELD_byte(x)      ((x)->data.byte)
#define PMIX_VAL_FIELD_flag(x)      ((x)->data.flag)

#define PMIX_VAL_TYPE_int      PMIX_INT
#define PMIX_VAL_TYPE_uint32_t PMIX_UINT32
#define PMIX_VAL_TYPE_uint16_t PMIX_UINT16
#define PMIX_VAL_TYPE_string   PMIX_STRING
#define PMIX_VAL_TYPE_float    PMIX_FLOAT
#define PMIX_VAL_TYPE_byte     PMIX_BYTE
#define PMIX_VAL_TYPE_flag     PMIX_BOOL

#define PMIX_VAL_set_assign(_v, _field, _val )   \
    do {                                                            \
        (_v)->type = PMIX_VAL_TYPE_ ## _field;                      \
        PMIX_VAL_FIELD_ ## _field((_v)) = _val;                     \
    } while (0)

#define PMIX_VAL_set_strdup(_v, _field, _val )       \
    do {                                                                \
        (_v)->type = PMIX_VAL_TYPE_ ## _field;                          \
        PMIX_VAL_FIELD_ ## _field((_v)) = strdup(_val);                 \
    } while (0)

#define PMIX_VAL_SET_int        PMIX_VAL_set_assign
#define PMIX_VAL_SET_uint32_t   PMIX_VAL_set_assign
#define PMIX_VAL_SET_uint16_t   PMIX_VAL_set_assign
#define PMIX_VAL_SET_string     PMIX_VAL_set_strdup
#define PMIX_VAL_SET_float      PMIX_VAL_set_assign
#define PMIX_VAL_SET_byte       PMIX_VAL_set_assign
#define PMIX_VAL_SET_flag       PMIX_VAL_set_assign

#define PMIX_VAL_SET(_v, _field, _val )   \
    PMIX_VAL_SET_ ## _field(_v, _field, _val)

#define PMIX_VAL_cmp_val(_val1, _val2)      ((_val1) != (_val2))
#define PMIX_VAL_cmp_float(_val1, _val2)    (((_val1)>(_val2))?(((_val1)-(_val2))>0.000001):(((_val2)-(_val1))>0.000001))
#define PMIX_VAL_cmp_ptr(_val1, _val2)      strncmp(_val1, _val2, strlen(_val1)+1)

#define PMIX_VAL_CMP_int        PMIX_VAL_cmp_val
#define PMIX_VAL_CMP_uint32_t   PMIX_VAL_cmp_val
#define PMIX_VAL_CMP_uint16_t   PMIX_VAL_cmp_val
#define PMIX_VAL_CMP_float      PMIX_VAL_cmp_float
#define PMIX_VAL_CMP_string     PMIX_VAL_cmp_ptr
#define PMIX_VAL_CMP_byte       PMIX_VAL_cmp_val
#define PMIX_VAL_CMP_flag       PMIX_VAL_cmp_val

#define PMIX_VAL_ASSIGN(_v, _field, _val) \
    PMIX_VAL_set_assign(_v, _field, _val)

#define PMIX_VAL_CMP(_field, _val1, _val2) \
    PMIX_VAL_CMP_ ## _field(_val1, _val2)

#define PMIX_VAL_FREE(_v) \
     PMIx_free_value_data(_v)

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif
