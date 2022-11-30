#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#include <inttypes.h>
#include <time.h>

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_time.h>
#include <rte_trace.h>
#include <rte_version.h>

#include "eal_trace.h"

__rte_format_printf(2, 0)
static int
metadata_printf(char **str, const char *fmt, ...)
{
	va_list ap;
	int rc;

	*str = NULL;
	va_start(ap, fmt);
	rc = vasprintf(str, fmt, ap);
	va_end(ap);

	return rc;
}

static int
meta_copy(char **meta, int *offset, char *str, int rc)
{
	int count = *offset;
	char *ptr = *meta;

	if (rc < 0)
		return rc;

	ptr = realloc(ptr, count + rc + 1);
	if (ptr == NULL)
		goto free_str;

	memcpy(RTE_PTR_ADD(ptr, count), str, rc);
	ptr[count + rc] = '\0';
	count += rc;
	free(str);

	*meta = ptr;
	*offset = count;

	return rc;

free_str:
	if (str)
		free(str);
	return -ENOMEM;
}

static int
meta_data_type_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"/* CTF 1.8 */\n"
		"typealias integer {size = 8; base = x;}:= uint8_t;\n"
		"typealias integer {size = 16; base = x;} := uint16_t;\n"
		"typealias integer {size = 32; base = x;} := uint32_t;\n"
		"typealias integer {size = 64; base = x;} := uint64_t;\n"
		"typealias integer {size = 8; signed = true;}  := int8_t;\n"
		"typealias integer {size = 16; signed = true;} := int16_t;\n"
		"typealias integer {size = 32; signed = true;} := int32_t;\n"
		"typealias integer {size = 64; signed = true;} := int64_t;\n"
#ifdef RTE_ARCH_64
		"typealias integer {size = 64; base = x;} := uintptr_t;\n"
#else
		"typealias integer {size = 32; base = x;} := uintptr_t;\n"
#endif
#ifdef RTE_ARCH_64
		"typealias integer {size = 64; base = x;} := long;\n"
#else
		"typealias integer {size = 32; base = x;} := long;\n"
#endif
		"typealias integer {size = 8; signed = false; encoding = ASCII; } := string_bounded_t;\n\n"
#ifdef RTE_ARCH_64
		"typealias integer {size = 64; base = x;} := size_t;\n"
#else
		"typealias integer {size = 32; base = x;} := size_t;\n"
#endif
		"typealias floating_point {\n"
		"    exp_dig = 8;\n"
		"    mant_dig = 24;\n"
		"} := float;\n\n"
		"typealias floating_point {\n"
		"    exp_dig = 11;\n"
		"    mant_dig = 53;\n"
		"} := double;\n\n");

	return meta_copy(meta, offset, str, rc);
}

static int
is_be(void)
{
#if RTE_BYTE_ORDER == RTE_BIG_ENDIAN
	return 1;
#else
	return 0;
#endif
}

static int
meta_header_emit(char **meta, int *offset)
{
	struct trace *trace = trace_obj_get();
	char uustr[RTE_UUID_STRLEN];
	char *str = NULL;
	int rc;

	rte_uuid_unparse(trace->uuid, uustr, RTE_UUID_STRLEN);
	rc = metadata_printf(&str,
		"trace {\n"
		"    major = 1;\n"
		"    minor = 8;\n"
		"    uuid = \"%s\";\n"
		"    byte_order = %s;\n"
		"    packet.header := struct {\n"
		"	    uint32_t magic;\n"
		"	    uint8_t  uuid[16];\n"
		"    };\n"
		"};\n\n", uustr, is_be() ? "be" : "le");
	return meta_copy(meta, offset, str, rc);
}

static int
meta_env_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"env {\n"
		"    dpdk_version = \"%s\";\n"
		"    tracer_name = \"dpdk\";\n"
		"};\n\n", rte_version());
	return meta_copy(meta, offset, str, rc);
}

static int
meta_clock_pass1_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"clock {\n"
		"    name = \"dpdk\";\n"
		"    freq = ");
	return meta_copy(meta, offset, str, rc);
}

static int
meta_clock_pass2_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"%20"PRIu64";\n"
		"    offset_s =", 0);
	return meta_copy(meta, offset, str, rc);
}

static int
meta_clock_pass3_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"%20"PRIu64";\n"
		"    offset =", 0);
	return meta_copy(meta, offset, str, rc);
}

static int
meta_clock_pass4_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"%20"PRIu64";\n};\n\n"
		"typealias integer {\n"
		"    size = 48; align = 1; signed = false;\n"
		"    map = clock.dpdk.value;\n"
		"} := uint48_clock_dpdk_t;\n\n", 0);

	return meta_copy(meta, offset, str, rc);
}

static int
meta_stream_emit(char **meta, int *offset)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"stream {\n"
		"    packet.context := struct {\n"
		"         uint32_t cpu_id;\n"
		"         string_bounded_t name[32];\n"
		"    };\n"
		"    event.header := struct {\n"
		"          uint48_clock_dpdk_t timestamp;\n"
		"          uint16_t id;\n"
		"    } align(64);\n"
		"};\n\n");
	return meta_copy(meta, offset, str, rc);
}

static int
meta_event_emit(char **meta, int *offset, struct trace_point *tp)
{
	char *str = NULL;
	int rc;

	rc = metadata_printf(&str,
		"event {\n"
		"    id = %d;\n"
		"    name = \"%s\";\n"
		"    fields := struct {\n"
		"%s"
		"    };\n"
		"};\n\n", trace_id_get(tp->handle), tp->name,
		tp->ctf_field != NULL ? tp->ctf_field : "");
	return meta_copy(meta, offset, str, rc);
}

int
trace_metadata_create(void)
{
	struct trace_point_head *tp_list = trace_list_head_get();
	struct trace *trace = trace_obj_get();
	struct trace_point *tp;
	int rc, offset = 0;
	char *meta = NULL;

	rc = meta_data_type_emit(&meta, &offset);
	if (rc < 0)
		goto fail;

	rc = meta_header_emit(&meta, &offset);
	if (rc < 0)
		goto fail;

	rc = meta_env_emit(&meta, &offset);
	if (rc < 0)
		goto fail;

	rc = meta_clock_pass1_emit(&meta, &offset);
	if (rc < 0)
		goto fail;
	trace->ctf_meta_offset_freq = offset;

	rc = meta_clock_pass2_emit(&meta, &offset);
	if (rc < 0)
		goto fail;
	trace->ctf_meta_offset_freq_off_s = offset;

	rc = meta_clock_pass3_emit(&meta, &offset);
	if (rc < 0)
		goto fail;
	trace->ctf_meta_offset_freq_off = offset;

	rc = meta_clock_pass4_emit(&meta, &offset);
	if (rc < 0)
		goto fail;

	rc = meta_stream_emit(&meta, &offset);
	if (rc < 0)
		goto fail;

	STAILQ_FOREACH(tp, tp_list, next)
		if (meta_event_emit(&meta, &offset, tp) < 0)
			goto fail;

	trace->ctf_meta = meta;
	return 0;

fail:
	if (meta)
		free(meta);
	return -EBADF;
}

void
trace_metadata_destroy(void)
{
	struct trace *trace = trace_obj_get();

	if (trace->ctf_meta) {
		free(trace->ctf_meta);
		trace->ctf_meta = NULL;
	}
}

static void
meta_fix_freq(struct trace *trace, char *meta)
{
	char *str;
	int rc;

	str = RTE_PTR_ADD(meta, trace->ctf_meta_offset_freq);
	rc = sprintf(str, "%20"PRIu64"", rte_get_timer_hz());
	str[rc] = ';';
}

static void
meta_fix_freq_offset(struct trace *trace, char *meta)
{
	uint64_t uptime_tickes_floor, uptime_ticks, freq, uptime_sec;
	uint64_t offset, offset_s;
	char *str;
	int rc;

	uptime_ticks = trace->uptime_ticks &
			((1ULL << __RTE_TRACE_EVENT_HEADER_ID_SHIFT) - 1);
	freq = rte_get_tsc_hz();
	uptime_tickes_floor = RTE_ALIGN_MUL_FLOOR(uptime_ticks, freq);

	uptime_sec = uptime_tickes_floor / freq;
	offset_s = trace->epoch_sec - uptime_sec;

	offset = uptime_ticks - uptime_tickes_floor;
	offset += trace->epoch_nsec * (freq / NSEC_PER_SEC);

	str = RTE_PTR_ADD(meta, trace->ctf_meta_offset_freq_off_s);
	rc = sprintf(str, "%20"PRIu64"", offset_s);
	str[rc] = ';';
	str = RTE_PTR_ADD(meta, trace->ctf_meta_offset_freq_off);
	rc = sprintf(str, "%20"PRIu64"", offset);
	str[rc] = ';';
}

static void
meta_fixup(struct trace *trace, char *meta)
{
	meta_fix_freq(trace, meta);
	meta_fix_freq_offset(trace, meta);
}

int
rte_trace_metadata_dump(FILE *f)
{
	struct trace *trace = trace_obj_get();
	char *ctf_meta = trace->ctf_meta;
	int rc;

	if (!rte_trace_is_enabled())
		return 0;

	if (ctf_meta == NULL)
		return -EINVAL;

	if (!__atomic_load_n(&trace->ctf_fixup_done, __ATOMIC_SEQ_CST) &&
				rte_get_timer_hz()) {
		meta_fixup(trace, ctf_meta);
		__atomic_store_n(&trace->ctf_fixup_done, 1, __ATOMIC_SEQ_CST);
	}

	rc = fprintf(f, "%s", ctf_meta);
	return rc < 0 ? rc : 0;
}

char *trace_metadata_fixup_field(const char *field)
{
	const char *ctf_reserved_words[] = {
		"align",
		"event",
	};
	unsigned int i;
	char *out;
	char *p;

	/* reserved keywords */
	for (i = 0; i < RTE_DIM(ctf_reserved_words); i++) {
		if (strcmp(field, ctf_reserved_words[i]) != 0)
			continue;
		if (asprintf(&out, "_%s", ctf_reserved_words[i]) == -1)
			out = NULL;
		return out;
	}

	/* nothing to replace, return early */
	if (strstr(field, ".") == NULL && strstr(field, "->") == NULL)
		return NULL;

	out = strdup(field);
	if (out == NULL)
		return NULL;
	p = out;
	while ((p = strstr(p, ".")) != NULL) {
		p[0] = '_';
		p++;
	}
	p = out;
	while ((p = strstr(p, "->")) != NULL) {
		p[0] = '_';
		p++;
		memmove(p, p + 1, strlen(p));
	}
	return out;
}
