#pragma once

#include <util/system/types.h>
#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NYql {

constexpr size_t YQL_JOB_CODEC_BLOCK_COUNT = 16;
constexpr size_t YQL_JOB_CODEC_BLOCK_SIZE = 1_MB;

constexpr size_t YQL_JOB_CODEC_MEM = YQL_JOB_CODEC_BLOCK_COUNT * YQL_JOB_CODEC_BLOCK_SIZE + (30_MB);

constexpr ui64 DEFAULT_TOP_SORT_LIMIT = 1000ULL;

constexpr ui16 DEFAULT_WIDE_FLOW_LIMIT = 101U;

constexpr bool DEFAULT_USE_FLOW = true;

constexpr bool DEFAULT_USE_NATIVE_YT_TYPES = false;

constexpr bool DEFAULT_USE_INTERMEDIATE_SCHEMA = true;

constexpr bool DEFAULT_USE_SKIFF = true;

constexpr bool DEFAULT_USE_SYS_COLUMNS = true;

constexpr bool DEFAULT_USE_MULTISET_ATTRS = true;

constexpr bool DEFAULT_MAP_JOIN_USE_FLOW = true;

constexpr bool DefaultHybridDqExecution = false;
constexpr auto DefaultHybridDqDataSizeLimitForOrdered = 1_GB;
constexpr auto DefaultHybridDqDataSizeLimitForUnordered = 16_GB;

constexpr bool DEFAULT_ROW_SPEC_COMPACT_FORM = false;

constexpr bool DEFAULT_USE_NEW_PREDICATE_EXTRACTION = false;
constexpr bool DEFAULT_PRUNE_KEY_FILTER_LAMBDA = false;
constexpr bool DEFAULT_DQ_PRUNE_KEY_FILTER_LAMBDA = false;
constexpr bool DEFAULT_MERGE_ADJACENT_POINT_RANGES = true;
constexpr bool DEFAULT_KEY_FILTER_FOR_STARTS_WITH = true;
constexpr ui64 DEFAULT_MAX_KEY_RANGE_COUNT = 1000ULL;

constexpr bool DEFAULT_USE_NATIVE_DESC_SORT = false;

constexpr ui64 DEFAULT_MAX_CHUNKS_FOR_DQ_READ = 500;

constexpr bool DEFAULT_USE_KEY_BOUND_API = false;

constexpr ui32 DEFAULT_MAX_OPERATION_FILES = 1000;

constexpr bool DEFAULT_JOIN_COMMON_USE_MULTI_OUT = false;

constexpr bool DEFAULT_USE_RPC_READER_IN_DQ = false;
constexpr size_t DEFAULT_RPC_READER_INFLIGHT = 1;
constexpr TDuration DEFAULT_RPC_READER_TIMEOUT = TDuration::Seconds(120);


constexpr auto DEFAULT_SWITCH_MEMORY_LIMIT = 128_MB;

constexpr ui32 DEFAULT_MAX_INPUT_TABLES = 3000;
constexpr ui32 DEFAULT_MAX_OUTPUT_TABLES = 100;
constexpr ui64 DEFAULT_APPLY_STORED_CONSTRAINTS = 0ULL;

} // NYql
