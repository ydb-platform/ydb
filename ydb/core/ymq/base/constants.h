#pragma once
#include <cstddef>

#include <util/generic/string.h>

#define INFLY_LIMIT 120000

namespace NKikimr::NSQS {

constexpr size_t MAX_SHARDS_COUNT = 32;
constexpr size_t MAX_PARTITIONS_COUNT = 128;

static const TString yaSqsArnPrefix = "yrn:ya:sqs";
static const TString cloudArnPrefix = "yrn:yc:ymq";

} // namespace NKikimr::NSQS
