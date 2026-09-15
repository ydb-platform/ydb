#pragma once
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NPQ {

void InitMaxHeaderSize(const NKikimrConfig::TFeatureFlags& featureFlags);
ui32 GetMaxHeaderSize();

NKikimrPQ::TBatchHeader ExtractHeader(const char* buffer, ui32 size);

}// NPQ
}// NKikimr
