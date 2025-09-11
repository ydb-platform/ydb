#pragma once
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NPQ {

ui32 GetMaxHeaderSize();

NKikimrPQ::TBatchHeader ExtractHeader(const char* buffer, ui32 size);

}// NPQ
}// NKikimr
