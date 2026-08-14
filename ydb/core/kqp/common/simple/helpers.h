#pragma once
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NKqp {

enum class ETableReadType {
    Other = 0,
    Scan = 1,
    FullScan = 2,
};

bool IsSqlQuery(const NKikimrKqp::EQueryType& queryType);
const char* GetTableSinkModeVerb(NKikimrKqp::TKqpTableSinkSettings::EType mode);

} // namespace NKikimr::NKqp
