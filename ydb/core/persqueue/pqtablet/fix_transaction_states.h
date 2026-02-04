#pragma once

#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ {

THashMap<ui64, NKikimrPQ::TTransaction> CollectTransactions(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges);

}
