#pragma once

#include "transaction.h"

#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ {

bool IsMainContextOfTransaction(const TString& key);
THashMap<ui64, NKikimrPQ::TTransaction> CollectTransactions(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges);

}
