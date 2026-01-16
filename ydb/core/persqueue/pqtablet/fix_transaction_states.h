#pragma once

#include "transaction.h"

#include <ydb/core/protos/msgbus_kv.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ {

bool IsMainContextOfTransaction(const TString& key);
void FixTransactionStates(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                          THashMap<ui64, TDistributedTransaction>& txs);

}
