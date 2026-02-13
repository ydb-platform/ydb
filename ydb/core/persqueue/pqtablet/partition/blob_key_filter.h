#pragma once

#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

THashSet<TString> FilterBlobsMetaData(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& ranges,
                                      const TPartitionId& partitionId);

}
