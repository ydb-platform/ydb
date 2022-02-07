#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/library/yql/dq/common/dq_value.h>


namespace NKikimr::NKqp {

void BuildLocks(NKikimrMiniKQL::TResult& result, const TVector<NKikimrTxDataShard::TLock>& locks);

TMap<ui64, TVector<NKikimrTxDataShard::TLock>> ExtractLocks(const TVector<NYql::NDq::TMkqlValueRef>& locks);

} // namespace NKikimr::NKqp
