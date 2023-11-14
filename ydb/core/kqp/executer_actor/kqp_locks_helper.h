#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/library/yql/dq/common/dq_value.h>


namespace NKikimr::NKqp {

void BuildLocks(NKikimrMiniKQL::TResult& result, const TVector<NKikimrDataEvents::TLock>& locks);

NKikimrDataEvents::TLock ExtractLock(const NYql::NDq::TMkqlValueRef& lock);

} // namespace NKikimr::NKqp
