#pragma once
#include <ydb/core/ymq/actor/cfg/defs.h>
#include <ydb/core/protos/sqs.pb.h>

namespace NKikimr::NSQS {

TString CalcMD5OfMessageAttributes(const google::protobuf::RepeatedPtrField<TMessageAttribute>& attributes);

} // namespace NKikimr::NSQS
