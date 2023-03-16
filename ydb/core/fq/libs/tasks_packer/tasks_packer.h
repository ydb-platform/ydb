#pragma once

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NFq {

namespace NTasksPacker {

void Pack(TVector<NYql::NDqProto::TDqTask>& tasks, THashMap<i64, TString>& stagePrograms);
void UnPack(TVector<NYql::NDqProto::TDqTask>& tasks, const THashMap<i64, TString>& stagePrograms);
void UnPack(
    google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src,
    const google::protobuf::Map<i64, TString>& stagePrograms);

} // namespace NTasksPacker

} // namespace NFq
