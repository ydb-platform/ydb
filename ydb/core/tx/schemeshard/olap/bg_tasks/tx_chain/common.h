#pragma once
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TTxChainData {
private:
    YDB_READONLY_DEF(TString, TablePath);
    YDB_ACCESSOR_DEF(std::vector<NKikimrSchemeOp::TModifyScheme>, Transactions);
public:
    using TProtoStorage = NKikimrSchemeShardTxBackgroundProto::TTxChainCommonData;
    TConclusionStatus DeserializeFromProto(const TProtoStorage& proto);
    TProtoStorage SerializeToProto() const;
};

}