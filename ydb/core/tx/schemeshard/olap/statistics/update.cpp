#include "update.h"

namespace NKikimr::NSchemeShard {

void TOlapStatisticsUpsert::SerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& requestedProto) const {
    requestedProto.SetName(Name);
    Constructor.SerializeToProto(requestedProto);
}

bool TOlapStatisticsUpsert::DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) {
    Name = proto.GetName();
    AFL_VERIFY(Constructor.DeserializeFromProto(proto))("incorrect_proto", proto.DebugString());
    return true;
}

bool TOlapStatisticsModification::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
    for (const auto& name : alterRequest.GetDropStatistics()) {
        if (!Drop.emplace(name).second) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "Duplicated statistics for drop");
            return false;
        }
    }
    TSet<TString> upsertNames;
    for (auto& schema : alterRequest.GetUpsertStatistics()) {
        TOlapStatisticsUpsert stat;
        AFL_VERIFY(stat.DeserializeFromProto(schema));
        if (!upsertNames.emplace(stat.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "stat '" << stat.GetName() << "' duplication for add");
            return false;
        }
        Upsert.emplace_back(std::move(stat));
    }
    return true;
}
}
