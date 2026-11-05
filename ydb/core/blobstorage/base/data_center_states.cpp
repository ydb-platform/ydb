#include "data_center_states.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>

#include <util/string/builder.h>

namespace NKikimr {
namespace {

using TDataCenter = NKikimrBlobStorage::TDefineDataCenterStates::TDataCenter;

} // namespace

std::expected<TDataCenterStates, TString> ParseDataCenterStates(const NKikimrBlobStorage::TDefineDataCenterStates& command) {
    TDataCenterStates result;
    for (const auto& dataCenter : command.GetDataCenter()) {
        if (!dataCenter.HasName() || dataCenter.GetName().empty() || result.contains(dataCenter.GetName())) {
            return std::unexpected<TString>(TStringBuilder() << "empty or duplicate DataCenter# " << dataCenter.GetName());
        }
        if (!dataCenter.HasState()) {
            return std::unexpected<TString>(TStringBuilder() << "missing state for DataCenter# " << dataCenter.GetName());
        }
        switch (dataCenter.GetState()) {
            case TDataCenter::INCLUDED:
            case TDataCenter::EXCLUDED:
                result.emplace(dataCenter.GetName(), dataCenter.GetState());
                break;
            default:
                return std::unexpected<TString>(TStringBuilder() << "unknown state for DataCenter# " << dataCenter.GetName());
        }
    }
    return result;
}

NKikimrBlobStorage::TDefineDataCenterStates SerializeDataCenterStates(const TDataCenterStates& states) {
    NKikimrBlobStorage::TDefineDataCenterStates result;
    for (const auto& [name, state] : states) {
        auto* dataCenter = result.AddDataCenter();
        dataCenter->SetName(name);
        dataCenter->SetState(state);
    }
    return result;
}

std::expected<void, TString> ValidateFailRealmStateTransition(const TFailRealmStates& before, const TFailRealmStates& after) {
    if (!IsAllowedMirror3dcFailRealmStateTransition(before, after)) {
        return std::unexpected<TString>("invalid mirror-3-dc fail realm state transition");
    }
    return {};
}

} // namespace NKikimr
