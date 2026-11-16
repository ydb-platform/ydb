#include "data_center_states.h"

#include <util/string/builder.h>

namespace NKikimr {
namespace {

using TDataCenter = NKikimrBlobStorage::TDataCenterState;

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
            case TDataCenter::RECOVERING:
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

} // namespace NKikimr
