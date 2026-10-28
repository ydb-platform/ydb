#include "data_center_states.h"

#include <util/string/builder.h>

#include <algorithm>
#include <optional>

namespace NKikimr {
namespace {

using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using TDataCenter = NKikimrBlobStorage::TDefineDataCenterStates::TDataCenter;

bool IsIncluded(TFailRealm::EState state) {
    return state == TFailRealm::Invariant0 || state == TFailRealm::Invariant1 || state == TFailRealm::Invariant2;
}

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
    for (ui32 realm = 0; realm < 3; ++realm) {
        if (!TFailRealm::EState_IsValid(before[realm]) || !TFailRealm::EState_IsValid(after[realm])) {
            return std::unexpected<TString>(TStringBuilder() << "unknown state for realm# " << realm);
        }
    }
    if (before == after) { // Table 1, row 9.
        return {};
    }
    const ui32 excluded = std::count(after.begin(), after.end(), TFailRealm::Excluded);
    const ui32 recovering = std::count(after.begin(), after.end(), TFailRealm::Recovering);
    const bool allInvariant0 = std::all_of(after.begin(), after.end(), [](TFailRealm::EState state) {
        return state == TFailRealm::Invariant0;
    });
    bool sameIncludedStates = true;
    std::optional<TFailRealm::EState> includedState;
    for (const TFailRealm::EState state : after) {
        if (IsIncluded(state)) {
            if (includedState && *includedState != state) {
                sameIncludedStates = false;
            }
            includedState = state;
        }
    }
    for (ui32 realm = 0; realm < 3; ++realm) {
        const TFailRealm::EState previous = before[realm];
        const TFailRealm::EState target = after[realm];
        if (previous == target) {
            continue;
        }
        bool allowed = false;
        if (IsIncluded(previous) && target == TFailRealm::Excluded) { // Row 1: sufficient surviving copy guarantees.
            ui32 invariant0 = 0;
            ui32 guaranteed = 0;
            for (ui32 other = 0; other < 3; ++other) {
                if (other != realm) {
                    invariant0 += after[other] == TFailRealm::Invariant0;
                    guaranteed += after[other] == TFailRealm::Invariant1 || after[other] == TFailRealm::Invariant2;
                }
            }
            allowed = guaranteed || invariant0 == 2;
        } else if (previous == TFailRealm::Excluded && target == TFailRealm::Recovering) { // Row 2: check the initial state.
            // Recovery is sequential, including when several realms are listed in one command.
            allowed = std::count(before.begin(), before.end(), TFailRealm::Recovering) == 0 && recovering == 1;
        } else if (previous == TFailRealm::Recovering && target == TFailRealm::Excluded) { // Row 3.
            allowed = true;
        } else if ((previous == TFailRealm::Recovering || previous == TFailRealm::Invariant1 || previous == TFailRealm::Invariant2)
                && target == TFailRealm::Invariant0) { // Rows 4 and 8: all realms return to Invariant0 together.
            allowed = allInvariant0;
        } else if ((previous == TFailRealm::Recovering || previous == TFailRealm::Invariant0 || previous == TFailRealm::Invariant1)
                && (target == TFailRealm::Invariant1 || target == TFailRealm::Invariant2)) { // Rows 5, 6 and 7.
            allowed = excluded == 1 && sameIncludedStates;
        }
        if (!allowed) {
            return std::unexpected<TString>(TStringBuilder() << "invalid state transition for realm# " << realm
                << " from " << TFailRealm::EState_Name(previous) << " to " << TFailRealm::EState_Name(target));
        }
    }
    return {};
}

} // namespace NKikimr
