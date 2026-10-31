#include "data_center_states.h"

#include <util/string/builder.h>

namespace NKikimr {
namespace {

using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using TDataCenter = NKikimrBlobStorage::TDefineDataCenterStates::TDataCenter;

struct TRealmStats {
    ui32 NumInvariant0 = 0;
    ui32 NumInvariant1 = 0;
    ui32 NumInvariant2 = 0;
    ui32 NumRecovering = 0;
    ui32 NumExcluded = 0;
    ui32 NumDistinctIncludedStates = 0;

    explicit TRealmStats(const TFailRealmStates& states) {
        for (const TFailRealm::EState state : states) {
            NumInvariant0 += state == TFailRealm::Invariant0;
            NumInvariant1 += state == TFailRealm::Invariant1;
            NumInvariant2 += state == TFailRealm::Invariant2;
            NumRecovering += state == TFailRealm::Recovering;
            NumExcluded += state == TFailRealm::Excluded;
        }
        NumDistinctIncludedStates = (NumInvariant0 != 0) + (NumInvariant1 != 0) + (NumInvariant2 != 0);
    }
};

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
    const TRealmStats statsBefore(before);
    const TRealmStats statsAfter(after);

    const auto validateRealm = [&](ui32 realm) -> std::expected<void, TString> {
        const TFailRealm::EState from = before[realm];
        const TFailRealm::EState to = after[realm];
        const auto expect = [&](bool condition) -> std::expected<void, TString> {
            if (condition) {
                return {};
            }
            return std::unexpected<TString>(TStringBuilder() << "invalid state transition for realm# " << realm
                << " from " << TFailRealm::EState_Name(from) << " to " << TFailRealm::EState_Name(to));
        };

        // RFC 190, Table 1, row 1.
        if (IsIncluded(from) && to == TFailRealm::Excluded) {
            return expect(statsAfter.NumInvariant1 || statsAfter.NumInvariant2 || statsAfter.NumInvariant0 == 2);
        }
        // RFC 190, Table 1, row 2.
        if (from == TFailRealm::Excluded && to == TFailRealm::Recovering) {
            return expect(statsBefore.NumRecovering == 0 && statsAfter.NumRecovering == 1);
        }
        // RFC 190, Table 1, row 3.
        if (from == TFailRealm::Recovering && to == TFailRealm::Excluded) {
            return {};
        }
        // RFC 190, Table 1, row 4.
        if (from == TFailRealm::Recovering && to == TFailRealm::Invariant0) {
            return expect(statsAfter.NumInvariant0 == 3);
        }
        // RFC 190, Table 1, row 5.
        if (from == TFailRealm::Recovering && (to == TFailRealm::Invariant1 || to == TFailRealm::Invariant2)) {
            return expect(statsAfter.NumExcluded == 1 && statsAfter.NumDistinctIncludedStates == 1);
        }
        // RFC 190, Table 1, row 6.
        if (from == TFailRealm::Invariant0 && (to == TFailRealm::Invariant1 || to == TFailRealm::Invariant2)) {
            return expect(statsAfter.NumExcluded == 1 && statsAfter.NumDistinctIncludedStates == 1);
        }
        // RFC 190, Table 1, row 7.
        if (from == TFailRealm::Invariant1 && to == TFailRealm::Invariant2) {
            return expect(statsAfter.NumExcluded == 1 && statsAfter.NumDistinctIncludedStates == 1);
        }
        // RFC 190, Table 1, row 8.
        if ((from == TFailRealm::Invariant1 || from == TFailRealm::Invariant2) && to == TFailRealm::Invariant0) {
            return expect(statsAfter.NumInvariant0 == 3);
        }
        // RFC 190, Table 1, row 9.
        if (from == to) {
            return {};
        }
        return expect(false);
    };

    for (ui32 realm = 0; realm < 3; ++realm) {
        if (const auto result = validateRealm(realm); !result) {
            return result;
        }
    }
    return {};
}

} // namespace NKikimr
