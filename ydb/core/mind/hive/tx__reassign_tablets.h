#pragma once

#include "hive_impl.h"
#include "hive_log.h"

#include <variant>

namespace NKikimr::NHive {

class TTxReassignTablets : public TTransactionBase<THive>, public TLoggedTransaction {
protected:
    struct TTabletFilter {
        struct TAllTablets {};

        std::variant<std::monostate, TTabletId, TAllTablets> TabletId = TAllTablets{};
        TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;

        bool operator()(const TLeaderTabletInfo* tablet) const;
        bool IsValidFilter(TString& error) const;

        template <typename TStream>
        void ToString(TStream& out) const {
            if (std::holds_alternative<TTabletId>(TabletId)) {
                out << std::get<TTabletId>(TabletId) << ", ";
            }
            if (TabletType != TTabletTypes::TypeInvalid) {
                out << TTabletTypes::EType_Name(TabletType) << ", ";
            }
        }
    };

    const TActorId Source;
    ui64 Cookie = 0;
    ui64 SelectedTablets = 0;
    TSubActorId ReassignId = 0;
    TTabletFilter TabletFilter;
    TVector<ui32> TabletChannels;
    ui32 GroupId = 0;
    TString StoragePool;
    TVector<ui32> ForcedGroupIds;
    TString Error;
    bool Async = false;
    ui64 MaxInFlight = 1;
    ui32 TabletPercent = 100;
    std::unique_ptr<IReassignCallback> Callback;
    TStringBuf LogDescription = "Hive API: reassign requested";

    TTxReassignTablets(const TActorId& source, THive* hive, TString user);

    virtual bool CheckTabletsCount(ui64) {
        return true;
    }

private:
    TInstant GetMaxTimestamp(const TReassignOperation& operation) const;

public:
    TTxReassignTablets(TEvReassignTablets::TEvCreateRequest::TPtr& ev, THive* hive);

    TTxType GetTxType() const override { return NHive::TXTYPE_REASSIGN_TABLETS; }
    bool Execute(TTransactionContext& txc, const TActorContext&) override;
    void Complete(const TActorContext& ctx) override;
};

} // namespace NKikimr::NHive
