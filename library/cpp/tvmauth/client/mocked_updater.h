#pragma once

#include "misc/async_updater.h"

namespace NTvmAuth {
    class TMockedUpdater: public TAsyncUpdaterBase {
    public:
        struct TSettings {
            struct TTuple {
                TClientSettings::TAlias Alias;
                TTvmId Id = 0;
                TString Value; // ticket or error
            };

            TTvmId SelfTvmId = 0;
            TVector<TTuple> Backends;
            TVector<TTuple> BadBackends;
            EBlackboxEnv UserTicketEnv = EBlackboxEnv::Test;
            NRoles::TRolesPtr Roles;

            static TSettings CreateDeafult();
        };

        TMockedUpdater(const TSettings& settings = TSettings::CreateDeafult());

        TClientStatus GetStatus() const override {
            return TClientStatus();
        }

        NRoles::TRolesPtr GetRoles() const override {
            Y_ENSURE_EX(Roles_, TIllegalUsage() << "Roles are not provided");
            return Roles_;
        }

        using TAsyncUpdaterBase::SetServiceContext;
        using TAsyncUpdaterBase::SetServiceTickets;
        using TAsyncUpdaterBase::SetUpdateTimeOfPublicKeys;
        using TAsyncUpdaterBase::SetUpdateTimeOfServiceTickets;
        using TAsyncUpdaterBase::SetUserContext;

    protected:
        NRoles::TRolesPtr Roles_;
    };
}
