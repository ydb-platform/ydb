#pragma once

#include "misc/async_updater.h"
#include "misc/checker.h"
#include "misc/default_uid_checker.h"
#include "misc/getter.h"
#include "misc/src_checker.h"

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

        TString GetServiceTicketFor(const TClientSettings::TAlias& dst) const override {
            auto c = GetCachedServiceTickets();
            return TServiceTicketGetter::GetTicket(dst, c);
        }

        TString GetServiceTicketFor(const TTvmId dst) const override {
            auto c = GetCachedServiceTickets();
            return TServiceTicketGetter::GetTicket(dst, c);
        }

        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket, const TServiceContext::TCheckFlags& flags) const override {
            TServiceContextPtr c = GetCachedServiceContext();
            TCheckedServiceTicket res = TServiceTicketChecker::Check(ticket, c, flags);

            if (Roles_ && res) {
                NRoles::TRolesPtr roles = GetRoles();
                return TSrcChecker::Check(std::move(res), roles);
            }

            return res;
        }

        TCheckedUserTicket CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overridenEnv) const override {
            auto c = GetCachedUserContext(overridenEnv);
            TCheckedUserTicket res = TUserTicketChecker::Check(ticket, c);

            if (Roles_ && res && res.GetEnv() == EBlackboxEnv::ProdYateam) {
                NRoles::TRolesPtr roles = GetRoles();
                return TDefaultUidChecker::Check(std::move(res), roles);
            }
            return res;
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
