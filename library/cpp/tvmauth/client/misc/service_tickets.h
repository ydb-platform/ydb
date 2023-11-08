#pragma once

#include "settings.h"
#include "roles/roles.h"

#include <library/cpp/tvmauth/src/utils.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NTvmAuth::NInternal {
    class TClientCaningKnife;
}

namespace NTvmAuth {
    class TServiceTickets: public TAtomicRefCount<TServiceTickets> {
    public:
        using TMapAliasStr = THashMap<TClientSettings::TAlias, TString>;
        using TMapIdStr = THashMap<TTvmId, TString>;
        using TIdSet = THashSet<TTvmId>;
        using TAliasSet = THashSet<TClientSettings::TAlias>;
        using TMapAliasId = THashMap<TClientSettings::TAlias, TTvmId>;

        TServiceTickets(TMapIdStr&& tickets, TMapIdStr&& errors, const TMapAliasId& dstMap)
            : TicketsById(std::move(tickets))
            , ErrorsById(std::move(errors))
        {
            InitAliasesAndUnfetchedIds(dstMap);
            InitInvalidationTime();
        }

        static TInstant GetInvalidationTime(const TMapIdStr& ticketsById) {
            TInstant res;

            for (const auto& pair : ticketsById) {
                TMaybe<TInstant> t = NTvmAuth::NInternal::TCanningKnife::GetExpirationTime(pair.second);
                if (!t) {
                    continue;
                }

                res = res == TInstant() ? *t : std::min(res, *t);
            }

            return res;
        }

    public:
        TMapIdStr TicketsById;
        TMapIdStr ErrorsById;
        TMapAliasStr TicketsByAlias;
        TMapAliasStr ErrorsByAlias;
        TInstant InvalidationTime;
        TIdSet UnfetchedIds;
        TAliasSet UnfetchedAliases;

    private:
        void InitAliasesAndUnfetchedIds(const TMapAliasId& dstMap) {
            for (const auto& pair : dstMap) {
                auto it = TicketsById.find(pair.second);
                auto errIt = ErrorsById.find(pair.second);

                if (it == TicketsById.end()) {
                    if (errIt != ErrorsById.end()) {
                        Y_ENSURE(ErrorsByAlias.insert({pair.first, errIt->second}).second,
                                 "failed to add: " << pair.first);
                    } else {
                        UnfetchedAliases.insert(pair.first);
                        UnfetchedIds.insert(pair.second);
                    }
                } else {
                    Y_ENSURE(TicketsByAlias.insert({pair.first, it->second}).second,
                             "failed to add: " << pair.first);
                }
            }
        }

        void InitInvalidationTime() {
            InvalidationTime = GetInvalidationTime(TicketsById);
        }
    };

    using TServiceTicketsPtr = TIntrusiveConstPtr<TServiceTickets>;
}
