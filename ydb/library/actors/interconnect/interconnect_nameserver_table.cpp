#include "interconnect.h"
#include "interconnect_impl.h"
#include "interconnect_address.h"
#include "interconnect_nameserver_base.h"
#include "events_local.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/memory_log/memlog.h>

namespace NActors {

    class TInterconnectNameserverTable: public TInterconnectNameserverBase<TInterconnectNameserverTable> {
        TIntrusivePtr<TTableNameserverSetup> Config;

    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::NAMESERVICE;
        }

        TInterconnectNameserverTable(const TIntrusivePtr<TTableNameserverSetup>& setup, ui32 /*resolvePoolId*/)
            : TInterconnectNameserverBase<TInterconnectNameserverTable>(&TInterconnectNameserverTable::StateFunc, setup->StaticNodeTable)
            , Config(setup)
        {
            Y_ABORT_UNLESS(Config->IsEntriesUnique());
        }

        STFUNC(StateFunc) {
            try {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvInterconnect::TEvResolveNode, Handle);
                    HFunc(TEvResolveAddress, Handle);
                    HFunc(TEvInterconnect::TEvListNodes, Handle);
                    HFunc(TEvInterconnect::TEvGetNode, Handle);
                }
            } catch (...) {
                // on error - do nothing
            }
        }
    };

    IActor* CreateNameserverTable(const TIntrusivePtr<TTableNameserverSetup>& setup, ui32 poolId) {
        return new TInterconnectNameserverTable(setup, poolId);
    }

    bool TTableNameserverSetup::IsEntriesUnique() const {
        TVector<const TNodeInfo*> infos;
        infos.reserve(StaticNodeTable.size());
        for (const auto& x : StaticNodeTable)
            infos.push_back(&x.second);

        auto CompareAddressLambda =
            [](const TNodeInfo* left, const TNodeInfo* right) {
                return left->Port == right->Port ? left->Address < right->Address : left->Port < right->Port;
            };

        Sort(infos, CompareAddressLambda);

        for (ui32 idx = 1, end = StaticNodeTable.size(); idx < end; ++idx) {
            const TNodeInfo* left = infos[idx - 1];
            const TNodeInfo* right = infos[idx];
            if (left->Address && left->Address == right->Address && left->Port == right->Port)
                return false;
        }

        auto CompareHostLambda =
            [](const TNodeInfo* left, const TNodeInfo* right) {
                return left->Port == right->Port ? left->ResolveHost < right->ResolveHost : left->Port < right->Port;
            };

        Sort(infos, CompareHostLambda);

        for (ui32 idx = 1, end = StaticNodeTable.size(); idx < end; ++idx) {
            const TNodeInfo* left = infos[idx - 1];
            const TNodeInfo* right = infos[idx];
            if (left->ResolveHost == right->ResolveHost && left->Port == right->Port)
                return false;
        }

        return true;
    }

    TActorId GetNameserviceActorId() {
        return TActorId(0, "namesvc");
    }

}
