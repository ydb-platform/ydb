#pragma once

#include <ydb/library/actors/core/event.h>

namespace NActors {

    enum class ENodeClass {
        SYSTEM,
        LOCAL_TENANT,
        PEER_TENANT,
        COUNT
    };

    class TEventFilter : TNonCopyable {
        using TRouteMask = ui16;

        TVector<TVector<TRouteMask>> ScopeRoutes;

    public:
        TEventFilter()
            : ScopeRoutes(65536)
        {}

        void RegisterEvent(ui32 type, TRouteMask routes) {
            auto& evSpaceIndex = ScopeRoutes[type >> 16];
            const ui16 subtype = type & 65535;
            size_t size = (subtype + 512) & ~511;
            if (evSpaceIndex.size() < size) {
                evSpaceIndex.resize(size);
            }
            evSpaceIndex[subtype] = routes;
        }

        bool CheckIncomingEvent(const IEventHandle& ev, const TScopeId& localScopeId) const {
            TRouteMask routes = 0;
            if (const auto& evSpaceIndex = ScopeRoutes[ev.Type >> 16]) {
                const ui16 subtype = ev.Type & 65535;
                routes = subtype < evSpaceIndex.size() ? evSpaceIndex[subtype] : 0;
            } else {
                routes = ~TRouteMask(); // allow unfilled event spaces by default
            }
            return routes & MakeRouteMask(GetNodeClass(ev.OriginScopeId, localScopeId), GetNodeClass(localScopeId, ev.OriginScopeId));
        }

        static ENodeClass GetNodeClass(const TScopeId& scopeId, const TScopeId& localScopeId) {
            if (scopeId.first == 0) {
                // system scope, or null scope
                return scopeId.second ? ENodeClass::SYSTEM : ENodeClass::COUNT;
            } else if (scopeId == localScopeId) {
                return ENodeClass::LOCAL_TENANT;
            } else {
                return ENodeClass::PEER_TENANT;
            }
        }

        static TRouteMask MakeRouteMask(ENodeClass from, ENodeClass to) {
            if (from == ENodeClass::COUNT || to == ENodeClass::COUNT) {
                return 0;
            }
            return 1U << (static_cast<unsigned>(from) * static_cast<unsigned>(ENodeClass::COUNT) + static_cast<unsigned>(to));
        }

        static TRouteMask MakeRouteMask(std::initializer_list<std::pair<ENodeClass, ENodeClass>> items) {
            TRouteMask mask = 0;
            for (const auto& p : items) {
                mask |= MakeRouteMask(p.first, p.second);
            }
            return mask;
        }
    };

} // NActors
