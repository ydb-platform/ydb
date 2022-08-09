#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>

namespace NPG {

enum EService : NActors::NLog::EComponent {
    MIN = 1000,
    PGWIRE,
    MAX
};

inline TString LogPrefix() { return {}; }

inline const TString& GetEServiceName(NActors::NLog::EComponent component) {
    static const TString pgwireName("PGWIRE");
    static const TString unknownName("UNKNOWN");
    switch (component) {
    case EService::PGWIRE:
        return pgwireName;
    default:
        return unknownName;
    }
}

}
