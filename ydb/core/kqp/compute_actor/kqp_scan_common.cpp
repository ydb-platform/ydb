#include "kqp_scan_common.h"

namespace NKikimr::NKqp::NScanPrivate {

bool IsDebugLogEnabled(const NActors::TActorSystem* actorSystem, NActors::NLog::EComponent component) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, component);
}

}
