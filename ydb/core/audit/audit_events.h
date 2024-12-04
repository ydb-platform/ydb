#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NKikimr {
    namespace NAudit {
        namespace NEvAuditLog {
            //
            // Events declaration
            //

            enum EEvents {
                EvBegin = EventSpaceBegin(TKikimrEvents::ES_YDB_AUDIT_LOG),

                // Request actors
                EvWriteAuditLog = EvBegin + 0,

                EvEnd
            };

            static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG),
                "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG)"
            );

            struct TEvWriteAuditLog : public NActors::TEventLocal<TEvWriteAuditLog, EvWriteAuditLog> {
                TInstant Time;
                TVector<std::pair<TString, TString>> Parts;

                TEvWriteAuditLog(TInstant time, TVector<std::pair<TString, TString>>&& parts)
                    : Time(time)
                    , Parts(std::move(parts))
                {}
            };
        }

        using TAuditLogItemBuilder = TString(*)(const NEvAuditLog::TEvWriteAuditLog*);

        // Registration of a function for converting audit events to a string in a specified format
        void RegisterAuditLogItemBuilder(NKikimrConfig::TAuditConfig::EFormat format, TAuditLogItemBuilder builder);
    }
}
