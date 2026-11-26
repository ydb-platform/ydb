#pragma once

#include "table_checksum_events.h"

#include <ydb/library/actors/core/actor.h>

#include <functional>

namespace NKikimr::NBackup {

class ITableChecksumJob {
public:
    virtual ~ITableChecksumJob() = default;

    virtual bool Handle(NActors::IEventHandle::TPtr& ev) = 0;
    virtual void Stop() = 0;
};

struct TChecksumMailboxAliases {
    std::function<NActors::TActorId()> Register;
    std::function<void(NActors::TActorId)> Unregister;
};

class ITableChecksumFactory {
public:
    virtual ~ITableChecksumFactory() = default;

    virtual std::unique_ptr<ITableChecksumJob> StartExport(
        TEvTableChecksum::TItem item, TChecksumMailboxAliases aliases) const = 0;
    virtual NActors::IActor* CreateImportActor() const = 0;
};

const ITableChecksumFactory& GetTableChecksumFactory();

} // namespace NKikimr::NBackup
