#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NReplication::NController {

class TController;

class TTabletLogPrefix {
public:
    explicit TTabletLogPrefix(const TController* self);
    explicit TTabletLogPrefix(const TController* self, const TString& txName);

    void Out(IOutputStream& out) const;

private:
    const ui64 TabletId;
    const TString TxName;
};

class TActorLogPrefix {
public:
    TActorLogPrefix() = default;
    explicit TActorLogPrefix(const TString& activity, ui64 rid = 0, ui64 tid = 0);

    void Out(IOutputStream& out) const;

private:
    const TString Activity;
    const ui64 ReplicationId = 0;
    const ui64 TargetId = 0;
};

}
