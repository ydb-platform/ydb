#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/ptr.h>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorHeartbeatTabletPipe : public TSimpleRefCount<TCdcAggregatorHeartbeatTabletPipe> {
public:
    using TPtr = TIntrusivePtr<TCdcAggregatorHeartbeatTabletPipe>;

    explicit TCdcAggregatorHeartbeatTabletPipe(ui64 tabletId);

    ui64 GetTabletId() const {
        return TabletId;
    }

    void Send(THolder<NActors::IEventBase> event);
    bool Reset(const NActors::TActorId& lostPipe);
    void Close();

private:
    const ui64 TabletId;
    NActors::TActorId Pipe;
};

} // namespace NKikimr::NCdcAggregator
