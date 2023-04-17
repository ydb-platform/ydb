#pragma once

class TFaultyPDiskActor : public TActor<TFaultyPDiskActor> {
    TActorId PDiskId;
    ui32 Counter;
    TManualEvent *Event;

public:
    TFaultyPDiskActor(const TActorId& pdiskId, ui32 counter, TManualEvent *event)
        : TActor<TFaultyPDiskActor>(&TFaultyPDiskActor::StateFunc)
        , PDiskId(pdiskId)
        , Counter(counter)
        , Event(event)
    {}

    STFUNC(StateFunc) {
        if (!Counter--) {
            Event->Signal();
            PassAway();
        } else {
            Forward(ev, PDiskId);
        }
    }
};
