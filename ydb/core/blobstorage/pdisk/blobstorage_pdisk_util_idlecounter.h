#pragma once

#include "defs.h"
#include "blobstorage_pdisk_mon.h"

namespace NKikimr {

namespace NPDisk {

class TIdleCounter {
    ui64 InFlight = 0; // Only accessed from TLight::Set callbacks
    TLight& IdleLight;

public:

    TIdleCounter(TLight& light)
        : IdleLight(light)
    {}

    void Increment() {
        IdleLight.Set([this] {
            Y_ABORT_UNLESS(InFlight < Max<ui64>());
            ++InFlight;
            return false;
        });
    }

    void Decrement() {
        IdleLight.Set([this] {
            Y_ABORT_UNLESS(InFlight > 0);
            return --InFlight == 0;
        });
    }
};

} // namespace NKikimr

} // namespace NPDisk
