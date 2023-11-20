#pragma once

#include <util/system/yassert.h>

namespace NKikimr::NPDisk {

class ISpdkState {
public:
    virtual void LaunchThread(int (*fn)(void *), void *cookie) = 0;
    virtual ui8 *Malloc(ui64 size, ui32 align) = 0;
    virtual void Free(ui8 *buff) = 0;
    //virtual ui64 GetDeviceSize() = 0;
    virtual void WaitAllThreads() = 0;
    virtual ~ISpdkState() {};
};

class TSpdkStateOSS : public ISpdkState {
public:
    TSpdkStateOSS() {}

    void LaunchThread(int (*)(void *), void *) override {
         Y_ABORT("Spdk is not supported now");
    }

    ui8 *Malloc(ui64, ui32) override {
         Y_ABORT("Spdk is not supported now");
    }

    void Free(ui8 *) override {
         Y_ABORT("Spdk is not supported now");
    }

    //ui64 GetDeviceSize() override {
    //     Y_ABORT("Spdk is not supported now");
    //}

    void WaitAllThreads() override {
         Y_ABORT("Spdk is not supported now");
    }
};

}
