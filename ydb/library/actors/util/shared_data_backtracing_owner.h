#pragma once

#include <util/system/sys_alloc.h>
#include <util/system/backtrace.h>

#include "shared_data.h"

class TBackTracingOwner : public NActors::TSharedData::IOwner {
    using THeader = NActors::TSharedData::THeader;
    using TSelf = TBackTracingOwner;
    using IOwner = NActors::TSharedData::IOwner;

    static constexpr size_t PrivateHeaderSize = NActors::TSharedData::PrivateHeaderSize;
    static constexpr size_t HeaderSize = NActors::TSharedData::HeaderSize;
    static constexpr size_t OverheadSize = NActors::TSharedData::OverheadSize;

    IOwner* RealOwner = nullptr;
    TBackTrace BackTrace;
    const char* Info;
public:

    static constexpr const char* INFO_FROM_SHARED_DATA = "FROM_SHARED_DATA";
    static constexpr const char* INFO_COPIED_STRING = "COPIED_STRING";
    static constexpr const char* INFO_ALLOC_UNINITIALIZED = "ALLOC_UNINITIALIZED";
    static constexpr const char* INFO_ALLOC_UNINIT_ROOMS = "ALLOC_UNINIT_ROOMS";

    static char* Allocate(size_t size, const char* info = nullptr) {
        char* raw = reinterpret_cast<char*>(y_allocate(OverheadSize + size));
        THeader* header = reinterpret_cast<THeader*>(raw + PrivateHeaderSize);
        TSelf* btOwner = new TSelf;
        btOwner->BackTrace.Capture();
        btOwner->Info = info;
        new (header) THeader(btOwner);
        char* data = raw + OverheadSize;
        return data;
    }

    static void FakeOwner(const NActors::TSharedData& data, const char* info = nullptr) {
        THeader* header = Header(data);
        if (header) {
            TSelf* btOwner = new TSelf();
            btOwner->BackTrace.Capture();
            btOwner->Info = info;
            if (header->Owner) {
                btOwner->RealOwner = header->Owner;
            }
            header->Owner = btOwner;
        }
    }

    static void UnsafePrintBackTrace(NActors::TSharedData& data) {
        THeader* header = Header(data);
        if(header->Owner) {
            TSelf* owner = static_cast<TSelf*>(header->Owner);
            owner->PrintBackTrace();
        }
    }

    void Deallocate(char* data) noexcept override {
        if (!RealOwner) {
            THeader* header = reinterpret_cast<THeader*>(data - sizeof(THeader));
            header->~THeader();
            char* raw = data - OverheadSize;
            y_deallocate(raw);
        } else {
            RealOwner->Deallocate(data);
        }

        delete this;
    }

    IOwner* GetRealOwner() const {
        return RealOwner;
    }

    void PrintBackTrace() {
        Cerr << "Deallocate TSharedData with info# " << Info << Endl;
        BackTrace.PrintTo(Cerr);
    }
private:
    static Y_FORCE_INLINE THeader* Header(const NActors::TSharedData& d) noexcept {
        char* data = const_cast<char*>(d.data());
        if (data) {
            return reinterpret_cast<THeader*>(data - HeaderSize);
        } else {
            return nullptr;
        }
    }
};
