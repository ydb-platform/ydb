#include "poller_tcp.h"

namespace NInterconnect {
    TPollerThreads::TPollerThreads(size_t units, bool useSelect)
        : Units(units)
    {
        Y_DEBUG_ABORT_UNLESS(!Units.empty());
        for (auto& unit : Units)
            unit = TPollerUnit::Make(useSelect);
    }

    TPollerThreads::~TPollerThreads() {
    }

    void TPollerThreads::Start() {
        for (const auto& unit : Units)
            unit->Start();
    }

    void TPollerThreads::Stop() {
        for (const auto& unit : Units)
            unit->Stop();
    }

    void TPollerThreads::StartRead(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) {
        auto& unit = Units[THash<SOCKET>()(s->GetDescriptor()) % Units.size()];
        unit->StartReadOperation(s, std::move(operation));
    }

    void TPollerThreads::StartWrite(const TIntrusivePtr<TSharedDescriptor>& s, TFDDelegate&& operation) {
        auto& unit = Units[THash<SOCKET>()(s->GetDescriptor()) % Units.size()];
        unit->StartWriteOperation(s, std::move(operation));
    }

}
