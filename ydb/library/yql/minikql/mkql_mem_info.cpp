#include "mkql_mem_info.h"

#include <util/generic/yexception.h>

namespace NKikimr {
namespace NMiniKQL {

TMemoryUsageInfo::TMemoryUsageInfo(const TStringBuf& title)
    : Title_(title)
    , Allocated_(0)
    , Freed_(0)
    , Peak_(0)
    , AllowMissing_(false)
    , CheckOnExit_(true)
{
}

TMemoryUsageInfo::~TMemoryUsageInfo() {
    if (CheckOnExit_ && !UncaughtException()) {
        VerifyDebug();
    }
}

void TMemoryUsageInfo::AllowMissing() {
    AllowMissing_ = true;
}

void TMemoryUsageInfo::CheckOnExit(bool check) {
    CheckOnExit_ = check;
}

#ifndef NDEBUG
void TMemoryUsageInfo::Take(const void* mem, ui64 size, TMkqlLocation location) {
    Allocated_ += size;
    Peak_ = Max(Peak_, Allocated_ - Freed_);
    if (size == 0) {
        return;
    }
    if (AllowMissing_) {
        auto it = AllocationsMap_.find(mem);
        if (it != AllocationsMap_.end() && it->second.IsDeleted) {
            AllocationsMap_.erase(it);
        }
    }
    auto res = AllocationsMap_.insert({mem, { size, std::move(location), false }});
    Y_DEBUG_ABORT_UNLESS(res.second, "Duplicate allocation at: %p, "
                                "already allocated at: %s", mem, (TStringBuilder() << res.first->second.Location).c_str());
    //Clog << Title_ << " take: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
}
#endif

#ifndef NDEBUG
void TMemoryUsageInfo::Return(const void* mem, ui64 size) {
    Freed_ += size;
    if (size == 0) {
        return;
    }
    //Clog << Title_ << " free: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
    auto it = AllocationsMap_.find(mem);
    if (AllowMissing_ && it == AllocationsMap_.end()) {
        return;
    }

    if (AllowMissing_) {
        Y_DEBUG_ABORT_UNLESS(!it->second.IsDeleted, "Double free at: %p", mem);
    } else {
        Y_DEBUG_ABORT_UNLESS(it != AllocationsMap_.end(), "Double free at: %p", mem);
    }

    Y_DEBUG_ABORT_UNLESS(size == it->second.Size,
                "Deallocating wrong size at: %p, "
                "allocated at: %s", mem, (TStringBuilder() << it->second.Location).c_str());
    if (AllowMissing_) {
        it->second.IsDeleted = true;
    } else {
        AllocationsMap_.erase(it);
    }
}
#endif

#ifndef NDEBUG
void TMemoryUsageInfo::Return(const void* mem) {
    //Clog << Title_ << " free: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
    auto it = AllocationsMap_.find(mem);
    if (AllowMissing_ && it == AllocationsMap_.end()) {
        return;
    }

    if (AllowMissing_) {
        Y_DEBUG_ABORT_UNLESS(!it->second.IsDeleted, "Double free at: %p", mem);
    } else {
        Y_DEBUG_ABORT_UNLESS(it != AllocationsMap_.end(), "Double free at: %p", mem);
    }

    Freed_ += it->second.Size;
    if (AllowMissing_) {
        it->second.IsDeleted = true;
    } else {
        AllocationsMap_.erase(it);
    }
}
#endif

i64 TMemoryUsageInfo::GetUsage() const {
    return static_cast<i64>(Allocated_) - static_cast<i64>(Freed_);
}

ui64 TMemoryUsageInfo::GetAllocated() const { return Allocated_; }
ui64 TMemoryUsageInfo::GetFreed() const { return Freed_; }
ui64 TMemoryUsageInfo::GetPeak() const { return Peak_; }

void TMemoryUsageInfo::PrintTo(IOutputStream& out) const {
    out << Title_ << TStringBuf(": usage=") << GetUsage()
        << TStringBuf(" (allocated=") << GetAllocated()
        << TStringBuf(", freed=") << GetFreed()
        << TStringBuf(", peak=") << GetPeak()
        << ')';
}

void TMemoryUsageInfo::VerifyDebug() const {
#ifndef NDEBUG
    size_t leakCount = 0;
    for (const auto& it: AllocationsMap_) {
        if (it.second.IsDeleted) {
            continue;
        }
        ++leakCount;
        Cerr << TStringBuf("Not freed ")
            << it.first << TStringBuf(" size: ") << it.second.Size
            << TStringBuf(", location: ") << it.second.Location
            << Endl;
    }

    if (!AllowMissing_) {
        Y_DEBUG_ABORT_UNLESS(GetUsage() == 0,
                "Allocated: %ld, Freed: %ld, Peak: %ld",
                GetAllocated(), GetFreed(), GetPeak());
    }
    Y_DEBUG_ABORT_UNLESS(!leakCount, "Has no freed memory");
#endif
}

}
}