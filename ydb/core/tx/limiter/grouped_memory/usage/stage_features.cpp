#include "stage_features.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TString TStageFeatures::DebugString() const {
    TStringBuilder result;
    result << "name=" << Name << ";limit=" << Limit << ";";
    if (Owner) {
        result << "owner=" << Owner->DebugString() << ";";
    }
    return result;
}

TStageFeatures::TStageFeatures(const TString& name, const ui64 limit, const std::optional<ui64>& hardLimit,
    const std::shared_ptr<TStageFeatures>& owner, const std::shared_ptr<TStageCounters>& counters)
    : Name(name)
    , Limit(limit)
    , HardLimit(hardLimit)
    , Owner(owner)
    , Counters(counters) {
    if (Counters) {
        Counters->ValueSoftLimit->Set(Limit);
        if (HardLimit) {
            Counters->ValueHardLimit->Set(*HardLimit);
        }
    }
}

TConclusionStatus TStageFeatures::Allocate(const ui64 volume) {
    std::optional<TConclusionStatus> result;
    {
        auto* current = this;
        while (current) {
            current->Waiting.Sub(volume);
            if (current->Counters) {
                current->Counters->Sub(volume, false);
            }
            if (current->HardLimit && *current->HardLimit < current->Usage.Val() + volume) {
                if (!result) {
                    result = TConclusionStatus::Fail(TStringBuilder() << current->Name << "::(limit:" << *current->HardLimit
                                                                      << ";val:" << current->Usage.Val() << ";delta=" << volume << ");");
                }
                if (current->Counters) {
                    current->Counters->OnCannotAllocate();
                }
                AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("name", current->Name)("event", "cannot_allocate")(
                    "limit", *current->HardLimit)("usage", current->Usage.Val())("delta", volume);
            }
            current = current->Owner.get();
        }
    }
    if (!!result) {
        return *result;
    }
    {
        auto* current = this;
        while (current) {
            current->Usage.Add(volume);
            AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("name", current->Name)("event", "allocate")("usage", current->Usage.Val())(
                "delta", volume);
            if (current->Counters) {
                current->Counters->Add(volume, true);
            }
            current = current->Owner.get();
        }
    }
    return TConclusionStatus::Success();
}

void TStageFeatures::Free(const ui64 volume, const bool allocated) {
    auto* current = this;
    while (current) {
        if (current->Counters) {
            current->Counters->Sub(volume, allocated);
        }
        if (allocated) {
            current->Usage.Sub(volume);
        } else {
            current->Waiting.Sub(volume);
        }
        AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("name", current->Name)("event", "free")("usage", current->Usage.Val())(
            "delta", volume);
        current = current->Owner.get();
    }
}

void TStageFeatures::UpdateVolume(const ui64 from, const ui64 to, const bool allocated) {
    if (Counters) {
        Counters->Sub(from, allocated);
        Counters->Add(to, allocated);
    }
    AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("name", Name)("event", "update")("usage", Usage.Val())("waiting", Waiting.Val())(
        "allocated", allocated)("from", from)("to", to);
    if (allocated) {
        Usage.Sub(from);
        Usage.Add(to);
    } else {
        Waiting.Sub(from);
        Waiting.Add(to);
    }

    if (Owner) {
        Owner->UpdateVolume(from, to, allocated);
    }
}

bool TStageFeatures::IsAllocatable(const ui64 volume, const ui64 additional) const {
    if (Limit < additional + Usage.Val() + volume) {
        return false;
    }
    if (Owner) {
        return Owner->IsAllocatable(volume, additional);
    }
    return true;
}

void TStageFeatures::Add(const ui64 volume, const bool allocated) {
    if (Counters) {
        Counters->Add(volume, allocated);
    }
    if (allocated) {
        Usage.Add(volume);
    } else {
        Waiting.Add(volume);
    }

    if (Owner) {
        Owner->Add(volume, allocated);
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
