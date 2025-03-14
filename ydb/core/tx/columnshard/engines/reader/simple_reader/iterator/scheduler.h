#pragma once

#include <util/generic/noncopyable.h>
#include <util/system/types.h>

#include <memory>

namespace NKikimr::NOlap::NReader::NSimple {

class ISourceFetchingScheduler {
protected:
    virtual void DoSetUnblocked(const ui32 sourceIdx) = 0;
    virtual void DoSetBlocked(const ui32 sourceIdx) = 0;

public:
    class TSourceBlockedGuard: TMoveOnly {
    private:
        std::weak_ptr<ISourceFetchingScheduler> Owner;
        ui32 SourceIdx;

    public:
        TSourceBlockedGuard(const ui32 sourceIdx, const std::shared_ptr<ISourceFetchingScheduler>& owner)
            : Owner(owner)
            , SourceIdx(sourceIdx) {
            owner->DoSetBlocked(sourceIdx);
        }

        ~TSourceBlockedGuard() {
            if (auto owner = Owner.lock()) {
                owner->DoSetUnblocked(SourceIdx);
            }
        }

        TSourceBlockedGuard(TSourceBlockedGuard&& other)
            : Owner(other.Owner)
            , SourceIdx(other.SourceIdx) {
            other.Owner.reset();
        }
        TSourceBlockedGuard& operator=(TSourceBlockedGuard&& other) {
            std::swap(Owner, other.Owner);
            std::swap(SourceIdx, other.SourceIdx);
            return *this;
        }
    };

public:
    [[nodiscard]] static TSourceBlockedGuard SetBlocked(const ui32 sourceIdx, const std::shared_ptr<ISourceFetchingScheduler>& self) {
        return TSourceBlockedGuard(sourceIdx, self);
    }

    virtual ~ISourceFetchingScheduler() = default;
};

}   // namespace NKikimr::NOlap::NReader::NSimple
