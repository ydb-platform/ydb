#include "ref.h"

#include "undumpable.h"

#include <util/generic/size_literals.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MinUndumpableSize = 64_KB;

struct TUndumpableHolder
    : public TSharedRangeHolder
{
    explicit TUndumpableHolder(const TSharedRef& ref)
        : Underlying(ref.GetHolder())
        , Mark(MarkUndumpable(const_cast<char*>(ref.Begin()), ref.Size()))
    { }

    ~TUndumpableHolder()
    {
        if (Mark) {
            UnmarkUndumpable(Mark);
        }
    }

    // TSharedRangeHolder overrides.
    std::optional<size_t> GetTotalByteSize() const override
    {
        return Underlying->GetTotalByteSize();
    }

    const TSharedRangeHolderPtr Underlying;
    TUndumpableMark* const Mark;
};

TSharedRef MarkUndumpable(const TSharedRef& ref)
{
    if (ref.Size() >= MinUndumpableSize) {
        return TSharedRef(ref, New<TUndumpableHolder>(ref));
    } else {
        return ref;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
