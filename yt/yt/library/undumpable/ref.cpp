#include "ref.h"

#include "undumpable.h"

#include <util/generic/size_literals.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MinUndumpableSize = 64_KB;

class TUndumpableHolder
    : public TSharedRangeHolder
{
public:
    explicit TUndumpableHolder(const TSharedRef& ref)
        : Underlying_(ref.GetHolder())
        , Mark_(MarkUndumpable(const_cast<char*>(ref.Begin()), ref.Size()))
    { }

    ~TUndumpableHolder()
    {
        if (Mark_) {
            UnmarkUndumpable(Mark_);
        }
    }

    // TSharedRangeHolder overrides.
    std::optional<size_t> GetTotalByteSize() const override
    {
        return Underlying_->GetTotalByteSize();
    }

private:
    const TSharedRangeHolderPtr Underlying_;
    TUndumpableMark* const Mark_;
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
