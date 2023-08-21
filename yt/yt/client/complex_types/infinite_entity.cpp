#include "infinite_entity.h"

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

TInfiniteEntity::TInfiniteEntity()
    : Stream_(TStringBuf("#;#;#;#;#;#;#;#;"))
    , Parser_(&Stream_, NYson::EYsonType::ListFragment)
    , Cursor_(&Parser_)
{
    YT_VERIFY(Cursor_.TryConsumeFragmentStart());
}

NYson::TYsonPullParserCursor* TInfiniteEntity::GetCursor()
{
    return &Cursor_;
}

TInfiniteEntity::TRingBufferStream::TRingBufferStream(TStringBuf buffer)
    : Buffer_(buffer)
    , Pointer_(Buffer_.data())
{ }

size_t TInfiniteEntity::TRingBufferStream::DoNext(const void** ptr, size_t len)
{
    const auto end = Buffer_.data() + Buffer_.size();
    auto result = Min<size_t>(len, end - Pointer_);
    *ptr = Pointer_;
    Pointer_ += result;
    if (Pointer_ == end) {
        Pointer_ = Buffer_.data();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
