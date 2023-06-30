#pragma once

#include "public.h"

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

class TInfiniteEntity
{
public:
    TInfiniteEntity();

    NYson::TYsonPullParserCursor* GetCursor();

private:
    class TRingBufferStream
        : public IZeroCopyInput
    {
    public:
        explicit TRingBufferStream(TStringBuf buffer);

    private:
        size_t DoNext(const void** ptr, size_t len) override;

    private:
        const TStringBuf Buffer_;
        const char* Pointer_;
    };

private:
    TRingBufferStream Stream_;
    NYson::TYsonPullParser Parser_;
    NYson::TYsonPullParserCursor Cursor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes