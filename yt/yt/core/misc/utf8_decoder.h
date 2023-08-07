#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TUtf8Transcoder
{
public:
    explicit TUtf8Transcoder(bool enableEncoding = true);

    TStringBuf Encode(TStringBuf str);
    TStringBuf Decode(TStringBuf str);

private:
    bool EnableEncoding_;
    std::vector<char> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
