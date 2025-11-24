#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

enum class ETokenType;
class TTokenizer;
class TRichYPath;

class TTrie;
class TTrieView;
class TTrieTraversalFrame;

using TYPath = TString;
using TYPathBuf = TStringBuf;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
