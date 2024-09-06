#pragma once

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/error/origin_attributes.h>

// TODO(arkady-e1ppa): Move this to library/cpp/yt/error when deps on ytree are gone.

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

// Weak symbol.
TOriginAttributes ExtractFromDictionary(const NYTree::IAttributeDictionaryPtr& attributes);

////////////////////////////////////////////////////////////////////////////////

// Default impl of weak symbol.
TOriginAttributes ExtractFromDictionaryDefault(const NYTree::IAttributeDictionaryPtr& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
