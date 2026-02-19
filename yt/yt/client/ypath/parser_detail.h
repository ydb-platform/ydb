#pragma once

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

std::pair<TYPath, NYTree::IAttributeDictionaryPtr> ParseRichYPathImpl(TStringBuf str);

TString ConvertToStringImpl(const TYPath& path, const NYTree::IAttributeDictionary& attributes, NYson::EYsonFormat ysonFormat);

NChunkClient::TReadRange RangeNodeToReadRange(
    const NTableClient::TComparator& comparator,
    const NYTree::IMapNodePtr& rangeNode,
    const NTableClient::TKeyColumnTypes& conversionTypeHints);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
