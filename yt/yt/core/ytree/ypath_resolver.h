#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<T> TryGetValue(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<i64> TryGetInt64(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<ui64> TryGetUint64(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<bool> TryGetBoolean(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<double> TryGetDouble(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<std::string> TryGetString(TStringBuf yson, NYPath::TYPathBuf ypath);
std::optional<std::string> TryGetAny(TStringBuf yson, NYPath::TYPathBuf ypath);

template <class T>
std::optional<T> TryParseValue(NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

bool ParseListUntilIndex(NYson::TYsonPullParserCursor* cursor, int targetIndex);
bool ParseMapOrAttributesUntilKey(NYson::TYsonPullParserCursor* cursor, TStringBuf key);
std::string ParseAnyValue(NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
