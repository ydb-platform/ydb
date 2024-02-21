#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<T> TryGetValue(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<i64> TryGetInt64(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<ui64> TryGetUint64(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<bool> TryGetBoolean(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<double> TryGetDouble(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<TString> TryGetString(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<TString> TryGetAny(TStringBuf yson, const NYPath::TYPath& ypath);

template <class T>
std::optional<T> TryParseValue(NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

bool ParseListUntilIndex(NYson::TYsonPullParserCursor* cursor, int targetIndex);
bool ParseMapOrAttributesUntilKey(NYson::TYsonPullParserCursor* cursor, TStringBuf key);
TString ParseAnyValue(NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
