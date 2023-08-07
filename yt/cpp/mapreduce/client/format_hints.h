#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/maybe.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
void ApplyFormatHints(TFormat* format, const TMaybe<TFormatHints>& formatHints);

template <>
void ApplyFormatHints<TNode>(TFormat* format, const TMaybe<TFormatHints>& formatHints);

template <>
void ApplyFormatHints<TYaMRRow>(TFormat* format, const TMaybe<TFormatHints>& formatHints);

template <>
void ApplyFormatHints<::google::protobuf::Message>(TFormat* format, const TMaybe<TFormatHints>& formatHints);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
