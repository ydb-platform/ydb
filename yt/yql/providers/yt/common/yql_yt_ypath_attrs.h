#pragma once

// Serialization of the extra (YQL-supported) attributes of an NYT::TRichYPath to
// and from a YSON string. Kept in yt/common (a low-level module shared by both
// the provider and the gateways) so that the provider does not need to depend on
// gateway/lib for it, which would create a provider <-> gateway/lib cycle.

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

TMaybe<TString> SerializeRichYPathAttrs(const NYT::TRichYPath& richPath);
void DeserializeRichYPathAttrs(const TString& serializedAttrs, NYT::TRichYPath& richPath);

} // namespace NYql
