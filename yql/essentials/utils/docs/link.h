#pragma once

#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NYql::NDocs {

    struct TLinkTarget {
        TString RelativePath;
        TMaybe<TString> Anchor;

        static TLinkTarget Parse(TStringBuf string);
    };

    using TLinkKey = TString;

    using TLinks = THashMap<TLinkKey, TLinkTarget>;

    TMaybe<TLinkTarget> Lookup(const TLinks& links, TStringBuf name);

    TLinkKey ParseLinkKey(TStringBuf string);

    TLinks ParseLinks(const NJson::TJsonValue& json);

    TLinks Merge(TLinks&& lhs, TLinks&& rhs);

} // namespace NYql::NDocs
