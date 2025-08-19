#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/stream/input.h>

namespace NYql::NDocs {

    struct TMarkdownHeader {
        TString Content;
        TMaybe<TString> Anchor;
    };

    struct TMarkdownSection {
        TMarkdownHeader Header;
        TString Body;
    };

    struct TMarkdownPage {
        TString Text;
        THashMap<TString, TMarkdownSection> SectionsByAnchor;
    };

    using TMarkdownCallback = std::function<void(TMarkdownSection&&)>;

    TMarkdownPage ParseMarkdownPage(TString markdown);

} // namespace NYql::NDocs
