#pragma once

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/stream/input.h>

namespace NSQLComplete {

    struct TMarkdownHeader {
        TString Content;
        TMaybe<TString> Anchor;
    };

    struct TMarkdownSection {
        TMarkdownHeader Header;
        TString Body;
    };

    using TMarkdownCallback = std::function<void(TMarkdownSection&&)>;

    void ParseMarkdown(IInputStream& markdown, TMarkdownCallback&& onSection);

} // namespace NSQLComplete
