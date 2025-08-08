#include "markdown.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/yexception.h>
#include <util/charset/utf8.h>

namespace NYql::NDocs {

    class TMarkdownParser {
    private:
        static constexpr TStringBuf HeaderRegex = R"re(([^#]+)(\s+{#([a-z0-9\-_]+)})?)re";

    public:
        explicit TMarkdownParser(size_t headerDepth)
            : HeaderDepth_(headerDepth)
            , SectionHeaderRegex_(" *" + TString(HeaderDepth_, '#') + " " + HeaderRegex)
            , IsSkipping_(true)
        {
        }

        void Parse(IInputStream& markdown, TMarkdownCallback&& onSection) {
            for (TString line; markdown.ReadLine(line) != 0;) {
                size_t depth = HeaderDepth(line);
                if (IsSkipping_) {
                    if (HeaderDepth_ == depth) {
                        ResetSection(std::move(line));
                        IsSkipping_ = false;
                    } else {
                        // Skip
                    }
                } else {
                    if (HeaderDepth_ == depth) {
                        onSection(std::move(Section_));
                        ResetSection(std::move(line));
                    } else if (depth == 0 || HeaderDepth_ < depth) {
                        line.append('\n');
                        Section_.Body.append(std::move(line));
                    } else {
                        onSection(std::move(Section_));
                        IsSkipping_ = true;
                    }
                }
            }

            if (!IsSkipping_) {
                onSection(std::move(Section_));
            }
        }

    private:
        void ResetSection(TString&& line) {
            Section_ = TMarkdownSection();

            TString content;
            std::optional<TString> dummy;
            std::optional<TString> anchor;
            if (!RE2::FullMatch(line, SectionHeaderRegex_, &content, &dummy, &anchor)) {
                Section_.Header.Content = std::move(line);
                return;
            }

            Section_.Header.Content = std::move(content);
            if (anchor) {
                Section_.Header.Anchor = std::move(*anchor);
            }
        }

        size_t HeaderDepth(TStringBuf line) const {
            while (line.StartsWith(' ') || line.StartsWith('\t')) {
                line.Skip(1);
            }

            if (!line.StartsWith('#')) {
                return 0;
            }

            size_t begin = line.find('#');
            size_t end = line.find_first_not_of('#', begin);
            return end != TStringBuf::npos ? (end - begin) : 0;
        }

        size_t HeaderDepth_;
        RE2 SectionHeaderRegex_;
        bool IsSkipping_;
        TMarkdownSection Section_;
    };

    TMaybe<TString> Anchor(const TMarkdownHeader& header) {
        static RE2 Regex(R"re([0-9a-z\-_]+)re");

        if (header.Anchor) {
            return header.Anchor;
        }

        TString content = ToLowerUTF8(header.Content);
        SubstGlobal(content, ' ', '-');

        if (RE2::FullMatch(content, Regex)) {
            return content;
        }

        return Nothing();
    }

    TMarkdownPage ParseMarkdownPage(TString markdown) {
        TMarkdownPage page;

        const auto onSection = [&](TMarkdownSection&& section) {
            if (TMaybe<TString> anchor = Anchor(section.Header)) {
                section.Header.Anchor = anchor;
                page.SectionsByAnchor[*anchor] = std::move(section);
            }
        };

        {
            TMarkdownParser parser(/*headerDepth=*/2);
            TStringStream stream(markdown);
            parser.Parse(stream, onSection);
        }

        {
            TMarkdownParser parser(/*headerDepth=*/3);
            TStringStream stream(markdown);
            parser.Parse(stream, onSection);
        }

        page.Text = std::move(markdown);

        return page;
    }

} // namespace NYql::NDocs
