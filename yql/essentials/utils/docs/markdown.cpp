#include "markdown.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/yexception.h>

namespace NSQLComplete {

    class TMarkdownParser {
    public:
        void Parse(IInputStream& markdown, TMarkdownCallback&& onSection) {
            for (TString line; markdown.ReadLine(line) != 0;) {
                if (IsSkipping_) {
                    if (IsSectionHeader(line)) {
                        ResetSection(std::move(line));
                        IsSkipping_ = false;
                    } else {
                        // Skip
                    }
                } else {
                    if (IsSectionHeader(line)) {
                        onSection(std::move(Section_));
                        ResetSection(std::move(line));
                    } else {
                        Section_.Body.append(std::move(line));
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
            YQL_ENSURE(
                RE2::FullMatch(line, SectionHeaderRegex_, &content, &dummy, &anchor),
                "line '" << line << "' does not match regex '"
                         << SectionHeaderRegex_.pattern() << "'");

            Section_.Header.Content = std::move(content);
            if (anchor) {
                Section_.Header.Anchor = std::move(*anchor);
            }
        }

        bool IsSectionHeader(TStringBuf line) const {
            return HeaderDepth(line) == 2;
        }

        size_t HeaderDepth(TStringBuf line) const {
            size_t pos = line.find_first_not_of('#');
            return pos != TStringBuf::npos ? pos : 0;
        }

        RE2 SectionHeaderRegex_{R"re(## ([^#]+)(\s+{(#[a-z\-_]+)})?)re"};
        bool IsSkipping_ = true;
        TMarkdownSection Section_;
    };

    void ParseMarkdown(IInputStream& markdown, TMarkdownCallback&& onSection) {
        TMarkdownParser parser;
        parser.Parse(markdown, std::forward<TMarkdownCallback>(onSection));
    }

} // namespace NSQLComplete
