#include "link_page.h"

#include "name.h"

#include <util/generic/hash_set.h>
#include <util/string/split.h>

namespace NYql::NDocs {

    TMaybe<TString> MatchSingleFunctionHeader(TStringBuf header) {
        return NormalizedName(TString(header));
    }

    TVector<TString> SplitBy(TStringBuf delim, const TVector<TString>& strings) {
        TVector<TString> parts;
        for (const TString& s : strings) {
            StringSplitter(s).SplitByString(delim).AddTo(&parts);
        }
        return parts;
    }

    TVector<TString> SplitByPunctuation(TStringBuf header) {
        TVector<TString> parts = {TString(header)};
        parts = SplitBy(" и ", parts);
        parts = SplitBy(" / ", parts);
        parts = SplitBy(", ", parts);
        return parts;
    }

    TVector<TString> MatchMultiFunctionHeader(TStringBuf header) {
        TVector<TString> names = SplitByPunctuation(header);

        for (TString& name : names) {
            TMaybe<TString> normalized = NormalizedName(std::move(name));
            if (!normalized) {
                return {};
            }

            name = std::move(*normalized);
        }

        return names;
    }

    TVector<TString> ExtractNormalized(TStringBuf header) {
        if (auto single = MatchSingleFunctionHeader(header)) {
            return {*single};
        }
        if (auto multi = MatchMultiFunctionHeader(header)) {
            return multi;
        }
        return {};
    }

    void EnrichFromMarkdown(TLinks& links, const TString& path, const TMarkdownHeader& header) {
        for (const TString& name : ExtractNormalized(header.Content)) {
            links[name] = {
                .RelativePath = path,
                .Anchor = header.Anchor,
            };
        }
    }

    void EnrichFromMarkdown(TLinks& links, const TString& path, const TMarkdownPage& page) {
        for (const auto& [anchor, section] : page.SectionsByAnchor) {
            const TMarkdownHeader& header = section.Header;
            EnrichFromMarkdown(links, path, header);
        }
    }

    void EnrichFromMarkdown(TLinks& links, const TPages& pages) {
        for (const auto& [path, page] : pages) {
            EnrichFromMarkdown(links, path, page);
        }
    }

    TLinks GetLinksFromPages(const TPages& pages) {
        TLinks links;
        EnrichFromMarkdown(links, pages);
        return links;
    }

    TPages Stripped(TPages&& pages, const TLinks& links) {
        THashSet<TString> usedPaths;
        THashMap<TString, THashSet<TString>> usedAnchors;
        for (const auto& [_, link] : links) {
            TString anchor = link.Anchor.GetOrElse("");
            usedAnchors[link.RelativePath].emplace(std::move(anchor));
        }

        THashSet<TString> unusedPaths;
        THashMap<TString, THashSet<TString>> unusedAnchors;
        for (const auto& [path, page] : pages) {
            for (const auto& [anchor, _] : page.SectionsByAnchor) {
                if (!usedAnchors.contains(path)) {
                    unusedPaths.emplace(path);
                } else if (!usedAnchors[path].contains(anchor)) {
                    unusedAnchors[path].emplace(anchor);
                }
            }
        }

        for (const auto& [path, anchors] : unusedAnchors) {
            for (const auto& anchor : anchors) {
                pages[path].SectionsByAnchor.erase(anchor);
            }
        }

        for (const auto& path : unusedPaths) {
            pages.erase(path);
        }

        return pages;
    }

} // namespace NYql::NDocs
