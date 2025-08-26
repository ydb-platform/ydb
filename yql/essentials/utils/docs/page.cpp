#include "page.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYql::NDocs {

    TString ResolvedMarkdownText(TStringBuf relativePath, TString text, TStringBuf baseURL) {
        static const RE2 anchorRegex(R"re(\[([^\\\]]+)\]\((#[^\\)]+)\))re");
        static const RE2 linkRegex(R"re(\[([^\\\]]+)\]\(([A-Za-z0-9/_\-\.]+).md(#[^\\)]+)?\))re");

        TString base = TString(baseURL) + "/" + TString(relativePath);
        TString anchorRewrite = "[\\1](" + base + "\\2)";
        TString linkRewrite = "[\\1](" + base + "/../" + "\\2\\3)";

        TString error;
        YQL_ENSURE(
            anchorRegex.CheckRewriteString(anchorRewrite, &error),
            "Bad rewrite '" << anchorRewrite << "': " << error);
        YQL_ENSURE(
            linkRegex.CheckRewriteString(linkRewrite, &error),
            "Bad rewrite '" << linkRewrite << "': " << error);

        RE2::GlobalReplace(&text, anchorRegex, anchorRewrite);
        RE2::GlobalReplace(&text, linkRegex, linkRewrite);

        return text;
    }

    TMarkdownPage Resolved(TStringBuf relativePath, TMarkdownPage page, TStringBuf baseURL) {
        page.Text = ResolvedMarkdownText(relativePath, page.Text, baseURL);
        for (auto& [_, section] : page.SectionsByAnchor) {
            section.Body = ResolvedMarkdownText(relativePath, section.Body, baseURL);
        }
        return page;
    }

    TPages ParsePages(TResourcesByRelativePath resources) {
        TPages pages;
        for (auto& [path, resource] : resources) {
            TMarkdownPage page = ParseMarkdownPage(std::move(resource));
            pages.emplace(std::move(path), std::move(page));
        }
        return pages;
    }

    TPages Resolved(TPages pages, TStringBuf baseURL) {
        for (auto& [relativeURL, page] : pages) {
            page = Resolved(relativeURL, std::move(page), baseURL);
        }
        return pages;
    }

} // namespace NYql::NDocs
