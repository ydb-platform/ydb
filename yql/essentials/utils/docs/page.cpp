#include "page.h"

namespace NYql::NDocs {

    TPages ParsePages(TResourcesByRelativePath resources) {
        TPages pages;
        for (auto& [path, resource] : resources) {
            TMarkdownPage page = ParseMarkdownPage(std::move(resource));
            pages.emplace(std::move(path), std::move(page));
        }
        return pages;
    }

} // namespace NYql::NDocs
