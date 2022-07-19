#include "file_tree_builder.h"

namespace NYql::NS3Details {

void TFileTreeBuilder::AddPath(const TString& path, ui64 fileSize) {
    const auto parts = SplitPath(path);
    std::map<TString, TPath>* currentChildren = &Roots;
    for (size_t i = 0, size = parts.size(); i < size; ++i) {
        TPath& p = (*currentChildren)[parts[i]];
        if (i == size - 1) { // last
            Y_VERIFY(p.FileSize == 0);
            Y_VERIFY(!p.Read);
            p.FileSize = fileSize;
            p.Read = true;
        } else {
            currentChildren = &p.Children;
        }
    }
}

void TFileTreeBuilder::Save(NS3::TRange* range) const {
    for (const auto& [n, p] : Roots) {
        SaveImpl(range->AddPaths(), n, p);
    }
}

void TFileTreeBuilder::SaveImpl(NS3::TRange::TPath* path, const TString& name, const TPath& srcPath) const {
    path->SetName(name);
    path->SetSize(srcPath.FileSize);
    path->SetRead(srcPath.Read);
    for (const auto& [n, p] : srcPath.Children) {
        SaveImpl(path->AddChildren(), n, p);
    }
}

std::vector<TString> TFileTreeBuilder::SplitPath(const TString& path) {
    std::vector<TString> parts = StringSplitter(path).Split('/');
    return parts;
}

} // namespace NYql::NS3Details
