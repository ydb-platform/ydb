#include "file_tree_builder.h"

namespace NYql::NS3Details {

void TFileTreeBuilder::AddPath(const TString& path, ui64 fileSize, bool isDirectory) {
    const auto parts = SplitPath(path);
    TPath::TFileTreeMap* currentChildren = &Roots;
    for (size_t i = 0, size = parts.size(); i < size; ++i) {
        bool isSubDirectory = i != size - 1;
        if (!isSubDirectory) {
            TPath& p = (*currentChildren)[TTreeKey{path == "" ? "/" : parts[i], isDirectory}];
            Y_ABORT_UNLESS(p.FileSize == 0);
            Y_ABORT_UNLESS(!p.Read);
            p.FileSize = fileSize;
            p.Read = true;
        } else {
            TPath& p = (*currentChildren)[TTreeKey{parts[i], isSubDirectory}];
            currentChildren = &p.Children;
        }
    }
}

void TFileTreeBuilder::Save(NS3::TRange* range) const {
    for (const auto& [n, p] : Roots) {
        SaveImpl(range->AddPaths(), n, p);
    }
}

void TFileTreeBuilder::SaveImpl(NS3::TRange::TPath* path, const TTreeKey& nodeKey, const TPath& srcPath) const {
    path->SetName(nodeKey.Name);
    path->SetIsDirectory(nodeKey.IsDirectory);
    path->SetSize(srcPath.FileSize);
    path->SetRead(srcPath.Read);
    for (const auto& [n, p] : srcPath.Children) {
        SaveImpl(path->AddChildren(), n, p);
    }
}

std::vector<TString> TFileTreeBuilder::SplitPath(const TString& path) {
    std::vector<TString> parts = StringSplitter(path).Split('/');
    if (!path.empty() && path.back() == '/') {
        parts.pop_back();
    }
    return parts;
}

} // namespace NYql::NS3Details
