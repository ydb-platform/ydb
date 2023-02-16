#pragma once
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <util/generic/string.h>
#include <util/string/split.h>

#include <map>

namespace NYql::NS3Details {

class TFileTreeBuilder {

    struct TTreeKey {
        TString Name;
        bool IsDirectory = false;

        std::strong_ordering operator<=>(const TTreeKey& other) const = default;
    };

    struct TPath {
        using TFileTreeMap = std::map<TTreeKey, TPath>;

        ui64 FileSize = 0;
        bool Read = false;
        TFileTreeMap Children;
    };

public:
    void AddPath(const TString& path, ui64 fileSize, bool isDirectory);
    void Save(NS3::TRange* range) const;

private:
    void SaveImpl(NS3::TRange::TPath* path, const TTreeKey& nodeKey, const TPath& srcPath) const;
    static std::vector<TString> SplitPath(const TString& path);

private:
    TPath::TFileTreeMap Roots;
};

} // namespace NYql::NS3Details
