#pragma once
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <util/generic/string.h>
#include <util/string/split.h>

#include <map>

namespace NYql::NS3Details {

class TFileTreeBuilder {
    struct TPath {
        ui64 FileSize = 0;
        bool Read = false;
        std::map<TString, TPath> Children;
    };

public:
    void AddPath(const TString& path, ui64 fileSize);
    void Save(NS3::TRange* range) const;

private:
    void SaveImpl(NS3::TRange::TPath* path, const TString& name, const TPath& srcPath) const;
    static std::vector<TString> SplitPath(const TString& path);

private:
    std::map<TString, TPath> Roots;
};

} // namespace NYql::NS3Details
