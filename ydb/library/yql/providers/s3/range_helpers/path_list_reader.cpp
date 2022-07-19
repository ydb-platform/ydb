#include "path_list_reader.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/stream/str.h>

namespace NYql::NS3Details {

static void BuildPathsFromTree(const google::protobuf::RepeatedPtrField<NYql::NS3::TRange::TPath>& children, TPathList& paths, TString& currentPath, size_t currentDepth = 0) {
    if (children.empty()) {
        return;
    }
    if (currentDepth) {
        currentPath += '/';
    }
    for (const auto& path : children) {
        const size_t prevSize = currentPath.size();
        currentPath += path.GetName();
        if (path.GetRead()) {
            paths.emplace_back(currentPath, path.GetSize());
        }
        BuildPathsFromTree(path.GetChildren(), paths, currentPath, currentDepth + 1);
        currentPath.resize(prevSize);
    }
}

void ReadPathsList(const NS3::TSource& sourceDesc, const THashMap<TString, TString>& taskParams, TPathList& paths, ui64& startPathIndex) {
    if (const auto taskParamsIt = taskParams.find(S3ProviderName); taskParamsIt != taskParams.cend()) {
        NS3::TRange range;
        TStringInput input(taskParamsIt->second);
        range.Load(&input);
        startPathIndex = range.GetStartPathIndex();

        // Modern way
        if (range.PathsSize()) {
            TString buf;
            BuildPathsFromTree(range.GetPaths(), paths, buf);
            return;
        }

        std::unordered_map<TString, size_t> map(sourceDesc.GetDeprecatedPath().size());
        for (auto i = 0; i < sourceDesc.GetDeprecatedPath().size(); ++i)
            map.emplace(sourceDesc.GetDeprecatedPath().Get(i).GetPath(), sourceDesc.GetDeprecatedPath().Get(i).GetSize());

        for (auto i = 0; i < range.GetDeprecatedPath().size(); ++i) {
            const auto& path = range.GetDeprecatedPath().Get(i);
            auto it = map.find(path);
            YQL_ENSURE(it != map.end());
            paths.emplace_back(path, it->second);
        }
    } else {
        for (auto i = 0; i < sourceDesc.GetDeprecatedPath().size(); ++i) {
            paths.emplace_back(sourceDesc.GetDeprecatedPath().Get(i).GetPath(), sourceDesc.GetDeprecatedPath().Get(i).GetSize());
        }
    }
}

} // namespace NYql::NS3Details
