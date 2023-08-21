#include "path_list_reader.h"

#include <ydb/library/yql/providers/s3/range_helpers/file_tree_builder.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <google/protobuf/text_format.h>

#include <util/stream/mem.h>
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
            auto isDirectory = path.GetIsDirectory();
            auto readPath = isDirectory ? currentPath + "/" : currentPath;
            paths.emplace_back(TPath{readPath, path.GetSize(), path.GetIsDirectory()});
        }
        BuildPathsFromTree(path.GetChildren(), paths, currentPath, currentDepth + 1);
        currentPath.resize(prevSize);
    }
}

void DecodeS3Range(const NS3::TSource& sourceDesc, const TString& data, TPathList& paths, ui64& startPathIndex) {
    NS3::TRange range;
    TStringInput input(data);
    range.Load(&input);
    startPathIndex = range.GetStartPathIndex();

    // Modern way
    if (range.PathsSize()) {
        TString buf;
        return BuildPathsFromTree(range.GetPaths(), paths, buf);
    }

    std::unordered_map<TString, size_t> map(sourceDesc.GetDeprecatedPath().size());
    for (auto i = 0; i < sourceDesc.GetDeprecatedPath().size(); ++i) {
        map.emplace(sourceDesc.GetDeprecatedPath().Get(i).GetPath(), sourceDesc.GetDeprecatedPath().Get(i).GetSize());
    }

    for (auto i = 0; i < range.GetDeprecatedPath().size(); ++i) {
        const auto& path = range.GetDeprecatedPath().Get(i);
        auto it = map.find(path);
        YQL_ENSURE(it != map.end());
        paths.emplace_back(TPath{path, it->second, false});
    }
}

void ReadPathsList(const NS3::TSource& sourceDesc, const THashMap<TString, TString>& taskParams, const TVector<TString>& readRanges, TPathList& paths, ui64& startPathIndex) {
    if (!readRanges.empty()) {
        bool firstReadRange = true;
        for (auto readRange : readRanges) {
            ui64 rangeStartPathIndex;
            DecodeS3Range(sourceDesc, readRange, paths, rangeStartPathIndex);
            if (firstReadRange) {
                startPathIndex = rangeStartPathIndex;
                firstReadRange = false;
            } else {
                YQL_ENSURE(startPathIndex == rangeStartPathIndex);
            }
        }
    } else if (const auto taskParamsIt = taskParams.find(S3ProviderName); taskParamsIt != taskParams.cend()) {
        DecodeS3Range(sourceDesc, taskParamsIt->second, paths, startPathIndex);
    } else {
        for (auto i = 0; i < sourceDesc.GetDeprecatedPath().size(); ++i) {
            paths.emplace_back(TPath{
                sourceDesc.GetDeprecatedPath().Get(i).GetPath(),
                sourceDesc.GetDeprecatedPath().Get(i).GetSize(),
                false});
        }
    }
}

void PackPathsList(const TPathList& paths, TString& packed, bool& isTextEncoded) {
    TFileTreeBuilder builder;
    for (const auto& [path, size, isDirectory] : paths) {
        builder.AddPath(path, size, isDirectory);
    }
    NS3::TRange range;
    builder.Save(&range);

    isTextEncoded = range.GetPaths().size() < 100;
    if (isTextEncoded) {
        google::protobuf::TextFormat::PrintToString(range, &packed);
    } else {
        packed = range.SerializeAsString();
    }
}

void UnpackPathsList(TStringBuf packed, bool isTextEncoded, TPathList& paths) {
    NS3::TRange range;
    TMemoryInput inputStream(packed);
    if (isTextEncoded) {
        ParseFromTextFormat(inputStream, range);
    } else {
        range.ParseFromArcadiaStream(&inputStream);
    }

    TString buf;
    BuildPathsFromTree(range.GetPaths(), paths, buf);
}

} // namespace NYql::NS3Details
