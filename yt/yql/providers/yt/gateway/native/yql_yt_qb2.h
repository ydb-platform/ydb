#pragma once

#include <yt/cpp/mapreduce/interface/common.h>

#include <util/generic/set.h>
#include <util/generic/string.h>

#include <map>
#include <tuple>
#include <vector>

namespace NYql {

namespace NNative {

using TRemapperKey = std::tuple<TString, TString, TString>; // binary path (must be first), log name, all fields
using TRemapperPayload = std::vector<ui32>; // table indicies
using TRemapperMap = std::map<TRemapperKey, TRemapperPayload>;

void ProcessTableQB2Premapper(const NYT::TNode& remapper,
    const TString& tableName,
    NYT::TRichYPath& tablePath,
    size_t tableNum,
    TRemapperMap& remapperMap,
    TSet<TString>& remapperAllFiles,
    bool& useSkiff);

TString GetQB2PremapperPrefix(const TRemapperMap& remapperMap, bool useSkiff);

void UpdateQB2PremapperUseSkiff(const TRemapperMap& remapperMap, bool& useSkiff);

} // NNative

} // NYql
