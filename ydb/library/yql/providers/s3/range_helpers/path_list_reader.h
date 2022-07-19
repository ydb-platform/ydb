#pragma once
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <utility>
#include <vector>

namespace NYql::NS3Details {

using TPath = std::tuple<TString, size_t>;
using TPathList = std::vector<TPath>;

void ReadPathsList(const NS3::TSource& sourceDesc, const THashMap<TString, TString>& taskParams, TPathList& paths, ui64& startPathIndex);

} // namespace NYql::NS3Details
