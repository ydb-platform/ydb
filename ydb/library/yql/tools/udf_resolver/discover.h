#pragma once

#include <ydb/library/yql/providers/common/proto/udf_resolver.pb.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/generic/hash.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NUdfResolver {
void DiscoverInDir(const TString& dir, IOutputStream& out, bool printAsProto);
void DiscoverInFile(const TString& filePath, IOutputStream& out, bool printAsProto);
void Discover(IInputStream& in, IOutputStream& out, bool printAsProto);
void FillImportResultModules(const THashSet<TString>& modules, NYql::TImportResult& importRes);
;
}
