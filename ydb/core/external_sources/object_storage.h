#pragma once

#include "external_source.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NKikimr::NExternalSource {

IExternalSource::TPtr CreateObjectStorageExternalSource(const std::vector<TRegExMatch>& hostnamePatterns, size_t pathsLimit);

NYql::TIssues Validate(const FederatedQuery::Schema& schema, const FederatedQuery::ObjectStorageBinding::Subset& objectStorage, size_t pathsLimit);

NYql::TIssues ValidateDateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting, bool matchAllSettings = false);

NYql::TIssues ValidateRawFormat(const TString& format, const FederatedQuery::Schema& schema, const google::protobuf::RepeatedPtrField<TString>& partitionedBy);

}
