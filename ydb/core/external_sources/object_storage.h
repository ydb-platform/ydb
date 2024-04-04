#pragma once

#include "external_source.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NKikimr::NExternalSource {

IExternalSource::TPtr CreateObjectStorageExternalSource(const std::vector<TRegExMatch>& hostnamePatterns);

NYql::TIssues ValidateObjectStorage(const FederatedQuery::Schema& schema, const FederatedQuery::ObjectStorageBinding::Subset& objectStorage, size_t pathsLimit);

NYql::TIssues ValidateDataStreams(const FederatedQuery::DataStreamsBinding& binding);

}
