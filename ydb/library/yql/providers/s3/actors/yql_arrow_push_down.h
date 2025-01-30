#pragma once

#include <parquet/metadata.h>

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

namespace NYql::NDq {

TVector<ui64> MatchedRowGroups(std::shared_ptr<parquet::FileMetaData> fileMetadata, const NYql::NConnector::NApi::TPredicate& predicate);

TVector<ui64> MatchedRowGroups(const std::unique_ptr<parquet::FileMetaData>& fileMetadata, const NYql::NConnector::NApi::TPredicate& predicate);

} // namespace NYql::NDq