#pragma once

#include <ydb/library/yql/protos/pg_ext.pb.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {

void PgExtensionsFromProto(const NYql::NProto::TPgExtensions& proto,
    TVector<NPg::TExtensionDesc>& extensions);

} // namespace NYql
