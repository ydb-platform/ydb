#pragma once

#include <yql/essentials/protos/pg_ext.pb.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

namespace NYql {

void PgExtensionsFromProto(const NYql::NProto::TPgExtensions& proto,
    TVector<NPg::TExtensionDesc>& extensions);

} // namespace NYql
