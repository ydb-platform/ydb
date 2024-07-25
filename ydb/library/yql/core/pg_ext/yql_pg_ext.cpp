#include "yql_pg_ext.h"

namespace NYql {

void PgExtensionsFromProto(const NYql::NProto::TPgExtensions& proto,
    TVector<NPg::TExtensionDesc>& extensions) {
    extensions.clear();
    for (const auto& e: proto.GetExtension()) {
        NPg::TExtensionDesc desc;
        desc.Name = e.GetName();
        desc.InstallName = e.GetInstallName();
        desc.DDLPath = e.GetDDLPath();
        desc.LibraryPath = e.GetLibraryPath();
        desc.TypesOnly = e.GetTypesOnly();
        extensions.emplace_back(desc);
    }
}

} // namespace NYql
