#include "yql_pg_ext.h"

namespace NYql {

void PgExtensionsFromProto(const NYql::NProto::TPgExtensions& proto,
    TVector<NPg::TExtensionDesc>& extensions) {
    extensions.clear();
    for (const auto& e: proto.GetExtension()) {
        NPg::TExtensionDesc desc;
        desc.Name = e.GetName();
        desc.InstallName = e.GetInstallName();
        for (const auto& p : e.GetSqlPath()) {
            desc.SqlPaths.push_back(p);
        }
        
        desc.LibraryPath = e.GetLibraryPath();
        desc.TypesOnly = e.GetTypesOnly();
        desc.LibraryMD5 = e.GetLibraryMD5();
        desc.Version = e.GetVersion();
        extensions.emplace_back(desc);
    }
}

} // namespace NYql
