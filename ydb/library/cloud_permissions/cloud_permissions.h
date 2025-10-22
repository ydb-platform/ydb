#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloudPermissions {

enum class EType {
    DEFAULT,
};

template<EType Type>
struct TCloudPermissions;

template<>
struct TCloudPermissions<EType::DEFAULT> {
    static const TVector<TString>& Get() {
        static TVector<TString> permissions {
            "ydb.databases.list",
            "ydb.databases.create",
            "ydb.databases.connect",
            "ydb.tables.select",
            "ydb.tables.write",
            "ydb.schemas.getMetadata",
            "ydb.streams.write"
        };
        return permissions;
    }
};

} // NCloudPermissions
