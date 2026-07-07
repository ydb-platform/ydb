#pragma once

#include <optional>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

enum class EMetaFieldType {
    Uint64,
    Timestamp,
    String,
    DictStringString,
};

struct TMetaFieldDescriptor {
    const TString Key;
    const TString SysColumn;
    const EMetaFieldType Type;
};

std::optional<TString> SkipPqSystemPrefix(const TString& sysColumn, bool* isTransparent = nullptr);

// Try to strip the __ydb_ prefix from a column name; returns the key portion if matched.
std::optional<TString> SkipYdbSystemPrefix(const TString& sysColumn);

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorByKey(
    const TString& key,
    bool addTransparentPrefix,
    bool includeUserAttributes);

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorBySysColumn(
    const TString& sysColumn,
    bool includeUserAttributes);

// Lookup a meta field descriptor by __ydb_-prefixed column name (e.g. "__ydb_write_time").
std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorByYdbSysColumn(
    const TString& sysColumn,
    bool includeUserAttributes);

std::vector<TString> GetAllowedPqMetaSysColumns(bool addTransparentPrefix, bool includeUserAttributes);

// Returns the list of allowed __ydb_-prefixed system column names.
std::vector<TString> GetAllowedYdbSysColumns(bool includeUserAttributes);

// Map a __ydb_-prefixed column name to the corresponding _yql_sys_-prefixed column name.
// Returns std::nullopt if the input is not a recognized __ydb_ column.
std::optional<TString> YdbSysColumnToOldSysColumn(const TString& ydbColumn, bool addTransparentPrefix);

} // namespace NYql
