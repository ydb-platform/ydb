#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr {
namespace NSysView {

template <typename Schema>
extern void FillSchema(ISystemViewResolver::TSchema& schema);

namespace {

using NKikimrSysView::ESysViewType;
using ESource = ISystemViewResolver::ESource;

// SCHEMAS MODIFICATION POLICY:
// ---------------------------
//  These schemas are CANONICAL and subject to strict modification rules:
//
//  FORBIDDEN:
//  - Changing column data types
//  - Renaming existing columns
//  - Removing columns from the primary key
//  - Deleting existing columns
//
//  ALLOWED:
//  - Adding new columns (use the next available sequential ID)
//  - Adding new schema definitions
//
//  When adding columns, maintain sequential Column IDs and update the TColumns list.
//  When adding tables, maintain sequential Table IDs .

struct Schema : NIceDb::Schema {
    struct ShowCreate : Table<1> {
        struct Path        : Column<1, NScheme::NTypeIds::Utf8> {};
        struct CreateQuery : Column<2, NScheme::NTypeIds::Utf8> {};
        struct PathType    : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path, PathType>;
        using TColumns = TableColumns<
            Path,
            CreateQuery,
            PathType
        >;
    };

};


// SYSTEM VIEWS REGISTRY MODIFICATION POLICY:
// ---------------------------
//  These system view lists are CANONICAL and subject to strict modification rules:
//
//  FORBIDDEN:
//  - Changing sysview types (implementation ids)
//  - Renaming existing sysviews
//  - Adding sysviews with existing names and same source object types
//  - Changing sysview schemas
//  - Deleting existing sysviews
//  - Deleting related source objects of existing sysviews
//
//  ALLOWED:
//  - Adding new sysviews
//  - Adding new source object types of existing sysviews

const TVector<SysViewsRegistryRecord> RewrittenSysViews = {
    {"show_create", ESysViewType::EShowCreate, {},  &FillSchema<Schema::ShowCreate>},
};

const THashMap<ESource, TStringBuf> SourceObjectTypeNames = {
    {ESource::Domain, "domain"},
    {ESource::SubDomain, "subdomain"},
    {ESource::OlapStore, "olap store"},
    {ESource::ColumnTable, "column table"}
};

struct TSysViewDescription {
    ESysViewType Type;
    ISystemViewResolver::TSchema Schema;
};

THashMap<TStringBuf, TSysViewDescription> MakeRewrittenSysViewsMap(const TVector<SysViewsRegistryRecord>& rewrittenSysViews,
    TStringBuf registryName)
{
    THashMap<TStringBuf, TSysViewDescription> sysViewsMap;
    for (const auto& registryRecord : rewrittenSysViews) {
        ISystemViewResolver::TSchema schema;
        registryRecord.FillSchemaFunc(schema);

        UNIT_ASSERT_C(!sysViewsMap.contains(registryRecord.Name),
            TStringBuilder() << "Dupliсate sysview name '" << registryRecord.Name << "'"
                << " among rewritten sysviews in " << registryName << " registry");

        sysViewsMap[registryRecord.Name] = {registryRecord.Type, schema};
    }

    return sysViewsMap;
}

TSet<NTable::TTag> FindColumnsSymmetricDifference(const THashMap<NTable::TTag, TSysTables::TTableColumnInfo>& lhs,
    const THashMap<NTable::TTag, TSysTables::TTableColumnInfo>& rhs)
{
    TSet<NTable::TTag> difference;
    for (const auto& [id, column] : lhs) {
        if (!rhs.contains(id)) {
            difference.emplace(id);
        }
    }

    return difference;
}

void CheckSysViewDescription(const TSysViewDescription& canonicalSysViewDescription,
    const TSysViewDescription& actualSysViewDescription, TStringBuf errorSuffix)
{
    UNIT_ASSERT_C(canonicalSysViewDescription.Type == actualSysViewDescription.Type,
        TStringBuilder() << "Type (implementation id) changed in " << errorSuffix);

    const auto& canonicalSchema = canonicalSysViewDescription.Schema;
    const auto& actualSchema = actualSysViewDescription.Schema;
    const auto& canonicalColumns = canonicalSchema.Columns;
    const auto& actualColumns = actualSchema.Columns;

    const auto deletedColumns = FindColumnsSymmetricDifference(canonicalColumns, actualColumns);
    UNIT_ASSERT_C(deletedColumns.empty(),
        TStringBuilder() << "Column '" << canonicalColumns.at(*deletedColumns.begin()).Name << "'"
            << " was deleted or its column id was changed in " << errorSuffix);

    const auto addedColumns = FindColumnsSymmetricDifference(actualColumns, canonicalColumns);
    UNIT_ASSERT_C(addedColumns.empty(),
        TStringBuilder() << "Column '" << actualColumns.at(*addedColumns.begin()).Name << "'"
            << " was added or its column id was changed in " << errorSuffix);

    for (const auto& [canonicalId, canonicalColumn] : canonicalColumns) {
        const auto actualColumnsIter = actualColumns.find(canonicalId);
        if (actualColumnsIter != actualColumns.end()) {
            const auto& actualColumn = actualColumnsIter->second;
            UNIT_ASSERT_C(canonicalColumn.Name == actualColumn.Name,
                TStringBuilder() << "Column '" << canonicalColumn.Name << "' was renamed in " << errorSuffix);

            const auto canonicalTypeName = NScheme::TypeName(canonicalColumn.PType, canonicalColumn.PTypeMod);
            const auto actualTypeName = NScheme::TypeName(actualColumn.PType, actualColumn.PTypeMod);
            UNIT_ASSERT_C(canonicalTypeName == actualTypeName,
                TStringBuilder() << "Column '" << canonicalColumn.Name << "' changed type in " << errorSuffix);

            UNIT_ASSERT_C(canonicalColumn.KeyOrder == actualColumn.KeyOrder,
                TStringBuilder() << "Primary key differs in " << errorSuffix);

            UNIT_ASSERT_C(canonicalColumn.IsNotNullColumn == actualColumn.IsNotNullColumn,
                TStringBuilder() << "IsNotNull attribute of '" << canonicalColumn.Name << "' column"
                    << " changed in " << errorSuffix);
        }
    }


    UNIT_ASSERT_C(canonicalSchema.KeyColumnTypes.size() == actualSchema.KeyColumnTypes.size(),
        TStringBuilder() << "Primary key differs in " << errorSuffix);

    for (size_t i = 0; i < canonicalSchema.KeyColumnTypes.size(); ++i) {
        UNIT_ASSERT_C(canonicalSchema.KeyColumnTypes[i] == actualSchema.KeyColumnTypes[i],
            TStringBuilder() << "Primary key differs in " << errorSuffix);
    }
}

void CheckSysViewsLists(const THashMap<TStringBuf, TSysViewDescription>& canonicalSysViewsList,
    THashMap<TStringBuf, TSysViewDescription>& actualSysViewsList, TStringBuf sysViewListName)
{
    for (const auto& [sysViewName, sysViewDescription] : canonicalSysViewsList) {
        const auto actualSysViewsIter = actualSysViewsList.find(sysViewName);
        UNIT_ASSERT_C(actualSysViewsIter != actualSysViewsList.end(),
            TStringBuilder() << "Sysview '" << sysViewName << "'"
                << " was deleted from" << sysViewListName << " sysviews");

        CheckSysViewDescription(sysViewDescription, actualSysViewsIter->second,
            TStringBuilder() << "sysview '" << sysViewName << "' from " << sysViewListName << " sysviews");

        actualSysViewsList.erase(actualSysViewsIter);
    }

    UNIT_ASSERT_C(actualSysViewsList.empty(),
        TStringBuilder() << "Sysview '" << actualSysViewsList.begin()->first << "'"
            << " was added to " << sysViewListName << " sysviews");
}

}

Y_UNIT_TEST_SUITE(SysViewsRegistry) {

    Y_UNIT_TEST(RewrittenSysViews) {
        auto canonicalSysViews = MakeRewrittenSysViewsMap(RewrittenSysViews, "canonical");
        auto actualSysViews = MakeRewrittenSysViewsMap(SysViewsRegistry::RewrittenSysViews, "actual");

        CheckSysViewsLists(canonicalSysViews, actualSysViews, "rewritten");
    }
}

} // NSysView
} // NKikimr
