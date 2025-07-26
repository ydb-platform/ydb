#pragma once

#include <library/cpp/yson/node/node.h>
#include <util/generic/vector.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/core/tx/scheme_cache/helpers.h>

namespace NKikimr::NReplication::NTransfer {

struct SystemColumns {
    static constexpr const char* Root = "__ydb_r";

    static constexpr const char* TargetTable = "__ydb_table";
};

struct TSchemeColumn {
    TString Name;
    ui32 Id;
    NScheme::TTypeInfo PType;
    bool KeyColumn;
    bool Nullable;

    bool operator==(const TSchemeColumn& other) const = default;

    TString ToString() const;
    TString TypeName() const;
};

struct TScheme {
    using TPtr = std::shared_ptr<TScheme>;

    TVector<TSchemeColumn> TopicColumns;
    TVector<TSchemeColumn> TableColumns;

    TVector<NKikimrKqp::TKqpColumnMetadataProto> StructMetadata;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> ColumnsMetadata;

    size_t TargetTableIndex;

    std::vector<ui32> WriteIndex;
    std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> Types = std::make_shared<TVector<std::pair<TString, Ydb::Type>>>();
};

TScheme BuildScheme(const TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& nav);
NYT::TNode MakeOutputSchema(const TVector<TSchemeColumn>& columns);

}
