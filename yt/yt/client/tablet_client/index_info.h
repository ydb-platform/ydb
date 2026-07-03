#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt_proto/yt/client/tablet_client/proto/secondary_index.pb.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TUnfoldedColumns
    : public NYTree::TYsonStructLite
{
    std::string TableColumn;
    std::string IndexColumn;

    TUnfoldedColumns(std::string tableColumn, std::string indexColumn);

    void Persist(const TStreamPersistenceContext& context);

    REGISTER_YSON_STRUCT_LITE(TUnfoldedColumns);

    static void Register(TRegistrar registrar);
};

void ToProto(NProto::TUnfoldedColumns* serialized, const TUnfoldedColumns& original);
void FromProto(TUnfoldedColumns* original, const NProto::TUnfoldedColumns& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TIndexInfo final
    : public NYTree::TYsonStructLite
{
    NObjectClient::TObjectId IndexObjectId;
    ESecondaryIndexKind Kind;
    std::optional<std::string> Predicate;
    std::optional<TUnfoldedColumns> UnfoldedColumns;
    ETableToIndexCorrespondence Correspondence;
    NTableClient::TTableSchemaPtr EvaluatedColumnsSchema;

    REGISTER_YSON_STRUCT_LITE(TIndexInfo);

    static void Register(TRegistrar registrar);
};

void ToProto(NProto::TIndexInfo* serialized, const TIndexInfo& original);
void FromProto(TIndexInfo* original, const NProto::TIndexInfo& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
