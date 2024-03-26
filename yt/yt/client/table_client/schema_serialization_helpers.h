#pragma once

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

struct TMaybeDeletedColumnSchema : public TColumnSchema
{
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, Deleted);

    TDeletedColumn GetDeletedColumnSchema() const;
};

void Deserialize(TMaybeDeletedColumnSchema& schema, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TMaybeDeletedColumnSchema& schema, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TSerializableColumnSchema
    : public TMaybeDeletedColumnSchema
    , public NYTree::TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TSerializableColumnSchema);

    static void Register(TRegistrar registrar);

    void RunPostprocessor();

public:
    void DeserializeFromCursor(NYson::TYsonPullParserCursor* cursor);

    void SetColumnSchema(const TColumnSchema& columnSchema);
    void SetDeletedColumnSchema(const TDeletedColumn& deletedColumnSchema);

private:
    std::optional<TColumnStableName> SerializedStableName_;

    std::optional<ESimpleLogicalValueType> LogicalTypeV1_;
    std::optional<bool> RequiredV1_;

    std::optional<TTypeV3LogicalTypeWrapper> LogicalTypeV3_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
