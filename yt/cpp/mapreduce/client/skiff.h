#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/mapreduce/skiff/wire_type.h>
#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <util/generic/vector.h>

namespace NYT::NYson {
struct IYsonConsumer;
} // namespace NYT::NYson

namespace NYT {

struct TClientContext;
enum class ENodeReaderFormat : int;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TCreateSkiffSchemaOptions
{
    using TSelf = TCreateSkiffSchemaOptions;

    FLUENT_FIELD_DEFAULT(bool, HasKeySwitch, false);
    FLUENT_FIELD_DEFAULT(bool, HasRangeIndex, false);

    using TRenameColumnsDescriptor = THashMap<TString, TString>;
    FLUENT_FIELD_OPTION(TRenameColumnsDescriptor, RenameColumns);
};

////////////////////////////////////////////////////////////////////////////////

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TVector<NSkiff::TSkiffSchemaPtr>& tableSchemas,
    const TCreateSkiffSchemaOptions& options);

NSkiff::TSkiffSchemaPtr GetJobInputSkiffSchema();

NSkiff::EWireType ValueTypeToSkiffType(EValueType valueType);

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TTableSchema& schema,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TNode& schemaNode,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

void Serialize(const NSkiff::TSkiffSchemaPtr& schema, NYson::IYsonConsumer* consumer);

void Deserialize(NSkiff::TSkiffSchemaPtr& schema, const TNode& node);

TFormat CreateSkiffFormat(const NSkiff::TSkiffSchemaPtr& schema);

NSkiff::TSkiffSchemaPtr CreateSkiffSchemaIfNecessary(
    const TClientContext& context,
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TTransactionId& transactionId,
    ENodeReaderFormat nodeReaderFormat,
    const TVector<TRichYPath>& tablePaths,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
