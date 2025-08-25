#include "table_upload_options.h"
#include "helpers.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NCompression;
using namespace NCypressClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEpochSchema::TEpochSchema(const TEpochSchema& other)
{
    *this = other;
}

TEpochSchema& TEpochSchema::operator=(const TEpochSchema& other)
{
    TableSchema_ = other.TableSchema_;
    Revision_ += other.Revision_ + 1;
    return *this;
}

TEpochSchema& TEpochSchema::operator=(TTableSchemaPtr schema)
{
    Set(schema);
    return *this;
}

const TTableSchema* TEpochSchema::operator->() const
{
    return TableSchema_.Get();
}

const TTableSchemaPtr& TEpochSchema::operator*() const
{
    return TableSchema_;
}

const TTableSchemaPtr& TEpochSchema::Get() const
{
    return TableSchema_;
}

ui64 TEpochSchema::GetRevision() const
{
    return Revision_;
}

ui64 TEpochSchema::Set(const TTableSchemaPtr& schema)
{
    TableSchema_ = schema;
    return ++Revision_;
}

void TEpochSchema::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Revision_);
    Persist<TNonNullableIntrusivePtrSerializer<>>(context, TableSchema_);
}

ui64 TEpochSchema::Reset()
{
    TableSchema_ = New<TTableSchema>();
    return ++Revision_;
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr TTableUploadOptions::GetUploadSchema() const
{
    switch (SchemaModification) {
        case ETableSchemaModification::None:
            return TableSchema.Get();

        case ETableSchemaModification::UnversionedUpdate:
            return TableSchema->ToUnversionedUpdate(/*sorted*/ true);

        default:
            YT_ABORT();
    }
}

void TTableUploadOptions::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UpdateMode);
    Persist(context, LockMode);
    Persist(context, TableSchema);
    Persist(context, SchemaId);
    Persist(context, SchemaModification);
    // COMPAT(dave11ar): NControllerAgent::ESnapshotVersion::VersionedMapReduceWrite
    if (context.GetVersion() >= 301602) {
        Persist(context, VersionedWriteOptions);
    }
    Persist(context, SchemaMode);
    Persist(context, OptimizeFor);
    Persist(context, ChunkFormat);
    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
    Persist(context, EnableStripedErasure);
    Persist(context, SecurityTags);
    Persist(context, PartiallySorted);
}

////////////////////////////////////////////////////////////////////////////////

static void ValidateSortColumnsEqual(const TSortColumns& sortColumns, const TTableSchema& schema)
{
    if (sortColumns != schema.GetSortColumns()) {
        THROW_ERROR_EXCEPTION("YPath attribute \"sorted_by\" must be compatible with table schema for a \"strong\" schema mode")
            << TErrorAttribute("sort_columns", sortColumns)
            << TErrorAttribute("table_schema", schema);
    }
}

static void ValidateAppendKeyColumns(const TSortColumns& sortColumns, const TTableSchema& schema, i64 rowCount)
{
    ValidateSortColumns(sortColumns);

    if (rowCount == 0) {
        return;
    }

    auto tableSortColumns = schema.GetSortColumns();
    bool areKeyColumnsCompatible = true;
    if (tableSortColumns.size() < sortColumns.size()) {
        areKeyColumnsCompatible = false;
    } else {
        for (int i = 0; i < std::ssize(sortColumns); ++i) {
            if (tableSortColumns[i] != sortColumns[i]) {
                areKeyColumnsCompatible = false;
                break;
            }
        }
    }

    if (!areKeyColumnsCompatible) {
        THROW_ERROR_EXCEPTION("Sort columns mismatch while trying to append sorted data into a non-empty table")
            << TErrorAttribute("append_sort_columns", sortColumns)
            << TErrorAttribute("table_sort_columns", tableSortColumns);
    }
}

const std::vector<std::string>& GetTableUploadOptionsAttributeKeys()
{
    static const std::vector<std::string> Result{
        "schema_mode",
        "optimize_for",
        "chunk_format",
        "compression_codec",
        "erasure_codec",
        "enable_striped_erasure",
        "dynamic"
    };
    return Result;
}

TTableUploadOptions GetTableUploadOptions(
    const TRichYPath& path,
    const IAttributeDictionary& cypressTableAttributes,
    const TTableSchemaPtr& schema,
    i64 rowCount)
{
    auto schemaMode = cypressTableAttributes.Get<ETableSchemaMode>("schema_mode");
    auto optimizeFor = cypressTableAttributes.Get<EOptimizeFor>("optimize_for");
    auto chunkFormat = cypressTableAttributes.Find<EChunkFormat>("chunk_format");
    auto compressionCodec = cypressTableAttributes.Get<NCompression::ECodec>("compression_codec");
    auto erasureCodec = cypressTableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
    auto enableStripedErasure = cypressTableAttributes.Get<bool>("enable_striped_erasure", false);
    auto dynamic = cypressTableAttributes.Get<bool>("dynamic");

    // Validate "optimize_for" and "chunk_format" compatibility.
    if (chunkFormat) {
        ValidateTableChunkFormatAndOptimizeFor(*chunkFormat, optimizeFor);
    }

    // Some ypath attributes are not compatible with attribute "schema".
    if (path.GetAppend() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"schema\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (!path.GetSortedBy().empty() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"sorted_by\" and \"schema\" are not compatible")
            << TErrorAttribute("path", path);
    }

    // Dynamic tables have their own requirements as well.
    if (dynamic) {
        if (path.GetSchema()) {
            THROW_ERROR_EXCEPTION("YPath attribute \"schema\" cannot be set on a dynamic table")
                << TErrorAttribute("path", path);
        }

        if (!path.GetSortedBy().empty()) {
            THROW_ERROR_EXCEPTION("YPath attribute \"sorted_by\" cannot be set on a dynamic table")
                << TErrorAttribute("path", path);
        }
    }

    TTableUploadOptions result;
    // NB: Saving schema to make sure that if changes are applied to it the schema revision also changes.
    result.TableSchema = schema;
    auto pathSchema = path.GetSchema();
    if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateSortColumnsEqual(path.GetSortedBy(), *schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
    } else if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour.
        ValidateAppendKeyColumns(path.GetSortedBy(), *schema, rowCount);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromSortColumns(path.GetSortedBy());
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = (schema->IsSorted() && !dynamic) ? ELockMode::Exclusive : ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour - reset key columns if there were any.
        result.LockMode = ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema.Reset();
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateSortColumnsEqual(path.GetSortedBy(), *schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromSortColumns(path.GetSortedBy());
    } else if (!path.GetAppend() && pathSchema && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = pathSchema;
    } else if (!path.GetAppend() && pathSchema && (schemaMode == ETableSchemaMode::Weak)) {
        // Change from Weak to Strong schema mode.
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = pathSchema;
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema.Reset();
    } else {
        // Do not use YT_ABORT here, since this code is executed inside scheduler.
        THROW_ERROR_EXCEPTION("Failed to define upload parameters")
            << TErrorAttribute("path", path)
            << TErrorAttribute("schema_mode", schemaMode)
            << TErrorAttribute("schema", *schema);
    }

    if (path.GetAppend() && path.GetOptimizeFor()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"optimize_for\" are not compatible")
            << TErrorAttribute("path", path);
    }

    result.OptimizeFor = path.GetOptimizeFor() ? *path.GetOptimizeFor() : optimizeFor;
    result.ChunkFormat = path.GetChunkFormat() ? *path.GetChunkFormat() : chunkFormat;

    if (path.GetAppend() && path.GetCompressionCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"compression_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetCompressionCodec()) {
        result.CompressionCodec = *path.GetCompressionCodec();
    } else {
        result.CompressionCodec = compressionCodec;
    }

    if (path.GetAppend() && path.GetErasureCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"erasure_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    result.ErasureCodec = path.GetErasureCodec().value_or(erasureCodec);
    result.EnableStripedErasure = enableStripedErasure;

    if (path.GetSchemaModification() == ETableSchemaModification::UnversionedUpdateUnsorted) {
        THROW_ERROR_EXCEPTION("YPath attribute \"schema_modification\" cannot have value %Qlv for output tables",
            path.GetSchemaModification())
            << TErrorAttribute("path", path);
    } else if (!dynamic && path.GetSchemaModification() != ETableSchemaModification::None) {
        THROW_ERROR_EXCEPTION("YPath attribute \"schema_modification\" can have value %Qlv only for dynamic tables",
            path.GetSchemaModification())
            << TErrorAttribute("path", path);
    }
    result.SchemaModification = path.GetSchemaModification();

    auto versionedWriteOptions = path.GetVersionedWriteOptions();
    if (!dynamic && versionedWriteOptions.WriteMode != EVersionedIOMode::Default) {
        THROW_ERROR_EXCEPTION("YPath attribute \"versioned_write_options/write_mode\" can have value %Qlv only for dynamic tables",
            versionedWriteOptions.WriteMode)
            << TErrorAttribute("path", path);
    }
    if (versionedWriteOptions.WriteMode != EVersionedIOMode::Default && path.GetSchemaModification() != ETableSchemaModification::None) {
        THROW_ERROR_EXCEPTION("YPath attributes \"versioned_write_options/write_mode\" and \"schema_modification\""
            "can not be set in non-trivial state together: \"versioned_write_options/write_mode\" is %Qlv, \"schema_modification\" is %Qlv",
            versionedWriteOptions.WriteMode,
            path.GetSchemaModification())
            << TErrorAttribute("path", path);
    }
    result.VersionedWriteOptions = versionedWriteOptions;

    if (!dynamic && path.GetPartiallySorted()) {
        THROW_ERROR_EXCEPTION("YPath attribute \"partially_sorted\" can be set only for dynamic tables")
            << TErrorAttribute("path", path);
    }
    result.PartiallySorted = path.GetPartiallySorted();

    result.SecurityTags = path.GetSecurityTags();

    return result;
}

TTableUploadOptions GetFileUploadOptions(
    const TRichYPath& path,
    const IAttributeDictionary& cypressTableAttributes)
{
    auto compressionCodec = cypressTableAttributes.Get<NCompression::ECodec>("compression_codec");
    auto enableStripedErasure = cypressTableAttributes.Get<bool>("enable_striped_erasure", false);
    auto erasureCodec = cypressTableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

    if (path.GetAppend()) {
        THROW_ERROR_EXCEPTION("Attribute \"append\" is not supported for files")
            << TErrorAttribute("path", path);
    }

    // NB(coteeq): Fill for sanity. They should not have impact on behaviour, because
    //             RichYPath's compression_codec & erasure_codec are disallowed in remote copy.
    // TODO(coteeq): Make it YT_VERIFY
    if (path.GetCompressionCodec() || path.GetErasureCodec()) {
        THROW_ERROR_EXCEPTION("\"compression_codec\" and \"erasure_codec\" are disallowed for files")
            << TErrorAttribute("path", path);
    }

    TTableUploadOptions result;
    result.CompressionCodec = compressionCodec;
    result.ErasureCodec = erasureCodec;
    result.EnableStripedErasure = enableStripedErasure;
    result.SecurityTags = path.GetSecurityTags();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
