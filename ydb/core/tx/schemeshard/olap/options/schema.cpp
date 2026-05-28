#include "schema.h"

namespace NKikimr::NSchemeShard {

bool TOlapOptionsDescription::ApplyUpdate(const TOlapOptionsUpdate& schemaUpdate, IErrorCollector& /*errors*/) {
    SchemeNeedActualization = schemaUpdate.GetSchemeNeedActualization();
    if (!!schemaUpdate.GetScanReaderPolicyName()) {
        ScanReaderPolicyName = *schemaUpdate.GetScanReaderPolicyName();
    }
    if (schemaUpdate.GetCompactionPlannerConstructor().HasObject()) {
        CompactionPlannerConstructor = schemaUpdate.GetCompactionPlannerConstructor();
    }
    if (schemaUpdate.GetMetadataManagerConstructor().HasObject()) {
        MetadataManagerConstructor = schemaUpdate.GetMetadataManagerConstructor();
    }
    if (schemaUpdate.GetInsertOptionsCompressionEnabled()) {
        InsertOptions.CompressionEnabled = *schemaUpdate.GetInsertOptionsCompressionEnabled();
    }
    if (schemaUpdate.GetInsertOptionsCompressionMinRawBytes()) {
        InsertOptions.CompressionMinRawBytes = *schemaUpdate.GetInsertOptionsCompressionMinRawBytes();
    }
    if (schemaUpdate.GetInsertOptionsBuildIndexesEnabled()) {
        InsertOptions.BuildIndexesEnabled = *schemaUpdate.GetInsertOptionsBuildIndexesEnabled();
    }
    if (schemaUpdate.GetInsertOptionsBuildIndexesMinBlobBytes()) {
        InsertOptions.BuildIndexesMinBlobBytes = *schemaUpdate.GetInsertOptionsBuildIndexesMinBlobBytes();
    }
    return true;
}

void TOlapOptionsDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    SchemeNeedActualization = tableSchema.GetOptions().GetSchemeNeedActualization();
    if (tableSchema.GetOptions().HasScanReaderPolicyName()) {
        ScanReaderPolicyName = tableSchema.GetOptions().GetScanReaderPolicyName();
    }
    if (tableSchema.GetOptions().HasCompactionPlannerConstructor()) {
        AFL_VERIFY(CompactionPlannerConstructor.DeserializeFromProto(tableSchema.GetOptions().GetCompactionPlannerConstructor()));
    }
    if (tableSchema.GetOptions().HasMetadataManagerConstructor()) {
        AFL_VERIFY(MetadataManagerConstructor.DeserializeFromProto(tableSchema.GetOptions().GetMetadataManagerConstructor()));
    }
    if (tableSchema.GetOptions().HasInsertOptions()) {
        const auto& options = tableSchema.GetOptions().GetInsertOptions();
        InsertOptions.CompressionEnabled = options.GetCompressionEnabled();
        InsertOptions.CompressionMinRawBytes = options.GetCompressionMinRawBytes();
        InsertOptions.BuildIndexesEnabled = options.GetBuildIndexesEnabled();
        InsertOptions.BuildIndexesMinBlobBytes = options.GetBuildIndexesMinBlobBytes();
    }
}

void TOlapOptionsDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    tableSchema.MutableOptions()->SetSchemeNeedActualization(SchemeNeedActualization);
    if (ScanReaderPolicyName) {
        tableSchema.MutableOptions()->SetScanReaderPolicyName(*ScanReaderPolicyName);
    }
    if (CompactionPlannerConstructor.HasObject()) {
        CompactionPlannerConstructor.SerializeToProto(*tableSchema.MutableOptions()->MutableCompactionPlannerConstructor());
    }
    if (MetadataManagerConstructor.HasObject()) {
        MetadataManagerConstructor.SerializeToProto(*tableSchema.MutableOptions()->MutableMetadataManagerConstructor());
    }
    if (InsertOptions.CompressionEnabled || InsertOptions.CompressionMinRawBytes || InsertOptions.BuildIndexesEnabled ||
        InsertOptions.BuildIndexesMinBlobBytes) {
        auto& options = *tableSchema.MutableOptions()->MutableInsertOptions();
        options.SetCompressionEnabled(InsertOptions.CompressionEnabled);
        options.SetCompressionMinRawBytes(InsertOptions.CompressionMinRawBytes);
        options.SetBuildIndexesEnabled(InsertOptions.BuildIndexesEnabled);
        options.SetBuildIndexesMinBlobBytes(InsertOptions.BuildIndexesMinBlobBytes);
    }
}

bool TOlapOptionsDescription::ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& /*errors*/) const {
    if (!opSchema.HasOptions()) {
        return true;
    }
    return true;
}

}
