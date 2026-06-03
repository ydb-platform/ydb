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
    if (schemaUpdate.GetInsertPromoteOptionsEnabled()) {
        InsertPromoteOptions.Enabled = *schemaUpdate.GetInsertPromoteOptionsEnabled();
    }
    if (schemaUpdate.GetInsertPromoteOptionsMinBlobBytes()) {
        InsertPromoteOptions.MinBlobBytes = *schemaUpdate.GetInsertPromoteOptionsMinBlobBytes();
    }
    if (schemaUpdate.GetInsertPromoteOptionsBuildIndexesEnabled()) {
        InsertPromoteOptions.BuildIndexesEnabled = *schemaUpdate.GetInsertPromoteOptionsBuildIndexesEnabled();
    }
    if (schemaUpdate.GetInsertPromoteOptionsCompactionTargetLevel()) {
        InsertPromoteOptions.CompactionTargetLevel = *schemaUpdate.GetInsertPromoteOptionsCompactionTargetLevel();
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
    if (tableSchema.GetOptions().HasInsertPromoteOptions()) {
        const auto& options = tableSchema.GetOptions().GetInsertPromoteOptions();
        InsertPromoteOptions.Enabled = options.GetEnabled();
        InsertPromoteOptions.MinBlobBytes = options.GetMinBlobBytes();
        InsertPromoteOptions.BuildIndexesEnabled = options.GetBuildIndexesEnabled();
        InsertPromoteOptions.CompactionTargetLevel = options.GetCompactionTargetLevel();
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
    if (InsertPromoteOptions.Enabled || InsertPromoteOptions.MinBlobBytes || InsertPromoteOptions.BuildIndexesEnabled ||
        InsertPromoteOptions.CompactionTargetLevel) {
        auto& options = *tableSchema.MutableOptions()->MutableInsertPromoteOptions();
        options.SetEnabled(InsertPromoteOptions.Enabled);
        options.SetMinBlobBytes(InsertPromoteOptions.MinBlobBytes);
        options.SetBuildIndexesEnabled(InsertPromoteOptions.BuildIndexesEnabled);
        options.SetCompactionTargetLevel(InsertPromoteOptions.CompactionTargetLevel);
    }
}

bool TOlapOptionsDescription::ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& /*errors*/) const {
    if (!opSchema.HasOptions()) {
        return true;
    }
    return true;
}

}
