#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TConverterModifyToAlter {
private:
    bool ParseFromDSRequest(const NKikimrSchemeOp::TTableDescription& dsDescription, NKikimrSchemeOp::TAlterColumnTable& olapDescription, IErrorCollector& errors) const {
        olapDescription.SetName(dsDescription.GetName());

        if (dsDescription.HasTTLSettings()) {
            auto& tableTtl = dsDescription.GetTTLSettings();
            NKikimrSchemeOp::TColumnDataLifeCycle* alterTtl = olapDescription.MutableAlterTtlSettings();
            if (tableTtl.HasEnabled()) {
                auto& enabled = tableTtl.GetEnabled();
                auto* alterEnabled = alterTtl->MutableEnabled();
                if (enabled.HasColumnName()) {
                    alterEnabled->SetColumnName(enabled.GetColumnName());
                }
                if (enabled.HasExpireAfterSeconds()) {
                    alterEnabled->SetExpireAfterSeconds(enabled.GetExpireAfterSeconds());
                }
                if (enabled.HasColumnUnit()) {
                    alterEnabled->SetColumnUnit(enabled.GetColumnUnit());
                }
            } else if (tableTtl.HasDisabled()) {
                alterTtl->MutableDisabled();
            }
            if (tableTtl.HasUseTiering()) {
                alterTtl->SetUseTiering(tableTtl.GetUseTiering());
            }
        }

        for (auto&& dsColumn : dsDescription.GetColumns()) {
            NKikimrSchemeOp::TAlterColumnTableSchema* alterSchema = olapDescription.MutableAlterSchema();
            NKikimrSchemeOp::TOlapColumnDescription* olapColumn = alterSchema->AddAddColumns();
            if (!ParseFromDSRequest(dsColumn, *olapColumn, errors)) {
                return false;
            }
        }

        for (auto&& dsColumn : dsDescription.GetDropColumns()) {
            NKikimrSchemeOp::TAlterColumnTableSchema* alterSchema = olapDescription.MutableAlterSchema();
            NKikimrSchemeOp::TOlapColumnDescription* olapColumn = alterSchema->AddDropColumns();
            if (!ParseFromDSRequest(dsColumn, *olapColumn, errors)) {
                return false;
            }
        }
        return true;
    }

    bool ParseFromDSRequest(const NKikimrSchemeOp::TColumnDescription& dsColumn, NKikimrSchemeOp::TOlapColumnDescription& olapColumn, IErrorCollector& errors) const {
        olapColumn.SetName(dsColumn.GetName());
        olapColumn.SetType(dsColumn.GetType());
        if (dsColumn.HasTypeId()) {
            olapColumn.SetTypeId(dsColumn.GetTypeId());
        }
        if (dsColumn.HasTypeInfo()) {
            *olapColumn.MutableTypeInfo() = dsColumn.GetTypeInfo();
        }
        if (dsColumn.HasNotNull()) {
            olapColumn.SetNotNull(dsColumn.GetNotNull());
        }
        if (dsColumn.HasId()) {
            olapColumn.SetId(dsColumn.GetId());
        }
        if (dsColumn.HasDefaultFromSequence()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "DefaultFromSequence not supported");
            return false;
        }
        if (dsColumn.HasFamilyName() || dsColumn.HasFamily()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "FamilyName and Family not supported");
            return false;
        }
        return true;
    }
public:
    std::optional<NKikimrSchemeOp::TAlterColumnTable> Convert(const NKikimrSchemeOp::TModifyScheme& modify, IErrorCollector& errors) {
        NKikimrSchemeOp::TAlterColumnTable result;
        if (modify.HasAlterColumnTable()) {
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterColumnTable) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "Invalid operation type");
                return std::nullopt;
            }
            result = modify.GetAlterColumnTable();
        } else {
            // from DDL (not known table type)
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterTable) {
                errors.AddError(NKikimrScheme::StatusSchemeError, "Invalid operation type");
                return std::nullopt;
            }
            if (!ParseFromDSRequest(modify.GetAlterTable(), result, errors)) {
                return std::nullopt;
            }
        }

        if (!result.HasName()) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "No table name in Alter");
            return std::nullopt;
        }

        if (result.HasAlterSchemaPresetName()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "Changing table schema is not supported");
            return std::nullopt;
        }

        if (result.HasRESERVED_AlterTtlSettingsPresetName()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "TTL presets are not supported");
            return std::nullopt;
        }

        return result;
    }
};

}