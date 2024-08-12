#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TConverterModifyToAlter {
private:
    TConclusionStatus ParseFromDSRequest(const NKikimrSchemeOp::TTableDescription& dsDescription, NKikimrSchemeOp::TAlterColumnTable& olapDescription) const {
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
            auto parse = ParseFromDSRequest(dsColumn, *olapColumn);
            if (parse.IsFail()) {
                return parse;
            }
        }

        for (auto&& dsColumn : dsDescription.GetDropColumns()) {
            NKikimrSchemeOp::TAlterColumnTableSchema* alterSchema = olapDescription.MutableAlterSchema();
            NKikimrSchemeOp::TOlapColumnDescription* olapColumn = alterSchema->AddDropColumns();
            auto parse = ParseFromDSRequest(dsColumn, *olapColumn);
            if (parse.IsFail()) {
                return parse;
            }
        }
        return TConclusionStatus::Success();
    }

    TConclusionStatus ParseFromDSRequest(const NKikimrSchemeOp::TColumnDescription& dsColumn, NKikimrSchemeOp::TOlapColumnDescription& olapColumn) const {
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
            return TConclusionStatus::Fail("DefaultFromSequence not supported");
        }
        if (dsColumn.HasFamilyName() || dsColumn.HasFamily()) {
            return TConclusionStatus::Fail("FamilyName and Family not supported");
        }
        return TConclusionStatus::Success();
    }
public:
    TConclusion<NKikimrSchemeOp::TAlterColumnTable> Convert(const NKikimrSchemeOp::TModifyScheme& modify) {
        NKikimrSchemeOp::TAlterColumnTable result;
        if (modify.HasAlterColumnTable()) {
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterColumnTable) {
                return TConclusionStatus::Fail("Invalid operation type: " + NKikimrSchemeOp::EOperationType_Name(modify.GetOperationType()));
            }
            result = modify.GetAlterColumnTable();
        } else {
            // from DDL (not known table type)
            if (modify.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterTable) {
                return TConclusionStatus::Fail("Invalid operation type");
            }
            auto parse = ParseFromDSRequest(modify.GetAlterTable(), result);
            if (parse.IsFail()) {
                return parse;
            }
        }

        if (!result.HasName()) {
            return TConclusionStatus::Fail("No table name in Alter");
        }

        if (result.HasAlterSchemaPresetName()) {
            return TConclusionStatus::Fail("Changing table schema is not supported");
        }

        return result;
    }
};

}