#include "constructor.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NReader {

TConclusionStatus IScannerConstructor::ParseProgram(const TProgramParsingContext& context, const NKikimrSchemeOp::EOlapProgramType programType,
    const TString& serializedProgram, TReadDescription& read, const NArrow::NSSA::IColumnResolver& columnResolver) const {
    std::set<TString> namesChecker;
    if (serializedProgram.empty()) {
        if (!read.ColumnIds.size()) {
            auto schema = read.TableMetadataAccessor->GetSnapshotSchemaVerified(context.GetVersionedSchemas(), read.GetSnapshot());
            read.ColumnIds = std::vector<ui32>(schema->GetColumnIds().begin(), schema->GetColumnIds().end());
        }
        TProgramContainer container;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "overriden_columns")("ids", JoinSeq(",", read.ColumnIds));
        //        container.OverrideProcessingColumns(read.ColumnIds);

        {
            NKikimrSSA::TProgram proto;
            auto* command = proto.AddCommand();
            for (auto&& i : read.ColumnIds) {
                command->MutableProjection()->AddColumns()->SetId(i);
            }

            container.Init(columnResolver, proto).Validate();
            read.SetProgram(std::move(container));
        }

        return TConclusionStatus::Success();
    } else {
        TProgramContainer ssaProgram;
        auto statusInit = ssaProgram.Init(columnResolver, programType, serializedProgram);
        if (statusInit.IsFail()) {
            return TConclusionStatus::Fail(TStringBuilder() << "Can't parse SsaProgram: " << statusInit.GetErrorMessage());
        }

        read.SetProgram(std::move(ssaProgram));

        return TConclusionStatus::Success();
    }
}

TConclusion<std::shared_ptr<TReadMetadataBase>> IScannerConstructor::BuildReadMetadata(
    const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    TConclusion<std::shared_ptr<TReadMetadataBase>> result = DoBuildReadMetadata(self, read);
    if (result.IsFail()) {
        return result;
    } else if (!*result) {
        return result.DetachResult();
    } else {
        (*result)->MutableLimitController().SetRequestedLimit(ItemsLimit);
        (*result)->SetScanIdentifier(read.GetScanIdentifier());
        return result;
    }
}

TConclusion<std::shared_ptr<NKikimr::NOlap::IScanCursor>> IScannerConstructor::BuildCursorFromProto(
    const NKikimrKqp::TEvKqpScanCursor& proto) const {
    auto result = DoBuildCursor();
    if (!result) {
        return result;
    }
    auto status = result->DeserializeFromProto(proto);
    if (status.IsFail()) {
        return status;
    }
    return result;
}

}   // namespace NKikimr::NOlap::NReader
