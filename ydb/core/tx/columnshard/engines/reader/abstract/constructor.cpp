#include "constructor.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/program/program.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

TConclusionStatus IScannerConstructor::ParseProgram(const TProgramParsingContext& context, const NKikimrSchemeOp::EOlapProgramType programType,
    const TString& serializedProgram, TReadDescription& read, const NArrow::NSSA::IColumnResolver& columnResolver) const {
    std::set<TString> namesChecker;
    if (serializedProgram.empty()) {
        if (!read.ColumnIds.size()) {
            auto schema = read.GetTableMetadataAccessor()->GetSnapshotSchemaVerified(context.GetVersionedSchemas(), read.GetSnapshot());
            read.ColumnIds = std::vector<ui32>(schema->GetColumnIds().begin(), schema->GetColumnIds().end());
        }
        TProgramContainer container;
        YDB_LOG_DEBUG("",
            {"event", "overriden_columns"},
            {"ids", JoinSeq(",", read.ColumnIds)});
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
        (*result)->SetRequestedLimit(ItemsLimit);
        (*result)->SetScanIdentifier(read.GetScanIdentifier());
        return result;
    }
}

TConclusion<std::shared_ptr<NKikimr::NOlap::IScanCursor>> IScannerConstructor::BuildCursorFromProto(
    const NKikimrKqp::TEvKqpScanCursor& proto, const ESourcesSorting sourcesSorting) const {
    auto result = DoBuildCursor(proto.GetImplementationCase());
    if (!result) {
        return TConclusionStatus::Fail(
            TStringBuilder() << "scan cursor implementation " << (ui64)proto.GetImplementationCase() << " cannot be used by this reader");
    }
    const auto protoSorting = SourcesSortingToProto(sourcesSorting);
    const auto tag = LegacyCursorTagFromProto(proto.GetImplementationCase());
    if (tag && *tag != LegacyCursorTag(protoSorting)) {
        return TConclusionStatus::Fail(TStringBuilder() << "scan cursor implementation " << (ui64)proto.GetImplementationCase()
                                                        << " was taken under another sources order than " << (ui64)sourcesSorting);
    }
    auto status = result->DeserializeFromProto(proto);
    if (status.IsFail()) {
        return status;
    }
    result->SetSourcesSorting(protoSorting);
    return result;
}

}   // namespace NKikimr::NOlap::NReader
