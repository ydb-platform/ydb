#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>
#include <ydb/core/tx/columnshard/transactions/locks/write.h>
#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTxInteractions {

Y_UNIT_TEST_SUITE(TxEventLogicalTypes) {
    Y_UNIT_TEST(RangeOperationEncodingsPreserveBoundaries) {
        const std::vector<NScheme::TTypeInfo> logicalTypes = { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)) };
        std::string upperValue(16, '\0');
        upperValue.front() = 2;
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.Append(std::string(16, '\0')).ok());
        UNIT_ASSERT(builder.Append(upperValue).ok());
        const auto values = NArrow::TStatusValidator::GetValid(builder.Finish());
        const auto schema = arrow::schema({ arrow::field("decimal", values->type()) });
        const auto fullSchema =
            NArrow::TStatusValidator::GetValid(schema->AddField(schema->num_fields(), arrow::field(".ydb_operation_type", arrow::uint32())));
        const auto keyBatch = arrow::RecordBatch::Make(schema, 2, { values });
        const NArrow::NMerger::TSortableBatchPosition lowerBound(keyBatch, 0, false, logicalTypes);
        const NArrow::NMerger::TSortableBatchPosition upperBound(keyBatch, 1, false, logicalTypes);
        for (const bool serializedEncoding : { false, true }) {
            for (const bool includeFrom : { false, true }) {
                for (const bool includeTo : { false, true }) {
                    const auto serializedFrom = includeFrom ? NArrow::ECompareType::GREATER_OR_EQUAL : NArrow::ECompareType::GREATER;
                    const auto serializedTo = includeTo ? NArrow::ECompareType::LESS_OR_EQUAL : NArrow::ECompareType::LESS;
                    const auto fromOperation =
                        serializedEncoding ? static_cast<ui32>(serializedFrom)
                                           : static_cast<ui32>(includeFrom ? NKernels::EOperation::GreaterEqual : NKernels::EOperation::Greater);
                    const auto toOperation = serializedEncoding
                                                 ? static_cast<ui32>(serializedTo)
                                                 : static_cast<ui32>(includeTo ? NKernels::EOperation::LessEqual : NKernels::EOperation::Less);
                    arrow::UInt32Builder operationsBuilder;
                    UNIT_ASSERT(operationsBuilder.Append(fromOperation).ok());
                    UNIT_ASSERT(operationsBuilder.Append(toOperation).ok());
                    const auto operations = NArrow::TStatusValidator::GetValid(operationsBuilder.Finish());
                    const auto filter = TPKRangesFilter::BuildFromRecordBatchFull(
                        arrow::RecordBatch::Make(fullSchema, 2, { values, operations }), schema, logicalTypes);
                    const auto serialized = filter->SerializeToRecordBatch(schema);
                    const auto& storedOperations = static_cast<const arrow::UInt32Array&>(*serialized->column(1));
                    UNIT_ASSERT_VALUES_EQUAL(storedOperations.Value(0), static_cast<ui32>(serializedFrom));
                    UNIT_ASSERT_VALUES_EQUAL(storedOperations.Value(1), static_cast<ui32>(serializedTo));
                    const auto restored = TPKRangesFilter::BuildFromString(filter->SerializeToString(schema), schema, logicalTypes);
                    for (const auto& candidate : { filter, restored }) {
                        UNIT_ASSERT_VALUES_EQUAL(candidate->Size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(candidate->begin()->GetPredicateFrom().IsInclude(), includeFrom);
                        UNIT_ASSERT_VALUES_EQUAL(candidate->begin()->GetPredicateTo().IsInclude(), includeTo);
                        UNIT_ASSERT_VALUES_EQUAL(candidate->CheckPoint(lowerBound), includeFrom);
                        UNIT_ASSERT_VALUES_EQUAL(candidate->CheckPoint(upperBound), includeTo);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PrefixRangeRoundTrip) {
        const std::vector<NScheme::TTypeInfo> logicalTypes = { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) };
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.Append(std::string(16, '\0')).ok());
        const auto values = NArrow::TStatusValidator::GetValid(builder.Finish());
        const auto schema = arrow::schema({ arrow::field("decimal", values->type()), arrow::field("uuid", values->type()) });
        const auto prefixSchema = arrow::schema({ schema->field(0) });
        const auto prefixBatch = arrow::RecordBatch::Make(prefixSchema, 1, { values });
        const auto filter = TPKRangesFilter::BuildFromRecordBatchLines(prefixBatch, { logicalTypes.front() });
        const auto serialized = filter->SerializeToRecordBatch(schema);
        UNIT_ASSERT_VALUES_EQUAL(serialized->num_columns(), 3);
        UNIT_ASSERT_VALUES_EQUAL(serialized->num_rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(serialized->column(1)->null_count(), 2);
        const auto restored = TPKRangesFilter::BuildFromString(filter->SerializeToString(schema), schema, logicalTypes);
        UNIT_ASSERT_VALUES_EQUAL(restored->Size(), 1);
        const auto& range = *restored->begin();
        for (const auto* predicate : { &range.GetPredicateFrom(), &range.GetPredicateTo() }) {
            UNIT_ASSERT_VALUES_EQUAL(predicate->NumColumns(), 1);
            const auto& restoredTypes = predicate->GetSortableBatchPosition().GetSorting()->GetLogicalTypes();
            UNIT_ASSERT_VALUES_EQUAL(restoredTypes.size(), 1);
            UNIT_ASSERT(restoredTypes.front() == logicalTypes.front());
        }
        UNIT_ASSERT(range.GetPredicateFrom().IsEqualPointTo(filter->begin()->GetPredicateFrom()));
        UNIT_ASSERT(range.GetPredicateTo().IsEqualPointTo(filter->begin()->GetPredicateTo()));
    }

    Y_UNIT_TEST(ReadStartRoundTripAndWriteConflicts) {
        const std::vector<NScheme::TTypeInfo> logicalTypes = { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)),
            NScheme::TTypeInfo(NScheme::TDecimalType(35, 2)), NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) };
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.Append(std::string(16, '\0')).ok());
        const auto values = NArrow::TStatusValidator::GetValid(builder.Finish());
        const auto schema = arrow::schema(
            { arrow::field("decimal1", values->type()), arrow::field("decimal2", values->type()), arrow::field("uuid", values->type()) });
        const auto batch = arrow::RecordBatch::Make(schema, 1, { values, values, values });
        const auto filter = TPKRangesFilter::BuildFromRecordBatchLines(batch, logicalTypes);

        arrow::UInt32Builder operationBuilder;
        UNIT_ASSERT(operationBuilder.Append(static_cast<ui32>(NKernels::EOperation::Equal)).ok());
        auto equalColumns = batch->columns();
        equalColumns.emplace_back(NArrow::TStatusValidator::GetValid(operationBuilder.Finish()));
        const auto fullSchema =
            NArrow::TStatusValidator::GetValid(schema->AddField(schema->num_fields(), arrow::field(".ydb_operation_type", arrow::uint32())));
        const auto equalFilter =
            TPKRangesFilter::BuildFromRecordBatchFull(arrow::RecordBatch::Make(fullSchema, 1, equalColumns), schema, logicalTypes);
        UNIT_ASSERT_VALUES_EQUAL(equalFilter->Size(), 1);
        UNIT_ASSERT(equalFilter->begin()->GetPredicateFrom().IsEqualPointTo(filter->begin()->GetPredicateFrom()));
        UNIT_ASSERT(equalFilter->begin()->GetPredicateTo().IsEqualPointTo(filter->begin()->GetPredicateTo()));

        const auto pathId =
            NColumnShard::TUnifiedPathId::BuildValid(TInternalPathId::FromRawValue(1), NColumnShard::TSchemeShardLocalPathId::FromRawValue(1));
        constexpr ui64 ReadLockId = 10;
        constexpr ui64 WriteLockId = 20;
        TTxEventContainer original(ReadLockId, std::make_shared<TEvReadStart>(pathId, schema, filter));

        TTxEventContainer restoredFromProto(ReadLockId);
        UNIT_ASSERT(restoredFromProto.DeserializeFromProto(original.SerializeToProto(), logicalTypes));
        TTxEventContainer restoredFromString(ReadLockId);
        auto inputTypes = logicalTypes;
        const auto status = restoredFromString.DeserializeFromString(original.SerializeToString(), inputTypes);
        UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
        inputTypes.clear();

        for (const auto* restored : { &restoredFromProto, &restoredFromString }) {
            const auto& restoredEvent = restored->GetAsSafe<TEvReadStart>();
            UNIT_ASSERT(restoredEvent.GetSchema()->Equals(schema));
            UNIT_ASSERT_VALUES_EQUAL(restoredEvent.GetFilter()->Size(), 1);
            const auto& range = *restoredEvent.GetFilter()->begin();
            for (const auto* predicate : { &range.GetPredicateFrom(), &range.GetPredicateTo() }) {
                const auto& restoredTypes = predicate->GetSortableBatchPosition().GetSorting()->GetLogicalTypes();
                UNIT_ASSERT(restoredTypes == logicalTypes);
                UNIT_ASSERT_VALUES_EQUAL(restoredTypes[0].GetDecimalType().GetPrecision(), 22);
                UNIT_ASSERT_VALUES_EQUAL(restoredTypes[0].GetDecimalType().GetScale(), 9);
                UNIT_ASSERT_VALUES_EQUAL(restoredTypes[1].GetDecimalType().GetPrecision(), 35);
                UNIT_ASSERT_VALUES_EQUAL(restoredTypes[1].GetDecimalType().GetScale(), 2);
                UNIT_ASSERT_VALUES_EQUAL(restoredTypes[2].GetTypeId(), NScheme::NTypeIds::Uuid);
            }
            UNIT_ASSERT(range.GetPredicateFrom().IsEqualPointTo(filter->begin()->GetPredicateFrom()));
            UNIT_ASSERT(range.GetPredicateTo().IsEqualPointTo(filter->begin()->GetPredicateTo()));
        }

        TInteractionsContext context;
        original.AddToInteraction(context);
        TTxConflicts conflicts;
        TTxConflicts notifications;
        TEvWriteWriter writer(pathId.InternalPathId, batch, schema, logicalTypes);
        UNIT_ASSERT(writer.CheckInteraction(WriteLockId, context, conflicts, notifications));
        const auto brokenLockIds = conflicts.GetBrokenTxIds(WriteLockId);
        UNIT_ASSERT_VALUES_EQUAL(brokenLockIds.size(), 1);
        UNIT_ASSERT(brokenLockIds.contains(ReadLockId));
        UNIT_ASSERT(notifications.IsEmpty());
        original.RemoveFromInteraction(context);
        UNIT_ASSERT(writer.CheckInteraction(WriteLockId, context, conflicts, notifications));
        UNIT_ASSERT(conflicts.IsEmpty());
    }
}

}   // namespace NKikimr::NOlap::NTxInteractions
