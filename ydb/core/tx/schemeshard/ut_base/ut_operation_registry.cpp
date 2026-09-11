#include <ydb/core/tx/schemeshard/schemeshard_operation_registry.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

using namespace NKikimr::NSchemeShard;

Y_UNIT_TEST_SUITE(TSchemeShardOperationRegistry) {
    Y_UNIT_TEST(EveryProtoOperationIsRegisteredOnce) {
        const auto* descriptor = NKikimrSchemeOp::EOperationType_descriptor();
        THashSet<int> registered;
        for (const auto& info : SchemeOperations) {
            UNIT_ASSERT_C(descriptor->FindValueByNumber(info.Type), static_cast<int>(info.Type));
            UNIT_ASSERT_C(registered.insert(info.Type).second, static_cast<int>(info.Type));
            UNIT_ASSERT(GetSchemeOperationSupport(info.Type) == info.Support);
            UNIT_ASSERT(FindSchemeOperation(info.Type) == &info);
        }
        UNIT_ASSERT_VALUES_EQUAL(registered.size(), descriptor->value_count());
        for (int i = 0; i < descriptor->value_count(); ++i) {
            const auto* value = descriptor->value(i);
            UNIT_ASSERT_C(registered.contains(value->number()), value->name());
        }
    }

    Y_UNIT_TEST(UnknownOperationsHaveNoRegistryEntry) {
        const auto* descriptor = NKikimrSchemeOp::EOperationType_descriptor();
        for (int value = -1; value <= NKikimrSchemeOp::EOperationType_ARRAYSIZE; ++value) {
            if (!descriptor->FindValueByNumber(value)) {
                const auto type = static_cast<NKikimrSchemeOp::EOperationType>(value);
                UNIT_ASSERT(!FindSchemeOperation(type));
            }
        }
    }

    Y_UNIT_TEST(SupportCategories) {
        using namespace NKikimrSchemeOp;
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOpMkDir) == ESchemeOperationSupport::Implemented);
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOpCreateCdcStreamAtTable) == ESchemeOperationSupport::Internal);
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOpAlterView) == ESchemeOperationSupport::Unsupported);
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOpAlterBlobDepot) == ESchemeOperationSupport::Unsupported);
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOpRestoreMultipleIncrementalBackups) == ESchemeOperationSupport::Rejected);
        UNIT_ASSERT(GetSchemeOperationSupport(ESchemeOp_DEPRECATED_35) == ESchemeOperationSupport::Unsupported);
    }
}
