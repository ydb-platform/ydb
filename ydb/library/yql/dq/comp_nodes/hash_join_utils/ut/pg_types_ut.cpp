#include "scalar_layout_converter_utils.h"

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(THashJoinPgTypesTest) {
    Y_UNIT_TEST(RejectPgTypes) {
        TScalarLayoutConverterTestData data;
        auto& pb = data.PgmBuilder;
        auto* intType = pb.NewDataType(NUdf::EDataSlot::Int64);
        for (ui32 typeId : {23u, 25u}) { // int4 (by value), text (by reference)
            auto* pgType = pb.NewPgType(typeId);
            auto* structType = pb.NewStructType({{"value", pgType}});
            const TVector<TType*> types = {
                pgType,
                pb.NewOptionalType(pgType),
                pb.NewOptionalType(pb.NewOptionalType(pgType)),
                pb.NewTaggedType(pgType, "pg"),
                structType,
                pb.NewOptionalType(structType),
                pb.NewTupleType({intType, structType}),
                pb.NewListType(structType),
                pb.NewDictType(pgType, intType, false),
                pb.NewDictType(intType, structType, false),
                pb.NewVariantType(pb.NewTupleType({intType, structType})),
                pb.NewTaggedType(pb.NewOptionalType(structType), "nested_pg"),
            };
            for (auto* type : types) {
                for (auto role : {NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload}) {
                    UNIT_ASSERT_EXCEPTION_CONTAINS(
                        MakeScalarLayoutConverter(TTypeInfoHelper(), {type}, {role}, data.HolderFactory),
                        yexception, "PG types are not supported in hash join");
                    UNIT_ASSERT_EXCEPTION_CONTAINS(
                        MakeBlockLayoutConverter(TTypeInfoHelper(), {type}, {role}, NUdf::GetYqlMemoryPool()),
                        yexception, "PG types are not supported in hash join");
                }
            }
        }
    }
}

} // namespace NKikimr::NMiniKQL
