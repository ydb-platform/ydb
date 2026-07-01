#include "mkql_block_test_helper.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>

#include <util/generic/guid.h>

namespace NKikimr::NMiniKQL {

namespace {

TGUID MakeTestGuid(ui8 fill) {
    TGUID guid;
    std::memset(&guid, fill, sizeof(guid));
    return guid;
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockUuidTest) {

Y_UNIT_TEST(UuidScalarRoundtrip) {
    TBlockHelper helper;
    const TGUID guid = MakeTestGuid(0xAB);

    auto [graph, value, itemType, blockType] = helper.GetScalarBlock(guid);
    Y_UNUSED(graph);
    Y_UNUSED(blockType);

    const auto& datum = TArrowBlock::From(value).GetDatum();
    UNIT_ASSERT(datum.is_scalar());
    UNIT_ASSERT_EQUAL(datum.type()->id(), arrow::Type::FIXED_SIZE_BINARY);
    const auto* fsbType = dynamic_cast<const arrow::FixedSizeBinaryType*>(datum.scalar()->type.get());
    UNIT_ASSERT_C(fsbType != nullptr, "Expected FixedSizeBinaryType");
    UNIT_ASSERT_VALUES_EQUAL(fsbType->byte_width(), sizeof(TGUID));

    auto blockReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    const auto item = blockReader->GetScalarItem(*datum.scalar());
    const TStringBuf expectedGuid(reinterpret_cast<const char*>(&guid), sizeof(guid));
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(item.AsStringRef()), expectedGuid);
}

Y_UNIT_TEST(UuidListRoundtrip) {
    const TVector<TGUID> data = {MakeTestGuid(1), MakeTestGuid(2), MakeTestGuid(3)};
    TBlockHelper helper;
    auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
    Y_UNUSED(graph);

    auto iterator = listValue.GetListIterator();
    NUdf::TUnboxedValue val;
    for (const auto& guid : data) {
        UNIT_ASSERT(iterator.Next(val));
        const TStringBuf expected(reinterpret_cast<const char*>(&guid), sizeof(guid));
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(val.AsStringRef()), expected);
    }
    UNIT_ASSERT(!iterator.Next(val));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockUuidTest)

} // namespace NKikimr::NMiniKQL
