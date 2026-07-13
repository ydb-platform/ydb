#include "mkql_block_serializer_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_block_transport.h>
#include <yql/essentials/minikql/computation/mkql_datum_validate.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/utils/chunked_buffer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> DoSerializerRoundtrip(
    const std::shared_ptr<arrow::ArrayData>& arrayData, TType* itemType, TType* blockType)
{
    const ui64 blockLen = static_cast<ui64>(arrayData->length);
    auto* pool = arrow::default_memory_pool();
    TBlockSerializerParams params(pool, Nothing(), /*shouldSerializeOffset=*/true);
    auto serializer = MakeBlockSerializer(TTypeInfoHelper(), itemType, params);
    auto deserializer = MakeBlockDeserializer(TTypeInfoHelper(), itemType, params);

    TVector<ui64> metadata;
    serializer->StoreMetadata(*arrayData, [&](ui64 meta) { metadata.push_back(meta); });
    NYql::TChunkedBuffer buffer;
    serializer->StoreArray(*arrayData, buffer);

    size_t metaIdx = 0;
    deserializer->LoadMetadata([&]() -> ui64 { return metadata[metaIdx++]; });
    auto restored = deserializer->LoadArray(buffer, blockLen, TMaybe<size_t>(0));

    ValidateDatum(restored, Nothing(), blockType, NYql::EDatumValidationMode::Expensive);
    UNIT_ASSERT_VALUES_EQUAL(restored->length, arrayData->length);
    return restored;
}

} // namespace NKikimr::NMiniKQL
