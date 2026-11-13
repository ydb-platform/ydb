#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine_layout.h>
#include <ydb/library/yql/dq/comp_nodes/dq_rh_hash.h>

#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <string>

namespace NKikimr::NMiniKQL {
namespace {

using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

constexpr size_t StringHeaderSize = sizeof(*TUnboxedValuePod{}.AsRawStringValue());

class TLayoutTestEnv {
public:
    TLayoutTestEnv()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("DqHashCombineLayoutTest")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , ValueBuilder(HolderFactory)
    {
    }

    template <typename T>
    TType* Data() {
        return TDataType::Create(NUdf::TDataType<T>::Id, Env);
    }

    TType* Optional(TType* item) {
        return TOptionalType::Create(item, Env);
    }

    TUnboxedValue Array(std::initializer_list<TUnboxedValuePod> values) {
        TUnboxedValue* items = nullptr;
        TUnboxedValue result = ValueBuilder.NewArray(values.size(), items);
        for (const auto& value : values) {
            *items++ = value;
        }
        return result;
    }

private:
    TScopedAlloc Alloc;

public:
    TTypeEnvironment Env;

private:
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder ValueBuilder;
};

class TStorage {
public:
    explicit TStorage(size_t size)
        : Words((size + sizeof(std::max_align_t) - 1) / sizeof(std::max_align_t))
    {
    }

    void* Data() { return Words.data(); }
    const void* Data() const { return Words.data(); }

private:
    std::vector<std::max_align_t> Words;
};

class TIndirectComposite final: public NUdf::TBoxedValue {
public:
    explicit TIndirectComposite(std::vector<TUnboxedValue> values)
        : Values(std::move(values))
    {}

    TUnboxedValue GetElement(ui32 index) const final {
        ++AccessCount;
        return Values.at(index);
    }

    const TUnboxedValue* GetElements() const final {
        ++AccessCount;
        return Values.data();
    }

    mutable size_t AccessCount = 0;

private:
    const std::vector<TUnboxedValue> Values;
};

template <size_t Alignment>
void TestPackedMemoryEstimation() {
    TLayoutTestEnv env;
    std::vector<TType*> keys = {env.Optional(env.Data<ui16>())};
    std::vector<TType*> states = {env.Data<ui64>(), env.Optional(env.Data<ui32>()), env.Data<ui16>()};
    TDqHashCombineRecordLayout<Alignment> layout(keys, states);
    const size_t recordSize = Alignment == 8 ? 32 : 48;
    UNIT_ASSERT_VALUES_EQUAL(layout.GetRecordSize(), recordSize);
    UNIT_ASSERT_VALUES_EQUAL(*layout.GetStaticMemorySize(), recordSize);
    TStorage storage(recordSize);
    std::vector<TUnboxedValue> key = {TUnboxedValuePod(ui16{7})};
    std::vector<TUnboxedValue> state = {TUnboxedValuePod(ui64{11}), TUnboxedValuePod(ui32{13}), TUnboxedValuePod(ui16{17})};
    layout.GetKeyLayout().PackMove(key, storage.Data());
    layout.GetStateLayout().PackMove(state, static_cast<char*>(storage.Data()) + layout.GetStateOffset());
    UNIT_ASSERT_VALUES_EQUAL(*layout.EstimateMemorySize(storage.Data()), recordSize);
    layout.Clear(storage.Data());
    UNIT_ASSERT_VALUES_EQUAL(*layout.EstimateMemorySize(storage.Data()), recordSize);

    states.push_back(env.Data<char*>());
    TDqHashCombineRecordLayout<Alignment> dynamicLayout(keys, states);
    const size_t dynamicRecordSize = Alignment == 8 ? 48 : 64;
    UNIT_ASSERT_VALUES_EQUAL(dynamicLayout.GetRecordSize(), dynamicRecordSize);
    UNIT_ASSERT(!dynamicLayout.GetStaticMemorySize());
    TStorage dynamicStorage(dynamicRecordSize);
    dynamicLayout.Clear(dynamicStorage.Data());
    const TString text("heap-backed state string for memory estimation");
    state = {TUnboxedValuePod(ui64{11}), {}, TUnboxedValuePod(ui16{17}), TUnboxedValuePod(NUdf::TStringValue(text))};
    dynamicLayout.GetStateLayout().PackMove(state, static_cast<char*>(dynamicStorage.Data()) + dynamicLayout.GetStateOffset());
    UNIT_ASSERT_VALUES_EQUAL(*dynamicLayout.EstimateMemorySize(dynamicStorage.Data()), dynamicRecordSize + StringHeaderSize + text.size());
    dynamicLayout.GetStateLayout().Destroy(static_cast<char*>(dynamicStorage.Data()) + dynamicLayout.GetStateOffset());
}

const TDqHashCombineTupleLayout::TItem& Item(const TDqHashCombineTupleLayout& layout, ui32 logicalIndex) {
    for (const auto& item : layout.GetItems()) {
        if (item.LogicalIndex == logicalIndex) {
            return item;
        }
    }
    Y_ABORT("Missing layout item");
}

template <size_t Alignment>
void TestRecordBoundaries(TArrayRef<TType* const> keyTypes, TArrayRef<TType* const> stateTypes,
    size_t stateOffset, size_t recordSize)
{
    TDqHashCombineRecordLayout<Alignment> layout(keyTypes, stateTypes);
    UNIT_ASSERT_VALUES_EQUAL(layout.GetStateOffset(), stateOffset);
    UNIT_ASSERT_VALUES_EQUAL(layout.GetRecordSize(), recordSize);
}

template <typename T>
void TestNativeSlot(typename NUdf::TDataType<T>::TLayout low, typename NUdf::TDataType<T>::TLayout high,
    TDqHashCombineTupleLayout::EStorage expectedStorage)
{
    using TPayload = typename NUdf::TDataType<T>::TLayout;
    TLayoutTestEnv env;
    for (bool optional : {false, true}) {
        auto* type = env.Data<T>();
        std::vector<TType*> types = {optional ? env.Optional(type) : type};
        TDqHashCombineTupleLayout layout(types);
        UNIT_ASSERT(Item(layout, 0).Storage == expectedStorage);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetSize(), sizeof(TPayload) + (optional ? sizeof(ui32) : 0));
        TStorage storage(layout.GetSize());
        TStorage copy(layout.GetSize());
        std::memset(storage.Data(), 0xFF, layout.GetSize());
        for (const auto value : {TUnboxedValuePod(low), TUnboxedValuePod(high), TUnboxedValuePod(TPayload{0}),
                optional ? TUnboxedValuePod{} : TUnboxedValuePod(low), TUnboxedValuePod(high)})
        {
            const std::vector<TUnboxedValuePod> logical = {value};
            layout.PackBorrowed(logical, storage.Data());
            layout.CopyWithRefs(storage.Data(), copy.Data());
            UNIT_ASSERT(layout.Equals(storage.Data(), copy.Data()));
            UNIT_ASSERT(layout.EqualsLogical(storage.Data(), logical));
            std::vector<TUnboxedValue> result(1);
            auto check = [&] {
                UNIT_ASSERT_VALUES_EQUAL(bool(result[0]), bool(value));
                if (value) {
                    UNIT_ASSERT_VALUES_EQUAL(result[0].Get<TPayload>(), value.Get<TPayload>());
                }
            };
            layout.UnpackCopy(storage.Data(), result);
            check();
            layout.UnpackCopyTo(storage.Data(), [&](size_t index) -> TUnboxedValue& { return result[index]; });
            check();
            layout.PackMove(result, copy.Data());
            UNIT_ASSERT(!result[0]);
            layout.UnpackMove(copy.Data(), result);
            check();
            const auto other = value && value.Get<TPayload>() == low ? TUnboxedValuePod(high) : TUnboxedValuePod(low);
            layout.PackMoveReplacingFrom(storage.Data(), [&](size_t) { return TUnboxedValue(other); });
            UNIT_ASSERT(!layout.EqualsLogical(storage.Data(), logical));
            UNIT_ASSERT(!layout.Equals(storage.Data(), copy.Data()));
            layout.PackMoveReplacingFrom(storage.Data(), [&](size_t) { return TUnboxedValue(value); });
            layout.UnpackMove(storage.Data(), result);
            check();
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqHashCombineLayoutTest) {
    Y_UNIT_TEST(PackedMemoryEstimation) {
        TestPackedMemoryEstimation<8>();
        TestPackedMemoryEstimation<16>();
    }

    Y_UNIT_TEST(TupleAndStructMemoryEstimation) {
        TLayoutTestEnv env;
        const size_t uvSize = sizeof(TUnboxedValuePod);
        const size_t holderSize = sizeof(TDirectArrayHolderInplace);
        for (const bool structure : {false, true}) {
            for (const bool dynamic : {false, true}) {
                auto* leafType = dynamic ? env.Data<char*>() : env.Data<ui64>();
                std::vector<TType*> elementTypes = {env.Optional(env.Data<ui16>()), leafType};
                std::vector<std::pair<TString, TType*>> members = {{"a", elementTypes[0]}, {"b", elementTypes[1]}};
                TType* type = structure ? static_cast<TType*>(TStructType::Create(members.data(), members.size(), env.Env)) :
                    TTupleType::Create(elementTypes.size(), elementTypes.data(), env.Env);
                const TString text("heap-backed composite field for estimation");
                TUnboxedValue leaf = dynamic ? TUnboxedValuePod(NUdf::TStringValue(text)) : TUnboxedValuePod(ui64{42});
                TUnboxedValue value = env.Array({TUnboxedValuePod{}, leaf});
                const size_t compositeSize = uvSize + holderSize + 2 * uvSize + (dynamic ? StringHeaderSize + text.size() : 0);
                UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize(value, type), compositeSize);

                std::vector<TType*> nestedTypes = {env.Optional(type)};
                auto* nestedType = TTupleType::Create(nestedTypes.size(), nestedTypes.data(), env.Env);
                TUnboxedValue nested = env.Array({value});
                TDqHashCombineTupleLayout layout(std::vector<TType*>{nestedType});
                TStorage storage(layout.GetSize());
                std::vector<TUnboxedValuePod> borrowed = {nested};
                layout.PackBorrowed(borrowed, storage.Data());
                const size_t externalSize = holderSize + compositeSize;
                UNIT_ASSERT_VALUES_EQUAL(*layout.EstimateExternalMemorySize(storage.Data()), externalSize);
                const auto bound = layout.GetStaticExternalMemorySize();
                UNIT_ASSERT_VALUES_EQUAL(bool(bound), !dynamic);
                if (bound) {
                    UNIT_ASSERT_VALUES_EQUAL(*bound, externalSize);
                }
                UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize({}, env.Optional(type)), uvSize);
            }
        }
    }

    Y_UNIT_TEST(NonDirectCompositeMemoryEstimation) {
        TLayoutTestEnv env;
        auto* number = env.Data<ui64>();
        const auto check = [&](const TUnboxedValuePod& value, TType* type, bool bounded) {
            UNIT_ASSERT(!TDqHashCombineTupleLayout::EstimateValueMemorySize(value, type));
            for (const bool key : {false, true}) {
                const std::vector<TType*> keyTypes = {key ? type : number};
                const std::vector<TType*> stateTypes = {key ? number : type};
                TDqHashCombineLayout layout(keyTypes, stateTypes);
                UNIT_ASSERT_VALUES_EQUAL(bool(layout.GetStaticMemorySize()), bounded);
                TStorage storage(layout.GetRecordSize());
                const std::vector<TUnboxedValuePod> keys = {key ? value : TUnboxedValuePod(ui64{1})};
                const std::vector<TUnboxedValuePod> state = {key ? TUnboxedValuePod(ui64{1}) : value};
                layout.GetKeyLayout().PackBorrowed(keys, storage.Data());
                layout.GetStateLayout().PackBorrowed(state, static_cast<char*>(storage.Data()) + layout.GetStateOffset());
                UNIT_ASSERT(!layout.EstimateMemorySize(storage.Data()));
            }
        };

        for (const bool structure : {false, true}) {
            for (const bool dynamic : {false, true}) {
                auto* leafType = dynamic ? env.Data<char*>() : number;
                std::pair<TString, TType*> member = {"a", leafType};
                TType* type = structure ? static_cast<TType*>(TStructType::Create(&member, 1, env.Env)) :
                    TTupleType::Create(1, &leafType, env.Env);
                TUnboxedValue leaf = dynamic ? TUnboxedValuePod(NUdf::TStringValue("heap-backed indirect composite field")) :
                    TUnboxedValuePod(ui64{42});
                auto* holder = new TIndirectComposite({leaf});
                TUnboxedValue value = TUnboxedValuePod(holder);
                auto* tagged = TTaggedType::Create(type, "tag", env.Env);
                const std::vector<TType*> wrappedTypes = {type, env.Optional(type), tagged, env.Optional(tagged)};
                for (auto* wrapped : wrappedTypes) {
                    check(value, wrapped, !dynamic);
                    const std::vector<TType*> nestedTypes = {number, wrapped};
                    auto* nestedType = TTupleType::Create(nestedTypes.size(), nestedTypes.data(), env.Env);
                    TUnboxedValue nested = env.Array({TUnboxedValuePod(ui64{7}), value});
                    check(nested, nestedType, !dynamic);
                }
                UNIT_ASSERT_VALUES_EQUAL(holder->AccessCount, 0);
                UNIT_ASSERT_VALUES_EQUAL(value.RefCount(), 1);
            }
        }
    }

    Y_UNIT_TEST(EmptyCompositeMemoryEstimation) {
        TLayoutTestEnv env;
        const TUnboxedValue empty = env.Array({});
        auto* holder = new TIndirectComposite({});
        const TUnboxedValue indirect = TUnboxedValuePod(holder);
        const TUnboxedValue string = TUnboxedValuePod(NUdf::TStringValue("heap-backed field beside an empty composite"));
        const size_t emptySize = sizeof(TUnboxedValuePod) + sizeof(TDirectArrayHolderInplace);
        const std::vector<TType*> types = {
            TTupleType::Create(0, nullptr, env.Env), TStructType::Create(nullptr, 0, env.Env),
        };
        for (auto* type : types) {
            TDqHashCombineTupleLayout layout(std::vector<TType*>{type});
            UNIT_ASSERT_VALUES_EQUAL(*layout.GetStaticExternalMemorySize(), sizeof(TDirectArrayHolderInplace));
            UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize({}, env.Optional(type)),
                sizeof(TUnboxedValuePod));
            for (const auto& value : {empty, indirect}) {
                const std::vector<TType*> wrappedTypes = {type, env.Optional(type), TTaggedType::Create(type, "tag", env.Env)};
                for (auto* wrapped : wrappedTypes) {
                    UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize(value, wrapped), emptySize);
                    const std::vector<TType*> nestedTypes = {wrapped, env.Data<char*>()};
                    auto* nestedType = TTupleType::Create(nestedTypes.size(), nestedTypes.data(), env.Env);
                    const auto nested = env.Array({value, string});
                    UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize(nested, nestedType),
                        2 * emptySize + sizeof(TUnboxedValuePod) + StringHeaderSize + string.AsStringRef().Size());
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(holder->AccessCount, 0);
    }

    Y_UNIT_TEST(TaggedMemoryEstimation) {
        TLayoutTestEnv env;
        const auto tagged = [&](TType* type) { return TTaggedType::Create(type, "tag", env.Env); };
        const auto check = [&](TType* type, const TUnboxedValuePod& value, size_t externalSize, bool bounded) {
            const std::vector<TType*> wrappedTypes = {
                tagged(type), env.Optional(tagged(type)), tagged(env.Optional(type)),
                tagged(env.Optional(tagged(env.Optional(type)))),
            };
            for (auto* wrapped : wrappedTypes) {
                const std::vector<TType*> types = {wrapped};
                TDqHashCombineLayout layout(types, types);
                const auto bound = layout.GetStaticMemorySize();
                UNIT_ASSERT_VALUES_EQUAL(bool(bound), bounded);
                if (bound) {
                    UNIT_ASSERT_VALUES_EQUAL(*bound, layout.GetRecordSize() + 2 * externalSize);
                }
                TStorage storage(layout.GetRecordSize());
                const std::vector<TUnboxedValuePod> values = {value};
                layout.GetKeyLayout().PackBorrowed(values, storage.Data());
                layout.GetStateLayout().PackBorrowed(values, static_cast<char*>(storage.Data()) + layout.GetStateOffset());
                const auto estimate = layout.EstimateMemorySize(storage.Data());
                UNIT_ASSERT(estimate);
                UNIT_ASSERT_VALUES_EQUAL(*estimate, layout.GetRecordSize() + 2 * externalSize);
                UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize({}, env.Optional(wrapped)),
                    sizeof(TUnboxedValuePod));
            }
        };

        check(env.Data<ui64>(), TUnboxedValuePod(ui64{42}), 0, true);
        TUnboxedValue uuid = TUnboxedValuePod(NUdf::TStringValue(TString(NUdf::UUID_SIZE, '\0')));
        check(env.Data<NUdf::TUuid>(), uuid, StringHeaderSize + NUdf::UUID_SIZE, true);
        const TString text("heap-backed tagged field for memory estimation");
        TUnboxedValue string = TUnboxedValuePod(NUdf::TStringValue(text));
        check(env.Data<char*>(), string, StringHeaderSize + text.size(), false);

        for (const bool structure : {false, true}) {
            for (const bool dynamic : {false, true}) {
                std::vector<TType*> elements = {
                    tagged(env.Optional(env.Data<ui16>())), tagged(dynamic ? env.Data<char*>() : env.Data<ui64>()),
                };
                std::vector<std::pair<TString, TType*>> members = {{"a", elements[0]}, {"b", elements[1]}};
                TType* type = structure ? static_cast<TType*>(TStructType::Create(members.data(), members.size(), env.Env)) :
                    TTupleType::Create(elements.size(), elements.data(), env.Env);
                TUnboxedValue leaf = dynamic ? string : TUnboxedValue(TUnboxedValuePod(ui64{42}));
                TUnboxedValue value = env.Array({TUnboxedValuePod{}, leaf});
                const size_t externalSize = sizeof(TDirectArrayHolderInplace) + 2 * sizeof(TUnboxedValuePod) +
                    (dynamic ? StringHeaderSize + text.size() : 0);
                check(type, value, externalSize, !dynamic);
            }
        }
    }

    Y_UNIT_TEST(UuidMemoryEstimation) {
        TLayoutTestEnv env;
        auto* uuidType = env.Data<NUdf::TUuid>();
        std::vector<TType*> keys = {uuidType};
        std::vector<TType*> states = {env.Optional(uuidType)};
        TDqHashCombineRecordLayout<8> record(keys, states);
        UNIT_ASSERT_VALUES_EQUAL(*record.GetStaticMemorySize(), 96);
        TUnboxedValue uuid = TUnboxedValuePod(NUdf::TStringValue(TString(NUdf::UUID_SIZE, '\0')));
        TUnboxedValue stateUuid = TUnboxedValuePod(NUdf::TStringValue(TString(NUdf::UUID_SIZE, '\1')));
        UNIT_ASSERT(uuid.IsString());
        TStorage storage(record.GetRecordSize());
        std::vector<TUnboxedValuePod> values = {uuid};
        auto* state = static_cast<char*>(storage.Data()) + record.GetStateOffset();
        record.GetKeyLayout().PackBorrowed(values, storage.Data());
        values[0] = stateUuid;
        record.GetStateLayout().PackBorrowed(values, state);
        UNIT_ASSERT_VALUES_EQUAL(*record.EstimateMemorySize(storage.Data()), 96);
        values[0] = TUnboxedValuePod{};
        record.GetStateLayout().PackBorrowed(values, state);
        UNIT_ASSERT_VALUES_EQUAL(*record.EstimateMemorySize(storage.Data()), 64);
        UNIT_ASSERT_VALUES_EQUAL(*record.GetStaticMemorySize(), 96);

        std::vector<TType*> elements = {uuidType, env.Optional(uuidType)};
        auto* tupleType = TTupleType::Create(elements.size(), elements.data(), env.Env);
        std::pair<TString, TType*> member = {"uuids", tupleType};
        auto* structType = TStructType::Create(&member, 1, env.Env);
        TDqHashCombineTupleLayout nestedLayout(std::vector<TType*>{structType});
        const size_t expected = 2 * sizeof(TDirectArrayHolderInplace) + 3 * sizeof(TUnboxedValuePod) + 2 * (StringHeaderSize + NUdf::UUID_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(*nestedLayout.GetStaticExternalMemorySize(), expected);
        TUnboxedValue tuple = env.Array({uuid, stateUuid});
        TUnboxedValue structure = env.Array({tuple});
        UNIT_ASSERT_VALUES_EQUAL(*TDqHashCombineTupleLayout::EstimateValueMemorySize(structure, structType),
            sizeof(TUnboxedValuePod) + expected);
    }

    Y_UNIT_TEST(RequiredNativeRejectsEmptyValue) {
        TLayoutTestEnv env;
        TUnboxedValue first = TUnboxedValuePod(NUdf::TStringValue("first long borrowed string"));
        TUnboxedValue second = TUnboxedValuePod(NUdf::TStringValue("second long borrowed string"));
        const i32 firstRefs = first.RefCount();
        const i32 secondRefs = second.RefCount();
        for (auto* type : {env.Data<ui16>(), env.Data<ui32>(), env.Data<ui64>()}) {
            std::vector<TType*> types = {
                env.Data<char*>(), env.Optional(env.Data<ui64>()), type, env.Data<char*>(),
            };
            TDqHashCombineTupleLayout layout(types);
            TStorage storage(layout.GetSize());
            std::memset(storage.Data(), 0xFF, layout.GetSize());
            std::vector<TUnboxedValuePod> borrowed = {first, TUnboxedValuePod(ui64{7}), {}, second};

            UNIT_ASSERT_EXCEPTION_CONTAINS(layout.PackWithRefs(borrowed, storage.Data()), yexception,
                "Empty value for required native column 2");
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);
            const auto* unboxed = static_cast<const TUnboxedValuePod*>(storage.Data());
            for (size_t i = 0; i < layout.GetUnboxedCount(); ++i) {
                UNIT_ASSERT(!unboxed[i]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReadUnaligned<ui32>(
                static_cast<const char*>(storage.Data()) + layout.GetValidityOffset()), 0);
            layout.Destroy(storage.Data());
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);

            borrowed[2] = TUnboxedValuePod(ui64{0});
            layout.PackWithRefs(borrowed, storage.Data());
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs + 1);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs + 1);
            layout.Destroy(storage.Data());
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);

            borrowed[2] = TUnboxedValuePod{};
            UNIT_ASSERT_EXCEPTION_CONTAINS(layout.PackMoveFrom(storage.Data(), [&](size_t index) {
                return TUnboxedValue(borrowed[index]);
            }), yexception, "Empty value for required native column 2");
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs + 1);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);
            layout.Destroy(storage.Data());
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);

            borrowed[2] = TUnboxedValuePod(ui64{0});
            layout.PackWithRefs(borrowed, storage.Data());
            borrowed[2] = TUnboxedValuePod{};
            UNIT_ASSERT_EXCEPTION_CONTAINS(layout.PackMoveReplacingFrom(storage.Data(), [&](size_t index) {
                return TUnboxedValue(borrowed[index]);
            }), yexception, "Empty value for required native column 2");
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs + 1);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs + 1);
            layout.Destroy(storage.Data());
            UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), firstRefs);
            UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), secondRefs);
        }
    }

    Y_UNIT_TEST(SectionOffsetsAndAlignment) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {
            env.Data<char*>(),
            env.Data<ui32>(),
            env.Optional(env.Data<double>()),
            env.Data<i64>(),
            env.Data<float>(),
            env.Optional(env.Data<ui64>()),
        };
        TDqHashCombineTupleLayout layout(types);

        UNIT_ASSERT_VALUES_EQUAL(layout.GetUnboxedCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetNative64Count(), 3);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetValidityWordCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetNative32Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetValidityOffset(), 40);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetSize(), 52);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 0).Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 2).Offset, 16);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 3).Offset, 24);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 5).Offset, 32);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 1).Offset, 44);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 4).Offset, 48);

        std::vector<TType*> stateTypes = {env.Data<i32>()};
        TestRecordBoundaries<8>(types, stateTypes, 56, 64);
        TestRecordBoundaries<16>(types, stateTypes, 64, 80);

        std::vector<TType*> empty;
        TestRecordBoundaries<8>(empty, empty, 0, 8);
        TestRecordBoundaries<16>(empty, empty, 0, 16);
    }

    Y_UNIT_TEST(Native16AndTemporalSlots) {
        using namespace NUdf;
        using EStorage = TDqHashCombineTupleLayout::EStorage;
        TestNativeSlot<i16>(std::numeric_limits<i16>::min(), std::numeric_limits<i16>::max(), EStorage::Native16);
        TestNativeSlot<ui16>(0, std::numeric_limits<ui16>::max(), EStorage::Native16);
        TestNativeSlot<TDate>(0, MAX_DATE - 1, EStorage::Native16);
        TestNativeSlot<TDatetime>(0, MAX_DATETIME - 1, EStorage::Native32);
        TestNativeSlot<TTimestamp>(0, MAX_TIMESTAMP - 1, EStorage::Native64);
        TestNativeSlot<TInterval>(-i64(MAX_TIMESTAMP) + 1, MAX_TIMESTAMP - 1, EStorage::Native64);
        TestNativeSlot<TDate32>(MIN_DATE32, MAX_DATE32, EStorage::Native32);
        TestNativeSlot<TDatetime64>(MIN_DATETIME64, MAX_DATETIME64, EStorage::Native64);
        TestNativeSlot<TTimestamp64>(MIN_TIMESTAMP64, MAX_TIMESTAMP64, EStorage::Native64);
        TestNativeSlot<TInterval64>(-MAX_INTERVAL64, MAX_INTERVAL64, EStorage::Native64);
    }

    Y_UNIT_TEST(Native16SectionAlignment) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {env.Data<i16>(), env.Data<ui64>(), env.Optional(env.Data<NUdf::TDate>()),
            env.Data<ui32>(), env.Data<char*>(), env.Data<ui16>()};
        TDqHashCombineTupleLayout layout(types);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetNative16Count(), 3);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetValidityOffset(), 24);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 3).Offset, 28);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 0).Offset, 32);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 2).Offset, 34);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 5).Offset, 36);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetSize(), 38);
        std::vector<TType*> stateTypes = {env.Data<i16>()};
        TestRecordBoundaries<8>(types, stateTypes, 40, 48);
        TestRecordBoundaries<16>(types, stateTypes, 48, 64);
        TestRecordBoundaries<8>(stateTypes, stateTypes, 8, 16);
    }

    Y_UNIT_TEST(TimezoneAndNestedOptionalFallback) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {env.Data<NUdf::TTzDate>(), env.Data<NUdf::TTzDatetime>(),
            env.Data<NUdf::TTzTimestamp>(), env.Data<NUdf::TTzDate32>(), env.Data<NUdf::TTzDatetime64>(),
            env.Data<NUdf::TTzTimestamp64>(), env.Optional(env.Optional(env.Data<NUdf::TDate>()))};
        for (bool optional : {false, true}) {
            if (optional) {
                for (auto*& type : types) {
                    type = env.Optional(type);
                }
            }
            TDqHashCombineTupleLayout layout(types);
            UNIT_ASSERT_VALUES_EQUAL(layout.GetUnboxedCount(), types.size());
            std::vector<TUnboxedValue> values = {TUnboxedValuePod(ui16{17}), TUnboxedValuePod(ui32{42}),
                TUnboxedValuePod(ui64{123}), TUnboxedValuePod(i32{-7}), TUnboxedValuePod(i64{-42}),
                TUnboxedValuePod(i64{-123}), TUnboxedValuePod{}.MakeOptional()};
            for (size_t i = 0; i < 6; ++i) {
                values[i].SetTimezoneId(123);
            }
            TStorage storage(layout.GetSize());
            layout.PackMove(values, storage.Data());
            layout.UnpackMove(storage.Data(), values);
            for (size_t i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(values[i].GetTimezoneId(), 123);
            }
            UNIT_ASSERT(values.back());
            UNIT_ASSERT(!values.back().GetOptionalValue());
        }
    }

    Y_UNIT_TEST(MultipleValidityWords) {
        TLayoutTestEnv env;
        std::vector<TType*> types;
        for (size_t i = 0; i < 33; ++i) {
            types.push_back(env.Optional(env.Data<ui32>()));
        }
        TDqHashCombineTupleLayout layout(types);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetValidityWordCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetValidityOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetSize(), 140);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 0).Offset, 8);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 32).Offset, 136);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 32).ValidityOffset, 4);
        UNIT_ASSERT_VALUES_EQUAL(Item(layout, 32).ValidityMask, 1);

        for (size_t i = 0; i < types.size(); ++i) {
            types[i] = env.Optional(i % 2 ? env.Data<ui64>() : env.Data<ui32>());
        }
        TDqHashCombineTupleLayout mixed(types);
        TStorage storage(mixed.GetSize());
        std::memset(storage.Data(), 0xFF, mixed.GetSize());
        std::vector<TUnboxedValue> values(types.size());
        for (size_t pass = 0; pass < 2; ++pass) {
            for (size_t i = 0; i < values.size(); ++i) {
                values[i] = (i + pass) % 3
                    ? (i % 2 ? TUnboxedValuePod(ui64(i)) : TUnboxedValuePod(ui32(i)))
                    : TUnboxedValuePod{};
            }
            mixed.PackMove(values, storage.Data());
            mixed.UnpackMove(storage.Data(), values);
            for (size_t i = 0; i < values.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(values[i].HasValue(), bool((i + pass) % 3));
                if (values[i]) {
                    UNIT_ASSERT_VALUES_EQUAL(i % 2 ? values[i].Get<ui64>() : values[i].Get<ui32>(), i);
                }
            }
        }
    }

    Y_UNIT_TEST(RoundTripNativeAndFallbackValues) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {
            env.Data<ui64>(), env.Data<i64>(), env.Data<double>(),
            env.Data<ui32>(), env.Data<i32>(), env.Data<float>(),
            env.Optional(env.Data<ui64>()), env.Optional(env.Data<i64>()), env.Optional(env.Data<double>()),
            env.Optional(env.Data<ui32>()), env.Optional(env.Data<i32>()), env.Optional(env.Data<float>()),
            env.Data<char*>(),
        };
        TDqHashCombineTupleLayout layout(types);
        TStorage storage(layout.GetSize());

        std::vector<TUnboxedValue> values(types.size());
        values[0] = TUnboxedValuePod(std::numeric_limits<ui64>::max());
        values[1] = TUnboxedValuePod(std::numeric_limits<i64>::min());
        values[2] = TUnboxedValuePod(std::numeric_limits<double>::infinity());
        values[3] = TUnboxedValuePod(std::numeric_limits<ui32>::max());
        values[4] = TUnboxedValuePod(std::numeric_limits<i32>::min());
        values[5] = TUnboxedValuePod(-0.0f);
        values[6] = TUnboxedValuePod(ui64{0});
        values[7] = TUnboxedValuePod(i64{-17});
        values[8] = TUnboxedValuePod(-std::numeric_limits<double>::infinity());
        values[9] = TUnboxedValuePod(ui32{42});
        values[10] = TUnboxedValuePod(i32{-42});
        values[11] = TUnboxedValuePod(3.25f);
        values[12] = TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes"));

        layout.PackMove(values, storage.Data());
        for (const auto& value : values) {
            UNIT_ASSERT(!value.HasValue());
        }

        std::vector<TUnboxedValue> result(types.size());
        layout.UnpackMove(storage.Data(), result);
        UNIT_ASSERT_VALUES_EQUAL(result[0].Get<ui64>(), std::numeric_limits<ui64>::max());
        UNIT_ASSERT_VALUES_EQUAL(result[1].Get<i64>(), std::numeric_limits<i64>::min());
        UNIT_ASSERT(std::isinf(result[2].Get<double>()));
        UNIT_ASSERT_VALUES_EQUAL(result[3].Get<ui32>(), std::numeric_limits<ui32>::max());
        UNIT_ASSERT_VALUES_EQUAL(result[4].Get<i32>(), std::numeric_limits<i32>::min());
        UNIT_ASSERT(std::signbit(result[5].Get<float>()));
        UNIT_ASSERT_VALUES_EQUAL(result[6].Get<ui64>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result[7].Get<i64>(), -17);
        UNIT_ASSERT(result[8].Get<double>() < 0 && std::isinf(result[8].Get<double>()));
        UNIT_ASSERT_VALUES_EQUAL(result[9].Get<ui32>(), 42);
        UNIT_ASSERT_VALUES_EQUAL(result[10].Get<i32>(), -42);
        UNIT_ASSERT_VALUES_EQUAL(result[11].Get<float>(), 3.25f);
        UNIT_ASSERT_VALUES_EQUAL(std::string(result[12].AsStringRef()), "a string longer than fourteen bytes");

        for (size_t i = 6; i <= 11; ++i) {
            result[i] = TUnboxedValue{};
        }
        layout.PackMove(result, storage.Data());
        layout.UnpackMove(storage.Data(), values);
        for (size_t i = 6; i <= 11; ++i) {
            UNIT_ASSERT(!values[i].HasValue());
        }
    }

    Y_UNIT_TEST(EqualitySemantics) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {
            env.Data<double>(), env.Data<float>(), env.Optional(env.Data<ui64>()), env.Data<char*>()
        };
        TDqHashCombineTupleLayout layout(types);
        TStorage left(layout.GetSize());
        TStorage right(layout.GetSize());

        const double nan1 = std::bit_cast<double>(ui64{0x7ff8000000000001ULL});
        const double nan2 = std::bit_cast<double>(ui64{0x7ff8000000000002ULL});
        std::vector<TUnboxedValue> lhs = {
            TUnboxedValuePod(nan1), TUnboxedValuePod(0.0f), TUnboxedValuePod{},
            TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes")),
        };
        std::vector<TUnboxedValue> rhs = {
            TUnboxedValuePod(nan2), TUnboxedValuePod(-0.0f), TUnboxedValuePod{}, lhs[3],
        };
        auto pods = [](const std::vector<TUnboxedValue>& values) {
            return TArrayRef<const TUnboxedValuePod>(
                reinterpret_cast<const TUnboxedValuePod*>(values.data()), values.size());
        };

        layout.PackBorrowed(pods(lhs), left.Data());
        layout.PackBorrowed(pods(rhs), right.Data());
        UNIT_ASSERT(layout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(layout.EqualsLogical(left.Data(), pods(rhs)));

        rhs[2] = TUnboxedValuePod(ui64{0});
        layout.PackBorrowed(pods(rhs), right.Data());
        UNIT_ASSERT(!layout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(!layout.EqualsLogical(left.Data(), pods(rhs)));
        rhs[2] = TUnboxedValuePod{};
        rhs[3] = TUnboxedValuePod(NUdf::TStringValue("a different long string value"));
        layout.PackBorrowed(pods(rhs), right.Data());
        UNIT_ASSERT(!layout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(!layout.EqualsLogical(left.Data(), pods(rhs)));

        rhs[3] = lhs[3];
        rhs[1] = TUnboxedValuePod(1.0f);
        UNIT_ASSERT(!layout.EqualsLogical(left.Data(), pods(rhs)));
    }

    Y_UNIT_TEST(DirectLayoutsAndDirtyStorage) {
        TLayoutTestEnv env;
        std::vector<TType*> stringTypes = {env.Optional(env.Data<char*>())};
        TDqHashCombineTupleLayout stringLayout(stringTypes);

        TStorage left(stringLayout.GetSize());
        TStorage right(stringLayout.GetSize());
        const std::vector<TUnboxedValuePod> nullValue = {TUnboxedValuePod{}};
        TUnboxedValue stringOwner = TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes"));
        const std::vector<TUnboxedValuePod> presentValue = {stringOwner};
        stringLayout.PackBorrowed(nullValue, left.Data());
        stringLayout.PackBorrowed(nullValue, right.Data());
        UNIT_ASSERT(stringLayout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(stringLayout.EqualsLogical(left.Data(), nullValue));
        stringLayout.PackBorrowed(presentValue, right.Data());
        UNIT_ASSERT(!stringLayout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(!stringLayout.EqualsLogical(left.Data(), presentValue));
        stringLayout.PackBorrowed(presentValue, left.Data());
        UNIT_ASSERT(stringLayout.Equals(left.Data(), right.Data()));
        UNIT_ASSERT(stringLayout.EqualsLogical(left.Data(), presentValue));

        std::vector<TType*> optionalTypes = {env.Optional(env.Data<ui64>())};
        TDqHashCombineTupleLayout optionalLayout(optionalTypes);
        TStorage dirty(optionalLayout.GetSize());
        std::memset(dirty.Data(), 0xFF, optionalLayout.GetSize());
        optionalLayout.PackBorrowed(nullValue, dirty.Data());
        UNIT_ASSERT(optionalLayout.EqualsLogical(dirty.Data(), nullValue));
        const std::vector<TUnboxedValuePod> zeroValue = {TUnboxedValuePod(ui64{0})};
        UNIT_ASSERT(!optionalLayout.EqualsLogical(dirty.Data(), zeroValue));
        std::vector<TUnboxedValue> result(1);
        optionalLayout.UnpackCopy(dirty.Data(), result);
        UNIT_ASSERT(!result[0].HasValue());
    }

    Y_UNIT_TEST(MixedLogicalProbeAcrossDisplacementAndGrowth) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {env.Data<ui32>(), env.Optional(env.Data<i64>()), env.Data<char*>()};
        TDqHashCombineTupleLayout layout(types);

        using TMap = TDqRobinHoodHashSet<char*, TDqHashCombinePackedEqual, std::allocator<char>>;
        TMap map(TDqHashCombinePackedEqual(&layout), 8);
        std::vector<TStorage> stored;
        stored.reserve(64);
        TUnboxedValue owner = TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes"));
        const i32 initialRefs = owner.RefCount();

        auto hash = [](ui32 number) { return number % 3 == 0 ? ui32{1} << 29 : 0; };
        auto insertLogical = [&](ui32 number, bool expectedNew) {
            TUnboxedValuePod logicalKey[] = {
                TUnboxedValuePod(number),
                number % 2 ? TUnboxedValuePod(-i64(number)) : TUnboxedValuePod{},
                owner,
            };
            bool isNew = false;
            auto* entry = map.InsertWithEqual(reinterpret_cast<char*>(logicalKey), hash(number), isNew,
                [&](char* packed, char* probe) {
                    UNIT_ASSERT(probe == reinterpret_cast<char*>(logicalKey));
                    return layout.EqualsLogical(packed, logicalKey);
                });
            UNIT_ASSERT_VALUES_EQUAL(isNew, expectedNew);
            if (isNew) {
                stored.emplace_back(layout.GetSize());
                layout.PackWithRefs(logicalKey, stored.back().Data());
                *static_cast<char**>(TMap::GetKeyPtr(entry)) = static_cast<char*>(stored.back().Data());
                map.CheckGrow();
            }
        };

        insertLogical(0, true);
        insertLogical(1, true);
        insertLogical(2, true);
        UNIT_ASSERT_VALUES_EQUAL(map.GetCapacity(), 8);
        UNIT_ASSERT(TMap::GetKeyValue(map.Begin() + 2 * TMap::GetCellSize()) == stored[0].Data());
        for (ui32 i = 3; i < 64; ++i) {
            insertLogical(i, true);
        }
        for (ui32 i = 0; i < 64; ++i) {
            insertLogical(i, false);
        }
        UNIT_ASSERT_VALUES_EQUAL(map.GetSize(), 64);
        UNIT_ASSERT(map.GetCapacity() > 8);
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefs + 64);
        for (auto& record : stored) {
            layout.Destroy(record.Data());
        }
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefs);
    }

    Y_UNIT_TEST(MoveCallbacksPreserveLogicalOrderAndOwnership) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {env.Data<ui32>(), env.Data<char*>(), env.Optional(env.Data<double>())};
        TDqHashCombineTupleLayout layout(types);
        TStorage storage(layout.GetSize());
        layout.Clear(storage.Data());
        TUnboxedValue owner = TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes"));
        std::vector<TUnboxedValue> values = {TUnboxedValuePod(ui32{42}), owner, TUnboxedValuePod(-0.0)};
        const i32 refs = owner.RefCount();
        size_t next = 0;
        layout.PackMoveFrom(storage.Data(), [&](size_t index) {
            UNIT_ASSERT_VALUES_EQUAL(index, next++);
            return std::move(values[index]);
        });
        UNIT_ASSERT_VALUES_EQUAL(next, types.size());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), refs);
        for (const auto& value : values) {
            UNIT_ASSERT(!value);
        }
        layout.UnpackMoveTo(storage.Data(), [&](size_t index, TUnboxedValue&& value) {
            values[index] = std::move(value);
        });
        layout.Destroy(storage.Data());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), refs);
        UNIT_ASSERT_VALUES_EQUAL(values[0].Get<ui32>(), 42);
        UNIT_ASSERT_VALUES_EQUAL(std::string(values[1].AsStringRef()), std::string(owner.AsStringRef()));
        UNIT_ASSERT(std::signbit(values[2].Get<double>()));
    }

    Y_UNIT_TEST(CopyAndReplaceCallbacksPreservePartialState) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {
            env.Data<char*>(), env.Data<char*>(),
            env.Optional(env.Data<ui64>()), env.Optional(env.Data<ui64>()),
        };
        TDqHashCombineTupleLayout layout(types);
        TStorage storage(layout.GetSize());
        TUnboxedValue first = TUnboxedValuePod(NUdf::TStringValue("first long string value"));
        TUnboxedValue second = TUnboxedValuePod(NUdf::TStringValue("second long string value"));
        std::vector<TUnboxedValuePod> borrowed = {first, second, TUnboxedValuePod(ui64{7}), {}};
        layout.PackWithRefs(borrowed, storage.Data());

        std::vector<TUnboxedValue> destinations(types.size());
        layout.UnpackCopyTo(storage.Data(), [&](size_t index) -> TUnboxedValue& {
            return destinations[index];
        });
        UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), 3);
        layout.UnpackCopyTo(storage.Data(), [&](size_t index) -> TUnboxedValue& {
            return destinations[index];
        });
        UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), 3);

        bool threw = false;
        try {
            layout.PackMoveReplacingFrom(storage.Data(), [&](size_t index) -> TUnboxedValue {
                if (index == 1) {
                    throw std::runtime_error("update failed");
                }
                return destinations[index];
            });
        } catch (const std::runtime_error&) {
            threw = true;
        }
        UNIT_ASSERT(threw);
        UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), 3);

        layout.PackMoveReplacingFrom(storage.Data(), [&](size_t index) -> TUnboxedValue {
            switch (index) {
                case 0: return destinations[0];
                case 1: return TUnboxedValuePod(NUdf::TStringValue("replacement string value"));
                case 2: return {};
                default: return TUnboxedValuePod(ui64{11});
            }
        });
        std::vector<TUnboxedValue> result(types.size());
        layout.UnpackCopy(storage.Data(), result);
        UNIT_ASSERT_VALUES_EQUAL(std::string(result[0].AsStringRef()), "first long string value");
        UNIT_ASSERT_VALUES_EQUAL(std::string(result[1].AsStringRef()), "replacement string value");
        UNIT_ASSERT(!result[2]);
        UNIT_ASSERT_VALUES_EQUAL(result[3].Get<ui64>(), 11);
        result.clear();

        layout.Destroy(storage.Data());
        destinations.clear();
        UNIT_ASSERT_VALUES_EQUAL(first.RefCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(second.RefCount(), 1);
    }

    Y_UNIT_TEST(NativeFloatingPointPayloadBits) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {
            env.Data<float>(), env.Data<double>(), env.Optional(env.Data<float>()), env.Optional(env.Data<double>())
        };
        TDqHashCombineTupleLayout layout(types);
        TStorage storage(layout.GetSize());
        const ui32 floatBits = 0xffc01234;
        const ui64 doubleBits = 0x7ff0000000000042ULL;
        std::vector<TUnboxedValue> values = {
            TUnboxedValuePod(std::bit_cast<float>(floatBits)),
            TUnboxedValuePod(std::bit_cast<double>(doubleBits)),
            TUnboxedValuePod(-0.0f), TUnboxedValuePod(-0.0),
        };
        layout.PackMove(values, storage.Data());
        layout.UnpackMove(storage.Data(), values);
        UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui32>(values[0].Get<float>()), floatBits);
        UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui64>(values[1].Get<double>()), doubleBits);
        UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui32>(values[2].Get<float>()), ui32{1} << 31);
        UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui64>(values[3].Get<double>()), ui64{1} << 63);
    }

    Y_UNIT_TEST(OwnershipAndMemoryEstimation) {
        TLayoutTestEnv env;
        std::vector<TType*> types = {env.Data<char*>(), env.Data<ui64>()};
        TDqHashCombineTupleLayout layout(types);
        UNIT_ASSERT_VALUES_EQUAL(layout.GetSize(), 24);
        UNIT_ASSERT(!layout.GetStaticExternalMemorySize());

        TUnboxedValue owner = TUnboxedValuePod(NUdf::TStringValue("a string longer than fourteen bytes"));
        const i32 initialRefCount = owner.RefCount();
        std::vector<TUnboxedValuePod> borrowed = {owner, TUnboxedValuePod(ui64{7})};
        TStorage scratch(layout.GetSize());
        TStorage persistent(layout.GetSize());
        layout.PackBorrowed(borrowed, scratch.Data());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount);
        layout.CopyWithRefs(scratch.Data(), persistent.Data());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount + 1);

        const auto memory = layout.EstimateExternalMemorySize(persistent.Data());
        UNIT_ASSERT(memory);
        UNIT_ASSERT_VALUES_EQUAL(*memory, StringHeaderSize + owner.AsStringRef().Size());

        std::vector<TUnboxedValue> copied(types.size());
        layout.UnpackCopy(persistent.Data(), copied);
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount + 2);
        copied.clear();
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount + 1);

        std::vector<TUnboxedValue> moved(types.size());
        layout.UnpackMove(persistent.Data(), moved);
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(std::string(moved[0].AsStringRef()), std::string(owner.AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(moved[1].Get<ui64>(), 7);
        layout.Destroy(persistent.Data());
        moved.clear();
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount);

        layout.PackWithRefs(borrowed, persistent.Data());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount + 1);
        layout.Destroy(persistent.Data());
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), initialRefCount);

        std::vector<TType*> empty;
        TDqHashCombineRecordLayout<16> recordLayout(types, empty);
        TStorage record(recordLayout.GetRecordSize());
        recordLayout.Clear(record.Data());
        recordLayout.GetKeyLayout().CopyWithRefs(scratch.Data(), record.Data());
        const auto recordMemory = recordLayout.EstimateMemorySize(record.Data());
        UNIT_ASSERT(recordMemory);
        UNIT_ASSERT_VALUES_EQUAL(*recordMemory, recordLayout.GetRecordSize() + StringHeaderSize + owner.AsStringRef().Size());
        recordLayout.GetKeyLayout().Destroy(record.Data());
    }
}

} // namespace NKikimr::NMiniKQL
