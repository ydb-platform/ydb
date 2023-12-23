#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include "mkql_computation_list_adapter.h"
#include "mkql_computation_node_impl.h"
#include "mkql_computation_node.h"
#include "mkql_value_builder.h"
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include "mkql_validate.h"

#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

#include <ydb/library/yql/public/udf/udf_helpers.h>

namespace NYql {

namespace {
using namespace NKikimr::NMiniKQL;
static const ui32 RAW_INDEX_NO_HOLE = -1;
static const ui32 RAW_BROKEN_INDEX_LIST_TO_DICT = 1;

template<class T>
NUdf::TUnboxedValue ToUnboxedValue(const T& val) {
    return NUdf::TUnboxedValuePod(val);
}

NUdf::TUnboxedValue ToUnboxedValue(const TString& val) {
    return MakeString(val);
}

NUdf::TUnboxedValue ToUnboxedValue(const NUdf::IBoxedValuePtr& val) {
    return NUdf::TUnboxedValuePod(NUdf::IBoxedValuePtr(val));
}

} // namespace NMiniKQL

/// support for build Struct type @{
namespace NUdf {

    template<class TContainer>
    struct TListRefIterator: public TBoxedValue {
        TListRefIterator(const TContainer& listRef, ui32 holePos)
            : ListRef(listRef)
            , Index(-1)
            , HolePos(holePos)
        {}
    private:
        const TContainer& ListRef;
        ui32 Index;
        ui32 HolePos;

        bool Next(NUdf::TUnboxedValue& value) final {
            if (++Index >= ListRef.size())
                return false;
            value = Index == HolePos ? NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(42)) : ToUnboxedValue(ListRef[Index]);
            return true;
        }
    };

    template<class TContainer, ui32 TIndexDictBrokenHole = RAW_INDEX_NO_HOLE, bool TNoDictIndex = false>
    struct TListRef: public NUdf::TBoxedValue {
        TListRef(const TContainer& listRef, ui32 holePos = RAW_INDEX_NO_HOLE)
            : ListRef(listRef)
            , HolePos(holePos)
        {}

    private:
        const TContainer& ListRef;
        const NUdf::IValueBuilder* ValueBuilder;
        ui32 HolePos;

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return ListRef.size();
        }

        ui64 GetEstimatedListLength() const override {
            return ListRef.size();
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TListRefIterator<TContainer>(ListRef, HolePos));
        }

        NUdf::IBoxedValuePtr ToIndexDictImpl(const IValueBuilder& builder) const override {
            return TNoDictIndex ? nullptr : builder.ToIndexDict(NUdf::TUnboxedValuePod(
                new TListRef<TContainer, TIndexDictBrokenHole, true>(ListRef, TIndexDictBrokenHole))).AsBoxed();
        }
    };

    struct PersonStruct {
        static const size_t MEMBERS_COUNT = 3;
        static ui32 MetaIndexes[MEMBERS_COUNT];
        static ui32 MetaBackIndexes[MEMBERS_COUNT];

        TString FirstName;
        TString LastName;
        ui32 Age;

        NUdf::TUnboxedValue GetByIndex(ui32 index) const {
            switch (index) {
                case 0: return ToUnboxedValue(FirstName);
                case 1: return ToUnboxedValue(LastName);
                case 2: return NUdf::TUnboxedValuePod(Age);
                default: Y_ABORT("Unexpected");
            }
        }
    };

    ui32 PersonStruct::MetaIndexes[MEMBERS_COUNT];
    ui32 PersonStruct::MetaBackIndexes[MEMBERS_COUNT];


    struct PersonStructWithOptList {
        static const size_t MEMBERS_COUNT = 4;
        static ui32 MetaIndexes[MEMBERS_COUNT];
        static ui32 MetaBackIndexes[MEMBERS_COUNT];

        TString FirstName;
        TString LastName;
        ui32 Age;
        typedef std::vector<ui32> TTagList;
        TTagList Tags;

        NUdf::TUnboxedValue GetByIndex(ui32 index) const {
            switch (index) {
                case 0: return ToUnboxedValue(FirstName);
                case 1: return ToUnboxedValue(LastName);
                case 2: return NUdf::TUnboxedValuePod(Age);
                case 3: return Tags.empty() ?
                    NUdf::TUnboxedValuePod() :
                    NUdf::TUnboxedValuePod(new TListRef<TTagList>(Tags));
                default: Y_ABORT("Unexpected");
            }
        }
    };

    ui32 PersonStructWithOptList::MetaIndexes[MEMBERS_COUNT];
    ui32 PersonStructWithOptList::MetaBackIndexes[MEMBERS_COUNT];

    struct TCallableOneUi32Arg {
    };

namespace NImpl {

    template<>
    struct TTypeBuilderHelper<NUdf::PersonStruct> {
        static TType* Build(const IFunctionTypeInfoBuilder& builder) {
            auto structBuilder = builder.Struct(3);
            structBuilder->AddField<char*>("FirstName", &PersonStruct::MetaIndexes[0])
                .AddField<char*>("LastName", &PersonStruct::MetaIndexes[1])
                .AddField<ui32>("Age", &PersonStruct::MetaIndexes[2]);
            auto structType = structBuilder->Build();
            for (const auto& index: PersonStruct::MetaIndexes) {
                Y_ABORT_UNLESS(index < NUdf::PersonStruct::MEMBERS_COUNT);
                NUdf::PersonStruct::MetaBackIndexes[index] = &index - PersonStruct::MetaIndexes;
                Y_ABORT_UNLESS(NUdf::PersonStruct::MetaBackIndexes[index] < NUdf::PersonStruct::MEMBERS_COUNT);
            }
            return structType;
        }
    };

    template<>
    struct TTypeBuilderHelper<NUdf::PersonStructWithOptList> {
        static TType* Build(const IFunctionTypeInfoBuilder& builder) {
            auto listTags = builder.List()->Item<ui32>().Build();
            auto optionalListTags = builder.Optional()->Item(listTags).Build();
            auto structBuilder = builder.Struct(3);
            structBuilder->AddField<char*>("FirstName", &PersonStructWithOptList::MetaIndexes[0])
                .AddField<char*>("LastName", &PersonStructWithOptList::MetaIndexes[1])
                .AddField<ui32>("Age", &PersonStructWithOptList::MetaIndexes[2])
                .AddField("Tags", optionalListTags, &PersonStructWithOptList::MetaIndexes[3]);
            auto structType = structBuilder->Build();
            for (const auto& index: PersonStructWithOptList::MetaIndexes) {
                Y_ABORT_UNLESS(index < NUdf::PersonStructWithOptList::MEMBERS_COUNT);
                NUdf::PersonStructWithOptList::MetaBackIndexes[index] = &index - PersonStructWithOptList::MetaIndexes;
                Y_ABORT_UNLESS(NUdf::PersonStructWithOptList::MetaBackIndexes[index] < NUdf::PersonStructWithOptList::MEMBERS_COUNT);
            }
            return structType;
        }
    };

    template<>
    struct TTypeBuilderHelper<NUdf::TCallableOneUi32Arg> {
        static TType* Build(const IFunctionTypeInfoBuilder& builder) {
            auto callableBuilder = builder.Callable(1);
            callableBuilder->Returns<ui32>();
            callableBuilder->Arg<ui32>();
            return callableBuilder->Build();
        }
    };
} // namespace NImpl
} // namespace NUdf
/// @}


    struct TBrokenSeqListIterator: public NUdf::TBoxedValue {
        TBrokenSeqListIterator(ui32 size, ui32 holePos)
            : Size(size)
            , HolePos(holePos)
            , Index(-1)
        {}
    private:
        ui32 Size;
        ui32 HolePos;
        ui32 Index;

        bool Skip() final {
            return ++Index < Size;
        }

        bool Next(NUdf::TUnboxedValue& value) final {
            if (!Skip())
                return false;
            value = Index == HolePos ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(Index);
            return true;
        }
    };

    struct TBrokenSeqListBoxedValue: public NUdf::TBoxedValue {
        TBrokenSeqListBoxedValue(ui32 size, ui32 holePos)
            : ListSize(size)
            , HolePos(holePos)
        {}

    private:
        ui32 ListSize;
        ui32 HolePos;

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return ListSize;
        }

        ui64 GetEstimatedListLength() const override {
            return ListSize;
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TBrokenSeqListIterator(ListSize, HolePos));
        }
    };

    template<class TStructType>
    struct TBrokenStructBoxedValue: public NUdf::TBoxedValue {
        TBrokenStructBoxedValue(const TStructType& data, ui32 holePos = RAW_INDEX_NO_HOLE)
            : Struct(data)
            , HolePos(holePos)
        {}

    private:
        const TStructType& Struct;
        ui32 HolePos;

        NUdf::TUnboxedValue GetElement(ui32 index) const override {
            if (index == HolePos) {
                return NUdf::TUnboxedValuePod();
            }
            return Struct.GetByIndex(TStructType::MetaBackIndexes[index]);
        }
    };


namespace {
    template<>
    NUdf::TUnboxedValue ToUnboxedValue<NUdf::PersonStruct>(const NUdf::PersonStruct& val) {
        return NUdf::TUnboxedValuePod(new TBrokenStructBoxedValue<NUdf::PersonStruct>(val));
    }

    template<>
    NUdf::TUnboxedValue ToUnboxedValue<NUdf::PersonStructWithOptList>(const NUdf::PersonStructWithOptList& val) {
        return NUdf::TUnboxedValuePod(new TBrokenStructBoxedValue<NUdf::PersonStructWithOptList>(val));
    }

    template<class TTupleType>
    struct TBrokenTupleBoxedValue: public NUdf::TBoxedValue {
        TBrokenTupleBoxedValue(const TTupleType& tuple, ui32 holePos)
            : Tuple(tuple)
            , HolePos(holePos)
        {}

    private:
        const TTupleType& Tuple;
        ui32 HolePos;

        NUdf::TUnboxedValue GetElement(ui32 index) const override {
            if (index == HolePos) {
                return NUdf::TUnboxedValuePod();
            }
            switch (index) {
                case 0: return ToUnboxedValue(std::get<0>(Tuple));
                case 1: return ToUnboxedValue(std::get<1>(Tuple));
                case 2: return ToUnboxedValue(std::get<2>(Tuple));
                case 3: return ToUnboxedValue(std::get<3>(Tuple));
                default: Y_ABORT("Unexpected");
            }
        }
    };

    typedef std::pair<ui32, ui32> PosPair;

    template<class TKey, class TValue>
    struct TBrokenDictIterator: public NUdf::TBoxedValue {
        TBrokenDictIterator(const std::vector<std::pair<TKey, TValue>>& dictData, PosPair holePos)
            : DictData(dictData)
            , HolePos(holePos)
            , Index(-1)
        {}

    private:
        const std::vector<std::pair<TKey, TValue>>& DictData;
        PosPair HolePos;
        ui32 Index;

        bool Skip() final {
            return ++Index < DictData.size();
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;
            key = Index == HolePos.first ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(DictData[Index].first);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key))
                return false;
            payload = Index == HolePos.second ? NUdf::TUnboxedValue() : ToUnboxedValue(DictData[Index].second);
            return true;
        }
    };

    template<class TKey, class TValue>
    struct TBrokenDictBoxedValue: public NUdf::TBoxedValue {
        TBrokenDictBoxedValue(const std::vector<std::pair<TKey, TValue>>& dictData,
            PosPair holePos, NUdf::TUnboxedValue&& hole = NUdf::TUnboxedValuePod())
            : DictData(dictData)
            , HolePos(holePos)
            , Hole(std::move(hole))
        {}

    private:
        const std::vector<std::pair<TKey, TValue>> DictData;
        PosPair HolePos;
        NUdf::TUnboxedValue Hole;

        NUdf::TUnboxedValue GetKeysIterator() const override {
            return NUdf::TUnboxedValuePod(new TBrokenDictIterator<TKey, TValue>(DictData, HolePos));
        }

        NUdf::TUnboxedValue GetDictIterator() const override {
            return NUdf::TUnboxedValuePod(new TBrokenDictIterator<TKey, TValue>(DictData, HolePos));
        }
    };

    struct TThrowerValue: public NUdf::TBoxedValue {
        static long Count;
        TThrowerValue(NUdf::IBoxedValuePtr&& owner = NUdf::IBoxedValuePtr())
            : Owner(std::move(owner))
        { ++Count; }
        ~TThrowerValue() { --Count; }
    private:
        const NUdf::IBoxedValuePtr Owner;

        bool Skip() override {
            ythrow yexception() << "Throw";
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TThrowerValue(const_cast<TThrowerValue*>(this)));
        }
    };

    SIMPLE_UDF(TException, NUdf::TListType<ui32>()) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return NUdf::TUnboxedValuePod(new TThrowerValue);
    }

    long TThrowerValue::Count = 0L;

    SIMPLE_UDF(TVoid, void()) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return NUdf::TUnboxedValuePod::Void();
    }

    SIMPLE_UDF_RUN(TNonEmpty, ui32(), NUdf::TOptional<void>) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return NUdf::TUnboxedValuePod(42);
    }

    SIMPLE_UDF(TOptionalNonEmpty, NUdf::TOptional<ui32>()) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return NUdf::TUnboxedValuePod(42);
    }

    SIMPLE_UDF_RUN(TOptionalEmpty, NUdf::TOptional<ui32>(), NUdf::TOptional<void>) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return NUdf::TUnboxedValuePod();
    }

    SIMPLE_UDF(TSub2Mul2BrokenOnLess2, ui32(ui32)) {
        Y_UNUSED(valueBuilder);
        const ui32 arg = args[0].Get<ui32>();
        if (arg >= 2) {
            return NUdf::TUnboxedValuePod((arg - 2) * 2);
        }
        return NUdf::TUnboxedValuePod();
    }

    SIMPLE_UDF_RUN(TBackSub2Mul2, ui32(NUdf::TCallableOneUi32Arg, ui32), NUdf::TOptional<void>) {
        const auto func = args[0];
        const auto& arg = args[1];
        auto usedArg = NUdf::TUnboxedValuePod();
        if (arg.Get<ui32>() < 100) {
            usedArg = arg;
        }
        auto funcResult = func.Run(valueBuilder, &usedArg);
        const auto& backResult = funcResult.Get<ui32>() / 2 + 2;
        return NUdf::TUnboxedValuePod(backResult);
    }

    SIMPLE_UDF(TSeqList, NUdf::TListType<ui32>(ui32)) {
        const ui32 size = args[0].Get<ui32>();
        std::vector<NUdf::TUnboxedValue> res;
        res.resize(size);
        for (ui32 i = 0; i < size; ++i) {
            res[i] = NUdf::TUnboxedValuePod(i);
        }
        return valueBuilder->NewList(res.data(), res.size());
    }

    SIMPLE_UDF_RUN(TSeqListWithHole, NUdf::TListType<ui32>(ui32, ui32), NUdf::TOptional<void>) {
        Y_UNUSED(valueBuilder);
        const ui32 size = args[0].Get<ui32>();
        const ui32 hole = args[1].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenSeqListBoxedValue(size, hole));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static const auto TUPLE = std::make_tuple(ui8(33), TString("world"), ui64(0xFEEDB00B2A115E), TString("funny bunny"));

    typedef NUdf::TTuple<ui8, char*, ui64, char*> NUdfTuple;

    SIMPLE_UDF(TTuple, NUdfTuple(ui32)) {
        Y_UNUSED(valueBuilder);
        const ui32 holePos = args[0].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenTupleBoxedValue<decltype(TUPLE)>(TUPLE, holePos));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static const std::vector<std::pair<ui32, ui64>> DICT_DIGIT2DIGIT = {
        {1, 100500},
        {42, 0xDEADBEAF},
        {911, 1234567890},
        {777, 777777777777},
    };

    typedef NUdf::TDict<ui32, ui64> NUdfDictDigDig;

    SIMPLE_UDF_RUN(TDictDigDig, NUdfDictDigDig(ui32, ui32), NUdf::TOptional<void>) {
        Y_UNUSED(valueBuilder);
        const ui32 holeKey = args[0].Get<ui32>();
        const ui32 holeValue = args[1].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenDictBoxedValue<ui32, ui64>(
            DICT_DIGIT2DIGIT, std::make_pair(holeKey, holeValue)));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    SIMPLE_UDF(TDictDigDigHoleAsOpt, NUdfDictDigDig(ui32, ui32)) {
        Y_UNUSED(valueBuilder);
        const ui32 holeKey = args[0].Get<ui32>();
        const ui32 holeValue = args[1].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenDictBoxedValue<ui32, ui64>(DICT_DIGIT2DIGIT,
            std::make_pair(holeKey, holeValue),
            NUdf::TUnboxedValuePod()));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static const NUdf::PersonStruct STRUCT_PERSON_JONNIE = {"Johnnie Walker", "Blue Label", 25};
    static const NUdf::PersonStruct STRUCT_PERSON_HITHCOCK = {"Alfred", "Hithcock", 81};
    static const NUdf::PersonStruct STRUCT_PERSON_LOVECRAFT = {"Howard", "Lovecraft", 25};
    static const NUdf::PersonStruct STRUCT_PERSON_KING = {"Stephen", "King", 25};
    static const NUdf::PersonStructWithOptList STRUCT_PERSON_HITHCOCK_LIST = {"Alfred", "Hithcock", 81, {}};
    static const NUdf::PersonStructWithOptList STRUCT_PERSON_LOVECRAFT_LIST = {"Howard", "Lovecraft", 25, {3, 2, 99}};
    static const NUdf::PersonStructWithOptList STRUCT_PERSON_KING_LIST = {"Stephen", "King", 25, {}};

    SIMPLE_UDF_RUN(TPersonStruct, NUdf::PersonStruct(ui32), NUdf::TOptional<void>) {
        Y_UNUSED(valueBuilder);
        const ui32 holePos = args[0].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_JONNIE, holePos));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    typedef NUdf::TTuple<NUdf::PersonStructWithOptList,NUdf::PersonStruct,NUdf::PersonStructWithOptList,NUdf::PersonStruct> NUdfPersonTuple;
    static const auto TUPLE_OF_PERSON = std::make_tuple(
        STRUCT_PERSON_HITHCOCK_LIST,
        STRUCT_PERSON_JONNIE,
        STRUCT_PERSON_LOVECRAFT_LIST,
        STRUCT_PERSON_KING);

    static const auto TUPLE_OF_PERSON_NO_LIST = std::make_tuple(STRUCT_PERSON_HITHCOCK_LIST,
        STRUCT_PERSON_JONNIE,
        STRUCT_PERSON_KING_LIST,
        STRUCT_PERSON_KING);

    SIMPLE_UDF(TTupleOfPersonStruct, NUdfPersonTuple(ui32)) {
        Y_UNUSED(valueBuilder);
        const ui32 holePos = args[0].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenTupleBoxedValue<decltype(TUPLE_OF_PERSON)>(TUPLE_OF_PERSON, holePos));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    SIMPLE_UDF(TTupleOfPersonStructNoList, NUdfPersonTuple(ui32)) {
        Y_UNUSED(valueBuilder);
        const ui32 holePos = args[0].Get<ui32>();
        NUdf::IBoxedValuePtr boxed(new TBrokenTupleBoxedValue<decltype(TUPLE_OF_PERSON_NO_LIST)>(TUPLE_OF_PERSON_NO_LIST, holePos));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static const std::vector<NUdf::PersonStructWithOptList> LIST_OF_STRUCT_PERSON = {
        STRUCT_PERSON_HITHCOCK_LIST,
        STRUCT_PERSON_LOVECRAFT_LIST,
        STRUCT_PERSON_KING_LIST
    };

    typedef NUdf::TDict<ui64,NUdf::PersonStructWithOptList> TIndexDictFromPersonList;
    SIMPLE_UDF(TListOfPersonStructToIndexDict, TIndexDictFromPersonList(ui32)) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        NUdf::IBoxedValuePtr boxed(new NUdf::TListRef<decltype(LIST_OF_STRUCT_PERSON)>(LIST_OF_STRUCT_PERSON));
        return valueBuilder->ToIndexDict(NUdf::TUnboxedValuePod(std::move(boxed)));
    }

    SIMPLE_UDF(TListOfPersonStruct, NUdf::TListType<NUdf::PersonStructWithOptList>(ui32)) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        NUdf::IBoxedValuePtr boxed(new NUdf::TListRef<decltype(LIST_OF_STRUCT_PERSON)>(LIST_OF_STRUCT_PERSON));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    SIMPLE_UDF(TListOfPersonStructWithBrokenIndexToDict, NUdf::TListType<NUdf::PersonStructWithOptList>()) {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        NUdf::IBoxedValuePtr boxed(new NUdf::TListRef<decltype(LIST_OF_STRUCT_PERSON), RAW_BROKEN_INDEX_LIST_TO_DICT>(
            LIST_OF_STRUCT_PERSON));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static const NUdf::PersonStruct* DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[] = {
        &STRUCT_PERSON_HITHCOCK, &STRUCT_PERSON_JONNIE, &STRUCT_PERSON_LOVECRAFT
    };
    const ui32 DICT_DIGIT2PERSON_BROKEN_PERSON_INDEX = 1;
    const ui32 DICT_DIGIT2PERSON_BROKEN_STRUCT_INDEX = 2;

    const std::vector<std::pair<ui32, NUdf::IBoxedValuePtr>> MAKE_DICT_DIGIT2PERSON_BROKEN() {
        std::vector<std::pair<ui32, NUdf::IBoxedValuePtr>> DICT_DIGIT2PERSON_BROKEN = {
            { 333, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_HITHCOCK, RAW_INDEX_NO_HOLE) },
            { 5, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_JONNIE, DICT_DIGIT2PERSON_BROKEN_STRUCT_INDEX) },
            { 77, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_LOVECRAFT, RAW_INDEX_NO_HOLE) },
        };

        return DICT_DIGIT2PERSON_BROKEN;
    }

    typedef NUdf::TDict<ui32,NUdf::PersonStruct> NUdfDictDigPerson;

    std::vector<std::pair<ui32, NUdf::IBoxedValuePtr>> MAKE_DICT_DIGIT2PERSON() {
        const std::vector<std::pair<ui32, NUdf::IBoxedValuePtr>> DICT_DIGIT2PERSON = {
            { 333, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_HITHCOCK, RAW_INDEX_NO_HOLE) },
            { 5, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_JONNIE, RAW_INDEX_NO_HOLE) },
            { 77, new TBrokenStructBoxedValue<NUdf::PersonStruct>(STRUCT_PERSON_LOVECRAFT, RAW_INDEX_NO_HOLE) },
        };

        return DICT_DIGIT2PERSON;
    }

    SIMPLE_UDF(TDictOfPerson, NUdfDictDigPerson()) {
        Y_UNUSED(args);
        Y_UNUSED(valueBuilder);
        NUdf::IBoxedValuePtr boxed(new TBrokenDictBoxedValue<ui32, NUdf::IBoxedValuePtr>(
            MAKE_DICT_DIGIT2PERSON(), std::make_pair(RAW_INDEX_NO_HOLE, RAW_INDEX_NO_HOLE)));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    SIMPLE_UDF_RUN(TDictOfPersonBroken, NUdfDictDigPerson(), NUdf::TOptional<void>) {
        Y_UNUSED(args);
        Y_UNUSED(valueBuilder);
        NUdf::IBoxedValuePtr boxed(new TBrokenDictBoxedValue<ui32, NUdf::IBoxedValuePtr>(
            MAKE_DICT_DIGIT2PERSON_BROKEN(), std::make_pair(RAW_INDEX_NO_HOLE, RAW_INDEX_NO_HOLE)));
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    SIMPLE_MODULE(TUtUDF,
        TException,
        TVoid,
        TNonEmpty,
        TOptionalNonEmpty,
        TOptionalEmpty,
        TSub2Mul2BrokenOnLess2,
        TBackSub2Mul2,
        TSeqList,
        TSeqListWithHole,
        TTuple,
        TDictDigDig,
        TDictDigDigHoleAsOpt,
        TPersonStruct,
        TTupleOfPersonStruct,
        TTupleOfPersonStructNoList,
        TListOfPersonStructToIndexDict,
        TListOfPersonStruct,
        TListOfPersonStructWithBrokenIndexToDict,
        TDictOfPerson,
        TDictOfPersonBroken
    )
} // unnamed namespace

TIntrusivePtr<IFunctionRegistry> CreateFunctionRegistryWithUDFs() {
    auto freg = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    freg->AddModule("", "UtUDF", new TUtUDF());
    return freg;
}

Y_UNIT_TEST_SUITE(TMiniKQLValidateTest) {
    typedef std::function<std::vector<TRuntimeNode>(TProgramBuilder&)> BuildArgsFunc;
    typedef std::function<void(const NUdf::TUnboxedValuePod&, const NUdf::IValueBuilder*)> ValidateValueFunc;
    typedef std::function<void(const NUdf::TUnboxedValuePod&, const NUdf::IValueBuilder*, const TType* type)> FullValidateValueFunc;


    void ProcessSimpleUdfFunc(const char* udfFuncName, BuildArgsFunc argsFunc = BuildArgsFunc(), ValidateValueFunc validateFunc = ValidateValueFunc(),
            FullValidateValueFunc fullValidateFunc = FullValidateValueFunc(),
            NUdf::EValidateMode validateMode = NUdf::EValidateMode::Lazy) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
        auto functionRegistry = CreateFunctionRegistryWithUDFs();
        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto funcName = pgmBuilder.Udf(udfFuncName);
        std::vector<TRuntimeNode> execArgs;
        if (argsFunc) {
            execArgs = argsFunc(pgmBuilder);
        }
        auto pgmReturn = pgmBuilder.Apply(funcName, execArgs);

        TExploringNodeVisitor explorer;
        explorer.Walk(pgmReturn.GetNode(), env);
        TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
            functionRegistry.Get(), validateMode,
            NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
        auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        const auto builder = static_cast<TDefaultValueBuilder*>(graph->GetTerminator());
        builder->RethrowAtTerminate();
        const TBindTerminator bind(graph->GetTerminator());
        auto value = graph->GetValue();

        if (validateFunc) {
            validateFunc(value, builder);
        }
        if (fullValidateFunc) {
            ui32 flags = 0;
            TFunctionTypeInfo funcInfo;
            TType* userType = nullptr;
            TStringBuf typeConfig;
            TStatus status = functionRegistry->FindFunctionTypeInfo(env, typeInfoHelper, nullptr, udfFuncName, userType, typeConfig, flags, {}, nullptr, &funcInfo);
            MKQL_ENSURE(status.IsOk(), status.GetError());
            auto type = funcInfo.FunctionType->GetReturnType();
            fullValidateFunc(value, builder, type);
        }
    }

    Y_UNIT_TEST(TestUdfException) {
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            valueBuilder->NewStringNotFilled(0xBAD).AsStringValue().Ref(); // Leak string.
            NUdf::TBoxedValueAccessor::Skip(*value.GetListIterator().AsBoxed().Release()); // Leak value and throw exception.
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.Exception", {}, validateFunc), yexception);
        UNIT_ASSERT_VALUES_EQUAL(TThrowerValue::Count, 0L);
    }

    Y_UNIT_TEST(TestUdfResultCheckVoid) {
        ProcessSimpleUdfFunc("UtUDF.Void");
    }

    Y_UNIT_TEST(TestUdfResultCheckExceptionOnEmpty) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        bool wrapped = false;
        UNIT_ASSERT_EXCEPTION(TValidate<TValidateErrorPolicyThrow>::Value(nullptr, env.GetTypeOfTypeLazy(),
            NUdf::TUnboxedValuePod(), "ut for verify empty value exception", &wrapped), TUdfValidateException);
        UNIT_ASSERT(!wrapped);
    }

    Y_UNIT_TEST(TestUdfResultCheckNonEmpty) {
        ProcessSimpleUdfFunc("UtUDF.NonEmpty");
    }

    Y_UNIT_TEST(TestUdfResultCheckOptionalNonEmpty) {
        ProcessSimpleUdfFunc("UtUDF.OptionalNonEmpty");
    }

    Y_UNIT_TEST(TestUdfResultCheckOptionalEmpty) {
        ProcessSimpleUdfFunc("UtUDF.OptionalEmpty");
    }

    std::vector<TRuntimeNode> MakeCallableInArgs(ui32 testVal, TProgramBuilder& pgmBuilder) {
        const auto& functionRegistry = pgmBuilder.GetFunctionRegistry();

        const auto udfFuncName = "UtUDF.Sub2Mul2BrokenOnLess2";
        ui32 flags = 0;
        TFunctionTypeInfo funcInfo;
        TType* userType = nullptr;
        TStringBuf typeConfig;
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
        TStatus status = functionRegistry.FindFunctionTypeInfo(pgmBuilder.GetTypeEnvironment(), typeInfoHelper, nullptr,
            udfFuncName, userType, typeConfig, flags, {}, nullptr, &funcInfo);
        MKQL_ENSURE(status.IsOk(), status.GetError());
        auto callable = pgmBuilder.Udf(udfFuncName);
        return std::vector<TRuntimeNode>{callable, pgmBuilder.NewDataLiteral(testVal)};
    };

    Y_UNIT_TEST(TestVerifyArgsCallableCorrect) {
        ui32 testVal = 44;
        BuildArgsFunc argsFunc = [testVal](TProgramBuilder& pgmBuilder) {
            return MakeCallableInArgs(testVal, pgmBuilder);
        };
        ValidateValueFunc validateFunc = [testVal](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            UNIT_ASSERT_VALUES_EQUAL(testVal, value.Get<ui32>());
        };
        ProcessSimpleUdfFunc("UtUDF.BackSub2Mul2", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestVerifyArgsCallableBrokenOnArgument) {
        ui32 testVal = 101;
        BuildArgsFunc argsFunc = [testVal](TProgramBuilder& pgmBuilder) {
            return MakeCallableInArgs(testVal, pgmBuilder);
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.BackSub2Mul2", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestVerifyArgsCallableBrokenOnReturn) {
        ui32 testVal = 1;
        BuildArgsFunc argsFunc = [testVal](TProgramBuilder& pgmBuilder) {
            return MakeCallableInArgs(testVal, pgmBuilder);
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.BackSub2Mul2", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckEmptySeqList) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(0)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto listIter = value.GetListIterator();
            UNIT_ASSERT(!listIter.Skip());
        };
        ProcessSimpleUdfFunc("UtUDF.SeqList", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckSeqList) {
        static constexpr ui32 listSize = 31;
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(listSize)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            ui32 index = 0;
            auto listIter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; listIter.Next(item); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), index);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, listSize);
        };
        ProcessSimpleUdfFunc("UtUDF.SeqList", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckSeqListWithHoleFirst) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            const ui32 listSize = 31;
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(listSize),
                pgmBuilder.NewDataLiteral<ui32>(0)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto listIter = value.GetListIterator();
            NUdf::TUnboxedValue item;
            UNIT_ASSERT_EXCEPTION(listIter.Next(item), TUdfValidateException);
            for (ui32 index = 1; listIter.Next(item); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), index);
            }
        };
        ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc);
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc, {}, NUdf::EValidateMode::Greedy), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckSeqListWithHoleMiddle) {
        static constexpr ui32 listSize = 31;
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(listSize),
                pgmBuilder.NewDataLiteral(listSize / 2)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), listSize);
            ui32 index = 0;
            const auto listIter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; index < listSize / 2 && listIter.Next(item); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), index);
            }
            NUdf::TUnboxedValue bad;
            UNIT_ASSERT_EXCEPTION(listIter.Next(bad), TUdfValidateException);
            ++index;
            for (NUdf::TUnboxedValue item; listIter.Next(item); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), index);
            }
        };
        ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc);
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc, {}, NUdf::EValidateMode::Greedy), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckSeqListWithHoleLast) {
        static constexpr ui32 listSize = 31;
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(listSize),
                pgmBuilder.NewDataLiteral(listSize - 1)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), listSize);
            ui32 index = 0;
            auto listIter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; index < listSize - 1 && listIter.Next(item); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), index);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, listSize - 1);
            NUdf::TUnboxedValue bad;
            UNIT_ASSERT_EXCEPTION(listIter.Next(bad), TUdfValidateException);
        };
        ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc);
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.SeqListWithHole", argsFunc, validateFunc, {}, NUdf::EValidateMode::Greedy), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckTuple) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ProcessSimpleUdfFunc("UtUDF.Tuple", argsFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckTupleWithHoleFirst) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(0)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.Tuple", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckTupleWithHoleMiddle) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(std::tuple_size<decltype(TUPLE)>::value / 2)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.Tuple", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckTupleWithHoleLast) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(std::tuple_size<decltype(TUPLE)>::value - 1)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.Tuple", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictDigitDigitFull) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE),
                pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto dictIter = value.GetDictIterator();
            ui32 index = 0;
            for (NUdf::TUnboxedValue key, payload; dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), DICT_DIGIT2DIGIT[index].first);
                UNIT_ASSERT_VALUES_EQUAL(payload.Get<ui64>(), DICT_DIGIT2DIGIT[index].second);
            }
            UNIT_ASSERT_VALUES_EQUAL(index, DICT_DIGIT2DIGIT.size());
        };
        ProcessSimpleUdfFunc("UtUDF.DictDigDig", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictDigitDigitKeyHole) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(0),
                pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
            for (ui32 index = 1; dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), DICT_DIGIT2DIGIT[index].first);
                UNIT_ASSERT_VALUES_EQUAL(payload.Get<ui64>(), DICT_DIGIT2DIGIT[index].second);
            }
        };
        ProcessSimpleUdfFunc("UtUDF.DictDigDig", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictDigitDigitValueHole) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE),
                pgmBuilder.NewDataLiteral<ui32>(DICT_DIGIT2DIGIT.size() - 1)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            for (ui32 index = 0; index < DICT_DIGIT2DIGIT.size() - 1 && dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), DICT_DIGIT2DIGIT[index].first);
                UNIT_ASSERT_VALUES_EQUAL(payload.Get<ui64>(), DICT_DIGIT2DIGIT[index].second);
            }
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
        };
        ProcessSimpleUdfFunc("UtUDF.DictDigDig", argsFunc, validateFunc);
    }
    Y_UNIT_TEST(TestUdfResultCheckDictDigitDigitHoleAsOptKeyHole) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE),
                pgmBuilder.NewDataLiteral<ui32>(0)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
            for (ui32 index = 1; dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), DICT_DIGIT2DIGIT[index].first);
                UNIT_ASSERT_VALUES_EQUAL(payload.Get<ui64>(), DICT_DIGIT2DIGIT[index].second);
            }
        };
        ProcessSimpleUdfFunc("UtUDF.DictDigDig", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictDigitDigitHoleAsOptValueHole) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(DICT_DIGIT2DIGIT.size() - 1),
                pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            for (ui32 index = 0; index < DICT_DIGIT2DIGIT.size() - 1 && dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), DICT_DIGIT2DIGIT[index].first);
                UNIT_ASSERT_VALUES_EQUAL(payload.Get<ui64>(), DICT_DIGIT2DIGIT[index].second);
            }
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
        };
        ProcessSimpleUdfFunc("UtUDF.DictDigDig", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckPersonStruct) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ProcessSimpleUdfFunc("UtUDF.PersonStruct", argsFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckPersonStructWithHoleFirst) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(0)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.PersonStruct", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckPersonStructWithHoleMiddle) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(1)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.PersonStruct", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckPersonStructWithHoleLast) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral<ui32>(2)};
        };
        UNIT_ASSERT_EXCEPTION(ProcessSimpleUdfFunc("UtUDF.PersonStruct", argsFunc), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckTupleOfPersonStruct) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        FullValidateValueFunc fullValidateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder,
            const TType* type) {
            bool wrapped = false;
            TValidate<TValidateErrorPolicyThrow, TValidateModeGreedy<TValidateErrorPolicyThrow>>::Value(valueBuilder, type, NUdf::TUnboxedValuePod(value), "full verify func", &wrapped);
            UNIT_ASSERT(!wrapped);
            TValidate<TValidateErrorPolicyThrow, TValidateModeLazy<TValidateErrorPolicyThrow>>::Value(valueBuilder, type, NUdf::TUnboxedValuePod(value), "full verify func", &wrapped);
            UNIT_ASSERT(wrapped);
        };
        ProcessSimpleUdfFunc("UtUDF.TupleOfPersonStruct", argsFunc, {}, fullValidateFunc);
        ProcessSimpleUdfFunc("UtUDF.TupleOfPersonStruct", argsFunc, {}, fullValidateFunc, NUdf::EValidateMode::Greedy);
    }

    Y_UNIT_TEST(TestUdfResultCheckTupleOfPersonStructNoList) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        FullValidateValueFunc fullValidateFunc = [](const NUdf::TUnboxedValuePod& value,
            const NUdf::IValueBuilder* valueBuilder, const TType* type) {
            bool wrapped = false;
            TValidate<TValidateErrorPolicyThrow>::Value(valueBuilder, type, NUdf::TUnboxedValuePod(value), "full verify func", &wrapped);
            UNIT_ASSERT(!wrapped);
        };
        ProcessSimpleUdfFunc("UtUDF.TupleOfPersonStructNoList", argsFunc, {}, fullValidateFunc);
        ProcessSimpleUdfFunc("UtUDF.TupleOfPersonStructNoList", argsFunc, {}, fullValidateFunc, NUdf::EValidateMode::Greedy);
    }

    void ValidateDictOfPersonStructFunc(const NUdf::TUnboxedValuePod& value, ui32 lookupIndex = 2, ui32 broken_index = RAW_INDEX_NO_HOLE) {
        const auto person = value.Lookup(NUdf::TUnboxedValuePod(ui64(lookupIndex)));
        UNIT_ASSERT(person);
        auto firstName = person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[0]);
        UNIT_ASSERT_VALUES_EQUAL(TString(firstName.AsStringRef()), LIST_OF_STRUCT_PERSON[lookupIndex].FirstName);
        auto lastName = person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[1]);
        UNIT_ASSERT_VALUES_EQUAL(TString(lastName.AsStringRef()), LIST_OF_STRUCT_PERSON[lookupIndex].LastName);
        UNIT_ASSERT_VALUES_EQUAL(person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[2]).Get<ui32>(), LIST_OF_STRUCT_PERSON[lookupIndex].Age);
        UNIT_ASSERT(!person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[3]));
        auto dictIter = value.GetDictIterator();
        NUdf::TUnboxedValue key, payload;
        for (ui32 index = 0; index < broken_index && dictIter.NextPair(key, payload); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(key.Get<ui64>(), index);
            auto person = payload;
            auto firstName = person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[0]);
            UNIT_ASSERT_VALUES_EQUAL(TString(firstName.AsStringRef()), LIST_OF_STRUCT_PERSON[index].FirstName);
            auto lastName = person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[1]);
            UNIT_ASSERT_VALUES_EQUAL(TString(lastName.AsStringRef()), LIST_OF_STRUCT_PERSON[index].LastName);
            UNIT_ASSERT_VALUES_EQUAL(person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[2]).Get<ui32>(), LIST_OF_STRUCT_PERSON[index].Age);
            const auto origListPtr = LIST_OF_STRUCT_PERSON[index].Tags;
            if (origListPtr.empty()) {
                UNIT_ASSERT(!person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[3]));
            } else {
                auto memberTags = person.GetElement(NUdf::PersonStructWithOptList::MetaIndexes[3]);
                UNIT_ASSERT(memberTags);
                UNIT_ASSERT_VALUES_EQUAL(memberTags.GetListLength(), origListPtr.size());
                auto origIter = origListPtr.begin();
                auto iter = memberTags.GetListIterator();
                for (NUdf::TUnboxedValue item; iter.Next(item); ++origIter) {
                    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), *origIter);
                }
            }
        }
        if (broken_index < RAW_INDEX_NO_HOLE)
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
    }

    Y_UNIT_TEST(TestUdfResultCheckListOfPersonStructToIndexDict) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder) {
            Y_UNUSED(valueBuilder);
            ValidateDictOfPersonStructFunc(value);
        };
        ProcessSimpleUdfFunc("UtUDF.ListOfPersonStructToIndexDict", argsFunc, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckListOfPersonStruct) {
        BuildArgsFunc argsFunc = [](TProgramBuilder& pgmBuilder) {
            return std::vector<TRuntimeNode>{pgmBuilder.NewDataLiteral(RAW_INDEX_NO_HOLE)};
        };
        FullValidateValueFunc fullValidateFunc = [](const NUdf::TUnboxedValuePod& value,
            const NUdf::IValueBuilder* valueBuilder, const TType* type) {
            Y_UNUSED(type);
            auto indexDict = valueBuilder->ToIndexDict(value);
            ValidateDictOfPersonStructFunc(indexDict);
        };
        ProcessSimpleUdfFunc("UtUDF.ListOfPersonStruct", argsFunc, {}, fullValidateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckListOfPersonStructWithBrokenIndexToDict) {
        FullValidateValueFunc fullValidateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder* valueBuilder,
            const TType* type) {
            Y_UNUSED(type);
            auto indexDict = valueBuilder->ToIndexDict(value);
            static_assert(RAW_BROKEN_INDEX_LIST_TO_DICT == 1, "a list is too small");
            ValidateDictOfPersonStructFunc(indexDict, RAW_BROKEN_INDEX_LIST_TO_DICT - 1, RAW_BROKEN_INDEX_LIST_TO_DICT);
            /// verify lookup fail on broken index
            UNIT_ASSERT_EXCEPTION(ValidateDictOfPersonStructFunc(indexDict, RAW_BROKEN_INDEX_LIST_TO_DICT, RAW_BROKEN_INDEX_LIST_TO_DICT), TUdfValidateException);
            ValidateDictOfPersonStructFunc(indexDict, RAW_BROKEN_INDEX_LIST_TO_DICT + 1, RAW_BROKEN_INDEX_LIST_TO_DICT);
        };
        ProcessSimpleUdfFunc("UtUDF.ListOfPersonStructWithBrokenIndexToDict", {}, {}, fullValidateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictOfPerson) {
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder*) {
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            for (ui32 index = 0; dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), MAKE_DICT_DIGIT2PERSON()[index].first);
                auto person = payload;
                auto firstName = person.GetElement(NUdf::PersonStruct::MetaIndexes[0]);
                UNIT_ASSERT_VALUES_EQUAL(TString(firstName.AsStringRef()), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->FirstName);
                auto lastName = person.GetElement(NUdf::PersonStruct::MetaIndexes[1]);
                UNIT_ASSERT_VALUES_EQUAL(TString(lastName.AsStringRef()), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->LastName);
                UNIT_ASSERT_VALUES_EQUAL(person.GetElement(NUdf::PersonStruct::MetaIndexes[2]).Get<ui32>(), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->Age);
            }
        };
        ProcessSimpleUdfFunc("UtUDF.DictOfPerson", {}, validateFunc);
    }

    Y_UNIT_TEST(TestUdfResultCheckDictOfPersonBroken) {
        ValidateValueFunc validateFunc = [](const NUdf::TUnboxedValuePod& value, const NUdf::IValueBuilder*) {
            auto dictIter = value.GetDictIterator();
            NUdf::TUnboxedValue key, payload;
            for (ui32 index = 0; index < DICT_DIGIT2PERSON_BROKEN_PERSON_INDEX && dictIter.NextPair(key, payload); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(key.Get<ui32>(), MAKE_DICT_DIGIT2PERSON_BROKEN()[index].first);
                auto person = payload;
                auto firstName = person.GetElement(NUdf::PersonStruct::MetaIndexes[0]);
                UNIT_ASSERT_VALUES_EQUAL(TString(firstName.AsStringRef()), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->FirstName);
                auto lastName = person.GetElement(NUdf::PersonStruct::MetaIndexes[1]);
                UNIT_ASSERT_VALUES_EQUAL(TString(lastName.AsStringRef()), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->LastName);
                UNIT_ASSERT_VALUES_EQUAL(person.GetElement(NUdf::PersonStruct::MetaIndexes[2]).Get<ui32>(), DICT_DIGIT2PERSON_BROKEN_CONTENT_BY_INDEX[index]->Age);
            }
            UNIT_ASSERT_EXCEPTION(dictIter.NextPair(key, payload), TUdfValidateException);
        };
        ProcessSimpleUdfFunc("UtUDF.DictOfPersonBroken", {}, validateFunc);
    }

}

}
