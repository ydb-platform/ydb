#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include "mkql_validate.h"
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

namespace NKikimr::NMiniKQL {

namespace {

static const TString VERIFY_DELIMITER = "\n - ";

using namespace NUdf;

template <class TValidateErrorPolicy>
struct TLazyVerifyListValue;

template <class TValidateErrorPolicy>
struct TLazyVerifyListIterator: public TBoxedValue {
    explicit TLazyVerifyListIterator(const TLazyVerifyListValue<TValidateErrorPolicy>& lazyList, ui64 index = 0)
        : LazyList_(lazyList)
        , OrigIter_(TBoxedValueAccessor::GetListIterator(*lazyList.Orig))
        , Index_(index)
    {
        if (!OrigIter_) {
            TValidateErrorPolicy::Generate(TStringBuilder() << "TLazyVerifyListIterator constructor expect non empty result of GetListIterator" << VERIFY_DELIMITER << LazyList_.Message);
        }
    }

private:
    const TLazyVerifyListValue<TValidateErrorPolicy>& LazyList_;
    TUnboxedValue OrigIter_;
    ui64 Index_;

    bool Next(NUdf::TUnboxedValue& item) final {
        ++Index_;
        if (!OrigIter_.Next(item)) {
            return false;
        }
        TType* itemType = LazyList_.ListType->GetItemType();
        item = TValidate<
            TValidateErrorPolicy,
            TValidateModeLazy<TValidateErrorPolicy>>::Value(LazyList_.ValueBuilder,
                                                            itemType,
                                                            std::move(item),
                                                            TStringBuilder() << "LazyList[" << Index_ << "]" << VERIFY_DELIMITER << LazyList_.Message);
        return true;
    }

    bool Skip() final {
        ++Index_;
        return OrigIter_.Skip();
    }
};

template <class TValidateErrorPolicy>
struct TLazyVerifyDictValue;

template <class TValidateErrorPolicy>
struct TLazyVerifyListValue: public TBoxedValue {
    TLazyVerifyListValue(const IValueBuilder* valueBuilder, const TListType* listType, IBoxedValuePtr&& orig, const TString& message)
        : ValueBuilder(valueBuilder)
        , ListType(listType)
        , Orig(std::move(orig))
        , Message(message)
    {
    }

    const IValueBuilder* const ValueBuilder;
    const TListType* const ListType;
    const IBoxedValuePtr Orig;
    const TString Message;

private:
    bool HasFastListLength() const override {
        return TBoxedValueAccessor::HasFastListLength(*Orig);
    }

    ui64 GetListLength() const override {
        return TBoxedValueAccessor::GetListLength(*Orig);
    }

    ui64 GetEstimatedListLength() const override {
        return TBoxedValueAccessor::GetEstimatedListLength(*Orig);
    }

    TUnboxedValue GetListIterator() const override {
        return TUnboxedValuePod(new TLazyVerifyListIterator<TValidateErrorPolicy>(*this));
    }

    const TOpaqueListRepresentation* GetListRepresentation() const override {
        return nullptr;
    }

    IBoxedValuePtr ReverseListImpl(const IValueBuilder& builder) const override {
        auto upList = TBoxedValueAccessor::ReverseListImpl(*Orig, builder);
        if (upList) {
            return new TLazyVerifyListValue<TValidateErrorPolicy>(ValueBuilder, ListType, std::move(upList), Message);
        }
        return upList;
    }

    IBoxedValuePtr SkipListImpl(const IValueBuilder& builder, ui64 count) const override {
        auto upList = TBoxedValueAccessor::SkipListImpl(*Orig, builder, count);
        if (upList) {
            return new TLazyVerifyListValue<TValidateErrorPolicy>(ValueBuilder, ListType, std::move(upList), Message);
        }
        return upList;
    }

    IBoxedValuePtr TakeListImpl(const IValueBuilder& builder, ui64 count) const override {
        auto upList = TBoxedValueAccessor::TakeListImpl(*Orig, builder, count);
        if (upList) {
            return new TLazyVerifyListValue<TValidateErrorPolicy>(ValueBuilder, ListType, std::move(upList), Message);
        }
        return upList;
    }

    IBoxedValuePtr ToIndexDictImpl(const IValueBuilder& builder) const override {
        auto dictImpl = TBoxedValueAccessor::ToIndexDictImpl(*Orig, builder);
        if (!dictImpl) {
            return dictImpl;
        }
        return IBoxedValuePtr(new TLazyVerifyDictValue<TValidateErrorPolicy>(
            &builder, ListType->IndexDictKeyType(), ListType->GetItemType(), std::move(dictImpl),
            TStringBuilder() << "LazyDict over IndexToDict" << VERIFY_DELIMITER << Message));
    }

    bool HasListItems() const override {
        return TBoxedValueAccessor::HasListItems(*Orig);
    }

    bool HasDictItems() const override {
        const bool result = TBoxedValueAccessor::HasDictItems(*Orig);
        if (result) {
            TValidateErrorPolicy::Generate(
                TStringBuilder() << "TLazyVerifyListValue::HasDictItems expect false, but got true" << VERIFY_DELIMITER << Message);
        }
        return result;
    }
};

template <class TValidateErrorPolicy, bool Keys>
struct TLazyVerifyDictIterator: public TBoxedValue {
    explicit TLazyVerifyDictIterator(const TLazyVerifyDictValue<TValidateErrorPolicy>& lazyDict, ui64 index = 0)
        : LazyDict_(lazyDict)
        , OrigIter_((Keys ? &TBoxedValueAccessor::GetKeysIterator : &TBoxedValueAccessor::GetDictIterator)(*LazyDict_.Orig))
        , Index_(index)
    {
        if (!OrigIter_) {
            TValidateErrorPolicy::Generate(
                TStringBuilder() << "TLazyVerifyDictIterator constructor expect non empty result of GetDictIterator" << VERIFY_DELIMITER << LazyDict_.Message);
        }
    }

private:
    const TLazyVerifyDictValue<TValidateErrorPolicy>& LazyDict_;
    TUnboxedValue OrigIter_;
    ui64 Index_;

    bool Skip() final {
        ++Index_;
        return OrigIter_.Skip();
    }

    bool Next(NUdf::TUnboxedValue& key) final {
        ++Index_;
        if (!OrigIter_.Next(key)) {
            return false;
        }
        key = TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
            LazyDict_.ValueBuilder,
            LazyDict_.KeyType,
            std::move(key),
            TStringBuilder() << "LazyDict[" << Index_ << "], validate key" << VERIFY_DELIMITER << LazyDict_.Message);
        return true;
    }

    bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
        ++Index_;
        if (!OrigIter_.NextPair(key, payload)) {
            return false;
        }
        key = TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
            LazyDict_.ValueBuilder,
            LazyDict_.KeyType,
            std::move(key),
            TStringBuilder() << "LazyDict[" << Index_ << "], validate key" << VERIFY_DELIMITER << LazyDict_.Message);
        payload = TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
            LazyDict_.ValueBuilder,
            LazyDict_.PayloadType,
            std::move(payload),
            TStringBuilder() << "LazyDict[" << Index_ << "], validate payload" << VERIFY_DELIMITER << LazyDict_.Message);
        return true;
    }
};

template <class TValidateErrorPolicy>
struct TLazyVerifyDictValue: public TBoxedValue {
    TLazyVerifyDictValue(const IValueBuilder* valueBuilder, const TDictType* dictType, IBoxedValuePtr&& orig, const TString& message)
        : TLazyVerifyDictValue(valueBuilder, dictType->GetKeyType(), dictType->GetPayloadType(), std::move(orig), message)
    {
    }

    TLazyVerifyDictValue(const IValueBuilder* valueBuilder, const TType* keyType, const TType* payloadType, IBoxedValuePtr&& orig, const TString& message)
        : ValueBuilder(valueBuilder)
        , KeyType(keyType)
        , PayloadType(payloadType)
        , Orig(std::move(orig))
        , Message(message)
    {
    }

    const IValueBuilder* const ValueBuilder;
    const TType* const KeyType;
    const TType* const PayloadType;
    const IBoxedValuePtr Orig;
    const TString Message;

private:
    ui64 GetDictLength() const override {
        return TBoxedValueAccessor::GetDictLength(*Orig);
    }

    TUnboxedValue GetKeysIterator() const override {
        return TUnboxedValuePod(new TLazyVerifyDictIterator<TValidateErrorPolicy, true>(*this));
    }

    TUnboxedValue GetDictIterator() const override {
        return TUnboxedValuePod(new TLazyVerifyDictIterator<TValidateErrorPolicy, false>(*this));
    }

    bool Contains(const TUnboxedValuePod& key) const override {
        return TBoxedValueAccessor::Contains(*Orig, key);
    }
    TUnboxedValue Lookup(const TUnboxedValuePod& key) const override {
        if (auto lookup = TBoxedValueAccessor::Lookup(*Orig, key)) {
            return TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
                       ValueBuilder,
                       PayloadType,
                       lookup.Release().GetOptionalValue(),
                       TStringBuilder() << "LazyDict, validate Lookup payload" << VERIFY_DELIMITER << Message)
                .Release()
                .MakeOptional();
        }
        return TUnboxedValuePod();
    }

    bool HasListItems() const override {
        const bool result = TBoxedValueAccessor::HasListItems(*Orig);
        if (result) {
            TValidateErrorPolicy::Generate(
                TStringBuilder() << "TLazyVerifyDictValue::HasListItems expect false, but got true" << VERIFY_DELIMITER << Message);
        }
        return result;
    }

    bool HasDictItems() const override {
        return TBoxedValueAccessor::HasDictItems(*Orig);
    }
};

template <class TValidateErrorPolicy, class TValidateMode>
class WrapCallableValue: public TBoxedValue {
public:
    WrapCallableValue(const TCallableType* callableType, TUnboxedValue&& callable, const TString& message);

private:
    const TCallableType* const CallableType_;
    const TUnboxedValue Callable_;
    const TString Message_;

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final;
};

template <class TValidateErrorPolicy, class TValidateMode>
WrapCallableValue<TValidateErrorPolicy, TValidateMode>::WrapCallableValue(
    const TCallableType* callableType, TUnboxedValue&& callable, const TString& message)
    : CallableType_(callableType)
    , Callable_(std::move(callable))
    , Message_(message)
{
}

template <class TValidateErrorPolicy, class TValidateMode>
TUnboxedValue WrapCallableValue<TValidateErrorPolicy, TValidateMode>::Run(
    const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
    const ui32 argsCount = CallableType_->GetArgumentsCount();
    TSmallVec<TUnboxedValue> wrapArgs(argsCount);
    bool childWrapped = false;
    for (ui32 indexArg = 0; indexArg < argsCount; ++indexArg) {
        const auto argType = CallableType_->GetArgumentType(indexArg);
        wrapArgs[indexArg] = TValidate<TValidateErrorPolicy, TValidateMode>::Value(valueBuilder, argType, TUnboxedValuePod(args[indexArg]), TStringBuilder() << "CallableWrapper<" << CallableType_->GetName() << ">.arg[" << indexArg << "]" << VERIFY_DELIMITER << Message_, &childWrapped);
    }
    return TValidate<TValidateErrorPolicy, TValidateMode>::Value(valueBuilder, CallableType_->GetReturnType(), Callable_.Run(valueBuilder, childWrapped ? wrapArgs.data() : args), TStringBuilder() << "CallableWrapper<" << CallableType_->GetName() << ">.result" << VERIFY_DELIMITER << Message_);
}

} // anonymous namespace

template <class TValidateErrorPolicy>
struct TValidateModeLazy {
    static NUdf::TUnboxedValue ProcessList(
        const IValueBuilder* valueBuilder, const TListType* listType,
        IBoxedValuePtr&& boxed, const TString& message, bool* wrapped) {
        if (wrapped) {
            *wrapped = true;
        }
        return NUdf::TUnboxedValuePod(new TLazyVerifyListValue<TValidateErrorPolicy>(
            valueBuilder,
            listType,
            std::move(boxed),
            message));
    }

    static NUdf::TUnboxedValue ProcessDict(
        const IValueBuilder* valueBuilder, const TDictType* dictType,
        IBoxedValuePtr&& boxed, const TString& message, bool* wrapped) {
        if (wrapped) {
            *wrapped = true;
        }
        return NUdf::TUnboxedValuePod(new TLazyVerifyDictValue<TValidateErrorPolicy>(
            valueBuilder,
            dictType,
            std::move(boxed),
            message));
    }
};

template <class TValidateErrorPolicy>
struct TValidateModeGreedy {
    static NUdf::TUnboxedValue ProcessList(const IValueBuilder* valueBuilder, const TListType* listType,
                                           IBoxedValuePtr&& boxed, const TString& message, bool* wrapped) {
        if (!TBoxedValueAccessor::HasFastListLength(*boxed)) {
            return NUdf::TUnboxedValuePod(new TLazyVerifyListValue<TValidateErrorPolicy>(
                valueBuilder,
                listType,
                std::move(boxed),
                message));
        }

        const TType* itemType = listType->GetItemType();
        std::vector<NUdf::TUnboxedValue> list;
        if (TBoxedValueAccessor::HasFastListLength(*boxed)) {
            list.reserve(TBoxedValueAccessor::GetListLength(*boxed));
        }
        bool childWrapped = false;
        ui64 curIndex = 0;
        const auto iter = TBoxedValueAccessor::GetListIterator(*boxed);
        for (NUdf::TUnboxedValue current; iter.Next(current); ++curIndex) {
            list.emplace_back(TValidate<TValidateErrorPolicy, TValidateModeGreedy>::Value(
                valueBuilder,
                itemType,
                std::move(current),
                TStringBuilder() << "LazyList[" << curIndex << "]" << VERIFY_DELIMITER << message, &childWrapped));
        }
        const auto elementsCount = TBoxedValueAccessor::GetListLength(*boxed);
        if (curIndex != elementsCount) {
            TValidateErrorPolicy::Generate(
                TStringBuilder() << "TValidateModeGreedy::ProcessList, wrong list length returned, expected: "
                                 << curIndex << ", but got: " << elementsCount << VERIFY_DELIMITER << message);
        }
        if (childWrapped) {
            if (wrapped) {
                *wrapped = true;
            }
            return valueBuilder->NewList(list.data(), list.size());
        }
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }

    static TUnboxedValue ProcessDict(const IValueBuilder* valueBuilder, const TDictType* dictType,
                                     IBoxedValuePtr&& boxed, const TString& message, bool* wrapped) {
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        auto dictBuilder = valueBuilder->NewDict(dictType, TDictFlags::Sorted);
        bool childWrapped = false;
        ui64 curIndex = 0;
        const auto iter = TBoxedValueAccessor::GetDictIterator(*boxed);
        for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload); ++curIndex) {
            key = TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
                valueBuilder,
                keyType,
                std::move(key),
                TStringBuilder() << "GreedyDict[" << curIndex << "], validate key" << VERIFY_DELIMITER << message);
            payload = TValidate<TValidateErrorPolicy, TValidateModeLazy<TValidateErrorPolicy>>::Value(
                valueBuilder,
                payloadType,
                std::move(payload),
                TStringBuilder() << "GreedyDict[" << curIndex << "], validate payload" << VERIFY_DELIMITER << message);
            dictBuilder->Add(std::move(key), std::move(payload));
        }
        const auto elementsCount = TBoxedValueAccessor::GetDictLength(*boxed);
        if (curIndex != elementsCount) {
            TValidateErrorPolicy::Generate(
                TStringBuilder() << "TValidateModeGreedy::ProcessDict, wrong dict length returned, expected: "
                                 << curIndex << ", but got: " << elementsCount << VERIFY_DELIMITER << message);
        }
        if (childWrapped) {
            if (wrapped) {
                *wrapped = true;
            }
            return dictBuilder->Build();
        }
        return NUdf::TUnboxedValuePod(std::move(boxed));
    }
};

template <class TValidateErrorPolicy, class TValidateMode>
NUdf::TUnboxedValue TValidate<TValidateErrorPolicy, TValidateMode>::Value(const IValueBuilder* valueBuilder, const TType* type, NUdf::TUnboxedValue&& value, const TString& message, bool* wrapped) {
    if (!value && !(type->IsOptional() || type->IsNull())) {
        TValidateErrorPolicy::Generate(
            TStringBuilder() << "Expected value '" << PrintNode(type, true)
                             << "', but got Empty" << VERIFY_DELIMITER << message);
    }

    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            break;

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            auto dataTypeId = dataType->GetSchemeType();
            auto slot = NUdf::FindDataSlot(dataTypeId);
            if (!slot) {
                TValidateErrorPolicy::GenerateExc(
                    TUdfValidateException() << "Unregistered TypeId: " << dataTypeId << VERIFY_DELIMITER << message);
            }
            if (!IsValidValue(*slot, value)) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "Expected value '" << PrintNode(type, true)
                                     << "' does not conform" << VERIFY_DELIMITER << message);
            }
            break;
        }

        case TType::EKind::Optional: {
            if (!value) {
                break;
            }
            auto optionalType = static_cast<const TOptionalType*>(type);
            bool childWrapped = false;
            auto upValue = TValidate<TValidateErrorPolicy, TValidateMode>::Value(
                valueBuilder,
                optionalType->GetItemType(),
                value.GetOptionalValue(),
                TStringBuilder() << "Optional" << message, &childWrapped);
            if (childWrapped) {
                if (wrapped) {
                    *wrapped = true;
                }
                return upValue.Release().MakeOptional();
            }
            break;
        }

        case TType::EKind::List: {
            if (!value.IsBoxed()) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "expected value '" << PrintNode(type, true)
                                     << "' not conform" << VERIFY_DELIMITER << message);
            }
            auto listType = static_cast<const TListType*>(type);
            return TValidateMode::ProcessList(valueBuilder, listType, std::move(value.Release().AsBoxed()), message, wrapped);
        }

        case TType::EKind::Struct: {
            if (!value.IsBoxed()) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "expected value '" << PrintNode(type, true)
                                     << "' not conform" << VERIFY_DELIMITER << message);
            }
            auto structType = static_cast<const TStructType*>(type);
            bool childWrapped = false;
            TSmallVec<TUnboxedValue> stackItems(structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                TType* memberType = structType->GetMemberType(index);
                stackItems[index] = TValidate<TValidateErrorPolicy, TValidateMode>::Value(
                    valueBuilder,
                    memberType,
                    value.GetElement(index),
                    TStringBuilder() << "Struct[" << structType->GetMemberName(index) << "]"
                                     << VERIFY_DELIMITER << message, &childWrapped);
            }
            if (childWrapped) {
                TUnboxedValue* items = nullptr;
                const auto wrappedStruct = valueBuilder->NewArray(
                    structType->GetMembersCount(), items);
                for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                    items[index] = std::move(stackItems[index]);
                }
                if (wrapped) {
                    *wrapped = true;
                }
                return wrappedStruct;
            }
            break;
        }

        case TType::EKind::Tuple: {
            if (!value.IsBoxed()) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "expected value '" << PrintNode(type, true)
                                     << "' not conform" << VERIFY_DELIMITER << message);
            }
            auto tupleType = static_cast<const TTupleType*>(type);
            bool childWrapped = false;
            TSmallVec<NUdf::TUnboxedValue> stackItems(tupleType->GetElementsCount());
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                TType* elementType = tupleType->GetElementType(index);
                stackItems[index] = TValidate<TValidateErrorPolicy, TValidateMode>::Value(
                    valueBuilder,
                    elementType,
                    value.GetElement(index),
                    TStringBuilder() << "Tuple[" << index << "]" << VERIFY_DELIMITER << message, &childWrapped);
            }
            if (childWrapped) {
                TUnboxedValue* items = nullptr;
                const auto wrappedTuple = valueBuilder->NewArray(tupleType->GetElementsCount(), items);
                for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                    items[index] = std::move(stackItems[index]);
                }
                if (wrapped) {
                    *wrapped = true;
                }
                return wrappedTuple;
            }
            break;
        }

        case TType::EKind::Dict: {
            if (!value.IsBoxed()) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "expected value '" << PrintNode(type, true)
                                     << "' not conform" << VERIFY_DELIMITER << message);
            }
            auto dictType = static_cast<const TDictType*>(type);
            return TValidateMode::ProcessDict(valueBuilder, dictType, std::move(value.Release().AsBoxed()), message, wrapped);
        }

        case TType::EKind::Callable: {
            if (!value.IsBoxed()) {
                TValidateErrorPolicy::Generate(
                    TStringBuilder() << "expected value '" << PrintNode(type, true)
                                     << "' not conform" << VERIFY_DELIMITER << message);
            }
            auto callableType = static_cast<const TCallableType*>(type);
            if (wrapped) {
                *wrapped = true;
            }
            TValidate<TValidateErrorPolicy>::WrapCallable(callableType, value, message);
            return value;
        }

        case TType::EKind::Type:
            // metatype allways valid if we meet it
            break;

        case TType::EKind::Resource:
            // resource allways valid if we meet it
            break;

        case TType::EKind::Stream:
        case TType::EKind::Variant:
        case TType::EKind::Block:
        case TType::EKind::Linear:
            // TODO validate it
            break;

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            bool childWrapped = false;
            auto upValue = TValidate<TValidateErrorPolicy, TValidateMode>::Value(
                valueBuilder,
                taggedType->GetBaseType(),
                TUnboxedValue(value),
                TStringBuilder() << "Tagged[" << taggedType->GetTag() << "]" << message, &childWrapped);
            if (childWrapped) {
                if (wrapped) {
                    *wrapped = true;
                }
                return upValue;
            }
            break;
        }

        default:
            Y_ABORT("Verify value meet unexpected type kind: %s", type->GetKindAsStr().data());
    }
    return std::move(value);
}

template <class TValidateErrorPolicy, class TValidateMode>
void TValidate<TValidateErrorPolicy, TValidateMode>::WrapCallable(
    const TCallableType* callableType, NUdf::TUnboxedValue& callable, const TString& message) {
    callable = NUdf::TUnboxedValuePod(new WrapCallableValue<TValidateErrorPolicy, TValidateMode>(
        callableType,
        std::move(callable),
        TStringBuilder() << "CallableWrapper<" << callableType->GetName() << ">" << VERIFY_DELIMITER << message));
}

template struct TValidate<TValidateErrorPolicyThrow, TValidateModeLazy<TValidateErrorPolicyThrow>>;
template struct TValidate<TValidateErrorPolicyThrow, TValidateModeGreedy<TValidateErrorPolicyThrow>>;
template struct TValidate<TValidateErrorPolicyFail, TValidateModeLazy<TValidateErrorPolicyFail>>;
template struct TValidate<TValidateErrorPolicyFail, TValidateModeGreedy<TValidateErrorPolicyFail>>;

} // namespace NKikimr::NMiniKQL
