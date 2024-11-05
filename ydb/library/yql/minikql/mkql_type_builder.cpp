#include "mkql_type_builder.h"
#include "mkql_node_cast.h"
#include "mkql_node_builder.h"
#include "mkql_alloc.h"

#include <ydb/library/yql/public/udf/udf_type_ops.h>
#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>
#include <ydb/library/yql/public/udf/arrow/block_item_hasher.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/compare.h>
#include <array>

#include <arrow/c/bridge.h>

// TODO: remove const_casts

namespace NKikimr {

namespace {

static const TString UdfName("UDF");

class TPgTypeIndex {
    using TUdfTypes = TVector<NYql::NUdf::TPgTypeDescription>;
    TUdfTypes Types;

public:
    TPgTypeIndex() {
        Rebuild();
    }

    void Rebuild() {
        Types.clear();
        ui32 maxTypeId = 0;
        NYql::NPg::EnumTypes([&](ui32 typeId, const NYql::NPg::TTypeDesc&) {
            maxTypeId = Max(maxTypeId, typeId);
        });

        Types.resize(maxTypeId + 1);
        NYql::NPg::EnumTypes([&](ui32 typeId, const NYql::NPg::TTypeDesc& t) {
            auto& e = Types[typeId];
            e.Name = t.Name;
            e.TypeId = t.TypeId;
            e.Typelen = t.TypeLen;
            e.ArrayTypeId = t.ArrayTypeId;
            e.ElementTypeId = t.ElementTypeId;
            e.PassByValue = t.PassByValue;
        });
    }

    const NYql::NUdf::TPgTypeDescription* Resolve(ui32 typeId) const {
        if (typeId >= Types.size()) {
            return nullptr;
        }
        auto& e = Types[typeId];
        if (!e.TypeId) {
            return nullptr;
        }
        return &e;
    }
};

/////////////////////////////////////////////////////////////////////////////
// TOptionalTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TOptionalTypeBuilder: public NUdf::IOptionalTypeBuilder
{
public:
    TOptionalTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::IOptionalTypeBuilder& Item(NUdf::TDataTypeId typeId) override {
        ItemType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IOptionalTypeBuilder& Item(const NUdf::TType* type) override {
        ItemType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IOptionalTypeBuilder& Item(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        ItemType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TOptionalType::Create(
                    const_cast<NMiniKQL::TType*>(ItemType_),
                    Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TListTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TListTypeBuilder: public NUdf::IListTypeBuilder
{
public:
    TListTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::IListTypeBuilder& Item(NUdf::TDataTypeId typeId) override {
        ItemType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IListTypeBuilder& Item(const NUdf::TType* type) override {
        ItemType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IListTypeBuilder& Item(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        ItemType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TListType::Create(
                    const_cast<NMiniKQL::TType*>(ItemType_), Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TStreamTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TStreamTypeBuilder : public NUdf::IStreamTypeBuilder
{
public:
    TStreamTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::IStreamTypeBuilder& Item(NUdf::TDataTypeId typeId) override {
        ItemType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IStreamTypeBuilder& Item(const NUdf::TType* type) override {
        ItemType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IStreamTypeBuilder& Item(
        const NUdf::ITypeBuilder& typeBuilder) override
    {
        ItemType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TStreamType::Create(
            const_cast<NMiniKQL::TType*>(ItemType_), Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TDictTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TDictTypeBuilder: public NUdf::IDictTypeBuilder
{
public:
    TDictTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::IDictTypeBuilder& Key(NUdf::TDataTypeId typeId) override {
        auto slot = NUdf::FindDataSlot(typeId);
        Y_ABORT_UNLESS(slot, "unknown type: %d", (int)typeId);
        Y_ABORT_UNLESS(NUdf::GetDataTypeInfo(*slot).Features & NUdf::CanCompare, "key type is not comparable");
        KeyType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IDictTypeBuilder& Key(const NUdf::TType* type) override {
        KeyType_ = static_cast<const NMiniKQL::TType*>(type);
        CheckKeyType();
        return *this;
    }

    NUdf::IDictTypeBuilder& Key(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        KeyType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        CheckKeyType();
        return *this;
    }

    NUdf::IDictTypeBuilder& Value(NUdf::TDataTypeId typeId) override {
        ValueType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IDictTypeBuilder& Value(const NUdf::TType* type) override {
        ValueType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IDictTypeBuilder& Value(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        ValueType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TDictType::Create(
                    const_cast<NMiniKQL::TType*>(KeyType_),
                    const_cast<NMiniKQL::TType*>(ValueType_), Parent_.Env());
    }

private:
    void CheckKeyType() const {
        auto t = AS_TYPE(NMiniKQL::TDataType, const_cast<NMiniKQL::TType*>(KeyType_));
        auto keySchemeType = t->GetSchemeType();
        auto slot = NUdf::FindDataSlot(keySchemeType);
        Y_ABORT_UNLESS(slot, "unknown type: %d", (int)keySchemeType);
        Y_ABORT_UNLESS(NUdf::GetDataTypeInfo(*slot).Features & NUdf::CanCompare, "key type is not comparable");
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* KeyType_ = nullptr;
    const NMiniKQL::TType* ValueType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TSetTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TSetTypeBuilder : public NUdf::ISetTypeBuilder
{
public:
    TSetTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::ISetTypeBuilder& Key(NUdf::TDataTypeId typeId) override {
        auto slot = NUdf::FindDataSlot(typeId);
        Y_ABORT_UNLESS(slot, "unknown type: %d", (int)typeId);
        Y_ABORT_UNLESS(NUdf::GetDataTypeInfo(*slot).Features & NUdf::CanCompare, "key type is not comparable");
        KeyType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::ISetTypeBuilder& Key(const NUdf::TType* type) override {
        KeyType_ = static_cast<const NMiniKQL::TType*>(type);
        CheckKeyType();
        return *this;
    }

    NUdf::ISetTypeBuilder& Key(
        const NUdf::ITypeBuilder& typeBuilder) override
    {
        KeyType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        CheckKeyType();
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TDictType::Create(
            const_cast<NMiniKQL::TType*>(KeyType_),
            Parent_.Env().GetVoidLazy()->GetType(), Parent_.Env());
    }

private:
    void CheckKeyType() const {
        auto t = AS_TYPE(NMiniKQL::TDataType, const_cast<NMiniKQL::TType*>(KeyType_));
        auto keySchemeType = t->GetSchemeType();
        auto slot = NUdf::FindDataSlot(keySchemeType);
        Y_ABORT_UNLESS(slot, "unknown type: %d", (int)keySchemeType);
        Y_ABORT_UNLESS(NUdf::GetDataTypeInfo(*slot).Features & NUdf::CanCompare, "key type is not comparable");
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* KeyType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TStructTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TStructTypeBuilder: public NUdf::IStructTypeBuilder
{
public:
    TStructTypeBuilder(
            const NMiniKQL::TFunctionTypeInfoBuilder& parent,
            ui32 itemsCount)
        : Parent_(parent)
        , StructBuilder_(Parent_.Env())
    {
        StructBuilder_.Reserve(itemsCount);
    }

    NUdf::IStructTypeBuilder& AddField(
            const NUdf::TStringRef& name,
            NUdf::TDataTypeId typeId,
            ui32* index) override
    {
        auto type = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        StructBuilder_.Add(name, type, index);
        return *this;
    }

    NUdf::IStructTypeBuilder& AddField(
            const NUdf::TStringRef& name,
            const NUdf::TType* type,
            ui32* index) override
    {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        StructBuilder_.Add(name, const_cast<NMiniKQL::TType*>(mkqlType), index);
        return *this;
    }

    NUdf::IStructTypeBuilder& AddField(
            const NUdf::TStringRef& name,
            const NUdf::ITypeBuilder& typeBuilder,
            ui32* index) override
    {
        auto type = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        StructBuilder_.Add(name, type, index);
        return *this;
    }

    NUdf::TType* Build() const override {
        auto structType = StructBuilder_.Build();
        StructBuilder_.FillIndexes();
        return structType;
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    mutable NMiniKQL::TStructTypeBuilder StructBuilder_;
};

//////////////////////////////////////////////////////////////////////////////
// TEnumTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TEnumTypeBuilder : public NUdf::IEnumTypeBuilder
{
public:
    TEnumTypeBuilder(
        const NMiniKQL::TFunctionTypeInfoBuilder& parent,
        ui32 itemsCount)
        : Parent_(parent)
        , StructBuilder_(Parent_.Env())
    {
        StructBuilder_.Reserve(itemsCount);
    }

    NUdf::IEnumTypeBuilder& AddField(
        const NUdf::TStringRef& name,
        ui32* index) override
    {
        StructBuilder_.Add(name, Parent_.Env().GetVoidLazy()->GetType(), index);
        return *this;
    }

    NUdf::TType* Build() const override {
        auto structType = StructBuilder_.Build();
        StructBuilder_.FillIndexes();
        return NMiniKQL::TVariantType::Create(structType, Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    mutable NMiniKQL::TStructTypeBuilder StructBuilder_;
};

//////////////////////////////////////////////////////////////////////////////
// TTupleTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TTupleTypeBuilder: public NUdf::ITupleTypeBuilder
{
public:
    TTupleTypeBuilder(
            const NMiniKQL::TFunctionTypeInfoBuilder& parent,
            ui32 itemsCount)
        : Parent_(parent)
    {
        ElementTypes_.reserve(itemsCount);
    }

    NUdf::ITupleTypeBuilder& Add(NUdf::TDataTypeId typeId) override {
        auto type = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        ElementTypes_.push_back(type);
        return *this;
    }

    NUdf::ITupleTypeBuilder& Add(const NUdf::TType* type) override {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        ElementTypes_.push_back(const_cast<NMiniKQL::TType*>(mkqlType));
        return *this;
    }

    NUdf::ITupleTypeBuilder& Add(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        auto type = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        ElementTypes_.push_back(type);
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TTupleType::Create(
                ElementTypes_.size(), ElementTypes_.data(),
                Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    TVector<NMiniKQL::TType*> ElementTypes_;
};

/////////////////////////////////////////////////////////////////////////////
// TVariantTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TVariantTypeBuilder : public NUdf::IVariantTypeBuilder
{
public:
    TVariantTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent)
        : Parent_(parent)
    {
    }

    NUdf::IVariantTypeBuilder& Over(const NUdf::TType* type) override {
        UnderlyingType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IVariantTypeBuilder& Over(
        const NUdf::ITypeBuilder& typeBuilder) override
    {
        UnderlyingType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TVariantType::Create(
            const_cast<NMiniKQL::TType*>(UnderlyingType_),
            Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* UnderlyingType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TCallableTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TCallableTypeBuilder: public NUdf::ICallableTypeBuilder
{
public:
    TCallableTypeBuilder(
            const NMiniKQL::TTypeEnvironment& env, ui32 argsCount)
        : Env_(env)
        , ReturnType_(nullptr)
        , OptionalArgs_(0)
    {
        Args_.reserve(argsCount);
    }

    NUdf::ICallableTypeBuilder& Returns(
            NUdf::TDataTypeId typeId) override
    {
        ReturnType_ = NMiniKQL::TDataType::Create(typeId, Env_);
        return *this;
    }

    NUdf::ICallableTypeBuilder& Returns(
            const NUdf::TType* type) override
    {
        ReturnType_ = const_cast<NMiniKQL::TType*>(
                    static_cast<const NMiniKQL::TType*>(type));
        return *this;
    }

    NUdf::ICallableTypeBuilder& Returns(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        ReturnType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::ICallableTypeBuilder& Arg(NUdf::TDataTypeId typeId) override {
        auto type = NMiniKQL::TDataType::Create(typeId, Env_);
        Args_.emplace_back().Type_ = type;
        return *this;
    }

    NUdf::ICallableTypeBuilder& Arg(const NUdf::TType* type) override {
        auto mkqlType = const_cast<NMiniKQL::TType*>(static_cast<const NMiniKQL::TType*>(type));
        Args_.emplace_back().Type_ = mkqlType;
        return *this;
    }

    NUdf::ICallableTypeBuilder& Arg(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        auto type = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        Args_.emplace_back().Type_ = type;
        return *this;
    }

    NUdf::ICallableTypeBuilder& Name(const NUdf::TStringRef& name) override {
        Args_.back().Name_ = Env_.InternName(name);
        return *this;
    }

    NUdf::ICallableTypeBuilder& Flags(ui64 flags) override {
        Args_.back().Flags_ = flags;
        return *this;
    }

    ICallableTypeBuilder& OptionalArgs(ui32 optionalArgs) override {
        OptionalArgs_ = optionalArgs;
        return *this;
    }

    NUdf::TType* Build() const override {
        Y_ABORT_UNLESS(ReturnType_, "callable returns type is not configured");

        NMiniKQL::TCallableTypeBuilder builder(Env_, UdfName, ReturnType_);
        for (const auto& arg : Args_) {
            builder.Add(arg.Type_);
            if (!arg.Name_.Str().empty()) {
                builder.SetArgumentName(arg.Name_.Str());
            }

            if (arg.Flags_ != 0) {
                builder.SetArgumentFlags(arg.Flags_);
            }
        }
        builder.SetOptionalArgs(OptionalArgs_);

        return builder.Build();
    }

private:
    const NMiniKQL::TTypeEnvironment& Env_;
    NMiniKQL::TType* ReturnType_;
    TVector<NMiniKQL::TArgInfo> Args_;
    ui32 OptionalArgs_;
};

//////////////////////////////////////////////////////////////////////////////
// TFunctionArgTypesBuilder
//////////////////////////////////////////////////////////////////////////////
class TFunctionArgTypesBuilder: public NUdf::IFunctionArgTypesBuilder
{
public:
    explicit TFunctionArgTypesBuilder(
            NMiniKQL::TFunctionTypeInfoBuilder& parent,
            TVector<NMiniKQL::TArgInfo>& args)
        : NUdf::IFunctionArgTypesBuilder(parent)
        , Env_(parent.Env())
        , Args_(args)
    {
    }

    NUdf::IFunctionArgTypesBuilder& Add(NUdf::TDataTypeId typeId) override {
        auto type = NMiniKQL::TDataType::Create(typeId, Env_);
        Args_.emplace_back();
        Args_.back().Type_ = type;
        return *this;
    }

    NUdf::IFunctionArgTypesBuilder& Add(const NUdf::TType* type) override {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        Args_.emplace_back();
        Args_.back().Type_ = const_cast<NMiniKQL::TType*>(mkqlType);
        return *this;
    }

    NUdf::IFunctionArgTypesBuilder& Add(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        auto type = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        Args_.emplace_back();
        Args_.back().Type_ = type;
        return *this;
    }

    NUdf::IFunctionArgTypesBuilder& Name(const NUdf::TStringRef& name) override {
        Args_.back().Name_ = Env_.InternName(name);
        return *this;
    }

    NUdf::IFunctionArgTypesBuilder& Flags(ui64 flags) override {
        Args_.back().Flags_ = flags;
        return *this;
    }

private:
    const NMiniKQL::TTypeEnvironment& Env_;
    TVector<NMiniKQL::TArgInfo>& Args_;
};

//////////////////////////////////////////////////////////////////////////////
// THash
//////////////////////////////////////////////////////////////////////////////

struct TTypeNotSupported : public yexception
{};

class TEmptyHash final : public NUdf::IHash {
public:
    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        Y_UNUSED(value);
        return 0;
    }
};

template <NMiniKQL::TType::EKind Kind, NUdf::EDataSlot Slot = NUdf::EDataSlot::Bool>
class THash;

template <NUdf::EDataSlot Slot>
class THash<NMiniKQL::TType::EKind::Data, Slot> final : public NUdf::IHash {
public:
    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        return NUdf::GetValueHash<Slot>(std::move(value));
    }
};

template <>
class THash<NMiniKQL::TType::EKind::Optional> final : public NUdf::IHash {
public:
    explicit THash(const NMiniKQL::TType* type)
        : Hash_(MakeHashImpl(static_cast<const NMiniKQL::TOptionalType*>(type)->GetItemType()))
    {}

    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        // keep hash computation in sync with
        // ydb/library/yql/public/udf/arrow/block_item_hasher.h: TBlockItemHasherBase::Hash()
        if (!value) {
            return 0;
        }
        return CombineHashes(ui64(1), Hash_->Hash(value.GetOptionalValue()));
    }

private:
    const NUdf::IHash::TPtr Hash_;
};

template <>
class THash<NMiniKQL::TType::EKind::List> final : public NUdf::IHash {
public:
    explicit THash(const NMiniKQL::TType* type)
        : Hash_(MakeHashImpl(static_cast<const NMiniKQL::TListType*>(type)->GetItemType()))
    {}

    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        ui64 result = 0ULL;
        NKikimr::NMiniKQL::TThresher<false>::DoForEachItem(value,
            [&result, this] (NUdf::TUnboxedValue&& item) {
                result = CombineHashes(result, Hash_->Hash(static_cast<const NUdf::TUnboxedValuePod&>(item)));
            }
        );
        return result;
    }

private:
    const NUdf::IHash::TPtr Hash_;
};

template <>
class THash<NMiniKQL::TType::EKind::Dict> final : public NUdf::IHash {
public:
    explicit THash(const NMiniKQL::TType* type)
    {
        auto dictType = static_cast<const NMiniKQL::TDictType*>(type);
        KeyHash_ = MakeHashImpl(dictType->GetKeyType());
        PayloadHash_ = MakeHashImpl(dictType->GetPayloadType());
    }

    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        auto iter = value.GetDictIterator();
        if (value.IsSortedDict()) {
            ui64 result = 0ULL;
            NUdf::TUnboxedValue key, payload;
            while (iter.NextPair(key, payload)) {
                result = CombineHashes(result, KeyHash_->Hash(static_cast<const NUdf::TUnboxedValuePod&>(key)));
                result = CombineHashes(result, PayloadHash_->Hash(static_cast<const NUdf::TUnboxedValuePod&>(payload)));
            }

            return result;
        } else {
            TVector<ui64, NKikimr::NMiniKQL::TMKQLAllocator<ui64>> hashes;
            hashes.reserve(value.GetDictLength());

            NUdf::TUnboxedValue key, payload;
            while (iter.NextPair(key, payload)) {
                auto keyHash = KeyHash_->Hash(static_cast<const NUdf::TUnboxedValuePod&>(key));
                auto payloadHash = PayloadHash_->Hash(static_cast<const NUdf::TUnboxedValuePod&>(payload));
                hashes.emplace_back(CombineHashes(keyHash, payloadHash));
            }

            Sort(hashes.begin(), hashes.end());

            ui64 result = 0ULL;
            for (const auto& x : hashes) {
                result = CombineHashes(result, x);
            }

            return result;
        }
    }

private:
    NUdf::IHash::TPtr KeyHash_;
    NUdf::IHash::TPtr PayloadHash_;
};

class TVectorHash : public NUdf::IHash {
public:
    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        // keep hash computation in sync with
        // ydb/library/yql/public/udf/arrow/block_item_hasher.h: TTupleBlockItemHasher::DoHash()
        ui64 result = 0ULL;
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < Hash_.size(); ++i) {
                result = CombineHashes(result, Hash_[i]->Hash(static_cast<const NUdf::TUnboxedValuePod&>(elements[i])));
            }
        } else {
            for (ui32 i = 0; i < Hash_.size(); ++i) {
                auto item = value.GetElement(i);
                result = CombineHashes(result, Hash_[i]->Hash(static_cast<const NUdf::TUnboxedValuePod&>(item)));
            }
        }

        return result;
    }

protected:
    std::vector<NUdf::IHash::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::IHash::TPtr>> Hash_;
};

template <>
class THash<NMiniKQL::TType::EKind::Tuple> final : public TVectorHash {
public:
    explicit THash(const NMiniKQL::TType* type) {
        auto tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
        auto count = tupleType->GetElementsCount();
        Hash_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Hash_.push_back(MakeHashImpl(tupleType->GetElementType(i)));
        }
    }
};

template <>
class THash<NMiniKQL::TType::EKind::Struct> final : public TVectorHash {
public:
    explicit THash(const NMiniKQL::TType* type) {
        auto structType = static_cast<const NMiniKQL::TStructType*>(type);
        auto count = structType->GetMembersCount();
        Hash_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Hash_.push_back(MakeHashImpl(structType->GetMemberType(i)));
        }
    }
};

template <>
class THash<NMiniKQL::TType::EKind::Variant> final : public NUdf::IHash {
public:
    explicit THash(const NMiniKQL::TType* type) {
        auto variantType = static_cast<const NMiniKQL::TVariantType*>(type);
        if (variantType->GetUnderlyingType()->IsStruct()) {
            auto structType = static_cast<const NMiniKQL::TStructType*>(variantType->GetUnderlyingType());
            ui32 count = structType->GetMembersCount();
            Hash_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Hash_.push_back(MakeHashImpl(structType->GetMemberType(i)));
            }
        } else {
            auto tupleType = static_cast<const NMiniKQL::TTupleType*>(variantType->GetUnderlyingType());
            ui32 count = tupleType->GetElementsCount();
            Hash_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Hash_.push_back(MakeHashImpl(tupleType->GetElementType(i)));
            }
        }
    }

    ui64 Hash(NUdf::TUnboxedValuePod value) const override {
        auto index = value.GetVariantIndex();
        MKQL_ENSURE(index < Hash_.size(), "Incorrect index");
        auto item = value.GetVariantItem();
        return CombineHashes(ui64(index), Hash_[index]->Hash(static_cast<const NUdf::TUnboxedValuePod&>(item)));
    }

private:
    std::vector<NUdf::IHash::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::IHash::TPtr>> Hash_;
};

//////////////////////////////////////////////////////////////////////////////
// TEquate
//////////////////////////////////////////////////////////////////////////////
class TEmptyEquate final : public NUdf::IEquate {
public:
    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        Y_UNUSED(lhs);
        Y_UNUSED(rhs);
        return true;
    }
};


template <NMiniKQL::TType::EKind Kind, NUdf::EDataSlot Slot = NUdf::EDataSlot::Bool>
class TEquate;

template <NUdf::EDataSlot Slot>
class TEquate<NMiniKQL::TType::EKind::Data, Slot> final : public NUdf::IEquate {
public:
    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return NUdf::EquateValues<Slot>(std::move(lhs), std::move(rhs));
    }
};

template <>
class TEquate<NMiniKQL::TType::EKind::Optional> final : public NUdf::IEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type)
        : Equate_(MakeEquateImpl(static_cast<const NMiniKQL::TOptionalType*>(type)->GetItemType()))
    {}

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if (!lhs) {
            if (!rhs) {
                return true;
            }
            return false;
        } else {
            if (!rhs) {
                return false;
            }
            return Equate_->Equals(lhs.GetOptionalValue(), rhs.GetOptionalValue());
        }
    }

private:
    const NUdf::IEquate::TPtr Equate_;
};

template <>
class TEquate<NMiniKQL::TType::EKind::List> final : public NUdf::IEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type)
        : Equate_(MakeEquateImpl(static_cast<const NMiniKQL::TListType*>(type)->GetItemType()))
    {}

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        auto elementsL = lhs.GetElements();
        auto elementsR = rhs.GetElements();
        if (elementsL && elementsR) {
            const auto size = lhs.GetListLength();
            if (size != rhs.GetListLength()) {
                return false;
            }

            for (ui64 i = 0ULL; i < size; ++i) {
                if (!Equate_->Equals(*elementsL++, *elementsR++)) {
                    return false;
                }
            }
        } else if (elementsL) {
            const auto iter = rhs.GetListIterator();
            auto size = lhs.GetListLength();
            for (NUdf::TUnboxedValue item; iter.Next(item); --size) {
                if (!size || !Equate_->Equals(*elementsL++, static_cast<const NUdf::TUnboxedValuePod&>(item))) {
                    return false;
                }
            }

            if (size) {
                return false;
            }
        } else if (elementsR) {
            const auto iter = lhs.GetListIterator();
            auto size = rhs.GetListLength();
            for (NUdf::TUnboxedValue item; iter.Next(item); --size) {
                if (!size || !Equate_->Equals(static_cast<const NUdf::TUnboxedValuePod&>(item), *elementsR++)) {
                    return false;
                }
            }

            if (size) {
                return false;
            }
        } else {
            const auto lIter = lhs.GetListIterator();
            const auto rIter = rhs.GetListIterator();
            for (NUdf::TUnboxedValue left, right;;) {
                if (const bool lOk  = lIter.Next(left), rOk  = rIter.Next(right); lOk && rOk) {
                    if (!Equate_->Equals(
                        static_cast<const NUdf::TUnboxedValuePod&>(left),
                        static_cast<const NUdf::TUnboxedValuePod&>(right))) {
                        return false;
                    }
                } else {
                    return !(lOk || rOk);
                }
            }
        }
        return true;
    }
private:
    const NUdf::IEquate::TPtr Equate_;
};

template <>
class TEquate<NMiniKQL::TType::EKind::Dict> final : public NUdf::IEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type)
    {
        auto dictType = static_cast<const NMiniKQL::TDictType*>(type);
        PayloadEquate_ = MakeEquateImpl(dictType->GetPayloadType());
    }

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if (lhs.GetDictLength() != rhs.GetDictLength()) {
            return false;
        }

        auto lhsIter = lhs.GetDictIterator();
        NUdf::TUnboxedValue lhsKey, lhsPayload;
        while (lhsIter.NextPair(lhsKey, lhsPayload)) {
            auto lookup = rhs.Lookup(lhsKey);
            if (!lookup) {
                return false;
            }

            NUdf::TUnboxedValue unpacked = lookup.GetOptionalValue();
            if (!PayloadEquate_->Equals(lhsPayload, unpacked)) {
                return false;
            }
        }

        return true;
    }

private:
    NUdf::IEquate::TPtr PayloadEquate_;
};


class TVectorEquate : public NUdf::IEquate {
public:
    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        for (size_t i = 0; i < Equate_.size(); ++i) {
            if (!Equate_[i]->Equals(
                static_cast<const NUdf::TUnboxedValuePod&>(lhs.GetElement(i)),
                static_cast<const NUdf::TUnboxedValuePod&>(rhs.GetElement(i)))) {
                return false;
            }
        }
        return true;
    }

protected:
    std::vector<NUdf::IEquate::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::IEquate::TPtr>> Equate_;
};

template <>
class TEquate<NMiniKQL::TType::EKind::Tuple> final : public TVectorEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type) {
        auto tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
        auto count = tupleType->GetElementsCount();
        Equate_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Equate_.push_back(MakeEquateImpl(tupleType->GetElementType(i)));
        }
    }
};

template <>
class TEquate<NMiniKQL::TType::EKind::Struct> final : public TVectorEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type) {
        auto structType = static_cast<const NMiniKQL::TStructType*>(type);
        auto count = structType->GetMembersCount();
        Equate_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Equate_.push_back(MakeEquateImpl(structType->GetMemberType(i)));
        }
    }
};

template <>
class TEquate<NMiniKQL::TType::EKind::Variant> final : public NUdf::IEquate {
public:
    explicit TEquate(const NMiniKQL::TType* type) {
        auto variantType = static_cast<const NMiniKQL::TVariantType*>(type);
        if (variantType->GetUnderlyingType()->IsStruct()) {
            auto structType = static_cast<const NMiniKQL::TStructType*>(variantType->GetUnderlyingType());
            ui32 count = structType->GetMembersCount();
            Equate_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Equate_.push_back(MakeEquateImpl(structType->GetMemberType(i)));
            }
        }
        else {
            auto tupleType = static_cast<const NMiniKQL::TTupleType*>(variantType->GetUnderlyingType());
            ui32 count = tupleType->GetElementsCount();
            Equate_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Equate_.push_back(MakeEquateImpl(tupleType->GetElementType(i)));
            }
        }
    }

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        auto lhsIndex = lhs.GetVariantIndex();
        auto rhsIndex = rhs.GetVariantIndex();
        if (lhsIndex != rhsIndex) {
            return false;
        }

        MKQL_ENSURE(lhsIndex < Equate_.size(), "Incorrect index");
        auto lhsItem = lhs.GetVariantItem();
        auto rhsItem = rhs.GetVariantItem();
        return Equate_[lhsIndex]->Equals(static_cast<const NUdf::TUnboxedValuePod&>(lhsItem), static_cast<const NUdf::TUnboxedValuePod&>(rhsItem));
    }

private:
    std::vector<NUdf::IEquate::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::IEquate::TPtr>> Equate_;
};

//////////////////////////////////////////////////////////////////////////////
// TCompare
//////////////////////////////////////////////////////////////////////////////
class TEmptyCompare final : public NUdf::ICompare {
public:
    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        Y_UNUSED(lhs);
        Y_UNUSED(rhs);
        return false;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        Y_UNUSED(lhs);
        Y_UNUSED(rhs);
        return 0;
    }
};

template <NMiniKQL::TType::EKind Kind, NUdf::EDataSlot Slot = NUdf::EDataSlot::Bool>
class TCompare;

template <NUdf::EDataSlot Slot>
class TCompare<NMiniKQL::TType::EKind::Data, Slot> final : public NUdf::ICompare {
public:
    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return NUdf::CompareValues<Slot>(std::move(lhs), std::move(rhs)) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return NUdf::CompareValues<Slot>(std::move(lhs), std::move(rhs));
    }
};

template <>
class TCompare<NMiniKQL::TType::EKind::Optional> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type)
        : Compare_(MakeCompareImpl(static_cast<const NMiniKQL::TOptionalType*>(type)->GetItemType()))
    {}

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if (!lhs) {
            if (!rhs) {
                return false;
            }
            return true;
        } else {
            if (!rhs) {
                return false;
            }
            return Compare_->Less(lhs.GetOptionalValue(), rhs.GetOptionalValue());
        }
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if (!lhs) {
            if (!rhs) {
                return 0;
            }
            return -1;
        } else {
            if (!rhs) {
                return 1;
            }
            return Compare_->Compare(lhs.GetOptionalValue(), rhs.GetOptionalValue());
        }
    }

private:
    const NUdf::ICompare::TPtr Compare_;
};

template <>
class TCompare<NMiniKQL::TType::EKind::Tuple> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type) {
        auto tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
        auto count = tupleType->GetElementsCount();
        Compare_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Compare_.push_back(MakeCompareImpl(tupleType->GetElementType(i)));
        }
    }

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return Compare(lhs, rhs) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        for (size_t i = 0; i < Compare_.size(); ++i) {
            auto cmp = Compare_[i]->Compare(
                static_cast<const NUdf::TUnboxedValuePod&>(lhs.GetElement(i)),
                static_cast<const NUdf::TUnboxedValuePod&>(rhs.GetElement(i)));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

private:
    std::vector<NUdf::ICompare::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::ICompare::TPtr>> Compare_;
};

template <>
class TCompare<NMiniKQL::TType::EKind::Struct> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type) {
        auto structType = static_cast<const NMiniKQL::TStructType*>(type);
        auto count = structType->GetMembersCount();
        Compare_.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            Compare_.push_back(MakeCompareImpl(structType->GetMemberType(i)));
        }
    }

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return Compare(lhs, rhs) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        for (size_t i = 0; i < Compare_.size(); ++i) {
            auto cmp = Compare_[i]->Compare(
                static_cast<const NUdf::TUnboxedValuePod&>(lhs.GetElement(i)),
                static_cast<const NUdf::TUnboxedValuePod&>(rhs.GetElement(i)));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

private:
    std::vector<NUdf::ICompare::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::ICompare::TPtr>> Compare_;
};

template <>
class TCompare<NMiniKQL::TType::EKind::Variant> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type) {
        auto variantType = static_cast<const NMiniKQL::TVariantType*>(type);
        if (variantType->GetUnderlyingType()->IsStruct()) {
            auto structType = static_cast<const NMiniKQL::TStructType*>(variantType->GetUnderlyingType());
            ui32 count = structType->GetMembersCount();
            Compare_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Compare_.push_back(MakeCompareImpl(structType->GetMemberType(i)));
            }
        } else {
            auto tupleType = static_cast<const NMiniKQL::TTupleType*>(variantType->GetUnderlyingType());
            ui32 count = tupleType->GetElementsCount();
            Compare_.reserve(count);
            for (ui32 i = 0; i < count; ++i) {
                Compare_.push_back(MakeCompareImpl(tupleType->GetElementType(i)));
            }
        }
    }

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return Compare(lhs, rhs) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        auto lhsIndex = lhs.GetVariantIndex();
        auto rhsIndex = rhs.GetVariantIndex();
        if (lhsIndex < rhsIndex) {
            return -1;
        }

        if (lhsIndex > rhsIndex) {
            return 1;
        }

        MKQL_ENSURE(lhsIndex < Compare_.size(), "Incorrect index");
        auto lhsItem = lhs.GetVariantItem();
        auto rhsItem = rhs.GetVariantItem();
        return Compare_[lhsIndex]->Compare(static_cast<const NUdf::TUnboxedValuePod&>(lhsItem), static_cast<const NUdf::TUnboxedValuePod&>(rhsItem));
    }

private:
    std::vector<NUdf::ICompare::TPtr, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::ICompare::TPtr>> Compare_;
};

template <>
class TCompare<NMiniKQL::TType::EKind::List> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type)
        : Compare_(MakeCompareImpl(static_cast<const NMiniKQL::TListType*>(type)->GetItemType()))
    {}

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return Compare(lhs, rhs) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        auto lhsElems = lhs.GetElements();
        auto rhsElems = rhs.GetElements();
        if (lhsElems && rhsElems) {
            ui32 lhsCount = lhs.GetListLength();
            ui32 rhsCount = rhs.GetListLength();
            for (ui32 index = 0;;++index) {
                if (index >= lhsCount || index >= rhsCount) {
                    if (lhsCount == rhsCount) {
                        return 0;
                    }

                    return lhsCount > rhsCount ? 1 : -1;
                }

                auto cmp = Compare_->Compare(
                    static_cast<const NUdf::TUnboxedValuePod&>(lhsElems[index]),
                    static_cast<const NUdf::TUnboxedValuePod&>(rhsElems[index]));
                if (cmp) {
                    return cmp;
                }
            }
        }

        auto lhsIter = lhs.GetListIterator();
        auto rhsIter = rhs.GetListIterator();
        for (;;) {
            NUdf::TUnboxedValue lhsItem;
            NUdf::TUnboxedValue rhsItem;
            bool hasLeft = lhsIter.Next(lhsItem);
            bool hasRight = rhsIter.Next(rhsItem);
            if (!hasLeft || !hasRight) {
                if (hasLeft == hasRight) {
                    return 0;
                }

                return hasLeft ? 1 : -1;
            }

            auto cmp = Compare_->Compare(static_cast<const NUdf::TUnboxedValuePod&>(lhsItem), static_cast<const NUdf::TUnboxedValuePod&>(rhsItem));
            if (cmp) {
                return cmp;
            }
        }
    }

private:
    const NUdf::ICompare::TPtr Compare_;
};

template <>
class TCompare<NMiniKQL::TType::EKind::Dict> final : public NUdf::ICompare {
public:
    explicit TCompare(const NMiniKQL::TType* type)
        : CompareKey_(MakeCompareImpl(static_cast<const NMiniKQL::TDictType*>(type)->GetKeyType()))
        , ComparePayload_(MakeCompareImpl(static_cast<const NMiniKQL::TDictType*>(type)->GetPayloadType()))
    {}

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        return Compare(lhs, rhs) < 0;
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        auto lhsIter = lhs.GetDictIterator();
        auto rhsIter = rhs.GetDictIterator();

        using TKP = std::pair<NUdf::TUnboxedValue, NUdf::TUnboxedValue>;
        TVector<TKP, NMiniKQL::TMKQLAllocator<TKP>> lhsData, rhsData;

        lhsData.reserve(lhs.GetDictLength());
        rhsData.reserve(rhs.GetDictLength());

        NUdf::TUnboxedValue key, payload;
        while (lhsIter.NextPair(key, payload)) {
            lhsData.emplace_back(std::make_pair(key, payload));
        }

        while (rhsIter.NextPair(key, payload)) {
            rhsData.emplace_back(std::make_pair(key, payload));
        }

        if (!lhs.IsSortedDict()) {
            Sort(lhsData.begin(), lhsData.end(), [&](const auto& x, const auto& y) {
                return CompareKey_->Less(x.first, y.first);
            });
        }

        if (!rhs.IsSortedDict()) {
            Sort(rhsData.begin(), rhsData.end(), [&](const auto& x, const auto& y) {
                return CompareKey_->Less(x.first, y.first);
            });
        }

        auto lhsCurr = lhsData.begin();
        auto rhsCurr = rhsData.begin();
        for (;;) {
            bool hasLeft = lhsCurr != lhsData.end();
            bool hasRight = rhsCurr != rhsData.end();
            if (!hasLeft || !hasRight) {
                if (hasLeft == hasRight) {
                    return 0;
                }

                return hasLeft ? 1 : -1;
            }

            auto cmpKeys = CompareKey_->Compare(
                static_cast<const NUdf::TUnboxedValuePod&>(lhsCurr->first),
                static_cast<const NUdf::TUnboxedValuePod&>(rhsCurr->first)
            );

            if (cmpKeys) {
                return cmpKeys;
            }

            auto cmpPayloads = ComparePayload_->Compare(
                static_cast<const NUdf::TUnboxedValuePod&>(lhsCurr->second),
                static_cast<const NUdf::TUnboxedValuePod&>(rhsCurr->second)
            );

            if (cmpPayloads) {
                return cmpPayloads;
            }

            ++lhsCurr;
            ++rhsCurr;
        }
    }

private:
    const NUdf::ICompare::TPtr CompareKey_;
    const NUdf::ICompare::TPtr ComparePayload_;
};

//////////////////////////////////////////////////////////////////////////////
// TBlockTypeBuilder
//////////////////////////////////////////////////////////////////////////////
class TBlockTypeBuilder: public NUdf::IBlockTypeBuilder
{
public:
    TBlockTypeBuilder(const NMiniKQL::TFunctionTypeInfoBuilder& parent, bool isScalar)
        : NUdf::IBlockTypeBuilder(isScalar)
        , Parent_(parent)
    {
    }

    NUdf::IBlockTypeBuilder& Item(NUdf::TDataTypeId typeId) override {
        ItemType_ = NMiniKQL::TDataType::Create(typeId, Parent_.Env());
        return *this;
    }

    NUdf::IBlockTypeBuilder& Item(const NUdf::TType* type) override {
        ItemType_ = static_cast<const NMiniKQL::TType*>(type);
        return *this;
    }

    NUdf::IBlockTypeBuilder& Item(
            const NUdf::ITypeBuilder& typeBuilder) override
    {
        ItemType_ = static_cast<NMiniKQL::TType*>(typeBuilder.Build());
        return *this;
    }

    NUdf::TType* Build() const override {
        return NMiniKQL::TBlockType::Create(
                    const_cast<NMiniKQL::TType*>(ItemType_),
                    (IsScalar_ ? NMiniKQL::TBlockType::EShape::Scalar : NMiniKQL::TBlockType::EShape::Many),
                    Parent_.Env());
    }

private:
    const NMiniKQL::TFunctionTypeInfoBuilder& Parent_;
    const NMiniKQL::TType* ItemType_ = nullptr;
};

} // namespace

namespace NMiniKQL {

bool ConvertArrowType(NUdf::EDataSlot slot, std::shared_ptr<arrow::DataType>& type) {
    switch (slot) {
    case NUdf::EDataSlot::Bool:
    case NUdf::EDataSlot::Uint8:
        type = arrow::uint8();
        return true;
    case NUdf::EDataSlot::Int8:
        type = arrow::int8();
        return true;
    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Date:
        type = arrow::uint16();
        return true;
    case NUdf::EDataSlot::Int16:
        type = arrow::int16();
        return true;
    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Datetime:
        type = arrow::uint32();
        return true;
    case NUdf::EDataSlot::Int32:
    case NUdf::EDataSlot::Date32:
        type = arrow::int32();
        return true;
    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Interval:
    case NUdf::EDataSlot::Interval64:
    case NUdf::EDataSlot::Timestamp64:
    case NUdf::EDataSlot::Datetime64:
        type = arrow::int64();
        return true;
    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Timestamp:
        type = arrow::uint64();
        return true;
    case NUdf::EDataSlot::Float:
        type = arrow::float32();
        return true;
    case NUdf::EDataSlot::Double:
        type = arrow::float64();
        return true;
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Yson:
    case NUdf::EDataSlot::JsonDocument:
        type = arrow::binary();
        return true;
    case NUdf::EDataSlot::Utf8:
    case NUdf::EDataSlot::Json:
        type = arrow::utf8();
        return true;
    case NUdf::EDataSlot::TzDate: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzDate>();
        return true;
    }
    case NUdf::EDataSlot::TzDatetime: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzDatetime>();
        return true;
    }
    case NUdf::EDataSlot::TzTimestamp: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzTimestamp>();
        return true;
    }
    case NUdf::EDataSlot::TzDate32: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzDate32>();
        return true;
    }
    case NUdf::EDataSlot::TzDatetime64: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzDatetime64>();
        return true;
    }
    case NUdf::EDataSlot::TzTimestamp64: {
        type = MakeTzDateArrowType<NYql::NUdf::EDataSlot::TzTimestamp64>();
        return true;
    }
    case NUdf::EDataSlot::Uuid: {
        return false;
    }
    case NUdf::EDataSlot::Decimal: {
        type = arrow::fixed_size_binary(sizeof(NYql::NUdf::TUnboxedValuePod));
        return true;
    }
    case NUdf::EDataSlot::DyNumber: {
        return false;
    }
    }
}

bool ConvertArrowType(TType* itemType, std::shared_ptr<arrow::DataType>& type, const TArrowConvertFailedCallback& onFail) {
    bool isOptional;
    auto unpacked = UnpackOptional(itemType, isOptional);
    if (unpacked->IsOptional() || isOptional && unpacked->IsPg()) {
        // at least 2 levels of optionals
        ui32 nestLevel = 0;
        auto currentType = itemType;
        auto previousType = itemType;
        do {
            ++nestLevel;
            previousType = currentType;
            currentType = AS_TYPE(TOptionalType, currentType)->GetItemType();
        } while (currentType->IsOptional());

        if (currentType->IsPg()) {
            previousType = currentType;
            ++nestLevel;
        }

        // previousType is always Optional
        std::shared_ptr<arrow::DataType> innerArrowType;
        if (!ConvertArrowType(previousType, innerArrowType, onFail)) {
            return false;
        }

        for (ui32 i = 1; i < nestLevel; ++i) {
            // wrap as one nullable field in struct
            std::vector<std::shared_ptr<arrow::Field>> fields;
            fields.emplace_back(std::make_shared<arrow::Field>("opt", innerArrowType, true));
            innerArrowType = std::make_shared<arrow::StructType>(fields);
        }

        type = innerArrowType;
        return true;
    }

    if (unpacked->IsStruct()) {
        auto structType = AS_TYPE(TStructType, unpacked);
        std::vector<std::shared_ptr<arrow::Field>> members;
        for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
            std::shared_ptr<arrow::DataType> childType;
            const TString memberName(structType->GetMemberName(i));
            auto memberType = structType->GetMemberType(i);
            if (!ConvertArrowType(memberType, childType, onFail)) {
                return false;
            }
            members.emplace_back(std::make_shared<arrow::Field>(memberName, childType, memberType->IsOptional()));
        }

        type = std::make_shared<arrow::StructType>(members);
        return true;
    }

    if (unpacked->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, unpacked);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            std::shared_ptr<arrow::DataType> childType;
            auto elementType = tupleType->GetElementType(i);
            if (!ConvertArrowType(elementType, childType, onFail)) {
                return false;
            }

            fields.emplace_back(std::make_shared<arrow::Field>("field" + ToString(i), childType, elementType->IsOptional()));
        }

        type = std::make_shared<arrow::StructType>(fields);
        return true;
    }

    if (unpacked->IsPg()) {
        auto pgType = AS_TYPE(TPgType, unpacked);
        const auto& desc = NYql::NPg::LookupType(pgType->GetTypeId());
        if (desc.PassByValue) {
            type = arrow::uint64();
        } else {
            type = arrow::binary();
        }

        return true;
    }

    if (unpacked->IsResource()) {
        type = arrow::fixed_size_binary(sizeof(NYql::NUdf::TUnboxedValuePod));
        return true;
    }

    if (!unpacked->IsData()) {
        if (onFail) {
            onFail(unpacked);
        }
        return false;
    }

    auto slot = AS_TYPE(TDataType, unpacked)->GetDataSlot();
    if (!slot) {
        if (onFail) {
            onFail(unpacked);
        }
        return false;
    }

    bool result = ConvertArrowType(*slot, type);
    if (!result && onFail) {
        onFail(unpacked);
    }
    return result;
}

void TArrowType::Export(ArrowSchema* out) const {
    auto status = arrow::ExportType(*Type, out);
    if (!status.ok()) {
        UdfTerminate(status.ToString().c_str());
    }
}

//////////////////////////////////////////////////////////////////////////////
// TFunctionTypeInfoBuilder
//////////////////////////////////////////////////////////////////////////////
TFunctionTypeInfoBuilder::TFunctionTypeInfoBuilder(
        const TTypeEnvironment& env,
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
        const TStringBuf& moduleName,
        NUdf::ICountersProvider* countersProvider,
        const NUdf::TSourcePosition& pos,
        const NUdf::ISecureParamsProvider* provider)
    : Env_(env)
    , ReturnType_(nullptr)
    , RunConfigType_(Env_.GetTypeOfVoidLazy())
    , UserType_(Env_.GetTypeOfVoidLazy())
    , TypeInfoHelper_(typeInfoHelper)
    , ModuleName_(moduleName)
    , CountersProvider_(countersProvider)
    , Pos_(pos)
    , SecureParamsProvider_(provider)
{
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::ImplementationImpl(
        NUdf::TUniquePtr<NUdf::IBoxedValue> impl)
{
    Implementation_ = std::move(impl);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder7& TFunctionTypeInfoBuilder::IRImplementationImpl(
    const NUdf::TStringRef& moduleIR,
    const NUdf::TStringRef& moduleIRUniqId,
    const NUdf::TStringRef& functionName
) {
    ModuleIR_ = moduleIR;
    ModuleIRUniqID_ = moduleIRUniqId;
    IRFunctionName_ = functionName;
    return *this;
}

NUdf::TType* TFunctionTypeInfoBuilder::Null() const {
    return Env_.GetTypeOfNullLazy();
}

NUdf::TType* TFunctionTypeInfoBuilder::EmptyList() const {
    return Env_.GetTypeOfEmptyListLazy();
}

NUdf::TType* TFunctionTypeInfoBuilder::EmptyDict() const {
    return Env_.GetTypeOfEmptyDictLazy();
}

void TFunctionTypeInfoBuilder::Unused1()
{
}

NUdf::ISetTypeBuilder::TPtr TFunctionTypeInfoBuilder::Set() const {
    return new TSetTypeBuilder(*this);
}

NUdf::IEnumTypeBuilder::TPtr TFunctionTypeInfoBuilder::Enum(ui32 expectedItems) const {
    return new TEnumTypeBuilder(*this, expectedItems);
}

NUdf::TType* TFunctionTypeInfoBuilder::Tagged(const NUdf::TType* baseType, const NUdf::TStringRef& tag) const {
    return TTaggedType::Create(const_cast<TType*>(static_cast<const TType*>(baseType)), tag, Env_);
}

NUdf::TType* TFunctionTypeInfoBuilder::Pg(ui32 typeId) const {
    return TPgType::Create(typeId, Env_);
}

NUdf::IBlockTypeBuilder::TPtr TFunctionTypeInfoBuilder::Block(bool isScalar) const {
    return new TBlockTypeBuilder(*this, isScalar);
}

void TFunctionTypeInfoBuilder::Unused2() {
}

void TFunctionTypeInfoBuilder::Unused3() {
}

NUdf::IFunctionTypeInfoBuilder15& TFunctionTypeInfoBuilder::SupportsBlocksImpl() {
    SupportsBlocks_ = true;
    return *this;
}

NUdf::IFunctionTypeInfoBuilder15& TFunctionTypeInfoBuilder::IsStrictImpl() {
    IsStrict_ = true;
    return *this;
}

const NUdf::IBlockTypeHelper& TFunctionTypeInfoBuilder::IBlockTypeHelper() const {
    return BlockTypeHelper;
}

bool TFunctionTypeInfoBuilder::GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const {
    if (SecureParamsProvider_)
        return SecureParamsProvider_->GetSecureParam(key, value);
    return false;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::ReturnsImpl(
        NUdf::TDataTypeId typeId)
{
    ReturnType_ = TDataType::Create(typeId, Env_);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::ReturnsImpl(
        const NUdf::TType* type)
{
    ReturnType_ = static_cast<const NMiniKQL::TType*>(type);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::ReturnsImpl(
        const NUdf::ITypeBuilder& typeBuilder)
{
    ReturnType_ = static_cast<TType*>(typeBuilder.Build());
    return *this;
}

NUdf::IFunctionArgTypesBuilder::TPtr TFunctionTypeInfoBuilder::Args(
        ui32 expectedItem)
{
    Args_.reserve(expectedItem);
    return new TFunctionArgTypesBuilder(*this, Args_);
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::OptionalArgsImpl(ui32 optionalArgs) {
    OptionalArgs_ = optionalArgs;
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::PayloadImpl(const NUdf::TStringRef& payload) {
    Payload_ = payload;
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::RunConfigImpl(
        NUdf::TDataTypeId typeId)
{
    RunConfigType_ = TDataType::Create(typeId, Env_);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::RunConfigImpl(
        const NUdf::TType* type)
{
    RunConfigType_ = static_cast<const NMiniKQL::TType*>(type);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::RunConfigImpl(
        const NUdf::ITypeBuilder& typeBuilder)
{
    RunConfigType_ = static_cast<TType*>(typeBuilder.Build());
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::UserTypeImpl(
        NUdf::TDataTypeId typeId)
{
    UserType_ = TDataType::Create(typeId, Env_);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::UserTypeImpl(
        const NUdf::TType* type)
{
    UserType_ = static_cast<const NMiniKQL::TType*>(type);
    return *this;
}

NUdf::IFunctionTypeInfoBuilder1& TFunctionTypeInfoBuilder::UserTypeImpl(
        const NUdf::ITypeBuilder& typeBuilder)
{
    UserType_ = static_cast<TType*>(typeBuilder.Build());
    return *this;
}

void TFunctionTypeInfoBuilder::SetError(const NUdf::TStringRef& error)
{
    if (!Error_) {
        Error_ = error;
    }
}

void TFunctionTypeInfoBuilder::Build(TFunctionTypeInfo* funcInfo)
{
    if (ReturnType_) {
        TCallableTypeBuilder builder(Env_, UdfName, const_cast<NMiniKQL::TType*>(ReturnType_));
        for (const auto& arg : Args_) {
            builder.Add(arg.Type_);
            if (!arg.Name_.Str().empty()) {
                builder.SetArgumentName(arg.Name_.Str());
            }

            if (arg.Flags_ != 0) {
                builder.SetArgumentFlags(arg.Flags_);
            }
        }

        if (!Payload_.empty()) {
            builder.SetPayload(Payload_);
        }

        builder.SetOptionalArgs(OptionalArgs_);
        funcInfo->FunctionType = builder.Build();
    }

    funcInfo->RunConfigType = RunConfigType_;
    funcInfo->UserType = UserType_;
    funcInfo->Implementation = std::move(Implementation_);
    funcInfo->ModuleIR = std::move(ModuleIR_);
    funcInfo->ModuleIRUniqID = std::move(ModuleIRUniqID_);
    funcInfo->IRFunctionName = std::move(IRFunctionName_);
    funcInfo->SupportsBlocks = SupportsBlocks_;
    funcInfo->IsStrict = IsStrict_;
}

NUdf::TType* TFunctionTypeInfoBuilder::Primitive(NUdf::TDataTypeId typeId) const
{
    return TDataType::Create(typeId, Env_);
}

NUdf::TType* TFunctionTypeInfoBuilder::Decimal(ui8 precision, ui8 scale) const
{
    return TDataDecimalType::Create(precision, scale, Env_);
}

NUdf::IOptionalTypeBuilder::TPtr TFunctionTypeInfoBuilder::Optional() const
{
    return new TOptionalTypeBuilder(*this);
}

NUdf::IListTypeBuilder::TPtr TFunctionTypeInfoBuilder::List() const
{
    return new TListTypeBuilder(*this);
}

NUdf::IDictTypeBuilder::TPtr TFunctionTypeInfoBuilder::Dict() const
{
    return new TDictTypeBuilder(*this);
}

NUdf::IStructTypeBuilder::TPtr TFunctionTypeInfoBuilder::Struct(
        ui32 expectedItems) const
{
    return new NKikimr::TStructTypeBuilder(*this, expectedItems);
}

NUdf::ITupleTypeBuilder::TPtr TFunctionTypeInfoBuilder::Tuple(
        ui32 expectedItems) const
{
    return new TTupleTypeBuilder(*this, expectedItems);
}

NUdf::ICallableTypeBuilder::TPtr TFunctionTypeInfoBuilder::Callable(
        ui32 expectedArgs) const
{
    return new NKikimr::TCallableTypeBuilder(Env_, expectedArgs);
}

NUdf::TType* TFunctionTypeInfoBuilder::Void() const
{
    return Env_.GetTypeOfVoidLazy();
}

NUdf::TType* TFunctionTypeInfoBuilder::Resource(const NUdf::TStringRef& tag) const {
    return TResourceType::Create(tag, Env_);
}

NUdf::IVariantTypeBuilder::TPtr TFunctionTypeInfoBuilder::Variant() const {
    return new TVariantTypeBuilder(*this);
}

NUdf::IStreamTypeBuilder::TPtr TFunctionTypeInfoBuilder::Stream() const {
    return new TStreamTypeBuilder(*this);
}

NUdf::ITypeInfoHelper::TPtr TFunctionTypeInfoBuilder::TypeInfoHelper() const {
    return TypeInfoHelper_;
}

NUdf::TCounter TFunctionTypeInfoBuilder::GetCounter(const NUdf::TStringRef& name, bool deriv) {
    if (CountersProvider_) {
        return CountersProvider_->GetCounter(ModuleName_, name, deriv);
    }

    return {};
}

NUdf::TScopedProbe TFunctionTypeInfoBuilder::GetScopedProbe(const NUdf::TStringRef& name) {
    if (CountersProvider_) {
        return CountersProvider_->GetScopedProbe(ModuleName_, name);
    }

    return {};
}

NUdf::TSourcePosition TFunctionTypeInfoBuilder::GetSourcePosition() {
    return Pos_;
}

NUdf::IHash::TPtr TFunctionTypeInfoBuilder::MakeHash(const NUdf::TType* type) {
    try {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        return MakeHashImpl(mkqlType);
    } catch (const TTypeNotSupported& ex) {
        SetError(TStringBuf(ex.what()));
        return nullptr;
    }
}

NUdf::IEquate::TPtr TFunctionTypeInfoBuilder::MakeEquate(const NUdf::TType* type) {
    try {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        return MakeEquateImpl(mkqlType);
    } catch (const TTypeNotSupported& ex) {
        SetError(TStringBuf(ex.what()));
        return nullptr;
    }
}

NUdf::ICompare::TPtr TFunctionTypeInfoBuilder::MakeCompare(const NUdf::TType* type) {
    try {
        auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
        return MakeCompareImpl(mkqlType);
    } catch (const TTypeNotSupported& ex) {
        SetError(TStringBuf(ex.what()));
        return nullptr;
    }
}

NUdf::ETypeKind TTypeInfoHelper::GetTypeKind(const NUdf::TType* type) const {
    if (!type) {
        return NUdf::ETypeKind::Unknown;
    }

    auto mkqlType = static_cast<const NMiniKQL::TType*>(type);
    switch (mkqlType->GetKind()) {
    case NMiniKQL::TType::EKind::Data: return NUdf::ETypeKind::Data;
    case NMiniKQL::TType::EKind::Struct: return NUdf::ETypeKind::Struct;
    case NMiniKQL::TType::EKind::List: return NUdf::ETypeKind::List;
    case NMiniKQL::TType::EKind::Optional: return NUdf::ETypeKind::Optional;
    case NMiniKQL::TType::EKind::Tuple: return NUdf::ETypeKind::Tuple;
    case NMiniKQL::TType::EKind::Dict: return NUdf::ETypeKind::Dict;
    case NMiniKQL::TType::EKind::Callable: return NUdf::ETypeKind::Callable;
    case NMiniKQL::TType::EKind::Resource: return NUdf::ETypeKind::Resource;
    case NMiniKQL::TType::EKind::Variant: return NUdf::ETypeKind::Variant;
    case NMiniKQL::TType::EKind::Void: return NUdf::ETypeKind::Void;
    case NMiniKQL::TType::EKind::Stream: return NUdf::ETypeKind::Stream;
    case NMiniKQL::TType::EKind::Null: return NUdf::ETypeKind::Null;
    case NMiniKQL::TType::EKind::EmptyList: return NUdf::ETypeKind::EmptyList;
    case NMiniKQL::TType::EKind::EmptyDict: return NUdf::ETypeKind::EmptyDict;
    case NMiniKQL::TType::EKind::Tagged: return NUdf::ETypeKind::Tagged;
    case NMiniKQL::TType::EKind::Pg: return NUdf::ETypeKind::Pg;
    case NMiniKQL::TType::EKind::Block: return NUdf::ETypeKind::Block;
    default:
        Y_DEBUG_ABORT_UNLESS(false, "Wrong MQKL type kind %s", mkqlType->GetKindAsStr().data());
        return NUdf::ETypeKind::Unknown;
    }
}

void TTypeInfoHelper::VisitType(const NUdf::TType* type, NUdf::ITypeVisitor* visitor) const
{
    if (!type) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(visitor->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(1, 0)));
    auto mkqlType = static_cast<const NMiniKQL::TType*>(type);

#define MKQL_HANDLE_UDF_TYPE(TypeKind) \
case NMiniKQL::TType::EKind::TypeKind: { \
    auto mkqlType = static_cast<const NMiniKQL::T##TypeKind##Type*>(type); \
    Do##TypeKind(mkqlType, visitor); \
    break; \
}

    switch (mkqlType->GetKind()) {
        MKQL_HANDLE_UDF_TYPE(Data)
        MKQL_HANDLE_UDF_TYPE(Struct)
        MKQL_HANDLE_UDF_TYPE(List)
        MKQL_HANDLE_UDF_TYPE(Optional)
        MKQL_HANDLE_UDF_TYPE(Tuple)
        MKQL_HANDLE_UDF_TYPE(Dict)
        MKQL_HANDLE_UDF_TYPE(Callable)
        MKQL_HANDLE_UDF_TYPE(Variant)
        MKQL_HANDLE_UDF_TYPE(Stream)
        MKQL_HANDLE_UDF_TYPE(Resource)
        MKQL_HANDLE_UDF_TYPE(Tagged)
        MKQL_HANDLE_UDF_TYPE(Pg)
        MKQL_HANDLE_UDF_TYPE(Block)
    default:
        Y_DEBUG_ABORT_UNLESS(false, "Wrong MQKL type kind %s", mkqlType->GetKindAsStr().data());
    }

#undef MKQL_HANDLE_UDF_TYPE
}

bool TTypeInfoHelper::IsSameType(const NUdf::TType* type1, const NUdf::TType* type2) const {
    if (!type1 || !type2) {
        return false;
    }

    auto mkqlType1 = static_cast<const NMiniKQL::TType*>(type1);
    auto mkqlType2 = static_cast<const NMiniKQL::TType*>(type2);
    return mkqlType1->IsSameType(*mkqlType2);
}

const NYql::NUdf::TPgTypeDescription* TTypeInfoHelper::FindPgTypeDescription(ui32 typeId) const {
    return HugeSingleton<TPgTypeIndex>()->Resolve(typeId);
}

NUdf::IArrowType::TPtr TTypeInfoHelper::MakeArrowType(const NUdf::TType* type) const {
    std::shared_ptr<arrow::DataType> arrowType;
    if (!ConvertArrowType(const_cast<TType*>(static_cast<const TType*>(type)), arrowType)) {
        return nullptr;
    }

    return new TArrowType(arrowType);
}

NUdf::IArrowType::TPtr TTypeInfoHelper::ImportArrowType(ArrowSchema* schema) const {
    auto res = arrow::ImportType(schema);
    auto status = res.status();
    if (!status.ok()) {
        UdfTerminate(status.ToString().c_str());
    }

    return new TArrowType(std::move(res).ValueOrDie());
}

ui64 TTypeInfoHelper::GetMaxBlockLength(const NUdf::TType* type) const {
   return CalcBlockLen(CalcMaxBlockItemSize(static_cast<const TType*>(type)));
}

ui64 TTypeInfoHelper::GetMaxBlockBytes() const {
   return MaxBlockSizeInBytes;
}

void TTypeInfoHelper::DoData(const NMiniKQL::TDataType* dt, NUdf::ITypeVisitor* v) {
    const auto typeId = dt->GetSchemeType();
    v->OnDataType(typeId);
    if (v->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(2, 13))) {
        if (NUdf::TDataType<NUdf::TDecimal>::Id == typeId) {
            const auto& params = static_cast<const NMiniKQL::TDataDecimalType*>(dt)->GetParams();
            v->OnDecimal(params.first, params.second);
        }
    }
}

void TTypeInfoHelper::DoStruct(const NMiniKQL::TStructType* st, NUdf::ITypeVisitor* v) {
    ui32 membersCount = st->GetMembersCount();

    TSmallVec<NUdf::TStringRef> membersNames;
    TSmallVec<const NUdf::TType*> membersTypes;
    membersNames.reserve(membersCount);
    membersTypes.reserve(membersCount);

    for (ui32 i = 0; i < membersCount; i++) {
        membersNames.push_back(st->GetMemberName(i));
        membersTypes.push_back(st->GetMemberType(i));
    }

    v->OnStruct(membersCount, membersNames.data(), membersTypes.data());
}

void TTypeInfoHelper::DoList(const NMiniKQL::TListType* lt, NUdf::ITypeVisitor* v) {
    v->OnList(lt->GetItemType());
}

void TTypeInfoHelper::DoOptional(const NMiniKQL::TOptionalType* lt, NUdf::ITypeVisitor* v) {
    v->OnOptional(lt->GetItemType());
}

void TTypeInfoHelper::DoTuple(const NMiniKQL::TTupleType* tt, NUdf::ITypeVisitor* v) {
    ui32 elementsCount = tt->GetElementsCount();

    TSmallVec<const NUdf::TType*> elementsTypes;
    elementsTypes.reserve(elementsCount);

    for (ui32 i = 0; i < elementsCount; i++) {
        elementsTypes.push_back(tt->GetElementType(i));
    }

    v->OnTuple(elementsCount, elementsTypes.data());
}

void TTypeInfoHelper::DoDict(const NMiniKQL::TDictType* dt, NUdf::ITypeVisitor* v) {
    v->OnDict(dt->GetKeyType(), dt->GetPayloadType());
}

void TTypeInfoHelper::DoCallable(const NMiniKQL::TCallableType* ct, NUdf::ITypeVisitor* v) {
    auto returnType = ct->GetReturnType();
    ui32 argsCount = ct->GetArgumentsCount();
    ui32 optionalArgsCount = ct->GetOptionalArgumentsCount();

    TSmallVec<const NUdf::TType*> argsTypes;
    argsTypes.reserve(argsCount);

    for (ui32 i = 0; i < argsCount; i++) {
        argsTypes.push_back(ct->GetArgumentType(i));
    }

    if (ct->GetPayload()) {
        TCallablePayload payload(ct->GetPayload());
        v->OnCallable(returnType, argsCount, argsTypes.data(), optionalArgsCount, &payload);
    }
    else {
        v->OnCallable(returnType, argsCount, argsTypes.data(), optionalArgsCount, nullptr);
    }
}

void TTypeInfoHelper::DoVariant(const NMiniKQL::TVariantType* vt, NUdf::ITypeVisitor* v) {
    v->OnVariant(vt->GetUnderlyingType());
}

void TTypeInfoHelper::DoStream(const NMiniKQL::TStreamType* st, NUdf::ITypeVisitor* v) {
    v->OnStream(st->GetItemType());
}

void TTypeInfoHelper::DoResource(const NMiniKQL::TResourceType* rt, NUdf::ITypeVisitor* v) {
    if (v->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(2, 15))) {
        v->OnResource(rt->GetTag());
    }
}

void TTypeInfoHelper::DoTagged(const NMiniKQL::TTaggedType* tt, NUdf::ITypeVisitor* v) {
    if (v->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(2, 21))) {
        v->OnTagged(tt->GetBaseType(), tt->GetTag());
    }
}

void TTypeInfoHelper::DoPg(const NMiniKQL::TPgType* tt, NUdf::ITypeVisitor* v) {
    if (v->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(2, 25))) {
        v->OnPg(tt->GetTypeId());
    }
}

void TTypeInfoHelper::DoBlock(const NMiniKQL::TBlockType* tt, NUdf::ITypeVisitor* v) {
    if (v->IsCompatibleTo(NUdf::MakeAbiCompatibilityVersion(2, 26))) {
        v->OnBlock(tt->GetItemType(), tt->GetShape() == TBlockType::EShape::Scalar);
    }
}

bool CanHash(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Data: {
            auto slot = static_cast<const NMiniKQL::TDataType*>(type)->GetDataSlot();
            if (!slot) {
                return false;
            }
            if (!(NUdf::GetDataTypeInfo(*slot).Features & NUdf::CanHash)) {
                return false;
            }

            return true;
        }
        case NMiniKQL::TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            return CanHash(optionalType->GetItemType());
        }

        case NMiniKQL::TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                if (!CanHash(tupleType->GetElementType(i))) {
                    return false;
                }
            }

            return true;
        }

        case NMiniKQL::TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                if (!CanHash(structType->GetMemberType(i))) {
                    return false;
                }
            }

            return true;
        }

        case NMiniKQL::TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            return CanHash(listType->GetItemType());
        }

        case NMiniKQL::TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            return CanHash(variantType->GetUnderlyingType());
        }

        case NMiniKQL::TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            return CanHash(dictType->GetKeyType()) && CanHash(dictType->GetPayloadType());
        };

        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
            return true;
        case NMiniKQL::TType::EKind::Pg: {
            auto pgType = static_cast<const TPgType*>(type);
            return NYql::NPg::LookupType(pgType->GetTypeId()).HashProcId != 0;
        }
        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            return CanHash(taggedType->GetBaseType());
        }
        default:
            return false;
    }
}

NUdf::IHash::TPtr MakeHashImpl(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Data: {

#define MAKE_HASH(slot, ...)        \
        case NUdf::EDataSlot::slot: \
            return new THash<NMiniKQL::TType::EKind::Data, NUdf::EDataSlot::slot>;

            auto slot = static_cast<const NMiniKQL::TDataType*>(type)->GetDataSlot();
            if (!slot) {
                throw TTypeNotSupported() << "Invalid data slot";
            }
            const auto& info = NUdf::GetDataTypeInfo(*slot);
            if (!(info.Features & NUdf::CanHash)) {
                throw TTypeNotSupported() << "Type " << info.Name << " is not hashable";
            }
            switch (*slot) {
                UDF_TYPE_ID_MAP(MAKE_HASH)
            }

#undef MAKE_HASH
        }
        case NMiniKQL::TType::EKind::Optional:
            return new THash<NMiniKQL::TType::EKind::Optional>(type);
        case NMiniKQL::TType::EKind::Tuple:
            return new THash<NMiniKQL::TType::EKind::Tuple>(type);
        case NMiniKQL::TType::EKind::Struct:
            return new THash<NMiniKQL::TType::EKind::Struct>(type);
        case NMiniKQL::TType::EKind::List:
            return new THash<NMiniKQL::TType::EKind::List>(type);
        case NMiniKQL::TType::EKind::Variant:
            return new THash<NMiniKQL::TType::EKind::Variant>(type);
        case NMiniKQL::TType::EKind::Dict:
            return new THash<NMiniKQL::TType::EKind::Dict>(type);
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
            return new TEmptyHash();
        case NMiniKQL::TType::EKind::Pg:
            return MakePgHash((const TPgType*)type);
        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            return MakeHashImpl(taggedType->GetBaseType());
        }
        default:
            throw TTypeNotSupported() << "Data, Pg, Optional, Tuple, Struct, List, Variant or Dict is expected for hashing, "
            << "but got: " << PrintNode(type);
    }
}

NUdf::ICompare::TPtr MakeCompareImpl(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Data: {

#define MAKE_COMPARE(slot, ...)     \
        case NUdf::EDataSlot::slot: \
            return new TCompare<NMiniKQL::TType::EKind::Data, NUdf::EDataSlot::slot>;

            auto slot = static_cast<const NMiniKQL::TDataType*>(type)->GetDataSlot();
            if (!slot) {
                throw TTypeNotSupported() << "Invalid data slot";
            }
            const auto& info = NUdf::GetDataTypeInfo(*slot);
            if (!(info.Features & NUdf::CanCompare)) {
                throw TTypeNotSupported() << "Type " << info.Name << " is not comparable";
            }
            switch (*slot) {
                UDF_TYPE_ID_MAP(MAKE_COMPARE)
            }

#undef MAKE_COMPARE
        }
        case NMiniKQL::TType::EKind::Optional:
            return new TCompare<NMiniKQL::TType::EKind::Optional>(type);
        case NMiniKQL::TType::EKind::Tuple:
            return new TCompare<NMiniKQL::TType::EKind::Tuple>(type);
        case NMiniKQL::TType::EKind::Struct:
            return new TCompare<NMiniKQL::TType::EKind::Struct>(type);
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
            return new TEmptyCompare();
        case NMiniKQL::TType::EKind::Variant: {
            return new TCompare<NMiniKQL::TType::EKind::Variant>(type);
        }
        case NMiniKQL::TType::EKind::List:
            return new TCompare<NMiniKQL::TType::EKind::List>(type);
        case NMiniKQL::TType::EKind::Dict:
            return new TCompare<NMiniKQL::TType::EKind::Dict>(type);
        case NMiniKQL::TType::EKind::Pg:
            return MakePgCompare((const TPgType*)type);
        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            return MakeCompareImpl(taggedType->GetBaseType());
        }
        default:
            throw TTypeNotSupported() << "Data, Pg, Optional, Variant, Tuple, Struct, List or Dict are expected for comparing, "
            << "but got: " << PrintNode(type);
    }
}

NUdf::IEquate::TPtr MakeEquateImpl(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Data: {

#define MAKE_EQUATE(slot, ...)      \
        case NUdf::EDataSlot::slot: \
            return new TEquate<NMiniKQL::TType::EKind::Data, NUdf::EDataSlot::slot>;

            auto slot = static_cast<const NMiniKQL::TDataType*>(type)->GetDataSlot();
            if (!slot) {
                throw TTypeNotSupported() << "Invalid data slot";
            }
            const auto& info = NUdf::GetDataTypeInfo(*slot);
            if (!(info.Features & NUdf::CanEquate)) {
                throw TTypeNotSupported() << "Type " << info.Name << " is not equatable";
            }
            switch (*slot) {
                UDF_TYPE_ID_MAP(MAKE_EQUATE)
            }

#undef MAKE_EQUATE
        }
        case NMiniKQL::TType::EKind::Optional:
            return new TEquate<NMiniKQL::TType::EKind::Optional>(type);
        case NMiniKQL::TType::EKind::Tuple:
            return new TEquate<NMiniKQL::TType::EKind::Tuple>(type);
        case NMiniKQL::TType::EKind::Struct:
            return new TEquate<NMiniKQL::TType::EKind::Struct>(type);
        case NMiniKQL::TType::EKind::List:
            return new TEquate<NMiniKQL::TType::EKind::List>(type);
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
            return new TEmptyEquate();
        case NMiniKQL::TType::EKind::Variant:
            return new TEquate<NMiniKQL::TType::EKind::Variant>(type);
        case NMiniKQL::TType::EKind::Dict:
            return new TEquate<NMiniKQL::TType::EKind::Dict>(type);
        case NMiniKQL::TType::EKind::Pg:
            return MakePgEquate((const TPgType*)type);
        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            return MakeEquateImpl(taggedType->GetBaseType());
        }
        default:
            throw TTypeNotSupported() << "Data, Pg, Optional, Tuple, Struct, List, Variant or Dict is expected for equating, "
            << "but got: " << PrintNode(type);
    }
}

size_t CalcMaxBlockItemSize(const TType* type) {
    // we do not count block bitmap size
    if (type->IsOptional()) {
        return CalcMaxBlockItemSize(AS_TYPE(TOptionalType, type)->GetItemType());
    }

    if (type->IsStruct()) {
        auto structType = AS_TYPE(TStructType, type);
        size_t result = 0;
        for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
            result = std::max(result, CalcMaxBlockItemSize(structType->GetMemberType(i)));
        }
        return result;
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        size_t result = 0;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            result = std::max(result, CalcMaxBlockItemSize(tupleType->GetElementType(i)));
        }
        return result;
    }

    if (type->IsPg()) {
        auto pgType = AS_TYPE(TPgType, type);
        const auto& desc = NYql::NPg::LookupType(pgType->GetTypeId());
        if (desc.PassByValue) {
            return 8;
        } else {
            return sizeof(arrow::BinaryType::offset_type);
        }
    }

    if (type->IsResource()) {
        return sizeof(NYql::NUdf::TUnboxedValue);
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Int16:
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
        case NUdf::EDataSlot::Date32:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Double: {
            size_t sz = GetDataTypeInfo(slot).FixedSize;
            MKQL_ENSURE(sz > 0, "Unexpected fixed data size");
            return sz;
        }
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::JsonDocument:
            // size of offset part
            return sizeof(arrow::BinaryType::offset_type);
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
            // size of offset part
            return sizeof(arrow::StringType::offset_type);
        case NUdf::EDataSlot::TzDate:
            return sizeof(typename NUdf::TDataType<NUdf::TTzDate>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::TzDatetime:
            return sizeof(typename NUdf::TDataType<NUdf::TTzDatetime>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::TzTimestamp:
            return sizeof(typename NUdf::TDataType<NUdf::TTzTimestamp>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::TzDate32:
            return sizeof(typename NUdf::TDataType<NUdf::TTzDate32>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::TzDatetime64:
            return sizeof(typename NUdf::TDataType<NUdf::TTzDatetime64>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::TzTimestamp64:
            return sizeof(typename NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout) + sizeof(NYql::NUdf::TTimezoneId);
        case NUdf::EDataSlot::Uuid: {
            MKQL_ENSURE(false, "Unsupported data slot: " << slot);
        }
        case NUdf::EDataSlot::Decimal: {
            return sizeof(NYql::NDecimal::TInt128);
        }
        case NUdf::EDataSlot::DyNumber: {
            MKQL_ENSURE(false, "Unsupported data slot: " << slot);
        }
        }
    }

    MKQL_ENSURE(false, "Unsupported type: " << *type);
}

struct TComparatorTraits {
    using TResult = NUdf::IBlockItemComparator;
    template <bool Nullable>
    using TTuple = NUdf::TTupleBlockItemComparator<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = NUdf::TFixedSizeBlockItemComparator<T, Nullable>;
    template <typename TStringType, bool Nullable, NUdf::EDataSlot TOriginal = NUdf::EDataSlot::String>
    using TStrings = NUdf::TStringBlockItemComparator<TStringType, Nullable>;
    using TExtOptional = NUdf::TExternalOptionalBlockItemComparator;
    template <typename T, bool Nullable>
    using TTzDateComparator = NUdf::TTzDateBlockItemComparator<T, Nullable>;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        return std::unique_ptr<TResult>(MakePgItemComparator(desc.TypeId).Release());
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        ythrow yexception() << "Comparator not implemented for block resources: ";
    }

    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateComparator<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateComparator<TTzDate, false>>();
        }
    }
};

struct THasherTraits {
    using TResult = NUdf::IBlockItemHasher;
    template <bool Nullable>
    using TTuple = NUdf::TTupleBlockItemHasher<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = NUdf::TFixedSizeBlockItemHasher<T, Nullable>;
    template <typename TStringType, bool Nullable, NUdf::EDataSlot TOriginal = NUdf::EDataSlot::String>
    using TStrings = NUdf::TStringBlockItemHasher<TStringType, Nullable>;
    using TExtOptional = NUdf::TExternalOptionalBlockItemHasher;
    template <typename T, bool Nullable>
    using TTzDateHasher = NYql::NUdf::TTzDateBlockItemHasher<T, Nullable>;

    static std::unique_ptr<TResult> MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        return std::unique_ptr<TResult>(MakePgItemHasher(desc.TypeId).Release());
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        Y_UNUSED(isOptional);
        ythrow yexception() << "Hasher not implemented for block resources";
    }
    
    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateHasher<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateHasher<TTzDate, false>>();
        }
    }
};

NUdf::IBlockItemComparator::TPtr TBlockTypeHelper::MakeComparator(NUdf::TType* type) const {
    return NUdf::MakeBlockReaderImpl<TComparatorTraits>(TTypeInfoHelper(), type, nullptr).release();
}

NUdf::IBlockItemHasher::TPtr TBlockTypeHelper::MakeHasher(NUdf::TType* type) const {
    return NUdf::MakeBlockReaderImpl<THasherTraits>(TTypeInfoHelper(), type, nullptr).release();
}

TType* TTypeBuilder::NewVoidType() const {
    return TRuntimeNode(Env.GetVoidLazy(), true).GetStaticType();
}

TType* TTypeBuilder::NewNullType() const {
    if (!UseNullType || RuntimeVersion < 11) {
        TCallableBuilder callableBuilder(Env, "Null", NewOptionalType(NewVoidType()));
        return TRuntimeNode(callableBuilder.Build(), false).GetStaticType();
    } else {
        return TRuntimeNode(Env.GetNullLazy(), true).GetStaticType();
    }
}

TType* TTypeBuilder::NewEmptyStructType() const {
    return Env.GetEmptyStructLazy()->GetGenericType();
}

TType* TTypeBuilder::NewStructType(TType* baseStructType, const std::string_view& memberName, TType* memberType) const {
    MKQL_ENSURE(baseStructType->IsStruct(), "Expected struct type");

    const auto& detailedBaseStructType = static_cast<const TStructType&>(*baseStructType);
    TStructTypeBuilder builder(Env);
    builder.Reserve(detailedBaseStructType.GetMembersCount() + 1);
    for (ui32 i = 0, e = detailedBaseStructType.GetMembersCount(); i < e; ++i) {
        builder.Add(detailedBaseStructType.GetMemberName(i), detailedBaseStructType.GetMemberType(i));
    }

    builder.Add(memberName, memberType);
    return builder.Build();
}

TType* TTypeBuilder::NewStructType(const TArrayRef<const std::pair<std::string_view, TType*>>& memberTypes) const {
    TStructTypeBuilder builder(Env);
    builder.Reserve(memberTypes.size());
    for (auto& x : memberTypes) {
        builder.Add(x.first, x.second);
    }

    return builder.Build();
}

TType* TTypeBuilder::NewArrayType(const TArrayRef<const std::pair<std::string_view, TType*>>& memberTypes) const {
    return NewStructType(memberTypes);
}

TType* TTypeBuilder::NewDataType(NUdf::TDataTypeId schemeType, bool optional) const {
    return optional ? NewOptionalType(TDataType::Create(schemeType, Env)) : TDataType::Create(schemeType, Env);
}

TType* TTypeBuilder::NewPgType(ui32 typeId) const {
    return TPgType::Create(typeId, Env);
}

TType* TTypeBuilder::NewDecimalType(ui8 precision, ui8 scale) const {
    return TDataDecimalType::Create(precision, scale, Env);
}

TType* TTypeBuilder::NewOptionalType(TType* itemType) const {
    return TOptionalType::Create(itemType, Env);
}

TType* TTypeBuilder::NewListType(TType* itemType) const {
    return TListType::Create(itemType, Env);
}

TType* TTypeBuilder::NewStreamType(TType* itemType) const {
    return TStreamType::Create(itemType, Env);
}

TType* TTypeBuilder::NewFlowType(TType* itemType) const {
    return TFlowType::Create(itemType, Env);
}

TType* TTypeBuilder::NewBlockType(TType* itemType, TBlockType::EShape shape) const {
    return TBlockType::Create(itemType, shape, Env);
}

TType* TTypeBuilder::NewTaggedType(TType* baseType, const std::string_view& tag) const {
    return TTaggedType::Create(baseType, tag, Env);
}

TType* TTypeBuilder::NewDictType(TType* keyType, TType* payloadType, bool multi) const {
    return TDictType::Create(keyType, multi ? NewListType(payloadType) : payloadType, Env);
}

TType* TTypeBuilder::NewEmptyTupleType() const {
    return Env.GetEmptyTupleLazy()->GetGenericType();
}

TType* TTypeBuilder::NewTupleType(const TArrayRef<TType* const>& elements) const {
    return TTupleType::Create(elements.size(), elements.data(), Env);
}

TType* TTypeBuilder::NewArrayType(const TArrayRef<TType* const>& elements) const {
    return NewTupleType(elements);
}

TType* TTypeBuilder::NewEmptyMultiType() const {
    if (RuntimeVersion > 35) {
        return TMultiType::Create(0, nullptr, Env);
    }
    return Env.GetEmptyTupleLazy()->GetGenericType();
}

TType* TTypeBuilder::NewMultiType(const TArrayRef<TType* const>& elements) const {
    if (RuntimeVersion > 35) {
        return TMultiType::Create(elements.size(), elements.data(), Env);
    }
    return TTupleType::Create(elements.size(), elements.data(), Env);
}

TType* TTypeBuilder::NewResourceType(const std::string_view& tag) const {
    return TResourceType::Create(tag, Env);
}

TType* TTypeBuilder::NewVariantType(TType* underlyingType) const {
    return TVariantType::Create(underlyingType, Env);
}

void RebuildTypeIndex() {
    HugeSingleton<TPgTypeIndex>()->Rebuild();
}

} // namespace NMiniKQL
} // namespace Nkikimr
