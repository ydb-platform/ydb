#include "yql_result_format_data.h"
#include "yql_result_format_type.h"
#include "yql_result_format_impl.h"

#include "yql_restricted_yson.h"
#include <yql/essentials/utils/parse_double.h>
#include <yql/essentials/utils/utf8.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/string/cast.h>


namespace NYql::NResult {

class IDataProcessor {
public:
    virtual ~IDataProcessor() = default;
    virtual void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) = 0;
};

class TVoidProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsString());
        CHECK(dataNode.AsString() == "Void");
        visitor.OnVoid();
    }
};

class TNullProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsEntity());
        visitor.OnNull();
    }
};

class TEmptyListProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList() && dataNode.AsList().size() == 0);
        visitor.OnEmptyList();
    }
};

class TEmptyDictProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList() && dataNode.AsList().size() == 0);
        visitor.OnEmptyDict();
    }
};

template <typename T, void (IDataVisitor::*Func)(T)>
class TIntegerProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsString());
        (visitor.*Func)(FromString<T>(dataNode.AsString()));
    }
};

template <typename T, T (*Conv)(TStringBuf), void (IDataVisitor::*Func)(T)>
class TFloatingProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsString());
        (visitor.*Func)(Conv(dataNode.AsString()));
    }
};

class TBoolProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsBool());
        visitor.OnBool(dataNode.AsBool());
    }
};

template <void (IDataVisitor::*Func)(TStringBuf, bool)>
class TStringProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsString() ||
            dataNode.IsList() && dataNode.AsList().size() == 1 && dataNode.AsList()[0].IsString());
        if (dataNode.IsString()) {
            (visitor.*Func)(dataNode.AsString(), true);
        } else {
            (visitor.*Func)(Base64Decode(dataNode.AsList()[0].AsString()), false);
        }
    }
};

template <void (IDataVisitor::*Func)(TStringBuf)>
class TUtf8Processor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsString());
        (visitor.*Func)(dataNode.AsString());
    }
};

class TYsonProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        auto str = DecodeRestrictedYson(dataNode);
        visitor.OnYson(str, IsUtf8(str));
    }
};

class TOptionalProcessor : public IDataProcessor {
public:
    TOptionalProcessor(std::unique_ptr<IDataProcessor>&& inner)
        : Inner_(std::move(inner))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsEntity() || dataNode.IsList() && dataNode.AsList().size() <= 1);
        visitor.OnBeginOptional();
        if (dataNode.IsEntity() || dataNode.AsList().empty()) {
            visitor.OnEmptyOptional();
        } else {
            visitor.OnBeforeOptionalItem();
            Inner_->Process(dataNode.AsList()[0], visitor);
            visitor.OnAfterOptionalItem();
        }

        visitor.OnEndOptional();
    }

private:
    const std::unique_ptr<IDataProcessor> Inner_;
};

class TListProcessor : public IDataProcessor {
public:
    TListProcessor(std::unique_ptr<IDataProcessor>&& inner)
        : Inner_(std::move(inner))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList());
        visitor.OnBeginList();
        for (const auto& item : dataNode.AsList()) {
            visitor.OnBeforeListItem();
            Inner_->Process(item, visitor);
            visitor.OnAfterListItem();
        }

        visitor.OnEndList();
    }

private:
    const std::unique_ptr<IDataProcessor> Inner_;
};

class TTupleProcessor : public IDataProcessor {
public:
    TTupleProcessor(TVector<std::unique_ptr<IDataProcessor>>&& inners)
        : Inners_(std::move(inners))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList());
        visitor.OnBeginTuple();
        CHECK(dataNode.AsList().size() == Inners_.size());
        for (ui32 i = 0; i < Inners_.size(); ++i) {
            visitor.OnBeforeTupleItem();
            Inners_[i]->Process(dataNode.AsList()[i], visitor);
            visitor.OnAfterTupleItem();
        }

        visitor.OnEndTuple();
    }

private:
    const TVector<std::unique_ptr<IDataProcessor>> Inners_;
};

class TStructProcessor : public IDataProcessor {
public:
    TStructProcessor(TVector<std::unique_ptr<IDataProcessor>>&& inners)
        : Inners_(std::move(inners))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList());
        visitor.OnBeginStruct();
        CHECK(dataNode.AsList().size() == Inners_.size());
        for (ui32 i = 0; i < Inners_.size(); ++i) {
            visitor.OnBeforeStructItem();
            Inners_[i]->Process(dataNode.AsList()[i], visitor);
            visitor.OnAfterStructItem();
        }

        visitor.OnEndStruct();
    }

private:
    const TVector<std::unique_ptr<IDataProcessor>> Inners_;
};

class TDictProcessor : public IDataProcessor {
public:
    TDictProcessor(std::unique_ptr<IDataProcessor>&& innerKey, std::unique_ptr<IDataProcessor>&& innerPayload)
        : InnerKey_(std::move(innerKey))
        , InnerPayload_(std::move(innerPayload))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList());
        visitor.OnBeginDict();
        for (const auto& item : dataNode.AsList()) {
            CHECK(item.IsList() && item.AsList().size() == 2);
            visitor.OnBeforeDictItem();
            visitor.OnBeforeDictKey();
            InnerKey_->Process(item.AsList()[0], visitor);
            visitor.OnAfterDictKey();
            visitor.OnBeforeDictPayload();
            InnerPayload_->Process(item.AsList()[1], visitor);
            visitor.OnAfterDictPayload();
            visitor.OnAfterDictItem();
        }

        visitor.OnEndDict();
    }

private:
    const std::unique_ptr<IDataProcessor> InnerKey_;
    const std::unique_ptr<IDataProcessor> InnerPayload_;
};

class TVariantProcessor : public IDataProcessor {
public:
    TVariantProcessor(TVector<std::unique_ptr<IDataProcessor>>&& inners)
        : Inners_(std::move(inners))
    {}

    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsList() && dataNode.AsList().size() == 2);
        CHECK(dataNode.AsList()[0].IsString());
        ui32 index = FromString<ui32>(dataNode.AsList()[0].AsString());
        CHECK(index < Inners_.size());
        visitor.OnBeginVariant(index);
        Inners_[index]->Process(dataNode.AsList()[1], visitor);
        visitor.OnEndVariant();
    }

private:
    const TVector<std::unique_ptr<IDataProcessor>> Inners_;
};

class TPgProcessor : public IDataProcessor {
public:
    void Process(const NYT::TNode& dataNode, IDataVisitor& visitor) final {
        CHECK(dataNode.IsEntity() || dataNode.IsString() ||
            dataNode.IsList() && dataNode.AsList().size() == 1 && dataNode.AsList()[0].IsString());
        if (dataNode.IsEntity()) {
            visitor.OnPg(Nothing(), true);
        } else if (dataNode.IsString()) {
            visitor.OnPg(dataNode.AsString(), true);
        } else {
            visitor.OnPg(Base64Decode(dataNode.AsList()[0].AsString()), false);
        }
    }
};

class TDataProcessorBuilder : public TThrowingTypeVisitor {
public:
    TDataProcessorBuilder() {
        IsVariant_.push_back(false);
    }

    IDataProcessor& GetResult() {
        CHECK(Stack_.size() == 1);
        return *Stack_.front();
    }

    void OnVoid() final {
        Stack_.push_back(std::make_unique<TVoidProcessor>());
    }

    void OnNull() final {
        Stack_.push_back(std::make_unique<TNullProcessor>());
    }

    void OnEmptyList() final {
        Stack_.push_back(std::make_unique<TEmptyListProcessor>());
    }

    void OnEmptyDict() final {
        Stack_.push_back(std::make_unique<TEmptyDictProcessor>());
    }

    void OnBool() final {
        Stack_.push_back(std::make_unique<TBoolProcessor>());
    }

    void OnInt8() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i8, &IDataVisitor::OnInt8>>());
    }

    void OnUint8() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui8, &IDataVisitor::OnUint8>>());
    }

    void OnInt16() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i16, &IDataVisitor::OnInt16>>());
    }

    void OnUint16() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui16, &IDataVisitor::OnUint16>>());
    }

    void OnInt32() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i32, &IDataVisitor::OnInt32>>());
    }

    void OnUint32() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui32, &IDataVisitor::OnUint32>>());
    }

    void OnInt64() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i64, &IDataVisitor::OnInt64>>());
    }

    void OnUint64() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui64, &IDataVisitor::OnUint64>>());
    }

    void OnFloat() final {
        Stack_.push_back(std::make_unique<TFloatingProcessor<float, FloatFromString, &IDataVisitor::OnFloat>>());
    }

    void OnDouble() final {
        Stack_.push_back(std::make_unique<TFloatingProcessor<double, DoubleFromString, &IDataVisitor::OnDouble>>());
    }

    void OnString() final {
        Stack_.push_back(std::make_unique<TStringProcessor<&IDataVisitor::OnString>>());
    }

    void OnUtf8() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnUtf8>>());
    }

    void OnYson() final {
        Stack_.push_back(std::make_unique<TYsonProcessor>());
    }

    void OnJson() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnJson>>());
    }

    void OnJsonDocument() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnJsonDocument>>());
    }

    void OnUuid() final {
        Stack_.push_back(std::make_unique<TStringProcessor<&IDataVisitor::OnUuid>>());
    }

    void OnDyNumber() final {
        Stack_.push_back(std::make_unique<TStringProcessor<&IDataVisitor::OnDyNumber>>());
    }

    void OnDate() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui16, &IDataVisitor::OnDate>>());
    }

    void OnDatetime() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui32, &IDataVisitor::OnDatetime>>());
    }

    void OnTimestamp() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<ui64, &IDataVisitor::OnTimestamp>>());
    }

    void OnTzDate() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzDate>>());
    }

    void OnTzDatetime() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzDatetime>>());
    }

    void OnTzTimestamp() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzTimestamp>>());
    }

    void OnInterval() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i64, &IDataVisitor::OnInterval>>());
    }

    void OnDate32() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i32, &IDataVisitor::OnDate32>>());
    }

    void OnDatetime64() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i64, &IDataVisitor::OnDatetime64>>());
    }

    void OnTimestamp64() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i64, &IDataVisitor::OnTimestamp64>>());
    }

    void OnTzDate32() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzDate32>>());
    }

    void OnTzDatetime64() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzDatetime64>>());
    }

    void OnTzTimestamp64() final {
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnTzTimestamp64>>());
    }

    void OnInterval64() final {
        Stack_.push_back(std::make_unique<TIntegerProcessor<i64, &IDataVisitor::OnInterval64>>());
    }

    void OnDecimal(ui32 precision, ui32 scale) final {
        Y_UNUSED(precision);
        Y_UNUSED(scale);
        Stack_.push_back(std::make_unique<TUtf8Processor<&IDataVisitor::OnDecimal>>());
    }

    void OnBeginOptional() final {
    }

    void OnEndOptional() final {
        auto inner = Pop();
        Stack_.push_back(std::make_unique<TOptionalProcessor>(std::move(inner)));
    }

    void OnBeginList() final {
    }

    void OnEndList() final {
        auto inner = Pop();
        Stack_.push_back(std::make_unique<TListProcessor>(std::move(inner)));
    }

    void OnBeginTuple() final {
        IsVariant_.push_back(false);
        Args_.push_back(0);
    }

    void OnTupleItem() final {
        Args_.back() += 1;
    }

    void OnEndTuple() final {
        const ui32 width = Args_.back();
        auto inners = Pop(width);
        if (!IsVariant_[IsVariant_.size() - 2]) {
            Stack_.push_back(std::make_unique<TTupleProcessor>(std::move(inners)));
        } else {
            Stack_.push_back(std::make_unique<TVariantProcessor>(std::move(inners)));
        }

        Args_.pop_back();
        IsVariant_.pop_back();
    }

    void OnBeginStruct() final {
        IsVariant_.push_back(false);
        Args_.push_back(0);
    }

    void OnStructItem(TStringBuf member) final {
        Y_UNUSED(member);
        Args_.back() += 1;
    }

    void OnEndStruct() final {
        const ui32 width = Args_.back();
        auto inners = Pop(width);
        if (!IsVariant_[IsVariant_.size() - 2]) {
            Stack_.push_back(std::make_unique<TStructProcessor>(std::move(inners)));
        } else {
            Stack_.push_back(std::make_unique<TVariantProcessor>(std::move(inners)));
        }

        Args_.pop_back();
        IsVariant_.pop_back();
    }

    void OnBeginDict() final {
    }

    void OnDictKey() final {
    }

    void OnDictPayload() final {
    }

    void OnEndDict() final {
        auto innerPayload = Pop();
        auto innerKey = Pop();
        Stack_.push_back(std::make_unique<TDictProcessor>(std::move(innerKey), std::move(innerPayload)));
    }

    void OnBeginVariant() {
        IsVariant_.push_back(true);
    }

    void OnEndVariant() {
        IsVariant_.pop_back();
    }

    void OnBeginTagged(TStringBuf tag) {
        Y_UNUSED(tag);
    }

    void OnEndTagged() {
    }

    void OnPg(TStringBuf name, TStringBuf category) final {
        Y_UNUSED(name);
        Y_UNUSED(category);
        Stack_.push_back(std::make_unique<TPgProcessor>());
    }

private:
    std::unique_ptr<IDataProcessor> Pop() {
        CHECK(!Stack_.empty());
        auto res = std::move(Stack_.back());
        Stack_.pop_back();
        return res;
    }

    TVector<std::unique_ptr<IDataProcessor>> Pop(ui32 width) {
        CHECK(Stack_.size() >= width);
        TVector<std::unique_ptr<IDataProcessor>> res;
        res.reserve(width);
        for (ui32 i = 0; i < width; ++i) {
            res.push_back(Pop());
        }

        Reverse(res.begin(), res.end());
        return res;
    }

private:
    TVector<std::unique_ptr<IDataProcessor>> Stack_;
    TVector<ui32> Args_;
    TVector<bool> IsVariant_;
};

void ParseData(const NYT::TNode& typeNode, const NYT::TNode& dataNode, IDataVisitor& visitor) {
    TDataProcessorBuilder builder;
    ParseType(typeNode, builder);
    builder.GetResult().Process(dataNode, visitor);
}

TDataBuilder::TDataBuilder() {
    Stack_.push_back(&Root_);
}

const NYT::TNode& TDataBuilder::GetResult() const {
    CHECK(Stack_.size() == 1);
    return Root_;
}

void TDataBuilder::OnVoid() {
    Top() = "Void";
}

void TDataBuilder::OnNull() {
    Top() = NYT::TNode::CreateEntity();
}

void TDataBuilder::OnEmptyList() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnEmptyDict() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBool(bool value) {
    Top() = value;
}

void TDataBuilder::OnInt8(i8 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnUint8(ui8 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnInt16(i16 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnUint16(ui16 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnInt32(i32 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnUint32(ui32 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnInt64(i64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnUint64(ui64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnFloat(float value) {
    Top() = FloatToString(value);
}

void TDataBuilder::OnDouble(double value) {
    Top() = FloatToString(value);
}

void TDataBuilder::OnString(TStringBuf value, bool isUtf8) {
    if (isUtf8) {
        Top() = value;
    } else {
        Top() = NYT::TNode().Add(Base64Encode(value));
    }
}

void TDataBuilder::OnUtf8(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnYson(TStringBuf value, bool isUtf8) {
    Y_UNUSED(isUtf8);
    NYT::TNodeBuilder builder(&Top());
    TYsonResultWriter writer(builder);
    EncodeRestrictedYson(writer, value);
}

void TDataBuilder::OnJson(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnJsonDocument(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnUuid(TStringBuf value, bool isUtf8) {
    OnString(value, isUtf8);
}

void TDataBuilder::OnDyNumber(TStringBuf value, bool isUtf8) {
    OnString(value, isUtf8);
}

void TDataBuilder::OnDate(ui16 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnDatetime(ui32 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnTimestamp(ui64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnTzDate(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnTzDatetime(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnTzTimestamp(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnInterval(i64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnDate32(i32 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnDatetime64(i64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnTimestamp64(i64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnTzDate32(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnTzDatetime64(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnTzTimestamp64(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnInterval64(i64 value) {
    Top() = ToString(value);
}

void TDataBuilder::OnDecimal(TStringBuf value) {
    Top() = value;
}

void TDataBuilder::OnBeginOptional() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBeforeOptionalItem() {
    Top().Add();
    Push(&Top().AsList()[0]);
}

void TDataBuilder::OnAfterOptionalItem() {
    Pop();
}

void TDataBuilder::OnEmptyOptional() {
}

void TDataBuilder::OnEndOptional() {
}

void TDataBuilder::OnBeginList() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBeforeListItem() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnAfterListItem() {
    Pop();
}

void TDataBuilder::OnEndList() {
}

void TDataBuilder::OnBeginTuple() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBeforeTupleItem() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnAfterTupleItem() {
    Pop();
}

void TDataBuilder::OnEndTuple() {
}

void TDataBuilder::OnBeginStruct() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBeforeStructItem() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnAfterStructItem() {
    Pop();
}

void TDataBuilder::OnEndStruct() {
}

void TDataBuilder::OnBeginDict() {
    Top() = NYT::TNode::CreateList();
}

void TDataBuilder::OnBeforeDictItem() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnBeforeDictKey() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnAfterDictKey() {
    Pop();
}

void TDataBuilder::OnBeforeDictPayload() {
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnAfterDictPayload() {
    Pop();
}

void TDataBuilder::OnAfterDictItem() {
    Pop();
}

void TDataBuilder::OnEndDict() {
}

void TDataBuilder::OnBeginVariant(ui32 index) {
    Top() = NYT::TNode::CreateList();
    Top().Add(ToString(index));
    Top().Add();
    Push(&Top().AsList().back());
}

void TDataBuilder::OnEndVariant() {
    Pop();
}

void TDataBuilder::OnPg(TMaybe<TStringBuf> value, bool isUtf8) {
    if (!value.Defined()) {
        Top() = NYT::TNode::CreateEntity();
    } else if (isUtf8) {
        Top() = *value;
    } else {
        Top() = NYT::TNode().Add(Base64Encode(*value));
    }
}

NYT::TNode& TDataBuilder::Top() {
    return *Stack_.back();
}

void TDataBuilder::Push(NYT::TNode* value) {
    Stack_.push_back(value);
}

void TDataBuilder::Pop() {
    Stack_.pop_back();
}

void TSameActionDataVisitor::OnVoid() {
    Do();
}

void TSameActionDataVisitor::OnNull() {
    Do();
}

void TSameActionDataVisitor::OnEmptyList() {
    Do();
}

void TSameActionDataVisitor::OnEmptyDict() {
    Do();
}

void TSameActionDataVisitor::OnBool(bool value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInt8(i8 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnUint8(ui8 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInt16(i16 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnUint16(ui16 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInt32(i32 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnUint32(ui32 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInt64(i64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnUint64(ui64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnFloat(float value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnDouble(double value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnString(TStringBuf value, bool isUtf8) {
    Y_UNUSED(value);
    Y_UNUSED(isUtf8);
    Do();
}

void TSameActionDataVisitor::OnUtf8(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnYson(TStringBuf value, bool isUtf8) {
    Y_UNUSED(value);
    Y_UNUSED(isUtf8);
    Do();
}

void TSameActionDataVisitor::OnJson(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnJsonDocument(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnUuid(TStringBuf value, bool isUtf8) {
    Y_UNUSED(value);
    Y_UNUSED(isUtf8);
    Do();
}

void TSameActionDataVisitor::OnDyNumber(TStringBuf value, bool isUtf8) {
    Y_UNUSED(value);
    Y_UNUSED(isUtf8);
    Do();
}

void TSameActionDataVisitor::OnDate(ui16 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnDatetime(ui32 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTimestamp(ui64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzDate(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzDatetime(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzTimestamp(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInterval(i64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnDate32(i32 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnDatetime64(i64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTimestamp64(i64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzDate32(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzDatetime64(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnTzTimestamp64(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnInterval64(i64 value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnDecimal(TStringBuf value) {
    Y_UNUSED(value);
    Do();
}

void TSameActionDataVisitor::OnBeginOptional() {
    Do();
}

void TSameActionDataVisitor::OnBeforeOptionalItem() {
    Do();
}

void TSameActionDataVisitor::OnAfterOptionalItem() {
    Do();
}

void TSameActionDataVisitor::OnEmptyOptional() {
    Do();
}

void TSameActionDataVisitor::OnEndOptional() {
    Do();
}

void TSameActionDataVisitor::OnBeginList() {
    Do();
}

void TSameActionDataVisitor::OnBeforeListItem() {
    Do();
}

void TSameActionDataVisitor::OnAfterListItem() {
    Do();
}

void TSameActionDataVisitor::OnEndList() {
    Do();
}

void TSameActionDataVisitor::OnBeginTuple() {
    Do();
}

void TSameActionDataVisitor::OnBeforeTupleItem() {
    Do();
}

void TSameActionDataVisitor::OnAfterTupleItem() {
    Do();
}

void TSameActionDataVisitor::OnEndTuple() {
    Do();
}

void TSameActionDataVisitor::OnBeginStruct() {
    Do();
}

void TSameActionDataVisitor::OnBeforeStructItem() {
    Do();
}

void TSameActionDataVisitor::OnAfterStructItem() {
    Do();
}

void TSameActionDataVisitor::OnEndStruct() {
    Do();
}

void TSameActionDataVisitor::OnBeginDict() {
    Do();
}

void TSameActionDataVisitor::OnBeforeDictItem() {
    Do();
}

void TSameActionDataVisitor::OnBeforeDictKey() {
    Do();
}

void TSameActionDataVisitor::OnAfterDictKey() {
    Do();
}

void TSameActionDataVisitor::OnBeforeDictPayload() {
    Do();
}

void TSameActionDataVisitor::OnAfterDictPayload() {
    Do();
}

void TSameActionDataVisitor::OnAfterDictItem() {
    Do();
}

void TSameActionDataVisitor::OnEndDict() {
    Do();
}

void TSameActionDataVisitor::OnBeginVariant(ui32 index) {
    Y_UNUSED(index);
    Do();
}

void TSameActionDataVisitor::OnEndVariant() {
    Do();
}

void TSameActionDataVisitor::OnPg(TMaybe<TStringBuf> value, bool isUtf8) {
    Y_UNUSED(value);
    Y_UNUSED(isUtf8);
    Do();
}

void TThrowingDataVisitor::Do() {
    UNEXPECTED;
}

void TEmptyDataVisitor::Do() {
}

}
