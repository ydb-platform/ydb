#include "validator_checks.h"

#include "validator_builder.h"
#include "validator.h"

#include <util/string/cast.h>

namespace NKikimr::NYamlConfig::NValidator {

TNodeWrapper::TNodeWrapper(
    TCheckContext& context,
    NFyaml::TNodeRef node,
    TValidator* validator,
    TMaybe<ENodeType> nodeType,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator)
    , NodeType_(nodeType) {}

TGenericNodeWrapper TNodeWrapper::Generic() {
    // opaque nodes doesn't have validators and generic node type doesn't exist
    Y_ASSERT(!IsOpaqueChild());

    TGenericValidator* validator = nullptr;

    if (Node_) {
        Y_ASSERT(NodeType_ == ENodeType::Generic);

        validator = static_cast<TGenericValidator*>(Validator_);
    }

    return TGenericNodeWrapper(
        static_cast<TGenericCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TMapNodeWrapper TNodeWrapper::Map() {
    TMapValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Map);
        } else {
            if (Node_.Type() != NFyaml::ENodeType::Mapping) {
                throw TCheckException() << PathFromCheckNode_ << ": Must be a map";
            }
        }

        validator = static_cast<TMapValidator*>(Validator_);
    }
    
    return TMapNodeWrapper(
        static_cast<TMapCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TArrayNodeWrapper TNodeWrapper::Array() {
    TArrayValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Array);
        } else {
            if (Node_.Type() != NFyaml::ENodeType::Sequence) {
                throw TCheckException() << PathFromCheckNode_ << ": Must be an array";
            }
        }

        validator = static_cast<TArrayValidator*>(Validator_);
    }
    
    return TArrayNodeWrapper(
        static_cast<TArrayCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

namespace {
    void ThrowIfScalarNodeValidationIsNotOk(TValidationResult validationResult, TString pathFromCheckNode) {
        if (!validationResult.Ok()) {
            auto e = TCheckException() << pathFromCheckNode << ": " << validationResult.Issues[0].Problem;
            for (int i = 1; i < validationResult.Issues.ysize(); ++i) {
                e << ", " << validationResult.Issues[i].Problem;
            }

            throw e;
        }
    }
}

TInt64NodeWrapper TNodeWrapper::Int64() {
    TInt64Validator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Int64);
        } else {
            ThrowIfScalarNodeValidationIsNotOk(
                TInt64Builder().CreateValidator().Validate(Node_),
                PathFromCheckNode_);
        }

        validator = static_cast<TInt64Validator*>(Validator_);
    }
    
    return TInt64NodeWrapper(
        static_cast<TInt64CheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TStringNodeWrapper TNodeWrapper::String() {
    TStringValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::String);
        } else {
            ThrowIfScalarNodeValidationIsNotOk(
                TStringBuilder().CreateValidator().Validate(Node_),
                PathFromCheckNode_);
        }

        validator = static_cast<TStringValidator*>(Validator_);
    }
    
    return TStringNodeWrapper(
        static_cast<TStringCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TBoolNodeWrapper TNodeWrapper::Bool() {
    TBoolValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Bool);
        } else {
            ThrowIfScalarNodeValidationIsNotOk(
                TBoolBuilder().CreateValidator().Validate(Node_),
                PathFromCheckNode_);
        }

        validator = static_cast<TBoolValidator*>(Validator_);
    }
    
    return TBoolNodeWrapper(
        static_cast<TBoolCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TEnumNodeWrapper TNodeWrapper::Enum() {
    // opaque enum is just a string
    Y_ASSERT(!IsOpaqueChild());

    TEnumValidator* validator = nullptr;

    if (Node_) {
        Y_ASSERT(NodeType_ == ENodeType::Enum);

        validator = static_cast<TEnumValidator*>(Validator_);
    }

    return TEnumNodeWrapper(
        static_cast<TEnumCheckContext&>(Context_),
        Node_,
        validator,
        PathFromCheckNode_);
}

TString TNodeWrapper::Scalar() {
    if (!IsScalar()) {
        throw TCheckException() << "Node " + Node_.Path() + " must be a scalar";
    }
    return Node_.Scalar();
}

TMaybe<ENodeType> TNodeWrapper::ValidatorType() {
    ThrowIfNullNode();
    return NodeType_;
}

bool TNodeWrapper::IsMap() {
    ThrowIfNullNode();
    return Node_.Type() == NFyaml::ENodeType::Mapping;
}

bool TNodeWrapper::IsArray() {
    ThrowIfNullNode();
    return Node_.Type() == NFyaml::ENodeType::Sequence;
}

bool TNodeWrapper::IsInt64() {
    ThrowIfNullNode();
    return TInt64Builder().CreateValidator().Validate(Node_).Ok();
}

bool TNodeWrapper::IsString() {
    ThrowIfNullNode();
    return TStringBuilder().CreateValidator().Validate(Node_).Ok();
}

bool TNodeWrapper::IsBool() {
    ThrowIfNullNode();
    return TBoolBuilder().CreateValidator().Validate(Node_).Ok();
}

bool TNodeWrapper::IsScalar() {
    return IsString();
}


TGenericNodeWrapper::TGenericNodeWrapper(
    TGenericCheckContext& context,
    NFyaml::TNodeRef node,
    TGenericValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {}

TGenericNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Generic, PathFromCheckNode_);
}


TMapNodeWrapper::TMapNodeWrapper(
    TMapCheckContext& context,
    NFyaml::TNodeRef node,
    TMapValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {
        if (Node_) {
            Y_ASSERT(node.Type() == NFyaml::ENodeType::Mapping);
        }
    }

TNodeWrapper TMapNodeWrapper::operator[](const TString& field) {
    NFyaml::TNodeRef node(nullptr);
    TValidator* validator = nullptr;
    TMaybe<ENodeType> nodeType;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(Validator_);
            if (Validator_->Children_.contains(field)) {
                validator = Validator_->Children_.at(field).Get();
                nodeType = validator->NodeType_;
            }
        }

        if (Map().Has(field)) {
            node = Map().at(field);
        }
    }

    return TNodeWrapper(
        Context_,
        node,
        validator,
        nodeType,
        PathFromCheckNode_ + "/" + field);
}

TNodeWrapper TMapNodeWrapper::At(const TString& field) {
    ThrowIfNullNode();

    if (!Map().Has(field)) {
        TString message = "Node \"" + PathFromCheckNode_ + "\" is not presented";
        throw TCheckException() << message;
    }

    TValidator* validator = nullptr;
    TMaybe<ENodeType> nodeType;

    if (!IsOpaqueChild()) {
        Y_ASSERT(Validator_);
        if (Validator_->Children_.contains(field)) {
            validator = Validator_->Children_[field].Get();
            nodeType = validator->NodeType_;
        }
    }

    return TNodeWrapper(
        Context_,
        Map()[field],
        validator,
        nodeType,
        PathFromCheckNode_ + "/" + field);
}

bool TMapNodeWrapper::Has(const TString& field) {
    return Node_ && Map().Has(field);
}

NFyaml::TMapping TMapNodeWrapper::Map() {
    return Node_.Map();
}

TMapNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Map, PathFromCheckNode_);
}


TArrayNodeWrapper::TArrayNodeWrapper(
    TArrayCheckContext& context,
    NFyaml::TNodeRef node,
    TArrayValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {
        Y_ASSERT(node.Type() == NFyaml::ENodeType::Sequence);
    }

int TArrayNodeWrapper::Length() {
    ThrowIfNullNode();
    return Sequence().size();
}

TNodeWrapper TArrayNodeWrapper::operator[](size_t index) {
    NFyaml::TNodeRef node(nullptr);
    TValidator* validator = nullptr;
    TMaybe<ENodeType> nodeType;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(Validator_);
            if (Validator_->ItemValidatorPtr_) {
                validator = Validator_->ItemValidatorPtr_.Get();
                nodeType = validator->NodeType_;
            }
        }

        if (0 <= index && index < Sequence().size()) {
            node = Sequence()[index];
        }
    }

    return TNodeWrapper(
        Context_,
        node,
        validator,
        nodeType,
        PathFromCheckNode_ + "/" + ToString(index));
}

NFyaml::TSequence TArrayNodeWrapper::Sequence() {
    return Node_.Sequence();
}

TArrayNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Array, PathFromCheckNode_);
}


TInt64NodeWrapper::TInt64NodeWrapper(
    TInt64CheckContext& context,
    NFyaml::TNodeRef node,
    TInt64Validator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {}

i64 TInt64NodeWrapper::Value() const {
    ThrowIfNullNode();
    return FromString<i64>(Node_.Scalar());
}

TInt64NodeWrapper::operator i64() const {
    return Value();
}

TInt64NodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Int64, PathFromCheckNode_);
}


TStringNodeWrapper::TStringNodeWrapper(
    TStringCheckContext& context,
    NFyaml::TNodeRef node,
    TStringValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {}

TString TStringNodeWrapper::Value() const {
    ThrowIfNullNode();
    return Node_.Scalar();
}

TStringNodeWrapper::operator TString() const {
    return Value();
}

TStringNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::String, PathFromCheckNode_);
}


TBoolNodeWrapper::TBoolNodeWrapper(
    TBoolCheckContext& context,
    NFyaml::TNodeRef node,
    TBoolValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {}

bool TBoolNodeWrapper::Value() const {
    ThrowIfNullNode();
    TString scalar = Node_.Scalar();
    scalar.to_lower();
    return scalar == "true";
}

TBoolNodeWrapper::operator bool() const {
    return Value();
}

TBoolNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Bool, PathFromCheckNode_);
}


TEnumNodeWrapper::TEnumNodeWrapper(
    TEnumCheckContext& context,
    NFyaml::TNodeRef node,
    TEnumValidator* validator,
    const TString& pathFromCheckNode)
    : TBase(context, node, pathFromCheckNode)
    , Validator_(validator) {}

TString TEnumNodeWrapper::Value() const {
    ThrowIfNullNode();
    return Node_.Scalar();
}

TEnumNodeWrapper::operator TString() const {
    return Value();
}

TEnumNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Bool, PathFromCheckNode_);
}


TCheckContext::TCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath)
    : Node_(node)
    , CheckNodePath_(checkNodePath) {}

void TCheckContext::Expect(bool condition, TString error) {
    if (!condition) {
        AddError(error);
        someErrorOccured_ = true;
    }
}

void TCheckContext::Expect(bool condition) {
    someErrorOccured_ |= !condition;
}

void TCheckContext::AddError(TString error) {
    Errors_.emplace_back(std::move(error));
}

void TCheckContext::Fail() {
    throw TFailException();
}

void TCheckContext::Fail(TString error) {
    AddError(error);
    Fail();
}

void TCheckContext::Assert(bool condition) {
    if (!condition) {
        Fail();
    }
}

void TCheckContext::Assert(bool condition, TString error) {
    if (!condition) {
        Fail(error);
    }
}

TGenericCheckContext::TGenericCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TGenericValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TGenericNodeWrapper TGenericCheckContext::Node() {
    return TGenericNodeWrapper(*this, Node_, Validator_, CheckNodePath_);
}

TMapCheckContext::TMapCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TMapValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TMapNodeWrapper TMapCheckContext::Node() {
    return TMapNodeWrapper(*this, Node_.Map(), Validator_, CheckNodePath_);
}

TArrayCheckContext::TArrayCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TArrayValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TArrayNodeWrapper TArrayCheckContext::Node() {
    return TArrayNodeWrapper(*this, Node_.Sequence(), Validator_, CheckNodePath_);
}

TInt64CheckContext::TInt64CheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TInt64Validator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TInt64NodeWrapper TInt64CheckContext::Node() {
    return TInt64NodeWrapper(*this, Node_, Validator_, CheckNodePath_);
}

TStringCheckContext::TStringCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TStringValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TStringNodeWrapper TStringCheckContext::Node() {
    return TStringNodeWrapper(*this, Node_, Validator_, CheckNodePath_);
}

TBoolCheckContext::TBoolCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TBoolValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TBoolNodeWrapper TBoolCheckContext::Node() {
    return TBoolNodeWrapper(*this, Node_, Validator_, CheckNodePath_);
}

TEnumCheckContext::TEnumCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TEnumValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TEnumNodeWrapper TEnumCheckContext::Node() {
    return TEnumNodeWrapper(*this, Node_, Validator_, CheckNodePath_);
}

} // namespace NKikimr::NYamlConfig::NValidator
