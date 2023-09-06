#include "validator_checks.h"

#include "validator_builder.h"
#include "validator.h"

#include <util/string/cast.h>

namespace NYamlConfig::NValidator {

TNodeWrapper::TNodeWrapper(
    TCheckContext* context,
    NFyaml::TNodeRef node,
    TValidator* validator,
    TMaybe<ENodeType> nodeType,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , NodeType_(nodeType)
    , PathFromCheckNode_(pathFromCheckNode) {}

TGenericNodeWrapper TNodeWrapper::Generic() {
    // opaque nodes doesn't have validators and generic node type doesn't exist
    Y_ASSERT(!IsOpaqueChild());

    TGenericCheckContext* context = nullptr;
    TGenericValidator* validator = nullptr;

    if (Node_) {
        Y_ASSERT(NodeType_ == ENodeType::Generic);

        context = static_cast<TGenericCheckContext*>(Context_);
        validator = static_cast<TGenericValidator*>(Validator_);
    }

    return TGenericNodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
}

TMapNodeWrapper TNodeWrapper::Map() {
    TMapCheckContext* context = nullptr;
    TMapValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Map);
        } else {
            if (Node_.Type() != NFyaml::ENodeType::Mapping) {
                throw yexception() << "Must be a map";
            }
        }

        context = static_cast<TMapCheckContext*>(Context_);
        validator = static_cast<TMapValidator*>(Validator_);
    }
    
    return TMapNodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
}

TArrayNodeWrapper TNodeWrapper::Array() {
    TArrayCheckContext* context = nullptr;
    TArrayValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Array);
        } else {
            if (Node_.Type() != NFyaml::ENodeType::Sequence) {
                throw yexception() << "Must be an array";
            }
        }

        context = static_cast<TArrayCheckContext*>(Context_);
        validator = static_cast<TArrayValidator*>(Validator_);
    }
    
    return TArrayNodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
}

TInt64NodeWrapper TNodeWrapper::Int64() {
    TInt64CheckContext* context = nullptr;
    TInt64Validator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Int64);
        } else {
            auto validationResult = TInt64Builder().CreateValidator().Validate(Node_);
            if (!validationResult.Ok()) {
                throw yexception() << validationResult;
            }
        }

        context = static_cast<TInt64CheckContext*>(Context_);
        validator = static_cast<TInt64Validator*>(Validator_);
    }
    
    return TInt64NodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
}

TStringNodeWrapper TNodeWrapper::String() {
    TStringCheckContext* context = nullptr;
    TStringValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::String);
        } else {
            auto validationResult = TStringBuilder().CreateValidator().Validate(Node_);
            if (!validationResult.Ok()) {
                throw yexception() << validationResult;
            }
        }

        context = static_cast<TStringCheckContext*>(Context_);
        validator = static_cast<TStringValidator*>(Validator_);
    }
    
    return TStringNodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
}

TBoolNodeWrapper TNodeWrapper::Bool() {
    TBoolCheckContext* context = nullptr;
    TBoolValidator* validator = nullptr;

    if (Node_) {
        if (!IsOpaqueChild()) {
            Y_ASSERT(NodeType_ == ENodeType::Bool);
        } else {
            auto validationResult = TBoolBuilder().CreateValidator().Validate(Node_);
            if (!validationResult.Ok()) {
                throw yexception() << validationResult;
            }
        }

        context = static_cast<TBoolCheckContext*>(Context_);
        validator = static_cast<TBoolValidator*>(Validator_);
    }
    
    return TBoolNodeWrapper(
        context,
        Node_,
        validator,
        PathFromCheckNode_);
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
    return IsInt64() || IsString() || IsBool();
}


TGenericNodeWrapper::TGenericNodeWrapper(
    TGenericCheckContext* context,
    NFyaml::TNodeRef node,
    TGenericValidator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {
        Y_UNUSED(Validator_);
        Y_UNUSED(Context_);
        Y_UNUSED(Node_);
    }

TGenericNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Generic, PathFromCheckNode_);
}


TMapNodeWrapper::TMapNodeWrapper(
    TMapCheckContext* context,
    NFyaml::TNodeRef node,
    TMapValidator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {
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
        TString nodePath = Context_->CheckNodePath_ + "/" + PathFromCheckNode_;
        TString message = "Node \"" + nodePath + "\" is not presented";
        throw yexception() << message;
    }

    TValidator* validator = nullptr;
    TMaybe<ENodeType> nodeType;

    if (!IsOpaqueChild()) {
        Y_ASSERT(Validator_);
        if (Validator_->Children_.contains(field)) {
            validator = Validator_->Children_.at(field).Get();
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
    TArrayCheckContext* context,
    NFyaml::TNodeRef node,
    TArrayValidator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {
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
    TInt64CheckContext* context,
    NFyaml::TNodeRef node,
    TInt64Validator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {}

i64 TInt64NodeWrapper::Value() {
    ThrowIfNullNode();
    return FromString<i64>(Node_.Scalar());
}

TInt64NodeWrapper::operator i64() {
    return Value();
}

TInt64NodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::Int64, PathFromCheckNode_);
}


TStringNodeWrapper::TStringNodeWrapper(
    TStringCheckContext* context,
    NFyaml::TNodeRef node,
    TStringValidator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {}

TString TStringNodeWrapper::Value() {
    ThrowIfNullNode();
    return Node_.Scalar();
}

TStringNodeWrapper::operator TString() {
    return Value();
}

TStringNodeWrapper::operator TNodeWrapper() {
    return TNodeWrapper(Context_, Node_, Validator_, ENodeType::String, PathFromCheckNode_);
}


TBoolNodeWrapper::TBoolNodeWrapper(
    TBoolCheckContext* context,
    NFyaml::TNodeRef node,
    TBoolValidator* validator,
    const TString& pathFromCheckNode)
    : Context_(context)
    , Node_(node)
    , Validator_(validator)
    , PathFromCheckNode_(pathFromCheckNode) {}

bool TBoolNodeWrapper::Value() {
    ThrowIfNullNode();
    TString scalar = Node_.Scalar();
    scalar.to_lower();
    return scalar == "true";
}

TBoolNodeWrapper::operator bool() {
    return Value();
}

TBoolNodeWrapper::operator TNodeWrapper() {
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
        someExpectFailed = true;
    }
}

void TCheckContext::Expect(bool condition) {
    someExpectFailed |= !condition;
}

void TCheckContext::AddError(TString error) {
    Errors_.emplace_back(std::move(error));
}

TGenericCheckContext::TGenericCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TGenericValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TGenericNodeWrapper TGenericCheckContext::Node() {
    return TGenericNodeWrapper(this, Node_, Validator_, CheckNodePath_);
}

TMapCheckContext::TMapCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TMapValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TMapNodeWrapper TMapCheckContext::Node() {
    return TMapNodeWrapper(this, Node_.Map(), Validator_, CheckNodePath_);
}

TArrayCheckContext::TArrayCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TArrayValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TArrayNodeWrapper TArrayCheckContext::Node() {
    return TArrayNodeWrapper(this, Node_.Sequence(), Validator_, CheckNodePath_);
}

TInt64CheckContext::TInt64CheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TInt64Validator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TInt64NodeWrapper TInt64CheckContext::Node() {
    return TInt64NodeWrapper(this, Node_, Validator_, CheckNodePath_);
}

TStringCheckContext::TStringCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TStringValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TStringNodeWrapper TStringCheckContext::Node() {
    return TStringNodeWrapper(this, Node_, Validator_, CheckNodePath_);
}

TBoolCheckContext::TBoolCheckContext(
    NFyaml::TNodeRef node,
    const TString& checkNodePath,
    TBoolValidator* validator)
    : TCheckContext(node, checkNodePath), Validator_(validator) {}

TBoolNodeWrapper TBoolCheckContext::Node() {
    return TBoolNodeWrapper(this, Node_, Validator_, CheckNodePath_);
}

}