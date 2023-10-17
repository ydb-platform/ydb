#include "skiff.h"
#include "skiff_validator.h"

#include <vector>
#include <stack>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

struct IValidatorNode;

using TValidatorNodeList = std::vector<std::shared_ptr<IValidatorNode>>;
using TSkiffSchemaList = std::vector<std::shared_ptr<TSkiffSchema>>;

static std::shared_ptr<IValidatorNode> CreateUsageValidatorNode(const std::shared_ptr<TSkiffSchema>& skiffSchema);
static TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
inline void ThrowUnexpectedParseWrite(T wireType)
{
    ythrow TSkiffException() << "Unexpected parse/write of \"" << ::ToString(wireType) << "\" token";
}

////////////////////////////////////////////////////////////////////////////////

struct IValidatorNode
{
    virtual ~IValidatorNode() = default;

    virtual void OnBegin(TValidatorNodeStack* /*validatorNodeStack*/)
    { }

    virtual void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/)
    {
        Y_ABORT();
    }

    virtual void OnSimpleType(TValidatorNodeStack* /*validatorNodeStack*/, EWireType wireType)
    {
        ThrowUnexpectedParseWrite(wireType);
    }

    virtual void BeforeVariant8Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant8);
    }

    virtual void OnVariant8Tag(TValidatorNodeStack* /*validatorNodeStack*/, ui8 /*tag*/)
    {
        IValidatorNode::BeforeVariant8Tag();
    }

    virtual void BeforeVariant16Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant16);
    }

    virtual void OnVariant16Tag(TValidatorNodeStack* /*validatorNodeStack*/, ui16 /*tag*/)
    {
        IValidatorNode::BeforeVariant16Tag();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TValidatorNodeStack
{
public:
    explicit TValidatorNodeStack(std::shared_ptr<IValidatorNode> validator)
        : RootValidator_(std::move(validator))
    { }

    void PushValidator(IValidatorNode* validator)
    {
        ValidatorStack_.push(validator);
        validator->OnBegin(this);
    }

    void PopValidator()
    {
        Y_ABORT_UNLESS(!ValidatorStack_.empty());
        ValidatorStack_.pop();
        if (!ValidatorStack_.empty()) {
            ValidatorStack_.top()->OnChildDone(this);
        }
    }

    void PushRootIfRequired()
    {
        if (ValidatorStack_.empty()) {
            PushValidator(RootValidator_.get());
        }
    }

    IValidatorNode* Top() const
    {
        Y_ABORT_UNLESS(!ValidatorStack_.empty());
        return ValidatorStack_.top();
    }

    bool IsFinished() const
    {
        return ValidatorStack_.empty();
    }

private:
    const std::shared_ptr<IValidatorNode> RootValidator_;
    std::stack<IValidatorNode*> ValidatorStack_;
};

////////////////////////////////////////////////////////////////////////////////

class TNothingTypeValidator
    : public IValidatorNode
{
public:
    void OnBegin(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TSimpleTypeUsageValidator(EWireType type)
        : Type_(type)
    { }

    void OnSimpleType(TValidatorNodeStack* validatorNodeStack, EWireType type) override
    {
        if (type != Type_) {
            ThrowUnexpectedParseWrite(type);
        }
        validatorNodeStack->PopValidator();
    }

private:
    const EWireType Type_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
void ValidateVariantTag(TValidatorNodeStack* validatorNodeStack, TTag tag, const TValidatorNodeList& children)
{
    if (tag == EndOfSequenceTag<TTag>()) {
        // Root validator is pushed into the stack before variant tag
        // if the stack is empty.
        validatorNodeStack->PopValidator();
    } else if (tag >= children.size()) {
        ythrow TSkiffException() << "Variant tag \"" << tag << "\" "
            << "exceeds number of children \"" << children.size();
    } else {
        validatorNodeStack->PushValidator(children[tag].get());
    }
}

class TVariant8TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TVariant8TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void BeforeVariant8Tag() override
    { }

    void OnVariant8Tag(TValidatorNodeStack* validatorNodeStack, ui8 tag) override
    {
        ValidateVariantTag(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void BeforeVariant16Tag() override
    { }

    void OnVariant16Tag(TValidatorNodeStack* validatorNodeStack, ui16 tag) override
    {
        ValidateVariantTag(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedVariant8TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedVariant8TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void BeforeVariant8Tag() override
    { }

    void OnVariant8Tag(TValidatorNodeStack* validatorNodeStack, ui8 tag) override
    {
        ValidateVariantTag(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/) override
    { }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void BeforeVariant16Tag() override
    { }

    void OnVariant16Tag(TValidatorNodeStack* validatorNodeStack, ui16 tag) override
    {
        ValidateVariantTag(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/) override
    { }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleTypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TTupleTypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void OnBegin(TValidatorNodeStack* validatorNodeStack) override
    {
        Position_ = 0;
        if (!Children_.empty()) {
            validatorNodeStack->PushValidator(Children_[0].get());
        }
    }

    void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        Position_++;
        if (Position_ < Children_.size()) {
            validatorNodeStack->PushValidator(Children_[Position_].get());
        } else {
            validatorNodeStack->PopValidator();
        }
    }

private:
    const TValidatorNodeList Children_;
    ui32 Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSkiffValidator::TSkiffValidator(std::shared_ptr<TSkiffSchema> skiffSchema)
    : Context_(std::make_unique<TValidatorNodeStack>(CreateUsageValidatorNode(std::move(skiffSchema))))
{ }

TSkiffValidator::~TSkiffValidator()
{ }

void TSkiffValidator::BeforeVariant8Tag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariant8Tag();
}

void TSkiffValidator::OnVariant8Tag(ui8 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariant8Tag(Context_.get(), tag);
}

void TSkiffValidator::BeforeVariant16Tag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariant16Tag();
}

void TSkiffValidator::OnVariant16Tag(ui16 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariant16Tag(Context_.get(), tag);
}

void TSkiffValidator::OnSimpleType(EWireType value)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnSimpleType(Context_.get(), value);
}

void TSkiffValidator::ValidateFinished()
{
    if (!Context_->IsFinished()) {
        ythrow TSkiffException() << "Parse/write is not finished";
    }
}

////////////////////////////////////////////////////////////////////////////////

TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList)
{
    TValidatorNodeList result;
    result.reserve(skiffSchemaList.size());
    for (const auto& skiffSchema : skiffSchemaList) {
        result.push_back(CreateUsageValidatorNode(skiffSchema));
    }
    return result;
}

std::shared_ptr<IValidatorNode> CreateUsageValidatorNode(const std::shared_ptr<TSkiffSchema>& skiffSchema)
{
    switch (skiffSchema->GetWireType()) {
        case EWireType::Int8:
        case EWireType::Int16:
        case EWireType::Int32:
        case EWireType::Int64:
        case EWireType::Int128:

        case EWireType::Uint8:
        case EWireType::Uint16:
        case EWireType::Uint32:
        case EWireType::Uint64:
        case EWireType::Uint128:

        case EWireType::Double:
        case EWireType::Boolean:
        case EWireType::String32:
        case EWireType::Yson32:
            return std::make_shared<TSimpleTypeUsageValidator>(skiffSchema->GetWireType());
        case EWireType::Nothing:
            return std::make_shared<TNothingTypeValidator>();
        case EWireType::Tuple:
            return std::make_shared<TTupleTypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::Variant8:
            return std::make_shared<TVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::Variant16:
            return std::make_shared<TVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant8:
            return std::make_shared<TRepeatedVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant16:
            return std::make_shared<TRepeatedVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
    }
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
