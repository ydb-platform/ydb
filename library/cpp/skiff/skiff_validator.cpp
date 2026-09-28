#include "skiff.h"
#include "skiff_validator.h"

#include <stack>
#include <utility>
#include <vector>

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

    virtual void OnStringFixed(TValidatorNodeStack* /*validatorNodeStack*/, i64 /*size*/)
    {
        ThrowUnexpectedParseWrite(EWireType::StringFixed);
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

    virtual void BeforeVariantVarTag()
    {
        ThrowUnexpectedParseWrite(EWireType::VariantVar);
    }

    virtual void OnVariantVarTag(TValidatorNodeStack* /*validatorNodeStack*/, i32 /*tag*/)
    {
        IValidatorNode::BeforeVariantVarTag();
    }

    virtual void BeforeBlockVarHeader()
    {
        ThrowUnexpectedParseWrite(EWireType::RepeatedBlockVar);
    }

    virtual void OnBlockVarHeader(TValidatorNodeStack* /*validatorNodeStack*/, const TBlockVarHeader& /*blockVarHeader*/)
    {
        IValidatorNode::BeforeBlockVarHeader();
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
        if (ValidatorStack_.empty()) {
            ythrow TSkiffException() << "Unexpected parse/write";
        }
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

class TStringFixedValidator
    : public IValidatorNode
{
public:
    explicit TStringFixedValidator(i64 size)
        : Size_(size)
    { }

    void OnStringFixed(TValidatorNodeStack* validatorNodeStack, i64 size) override
    {
        if (size != Size_) {
            ythrow TSkiffException() << "\"" << ToString(EWireType::StringFixed) << "\" size mismatch: expected " << Size_ << ", actual " << size;
        }
        validatorNodeStack->PopValidator();
    }

private:
    const i64 Size_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TTag>
void PushVariantChild(TValidatorNodeStack* validatorNodeStack, TTag tag, const TValidatorNodeList& children)
{
    if (!std::in_range<size_t>(tag) || static_cast<size_t>(tag) >= children.size()) {
        ythrow TSkiffException() << "Variant tag \"" << tag << "\" "
            << "is out of range [0, " << children.size() << ")";
    }
    validatorNodeStack->PushValidator(children[static_cast<size_t>(tag)].get());
}

template <typename TTag>
void ValidateRepeatedVariantTag(TValidatorNodeStack* validatorNodeStack, TTag tag, const TValidatorNodeList& children)
{
    if (tag == EndOfSequenceTag<TTag>()) {
        // Root validator is pushed into the stack before variant tag
        // if the stack is empty.
        validatorNodeStack->PopValidator();
        return;
    }
    PushVariantChild(validatorNodeStack, tag, children);
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
        PushVariantChild(validatorNodeStack, tag, Children_);
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
        PushVariantChild(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TVariantVarValidator
    : public IValidatorNode
{
public:
    explicit TVariantVarValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    void BeforeVariantVarTag() override
    { }

    void OnVariantVarTag(TValidatorNodeStack* validatorNodeStack, i32 tag) override
    {
        PushVariantChild(validatorNodeStack, tag, Children_);
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
        ValidateRepeatedVariantTag(validatorNodeStack, tag, Children_);
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
        ValidateRepeatedVariantTag(validatorNodeStack, tag, Children_);
    }

    void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/) override
    { }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedBlockVarValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedBlockVarValidator(std::shared_ptr<IValidatorNode> child)
        : Child_(std::move(child))
        , IsChildNothing_(dynamic_cast<TNothingTypeValidator*>(Child_.get()) != nullptr)
    { }

    void BeforeBlockVarHeader() override
    { }

    void OnBlockVarHeader(TValidatorNodeStack* validatorNodeStack, const TBlockVarHeader& blockVarHeader) override
    {
        if (blockVarHeader.Count < 0) {
            ythrow TSkiffException() << "Block count must be nonnegative, got " << blockVarHeader.Count;
        }
        if (blockVarHeader.ByteSize && *blockVarHeader.ByteSize < 0) {
            ythrow TSkiffException() << "Block byte size must be nonnegative, got " << *blockVarHeader.ByteSize;
        }
        if (blockVarHeader.Count == 0 && blockVarHeader.ByteSize) {
            ythrow TSkiffException() << "Block with zero count must not have byte size";
        }

        if (blockVarHeader.Count == 0) {
            validatorNodeStack->PopValidator();
            return;
        }

        Count_ = blockVarHeader.Count;
        if (IsChildNothing_) {
            Current_ = Count_;
        } else {
            Current_ = 0;
            validatorNodeStack->PushValidator(Child_.get());
        }
    }

    void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        ++Current_;
        if (Current_ < Count_) {
            validatorNodeStack->PushValidator(Child_.get());
        }
    }

private:
    const std::shared_ptr<IValidatorNode> Child_;
    const bool IsChildNothing_;

    i64 Count_ = 0;
    i64 Current_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleTypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TTupleTypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    {
        Y_ABORT_IF(Children_.empty());
    }

    void OnBegin(TValidatorNodeStack* validatorNodeStack) override
    {
        Position_ = 0;
        validatorNodeStack->PushValidator(Children_[0].get());
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

void TSkiffValidator::BeforeVariantVarTag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariantVarTag();
}

void TSkiffValidator::OnVariantVarTag(i32 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariantVarTag(Context_.get(), tag);
}

void TSkiffValidator::BeforeBlockVarHeader()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeBlockVarHeader();
}

void TSkiffValidator::OnBlockVarHeader(const TBlockVarHeader& blockVarHeader)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnBlockVarHeader(Context_.get(), blockVarHeader);
}

void TSkiffValidator::OnSimpleType(EWireType value)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnSimpleType(Context_.get(), value);
}

void TSkiffValidator::OnStringFixed(i64 size)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnStringFixed(Context_.get(), size);
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
        case EWireType::Boolean:
        case EWireType::Int8:
        case EWireType::Int16:
        case EWireType::Int32:
        case EWireType::Int64:
        case EWireType::Int128:
        case EWireType::Int256:
        case EWireType::VarInt32:
        case EWireType::VarInt64:
        case EWireType::Uint8:
        case EWireType::Uint16:
        case EWireType::Uint32:
        case EWireType::Uint64:
        case EWireType::Uint128:
        case EWireType::Uint256:
        case EWireType::Float:
        case EWireType::Double:
        case EWireType::String32:
        case EWireType::StringVar:
        case EWireType::Yson32:
            return std::make_shared<TSimpleTypeUsageValidator>(skiffSchema->GetWireType());
        case EWireType::Nothing:
            return std::make_shared<TNothingTypeValidator>();
        case EWireType::StringFixed:
            return std::make_shared<TStringFixedValidator>(skiffSchema->GetSize());
        case EWireType::Tuple: {
            auto children = CreateUsageValidatorNodeList(skiffSchema->GetChildren());
            TValidatorNodeList nonNothingChildren;
            nonNothingChildren.reserve(children.size());
            for (auto& child : children) {
                if (!std::dynamic_pointer_cast<TNothingTypeValidator>(child)) {
                    nonNothingChildren.push_back(std::move(child));
                }
            }
            if (nonNothingChildren.empty()) {
                return std::make_shared<TNothingTypeValidator>();
            }
            return std::make_shared<TTupleTypeUsageValidator>(std::move(nonNothingChildren));
        }
        case EWireType::Variant8:
            return std::make_shared<TVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::Variant16:
            return std::make_shared<TVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::VariantVar:
            return std::make_shared<TVariantVarValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant8:
            return std::make_shared<TRepeatedVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant16:
            return std::make_shared<TRepeatedVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedBlockVar: {
            const auto& children = skiffSchema->GetChildren();
            Y_ABORT_UNLESS(children.size() == 1);
            return std::make_shared<TRepeatedBlockVarValidator>(CreateUsageValidatorNode(children[0]));
        }
    }
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
