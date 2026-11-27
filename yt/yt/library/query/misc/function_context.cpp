#include "function_context.h"

#include "objects_holder.h"

#include <library/cpp/yt/assert/assert.h>

#include <vector>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionContext::TImpl
    : public TObjectsHolder
{
public:
    explicit TImpl(std::unique_ptr<bool[]> literalArgs)
        : LiteralArgs_(std::move(literalArgs))
    { }

    void* GetPrivateData() const
    {
        return PrivateData_;
    }

    void SetPrivateData(void* privateData)
    {
        PrivateData_ = privateData;
    }

    bool IsLiteralArg(int argIndex) const
    {
        YT_ASSERT(argIndex >= 0);
        return LiteralArgs_[argIndex];
    }

private:
    const std::unique_ptr<bool[]> LiteralArgs_;

    void* PrivateData_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

TFunctionContext::TFunctionContext(std::unique_ptr<bool[]> literalArgs)
    : Impl_(std::make_unique<TImpl>(std::move(literalArgs)))
{ }

TFunctionContext::~TFunctionContext() = default;

void* TFunctionContext::CreateUntypedObject(void* pointer, void(*deleter)(void*))
{
    return Impl_->Register(pointer, deleter);
}

void* TFunctionContext::GetPrivateData() const
{
    return Impl_->GetPrivateData();
}

void TFunctionContext::SetPrivateData(void* data)
{
    Impl_->SetPrivateData(data);
}

bool TFunctionContext::IsLiteralArg(int argIndex) const
{
    return Impl_->IsLiteralArg(argIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

