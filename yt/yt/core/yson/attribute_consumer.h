#pragma once

#include "public.h"
#include "async_consumer.h"

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Consumes a map fragment representing the attributes
//! and if the fragment is non-empty then encloses it with angle brackets.
class TAttributeFragmentConsumer
    : public IAsyncYsonConsumer
{
public:
    explicit TAttributeFragmentConsumer(IAsyncYsonConsumer* underlyingConsumer);
    ~TAttributeFragmentConsumer();

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    using IAsyncYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type) override;
    void OnRaw(TFuture<TYsonString> asyncStr) override;

    void Finish();

private:
    IAsyncYsonConsumer* const UnderlyingConsumer_;
    bool HasAttributes_ = false;
    bool Finished_ = false;

    void Start();
};

////////////////////////////////////////////////////////////////////////////////

//! Consumes an attribute value and if it is non-empty then prepends it with
//! the attribute key.
class TAttributeValueConsumer
    : public IAsyncYsonConsumer
{
public:
    TAttributeValueConsumer(
        IAsyncYsonConsumer* underlyingConsumer,
        NYTree::IAttributeDictionary::TKey key);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type) override;
    void OnRaw(TFuture<TYsonString> asyncStr) override;

private:
    IAsyncYsonConsumer* const UnderlyingConsumer_;
    const NYTree::IAttributeDictionary::TKey Key_;
    bool Empty_ = true;

    void ProduceKeyIfNeeded();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

