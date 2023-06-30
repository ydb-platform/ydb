#pragma once

#include "consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Consumer that does nothing.
class TNullYsonConsumer
    : public virtual IYsonConsumer
{
    void OnStringScalar(TStringBuf /*value*/) override;

    void OnInt64Scalar(i64 /*value*/) override;

    void OnUint64Scalar(ui64 /*value*/) override;

    void OnDoubleScalar(double /*value*/) override;

    void OnBooleanScalar(bool /*value*/) override;

    void OnEntity() override;

    void OnBeginList() override;

    void OnListItem() override;

    void OnEndList() override;

    void OnBeginMap() override;

    void OnKeyedItem(TStringBuf /*name*/) override;

    void OnEndMap() override;

    void OnBeginAttributes() override;

    void OnEndAttributes() override;

    void OnRaw(TStringBuf /*yson*/, EYsonType /*type*/) override;
};

////////////////////////////////////////////////////////////////////////////////

//! Returns the singleton instance of the null consumer.
IYsonConsumer* GetNullYsonConsumer();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
