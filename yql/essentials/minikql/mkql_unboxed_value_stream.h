#pragma once
#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NMiniKQL {

class TUnboxedValueStream: public IOutputStream {
public:
    TUnboxedValueStream();

    NUdf::TUnboxedValuePod Value();

    void DoWrite(const void* buf, size_t len) override;

private:
    NUdf::TUnboxedValue Value_;
};

} // namespace NKikimr::NMiniKQL
