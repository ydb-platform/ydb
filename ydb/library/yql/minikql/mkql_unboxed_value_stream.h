#pragma once
#include <ydb/library/yql/public/udf/udf_value.h>

namespace NKikimr {
namespace NMiniKQL {

struct TUnboxedValueStream : public IOutputStream {
    NUdf::TUnboxedValue Value_;

    TUnboxedValueStream();

    NUdf::TUnboxedValuePod Value();

    void DoWrite(const void* buf, size_t len) override;
};

}
}
