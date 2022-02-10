#pragma once

#include "event.h"

#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NYsonPull {
    class IConsumer {
    public:
        virtual ~IConsumer() = default;

        virtual void OnBeginStream() = 0;
        virtual void OnEndStream() = 0;

        virtual void OnBeginList() = 0;
        virtual void OnEndList() = 0;

        virtual void OnBeginMap() = 0;
        virtual void OnEndMap() = 0;

        virtual void OnBeginAttributes() = 0;
        virtual void OnEndAttributes() = 0;

        virtual void OnKey(TStringBuf name) = 0;

        virtual void OnEntity() = 0;
        virtual void OnScalarBoolean(bool value) = 0;
        virtual void OnScalarInt64(i64 value) = 0;
        virtual void OnScalarUInt64(ui64 value) = 0;
        virtual void OnScalarFloat64(double value) = 0;
        virtual void OnScalarString(TStringBuf value) = 0;

        virtual void OnScalar(const TScalar& value);
        virtual void OnEvent(const TEvent& value);
    };
}
