#pragma once

#include <util/generic/string.h>

namespace google {
    namespace protobuf {
        class FieldDescriptor;
        class Descriptor;
    }
}

namespace NProtobufJson {
    /*  Methods OnEnter.../OnLeave... are called on every field of structure
     *  during traverse and should be used to build context
     *  Method OnUnknownField are called every time when field which can't
     *  be mapped
     */
    struct IUnknownFieldsCollector {
        virtual ~IUnknownFieldsCollector() = default;

        virtual void OnEnterMapItem(const TString& key) = 0;
        virtual void OnLeaveMapItem() = 0;

        virtual void OnEnterArrayItem(ui64 id) = 0;
        virtual void OnLeaveArrayItem() = 0;

        virtual void OnUnknownField(const TString& key, const google::protobuf::Descriptor& value) = 0;
    };
}
