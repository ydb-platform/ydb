#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

namespace NProtobufJson {
    // Functor that defines whether given field has some option set to true
    //
    // Example:
    // message T {
    //     optional stroka some_field = 1 [(some_option) = true];
    // }
    //
    template <typename TFieldOptionExtensionId>
    class TFieldOptionFunctor {
    public:
        TFieldOptionFunctor(const TFieldOptionExtensionId& option, bool positive = true)
            : Option(option)
            , Positive(positive)
        {
        }

        bool operator()(const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor* field) const {
            const NProtoBuf::FieldOptions& opt = field->options();
            const bool val = opt.GetExtension(Option);
            return Positive ? val : !val;
        }

    private:
        const TFieldOptionExtensionId& Option;
        bool Positive;
    };

    template <typename TFieldOptionExtensionId>
    TFieldOptionFunctor<TFieldOptionExtensionId> MakeFieldOptionFunctor(const TFieldOptionExtensionId& option, bool positive = true) {
        return TFieldOptionFunctor<TFieldOptionExtensionId>(option, positive);
    }

}
