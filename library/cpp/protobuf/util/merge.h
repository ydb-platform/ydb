#pragma once

namespace google {
    namespace protobuf {
        class Message;
    }
}

namespace NProtoBuf {
    using Message = ::google::protobuf::Message;
}

namespace NProtoBuf {
    // Similiar to Message::MergeFrom, overwrites existing repeated fields
    // and embedded messages completely instead of recursive merging.
    void RewriteMerge(const Message& src, Message& dst);

    // Does standard MergeFrom() by default, except messages/fields marked with DontMerge or DontMergeField option.
    // Such fields are merged using RewriteMerge() (i.e. destination is cleared before merging anything from source)
    void CustomMerge(const Message& src, Message& dst);

}
