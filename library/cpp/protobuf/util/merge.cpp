#include "merge.h"
#include "simple_reflection.h"

#include <google/protobuf/message.h>

#include <library/cpp/protobuf/util/proto/merge.pb.h>

namespace NProtoBuf {
    void RewriteMerge(const Message& src, Message& dst) {
        const Descriptor* d = src.GetDescriptor();
        Y_ASSERT(d == dst.GetDescriptor());

        for (int i = 0; i < d->field_count(); ++i) {
            if (TConstField(src, d->field(i)).Has())
                TMutableField(dst, d->field(i)).Clear();
        }

        dst.MergeFrom(src);
    }

    static void ClearNonMergeable(const Message& src, Message& dst) {
        const Descriptor* d = src.GetDescriptor();
        if (d->options().GetExtension(DontMerge)) {
            dst.Clear();
            return;
        }

        for (int i = 0; i < d->field_count(); ++i) {
            const FieldDescriptor* fd = d->field(i);
            TConstField srcField(src, fd);
            if (srcField.Has()) {
                TMutableField dstField(dst, fd);
                if (fd->options().GetExtension(DontMergeField))
                    dstField.Clear();
                else if (!fd->is_repeated() && dstField.IsMessage() && dstField.Has())
                    ClearNonMergeable(*srcField.Get<const Message*>(), *dstField.MutableMessage());
            }
        }
    }

    void CustomMerge(const Message& src, Message& dst) {
        ClearNonMergeable(src, dst);
        dst.MergeFrom(src);
    }

}
