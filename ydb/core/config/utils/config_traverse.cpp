#include "config_traverse.h"

#include <contrib/libs/protobuf/src/google/protobuf/descriptor.h>

#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/compiler.h>

namespace NKikimr::NConfig {

ssize_t FindLoop(TDeque<const Descriptor*>& typePath, const Descriptor* child) {
    for (ssize_t i = 0; i < (ssize_t)typePath.size(); ++i) {
        if (typePath[i] == child) {
            return i;
        }
    }
    return -1;
}

void Traverse(const Descriptor* d, TDeque<const Descriptor*>& typePath, TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, TOnEntryFn onEntry) {
    ssize_t loop = FindLoop(typePath, d);

    Y_ABORT_IF(!d && !field, "Either field or descriptor must be defined");

    onEntry(d, typePath, fieldPath, field, loop);

    if (!d || loop != -1) {
        return;
    }

    typePath.push_back(d);

    for (int i = 0; i < d->field_count(); ++i) {
        const FieldDescriptor* fieldDescriptor = d->field(i);
        fieldPath.push_back(fieldDescriptor);
        Traverse(fieldDescriptor->message_type(), typePath, fieldPath, fieldDescriptor, onEntry);
        fieldPath.pop_back();
    }

    typePath.pop_back();
}

void Traverse(TOnEntryFn onEntry, const Descriptor* descriptor) {
    TDeque<const Descriptor*> typePath;
    TDeque<const FieldDescriptor*> fieldPath;
    fieldPath.push_back(nullptr);
    Traverse(descriptor, typePath, fieldPath, nullptr, onEntry);
    fieldPath.pop_back();
}

} // namespace NKikimr::NConfig
