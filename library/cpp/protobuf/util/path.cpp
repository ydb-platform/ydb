#include "path.h"

#include <util/generic/yexception.h>

namespace NProtoBuf {
    TFieldPath::TFieldPath() {
    }

    TFieldPath::TFieldPath(const Descriptor* msgType, const TStringBuf& path) {
        Init(msgType, path);
    }

    TFieldPath::TFieldPath(const TVector<const FieldDescriptor*>& path)
        : Path(path)
    {
    }

    bool TFieldPath::InitUnsafe(const Descriptor* msgType, TStringBuf path) {
        Path.clear();
        while (path) {
            TStringBuf next;
            while (!next && path)
                next = path.NextTok('/');
            if (!next)
                return true;

            if (!msgType) // need field but no message type
                return false;

            TString nextStr(next);
            const FieldDescriptor* field = msgType->FindFieldByName(nextStr);
            if (!field) {
                // Try to find extension field by FindAllExtensions()
                const DescriptorPool* pool = msgType->file()->pool();
                Y_ASSERT(pool); // never NULL by protobuf docs
                TVector<const FieldDescriptor*> extensions;
                pool->FindAllExtensions(msgType, &extensions); // find all extensions of this extendee
                for (const FieldDescriptor* ext : extensions) {
                    if (ext->full_name() == nextStr || ext->name() == nextStr) {
                        if (field)
                            return false; // ambiguity
                        field = ext;
                    }
                }
            }

            if (!field)
                return false;

            Path.push_back(field);
            msgType = field->type() == FieldDescriptor::TYPE_MESSAGE ? field->message_type() : nullptr;
        }
        return true;
    }

    void TFieldPath::Init(const Descriptor* msgType, const TStringBuf& path) {
        if (!InitUnsafe(msgType, path))
            ythrow yexception() << "Failed to resolve path \"" << path << "\" relative to " << msgType->full_name();
    }

}
