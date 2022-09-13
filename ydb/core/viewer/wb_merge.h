#pragma once
#include <unordered_map>
#include <util/string/vector.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "wb_filter.h"
#include "wb_group.h"

namespace NKikimr {
namespace NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

struct TWhiteboardDefaultInfo {
};

template <typename ResponseType>
struct TWhiteboardInfo;

template <typename ResponseType>
struct TWhiteboardMergerComparator {
    bool operator ()(const ResponseType& a, const ResponseType& b) const {
        return a.GetChangeTime() < b.GetChangeTime();
    }
};

class TWhiteboardMergerBase {
public:
    using TMergerKey = const ::google::protobuf::FieldDescriptor*;
    using TMergerValue = std::function<void(
        const ::google::protobuf::Reflection& reflectionTo,
        const ::google::protobuf::Reflection& reflectionFrom,
        ::google::protobuf::Message& protoTo,
        const ::google::protobuf::Message& protoFrom,
        const ::google::protobuf::FieldDescriptor* field)>;
    static std::unordered_map<TMergerKey, TMergerValue> FieldMerger;

    template <typename PropertyType>
    static void ProtoMergeField(
            const ::google::protobuf::Reflection& reflectionTo,
            const ::google::protobuf::Reflection& reflectionFrom,
            ::google::protobuf::Message& protoTo,
            const ::google::protobuf::Message& protoFrom,
            const ::google::protobuf::FieldDescriptor* field,
            PropertyType (::google::protobuf::Reflection::* getter)(const ::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*) const,
            void (::google::protobuf::Reflection::* setter)(::google::protobuf::Message*, const ::google::protobuf::FieldDescriptor*, PropertyType) const
            ) {
        bool has = reflectionTo.HasField(protoTo, field);
        PropertyType newVal = (reflectionFrom.*getter)(protoFrom, field);
        if (!has) {
            (reflectionTo.*setter)(&protoTo, field, newVal);
        } else {
            PropertyType oldVal = (reflectionTo.*getter)(protoTo, field);
            if (oldVal != newVal) {
                (reflectionTo.*setter)(&protoTo, field, newVal);
            }
        }
    }

    static void ProtoMaximizeEnumField(
            const ::google::protobuf::Reflection& reflectionTo,
            const ::google::protobuf::Reflection& reflectionFrom,
            ::google::protobuf::Message& protoTo,
            const ::google::protobuf::Message& protoFrom,
            const ::google::protobuf::FieldDescriptor* field);

    static void ProtoMaximizeBoolField(
            const ::google::protobuf::Reflection& reflectionTo,
            const ::google::protobuf::Reflection& reflectionFrom,
            ::google::protobuf::Message& protoTo,
            const ::google::protobuf::Message& protoFrom,
            const ::google::protobuf::FieldDescriptor* field);

    static void ProtoMerge(::google::protobuf::Message& protoTo, const ::google::protobuf::Message& protoFrom);
};

template <typename ResponseType>
class TWhiteboardMerger : public TWhiteboardMergerBase {
public:
    using TResponseType = typename TWhiteboardInfo<ResponseType>::TResponseType;
    using TElementType = typename TWhiteboardInfo<ResponseType>::TElementType;
    using TElementKeyType = typename TWhiteboardInfo<ResponseType>::TElementKeyType;

    static TString GetFieldString(const Reflection& reflection, const Message& message, const FieldDescriptor* fieldDescriptor) {
        switch (fieldDescriptor->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            return ToString(TFieldProtoValueExtractor<i32>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_INT64:
            return ToString(TFieldProtoValueExtractor<i64>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_UINT32:
            return ToString(TFieldProtoValueExtractor<ui32>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_UINT64:
            return ToString(TFieldProtoValueExtractor<ui64>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_DOUBLE:
            return ToString(TFieldProtoValueExtractor<double>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_FLOAT:
            return ToString(TFieldProtoValueExtractor<float>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_BOOL:
            return ToString(TFieldProtoValueExtractor<bool>(fieldDescriptor).ExtractValue(reflection, message));
        case FieldDescriptor::CPPTYPE_ENUM:
            return TFieldProtoValueExtractor<TEnumValue>(fieldDescriptor).ExtractValue(reflection, message).Name;
        case FieldDescriptor::CPPTYPE_STRING:
            return TFieldProtoValueExtractor<TString>(fieldDescriptor).ExtractValue(reflection, message);
        case FieldDescriptor::CPPTYPE_MESSAGE: {
                const Message& subMessage = reflection.GetMessage(message, fieldDescriptor);
                const Reflection& subReflection = *subMessage.GetReflection();
                TVector<const FieldDescriptor*> subFields;
                subReflection.ListFields(subMessage, &subFields);
                return "{" + GetDynamicKey(subReflection, subMessage, subFields) + "}";
            }
        default:
            return "undefined";
        }
    }

    static TString GetDynamicKey(const Reflection& reflection, const Message& message, const TVector<const FieldDescriptor*>& fields) {
        TString key;
        for (const FieldDescriptor* field : fields) {
            if (!key.empty()) {
                key += '-';
            }
            key += GetFieldString(reflection, message, field);
        }
        return key;
    }

    struct TDynamicMergeKey {
        TVector<const FieldDescriptor*> MergeFields;
        using KeyType = TString;

        TDynamicMergeKey(const TString& fields)
            : MergeFields(TWhiteboardGrouper<TResponseType>::GetProtoFields(fields))
        {
        }

        TString GetKey(TElementType& info) const {
            return GetDynamicKey(*info.GetReflection(), info, MergeFields);
        }
    };

    template <typename ElementKeyType>
    struct TStaticMergeKey {
        using KeyType = ElementKeyType;

        ElementKeyType GetKey(TElementType& info) const {
            return TWhiteboardInfo<ResponseType>::GetElementKey(info);
        }
    };

    template <typename MergeKey>
    static THolder<TResponseType> MergeResponsesBase(TMap<ui32, THolder<TResponseType>>& responses, const MergeKey& mergeKey) {
        std::unordered_map<typename MergeKey::KeyType, TElementType*> mergedData;
        ui64 minResponseTime = 0;
        ui64 maxResponseDuration = 0;
        //ui64 sumProcessDuration = 0;
        TWhiteboardMergerComparator<TElementType> comparator;
        for (auto it = responses.begin(); it != responses.end(); ++it) {
            if (it->second != nullptr) {
                auto* stateInfo = TWhiteboardInfo<ResponseType>::GetElementsField(it->second.Get());
                for (TElementType& info : *stateInfo) {
                    if (!info.HasNodeId()) {
                        info.SetNodeId(it->first);
                    }
                    auto key = mergeKey.GetKey(info);
                    auto inserted = mergedData.emplace(key, &info);
                    if (!inserted.second) {
                        if (comparator(*inserted.first->second, info)) {
                            inserted.first->second = &info;
                        }
                    }
                }
                if (minResponseTime == 0 || it->second->Record.GetResponseTime() < minResponseTime) {
                    minResponseTime = it->second->Record.GetResponseTime();
                }
                if (maxResponseDuration == 0 || it->second->Record.GetResponseDuration() > maxResponseDuration) {
                    maxResponseDuration = it->second->Record.GetResponseDuration();
                }
                //sumProcessDuration += it->second->Record.GetProcessDuration();
            }
        }

        THolder<TResponseType> result = MakeHolder<TResponseType>();
        auto* field = TWhiteboardInfo<ResponseType>::GetElementsField(result.Get());
        field->Reserve(mergedData.size());
        for (auto it = mergedData.begin(); it != mergedData.end(); ++it) {
            auto* element = field->Add();
            element->Swap(it->second);
        }
        if (minResponseTime) {
            result->Record.SetResponseTime(minResponseTime);
        }
        if (maxResponseDuration) {
            result->Record.SetResponseDuration(maxResponseDuration);
        }
        //if (sumProcessDuration) {
        //    result->Record.SetProcessDuration(sumProcessDuration);
        //}
        return result;
    }

    static THolder<TResponseType> MergeResponsesElementKey(TMap<ui32, THolder<TResponseType>>& responses) {
        TStaticMergeKey<typename TWhiteboardInfo<ResponseType>::TElementKeyType> mergeKey;
        return MergeResponsesBase(responses, mergeKey);
    }

    static THolder<TResponseType> MergeResponses(TMap<ui32, THolder<TResponseType>>& responses, const TString& fields) {
        TDynamicMergeKey mergeKey(fields);
        return MergeResponsesBase(responses, mergeKey);
    }
};

template <typename ResponseType>
THolder<ResponseType> MergeWhiteboardResponses(TMap<ui32, THolder<ResponseType>>& responses, const TString& fields = TWhiteboardInfo<ResponseType>::GetDefaultMergeField()) {
    return TWhiteboardInfo<ResponseType>::MergeResponses(responses, fields);
}

}
}
