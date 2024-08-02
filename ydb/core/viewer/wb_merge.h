#pragma once
#include "wb_filter.h"
#include "wb_group.h"
#include <unordered_map>
#include <util/string/vector.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

struct TWhiteboardDefaultInfo {};

template<typename ResponseType>
struct TWhiteboardInfo;

template<typename ResponseType>
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

template<typename ResponseType>
struct TStaticMergeKey {
    using KeyType = typename TWhiteboardInfo<ResponseType>::TElementKeyType;

    template<typename ElementType>
    KeyType GetKey(const ElementType& info) const {
        return TWhiteboardInfo<ResponseType>::GetElementKey(info);
    }
};

template <typename ResponseType>
class TWhiteboardMerger : public TWhiteboardMergerBase {
public:
    using TResponseType = typename TWhiteboardInfo<ResponseType>::TResponseType;
    using TElementType = typename TWhiteboardInfo<ResponseType>::TElementType;

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

    template<typename MergeKey>
    static void MergeResponsesBaseHybrid(TResponseType& result, TMap<ui32, TResponseType>& responses, const MergeKey& mergeKey) {
        using TElementType = typename TWhiteboardInfo<ResponseType>::TElementType;
        using TElementTypePacked5 = typename TWhiteboardInfo<ResponseType>::TElementTypePacked5;

        std::unordered_map<typename MergeKey::KeyType, TElementType*> mergedData;

        struct TPackedDataCtx {
            const TElementTypePacked5* Element;
            ui32 NodeId;
        };

        std::unordered_map<typename MergeKey::KeyType, TPackedDataCtx> mergedDataPacked5;

        size_t projectedSize = 0;
        for (auto it = responses.begin(); it != responses.end(); ++it) {
            projectedSize += TWhiteboardInfo<ResponseType>::GetElementsCount(it->second);
        }
        mergedData.reserve(projectedSize);
        mergedDataPacked5.reserve(projectedSize);

        ui64 minResponseTime = 0;
        ui64 maxResponseDuration = 0;
        ui64 sumProcessDuration = 0;

        for (auto it = responses.begin(); it != responses.end(); ++it) {
            {
                TWhiteboardMergerComparator<TElementType> comparator;
                auto& stateInfo = TWhiteboardInfo<ResponseType>::GetElementsField(it->second);
                for (TElementType& info : stateInfo) {
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
            }
            {
                TWhiteboardMergerComparator<TElementTypePacked5> comparator;
                auto stateInfo = TWhiteboardInfo<ResponseType>::GetElementsFieldPacked5(it->second);
                for (auto& info : stateInfo) {
                    auto key = mergeKey.GetKey(info);
                    auto inserted = mergedDataPacked5.emplace(key, TPackedDataCtx{
                        .Element = &info,
                        .NodeId = it->first
                    });
                    if (!inserted.second) {
                        if (comparator(*inserted.first->second.Element, info)) {
                            inserted.first->second = {
                                .Element = &info,
                                .NodeId = it->first
                            };
                        }
                    }
                }
            }
            if (minResponseTime == 0 || it->second.GetResponseTime() < minResponseTime) {
                minResponseTime = it->second.GetResponseTime();
            }
            if (maxResponseDuration == 0 || it->second.GetResponseDuration() > maxResponseDuration) {
                maxResponseDuration = it->second.GetResponseDuration();
            }
            sumProcessDuration += it->second.GetProcessDuration();
        }

        auto& field = TWhiteboardInfo<ResponseType>::GetElementsField(result);
        field.Reserve(mergedData.size() + mergedDataPacked5.size());
        for (auto it = mergedDataPacked5.begin(); it != mergedDataPacked5.end(); ++it) {
            auto* element = field.Add();
            it->second.Element->Fill(*element);
            element->SetNodeId(it->second.NodeId);
            mergedData.erase(it->first);
        }
        for (auto it = mergedData.begin(); it != mergedData.end(); ++it) {
            auto* element = field.Add();
            element->Swap(it->second);
        }
        if (minResponseTime) {
            result.SetResponseTime(minResponseTime);
        }
        if (maxResponseDuration) {
            result.SetResponseDuration(maxResponseDuration);
        }
        if (sumProcessDuration) {
            result.SetProcessDuration(sumProcessDuration);
        }
    }

    template<typename MergeKey>
    static void MergeResponsesBase(TResponseType& result, TMap<ui32, TResponseType>& responses, const MergeKey& mergeKey) {
        std::unordered_map<typename MergeKey::KeyType, TElementType*> mergedData;
        ui64 minResponseTime = 0;
        ui64 maxResponseDuration = 0;
        //ui64 sumProcessDuration = 0;
        TWhiteboardMergerComparator<TElementType> comparator;
        for (auto it = responses.begin(); it != responses.end(); ++it) {
            auto& stateInfo = TWhiteboardInfo<ResponseType>::GetElementsField(it->second);
            for (TElementType& info : stateInfo) {
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
            if (minResponseTime == 0 || it->second.GetResponseTime() < minResponseTime) {
                minResponseTime = it->second.GetResponseTime();
            }
            if (maxResponseDuration == 0 || it->second.GetResponseDuration() > maxResponseDuration) {
                maxResponseDuration = it->second.GetResponseDuration();
            }
            //sumProcessDuration += it->second.GetProcessDuration();
        }

        auto& field = TWhiteboardInfo<ResponseType>::GetElementsField(result);
        field.Reserve(mergedData.size());
        for (auto it = mergedData.begin(); it != mergedData.end(); ++it) {
            auto* element = field.Add();
            element->Swap(it->second);
        }
        if (minResponseTime) {
            result.SetResponseTime(minResponseTime);
        }
        if (maxResponseDuration) {
            result.SetResponseDuration(maxResponseDuration);
        }
        //if (sumProcessDuration) {
        //    result->Record.SetProcessDuration(sumProcessDuration);
        //}
    }

    static void MergeResponsesElementKey(TResponseType& result, TMap<ui32, TResponseType>& responses) {
        TStaticMergeKey<ResponseType> mergeKey;
        MergeResponsesBase(result, responses, mergeKey);
    }

    static void MergeResponses(TResponseType& result, TMap<ui32, TResponseType>& responses, const TString& fields) {
        TDynamicMergeKey mergeKey(fields);
        MergeResponsesBase(result, responses, mergeKey);
    }
};

template<typename ResponseType>
void MergeWhiteboardResponses(ResponseType& result, TMap<ui32, ResponseType>& responses, const TString& fields = TWhiteboardInfo<ResponseType>::GetDefaultMergeField()) {
    TWhiteboardInfo<ResponseType>::MergeResponses(result, responses, fields);
}

}
