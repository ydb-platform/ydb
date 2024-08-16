#pragma once
#include <util/string/split.h>
#include <util/string/vector.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

template<typename ResponseType>
struct TWhiteboardInfo;

struct TEnumValue {
    TString Name;
    int Value;

    TEnumValue(const TString& string) {
        if (!TryFromString<int>(string, Value)) {
            Name = string;
        }
    }

    TEnumValue(const EnumValueDescriptor* descriptor)
        : Name(descriptor->name())
        , Value(descriptor->number())
    {}

    bool operator ==(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name == value.Name;
        }
        return Value == value.Value;
    }

    bool operator !=(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name != value.Name;
        }
        return Value != value.Value;
    }

    bool operator <(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name < value.Name;
        }
        return Value < value.Value;
    }

    bool operator <=(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name <= value.Name;
        }
        return Value <= value.Value;
    }

    bool operator >(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name > value.Name;
        }
        return Value > value.Value;
    }

    bool operator >=(const TEnumValue& value) const {
        if (!Name.empty() && !value.Name.empty()) {
            return Name >= value.Name;
        }
        return Value >= value.Value;
    }
};

struct TMessageValue {
    TString Value;

    TMessageValue(const TString& value)
        : Value(value)
    {}

    bool operator ==(const TMessageValue& value) const {
        return Value == value.Value;
    }

    bool operator !=(const TMessageValue& value) const {
        return Value != value.Value;
    }

    bool operator <(const TMessageValue& value) const {
        return Value < value.Value;
    }
};

template <typename CppType>
class TFieldProtoValueExtractor {
public:
    TFieldProtoValueExtractor(const FieldDescriptor* field)
        : Field(field)
    {}

    CppType ExtractValue(const Reflection& reflection, const Message& element) const;

protected:
    const FieldDescriptor* Field;
};

template<typename ResponseType>
class TWhiteboardFilter {
public:
    using TResponseType = typename TWhiteboardInfo<ResponseType>::TResponseType;
    using TElementType = typename TWhiteboardInfo<ResponseType>::TElementType;

    class IFieldProtoFilter {
    public:
        virtual ~IFieldProtoFilter() = default;
        virtual bool CheckFilter(const TElementType& element) const = 0;
    };

    template <typename CppType>
    class TFieldProtoFilterEqValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterEqValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) == Value;
        }

    protected:
        CppType Value;
    };

    template <typename CppType>
    class TFieldProtoFilterInValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterInValue(const FieldDescriptor* field, const TVector<CppType>& values)
            : TFieldProtoValueExtractor<CppType>(field)
            , Values(values.begin(), values.end())
        {}

        bool CheckFilter(const TElementType& element) const override {
            auto value = TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element);
            return Values.count(value) != 0;
        }

    protected:
        TSet<CppType> Values;
    };

    template <typename CppType>
    class TFieldProtoFilterNeValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterNeValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) != Value;
        }

    protected:
        CppType Value;
    };

    template <typename CppType>
    class TFieldProtoFilterLtValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterLtValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) < Value;
        }

    protected:
        CppType Value;
    };

    template <typename CppType>
    class TFieldProtoFilterLeValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterLeValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) <= Value;
        }

    protected:
        CppType Value;
    };

    template <typename CppType>
    class TFieldProtoFilterGtValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterGtValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) > Value;
        }

    protected:
        CppType Value;
    };

    template <typename CppType>
    class TFieldProtoFilterGeValue : public IFieldProtoFilter, public TFieldProtoValueExtractor<CppType> {
    public:
        TFieldProtoFilterGeValue(const FieldDescriptor* field, CppType value)
            : TFieldProtoValueExtractor<CppType>(field)
            , Value(value)
        {}

        bool CheckFilter(const TElementType& element) const override {
            return TFieldProtoValueExtractor<CppType>::ExtractValue(*element.GetReflection(), element) >= Value;
        }

    protected:
        CppType Value;
    };

    static void FilterResponse(TResponseType& source, const TVector<THolder<IFieldProtoFilter>>& filters) {
        ResponseType result;
        auto& field = TWhiteboardInfo<ResponseType>::GetElementsField(result);
        auto& sourceField = TWhiteboardInfo<ResponseType>::GetElementsField(source);
        field.Reserve(sourceField.size());
        for (TElementType& info : sourceField) {
            size_t cnt = 0;
            for (const THolder<IFieldProtoFilter>& filter : filters) {
                if (!filter->CheckFilter(info))
                    break;
                ++cnt;
            }
            if (cnt == filters.size()) {
                // TODO: swap already allocated element of repeatedptr field
                auto* element = field.Add();
                element->Swap(&info);
            }
        }
        result.SetResponseTime(source.GetResponseTime());
        source = std::move(result);
    }

    template <typename Type>
    static TVector<Type> FromStringWithDefaultArray(const TVector<TString>& values) {
        TVector<Type> result;
        for (const TString& value : values) {
            result.emplace_back(FromStringWithDefault<Type>(value));
        }
        return result;
    }

    template <typename Type>
    static TVector<Type> ConvertStringArray(const TVector<TString>& values) {
        TVector<Type> result;
        for (const TString& value : values) {
            result.emplace_back(value);
        }
        return result;
    }

    static TVector<THolder<IFieldProtoFilter>> GetProtoFilters(TString filters) {
        // TODO: convert to StringBuf operations?
        const Descriptor& descriptor = *TElementType::descriptor();
        TVector<TString> requestedFilters;
        TVector<THolder<IFieldProtoFilter>> foundFilters;
        if (filters.StartsWith('(') && filters.EndsWith(')')) {
            filters = filters.substr(1, filters.size() - 2);
        }
        StringSplitter(filters).Split(';').SkipEmpty().Collect(&requestedFilters);
        for (const TString& str : requestedFilters) {
            size_t opFirstPos = str.find_first_of("!><=");
            // TODO: replace with error reporting
            //Y_ABORT_UNLESS(opPos != TString::npos);
            THolder<IFieldProtoFilter> filter;
            TString field = str.substr(0, opFirstPos);
            size_t opEndPos = str.find_first_not_of("!><=", opFirstPos);
            if (opEndPos != TString::npos) {
                TString op = str.substr(opFirstPos, opEndPos - opFirstPos);
                TString value = str.substr(opEndPos);
                // TODO: replace with error reporting
                //Y_ABORT_UNLESS(op == "=");
                const FieldDescriptor* fieldDescriptor = descriptor.FindFieldByName(field);
                if (fieldDescriptor != nullptr) {
                    if (op == "=" || op == "==") {
                        if (value.StartsWith('[') && value.EndsWith(']')) {
                            TVector<TString> values;
                            StringSplitter(value.substr(1, value.size() - 2)).Split(',').SkipEmpty().Collect(&values);
                            switch (fieldDescriptor->cpp_type()) {
                            case FieldDescriptor::CPPTYPE_INT32:
                                filter = MakeHolder<TFieldProtoFilterInValue<i32>>(fieldDescriptor, FromStringWithDefaultArray<i32>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_INT64:
                                filter = MakeHolder<TFieldProtoFilterInValue<i64>>(fieldDescriptor, FromStringWithDefaultArray<i64>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_UINT32:
                                filter = MakeHolder<TFieldProtoFilterInValue<ui32>>(fieldDescriptor, FromStringWithDefaultArray<ui32>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_UINT64:
                                filter = MakeHolder<TFieldProtoFilterInValue<ui64>>(fieldDescriptor, FromStringWithDefaultArray<ui64>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_DOUBLE:
                                filter = MakeHolder<TFieldProtoFilterInValue<double>>(fieldDescriptor, FromStringWithDefaultArray<double>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_FLOAT:
                                filter = MakeHolder<TFieldProtoFilterInValue<float>>(fieldDescriptor, FromStringWithDefaultArray<float>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_BOOL:
                                filter = MakeHolder<TFieldProtoFilterInValue<bool>>(fieldDescriptor, FromStringWithDefaultArray<bool>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_ENUM:
                                filter = MakeHolder<TFieldProtoFilterInValue<TEnumValue>>(fieldDescriptor, ConvertStringArray<TEnumValue>(values));
                                break;
                            case FieldDescriptor::CPPTYPE_STRING:
                                filter = MakeHolder<TFieldProtoFilterInValue<TString>>(fieldDescriptor, values);
                                break;
                            case FieldDescriptor::CPPTYPE_MESSAGE:
                                filter = MakeHolder<TFieldProtoFilterInValue<TMessageValue>>(fieldDescriptor, ConvertStringArray<TMessageValue>(values));
                                break;
                            default:
                                break;
                            }
                        } else {
                            switch (fieldDescriptor->cpp_type()) {
                            case FieldDescriptor::CPPTYPE_INT32:
                                filter = MakeHolder<TFieldProtoFilterEqValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_INT64:
                                filter = MakeHolder<TFieldProtoFilterEqValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_UINT32:
                                filter = MakeHolder<TFieldProtoFilterEqValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_UINT64:
                                filter = MakeHolder<TFieldProtoFilterEqValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_DOUBLE:
                                filter = MakeHolder<TFieldProtoFilterEqValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_FLOAT:
                                filter = MakeHolder<TFieldProtoFilterEqValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_BOOL:
                                filter = MakeHolder<TFieldProtoFilterEqValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                                break;
                            case FieldDescriptor::CPPTYPE_ENUM:
                                filter = MakeHolder<TFieldProtoFilterEqValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                                break;
                            case FieldDescriptor::CPPTYPE_STRING:
                                filter = MakeHolder<TFieldProtoFilterEqValue<TString>>(fieldDescriptor, value);
                                break;
                            case FieldDescriptor::CPPTYPE_MESSAGE:
                                filter = MakeHolder<TFieldProtoFilterEqValue<TMessageValue>>(fieldDescriptor, TMessageValue(value));
                                break;
                            default:
                                break;
                            }
                        }
                    } else if (op == "<>" || op == "!=") {
                        switch (fieldDescriptor->cpp_type()) {
                        case FieldDescriptor::CPPTYPE_INT32:
                            filter = MakeHolder<TFieldProtoFilterNeValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_INT64:
                            filter = MakeHolder<TFieldProtoFilterNeValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT32:
                            filter = MakeHolder<TFieldProtoFilterNeValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT64:
                            filter = MakeHolder<TFieldProtoFilterNeValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_DOUBLE:
                            filter = MakeHolder<TFieldProtoFilterNeValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_FLOAT:
                            filter = MakeHolder<TFieldProtoFilterNeValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_BOOL:
                            filter = MakeHolder<TFieldProtoFilterNeValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_ENUM:
                            filter = MakeHolder<TFieldProtoFilterNeValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                            break;
                        case FieldDescriptor::CPPTYPE_STRING:
                            filter = MakeHolder<TFieldProtoFilterNeValue<TString>>(fieldDescriptor, value);
                            break;
                        case FieldDescriptor::CPPTYPE_MESSAGE:
                            filter = MakeHolder<TFieldProtoFilterNeValue<TMessageValue>>(fieldDescriptor, TMessageValue(value));
                            break;
                        default:
                            break;
                        }
                    } else if (op == "<") {
                        switch (fieldDescriptor->cpp_type()) {
                        case FieldDescriptor::CPPTYPE_INT32:
                            filter = MakeHolder<TFieldProtoFilterLtValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_INT64:
                            filter = MakeHolder<TFieldProtoFilterLtValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT32:
                            filter = MakeHolder<TFieldProtoFilterLtValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT64:
                            filter = MakeHolder<TFieldProtoFilterLtValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_DOUBLE:
                            filter = MakeHolder<TFieldProtoFilterLtValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_FLOAT:
                            filter = MakeHolder<TFieldProtoFilterLtValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_BOOL:
                            filter = MakeHolder<TFieldProtoFilterLtValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_ENUM:
                            filter = MakeHolder<TFieldProtoFilterLtValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                            break;
                        case FieldDescriptor::CPPTYPE_STRING:
                            filter = MakeHolder<TFieldProtoFilterLtValue<TString>>(fieldDescriptor, value);
                            break;
                        default:
                            break;
                        }
                    } else if (op == ">") {
                        switch (fieldDescriptor->cpp_type()) {
                        case FieldDescriptor::CPPTYPE_INT32:
                            filter = MakeHolder<TFieldProtoFilterGtValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_INT64:
                            filter = MakeHolder<TFieldProtoFilterGtValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT32:
                            filter = MakeHolder<TFieldProtoFilterGtValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT64:
                            filter = MakeHolder<TFieldProtoFilterGtValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_DOUBLE:
                            filter = MakeHolder<TFieldProtoFilterGtValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_FLOAT:
                            filter = MakeHolder<TFieldProtoFilterGtValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_BOOL:
                            filter = MakeHolder<TFieldProtoFilterGtValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_ENUM:
                            filter = MakeHolder<TFieldProtoFilterGtValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                            break;
                        case FieldDescriptor::CPPTYPE_STRING:
                            filter = MakeHolder<TFieldProtoFilterGtValue<TString>>(fieldDescriptor, value);
                            break;
                        default:
                            break;
                        }
                    } else if (op == "<=") {
                        switch (fieldDescriptor->cpp_type()) {
                        case FieldDescriptor::CPPTYPE_INT32:
                            filter = MakeHolder<TFieldProtoFilterLeValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_INT64:
                            filter = MakeHolder<TFieldProtoFilterLeValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT32:
                            filter = MakeHolder<TFieldProtoFilterLeValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT64:
                            filter = MakeHolder<TFieldProtoFilterLeValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_DOUBLE:
                            filter = MakeHolder<TFieldProtoFilterLeValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_FLOAT:
                            filter = MakeHolder<TFieldProtoFilterLeValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_BOOL:
                            filter = MakeHolder<TFieldProtoFilterLeValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_ENUM:
                            filter = MakeHolder<TFieldProtoFilterLeValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                            break;
                        case FieldDescriptor::CPPTYPE_STRING:
                            filter = MakeHolder<TFieldProtoFilterLeValue<TString>>(fieldDescriptor, value);
                            break;
                        default:
                            break;
                        }
                    } else if (op == ">=") {
                        switch (fieldDescriptor->cpp_type()) {
                        case FieldDescriptor::CPPTYPE_INT32:
                            filter = MakeHolder<TFieldProtoFilterGeValue<i32>>(fieldDescriptor, FromStringWithDefault<i32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_INT64:
                            filter = MakeHolder<TFieldProtoFilterGeValue<i64>>(fieldDescriptor, FromStringWithDefault<i64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT32:
                            filter = MakeHolder<TFieldProtoFilterGeValue<ui32>>(fieldDescriptor, FromStringWithDefault<ui32>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_UINT64:
                            filter = MakeHolder<TFieldProtoFilterGeValue<ui64>>(fieldDescriptor, FromStringWithDefault<ui64>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_DOUBLE:
                            filter = MakeHolder<TFieldProtoFilterGeValue<double>>(fieldDescriptor, FromStringWithDefault<double>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_FLOAT:
                            filter = MakeHolder<TFieldProtoFilterGeValue<float>>(fieldDescriptor, FromStringWithDefault<float>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_BOOL:
                            filter = MakeHolder<TFieldProtoFilterGeValue<bool>>(fieldDescriptor, FromStringWithDefault<bool>(value));
                            break;
                        case FieldDescriptor::CPPTYPE_ENUM:
                            filter = MakeHolder<TFieldProtoFilterGeValue<TEnumValue>>(fieldDescriptor, TEnumValue(value));
                            break;
                        case FieldDescriptor::CPPTYPE_STRING:
                            filter = MakeHolder<TFieldProtoFilterGeValue<TString>>(fieldDescriptor, value);
                            break;
                        default:
                            break;
                        }
                    }
                }
                if (filter == nullptr) {
                    throw yexception() << "invalid filter specified";
                }
                // TODO: replace with error reporting
                //Y_ABORT_UNLESS(filter != nullptr);
                foundFilters.emplace_back(std::move(filter));
            }
        }
        // TODO: replace with error reporting
        //Y_ABORT_UNLESS(requestedFilters.size() == foundFilters.size());
        return foundFilters;
    }
};

template<typename ResponseType>
void FilterWhiteboardResponses(ResponseType& response, const TString& filters) {
    TVector<THolder<typename TWhiteboardFilter<ResponseType>::IFieldProtoFilter>> filterFilters =
            TWhiteboardFilter<ResponseType>::GetProtoFilters(filters);
    TWhiteboardFilter<ResponseType>::FilterResponse(response, filterFilters);
}

}
