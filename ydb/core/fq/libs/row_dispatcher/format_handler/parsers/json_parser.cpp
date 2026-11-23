#ifndef NDEBUG
#define SIMDJSON_DEVELOPMENT_CHECKS 1
#endif
#include "json_parser.h"
#include "parser_base.h"

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/simdjson/include/simdjson.h>
#include <library/cpp/containers/absl/flat_hash_map.h>
#include <util/generic/scope.h>
#include <util/string/join.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <sstream>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::FQ_ROW_DISPATCHER

namespace NFq::NRowDispatcher {

namespace {

TStringBuf GetTypeName(const NKikimr::NMiniKQL::TType* type) {
    if (type->GetKind() == NKikimr::NMiniKQL::TTypeBase::EKind::Data) {
        if (auto maybeDataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot()) {
            return NYql::NUdf::GetDataTypeInfo(*maybeDataSlot).Name;
        }
    }
    return type->GetKindAsStr();
}

static std::string JsonTypeToString(const simdjson::builtin::ondemand::json_type& json_type) {
    std::stringstream ss;
    ss << json_type;
    return std::move(ss).str();
}

#define CHECK_JSON_ERROR(value)               \
    const simdjson::error_code error = value; \
    if (Y_UNLIKELY(error))

template <typename T>
using TParserVector = TVector<T, NKikimr::NMiniKQL::TMKQLAllocator<T>>;
using TParserString = std::basic_string<char, std::char_traits<char>, NKikimr::NMiniKQL::TMKQLAllocator<char>>;

struct TJsonParserBuffer: public TNonCopyable {
    explicit TJsonParserBuffer(NKikimr::NMiniKQL::TScopedAlloc& alloc)
        : Alloc(alloc)
    {
    }

    ~TJsonParserBuffer() {
        with_lock(Alloc) {
            decltype(Values)().swap(Values);
            decltype(MessageOffsets)().swap(MessageOffsets);
            decltype(Offsets)().swap(Offsets);
        }
    }

    ui16 NumberValues = 0;
    bool Finished = false;
    TInstant CreationStartTime = TInstant::Now();
    TParserVector<ui64> Offsets = {};   // Offsets in topics (seqno).
    TParserVector<ui32> MessageOffsets = {};   // Message boundaries in Values; Finish() appends the final boundary.

    bool IsReady() const {
        return !Finished && NumberValues > 0;
    }

    size_t GetSize() const {
        return Values.size();
    }

    void Reserve(size_t size, size_t numberValues) {
        Values.reserve(size + simdjson::SIMDJSON_PADDING);
        MessageOffsets.reserve(numberValues + 1);
        Offsets.reserve(numberValues);
    }

    void AddRecord(const TMessageStreamRecord& record) {
        Y_ENSURE(!Finished, "Cannot add messages into finished buffer");

        const auto offset = record.Id.Offset;
        if (Y_UNLIKELY(Offsets && Offsets.back() > offset)) {
            YDB_LOG_WARN("Got message with offset which is less than previous offset",
                {"logPrefix", LogPrefix},
                {"offset", offset},
                {"offsetsBack", Offsets.back()});
        }

        if (!record.Data) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported) << "PQ parser does not support null message payloads";
        }
        const auto& data = *record.Data;
        try {
            MessageOffsets.emplace_back(Values.size());
            Offsets.emplace_back(offset);
            Values.append(data.data(), data.size());
        } catch (...) {
            // Preserve already buffered messages if an allocation fails.
            MessageOffsets.resize(NumberValues);
            Offsets.resize(NumberValues);
            throw;
        }
        ++NumberValues;
    }

    std::pair<const char*, size_t> Finish() {
        Y_ENSURE(!Finished, "Cannot finish buffer twice");
        Finished = true;
        MessageOffsets.emplace_back(Values.size());
        Values.resize(Values.size() + simdjson::SIMDJSON_PADDING, ' ');
        return { Values.data(), Values.size() - simdjson::SIMDJSON_PADDING };
    }

    void Clear() {
        NumberValues = 0;
        Finished = false;
        CreationStartTime = TInstant::Now();
        Values.clear();
        Offsets.clear();
        MessageOffsets.clear();
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc& Alloc;
    TParserString Values = {};
    const TString LogPrefix = "TJsonParser: Buffer: ";
};

class TColumnParser: public TMoveOnly {
    template <typename K, typename V>
    using TColumnMap = THashMap<K, V, THash<K>, TEqualTo<K>, NKikimr::NMiniKQL::TMKQLAllocator<std::pair<const K, V>>>;
    using TMemberNames = TColumnMap<std::string_view, size_t>;
    using TStructMembers = TColumnMap<const NKikimr::NMiniKQL::TStructType*, TMemberNames>;

public:
    TParserString Name;
    TParserString TypeYson;

public:
    TStatus InitParser(const TString& name, const TString& typeYson, std::span<size_t> parsedRows, const NKikimr::NMiniKQL::TType* typeMkql,
        bool skipErrors, NKikimr::NMiniKQL::THolderFactory* holderFactory) {
        ClearParsedRows();
        StructMembers = TStructMembers();
        Name = TParserString(name.data(), name.size());
        TypeYson = TParserString(typeYson.data(), typeYson.size());
        IsOptional = false;
        SkipErrors = skipErrors;
        ParsedRows = parsedRows;
        TypeMkql = nullptr;
        HolderFactory = holderFactory;
        Status = ExtractDataSlot(typeMkql);
        return Status;
    }

    void SetParsedRows(std::span<size_t> parsedRows) {
        ParsedRows = parsedRows;
    }

    bool GetIsOptional() {
        return IsOptional;
    }

    size_t GetParsedRowsCount() const {
        return ParsedRowsCount;
    }

    const std::span<size_t>& GetParsedRows() const {
        return ParsedRows;
    }

    TStatus GetStatus() const {
        return Status;
    }

    bool IsNullableType(const NKikimr::NMiniKQL::TType* type, NYql::NUdf::TUnboxedValue& resultValue) const {
        Y_DEBUG_ABORT_UNLESS(!resultValue);
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional:
                return true;
            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant: {
                auto variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);
                auto alternativeCount = variantType->GetAlternativesCount();
                for (ui32 index = 0; index != alternativeCount; ++index) {
                    if (IsNullableType(variantType->GetAlternativeType(index), resultValue)) {
                        // nullable alternative found, wrap in holder
                        resultValue = HolderFactory->CreateVariantHolder(std::move(resultValue.Release()), index);
                        return true;
                    }
                }
                [[fallthrough]];
            }
            default:
                return false;
        }
    }

    static bool ValidateJsonType(
        const NKikimr::NMiniKQL::TType* type, simdjson::builtin::ondemand::json_type cellType, simdjson::builtin::ondemand::value& jsonValue) {
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional:
                // null are handled separately
                return ValidateJsonType(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), cellType, jsonValue);

            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant: {
                auto variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);
                const auto alternativesCount = variantType->GetAlternativesCount();
                for (ui32 index = 0; index != alternativesCount; ++index) {
                    auto alternativeType = variantType->GetAlternativeType(index);
                    if (ValidateJsonType(alternativeType, cellType, jsonValue)) {
                        return true;
                    }
                }
                return false;
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                auto maybeDataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot();
                Y_ENSURE(maybeDataSlot);
                auto dataSlot = *maybeDataSlot;
                switch (dataSlot) {
                    case NYql::NUdf::EDataSlot::Int8:
                    case NYql::NUdf::EDataSlot::Int16:
                    case NYql::NUdf::EDataSlot::Int32:
                    case NYql::NUdf::EDataSlot::Int64:
                        if (cellType != simdjson::builtin::ondemand::json_type::number) {
                            return false;
                        }
                        return jsonValue.is_integer();
                    case NYql::NUdf::EDataSlot::Uint8:
                    case NYql::NUdf::EDataSlot::Uint16:
                    case NYql::NUdf::EDataSlot::Uint32:
                    case NYql::NUdf::EDataSlot::Uint64:
                        if (cellType != simdjson::builtin::ondemand::json_type::number) {
                            return false;
                        }
                        if (jsonValue.is_negative()) {
                            return false;
                        }
                        return jsonValue.is_integer();
                    case NYql::NUdf::EDataSlot::Double:
                    case NYql::NUdf::EDataSlot::Float:
                        return cellType == simdjson::builtin::ondemand::json_type::number;
                    case NYql::NUdf::EDataSlot::String:
                    case NYql::NUdf::EDataSlot::Utf8:
                        return cellType == simdjson::builtin::ondemand::json_type::string;
                    case NYql::NUdf::EDataSlot::Bool:
                        return cellType == simdjson::builtin::ondemand::json_type::boolean;
                    case NYql::NUdf::EDataSlot::Json:
                        return true;
                    default:
                        // Note: we cannot distinguish between various subtypes
                        // E.g. string may be parsable as Datetime, but first alternative
                        // is Interval; parsing will fail as a result
                        return cellType == simdjson::builtin::ondemand::json_type::string;
                }
            }
            case NKikimr::NMiniKQL::TTypeBase::EKind::Struct:
            case NKikimr::NMiniKQL::TTypeBase::EKind::Dict:
                return cellType == simdjson::builtin::ondemand::json_type::object;

            case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple:
            case NKikimr::NMiniKQL::TTypeBase::EKind::List:
                return cellType == simdjson::builtin::ondemand::json_type::array;

            default:
                Y_DEBUG_ABORT();
                return true;
        }
    }

    bool ParseVariantValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isQuiet, simdjson::builtin::ondemand::json_type cellType) const {
        auto variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);
        const auto alternativesCount = variantType->GetAlternativesCount();
        simdjson::ondemand::parser parser;
        simdjson::simdjson_result<simdjson::ondemand::document> doc;
        // we need to know validAlternativesLimit to
        // 1) check if non-scalar alternative is last possible (then we can avoid re-parsing);
        // 2) return error from last failed alternative;
        // tc is O(sum alternativesCount) anyway
        ui32 validAlternativesLimit = alternativesCount;
        while (validAlternativesLimit > 0) {
            auto alternativeType = variantType->GetAlternativeType(validAlternativesLimit - 1);
            if (ValidateJsonType(alternativeType, cellType, jsonValue)) {
                break;
            }
            --validAlternativesLimit;
        }
        auto innerJsonValue = jsonValue;
        for (ui32 index = 0; index != validAlternativesLimit; ++index) {
            auto alternativeType = variantType->GetAlternativeType(index);
            if (!ValidateJsonType(alternativeType, cellType, jsonValue)) {
                continue;
            }
            if (doc.error() == simdjson::error_code::UNINITIALIZED && !jsonValue.is_scalar() && index + 1 != validAlternativesLimit) {
                // With non-scalar types we must use re-parsing
                // With scalar types, we cannot do reparsing (and it would be inefficient)
                // Note that scalar case relies on repeated calls `value.get_string(consumer)`, which is not exactly documented (and, on the contrary, `sv = value.get_string()` does NOT work, in a very nasty way -- it triggers OOB writes)
                // Also note that we cannot just (easily) reparse scalar values, as
                // we cannot .get(value) from scalar document
                std::string_view rawJson;
                CHECK_JSON_ERROR(jsonValue.raw_json().get(rawJson)) {
                    SetParsingError(error, jsonValue, "reparse as json", status, isQuiet);
                    return false;
                }
                doc = parser.iterate(simdjson::padded_string_view(rawJson,
                    rawJson.size() + simdjson::SIMDJSON_PADDING));   // as rawJson points to original document, we *know* padding is present
            }
            if (doc.error() != simdjson::error_code::UNINITIALIZED) {
                CHECK_JSON_ERROR(doc.get(innerJsonValue)) {
                    SetParsingError(error, jsonValue, "reparse as json", status, isQuiet);
                    return false;
                }
            }
            status = TStatus::Success();
            if (ParseNestedValue(innerJsonValue, resultValue, status, alternativeType, false, index + 1 != validAlternativesLimit || isQuiet)) {
                resultValue = HolderFactory->CreateVariantHolder(std::move(resultValue.Release()), index);
                return true;
            }
            if (doc.error() != simdjson::error_code::UNINITIALIZED) {
                if (!doc->is_alive()) {
                    // json parsing error, unrecoverable, not safe to continue
                    break;
                }
                doc.rewind();
            }
            resultValue = NYql::NUdf::TUnboxedValue();
        }
        if (status.IsSuccess()) {
            // Note: we try to keep error from last alternative, but this won't work if tail is rejected by early json type mismatch check. So, just ensure some error is reported
            status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                isQuiet ? TString()
                        : TStringBuilder() << "Failed to parse as Variant type" << ", json type: " << JsonTypeToString(cellType)
                                           << ", current token: '" << TruncateString(jsonValue.raw_json_token()) << "'");
        } else if (!isQuiet) {
            status.AddParentIssue(TStringBuilder() << "Failed to parse as Variant type");
        }
        return false;
    }

    bool ParseListValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isQuiet, simdjson::builtin::ondemand::json_type cellType) const {
        if (cellType != simdjson::builtin::ondemand::json_type::array) {
            status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, isQuiet ? TString()
                                                                           : TStringBuilder()
                                                                                 << "Failed to parse nested json value (" << type->GetKindAsStr()
                                                                                 << "), expected array, but got " << JsonTypeToString(cellType));
            return false;
        }
        simdjson::ondemand::array array;
        {
            CHECK_JSON_ERROR(jsonValue.get_array().get(array)) {
                SetParsingError(error, jsonValue, "parse as array", status, isQuiet);
                return false;
            }
        }
        auto listType = AS_TYPE(NKikimr::NMiniKQL::TListType, type);
        auto itemType = listType->GetItemType();
        auto listBuilder = HolderFactory->NewList();
        array.reset();
        for (auto elt : array) {
            simdjson::builtin::ondemand::value eltValue;
            CHECK_JSON_ERROR(elt.get(eltValue)) {
                SetParsingError(error, jsonValue, "parse as array", status, isQuiet);
                return false;
            }
            NYql::NUdf::TUnboxedValue value;
            if (!ParseNestedValue(std::move(eltValue), value, status, itemType, false, isQuiet)) {
                return false;
            }
            listBuilder->Add(std::move(value));
        }
        resultValue = listBuilder->Build();
        return true;
    }

    bool ParseTupleValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isQuiet, simdjson::builtin::ondemand::json_type cellType) const {
        if (cellType != simdjson::builtin::ondemand::json_type::array) {
            status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, isQuiet ? TString()
                                                                           : TStringBuilder()
                                                                                 << "Failed to parse nested json value (" << type->GetKindAsStr()
                                                                                 << "), expected array, but got " << JsonTypeToString(cellType));
            return false;
        }
        simdjson::ondemand::array array;
        {
            CHECK_JSON_ERROR(jsonValue.get_array().get(array)) {
                SetParsingError(error, jsonValue, "parse as array", status, isQuiet);
                return false;
            }
        }
        auto tupleType = AS_TYPE(NKikimr::NMiniKQL::TTupleType, type);
        auto elementsCount = tupleType->GetElementsCount();
        NYql::NUdf::TUnboxedValue* resultValues;
        NYql::NUdf::TUnboxedValue tuple = HolderFactory->CreateDirectArrayHolder(elementsCount, resultValues);
        size_t idx = 0;
        array.reset();
        for (auto elt : array) {
            if (idx == elementsCount) {
                break;
            }
            simdjson::builtin::ondemand::value eltValue;
            CHECK_JSON_ERROR(elt.get(eltValue)) {
                SetParsingError(error, jsonValue, "parse as array (tuple)", status, isQuiet);
                return false;
            }
            if (!ParseNestedValue(std::move(eltValue), resultValues[idx], status, tupleType->GetElementType(idx), false, isQuiet)) {
                return false;
            }
            ++idx;
        }
        for (; idx != elementsCount; ++idx) {
            // tail must be optional
            if (tupleType->GetElementType(idx)->GetKind() != NKikimr::NMiniKQL::TTypeBase::EKind::Optional) {
                status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                    TStringBuilder() << "Failed to parse nested json value (" << type->GetKindAsStr() << "), short json array at index " << idx
                                     << ", expected non optional type " << GetTypeName(tupleType->GetElementType(idx)));
                return false;
            }
        }
        resultValue = std::move(tuple);
        return true;
    }

    bool ParseStructValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isQuiet, simdjson::builtin::ondemand::json_type cellType) const {
        if (cellType != simdjson::builtin::ondemand::json_type::object) {
            status = TStatus::Fail(
                EStatusId::PRECONDITION_FAILED, isQuiet ? TString()
                                                        : TStringBuilder() << "Failed to parse nested json value (" << type->GetKindAsStr()
                                                                           << "), expected object, but got " << JsonTypeToString(cellType));
            return false;
        }
        simdjson::ondemand::object object;
        {
            CHECK_JSON_ERROR(jsonValue.get_object().get(object)) {
                SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                return false;
            }
        }
        auto structType = AS_TYPE(NKikimr::NMiniKQL::TStructType, type);
        auto membersCount = structType->GetMembersCount();

        auto it = StructMembers.find(structType);
        Y_ENSURE(it != StructMembers.end());
        const auto& memberNames = it->second;
        Y_ENSURE(memberNames.size() == membersCount);

        NYql::NUdf::TUnboxedValue* resultValues;
        NYql::NUdf::TUnboxedValue structValue = HolderFactory->CreateDirectArrayHolder(membersCount, resultValues);
        object.reset();
        for (auto elt : object) {
            std::string_view name;
            {
                CHECK_JSON_ERROR(elt.escaped_key().get(name)) {
                    SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                    return false;
                }
            }
            auto itName = memberNames.find(name);
            if (itName == memberNames.end()) {
                continue;
            }
            auto idx = itName->second;

            simdjson::builtin::ondemand::value eltValue;
            CHECK_JSON_ERROR(elt.value().get(eltValue)) {
                SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                return false;
            }

            if (!ParseNestedValue(std::move(eltValue), resultValues[idx], status, structType->GetMemberType(idx), false, isQuiet)) {
                return false;
            }
        }

        for (ui32 idx = 0; idx != membersCount; ++idx) {
            if (!resultValues[idx] && structType->GetMemberType(idx)->GetKind() != NKikimr::NMiniKQL::TTypeBase::EKind::Optional) {
                status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                    isQuiet ? TString()
                            : TStringBuilder() << "Failed to parse nested json value (" << type->GetKindAsStr()
                                               << "), expected non-optional field " << structType->GetMemberName(idx));
                return false;
            }
        }
        resultValue = std::move(structValue);
        return true;
    }

    bool ParseDictValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isQuiet, simdjson::builtin::ondemand::json_type cellType) const {
        if (cellType != simdjson::builtin::ondemand::json_type::object) {
            status = TStatus::Fail(
                EStatusId::PRECONDITION_FAILED, isQuiet ? TString()
                                                        : TStringBuilder() << "Failed to parse nested json value (" << type->GetKindAsStr()
                                                                           << "), expected object, but got " << JsonTypeToString(cellType));
            return false;
        }
        simdjson::ondemand::object object;
        {
            CHECK_JSON_ERROR(jsonValue.get_object().get(object)) {
                SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                return false;
            }
        }

        {
            bool isEmpty;
            CHECK_JSON_ERROR(object.is_empty().get(isEmpty)) {
                SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                return false;
            }
            if (isEmpty) {
                resultValue = HolderFactory->GetEmptyContainerLazy();
                return true;
            }
        }
        object.reset();
        auto dictType = AS_TYPE(NKikimr::NMiniKQL::TDictType, type);
        auto keyType = dictType->GetKeyType();
        Y_ENSURE(keyType->GetKind() == NKikimr::NMiniKQL::TTypeBase::EKind::Data);   // should be already verified, not user-data-error
        auto keyDataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, keyType)->GetDataSlot();
        Y_ENSURE(keyDataSlot);   // should be already verified, not user-data-error
        auto payloadType = dictType->GetPayloadType();
        NKikimr::NMiniKQL::TKeyTypes keyTypes;
        bool isTuple;
        bool encoded;
        bool useIHash;
        GetDictionaryKeyTypes(keyType, keyTypes, isTuple, encoded, useIHash);
        Y_ENSURE(!(isTuple || encoded || useIHash));
        status = TStatus::Success();
        NYql::NUdf::TUnboxedValue dictValue = HolderFactory->CreateDirectHashedDictHolder([&, this](auto& map) {
            for (auto elt : object) {
                std::string_view name;
                {
                    CHECK_JSON_ERROR(elt.escaped_key().get(name)) {
                        SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                        return;
                    }
                }
                simdjson::builtin::ondemand::value eltValue;
                CHECK_JSON_ERROR(elt.value().get(eltValue)) {
                    SetParsingError(error, jsonValue, "parse as object", status, isQuiet);
                    return;
                }
                NYql::NUdf::TUnboxedValue payload;
                if (!ParseNestedValue(std::move(eltValue), payload, status, payloadType, false, isQuiet)) {
                    return;
                }
                map.emplace(NKikimr::NMiniKQL::ValueFromString(*keyDataSlot, name), std::move(payload));
            }
        }, keyTypes, false, true, nullptr, nullptr, nullptr);
        if (status.IsSuccess()) {
            resultValue = std::move(dictValue);
            return true;
        }
        return false;
    }

    bool ParseNestedValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        const NKikimr::NMiniKQL::TType* type, bool isOptional, bool isQuiet = false) const {
        Y_ENSURE(HolderFactory);   // should be already verified by ParseNestedType
        simdjson::builtin::ondemand::json_type cellType;
        CHECK_JSON_ERROR(jsonValue.type().get(cellType)) {
            SetParsingError(error, jsonValue, "determine json value type", status, isQuiet);
            return false;
        }

        if (cellType == simdjson::builtin::ondemand::json_type::null) {
            bool isNull;
            CHECK_JSON_ERROR(jsonValue.is_null().get(isNull)) {
                SetParsingError(error, jsonValue, "parse as null", status, isQuiet);
                return false;
            }
            if (isOptional) {
                resultValue = NYql::NUdf::TUnboxedValuePod();
                return true;
            }
            if (IsNullableType(type, resultValue)) {
                return true;
            }
            status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                isQuiet ? TString() : TStringBuilder() << "Found unexpected null value, expected non optional type " << GetTypeName(type));
            return false;
        }

        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant:
                return ParseVariantValue(jsonValue, resultValue, status, type, isQuiet, cellType);

            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                auto maybeDataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot();
                Y_ENSURE(maybeDataSlot);
                auto dataSlot = *maybeDataSlot;
                if (dataSlot != NYql::NUdf::EDataSlot::Json) {
                    return ParseDataType(
                        std::move(jsonValue), resultValue, status, dataSlot, isOptional, NYql::NUdf::GetDataTypeInfo(dataSlot).Name, isQuiet);
                } else {
                    return ParseJsonType(std::move(jsonValue), resultValue, status, isQuiet);
                }
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                Y_ENSURE(!isOptional);
                return ParseNestedValue(
                    std::move(jsonValue), resultValue, status, AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), true, isQuiet);
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::List:
                return ParseListValue(jsonValue, resultValue, status, type, isQuiet, cellType);

            case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple:
                return ParseTupleValue(jsonValue, resultValue, status, type, isQuiet, cellType);

            case NKikimr::NMiniKQL::TTypeBase::EKind::Struct:
                return ParseStructValue(jsonValue, resultValue, status, type, isQuiet, cellType);

            case NKikimr::NMiniKQL::TTypeBase::EKind::Dict:
                return ParseDictValue(jsonValue, resultValue, status, type, isQuiet, cellType);

            default:
                // should've been handled in ParseNestedType
                status = TStatus::Fail(
                    EStatusId::UNSUPPORTED, isQuiet ? TString() : TStringBuilder() << "Unsupported type kind: " << type->GetKindAsStr());
                return false;
        }
        return true;
    }

    bool ParseJsonValue(ui64 offset, size_t rowId, simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
        if (Y_UNLIKELY(!SkipErrors && Status.IsFail())) {
            return false;
        }
        if (ParsedRowsCount > 0 && ParsedRows[ParsedRowsCount - 1] == rowId) {
            // duplicated key, ignore
            return true;
        }
        ParsedRows[ParsedRowsCount++] = rowId;
        bool success = false;
        if (TypeMkql) {
            success = ParseNestedValue(std::move(jsonValue), resultValue, Status, TypeMkql, IsOptional);
        } else if (DataSlot != NYql::NUdf::EDataSlot::Json) {
            success = ParseDataType(std::move(jsonValue), resultValue, Status);
        } else {
            success = ParseJsonType(std::move(jsonValue), resultValue, Status);
        }
        Y_DEBUG_ABORT_UNLESS(ValidateObjectRefs(resultValue));
        resultValue = LockObject(std::move(resultValue));

        if (Y_UNLIKELY(!SkipErrors && Status.IsFail())) {
            Status.AddParentIssue(TStringBuilder() << "Failed to parse json string at offset " << offset << ", got parsing error for column '"
                                                   << TStringBuf(Name) << "' with type " << TStringBuf(TypeYson));
            return false;
        }
        return success;
    }

    void ValidateNumberValues(size_t expectedNumberValues, std::span<const ui64> offsets) {
        if (Status.IsFail()) {
            return;
        }
        if (Y_UNLIKELY(!IsOptional && ParsedRowsCount < expectedNumberValues)) {
            Status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                TStringBuilder() << "Failed to parse json messages, found " << expectedNumberValues - ParsedRowsCount
                                 << " missing values in non optional column '" << TStringBuf(Name) << "' with type " << TStringBuf(TypeYson)
                                 << ", buffered offsets: " << JoinSeq(' ', offsets));
        }
    }

    void ClearParsedRows() {
        ParsedRowsCount = 0;
        Status = TStatus::Success();
    }

    bool ClearParsedRow(size_t rowId) {
        if (ParsedRowsCount > 0 && ParsedRows[ParsedRowsCount - 1] == rowId) {
            --ParsedRowsCount;
            return true;
        }
        return false;
    }

    bool ValidateObjectRefs(const NYql::NUdf::TUnboxedValue& value) {
        return TypeMkql ? ValidateObjectRefs(TypeMkql, value) : true;
    }

    bool ValidateObjectRefs(const NKikimr::NMiniKQL::TType* type, const NYql::NUdf::TUnboxedValuePod& value, i32 expectedRefs = 1) {
        if (value.RefCount() == -1) {   // NULL/POD/Embedded String
            return true;
        }
        if (type->IsOptional()) {
            type = AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType();
        }
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data:
                if (value.RefCount() != expectedRefs) {
                    Y_DEBUG_ABORT("%s", (TStringBuilder() << value.RefCount() << "!=" << expectedRefs).c_str());
                    return false;
                }
                return true;

            case NKikimr::NMiniKQL::TTypeBase::EKind::Struct: {
                if (!value.IsBoxed()) {
                    Y_DEBUG_ABORT();
                    return false;
                }

                auto structType = AS_TYPE(NKikimr::NMiniKQL::TStructType, type);
                auto membersCount = structType->GetMembersCount();
                if (value.RefCount() != expectedRefs) {
                    if (membersCount == 0) {
                        return true;
                    }
                    Y_DEBUG_ABORT("%s", (TStringBuilder() << value.RefCount() << "!=" << expectedRefs).c_str());
                    return false;
                }
                auto elements = value.GetElements();
                for (ui32 i = 0; i < membersCount; ++i) {
                    if (!ValidateObjectRefs(structType->GetMemberType(i), elements[i])) {
                        Y_DEBUG_ABORT();
                        return false;
                    }
                }
                break;
            }
            case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple: {
                if (!value.IsBoxed()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                auto tupleType = AS_TYPE(NKikimr::NMiniKQL::TTupleType, type);
                auto elementsCount = tupleType->GetElementsCount();
                if (value.RefCount() != expectedRefs) {
                    if (elementsCount == 0) {   // special case: shared empty list
                        return true;
                    }
                    Y_DEBUG_ABORT("%s", (TStringBuilder() << value.RefCount() << "!=" << expectedRefs).c_str());
                    return false;
                }
                auto elements = value.GetElements();
                for (ui32 i = 0; i < elementsCount; ++i) {
                    if (!ValidateObjectRefs(tupleType->GetElementType(i), elements[i])) {
                        Y_DEBUG_ABORT("%d", i);
                        return false;
                    }
                }
                break;
            }
            case NKikimr::NMiniKQL::TTypeBase::EKind::List: {
                if (!value.IsBoxed()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                if (value.RefCount() != expectedRefs) {
                    if (value.GetListLength() == 0) {   // special case: shared empty list
                        return true;
                    }
                    Y_DEBUG_ABORT("%s", (TStringBuilder() << value.RefCount() << "!=" << expectedRefs).c_str());
                    return false;
                }
                auto listType = AS_TYPE(NKikimr::NMiniKQL::TListType, type);
                auto itemType = listType->GetItemType();
                auto listIterator = value.GetListIterator();

                for (NYql::NUdf::TUnboxedValue itemValue; listIterator.Next(itemValue);) {
                    if (!ValidateObjectRefs(itemType, itemValue.Release())) {
                        Y_DEBUG_ABORT();
                        return false;
                    }
                }
                break;
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Dict: {
                if (!value.IsBoxed()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                if (value.RefCount() != expectedRefs) {
                    if (value.GetDictLength() == 0) {   // special case: shared empty dict
                        return true;
                    }
                    Y_DEBUG_ABORT("%s", (TStringBuilder() << value.RefCount() << "!=" << expectedRefs).c_str());
                    return false;
                }
                auto dictType = AS_TYPE(NKikimr::NMiniKQL::TDictType, type);
                auto keyType = dictType->GetKeyType();
                auto valueType = dictType->GetPayloadType();
                auto dictIterator = value.GetDictIterator();

                for (NYql::NUdf::TUnboxedValue keyValue, payload; dictIterator.NextPair(keyValue, payload);) {
                    if (!ValidateObjectRefs(keyType, keyValue.Release())) {
                        Y_DEBUG_ABORT();
                        return false;
                    }
                    if (!ValidateObjectRefs(valueType, payload.Release())) {
                        Y_DEBUG_ABORT();
                        return false;
                    }
                }
                break;
            }
            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant: {
                auto variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);
                auto alternativeCount = variantType->GetAlternativesCount();
                auto index = value.GetVariantIndex();
                if (!(index < alternativeCount)) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                auto variantItem = value.GetVariantItem();
                if (!ValidateObjectRefs(variantType->GetAlternativeType(index), variantItem.Release())) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                break;
            }
            default:
                Y_ABORT();
        }
        return true;
    }

    TStatus ParseNestedType(const NKikimr::NMiniKQL::TType* type, bool isOptional = false) {
        if (!HolderFactory) {
            return TStatus::Fail(
                EStatusId::UNSUPPORTED, "Structured Json Parsing is disabled. Please contact your system administrator to enable it");
        }
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                auto dataSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, type)->GetDataSlot();
                if (!dataSlot) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "DataType has no DataSlot");
                }
                return TStatus::Success();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                if (isOptional) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Nested optionals is not supported as input type");
                }
                return ParseNestedType(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), true);
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Struct: {
                auto structType = AS_TYPE(NKikimr::NMiniKQL::TStructType, type);
                auto membersCount = structType->GetMembersCount();

                auto [it, inserted] = StructMembers.try_emplace(structType);
                auto& memberNames = it->second;
                if (!inserted) {
                    Y_ENSURE(membersCount == memberNames.size());
                    break;
                }

                for (ui32 idx = 0; idx != membersCount; ++idx) {
                    auto success = ParseNestedType(structType->GetMemberType(idx));
                    if (!success) {
                        return success;
                    }

                    auto [_, nameInserted] = memberNames.emplace(structType->GetMemberName(idx), idx);
                    Y_ENSURE(nameInserted);
                }
                break;
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Dict: {
                auto dictType = AS_TYPE(NKikimr::NMiniKQL::TDictType, type);
                auto keyType = dictType->GetKeyType();
                if (keyType->GetKind() != NKikimr::NMiniKQL::TTypeBase::EKind::Data) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED,
                        TStringBuilder() << "Dict key type: expected either String, or Utf8, but got " << keyType->GetKindAsStr());
                }
                auto keyTypeSlot = AS_TYPE(NKikimr::NMiniKQL::TDataType, keyType)->GetDataSlot();
                if (!IsIn({ NYql::NUdf::EDataSlot::String, NYql::NUdf::EDataSlot::Utf8 }, keyTypeSlot)) {
                    return TStatus::Fail(
                        EStatusId::UNSUPPORTED, TStringBuilder() << "Dict key type: expected either String, or Utf8, but got "
                                                                 << (keyTypeSlot ? NYql::NUdf::GetDataTypeInfo(*keyTypeSlot).Name : "<empty>"));
                }
                return ParseNestedType(dictType->GetPayloadType());
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::List: {
                auto listType = AS_TYPE(NKikimr::NMiniKQL::TListType, type);
                return ParseNestedType(listType->GetItemType());
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple: {
                auto tupleType = AS_TYPE(NKikimr::NMiniKQL::TTupleType, type);
                auto elementsCount = tupleType->GetElementsCount();

                for (ui32 idx = 0; idx != elementsCount; ++idx) {
                    auto success = ParseNestedType(tupleType->GetElementType(idx));
                    if (!success) {
                        return success;
                    }
                }
                break;
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant: {
                auto variantType = AS_TYPE(NKikimr::NMiniKQL::TVariantType, type);
                const auto alternativesCount = variantType->GetAlternativesCount();
                if (alternativesCount == 0) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Variant with zero alternatives");
                }
                for (ui32 index = 0; index != alternativesCount; ++index) {
                    auto alternativeType = variantType->GetAlternativeType(index);
                    auto success = ParseNestedType(alternativeType);
                    if (!success) {
                        return success;
                    }
                }
                break;
            }

            default: {
                return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Unsupported type kind: " << type->GetKindAsStr());
            }
        }
        return TStatus::Success();
    }

    TStatus ExtractDataSlot(const NKikimr::NMiniKQL::TType* type) {
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                auto slotStatus = GetDataSlot(type);
                if (slotStatus.IsFail()) {
                    return slotStatus;
                }
                DataSlot = slotStatus.DetachResult();
                DataTypeName = NYql::NUdf::GetDataTypeInfo(DataSlot).Name;
                return TStatus::Success();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                if (IsOptional) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Nested optionals is not supported as input type");
                }
                IsOptional = true;
                return ExtractDataSlot(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType());
            }
            case NKikimr::NMiniKQL::TTypeBase::EKind::Variant:
            case NKikimr::NMiniKQL::TTypeBase::EKind::Dict:
            case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple:
            case NKikimr::NMiniKQL::TTypeBase::EKind::Struct:
            case NKikimr::NMiniKQL::TTypeBase::EKind::List: {
                TypeMkql = type;
                return ParseNestedType(type);
            }

            default: {
                return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Unsupported type kind: " << type->GetKindAsStr());
            }
        }
    }

    Y_FORCE_INLINE bool ParseDataType(
        simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status) const {
        return ParseDataType(jsonValue, resultValue, status, DataSlot, IsOptional, DataTypeName);
    }

    Y_FORCE_INLINE bool ParseDataType(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status,
        NYql::NUdf::EDataSlot dataSlot, bool isOptional, const TStringBuf dataTypeName, bool isQuiet = false) const {
        simdjson::builtin::ondemand::json_type cellType;
        CHECK_JSON_ERROR(jsonValue.type().get(cellType)) {
            SetParsingError(error, jsonValue, "determine json value type", status, isQuiet);
            return false;
        }

        switch (cellType) {
            case simdjson::builtin::ondemand::json_type::number: {
                switch (dataSlot) {
                    case NYql::NUdf::EDataSlot::Int8:
                        ParseJsonNumber<i8>(jsonValue.get_int64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Int16:
                        ParseJsonNumber<i16>(jsonValue.get_int64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Int32:
                        ParseJsonNumber<i32>(jsonValue.get_int64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Int64:
                        ParseJsonNumber<i64>(jsonValue.get_int64(), resultValue, status, isQuiet);
                        break;

                    case NYql::NUdf::EDataSlot::Uint8:
                        ParseJsonNumber<ui8>(jsonValue.get_uint64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Uint16:
                        ParseJsonNumber<ui16>(jsonValue.get_uint64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Uint32:
                        ParseJsonNumber<ui32>(jsonValue.get_uint64(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Uint64:
                        ParseJsonNumber<ui64>(jsonValue.get_uint64(), resultValue, status, isQuiet);
                        break;

                    case NYql::NUdf::EDataSlot::Double:
                        ParseJsonDouble<double>(jsonValue.get_double(), resultValue, status, isQuiet);
                        break;
                    case NYql::NUdf::EDataSlot::Float:
                        ParseJsonDouble<float>(jsonValue.get_double(), resultValue, status, isQuiet);
                        break;

                    default:
                        status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                            isQuiet ? TString() : TStringBuilder() << "Number value is not expected for data type " << dataTypeName);
                        break;
                }
                if (Y_UNLIKELY(status.IsFail()) && !isQuiet) {
                    status.AddParentIssue(TStringBuilder() << "Failed to parse data type " << dataTypeName << " from json number (raw: '"
                                                           << TruncateString(jsonValue.raw_json_token()) << "')");
                }
                return resultValue.HasValue();
            }

            case simdjson::builtin::ondemand::json_type::string: {
                std::string_view rawString;
                CHECK_JSON_ERROR(jsonValue.get_string(rawString)) {
                    SetParsingError(error, jsonValue, "extract json string", status, isQuiet);
                    return false;
                }

                resultValue = NKikimr::NMiniKQL::ValueFromString(dataSlot, rawString);
                if (Y_UNLIKELY(!resultValue)) {
                    status = TStatus::Fail(EStatusId::BAD_REQUEST, isQuiet ? TString()
                                                                           : TStringBuilder()
                                                                                 << "Failed to parse data type " << dataTypeName
                                                                                 << " from json string: '" << TruncateString(rawString) << "'");
                }
                return resultValue.HasValue();
            }

            case simdjson::builtin::ondemand::json_type::array:
            case simdjson::builtin::ondemand::json_type::object: {
                std::string_view rawJson;
                CHECK_JSON_ERROR(jsonValue.raw_json().get(rawJson)) {
                    SetParsingError(error, jsonValue, "extract json value", status, isQuiet);
                    return false;
                }
                status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                    isQuiet ? TString()
                            : TStringBuilder() << "Found unexpected nested value (raw: '" << TruncateString(rawJson) << "'), expected data type "
                                               << dataTypeName << ", please use Json type for nested values");
                return false;
            }

            case simdjson::builtin::ondemand::json_type::boolean: {
                if (Y_UNLIKELY(dataSlot != NYql::NUdf::EDataSlot::Bool)) {
                    status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                        isQuiet ? TString() : TStringBuilder() << "Found unexpected bool value, expected data type " << dataTypeName);
                    return false;
                }

                bool boolValue;
                CHECK_JSON_ERROR(jsonValue.get_bool().get(boolValue)) {
                    SetParsingError(error, jsonValue, "extract json bool", status, isQuiet);
                    return false;
                }

                resultValue = NYql::NUdf::TUnboxedValuePod(boolValue);
                return true;
            }

            case simdjson::builtin::ondemand::json_type::null: {
                bool isNull;
                CHECK_JSON_ERROR(jsonValue.is_null().get(isNull)) {
                    SetParsingError(error, jsonValue, "parse as null", status, isQuiet);
                    return false;
                }
                if (Y_UNLIKELY(!isOptional)) {
                    status = TStatus::Fail(EStatusId::PRECONDITION_FAILED,
                        isQuiet ? TString()
                                : TStringBuilder() << "Found unexpected null value, expected non optional data type " << dataTypeName);
                    return false;
                }

                resultValue = NYql::NUdf::TUnboxedValuePod();
                return true;
            }
            case simdjson::builtin::ondemand::json_type::unknown: {
                std::string_view rawString;
                CHECK_JSON_ERROR(jsonValue.get_string(rawString)) {
                    SetParsingError(error, jsonValue, "extract json string", status, isQuiet);
                    return false;
                }
                status = TStatus::Fail(EStatusId::BAD_REQUEST, isQuiet ? TString()
                                                                       : TStringBuilder()
                                                                             << "Failed to parse data type " << dataTypeName
                                                                             << " from json string: '" << TruncateString(rawString) << "'");
                return false;
            }
        }
    }

    Y_FORCE_INLINE bool ParseJsonType(
        simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status, bool isQuiet = false) const {
        std::string_view rawJson;
        CHECK_JSON_ERROR(jsonValue.raw_json().get(rawJson)) {
            SetParsingError(error, jsonValue, "extract json value", status, isQuiet);
            return false;
        }

        if (Y_UNLIKELY(!NYql::NDom::IsValidJson(rawJson))) {
            status = TStatus::Fail(
                EStatusId::BAD_REQUEST, isQuiet ? TString() : TStringBuilder() << "Found bad json value: '" << TruncateString(rawJson) << "'");
            return false;
        }

        resultValue = NKikimr::NMiniKQL::MakeString(rawJson);
        return true;
    }

    template <typename TResult, typename TJsonNumber>
    Y_FORCE_INLINE static void ParseJsonNumber(
        simdjson::simdjson_result<TJsonNumber> jsonNumber, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status, bool isQuiet = false) {
        CHECK_JSON_ERROR(jsonNumber.error()) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST,
                isQuiet ? TString() : TStringBuilder() << "Failed to extract json integer number, error: " << simdjson::error_message(error));
            return;
        }

        TJsonNumber number = jsonNumber.value();
        if (Y_UNLIKELY(number < std::numeric_limits<TResult>::min() || std::numeric_limits<TResult>::max() < number)) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, isQuiet ? TString()
                                                                   : TStringBuilder() << "Number is out of range ["
                                                                                      << ToString(std::numeric_limits<TResult>::min()) << ", "
                                                                                      << ToString(std::numeric_limits<TResult>::max()) << "]");
            return;
        }

        resultValue = NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(number));
    }

    template <typename TResult>
    Y_FORCE_INLINE static void ParseJsonDouble(
        simdjson::simdjson_result<double> jsonNumber, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status, bool isQuiet = false) {
        CHECK_JSON_ERROR(jsonNumber.error()) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST,
                isQuiet ? TString() : TStringBuilder() << "Failed to extract json float number, error: " << simdjson::error_message(error));
            return;
        }

        const double number = jsonNumber.value();
        if (Y_UNLIKELY(number < std::numeric_limits<TResult>::lowest() || number > std::numeric_limits<TResult>::max())) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, "Floating point number is out of range");
            return;
        }

        resultValue = NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(number));
    }

    static void SetParsingError(simdjson::error_code error, simdjson::builtin::ondemand::value jsonValue, const TString& description,
        TStatus& status, bool isQuiet = false) {
        status = TStatus::Fail(EStatusId::BAD_REQUEST, isQuiet ? TString()
                                                               : TStringBuilder() << "Failed to " << description << ", current token: '"
                                                                                  << TruncateString(jsonValue.raw_json_token())
                                                                                  << "', error: " << simdjson::error_message(error));
    }

private:
    NKikimr::NMiniKQL::THolderFactory* HolderFactory;
    TStructMembers StructMembers;
    const NKikimr::NMiniKQL::TType* TypeMkql = nullptr;   // for complex types
    NYql::NUdf::EDataSlot DataSlot;
    TStringBuf DataTypeName;
    bool IsOptional = false;
    bool SkipErrors = false;

    size_t ParsedRowsCount = 0;
    std::span<size_t> ParsedRows;
    TStatus Status = TStatus::Success();
};

class TJsonParser: public TTopicParserBase {
public:
    using TBase = TTopicParserBase;
    using TPtr = TIntrusivePtr<TJsonParser>;

    static constexpr ui64 NUMBER_ROWS_LIMIT = 1000;
    static_assert(NUMBER_ROWS_LIMIT <= Max<uint16_t>());

public:
    TJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters)
        : TBase(std::move(consumer), __LOCATION__, config.FunctionRegistry, counters, config.MemoryQuotaManager, "JsonParserAlloc")
        , Config(config)
        , MaxNumberRows(CalculateMaxNumberRows())
        , LogPrefix("TJsonParser: ")
        , ColumnIndexMemory(config.MemoryQuotaManager, "ColumnIndexMemory", counters.ReadGroupSubgroup)
        , SimdJsonMemory(config.MemoryQuotaManager, "SimdJsonMemory", counters.ReadGroupSubgroup)
        , Buffer(Alloc)
        , Counters(counters.CountersSubgroup)
        , PartitionParsingErrors(Counters->GetCounter("ParsingErrors", true))
        , ReadGroupParsingErrors(counters.ReadGroupSubgroup->GetCounter("JsonParsingErrors", true))
    {
        Parser.threaded = false;
    }

    TStatus Init() {
        const TGuard<NKikimr::NMiniKQL::TScopedAlloc> guard(Alloc);
        FillColumnsBuffers();
        Buffer.Reserve(Config.BatchSize, MaxNumberRows);

        YDB_LOG_INFO("JsonParser was created",
            {"logPrefix", LogPrefix},
            {"activeImplementationName", simdjson::get_active_implementation()->name()},
            {"activeImplementationDescription", simdjson::get_active_implementation()->description()},
            {"skipErrors", Config.SkipErrors},
            {"batchSize", Config.BatchSize},
            {"latencyLimit", Config.LatencyLimit},
            {"bufferCellCount", Config.BufferCellCount},
            {"maxNumberRows", MaxNumberRows});
        return InitColumnsParsers();
    }

    ~TJsonParser() override {
        with_lock(Alloc) {
            // Release capacities here: member destructors run after this guard.
            decltype(Columns)().swap(Columns);
            decltype(ParsedValues)().swap(ParsedValues);
            decltype(ParsedValuesBuffer)().swap(ParsedValuesBuffer);
            decltype(ParsedRowsIdxBuffer)().swap(ParsedRowsIdxBuffer);
            decltype(OutputOffsets)().swap(OutputOffsets);
        }
    }

    TStatus InitColumnsParsers() {
        const TGuard<NKikimr::NMiniKQL::TScopedAlloc> guard(Alloc);
        const auto& consumerColumns = Consumer->GetColumns();

        const std::span parsedRowsIdxSpan(ParsedRowsIdxBuffer);

        NonOptionalColumnsCount = 0;
        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            const auto& name = consumerColumns[i].Name;
            const auto& typeYson = consumerColumns[i].TypeYson;
            auto typeStatus = ParseTypeYson(typeYson);
            if (typeStatus.IsFail()) {
                return TStatus(typeStatus).AddParentIssue(TStringBuilder() << "Failed to parse column '" << name << "' type " << typeYson);
            }

            if (auto status = Columns[i].InitParser(name, typeYson, parsedRowsIdxSpan.subspan(i * RowCapacity, RowCapacity),
                    typeStatus.DetachResult(), Config.SkipErrors, Config.StructuredParsing ? HolderFactory.get() : nullptr);
                status.IsFail()) {
                return status.AddParentIssue(TStringBuilder() << "Failed to create parser for column '" << name << "' with type " << typeYson);
            }
            if (!Columns[i].GetIsOptional()) {
                ++NonOptionalColumnsCount;
            }
        }

        return TStatus::Success();
    }

public:
    void ParseRecords(const std::vector<TMessageStreamRecord>& records) override {
        YDB_LOG_TRACE("Add messages to parse",
            {"logPrefix", LogPrefix},
            {"messages", records.size()});

        Y_ENSURE(!Buffer.Finished, "Cannot parse messages with finished buffer");
        for (auto record = records.begin(); record != records.end();) {
            with_lock(Alloc) {
                do {
                    Buffer.AddRecord(*record++);
                } while (record != records.end() && Buffer.NumberValues < MaxNumberRows && Buffer.GetSize() < Config.BatchSize);
            }
            if (Buffer.IsReady() && (Buffer.NumberValues >= MaxNumberRows || Buffer.GetSize() >= Config.BatchSize)) {
                ParseBuffer();
            }
        }

        if (Buffer.IsReady()) {
            if (!Config.LatencyLimit) {
                ParseBuffer();
            } else {
                YDB_LOG_TRACE("Collecting data to parse, skip parsing",
                    {"logPrefix", LogPrefix},
                    {"currentBufferSize", Buffer.GetSize()});
            }
        }
    }

    void Refresh(bool force) override {
        TBase::Refresh(force);

        if (!Buffer.IsReady()) {
            return;
        }

        const auto creationDuration = TInstant::Now() - Buffer.CreationStartTime;
        if (force || creationDuration > Config.LatencyLimit) {
            ParseBuffer();
        } else {
            YDB_LOG_TRACE("Refresh, skip parsing",
                {"logPrefix", LogPrefix},
                {"bufferCreationDuration", creationDuration});
        }
    }

    TStatus ChangeConsumer(IParsedDataConsumer::TPtr consumer) override {
        Refresh(true);

        if (auto status = TBase::ChangeConsumer(std::move(consumer)); status.IsFail()) {
            return status;
        }

        MaxNumberRows = CalculateMaxNumberRows();
        FillColumnsBuffers();
        YDB_LOG_DEBUG("Parser columns count changed",
            {"logPrefix", LogPrefix},
            {"columns", Columns.size()},
            {"consumerColumns", Consumer->GetColumns().size()});

        return InitColumnsParsers();
    }

    std::span<const ui64> GetOffsets() const override {
        return OutputOffsets;
    }

    std::optional<ui64> GetLastMessageOffset() const override {
        return Buffer.Offsets.empty() ? std::nullopt : std::optional<ui64>(Buffer.Offsets.back());
    }

    TValueStatus<std::span<NYql::NUdf::TUnboxedValue>> GetParsedColumn(ui64 columnId) override {
        if (auto status = Columns[columnId].GetStatus(); !Config.SkipErrors && status.IsFail()) {
            return status;
        }
        return ParsedValues[columnId].first(OutputOffsets.size());
    }

protected:
    TStatus DoParsing() override {
        Y_ENSURE(Buffer.IsReady(), "Nothing to parse");
        Y_ENSURE(Buffer.NumberValues <= MaxNumberRows, "Too many values to parse");

        SimdJsonMemory.Reserve(12 * std::max<size_t>(Buffer.GetSize(), simdjson::ondemand::MINIMAL_BATCH_SIZE) +
                               64_KB);   // Approximated upper bound for simdjson memory
        auto [values, size] = Buffer.Finish();
        OutputOffsets.clear();
        ParsingFailedRowCount = 0;

        if (Buffer.NumberValues == 1 || ParseDocuments(values, size, Buffer.Offsets.front(), true).IsFail()) {
            ClearParsedData();
            for (size_t messageId = 0; messageId < Buffer.NumberValues; ++messageId) {
                const size_t begin = Buffer.MessageOffsets[messageId];
                const size_t end = Buffer.MessageOffsets[messageId + 1];
                if (auto status = ParseDocuments(values + begin, end - begin, Buffer.Offsets[messageId]); status.IsFail()) {
                    return status;
                }
            }
        }

        if (ParsingFailedRowCount) {
            PartitionParsingErrors->Add(ParsingFailedRowCount);
            ReadGroupParsingErrors->Add(ParsingFailedRowCount);
        }
        for (auto& column : Columns) {
            column.ValidateNumberValues(OutputOffsets.size(), GetOffsets());
        }
        return TStatus::Success();
    }

    void ClearBuffer() override {
        ClearParsedData();
        Buffer.Clear();
    }

private:
    // Unlike ClearBuffer(), preserve the input for a per-message retry.
    void ClearParsedData() {
        for (size_t i = 0; i < Columns.size(); ++i) {
            auto& parsedColumn = ParsedValues[i];

            auto& column = Columns[i];
            const auto parsedRows = column.GetParsedRows();
            const auto parsedRowsCount = column.GetParsedRowsCount();
            for (size_t rowId = 0; rowId < parsedRowsCount; ++rowId) {
                Y_DEBUG_ABORT_UNLESS(column.ValidateObjectRefs(parsedColumn[parsedRows[rowId]]));
                ClearObject(parsedColumn[parsedRows[rowId]]);
            }

            column.ClearParsedRows();
        }

        OutputOffsets.clear();
        ParsingFailedRowCount = 0;
    }

private:
    // In batch mode every document must fit within its original message.
    // The isolated mode is also the fallback for malformed batches, preserving
    // error diagnostics and SkipErrors recovery at topic message boundaries.
    TStatus ParseDocuments(const char* values, size_t size, ui64 offset, bool batch = false) {
        auto parsingError = [&](const TString& message) {
            return TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder()
                                                             << "Failed to parse json message for offset " << offset << ", " << message
                                                             << " Current data batch: " << TruncateString(std::string_view(values, size)));
        };
        auto handleError = [&](TStatus status) {
            if (!batch && Config.SkipErrors) {
                ++ParsingFailedRowCount;
                return TStatus::Success();
            }
            return status;
        };

        const auto isJsonWhitespace = [](char c) {
            return c == ' ' || c == '\t' || c == '\r' || c == '\n';
        };
        // Empty and whitespace-only messages are valid inputs with no rows.
        // They must not increment parsing error counters, even with SkipErrors.
        if (std::all_of(values, values + size, isJsonWhitespace)) {
            return TStatus::Success();
        }

        size_t expectedMessages = 0;
        if (batch) {
            for (size_t messageId = 0; messageId < Buffer.NumberValues; ++messageId) {
                const size_t begin = Buffer.MessageOffsets[messageId];
                const size_t end = Buffer.MessageOffsets[messageId + 1];
                if (!std::all_of(values + begin, values + end, isJsonWhitespace)) {
                    ++expectedMessages;
                }
            }
        }

        simdjson::ondemand::document_stream documents;
        CHECK_JSON_ERROR(Parser.iterate_many(values, size, std::max(size, simdjson::ondemand::MINIMAL_BATCH_SIZE)).get(documents)) {
            return handleError(parsingError(TStringBuilder() << "json documents was corrupted: " << simdjson::error_message(error)));
        }

        size_t numberDocuments = 0;
        size_t messageId = 0;
        size_t numberMessages = 0;
        size_t lastMessageId = Buffer.NumberValues;
        for (auto it = documents.begin(); it != documents.end(); ++it) {
            auto document = *it;
            CHECK_JSON_ERROR(document.error()) {
                return handleError(parsingError(TStringBuilder() << "json document was corrupted: " << simdjson::error_message(error)));
            }
            simdjson::ondemand::json_type type;
            {
                CHECK_JSON_ERROR(document.type().get(type)) {
                    return handleError(parsingError(simdjson::error_message(error)));
                }
            }
            if (batch && type != simdjson::ondemand::json_type::object && type != simdjson::ondemand::json_type::array) {
                return parsingError("expected json object or array");
            }
            if (batch) {
                const char* location = nullptr;
                {
                    CHECK_JSON_ERROR(document.current_location().get(location)) {
                        return parsingError(simdjson::error_message(error));
                    }
                }
                const size_t start = location - values;
                while (messageId + 1 < Buffer.NumberValues && start >= Buffer.MessageOffsets[messageId + 1]) {
                    ++messageId;
                }
                offset = Buffer.Offsets[messageId];
                const size_t end = Buffer.MessageOffsets[messageId + 1];
                // source() includes the closing brace/bracket; inspect it before
                // consuming any fields. current_index() alone is insufficient:
                // the first document's index may include leading whitespace.
                const auto source = it.source();
                const size_t sourceBegin = source.data() - values;
                if (start < Buffer.MessageOffsets[messageId] || start >= end || sourceBegin > end || source.size() > end - sourceBegin) {
                    return parsingError("json document crosses message boundary");
                }
                if (lastMessageId != messageId) {
                    ++numberMessages;
                    lastMessageId = messageId;
                }
            }
            ++numberDocuments;
            bool stopMessage = false;
            auto parseObject = [&](simdjson::simdjson_result<simdjson::ondemand::object> object) -> TStatus {
                CHECK_JSON_ERROR(object.error()) {
                    stopMessage = true;
                    return handleError(parsingError(simdjson::error_message(error)));
                }
                const size_t rowId = OutputOffsets.size();
                EnsureRowCapacity(rowId + 1);
                size_t parsedNonOptional = 0;
                bool skipRow = false;
                bool unrecoverableColumnError = false;
                for (auto item : object) {
                    CHECK_JSON_ERROR(item.error()) {
                        ClearRowBuffer(rowId);
                        stopMessage = true;
                        return handleError(parsingError(TStringBuilder() << "json item was corrupted: " << simdjson::error_message(error)));
                    }
                    const auto it = ColumnsIndex.find(item.escaped_key().value());
                    if (it == ColumnsIndex.end()) {
                        continue;
                    }
                    const size_t columnId = it->second;
                    auto& column = Columns[columnId];
                    auto& value = ParsedValues[columnId][rowId];
                    const bool valueWasSet = bool(value);
                    const bool success = column.ParseJsonValue(offset, rowId, item.value(), value);
                    if (!column.GetIsOptional() && !valueWasSet) {
                        ++parsedNonOptional;
                    }
                    skipRow |= Config.SkipErrors && !success;
                    simdjson::ondemand::document& parsedDocument = document.value_unsafe();
                    if (!success && !parsedDocument.is_alive()) {
                        // Preserve the column diagnostic, but never continue an
                        // ondemand stream after its iterator has been abandoned.
                        unrecoverableColumnError = true;
                        break;
                    }
                }
                if (Config.SkipErrors && (skipRow || parsedNonOptional != NonOptionalColumnsCount)) {
                    ClearRowBuffer(rowId);
                    ++ParsingFailedRowCount;
                    if (unrecoverableColumnError) {
                        stopMessage = true;
                        return TStatus::Success();
                    }
                    return TStatus::Success();
                }
                OutputOffsets.push_back(offset);
                if (unrecoverableColumnError) {
                    stopMessage = true;
                    return TStatus::Success();
                }
                return TStatus::Success();
            };

            if (type == simdjson::ondemand::json_type::array) {
                for (auto element : document.get_array()) {
                    CHECK_JSON_ERROR(element.error()) {
                        return handleError(parsingError(simdjson::error_message(error)));
                    }
                    auto status = parseObject(element.get_object());
                    if (status.IsFail() || stopMessage) {
                        if (batch && stopMessage) {
                            return parsingError("json iterator was abandoned");
                        }
                        return status;
                    }
                }
            } else {
                auto status = parseObject(document.get_object());
                if (status.IsFail() || stopMessage) {
                    if (batch && stopMessage) {
                        return parsingError("json iterator was abandoned");
                    }
                    return status;
                }
            }
        }

        if (const auto truncatedBytes = documents.truncated_bytes()) {
            return handleError(parsingError(TStringBuilder() << "json batch truncated bytes: " << truncatedBytes));
        }
        if (batch && numberMessages != expectedMessages) {
            return parsingError("message without a complete json document");
        }
        if (!numberDocuments) {
            return handleError(parsingError("expected at least one json object, got empty message"));
        }
        return TStatus::Success();
    }

    void EnsureRowCapacity(size_t required) {
        if (required <= RowCapacity) {
            return;
        }
        const size_t capacity = std::max(required, 2 * RowCapacity);
        // Allocate both buffers before moving values: a quota failure must leave
        // the old spans and object references intact for ClearBuffer().
        TParserVector<NYql::NUdf::TUnboxedValue> values(Columns.size() * capacity);
        TParserVector<size_t> rows(Columns.size() * capacity);
        OutputOffsets.reserve(capacity);
        for (size_t columnId = 0; columnId < Columns.size(); ++columnId) {
            for (size_t rowId = 0; rowId < RowCapacity; ++rowId) {
                values[columnId * capacity + rowId] = std::move(ParsedValues[columnId][rowId]);
            }
            const auto oldRows = Columns[columnId].GetParsedRows();
            std::copy_n(oldRows.begin(), Columns[columnId].GetParsedRowsCount(), rows.begin() + columnId * capacity);
        }
        ParsedValuesBuffer.swap(values);
        ParsedRowsIdxBuffer.swap(rows);
        RowCapacity = capacity;
        for (size_t columnId = 0; columnId < Columns.size(); ++columnId) {
            ParsedValues[columnId] = std::span(ParsedValuesBuffer).subspan(columnId * capacity, capacity);
            Columns[columnId].SetParsedRows(std::span(ParsedRowsIdxBuffer).subspan(columnId * capacity, capacity));
        }
    }

    void ClearRowBuffer(size_t outputRowId) {
        for (size_t columnId = 0; columnId < Columns.size(); ++columnId) {
            if (Columns[columnId].ClearParsedRow(outputRowId)) {
                Y_DEBUG_ABORT_UNLESS(Columns[columnId].ValidateObjectRefs(ParsedValues[columnId][outputRowId]));
                ClearObject(ParsedValues[columnId][outputRowId]);
            }
        }
    }

    void FillColumnsBuffers() {
        const TGuard<NKikimr::NMiniKQL::TScopedAlloc> guard(Alloc);
        const auto& consumerColumns = Consumer->GetColumns();

        if (consumerColumns.size() > ColumnsIndex.size()) {
            constexpr size_t indexSlotSize = sizeof(decltype(ColumnsIndex)::value_type) + 1;
            constexpr size_t indexOverhead = 128;
            ColumnIndexMemory.Reserve(3 * (std::max(ColumnsIndex.capacity(), consumerColumns.size()) + 1) * indexSlotSize + indexOverhead);
        }

        ColumnsIndex.clear();
        ColumnsIndex.reserve(consumerColumns.size());
        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            ColumnsIndex.emplace(std::string_view(consumerColumns[i].Name), i);
        }

        const auto columns = consumerColumns.size();
        RowCapacity = MaxNumberRows;
        const auto cells = columns * RowCapacity;
        ParsedValuesBuffer.resize(cells);
        ParsedRowsIdxBuffer.resize(cells);
        Columns.resize(columns);
        if (MaxNumberRows > OutputOffsets.capacity()) {
            OutputOffsets.reserve(std::max<size_t>(MaxNumberRows, 2 * OutputOffsets.capacity()));
        }
        const std::span valuesBufferSpan(ParsedValuesBuffer);

        ParsedValues.resize(consumerColumns.size());
        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            ParsedValues[i] = valuesBufferSpan.subspan(i * RowCapacity, RowCapacity);
        }
    }

    ui16 CalculateMaxNumberRows() const {
        return std::min((Config.BufferCellCount - 1) / Consumer->GetColumns().size() + 1, NUMBER_ROWS_LIMIT);
    }

private:
    const TJsonParserConfig Config;
    ui16 MaxNumberRows = 0;
    size_t RowCapacity = 0;
    const TString LogPrefix;

    TMemoryQuota ColumnIndexMemory;
    TMemoryQuota SimdJsonMemory;
    TParserVector<ui64> OutputOffsets = {};
    TParserVector<TColumnParser> Columns;
    TParserVector<size_t> ParsedRowsIdxBuffer;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    simdjson::ondemand::parser Parser;
    TParserVector<NYql::NUdf::TUnboxedValue> ParsedValuesBuffer;
    TParserVector<std::span<NYql::NUdf::TUnboxedValue>> ParsedValues;
    NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TDynamicCounters::TCounterPtr PartitionParsingErrors;
    NMonitoring::TDynamicCounters::TCounterPtr ReadGroupParsingErrors;
    ui64 ParsingFailedRowCount = 0;
    ui64 NonOptionalColumnsCount = 0;
};

}   // anonymous namespace

TValueStatus<ITopicParser::TPtr> CreateJsonParser(
    IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters) {
    TJsonParser::TPtr parser = MakeIntrusive<TJsonParser>(consumer, config, counters);
    if (auto status = parser->Init(); status.IsFail()) {
        return status;
    }

    return ITopicParser::TPtr(parser);
}

TJsonParserConfig CreateJsonParserConfig(const TRowDispatcherSettings::TJsonParserSettings& parserConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, bool skipErrors) {
    return {
        .FunctionRegistry = functionRegistry,
        .BatchSize = parserConfig.GetBatchSizeBytes(),
        .LatencyLimit = parserConfig.GetBatchCreationTimeout(),
        .BufferCellCount = parserConfig.GetBufferCellCount(),
        .SkipErrors = skipErrors,
        .StructuredParsing = parserConfig.GetStructuredParsing(),
    };
}

}   // namespace NFq::NRowDispatcher
