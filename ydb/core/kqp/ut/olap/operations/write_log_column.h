#pragma once

#include "arrow_type_mapping.h"

#include <ydb/library/actors/struct_log/key_name.h>
#include <ydb/library/actors/struct_log/log_sink.h>
#include <ydb/library/actors/struct_log/native_value_extractor.h>
#include <ydb/library/actors/struct_log/string_value_extractor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/key_value_metadata.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/system/types.h>

#include <string>
#include <vector>

namespace NKikimr::NKqp::NSchematizedLog {

using namespace NActors::NStructuredLog;

class TSchematizedLogColumn {
public:
    struct TDatabaseSettings {
        TString Extra;
        bool IsPK {false};
        bool IsNotNull {false};
        bool IsShardingKey {false};
        bool IsDictionary {false};

        TDatabaseSettings& SetExtra(const TString& extra) {
            Extra = extra;
            return *this;
        }

        TDatabaseSettings& SetPK(bool isPK) {
            IsPK = isPK;
            return *this;
        }

        TDatabaseSettings& SetNotNull(bool isNotNull) {
            IsNotNull = isNotNull;
            return *this;
        }

        TDatabaseSettings& SetShardingKey(bool isShardingKey) {
            IsShardingKey = isShardingKey;
            return *this;
        }

        TDatabaseSettings& SetDictionary(bool isDictionary) {
            IsDictionary = isDictionary;
            return *this;
        }

        static TDatabaseSettings PK() {
            return TDatabaseSettings {.IsPK = true, .IsNotNull = true};
        }

        static TDatabaseSettings ShardingKey() {
            return TDatabaseSettings {.IsNotNull = true, .IsShardingKey = true};
        }

        static TDatabaseSettings PKShardingKey() {
            return TDatabaseSettings {.IsPK = true, .IsNotNull = true, .IsShardingKey = true};
        }

        static TDatabaseSettings NotNull() {
            return TDatabaseSettings {.IsNotNull = true};
        }

        static TDatabaseSettings Dictionary() {
            return TDatabaseSettings {.IsDictionary = true};
        }
    };

    TSchematizedLogColumn() = default;
    TSchematizedLogColumn(TString name, TString type)
        : TSchematizedLogColumn(std::move(name), std::move(type), TDatabaseSettings()) {}
    TSchematizedLogColumn(TString name, TString type, TDatabaseSettings settings)
        : Name(std::move(name))
        , Type(std::move(type))
        , Settings(std::move(settings)) {}
    virtual ~TSchematizedLogColumn() = default;

    const TString Name;
    const TString Type;
    const TDatabaseSettings Settings;

    virtual std::shared_ptr<arrow::DataType> GetArrowDataType() const = 0;

    std::shared_ptr<arrow::Field> MakeArrowField() const {
        auto arrowSchemaField = std::make_shared<arrow::KeyValueMetadata>(
            std::vector<std::string>{"ydb.type"},
            std::vector<std::string>{std::string(Type)});
        return arrow::field(std::string(Name), GetArrowDataType(), !Settings.IsNotNull, arrowSchemaField);
    }

    virtual void Reset() = 0;

    enum class TWriteResultKind {
        Success,
        DummyValueInsteadOfNull,
        DummyValueInsteadOfCastError,
        NullInsteadOfCastError,
        ArrowError,
        UnknownError
    };
    struct TWriteResult {
        TWriteResultKind Kind{TWriteResultKind::Success};
        TString Value;

        TWriteResult() = default;
        TWriteResult(TWriteResultKind kind): Kind(kind) {}
        TWriteResult(TWriteResultKind kind, const TString& value): Kind(kind), Value(value) {}
    };
    virtual TWriteResult Write(const NActors::NStructuredLog::TLogMessage&) = 0;
    virtual std::shared_ptr<arrow::Array> MakeArray() = 0;
};

template <typename T>
class TTypedDBLogColumn : public TSchematizedLogColumn {
public:
    using TValueType = T;
    using TArrowBuilderType = TArrowTypeMapper<TValueType>::TArrowBuilderType;
    using TArrowArrayType = TArrowTypeMapper<TValueType>::TArrowArrayType;
    static constexpr const char* TypeName = TArrowTypeMapper<TValueType>::TypeName;

    std::shared_ptr<TArrowBuilderType> Builder;

    TTypedDBLogColumn(TString name, TDatabaseSettings settings)
        : TSchematizedLogColumn(std::move(name), TypeName, std::move(settings)),
        Builder(TArrowTypeMapper<TValueType>::CreateBuilder()) {}

    std::shared_ptr<arrow::DataType> GetArrowDataType() const override {
        return TArrowTypeMapper<TValueType>::GetArrowDataType();
    }

    void Reset() override {
        Builder->Reset();
    }

    bool AppendValue(const TValueType& value) {
        return TArrowTypeMapper<TValueType>::AppendValue(*Builder, value);
    }

    bool AppendNull() {
        return TArrowTypeMapper<TValueType>::AppendNull(*Builder);
    }

    std::shared_ptr<arrow::Array> MakeArray() override {
        std::shared_ptr<arrow::Array> result;
        auto status = Builder->Finish(&result);
        if (!status.ok()) {
            return nullptr;
        }
        return result;
    }
};

// Write message unique id to column
class TDBLogMessageIdColumn : public TTypedDBLogColumn<ui64> {
public:
    using TBase = TTypedDBLogColumn<ui64>;

    ui64 CurrentValue;

    TDBLogMessageIdColumn(ui64 currentValue=0) : TBase("id", TDatabaseSettings::PKShardingKey()), CurrentValue(currentValue)  {}

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& ) override {
        return AppendValue(CurrentValue++) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message time to column
class TDBLogMessageTimeColumn : public TTypedDBLogColumn<TInstant> {
public:
    using TBase = TTypedDBLogColumn<TInstant>;

    TDBLogMessageTimeColumn() : TBase("timestamp", TDatabaseSettings::PKShardingKey())  {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(message.Time) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message priority to column
class TDBLogMessagePrioColumn : public TTypedDBLogColumn<ui16> {
public:
    using TBase = TTypedDBLogColumn<ui16>;

    TDBLogMessagePrioColumn() : TBase("priority", TDatabaseSettings().SetNotNull(true)) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(static_cast<ui16>(message.Priority)) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message priority to column
class TDBLogMessageNodeIdColumn : public TTypedDBLogColumn<ui16> {
public:
    using TBase = TTypedDBLogColumn<ui16>;

    TDBLogMessageNodeIdColumn() : TBase("node_id", TDatabaseSettings::PK().SetNotNull(true)) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(static_cast<ui16>(message.NodeId)) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message text to column
class TDBLogMessageTextColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    TDBLogMessageTextColumn() : TBase("message", TDatabaseSettings().SetNotNull(true).SetDictionary(true)) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(message.TextMessage) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message location to column
class TDBLogMessageLocationColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    TDBLogMessageLocationColumn() : TBase("location", TDatabaseSettings().SetNotNull(true).SetDictionary(true)) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        TString location;
        if (message.FileName) {
            location = TString(message.FileName) + ':' + ToString(message.LineNumber);
        }
        return AppendValue(location) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message writer error to column
class TDBLogMessageErrorColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    TDBLogMessageErrorColumn() : TBase("write_error", TDatabaseSettings().SetDictionary(true)) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& ) override {
        return TWriteResultKind::UnknownError;
    }

    TWriteResult Write(const TString& error) {
        if (error.empty()) {
            return AppendNull()? TWriteResultKind::Success : TWriteResultKind::ArrowError;
        } else {
            return AppendValue(error)? TWriteResultKind::Success : TWriteResultKind::ArrowError;
        }
    }
};

// Write message structured value to column as string
class TDBLogMessageStringValueColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    std::vector<TKeyName> KeyName;

    TDBLogMessageStringValueColumn(const TString& columnName, const std::vector<TKeyName>& keyName) :
        TBase(columnName, TDatabaseSettings()),
        KeyName(keyName) {
    }

    TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        TStringValueExtractor extractor;
        auto value = extractor.ExtractValue(message.StructuredMessage, KeyName);
        if (value.has_value()) {
            return AppendValue(value.value()) ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
        }
        if (TSchematizedLogColumn::Settings.IsNotNull) {
            return AppendValue("") ? TWriteResultKind::DummyValueInsteadOfNull : TWriteResultKind::ArrowError;
        }
        return AppendNull() ? TWriteResultKind::Success : TWriteResultKind::ArrowError;
    }
};

// Write message structured value to column as typed value
template <typename T>
class TDBLogMessageTypedValueColumn : public TTypedDBLogColumn<T> {
public:
    using TBase = TTypedDBLogColumn<T>;

    std::vector<TKeyName> KeyName;

    TDBLogMessageTypedValueColumn(const TString& columnName, const std::vector<TKeyName>& keyName, const TBase::TDatabaseSettings& settings = {}) :
        TBase(columnName, settings),
        KeyName(keyName) {
    }

    TBase::TWriteResult Write(const NActors::NStructuredLog::TLogMessage& message) override {
        using TThisWriteResult = TBase::TWriteResult;
        using TThisWriteResultKind = TBase::TWriteResultKind;

        using TExtractor = TNativeValueExtractor<T>;
        TExtractor extractor;
        auto value = extractor.ExtractValue(message.StructuredMessage, KeyName);
        switch (value.first) {
            case TExtractor::TResultKind::Ok:
                return TBase::AppendValue(value.second.value())
                    ? TThisWriteResultKind::Success
                    : TThisWriteResultKind::ArrowError;
            case TExtractor::TResultKind::NoCast:
                if (TSchematizedLogColumn::Settings.IsNotNull) {
                    TStringValueExtractor stringExtractor;
                    auto strValue = stringExtractor.ExtractValue(message.StructuredMessage, KeyName).value_or("");
                    return TBase::AppendValue(T{})
                        ? TThisWriteResult(TThisWriteResultKind::DummyValueInsteadOfCastError, strValue)
                        : TThisWriteResult(TThisWriteResultKind::ArrowError);
                } else {
                    TStringValueExtractor stringExtractor;
                    auto strValue = stringExtractor.ExtractValue(message.StructuredMessage, KeyName).value_or("");
                    return TBase::AppendNull()
                        ? TThisWriteResult(TThisWriteResultKind::NullInsteadOfCastError, strValue)
                        : TThisWriteResult(TThisWriteResultKind::ArrowError);
                }
            case TExtractor::TResultKind::NoValue:
                if (TSchematizedLogColumn::Settings.IsNotNull) {
                    TStringValueExtractor stringExtractor;
                    auto strValue = stringExtractor.ExtractValue(message.StructuredMessage, KeyName).value_or("");
                    return TBase::AppendValue(T{})  // @todo Generate dummy value
                        ? TThisWriteResult(TThisWriteResultKind::DummyValueInsteadOfNull, strValue)
                        : TThisWriteResult(TThisWriteResultKind::ArrowError);
                } else {
                    return TBase::AppendNull()
                        ? TThisWriteResultKind::Success
                        : TThisWriteResultKind::ArrowError;
                }
        }
        return TThisWriteResultKind::UnknownError;
    }
};

using TDBLogColumnBool = TDBLogMessageTypedValueColumn<bool>;
using TDBLogColumnInt8 = TDBLogMessageTypedValueColumn<i8>;
using TDBLogColumnUint8 = TDBLogMessageTypedValueColumn<ui8>;
using TDBLogColumnInt16 = TDBLogMessageTypedValueColumn<i16>;
using TDBLogColumnUint16 = TDBLogMessageTypedValueColumn<ui16>;
using TDBLogColumnInt32 = TDBLogMessageTypedValueColumn<i32>;
using TDBLogColumnUint32 = TDBLogMessageTypedValueColumn<ui32>;
using TDBLogColumnInt64 = TDBLogMessageTypedValueColumn<i64>;
using TDBLogColumnUint64 = TDBLogMessageTypedValueColumn<ui64>;
using TDBLogColumnFloat = TDBLogMessageTypedValueColumn<float>;
using TDBLogColumnDouble = TDBLogMessageTypedValueColumn<double>;
using TDBLogColumnUtf8 = TDBLogMessageTypedValueColumn<TString>;
using TDBLogColumnTimestamp = TDBLogMessageTypedValueColumn<TInstant>;

}
