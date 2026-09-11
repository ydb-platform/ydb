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

namespace NKikimr::NKqp::NLogToDB {

using namespace NActors::NStructuredLog;

class TBaseDBLogColumn {
public:
    struct TDatabaseSettings {
        TString Extra;
        bool IsPK {false};
        bool IsNotNull {false};
        bool IsShardingKey {false};

        static TDatabaseSettings PK(const TString& extra = {}) {
            return TDatabaseSettings {.Extra = extra, .IsPK = true, .IsNotNull = true};
        }

        static TDatabaseSettings ShardingKey(const TString& extra = {}) {
            return TDatabaseSettings {.Extra = extra, .IsNotNull = true, .IsShardingKey = true};
        }

        static TDatabaseSettings PKShardingKey(const TString& extra = {}) {
            return TDatabaseSettings {.Extra = extra, .IsPK = true, .IsNotNull = true, .IsShardingKey = true};
        }

        static TDatabaseSettings NotNull(const TString& extra = {}) {
            return TDatabaseSettings {.Extra = extra, .IsNotNull = true};
        }
    };

    TBaseDBLogColumn() = default;
    TBaseDBLogColumn(TString name, TString type)
        : TBaseDBLogColumn(std::move(name), std::move(type), TDatabaseSettings()) {}
    TBaseDBLogColumn(TString name, TString type, TDatabaseSettings settings)
        : Name(std::move(name))
        , Type(std::move(type))
        , Settings(std::move(settings)) {}
    virtual ~TBaseDBLogColumn() = default;

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
    virtual bool Write(const NActors::NStructuredLog::TLogMessage&) = 0;
    virtual std::shared_ptr<arrow::Array> MakeArray() = 0;
};

template <typename T>
class TTypedDBLogColumn : public TBaseDBLogColumn {
public:
    using TValueType = T;
    using TArrowBuilderType = TArrowTypeMapper<TValueType>::TArrowBuilderType;
    using TArrowArrayType = TArrowTypeMapper<TValueType>::TArrowArrayType;
    static constexpr const char* TypeName = TArrowTypeMapper<TValueType>::TypeName;

    std::shared_ptr<TArrowBuilderType> Builder;

    TTypedDBLogColumn(TString name, TDatabaseSettings settings)
        : TBaseDBLogColumn(std::move(name), TypeName, std::move(settings)),
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

    bool Write(const NActors::NStructuredLog::TLogMessage&) override {
        // @todo Удалить реализацию -заглушку
        TValueType value{};
        return AppendValue(value);
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
    TDBLogMessageIdColumn() : TBase("id", TDatabaseSettings::PKShardingKey()), CurrentValue(Now().MilliSeconds()) {} // @todo Достаточно ли уникальности?

    TDBLogMessageIdColumn(ui64 currentValue) : TBase("id", TDatabaseSettings::PKShardingKey()), CurrentValue(currentValue)  {}

    bool Write(const NActors::NStructuredLog::TLogMessage& ) override {
        return AppendValue(CurrentValue++);
    }
};

// Write message time to column
class TDBLogMessageTimeColumn : public TTypedDBLogColumn<TInstant> {
public:
    using TBase = TTypedDBLogColumn<TInstant>;

    TDBLogMessageTimeColumn() : TBase("timestamp", TDatabaseSettings::PKShardingKey())  {
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(message.Time);
    }
};

// Write message priority to column
class TDBLogMessagePrioColumn : public TTypedDBLogColumn<ui16> {
public:
    using TBase = TTypedDBLogColumn<ui16>;

    TDBLogMessagePrioColumn() : TBase("priority", TDatabaseSettings()) {
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(static_cast<ui16>(message.Priority));
    }
};

// Write message text to column
class TDBLogMessageTextColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    TDBLogMessageTextColumn() : TBase("message", TDatabaseSettings()) {
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        return AppendValue(message.TextMessage);
    }
};

// Write message location to column
class TDBLogMessageLocationColumn : public TTypedDBLogColumn<TString> {
public:
    using TBase = TTypedDBLogColumn<TString>;

    TDBLogMessageLocationColumn() : TBase("location", TDatabaseSettings()) {
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        TString location;
        if (message.FileName) {
            location = TString(message.FileName) + ':' + ToString(message.LineNumber);
        }
        return AppendValue(location);
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

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        TStringValueExtractor extractor;
        auto value = extractor.ExtractValue(message.StructuredMessage, KeyName);
        if (value.has_value()) {
            return AppendValue(value.value());
        } else {
            return AppendNull();
        }
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

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        using TExtractor = TNativeValueExtractor<T>;
        TExtractor extractor;
        auto value = extractor.ExtractValue(message.StructuredMessage, KeyName);
        switch (value.first) {
            case TExtractor::TResultKind::Ok:
                if (!value.second.has_value()) {
                    return false;
                }
                return TBase::AppendValue(value.second.value());
            case TExtractor::TResultKind::NoCast:
                return TBase::AppendNull(); // @todo Обработка ошибок
            case TExtractor::TResultKind::NoValue:
                return TBase::AppendNull();
            break;
        }
    }
};

using TDBLogColumnUint64 = TDBLogMessageTypedValueColumn<ui64>;

}
