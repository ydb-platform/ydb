#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.pb.h>

#include <library/cpp/protobuf/dynamic_prototype/dynamic_prototype.h>

#include <util/generic/ptr.h>

enum EProtoFormat {
    PF_PROTOBIN = 0,
    PF_PROTOTEXT = 1,
    PF_JSON = 2,
};

enum class EEnumFormat {
    Number = 0,
    Name = 1,
    FullName = 2,
};

enum class ERecursionTraits {
    //! Падать, если на входе имеется рекурсивно определённый тип.
    Fail = 0,
    //! Игнорировать все поля с рекурсивно определённым типом.
    Ignore = 1,
    //! Возвращать поля с рекурсивным типом в виде сериализованной строки
    Bytes = 2,
};

struct TProtoTypeConfig {
    //! Имя сообщения.
    TString MessageName;
    //! Сериализованные метаданные.
    TString Metadata;
    //! Количество байт, которые надо отступить
    //! от начала каждого блока данных.
    ui32 SkipBytes = 0;
    //! Формат сериализации proto-сообщений.
    EProtoFormat ProtoFormat = PF_PROTOBIN;
    //! Формат представление значений Enum.
    EEnumFormat EnumFormat = EEnumFormat::Number;
    //! Способ интерпретации рекурсивно определённых типов.
    ERecursionTraits Recursion = ERecursionTraits::Fail;
    //! Выдать бинарными строками, если SERIALIZATION_PROTOBUF.
    //! Если SERIALIZATION_YT, то для рекурсивных структур поведение зависит от Recursion.
    bool YtMode = false;
    //! Заворачивать ли списочные типы в Optional.
    bool OptionalLists = false;
    //! Заполнять ли пустые Optional типы дефолтным значением (только для proto3).
    bool SyntaxAware = false;
};

struct TProtoTypeConfigOptions {
    //! Количество байт, которые надо отступить
    //! от начала каждого блока данных.
    ui32 SkipBytes = 0;
    //! Формат сериализации proto-сообщений.
    EProtoFormat ProtoFormat = PF_PROTOBIN;
    //! Формат представление значений Enum.
    EEnumFormat EnumFormat = EEnumFormat::Number;
    //! Способ интерпретации рекурсивно определённых типов.
    ERecursionTraits Recursion = ERecursionTraits::Fail;
    //! Выдать бинарными строками, если SERIALIZATION_PROTOBUF.
    //! Если SERIALIZATION_YT, то для рекурсивных структур поведение зависит от Recursion.
    bool YtMode = false;
    //! Заворачивать ли списочные типы в Optional.
    bool OptionalLists = false;
    //! Заполнять ли пустые Optional типы дефолтным значением (только для proto3).
    bool SyntaxAware = false;

    TProtoTypeConfigOptions& SetProtoFormat(EProtoFormat value) {
        ProtoFormat = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetSkipBytes(ui32 value) {
        SkipBytes = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetEnumFormat(EEnumFormat value) {
        EnumFormat = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetRecursionTraits(ERecursionTraits value) {
        Recursion = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetYtMode(bool value) {
        YtMode = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetOptionalLists(bool value) {
        OptionalLists = value;
        return *this;
    }

    TProtoTypeConfigOptions& SetSyntaxAware(bool value) {
        SyntaxAware = value;
        return *this;
    }
};

TString GenerateProtobufTypeConfig(
    const NProtoBuf::Descriptor* descriptor,
    const TProtoTypeConfigOptions& options = TProtoTypeConfigOptions());

template <typename T>
inline TString GenerateProtobufTypeConfig(
    const TProtoTypeConfigOptions& options = TProtoTypeConfigOptions()) {
    return GenerateProtobufTypeConfig(T::descriptor(), options);
}

TProtoTypeConfig ParseTypeConfig(const TStringBuf& config);

using TDynamicInfoRef = TIntrusivePtr<class TDynamicInfo>;

class TDynamicInfo: public TSimpleRefCount<TDynamicInfo> {
public:
    TDynamicInfo(TDynamicPrototypePtr);
    ~TDynamicInfo();

    static TDynamicInfoRef Create(const TStringBuf& typeConfig);

    const NProtoBuf::Descriptor* Descriptor() const;

    //! Текущий формат представление значений Enum.
    EEnumFormat GetEnumFormat() const;

    //! Текущий способ интерпретации рекурсивно определённых типов.
    ERecursionTraits GetRecursionTraits() const;

    bool GetYtMode() const;

    bool GetOptionalLists() const;

    bool GetSyntaxAware() const;

    TAutoPtr<NProtoBuf::Message> MakeProto();

    //! \param data сериализованное protobuf-сообщение.
    TAutoPtr<NProtoBuf::Message> Parse(const TStringBuf& data);

    //! \param proto protobuf-сообщение.
    TString Serialize(const NProtoBuf::Message& proto);

private:
    TDynamicPrototypePtr DynamicPrototype;
    EEnumFormat EnumFormat_;
    EProtoFormat ProtoFormat_;
    ERecursionTraits Recursion_;
    bool YtMode_;
    ui32 SkipBytes_;
    bool OptionalLists_;
    bool SyntaxAware_;
};
