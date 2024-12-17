#include "type_io.h"

#include "builder.h"
#include "type_constructors.h"
#include "type_factory.h"

#include <util/generic/overloaded.h>

#include <library/cpp/yson_pull/read_ops.h>

#include <util/stream/mem.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>
#include <util/generic/scope.h>

namespace NTi::NIo {
    namespace {
        class TYsonDeserializer: private TNonCopyable {
        public:
            TYsonDeserializer(IPoolTypeFactory* factory, NYsonPull::TReader* reader)
                : Factory_(factory)
                , Reader_(reader)
            {
            }

        public:
            const TType* ReadType() {
                if (++Depth_ > 100) {
                    ythrow TDeserializationException() << "types are nested too deep";
                }

                Y_DEFER {
                    Depth_--;
                };

                auto event = Reader_->NextEvent();

                if (event.Type() == NYsonPull::EEventType::BeginStream) {
                    event = Reader_->NextEvent();
                }

                if (event.Type() == NYsonPull::EEventType::EndStream) {
                    if (Depth_ == 1) {
                        return nullptr;
                    } else {
                        ythrow TDeserializationException() << "unexpected end of stream";
                    }
                }

                if (event.Type() == NYsonPull::EEventType::Scalar && event.AsScalar().Type() == NYsonPull::EScalarType::String) {
                    return ReadTypeFromData(TypeNameStringToEnum(event.AsString()), std::monostate{});
                } else if (event.Type() == NYsonPull::EEventType::BeginMap) {
                    return ReadTypeFromMap();
                } else {
                    ythrow TDeserializationException() << "type must be either a string or a map";
                }
            }

        private:
            struct TDictData {
                const TType *Key, *Value;
            };
            struct TDecimalData {
                ui8 Precision, Scale;
            };
            using TTypeData = std::variant<
                std::monostate,
                TDictData,
                TDecimalData,
                TStructBuilderRaw,
                TTupleBuilderRaw,
                TTaggedBuilderRaw>;

            const TType* ReadTypeFromMap() {
                TMaybe<ETypeName> typeName;
                TTypeData data;

                while (true) {
                    auto event = Reader_->NextEvent();
                    if (event.Type() == NYsonPull::EEventType::Key) {
                        auto mapKey = event.AsString();

                        if (mapKey == "type_name") {
                            if (typeName.Defined()) {
                                ythrow TDeserializationException() << R"(duplicate key R"(type_name"))";
                            } else {
                                typeName = TypeNameStringToEnum(ReadString(R"("type_name")"));
                            }
                        } else if (mapKey == "item") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TTaggedBuilderRaw(*Factory_);
                            }
                            if (std::holds_alternative<TTaggedBuilderRaw>(data)) {
                                auto& builder = std::get<TTaggedBuilderRaw>(data);
                                if (!builder.HasItem()) {
                                    builder.SetItem(ReadType());
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "item")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "item")";
                            }
                        } else if (mapKey == "key") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TDictData{nullptr, nullptr};
                            }
                            if (std::holds_alternative<TDictData>(data)) {
                                auto& dictData = std::get<TDictData>(data);
                                if (dictData.Key == nullptr) {
                                    dictData.Key = ReadType();
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "key")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "key")";
                            }
                        } else if (mapKey == "value") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TDictData{nullptr, nullptr};
                            }
                            if (std::holds_alternative<TDictData>(data)) {
                                auto& dictData = std::get<TDictData>(data);
                                if (dictData.Value == nullptr) {
                                    dictData.Value = ReadType();
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "value")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "value")";
                            }
                        } else if (mapKey == "members") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TStructBuilderRaw(*Factory_);
                            }
                            if (std::holds_alternative<TStructBuilderRaw>(data)) {
                                ReadMembers(std::get<TStructBuilderRaw>(data));
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "members")";
                            }
                        } else if (mapKey == "elements") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TTupleBuilderRaw(*Factory_);
                            }
                            if (std::holds_alternative<TTupleBuilderRaw>(data)) {
                                ReadElements(std::get<TTupleBuilderRaw>(data));
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "elements")";
                            }
                        } else if (mapKey == "tag") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TTaggedBuilderRaw(*Factory_);
                            }
                            if (std::holds_alternative<TTaggedBuilderRaw>(data)) {
                                auto& builder = std::get<TTaggedBuilderRaw>(data);
                                if (!builder.HasTag()) {
                                    builder.SetTag(ReadString(R"("tag")"));
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "tag")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "tag")";
                            }
                        } else if (mapKey == "precision") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TDecimalData{ReadSmallInt(R"("precision")"), 0};
                            } else if (std::holds_alternative<TDecimalData>(data)) {
                                auto& decimalData = std::get<TDecimalData>(data);
                                if (decimalData.Precision == 0) {
                                    decimalData.Precision = ReadSmallInt(R"("precision")");
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "precision")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "precision")";
                            }
                        } else if (mapKey == "scale") {
                            if (std::holds_alternative<std::monostate>(data)) {
                                data = TDecimalData{0, ReadSmallInt(R"("scale")")};
                            } else if (std::holds_alternative<TDecimalData>(data)) {
                                auto& decimalData = std::get<TDecimalData>(data);
                                if (decimalData.Scale == 0) {
                                    decimalData.Scale = ReadSmallInt(R"("scale")");
                                } else {
                                    ythrow TDeserializationException() << R"(duplicate key "scale")";
                                }
                            } else {
                                ythrow TDeserializationException() << R"(unexpected key "scale")";
                            }
                        } else {
                            NYsonPull::NReadOps::SkipValue(*Reader_);
                        }
                    } else if (event.Type() == NYsonPull::EEventType::EndMap) {
                        if (!typeName.Defined()) {
                            ythrow TDeserializationException() << R"(missing required key "type_name")";
                        }

                        return ReadTypeFromData(*typeName, std::move(data));
                    } else {
                        ythrow TDeserializationException() << "unexpected event " << event.Type();
                    }
                }
            }

            const TType* ReadTypeFromData(ETypeName typeName, TTypeData data) {
                const TType* type;

                switch (typeName) {
                    case ETypeName::Bool:
                        type = TBoolType::InstanceRaw();
                        break;
                    case ETypeName::Int8:
                        type = TInt8Type::InstanceRaw();
                        break;
                    case ETypeName::Int16:
                        type = TInt16Type::InstanceRaw();
                        break;
                    case ETypeName::Int32:
                        type = TInt32Type::InstanceRaw();
                        break;
                    case ETypeName::Int64:
                        type = TInt64Type::InstanceRaw();
                        break;
                    case ETypeName::Uint8:
                        type = TUint8Type::InstanceRaw();
                        break;
                    case ETypeName::Uint16:
                        type = TUint16Type::InstanceRaw();
                        break;
                    case ETypeName::Uint32:
                        type = TUint32Type::InstanceRaw();
                        break;
                    case ETypeName::Uint64:
                        type = TUint64Type::InstanceRaw();
                        break;
                    case ETypeName::Float:
                        type = TFloatType::InstanceRaw();
                        break;
                    case ETypeName::Double:
                        type = TDoubleType::InstanceRaw();
                        break;
                    case ETypeName::String:
                        type = TStringType::InstanceRaw();
                        break;
                    case ETypeName::Utf8:
                        type = TUtf8Type::InstanceRaw();
                        break;
                    case ETypeName::Date:
                        type = TDateType::InstanceRaw();
                        break;
                    case ETypeName::Datetime:
                        type = TDatetimeType::InstanceRaw();
                        break;
                    case ETypeName::Timestamp:
                        type = TTimestampType::InstanceRaw();
                        break;
                    case ETypeName::TzDate:
                        type = TTzDateType::InstanceRaw();
                        break;
                    case ETypeName::TzDatetime:
                        type = TTzDatetimeType::InstanceRaw();
                        break;
                    case ETypeName::TzTimestamp:
                        type = TTzTimestampType::InstanceRaw();
                        break;
                    case ETypeName::Interval:
                        type = TIntervalType::InstanceRaw();
                        break;
                    case ETypeName::Decimal: {
                        if (!std::holds_alternative<TDecimalData>(data)) {
                            ythrow TDeserializationException() << R"(missing required keys "precision" and "scale" for type Decimal)";
                        }

                        auto& decimalData = std::get<TDecimalData>(data);

                        if (decimalData.Precision == 0) {
                            ythrow TDeserializationException() << R"(missing required key "precision" for type Decimal)";
                        }

                        if (decimalData.Scale == 0) {
                            ythrow TDeserializationException() << R"(missing required key "scale" for type Decimal)";
                        }

                        return Factory_->DecimalRaw(decimalData.Precision, decimalData.Scale);
                    }
                    case ETypeName::Json:
                        type = TJsonType::InstanceRaw();
                        break;
                    case ETypeName::Yson:
                        type = TYsonType::InstanceRaw();
                        break;
                    case ETypeName::Uuid:
                        type = TUuidType::InstanceRaw();
                        break;
                    case ETypeName::Date32:
                        type = TDate32Type::InstanceRaw();
                        break;
                    case ETypeName::Datetime64:
                        type = TDatetime64Type::InstanceRaw();
                        break;
                    case ETypeName::Timestamp64:
                        type = TTimestamp64Type::InstanceRaw();
                        break;
                    case ETypeName::Interval64:
                        type = TInterval64Type::InstanceRaw();
                        break;
                    case ETypeName::Void:
                        type = TVoidType::InstanceRaw();
                        break;
                    case ETypeName::Null:
                        type = TNullType::InstanceRaw();
                        break;
                    case ETypeName::Optional: {
                        if (!std::holds_alternative<TTaggedBuilderRaw>(data)) {
                            ythrow TDeserializationException() << R"(missing required key "item" for type Optional)";
                        }

                        auto& builder = std::get<TTaggedBuilderRaw>(data);

                        if (!builder.HasItem()) {
                            ythrow TDeserializationException() << R"(missing required key "item" for type Optional)";
                        }

                        return Factory_->OptionalRaw(*builder.GetItem());
                    }
                    case ETypeName::List: {
                        if (!std::holds_alternative<TTaggedBuilderRaw>(data)) {
                            ythrow TDeserializationException() << R"(missing required key "item" for type List)";
                        }

                        auto& builder = std::get<TTaggedBuilderRaw>(data);

                        if (!builder.HasItem()) {
                            ythrow TDeserializationException() << R"(missing required key "item" for type List)";
                        }

                        return Factory_->ListRaw(*builder.GetItem());
                    }
                    case ETypeName::Dict: {
                        if (!std::holds_alternative<TDictData>(data)) {
                            ythrow TDeserializationException() << R"(missing required keys "key" and "value" for type Dict)";
                        }

                        auto& dictData = std::get<TDictData>(data);

                        if (dictData.Key == nullptr) {
                            ythrow TDeserializationException() << R"(missing required key "key" for type Dict)";
                        }

                        if (dictData.Value == nullptr) {
                            ythrow TDeserializationException() << R"(missing required key "value" for type Dict)";
                        }

                        return Factory_->DictRaw(dictData.Key, dictData.Value);
                    }
                    case ETypeName::Struct: {
                        if (!std::holds_alternative<TStructBuilderRaw>(data)) {
                            ythrow TDeserializationException() << R"(missing required key "members" for type Struct)";
                        }

                        return std::get<TStructBuilderRaw>(data).BuildRaw();
                    }
                    case ETypeName::Tuple: {
                        if (!std::holds_alternative<TTupleBuilderRaw>(data)) {
                            ythrow TDeserializationException() << R"(missing required key "elements" for type Tuple)";
                        }

                        return std::get<TTupleBuilderRaw>(data).BuildRaw();
                    }
                    case ETypeName::Variant: {
                        if (std::holds_alternative<TStructBuilderRaw>(data)) {
                            return std::get<TStructBuilderRaw>(data).BuildVariantRaw();
                        } else if (std::holds_alternative<TTupleBuilderRaw>(data)) {
                            return std::get<TTupleBuilderRaw>(data).BuildVariantRaw();
                        } else {
                            ythrow TDeserializationException() << R"(missing both keys "members" and "elements" for type Variant)";
                        }
                    }
                    case ETypeName::Tagged: {
                        if (!std::holds_alternative<TTaggedBuilderRaw>(data)) {
                            ythrow TDeserializationException() << R"(missing required keys "tag" and "item" for type Tagged)";
                        }

                        auto& builder = std::get<TTaggedBuilderRaw>(data);

                        if (!builder.HasItem()) {
                            ythrow TDeserializationException() << R"(missing required key "item" for type Tagged)";
                        }

                        if (!builder.HasTag()) {
                            ythrow TDeserializationException() << R"(missing required key "tag" for type Tagged)";
                        }

                        return builder.BuildRaw();
                    }
                }

                if (!std::holds_alternative<std::monostate>(data)) {
                    ythrow TDeserializationException() << "unexpected key for type " << typeName;
                }

                return type;
            }

            TStringBuf ReadString(TStringBuf what) {
                auto event = Reader_->NextEvent();

                if (event.Type() != NYsonPull::EEventType::Scalar || event.AsScalar().Type() != NYsonPull::EScalarType::String) {
                    ythrow TDeserializationException() << what << " must contain a string";
                }

                return event.AsString();
            }

            ui8 ReadSmallInt(TStringBuf what) {
                auto event = Reader_->NextEvent();

                if (event.Type() != NYsonPull::EEventType::Scalar || event.AsScalar().Type() != NYsonPull::EScalarType::Int64) {
                    ythrow TDeserializationException() << what << " must contain a signed integer";
                }

                auto result = event.AsScalar().AsInt64();

                if (result <= 0) {
                    ythrow TDeserializationException() << what << " must be greater than zero";
                }

                if (result > Max<ui8>()) {
                    ythrow TDeserializationException() << what << " is too big";
                }

                return static_cast<ui8>(result);
            }

            void ReadMembers(TStructBuilderRaw& builder) {
                if (Reader_->NextEvent().Type() != NYsonPull::EEventType::BeginList) {
                    ythrow TDeserializationException() << R"("members" must contain a list)";
                }

                while (true) {
                    auto event = Reader_->NextEvent();

                    if (event.Type() == NYsonPull::EEventType::BeginMap) {
                        while (true) {
                            auto event = Reader_->NextEvent();

                            if (event.Type() == NYsonPull::EEventType::Key) {
                                auto mapKey = event.AsString();
                                if (mapKey == "name") {
                                    if (builder.HasMemberName()) {
                                        ythrow TDeserializationException() << R"(duplicate key "name")";
                                    }

                                    builder.AddMemberName(ReadString(R"("name")"));
                                } else if (mapKey == "type") {
                                    if (builder.HasMemberType()) {
                                        ythrow TDeserializationException() << R"(duplicate key "type")";
                                    }

                                    builder.AddMemberType(ReadType());
                                } else {
                                    NYsonPull::NReadOps::SkipValue(*Reader_);
                                }
                            } else if (event.Type() == NYsonPull::EEventType::EndMap) {
                                if (!builder.HasMemberName()) {
                                    ythrow TDeserializationException() << R"(missing required key "name")";
                                }
                                if (!builder.HasMemberType()) {
                                    ythrow TDeserializationException() << R"(missing required key "type")";
                                }

                                builder.AddMember();
                                break;
                            } else {
                                ythrow TDeserializationException() << "unexpected event " << event.Type();
                            }
                        }
                    } else if (event.Type() == NYsonPull::EEventType::EndList) {
                        break;
                    } else {
                        ythrow TDeserializationException() << R"("members" must contain a list of maps)";
                    }
                }
            }

            void ReadElements(TTupleBuilderRaw& builder) {
                if (Reader_->NextEvent().Type() != NYsonPull::EEventType::BeginList) {
                    ythrow TDeserializationException() << R"("elements" must contain a list)";
                }

                while (true) {
                    auto event = Reader_->NextEvent();

                    if (event.Type() == NYsonPull::EEventType::BeginMap) {
                        while (true) {
                            auto event = Reader_->NextEvent();

                            if (event.Type() == NYsonPull::EEventType::Key) {
                                auto mapKey = event.AsString();
                                if (mapKey == "type") {
                                    if (builder.HasElementType()) {
                                        ythrow TDeserializationException() << R"(duplicate key "type")";
                                    }

                                    builder.AddElementType(ReadType());
                                } else {
                                    NYsonPull::NReadOps::SkipValue(*Reader_);
                                }
                            } else if (event.Type() == NYsonPull::EEventType::EndMap) {
                                if (!builder.HasElementType()) {
                                    ythrow TDeserializationException() << R"(missing required key "type")";
                                }

                                builder.AddElement();
                                break;
                            } else {
                                ythrow TDeserializationException() << "unexpected event " << event.Type();
                            }
                        }
                    } else if (event.Type() == NYsonPull::EEventType::EndList) {
                        break;
                    } else {
                        ythrow TDeserializationException() << R"("elements" must contain a list of maps)";
                    }
                }
            }

            static ETypeName TypeNameStringToEnum(TStringBuf name) {
                static const THashMap<TStringBuf, ETypeName> dispatch = {
                    {"void", ETypeName::Void},
                    {"null", ETypeName::Null},
                    {"bool", ETypeName::Bool},
                    {"int8", ETypeName::Int8},
                    {"int16", ETypeName::Int16},
                    {"int32", ETypeName::Int32},
                    {"int64", ETypeName::Int64},
                    {"uint8", ETypeName::Uint8},
                    {"uint16", ETypeName::Uint16},
                    {"uint32", ETypeName::Uint32},
                    {"uint64", ETypeName::Uint64},
                    {"float", ETypeName::Float},
                    {"double", ETypeName::Double},
                    {"string", ETypeName::String},
                    {"utf8", ETypeName::Utf8},
                    {"date", ETypeName::Date},
                    {"datetime", ETypeName::Datetime},
                    {"timestamp", ETypeName::Timestamp},
                    {"tz_date", ETypeName::TzDate},
                    {"tz_datetime", ETypeName::TzDatetime},
                    {"tz_timestamp", ETypeName::TzTimestamp},
                    {"interval", ETypeName::Interval},
                    {"json", ETypeName::Json},
                    {"yson", ETypeName::Yson},
                    {"uuid", ETypeName::Uuid},
                    {"decimal", ETypeName::Decimal},
                    {"optional", ETypeName::Optional},
                    {"list", ETypeName::List},
                    {"dict", ETypeName::Dict},
                    {"struct", ETypeName::Struct},
                    {"tuple", ETypeName::Tuple},
                    {"variant", ETypeName::Variant},
                    {"tagged", ETypeName::Tagged},
                };

                if (auto it = dispatch.find(name); it != dispatch.end()) {
                    return it->second;
                } else {
                    ythrow TDeserializationException() << "unknown type " << TString{name}.Quote();
                }
            }

        private:
            IPoolTypeFactory* Factory_;
            NYsonPull::TReader* Reader_;
            size_t Depth_ = 0;
        };
    }

    TTypePtr DeserializeYson(ITypeFactory& factory, NYsonPull::TReader& reader, bool deduplicate) {
        auto pool = PoolFactory(deduplicate, 2048);
        auto type = DeserializeYsonRaw(*pool, reader);
        return factory.Adopt(type->AsPtr());
    }

    TTypePtr DeserializeYson(ITypeFactory& factory, TStringBuf data, bool deduplicate) {
        auto reader = NYsonPull::TReader(NYsonPull::NInput::FromMemory(data), NYsonPull::EStreamType::Node);
        return DeserializeYson(factory, reader, deduplicate);
    }

    TTypePtr DeserializeYson(ITypeFactory& factory, IInputStream& input, bool deduplicate) {
        auto reader = NYsonPull::TReader(NYsonPull::NInput::FromInputStream(&input), NYsonPull::EStreamType::Node);
        return DeserializeYson(factory, reader, deduplicate);
    }

    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, NYsonPull::TReader& reader) {
        if (reader.LastEvent().Type() != NYsonPull::EEventType::BeginStream) {
            ythrow TDeserializationException() << "stream contains extraneous data";
        }

        auto type = DeserializeYsonMultipleRaw(factory, reader);

        if (type == nullptr) {
            ythrow TDeserializationException() << "unexpected end of stream";
        }

        if (reader.NextEvent().Type() != NYsonPull::EEventType::EndStream) {
            ythrow TDeserializationException() << "stream contains extraneous data";
        }

        return type;
    }

    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, TStringBuf data) {
        auto reader = NYsonPull::TReader(NYsonPull::NInput::FromMemory(data), NYsonPull::EStreamType::Node);
        return DeserializeYsonRaw(factory, reader);
    }

    const TType* DeserializeYsonRaw(IPoolTypeFactory& factory, IInputStream& input) {
        auto reader = NYsonPull::TReader(NYsonPull::NInput::FromInputStream(&input), NYsonPull::EStreamType::Node);
        return DeserializeYsonRaw(factory, reader);
    }

    const TType* DeserializeYsonMultipleRaw(IPoolTypeFactory& factory, NYsonPull::TReader& reader) {
        return TYsonDeserializer(&factory, &reader).ReadType();
    }

    TString SerializeYson(const TType* type, bool humanReadable, bool includeTags) {
        auto result = TString();
        auto writer = humanReadable
                          ? NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::Node)
                          : NYsonPull::MakeBinaryWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::Node);
        SerializeYson(type, writer.GetConsumer(), includeTags);
        return result;
    }

    void SerializeYson(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags) {
        consumer.OnBeginStream();
        SerializeYsonMultiple(type, consumer, includeTags);
        consumer.OnEndStream();
    }

    void SerializeYson(const TType* type, IOutputStream& stream, bool humanReadable, bool includeTags) {
        auto writer = humanReadable
                          ? NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromOutputStream(&stream), NYsonPull::EStreamType::Node)
                          : NYsonPull::MakeBinaryWriter(NYsonPull::NOutput::FromOutputStream(&stream), NYsonPull::EStreamType::Node);
        SerializeYson(type, writer.GetConsumer(), includeTags);
    }

    void SerializeYsonMultiple(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags) {
        type->VisitRaw(TOverloaded{
            [&consumer](const TVoidType*) {
                consumer.OnScalarString("void");
            },
            [&consumer](const TNullType*) {
                consumer.OnScalarString("null");
            },
            [&consumer](const TBoolType*) {
                consumer.OnScalarString("bool");
            },
            [&consumer](const TInt8Type*) {
                consumer.OnScalarString("int8");
            },
            [&consumer](const TInt16Type*) {
                consumer.OnScalarString("int16");
            },
            [&consumer](const TInt32Type*) {
                consumer.OnScalarString("int32");
            },
            [&consumer](const TInt64Type*) {
                consumer.OnScalarString("int64");
            },
            [&consumer](const TUint8Type*) {
                consumer.OnScalarString("uint8");
            },
            [&consumer](const TUint16Type*) {
                consumer.OnScalarString("uint16");
            },
            [&consumer](const TUint32Type*) {
                consumer.OnScalarString("uint32");
            },
            [&consumer](const TUint64Type*) {
                consumer.OnScalarString("uint64");
            },
            [&consumer](const TFloatType*) {
                consumer.OnScalarString("float");
            },
            [&consumer](const TDoubleType*) {
                consumer.OnScalarString("double");
            },
            [&consumer](const TStringType*) {
                consumer.OnScalarString("string");
            },
            [&consumer](const TUtf8Type*) {
                consumer.OnScalarString("utf8");
            },
            [&consumer](const TDateType*) {
                consumer.OnScalarString("date");
            },
            [&consumer](const TDatetimeType*) {
                consumer.OnScalarString("datetime");
            },
            [&consumer](const TTimestampType*) {
                consumer.OnScalarString("timestamp");
            },
            [&consumer](const TTzDateType*) {
                consumer.OnScalarString("tz_date");
            },
            [&consumer](const TTzDatetimeType*) {
                consumer.OnScalarString("tz_datetime");
            },
            [&consumer](const TTzTimestampType*) {
                consumer.OnScalarString("tz_timestamp");
            },
            [&consumer](const TIntervalType*) {
                consumer.OnScalarString("interval");
            },
            [&consumer](const TJsonType*) {
                consumer.OnScalarString("json");
            },
            [&consumer](const TYsonType*) {
                consumer.OnScalarString("yson");
            },
            [&consumer](const TUuidType*) {
                consumer.OnScalarString("uuid");
            },
            [&consumer](const TDate32Type*) {
                consumer.OnScalarString("date32");
            },
            [&consumer](const TDatetime64Type*) {
                consumer.OnScalarString("datetime64");
            },
            [&consumer](const TTimestamp64Type*) {
                consumer.OnScalarString("timestamp64");
            },
            [&consumer](const TInterval64Type*) {
                consumer.OnScalarString("interval64");
            },
            [&consumer](const TDecimalType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("decimal");

                consumer.OnKey("precision");
                consumer.OnScalarInt64(t->GetPrecision());

                consumer.OnKey("scale");
                consumer.OnScalarInt64(t->GetScale());

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TOptionalType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("optional");

                consumer.OnKey("item");
                SerializeYsonMultiple(t->GetItemTypeRaw(), consumer, includeTags);

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TListType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("list");

                consumer.OnKey("item");
                SerializeYsonMultiple(t->GetItemTypeRaw(), consumer, includeTags);

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TDictType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("dict");

                consumer.OnKey("key");
                SerializeYsonMultiple(t->GetKeyTypeRaw(), consumer, includeTags);

                consumer.OnKey("value");
                SerializeYsonMultiple(t->GetValueTypeRaw(), consumer, includeTags);

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TStructType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("struct");

                consumer.OnKey("members");
                consumer.OnBeginList();
                for (auto& item : t->GetMembers()) {
                    consumer.OnBeginMap();

                    consumer.OnKey("name");
                    consumer.OnScalarString(item.GetName());

                    consumer.OnKey("type");
                    SerializeYsonMultiple(item.GetTypeRaw(), consumer, includeTags);

                    consumer.OnEndMap();
                }
                consumer.OnEndList();

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TTupleType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("tuple");

                consumer.OnKey("elements");
                consumer.OnBeginList();
                for (auto& item : t->GetElements()) {
                    consumer.OnBeginMap();

                    consumer.OnKey("type");
                    SerializeYsonMultiple(item.GetTypeRaw(), consumer, includeTags);

                    consumer.OnEndMap();
                }
                consumer.OnEndList();

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TVariantType* t) {
                consumer.OnBeginMap();

                consumer.OnKey("type_name");
                consumer.OnScalarString("variant");

                t->VisitUnderlyingRaw(
                    TOverloaded{
                        [&consumer, includeTags](const TStructType* t) {
                            // Warning: we loose struct's name here.
                            // See https://ml.yandex-team.ru/thread/data-com-dev/171136785840079161/

                            consumer.OnKey("members");
                            consumer.OnBeginList();
                            for (auto& item : t->GetMembers()) {
                                consumer.OnBeginMap();

                                consumer.OnKey("name");
                                consumer.OnScalarString(item.GetName());

                                consumer.OnKey("type");
                                SerializeYsonMultiple(item.GetTypeRaw(), consumer, includeTags);

                                consumer.OnEndMap();
                            }
                            consumer.OnEndList();
                        },
                        [&consumer, includeTags](const TTupleType* t) {
                            // Warning: we loose tuple's name here.
                            // See https://ml.yandex-team.ru/thread/data-com-dev/171136785840079161/

                            consumer.OnKey("elements");
                            consumer.OnBeginList();
                            for (auto& item : t->GetElements()) {
                                consumer.OnBeginMap();

                                consumer.OnKey("type");
                                SerializeYsonMultiple(item.GetTypeRaw(), consumer, includeTags);

                                consumer.OnEndMap();
                            }
                            consumer.OnEndList();
                        }});

                consumer.OnEndMap();
            },
            [&consumer, includeTags](const TTaggedType* t) {
                if (includeTags) {
                    consumer.OnBeginMap();

                    consumer.OnKey("type_name");
                    consumer.OnScalarString("tagged");

                    consumer.OnKey("tag");
                    consumer.OnScalarString(t->GetTag());

                    consumer.OnKey("item");
                    SerializeYsonMultiple(t->GetItemTypeRaw(), consumer, includeTags);

                    consumer.OnEndMap();
                } else {
                    SerializeYsonMultiple(t->GetItemTypeRaw(), consumer, includeTags);
                }
            },
        });
    }

    namespace {
        void WriteVoidType(NYsonPull::IConsumer& consumer) {
            consumer.OnBeginList();
            consumer.OnScalarString("VoidType");
            consumer.OnEndList();
        }

        void WriteNullType(NYsonPull::IConsumer& consumer) {
            consumer.OnBeginList();
            consumer.OnScalarString("NullType");
            consumer.OnEndList();
        }

        void WriteDataType(NYsonPull::IConsumer& consumer, EPrimitiveTypeName name) {
            consumer.OnBeginList();
            consumer.OnScalarString("DataType");
            consumer.OnScalarString(ToString(name));
            consumer.OnEndList();
        }
    }

    void AsYqlType(const TType* type, NYsonPull::IConsumer& consumer, bool includeTags) {
        type->VisitRaw(TOverloaded{
            [&consumer](const TVoidType*) {
                WriteVoidType(consumer);
            },
            [&consumer](const TNullType*) {
                WriteNullType(consumer);
            },
            [&consumer](const TBoolType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Bool);
            },
            [&consumer](const TInt8Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Int8);
            },
            [&consumer](const TInt16Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Int16);
            },
            [&consumer](const TInt32Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Int32);
            },
            [&consumer](const TInt64Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Int64);
            },
            [&consumer](const TUint8Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Uint8);
            },
            [&consumer](const TUint16Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Uint16);
            },
            [&consumer](const TUint32Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Uint32);
            },
            [&consumer](const TUint64Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Uint64);
            },
            [&consumer](const TFloatType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Float);
            },
            [&consumer](const TDoubleType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Double);
            },
            [&consumer](const TStringType*) {
                WriteDataType(consumer, EPrimitiveTypeName::String);
            },
            [&consumer](const TUtf8Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Utf8);
            },
            [&consumer](const TDateType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Date);
            },
            [&consumer](const TDatetimeType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Datetime);
            },
            [&consumer](const TTimestampType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Timestamp);
            },
            [&consumer](const TTzDateType*) {
                WriteDataType(consumer, EPrimitiveTypeName::TzDate);
            },
            [&consumer](const TTzDatetimeType*) {
                WriteDataType(consumer, EPrimitiveTypeName::TzDatetime);
            },
            [&consumer](const TTzTimestampType*) {
                WriteDataType(consumer, EPrimitiveTypeName::TzTimestamp);
            },
            [&consumer](const TIntervalType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Interval);
            },
            [&consumer](const TJsonType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Json);
            },
            [&consumer](const TYsonType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Yson);
            },
            [&consumer](const TUuidType*) {
                WriteDataType(consumer, EPrimitiveTypeName::Uuid);
            },
            [&consumer](const TDate32Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Date32);
            },
            [&consumer](const TDatetime64Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Datetime64);
            },
            [&consumer](const TTimestamp64Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Timestamp64);
            },
            [&consumer](const TInterval64Type*) {
                WriteDataType(consumer, EPrimitiveTypeName::Interval64);
            },
            [&consumer](const TDecimalType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("DataType");
                consumer.OnScalarString("Decimal");
                consumer.OnScalarString(ToString(t->GetPrecision()));
                consumer.OnScalarString(ToString(t->GetScale()));
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TOptionalType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("OptionalType");
                AsYqlType(t->GetItemTypeRaw(), consumer, includeTags);
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TListType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("ListType");
                AsYqlType(t->GetItemTypeRaw(), consumer, includeTags);
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TDictType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("DictType");
                AsYqlType(t->GetKeyTypeRaw(), consumer, includeTags);
                AsYqlType(t->GetValueTypeRaw(), consumer, includeTags);
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TStructType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("StructType");
                {
                    consumer.OnBeginList();
                    for (auto& item : t->GetMembers()) {
                        consumer.OnBeginList();
                        consumer.OnScalarString(item.GetName());
                        AsYqlType(item.GetTypeRaw(), consumer, includeTags);
                        consumer.OnEndList();
                    }
                    consumer.OnEndList();
                }
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TTupleType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("TupleType");
                {
                    consumer.OnBeginList();
                    for (auto& item : t->GetElements()) {
                        AsYqlType(item.GetTypeRaw(), consumer, includeTags);
                    }
                    consumer.OnEndList();
                }

                consumer.OnEndList();
            },
            [&consumer, includeTags](const TVariantType* t) {
                consumer.OnBeginList();
                consumer.OnScalarString("VariantType");
                AsYqlType(t->GetUnderlyingTypeRaw(), consumer, includeTags);
                consumer.OnEndList();
            },
            [&consumer, includeTags](const TTaggedType* t) {
                if (includeTags) {
                    consumer.OnBeginList();
                    consumer.OnScalarString("TaggedType");
                    consumer.OnScalarString(t->GetTag());
                    AsYqlType(t->GetItemTypeRaw(), consumer, includeTags);
                    consumer.OnEndList();
                } else {
                    AsYqlType(t->GetItemTypeRaw(), consumer, includeTags);
                }
            },
        });
    }

    TString AsYqlType(const NTi::TType* type, bool includeTags) {
        auto result = TString();
        auto writer = NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::Node);
        writer.BeginStream();
        AsYqlType(type, writer.GetConsumer(), includeTags);
        writer.EndStream();
        return result;
    }

    void AsYqlRowSpec(const TType* maybeTagged, NYsonPull::IConsumer& consumer, bool includeTags) {
        auto* type = maybeTagged->StripTagsRaw();

        if (!type->IsStruct()) {
            ythrow TApiException() << "AsYqlRowSpec expected a struct type but got " << type->GetTypeName();
        }

        consumer.OnBeginMap();
        consumer.OnKey("StrictSchema");
        consumer.OnScalarBoolean(true);
        consumer.OnKey("Type");
        AsYqlType(type, consumer, includeTags);
        consumer.OnEndMap();
    }

    TString AsYqlRowSpec(const NTi::TType* type, bool includeTags) {
        auto result = TString();
        auto writer = NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::Node);
        writer.BeginStream();
        AsYqlRowSpec(type, writer.GetConsumer(), includeTags);
        writer.EndStream();
        return result;
    }

    void AsYtSchema(const TType* maybeTagged, NYsonPull::IConsumer& consumer, bool failOnEmptyStruct) {
        auto* type = maybeTagged->StripTagsRaw();

        if (!type->IsStruct()) {
            ythrow TApiException() << "AsYtSchema expected a struct type but got " << type->GetTypeName();
        }

        auto* structType = type->AsStructRaw();

        if (structType->GetMembers().empty()) {
            if (failOnEmptyStruct) {
                ythrow TApiException() << "AsYtSchema expected a non-empty struct";
            }

            AsYtSchema(Struct({{"_yql_fake_column", Optional(Bool())}}).Get(), consumer);
            return;
        }

        consumer.OnBeginAttributes();

        consumer.OnKey("strict");
        consumer.OnScalarBoolean(true);

        consumer.OnKey("unique_keys");
        consumer.OnScalarBoolean(false);

        consumer.OnEndAttributes();

        consumer.OnBeginList();
        for (auto& item : structType->GetMembers()) {
            auto* itemType = item.GetTypeRaw()->StripTagsRaw();

            bool required = true;

            if (itemType->IsOptional()) {
                // toplevel optionals make non-required columns
                itemType = itemType->AsOptionalRaw()->GetItemTypeRaw();
                required = false;
            }

            TStringBuf typeString = itemType->VisitRaw(TOverloaded{
                [](const TVoidType*) -> TStringBuf { return "any"; },
                [](const TNullType*) -> TStringBuf { return "any"; },
                [](const TBoolType*) -> TStringBuf { return "boolean"; },
                [](const TInt8Type*) -> TStringBuf { return "int8"; },
                [](const TInt16Type*) -> TStringBuf { return "int16"; },
                [](const TInt32Type*) -> TStringBuf { return "int32"; },
                [](const TInt64Type*) -> TStringBuf { return "int64"; },
                [](const TUint8Type*) -> TStringBuf { return "uint8"; },
                [](const TUint16Type*) -> TStringBuf { return "uint16"; },
                [](const TUint32Type*) -> TStringBuf { return "uint32"; },
                [](const TUint64Type*) -> TStringBuf { return "uint64"; },
                [](const TFloatType*) -> TStringBuf { return "double"; },
                [](const TDoubleType*) -> TStringBuf { return "double"; },
                [](const TStringType*) -> TStringBuf { return "string"; },
                [](const TUtf8Type*) -> TStringBuf { return "utf8"; },
                [](const TDateType*) -> TStringBuf { return "uint16"; },
                [](const TDatetimeType*) -> TStringBuf { return "uint32"; },
                [](const TTimestampType*) -> TStringBuf { return "uint64"; },
                [](const TTzDateType*) -> TStringBuf { return "string"; },
                [](const TTzDatetimeType*) -> TStringBuf { return "string"; },
                [](const TTzTimestampType*) -> TStringBuf { return "string"; },
                [](const TIntervalType*) -> TStringBuf { return "int64"; },
                [](const TJsonType*) -> TStringBuf { return "string"; },
                [](const TYsonType*) -> TStringBuf { return "any"; },
                [](const TUuidType*) -> TStringBuf { return "string"; },
                [](const TDate32Type*) -> TStringBuf { return "int64"; },
                [](const TDatetime64Type*) -> TStringBuf { return "int64"; },
                [](const TTimestamp64Type*) -> TStringBuf { return "int64"; },
                [](const TInterval64Type*) -> TStringBuf { return "int64"; },
                [](const TDecimalType*) -> TStringBuf { return "string"; },
                [](const TOptionalType*) -> TStringBuf { return "any"; },
                [](const TListType*) -> TStringBuf { return "any"; },
                [](const TDictType*) -> TStringBuf { return "any"; },
                [](const TStructType*) -> TStringBuf { return "any"; },
                [](const TTupleType*) -> TStringBuf { return "any"; },
                [](const TVariantType*) -> TStringBuf { return "any"; },
                [](const TTaggedType*) -> TStringBuf { return "any"; },
            });

            if (typeString == "any") {
                // columns of type `any` cannot be required
                required = false;
            }

            {
                consumer.OnBeginMap();

                consumer.OnKey("name");
                consumer.OnScalarString(item.GetName());

                consumer.OnKey("required");
                consumer.OnScalarBoolean(required);

                consumer.OnKey("type");
                consumer.OnScalarString(typeString);

                consumer.OnEndMap();
            }
        }
        consumer.OnEndList();
    }

    TString AsYtSchema(const NTi::TType* type, bool failOnEmptyStruct) {
        auto result = TString();
        auto writer = NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::Node);
        writer.BeginStream();
        AsYtSchema(type, writer.GetConsumer(), failOnEmptyStruct);
        writer.EndStream();
        return result;
    }
}
