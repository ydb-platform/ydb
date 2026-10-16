#include "format.h"

#include <ydb/core/scheme/scheme_type_id.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/charset/utf8.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/parser.h>

#include <cmath>
#include <limits>

namespace NKikimr::NBackup::NYt {
    namespace {

        struct TType {
            const char* Yt;
            const char* Ydb;
            Ydb::Type::PrimitiveTypeId Id;
            ui32 Internal;
        };

#define TYPE(yt, name, proto) {yt, #name, Ydb::Type::proto, NScheme::NTypeIds::name}
        const TType TYPES[] = {
            TYPE("int8", Int8, INT8),
            TYPE("int16", Int16, INT16),
            TYPE("int32", Int32, INT32),
            TYPE("int64", Int64, INT64),
            TYPE("uint8", Uint8, UINT8),
            TYPE("uint16", Uint16, UINT16),
            TYPE("uint32", Uint32, UINT32),
            TYPE("uint64", Uint64, UINT64),
            TYPE("string", String, STRING),
            TYPE("utf8", Utf8, UTF8),
            TYPE("bool", Bool, BOOL),
            TYPE("boolean", Bool, BOOL),
            TYPE("float", Float, FLOAT),
            TYPE("double", Double, DOUBLE),
        };
#undef TYPE

        void Check(bool condition, TStringBuf message) {
            if (!condition) {
                ythrow yexception() << message;
            }
        }

        const TType& FindType(TStringBuf name, bool internal = false) {
            for (const auto& type : TYPES) {
                if (name == (internal ? type.Ydb : type.Yt)) {
                    return type;
                }
            }
            ythrow yexception() << "Unsupported YT column type: " << name;
        }

        const TType& FindInternalType(const NKikimrSchemeOp::TColumnDescription& column) {
            if (column.HasTypeId()) {
                for (const auto& type : TYPES) {
                    if (column.GetTypeId() == type.Internal) {
                        return type;
                    }
                }
                ythrow yexception() << "Unsupported YT destination type id: " << column.GetTypeId();
            }
            return FindType(column.GetType(), true);
        }

        void AddColumn(TSchema& schema, const TString& name, const TType& type, bool nullable, bool key) {
            Check(!name.empty(), "Empty column name");
            for (const auto& column : schema.Columns) {
                Check(column.Name != name, "Duplicate column name");
            }
            Check(!key || !nullable, "Nullable YT keys are unsupported");
            Check(!key || (type.Id != Ydb::Type::FLOAT && type.Id != Ydb::Type::DOUBLE), "Floating point YT keys are unsupported");
            auto* column = schema.Table.add_columns();
            column->set_name(name);
            column->set_not_null(!nullable);
            auto* target = column->mutable_type();
            if (nullable) {
                target = target->mutable_optional_type()->mutable_item();
            }
            target->set_type_id(type.Id);
            auto* description = schema.Description.AddColumns();
            description->SetName(name);
            description->SetType(type.Ydb);
            description->SetTypeId(type.Internal);
            description->SetNotNull(!nullable);
            schema.Columns.push_back({name, type.Id, nullable});
            if (key) {
                schema.KeyColumns.push_back(schema.Columns.size() - 1);
                schema.Table.add_primary_key(name);
                schema.Description.AddKeyColumnNames(name);
            }
        }

    } // namespace

    bool ParseMetadata(TStringBuf data, TSchema& schema, TString& error) {
        try {
            TSchema result;
            auto metadata = ::NYT::NodeFromYsonString(data);
            Check(metadata.IsMap(), "YT metadata must be a map");
            const auto& source = metadata.At("schema");
            Check(source.IsList(), "YT schema must be a list");
            const auto& attributes = source.GetAttributes();
            Check(attributes.HasKey("strict") && attributes.At("strict").AsBool(), "YT schema must be strict");
            Check(attributes.HasKey("unique_keys") && attributes.At("unique_keys").AsBool(), "YT schema must have unique keys");
            bool valuesStarted = false;
            for (const auto& column : source.AsList()) {
                Check(column.IsMap(), "YT column must be a map");
                Check(!column.HasKey("expression") && !column.HasKey("aggregate") && !column.HasKey("deleted"), "Computed, aggregate and deleted YT columns are unsupported");
                TString name = column.At("name").AsString();
                bool key = column.HasKey("sort_order");
                if (key) {
                    Check(!valuesStarted, "YT key columns must precede value columns");
                    Check(column.At("sort_order").AsString() == "ascending", "YT key must be ascending");
                } else {
                    valuesStarted = true;
                }
                bool nullable;
                TString typeName;
                if (column.HasKey("type_v3")) {
                    const auto& type = column.At("type_v3");
                    nullable = false;
                    if (type.IsString()) {
                        typeName = type.AsString();
                    } else {
                        Check(type.IsMap() && type.At("type_name").AsString() == "optional", "Unsupported complex YT type_v3");
                        Check(type.At("item").IsString(), "Nested optional YT types are unsupported");
                        nullable = true;
                        typeName = type.At("item").AsString();
                    }
                } else {
                    typeName = column.At("type").AsString();
                    nullable = !column.HasKey("required") || !column.At("required").AsBool();
                }
                AddColumn(result, name, FindType(typeName), nullable, key);
            }
            Check(!result.KeyColumns.empty(), "YT schema requires sorted keys");
            schema = std::move(result);
            error.clear();
            return true;
        } catch (...) {
            error = CurrentExceptionMessage();
            return false;
        }
    }

    bool BuildSchema(const NKikimrSchemeOp::TTableDescription& description, TSchema& schema, TString& error) {
        try {
            TSchema result;
            THashSet<TString> keys;
            for (const auto& key : description.GetKeyColumnNames()) {
                Check(keys.insert(key).second, "Duplicate key column");
            }
            for (const auto& column : description.GetColumns()) {
                AddColumn(result, column.GetName(), FindInternalType(column), !column.GetNotNull(), keys.contains(column.GetName()));
            }
            result.KeyColumns.clear();
            result.Table.clear_primary_key();
            for (const auto& key : description.GetKeyColumnNames()) {
                bool found = false;
                for (ui32 index = 0; index < result.Columns.size(); ++index) {
                    if (result.Columns[index].Name == key) {
                        result.KeyColumns.push_back(index);
                        result.Table.add_primary_key(key);
                        found = true;
                        break;
                    }
                }
                Check(found, "Unknown key column");
            }
            Check(!result.KeyColumns.empty(), "YT schema requires sorted keys");
            result.Description = description;
            schema = std::move(result);
            error.clear();
            return true;
        } catch (...) {
            error = CurrentExceptionMessage();
            return false;
        }
    }

    bool ParsePartKey(TStringBuf prefix, TStringBuf key, TPartRange& range, TString& error) {
        auto fail = [&]() {
            error = "YT object key must be PREFIX_START_END with decimal START <= END";
            return false;
        };
        if (!key.StartsWith(prefix) || key.size() <= prefix.size() || key[prefix.size()] != '_') {
            return fail();
        }
        auto suffix = key.SubStr(prefix.size() + 1);
        const auto separator = suffix.find('_');
        if (separator == TStringBuf::npos || separator == 0 || separator + 1 == suffix.size()) {
            return fail();
        }
        auto start = suffix.SubStr(0, separator);
        auto end = suffix.SubStr(separator + 1);
        for (const auto part : {start, end}) {
            for (char character : part) {
                if (character < '0' || character > '9') {
                    return fail();
                }
            }
        }
        TPartRange result;
        if (!TryFromString(start, result.Start) || !TryFromString(end, result.End) || result.Start > result.End) {
            return fail();
        }
        range = result;
        error.clear();
        return true;
    }

    class TRowDecoder::TImpl: public ::NYT::NYson::TYsonConsumerBase {
    public:
        TImpl(const TSchema& schema, TCallback callback, size_t maxRowBytes)
            : Columns_(schema.Columns)
            , Callback_(std::move(callback))
            , MaxRowBytes_(maxRowBytes)
            , Parser_(this, ::NYT::NYson::EYsonType::ListFragment, Config(maxRowBytes))
        {
        }

        static ::NYT::NYson::TYsonParserConfig Config(size_t limit) {
            ::NYT::NYson::TYsonParserConfig config;
            config.MemoryLimit = limit;
            config.NestingLevelLimit = 4;
            config.EnableContext = false;
            return config;
        }

        void OnListItem() override {
            Check(!InRow_ && !ExpectRow_, "Unexpected YSON list item");
            ExpectRow_ = true;
        }
        void OnBeginMap() override {
            Check(ExpectRow_ && !InRow_, "YT row must be a scalar map");
            InRow_ = true;
            ExpectRow_ = false;
            Bytes_ = 0;
            Row_.Storage.assign(Columns_.size(), TString());
            Row_.Cells.assign(Columns_.size(), TCell());
            Seen_.assign(Columns_.size(), false);
        }
        void OnKeyedItem(TStringBuf key) override {
            Check(InRow_ && !Pending_, "Unexpected row field");
            Index_ = Columns_.size();
            for (size_t index = 0; index < Columns_.size(); ++index) {
                if (Columns_[index].Name == key) {
                    Index_ = index;
                    break;
                }
            }
            Check(Index_ < Columns_.size(), "Unknown YT row field");
            Check(!Seen_[Index_], "Duplicate YT row field");
            Seen_[Index_] = true;
            Pending_ = true;
        }
        void OnEndMap() override {
            Check(InRow_ && !Pending_, "Incomplete YT row");
            for (size_t index = 0; index < Seen_.size(); ++index) {
                Check(Seen_[index] || Columns_[index].Nullable, "Missing required YT field");
            }
            TString error;
            Check(Callback_(Row_, error), error.empty() ? TStringBuf("YT row rejected") : TStringBuf(error));
            InRow_ = false;
        }
        void OnEntity() override {
            Check(Pending_ && Columns_[Index_].Nullable, "Null in required YT column");
            Pending_ = false;
        }
        void OnStringScalar(TStringBuf value) override {
            auto type = Type();
            Check(type == Ydb::Type::STRING || type == Ydb::Type::UTF8, "String in non-string YT column");
            Check(type != Ydb::Type::UTF8 || IsUtf(value), "Invalid UTF8 in YT column");
            Store(value);
        }
        template <typename T, typename TValue>
        void Integer(TValue value) {
            Check(value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max(), "YT integer outside column range");
            T target = value;
            Store(TStringBuf(reinterpret_cast<const char*>(&target), sizeof(target)));
        }
        void OnInt64Scalar(i64 value) override {
            switch (Type()) {
                case Ydb::Type::INT8:
                    Integer<i8>(value);
                    break;
                case Ydb::Type::INT16:
                    Integer<i16>(value);
                    break;
                case Ydb::Type::INT32:
                    Integer<i32>(value);
                    break;
                case Ydb::Type::INT64:
                    Integer<i64>(value);
                    break;
                default:
                    ythrow yexception() << "Signed integer in non-signed YT column";
            }
        }
        void OnUint64Scalar(ui64 value) override {
            switch (Type()) {
                case Ydb::Type::UINT8:
                    Integer<ui8>(value);
                    break;
                case Ydb::Type::UINT16:
                    Integer<ui16>(value);
                    break;
                case Ydb::Type::UINT32:
                    Integer<ui32>(value);
                    break;
                case Ydb::Type::UINT64:
                    Integer<ui64>(value);
                    break;
                default:
                    ythrow yexception() << "Unsigned integer in non-unsigned YT column";
            }
        }
        void OnBooleanScalar(bool value) override {
            Check(Type() == Ydb::Type::BOOL, "Boolean in non-boolean YT column");
            Store(TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value)));
        }
        void OnDoubleScalar(double value) override {
            auto type = Type();
            Check(type == Ydb::Type::FLOAT || type == Ydb::Type::DOUBLE, "Double in non-floating YT column");
            if (type == Ydb::Type::FLOAT) {
                Check(!std::isfinite(value) || std::abs(value) <= std::numeric_limits<float>::max(), "YT float outside column range");
                float target = value;
                Store(TStringBuf(reinterpret_cast<const char*>(&target), sizeof(target)));
            } else {
                Store(TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value)));
            }
        }
        void OnBeginList() override {
            ythrow yexception() << "Nested YT lists unsupported";
        }
        void OnEndList() override {
            ythrow yexception() << "Unexpected YT list end";
        }
        void OnBeginAttributes() override {
            ythrow yexception() << "YT row attributes unsupported";
        }
        void OnEndAttributes() override {
            ythrow yexception() << "YT row attributes unsupported";
        }

        Ydb::Type::PrimitiveTypeId Type() {
            Check(InRow_ && Pending_, "YT row must be a map");
            return Columns_[Index_].Type;
        }
        void Store(TStringBuf bytes) {
            Check(bytes.size() <= MaxRowBytes_ - Bytes_, "YT row exceeds memory limit");
            Bytes_ += bytes.size();
            Row_.Storage[Index_] = bytes;
            Row_.Cells[Index_] = TCell(Row_.Storage[Index_].data(), Row_.Storage[Index_].size());
            Pending_ = false;
        }
        bool Feed(TStringBuf data, bool finish, TString& error) {
            if (Failed_ || Finished_) {
                error = "YT decoder already failed or finished";
                return false;
            }
            try {
                if (finish) {
                    Parser_.Finish();
                    Check(!InRow_ && !ExpectRow_, "Incomplete YT row at EOF");
                    Finished_ = true;
                } else {
                    Parser_.Read(data);
                }
                error.clear();
                return true;
            } catch (...) {
                Failed_ = true;
                error = CurrentExceptionMessage();
                return false;
            }
        }

    private:
        TVector<TColumn> Columns_;
        TCallback Callback_;
        size_t MaxRowBytes_;
        ::NYT::NYson::TYsonParser Parser_;
        TDecodedRow Row_;
        TVector<bool> Seen_;
        size_t Index_ = 0;
        size_t Bytes_ = 0;
        bool InRow_ = false;
        bool ExpectRow_ = false;
        bool Pending_ = false;
        bool Failed_ = false;
        bool Finished_ = false;
    };

    TRowDecoder::TRowDecoder(const TSchema& schema, TCallback callback, size_t maxRowBytes)
        : Impl_(MakeHolder<TImpl>(schema, std::move(callback), maxRowBytes))
    {
    }
    TRowDecoder::~TRowDecoder() = default;
    bool TRowDecoder::Feed(TStringBuf data, TString& error) {
        return Impl_->Feed(data, false, error);
    }
    bool TRowDecoder::Finish(TString& error) {
        return Impl_->Feed({}, true, error);
    }

} // namespace NKikimr::NBackup::NYt
