#include "impl.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>

#include <util/string/builder.h>

namespace NYdb::inline Dev {

namespace {

bool IsTypeComplete(const Ydb::Type& type) {
    switch (type.type_case()) {
        case Ydb::Type::TYPE_NOT_SET:
            return false;
        case Ydb::Type::kOptionalType:
            return IsTypeComplete(type.optional_type().item());
        case Ydb::Type::kListType:
            return IsTypeComplete(type.list_type().item());
        case Ydb::Type::kTupleType:
            for (const auto& element : type.tuple_type().elements()) {
                if (!IsTypeComplete(element)) {
                    return false;
                }
            }
            return true;
        case Ydb::Type::kStructType:
            for (const auto& member : type.struct_type().members()) {
                if (!IsTypeComplete(member.type())) {
                    return false;
                }
            }
            return true;
        case Ydb::Type::kDictType:
            return IsTypeComplete(type.dict_type().key()) && IsTypeComplete(type.dict_type().payload());
        case Ydb::Type::kVariantType: {
            const auto& variant = type.variant_type();
            switch (variant.type_case()) {
                case Ydb::VariantType::TYPE_NOT_SET:
                    return false;
                case Ydb::VariantType::kTupleItems:
                    for (const auto& element : variant.tuple_items().elements()) {
                        if (!IsTypeComplete(element)) {
                            return false;
                        }
                    }
                    return true;
                case Ydb::VariantType::kStructItems:
                    for (const auto& member : variant.struct_items().members()) {
                        if (!IsTypeComplete(member.type())) {
                            return false;
                        }
                    }
                    return true;
            }
            return false;
        }
        case Ydb::Type::kTaggedType:
            return IsTypeComplete(type.tagged_type().type());
        case Ydb::Type::kTypeId:
        case Ydb::Type::kDecimalType:
        case Ydb::Type::kVoidType:
        case Ydb::Type::kNullType:
        case Ydb::Type::kEmptyListType:
        case Ydb::Type::kEmptyDictType:
        case Ydb::Type::kPgType:
            return true;
    }
    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TParams::TParams(::google::protobuf::Map<TStringType, Ydb::TypedValue>&& protoMap)
    : Impl_(new TImpl(std::move(protoMap))) {}

::google::protobuf::Map<TStringType, Ydb::TypedValue>* TParams::GetProtoMapPtr() {
    return Impl_->GetProtoMapPtr();
}

const ::google::protobuf::Map<TStringType, Ydb::TypedValue>& TParams::GetProtoMap() const {
    return Impl_->GetProtoMap();
}

bool TParams::Empty() const {
    return Impl_->Empty();
}

std::map<std::string, TValue> TParams::GetValues() const {
    return Impl_->GetValues();
}

std::optional<TValue> TParams::GetValue(const std::string& name) const {
    return Impl_->GetValue(name);
}

////////////////////////////////////////////////////////////////////////////////

class TParamsBuilder::TImpl {
public:
    TImpl() = default;

    TImpl(const ::google::protobuf::Map<TStringType, Ydb::Type>& typeInfo)
        : HasTypeInfo_(true)
    {
        for (const auto& pair : typeInfo) {
            ParamsMap_[pair.first].mutable_type()->CopyFrom(pair.second);
        }
    }

    TImpl(const std::map<std::string, TType>& typeInfo)
        : HasTypeInfo_(true)
    {
        for (const auto& pair : typeInfo) {
            ParamsMap_[pair.first].mutable_type()->CopyFrom(pair.second.GetProto());
        }
    }

    bool HasTypeInfo() const {
        return HasTypeInfo_;
    }

    TParamValueBuilder& AddParam(TParamsBuilder& owner, const std::string& name) {
        auto param = GetParam(name);
        Y_ABORT_UNLESS(param);

        auto result = ValueBuildersMap_.emplace(name, TParamValueBuilder(owner, *param->mutable_type(),
            *param->mutable_value()));

        return result.first->second;
    }

    void AddParam(const std::string& name, const TValue& value) {
        auto param = GetParam(name);
        Y_ABORT_UNLESS(param);

        if (HasTypeInfo()) {
            if (!TypesEqual(param->type(), value.GetType().GetProto())) {
                FatalError(TStringBuilder() << "Type mismatch for parameter: " << name << ", expected: "
                    << FormatType(TType(param->type())) << ", actual: " << FormatType(value.GetType()));
            }
        } else {
            param->mutable_type()->CopyFrom(value.GetType().GetProto());
        }

        param->mutable_value()->CopyFrom(value.GetProto());
    }

    TParams Build() {
        for (auto& pair : ValueBuildersMap_) {
            if (!pair.second.Finished()) {
                FatalError(TStringBuilder() << "Incomplete value for parameter: " << pair.first
                    << ", call Build() on parameter value builder");
            }
        }

        for (const auto& [name, param] : ParamsMap_) {
            if (!IsTypeComplete(param.type())) {
                FatalError(TStringBuilder() << "Parameter '" << name
                    << "' has an invalid type: protobuf type is not set; check how the parameter value was built");
            }
        }

        ValueBuildersMap_.clear();

        ::google::protobuf::Map<TStringType, Ydb::TypedValue> paramsMap;
        paramsMap.swap(ParamsMap_);
        return TParams(std::move(paramsMap));
    }

private:
    Ydb::TypedValue* GetParam(const std::string& name) {
        if (HasTypeInfo()) {
            auto it = ParamsMap_.find(name);
            if (it == ParamsMap_.end()) {
                FatalError(TStringBuilder() << "Parameter not found: " << name);
                return nullptr;
            }

            return &it->second;
        } else {
            return &ParamsMap_[name];
        }
    }

    void FatalError(const std::string& msg) const {
        ThrowFatalError(TStringBuilder() << "TParamsBuilder: " << msg);
    }

private:
    bool HasTypeInfo_ = false;
    ::google::protobuf::Map<TStringType, Ydb::TypedValue> ParamsMap_;
    std::map<std::string, TParamValueBuilder> ValueBuildersMap_;
};

////////////////////////////////////////////////////////////////////////////////

TParamValueBuilder::TParamValueBuilder(TParamsBuilder& owner, Ydb::Type& typeProto, Ydb::Value& valueProto)
    : TValueBuilderBase(typeProto, valueProto)
    , Owner_(owner)
    , Finished_(false) {}

bool TParamValueBuilder::Finished() {
    return Finished_;
}

TParamsBuilder& TParamValueBuilder::Build() {
    CheckValue();

    Finished_ = true;
    return Owner_;
}

////////////////////////////////////////////////////////////////////////////////

TParamsBuilder::TParamsBuilder(TParamsBuilder&&) = default;
TParamsBuilder::~TParamsBuilder() = default;

TParamsBuilder::TParamsBuilder()
    : Impl_(new TImpl()) {}

TParamsBuilder::TParamsBuilder(const std::map<std::string, TType>& typeInfo)
    : Impl_(new TImpl(typeInfo)) {}

TParamsBuilder::TParamsBuilder(const ::google::protobuf::Map<TStringType, Ydb::Type>& typeInfo)
    : Impl_(new TImpl(typeInfo)) {}

bool TParamsBuilder::HasTypeInfo() const {
    return Impl_->HasTypeInfo();
}

TParamValueBuilder& TParamsBuilder::AddParam(const std::string& name) {
    return Impl_->AddParam(*this, name);
}

TParamsBuilder& TParamsBuilder::AddParam(const std::string& name, const TValue& value) {
    Impl_->AddParam(name, value);
    return *this;
}

TParams TParamsBuilder::Build() {
    return Impl_->Build();
}

} // namespace NYdb
