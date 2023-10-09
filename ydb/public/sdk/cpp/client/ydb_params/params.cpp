#include "params.h"
#include "impl.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NYdb {

////////////////////////////////////////////////////////////////////////////////

TParams::TParams(::google::protobuf::Map<TString, Ydb::TypedValue>&& protoMap)
    : Impl_(new TImpl(std::move(protoMap))) {}

::google::protobuf::Map<TString, Ydb::TypedValue>* TParams::GetProtoMapPtr() {
    return Impl_->GetProtoMapPtr();
}

const ::google::protobuf::Map<TString, Ydb::TypedValue>& TParams::GetProtoMap() const {
    return Impl_->GetProtoMap();
}

bool TParams::Empty() const {
    return Impl_->Empty();
}

TMap<TString, TValue> TParams::GetValues() const {
    return Impl_->GetValues();
}

TMaybe<TValue> TParams::GetValue(const TString& name) const {
    return Impl_->GetValue(name);
}

////////////////////////////////////////////////////////////////////////////////

class TParamsBuilder::TImpl {
public:
    TImpl() = default;

    TImpl(const ::google::protobuf::Map<TString, Ydb::Type>& typeInfo)
        : HasTypeInfo_(true)
    {
        for (const auto& pair : typeInfo) {
            ParamsMap_[pair.first].mutable_type()->CopyFrom(pair.second);
        }
    }

    TImpl(const TMap<TString, TType>& typeInfo)
        : HasTypeInfo_(true)
    {
        for (const auto& pair : typeInfo) {
            ParamsMap_[pair.first].mutable_type()->CopyFrom(pair.second.GetProto());
        }
    }

    bool HasTypeInfo() const {
        return HasTypeInfo_;
    }

    TParamValueBuilder& AddParam(TParamsBuilder& owner, const TString& name) {
        auto param = GetParam(name);
        Y_ABORT_UNLESS(param);

        auto result = ValueBuildersMap_.emplace(name, TParamValueBuilder(owner, *param->mutable_type(),
            *param->mutable_value()));

        return result.first->second;
    }

    void AddParam(const TString& name, const TValue& value) {
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

        ValueBuildersMap_.clear();

        ::google::protobuf::Map<TString, Ydb::TypedValue> paramsMap;
        paramsMap.swap(ParamsMap_);
        return TParams(std::move(paramsMap));
    }

private:
    Ydb::TypedValue* GetParam(const TString& name) {
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

    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TParamsBuilder: " << msg);
    }

private:
    bool HasTypeInfo_ = false;
    ::google::protobuf::Map<TString, Ydb::TypedValue> ParamsMap_;
    TMap<TString, TParamValueBuilder> ValueBuildersMap_;
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

TParamsBuilder::TParamsBuilder(const TMap<TString, TType>& typeInfo)
    : Impl_(new TImpl(typeInfo)) {}

TParamsBuilder::TParamsBuilder(const ::google::protobuf::Map<TString, Ydb::Type>& typeInfo)
    : Impl_(new TImpl(typeInfo)) {}

bool TParamsBuilder::HasTypeInfo() const {
    return Impl_->HasTypeInfo();
}

TParamValueBuilder& TParamsBuilder::AddParam(const TString& name) {
    return Impl_->AddParam(*this, name);
}

TParamsBuilder& TParamsBuilder::AddParam(const TString& name, const TValue& value) {
    Impl_->AddParam(name, value);
    return *this;
}

TParams TParamsBuilder::Build() {
    return Impl_->Build();
}

} // namespace NYdb
