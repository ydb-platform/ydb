#pragma once
#include "defs.h"

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/lib/value/value.h>

#include <util/generic/maybe.h>

namespace NKikimr::NSQS {

class TExecutorBuilder;

class TParameters {
public:
    explicit TParameters(NKikimrMiniKQL::TParams* params, TExecutorBuilder* parent = nullptr)
        : Params_(params)
        , Parent(parent)
    {
        Params_->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    }

    explicit TParameters(NKikimrMiniKQL::TParams& params, TExecutorBuilder* parent = nullptr)
        : TParameters(&params, parent)
    {
    }

    TExecutorBuilder& ParentBuilder() {
        Y_ABORT_UNLESS(Parent);
        return *Parent;
    }

    TParameters& Bool(const TString& name, const bool value) {
        DataType(name, NScheme::NTypeIds::Bool);
        Params_->MutableValue()->AddStruct()->SetBool(value);
        return *this;
    }

    TParameters& String(const TString& name, const TString& value) {
        DataType(name, NScheme::NTypeIds::String);
        Params_->MutableValue()->AddStruct()->SetBytes(value);
        return *this;
    }

    TParameters& Uint64(const TString& name, ui64 value) {
        DataType(name, NScheme::NTypeIds::Uint64);
        Params_->MutableValue()->AddStruct()->SetUint64(value);
        return *this;
    }

    TParameters& Uint32(const TString& name, ui32 value) {
        DataType(name, NScheme::NTypeIds::Uint32);
        Params_->MutableValue()->AddStruct()->SetUint32(value);
        return *this;
    }

    TParameters& OptionalBool(const TString& name, const TMaybe<bool>& value) {
        OptionalDataType(name, NScheme::NTypeIds::Bool);
        if (value) {
            Params_->MutableValue()->AddStruct()->MutableOptional()->SetBool(*value);
        } else {
            Params_->MutableValue()->AddStruct();
        }
        return *this;
    }

    TParameters& OptionalUint64(const TString& name, const TMaybe<ui64>& value) {
        OptionalDataType(name, NScheme::NTypeIds::Uint64);
        if (value) {
            Params_->MutableValue()->AddStruct()->MutableOptional()->SetUint64(*value);
        } else {
            Params_->MutableValue()->AddStruct();
        }
        return *this;
    }

    TParameters& Utf8(const TString& name, const TString& value) {
        DataType(name, NScheme::NTypeIds::Utf8);
        Params_->MutableValue()->AddStruct()->SetText(value);
        return *this;
    }

    TParameters& OptionalUtf8(const TString& name, const TMaybe<TString>& value) {
        OptionalDataType(name, NScheme::NTypeIds::Utf8);
        if (value) {
            Params_->MutableValue()->AddStruct()->MutableOptional()->SetText(*value);
        } else {
            Params_->MutableValue()->AddStruct();
        }

        return *this;
    }
    
    TParameters& AddWithType(const TString& name, ui64 value, NScheme::TTypeId typeId) {
        DataType(name, typeId);
        if (typeId == NScheme::NTypeIds::Uint32) {
            Params_->MutableValue()->AddStruct()->SetUint32(value);
        } else if (typeId == NScheme::NTypeIds::Uint64) {
            Params_->MutableValue()->AddStruct()->SetUint64(value);
        } else {
            Y_ABORT_UNLESS(false);
        }
        return *this;
    }

private:
    void DataType(const TString& name, NScheme::TTypeId typeId) {
        auto* member = Params_->MutableType()->MutableStruct()->AddMember();
        member->SetName(name);
        member->MutableType()->SetKind(NKikimrMiniKQL::Data);
        member->MutableType()->MutableData()->SetScheme(typeId);
    }

    void OptionalDataType(const TString& name, NScheme::TTypeId typeId) {
        auto* member = Params_->MutableType()->MutableStruct()->AddMember();
        member->SetName(name);
        member->MutableType()->SetKind(NKikimrMiniKQL::Optional);
        member->MutableType()->MutableOptional()->MutableItem()->SetKind(NKikimrMiniKQL::Data);
        member->MutableType()->MutableOptional()->MutableItem()->MutableData()->SetScheme(typeId);
    }

private:
    NKikimrMiniKQL::TParams* const Params_;
    TExecutorBuilder* Parent = nullptr;
};

} // namespace NKikimr::NSQS
