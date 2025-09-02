#pragma once

#include <library/cpp/json/json_value.h>

#include <util/datetime/base.h>

namespace NKikimr {

struct TBillRecord {
#define BILL_RECORD_FIELD_SETTER(type, name) \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define BILL_RECORD_FIELD(type, name) \
    BILL_RECORD_FIELD_SETTER(type, name) \
    type name##_

#define BILL_RECORD_FIELD_DEFAULT(type, name, defaultValue) \
    BILL_RECORD_FIELD_SETTER(type, name) \
    type name##_ = defaultValue

    using TSelf = TBillRecord;

    struct TUsage {
        using TSelf = TUsage;

        enum class EType {
            Delta /* "delta" */,
        };

        enum class EUnit {
            RequestUnit /* "request_unit" */,
            MByte /* "mbyte" */,
        };

        BILL_RECORD_FIELD(EType, Type);
        BILL_RECORD_FIELD(EUnit, Unit);
        BILL_RECORD_FIELD(ui64, Quantity);
        BILL_RECORD_FIELD(TInstant, Start);
        BILL_RECORD_FIELD(TInstant, Finish);

        NJson::TJsonMap ToJson() const;
        TString ToString() const;

    }; // TUsage

    BILL_RECORD_FIELD_DEFAULT(TString, Version, "1.0.0");
    BILL_RECORD_FIELD(TString, Id);
    BILL_RECORD_FIELD_DEFAULT(TString, Schema, "ydb.serverless.requests.v1");
    BILL_RECORD_FIELD(TString, CloudId);
    BILL_RECORD_FIELD(TString, FolderId);
    BILL_RECORD_FIELD(TString, ResourceId);
    BILL_RECORD_FIELD_DEFAULT(TString, SourceId, "sless-docapi-ydb-ss");
    BILL_RECORD_FIELD(TInstant, SourceWt);
    BILL_RECORD_FIELD_DEFAULT(NJson::TJsonMap, Tags, {});
    BILL_RECORD_FIELD_DEFAULT(NJson::TJsonMap, Labels, {});
    BILL_RECORD_FIELD(TUsage, Usage);

    NJson::TJsonMap ToJson() const;
    TString ToString() const;

    static TUsage RequestUnits(ui64 quantity, TInstant start, TInstant finish);
    static TUsage RequestUnits(ui64 quantity, TInstant now);

#undef BILL_RECORD_FIELD_DEFAULT
#undef BILL_RECORD_FIELD
#undef BILL_RECORD_FIELD_SETTER
};

}   // NKikimr
