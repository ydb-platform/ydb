#include "external_data_source_description.h"
#include "ydb_convert.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

#include <google/protobuf/map.h>

namespace NKikimr {

namespace {

using namespace NKikimrSchemeOp;
using TProperties = google::protobuf::Map<TProtoStringType, TProtoStringType>;

TString AdjustSecretSettingName(TStringBuf secretName, const TString& secretSettingPrefix) {
    return secretName.StartsWith('/') ? secretSettingPrefix + "_PATH" : secretSettingPrefix + "_NAME";
}

void SetSecretSettingName(TStringBuf secretName, const TString& secretSettingPrefix, TProperties& out) {
    out[AdjustSecretSettingName(secretName, secretSettingPrefix)] = secretName;
}

void Convert(const TServiceAccountAuth& in, TProperties& out) {
    out["SERVICE_ACCOUNT_ID"] = in.GetId();
    SetSecretSettingName(in.GetSecretName(), "SERVICE_ACCOUNT_SECRET", out);
}

void Convert(const TBasic& in, TProperties& out) {
    out["LOGIN"] = in.GetLogin();
    SetSecretSettingName(in.GetPasswordSecretName(), "PASSWORD_SECRET", out);
}

void Convert(const TMdbBasic& in, TProperties& out) {
    out["SERVICE_ACCOUNT_ID"] = in.GetServiceAccountId();
    SetSecretSettingName(in.GetServiceAccountSecretName(), "SERVICE_ACCOUNT_SECRET", out);
    out["LOGIN"] = in.GetLogin();
    SetSecretSettingName(in.GetPasswordSecretName(), "PASSWORD_SECRET", out);
}

void Convert(const TAws& in, TProperties& out) {
    SetSecretSettingName(in.GetAwsAccessKeyIdSecretName(), "AWS_ACCESS_KEY_ID_SECRET", out);
    SetSecretSettingName(in.GetAwsSecretAccessKeySecretName(), "AWS_SECRET_ACCESS_KEY_SECRET", out);
    out["AWS_REGION"] = in.GetAwsRegion();
}

void Convert(const TToken& in, TProperties& out) {
    SetSecretSettingName(in.GetTokenSecretName(), "TOKEN_SECRET", out);
}

void Convert(const TAuth& in, TProperties& out) {
    auto& authMethod = out["AUTH_METHOD"];

    switch (in.GetIdentityCase()) {
    case TAuth::kNone:
        authMethod = "NONE";
        return;
    case TAuth::kServiceAccount:
        authMethod = "SERVICE_ACCOUNT";
        Convert(in.GetServiceAccount(), out);
        return;
    case TAuth::kBasic:
        authMethod = "BASIC";
        Convert(in.GetBasic(), out);
        return;
    case TAuth::kMdbBasic:
        authMethod = "MDB_BASIC";
        Convert(in.GetMdbBasic(), out);
        return;
    case TAuth::kAws:
        authMethod = "AWS";
        Convert(in.GetAws(), out);
        return;
    case TAuth::kToken:
        authMethod = "TOKEN";
        Convert(in.GetToken(), out);
        return;
    case TAuth::IDENTITY_NOT_SET:
        return;
    }
}

void Convert(const TExternalTableReferences& in, TProperties& out) {
    auto& references = out["REFERENCES"];
    NJson::TJsonValue json(NJson::EJsonValueType::JSON_ARRAY);

    for (const auto& reference : in.GetReferences()) {
        if (reference.HasPath()) {
            json.AppendValue(reference.GetPath());
        } else if (reference.HasPathId()) {
            // this should never happen, but in case it does, it is better to return something meaningful
            json.AppendValue(TPathId::FromProto(reference.GetPathId()).ToString());
        }
    }
    references = WriteJson(json, false);
}

} // anonymous namespace

void FillExternalDataSourceDescription(
    Ydb::Table::DescribeExternalDataSourceResult& out,
    const NKikimrSchemeOp::TExternalDataSourceDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry)
{
    ConvertDirectoryEntry(inDirEntry, out.mutable_self(), true);

    out.set_source_type(inDesc.GetSourceType());
    out.set_location(inDesc.GetLocation());
    auto& properties = *out.mutable_properties();
    for (const auto& [key, value] : inDesc.GetProperties().GetProperties()) {
        properties[to_upper(key)] = value;
    }
    Convert(inDesc.GetAuth(), properties);
    Convert(inDesc.GetReferences(), properties);
}

} // namespace NKikimr
