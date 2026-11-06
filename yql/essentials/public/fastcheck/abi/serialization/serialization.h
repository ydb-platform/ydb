#pragma once

#include <yql/essentials/public/fastcheck/fastcheck.h>
#include <yql/essentials/public/fastcheck/linter.h>
#include <yql/essentials/public/fastcheck/abi/protos/fastcheck.pb.h>

namespace NYql::NFastCheck::NAbi {

void Encode(const TOptions& value, NProto::TFastCheckRequest& message);
void Decode(const NProto::TFastCheckRequest& message, TOptions& value);
void Encode(const TChecksRequest& value, NProto::TLinterRequest& message);
void Decode(const NProto::TLinterRequest& message, TChecksRequest& value);
void EncodeMap(const THashMap<TString, TString>& value, google::protobuf::RepeatedPtrField<NProto::TStringPair>& message);
void DecodeMap(const google::protobuf::RepeatedPtrField<NProto::TStringPair>& message, THashMap<TString, TString>& value);
void EncodeFilters(const TMaybe<TVector<TCheckFilter>>& value, NProto::TLinterRequest& message);
void DecodeFilters(const NProto::TLinterRequest& message, TMaybe<TVector<TCheckFilter>>& value);

} // namespace NYql::NFastCheck::NAbi
