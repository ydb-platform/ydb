#pragma once

#include <util/generic/string.h>
#include <util/generic/ptr.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/library/login/protos/login.pb.h>

namespace NKikimr {

bool IsLdapAuthenticationEnabled(const NKikimrProto::TAuthConfig& config);
bool IsUsernameFromLdapAuthDomain(const TString& username, const NKikimrProto::TAuthConfig& config);
TString PrepareLdapUsername(const TString& username, const NKikimrProto::TAuthConfig& config);
NKikimrScheme::TEvLogin CreatePlainLoginRequest(const TString& username, NLoginProto::EHashType::HashType hashType,
    const TString& hashToValidate, const TString& peerName, const NKikimrProto::TAuthConfig& config);
NKikimrScheme::TEvLogin CreatePlainLoginRequestOldFormat(const TString& username, const TString& password,
    const TString& peerName, const NKikimrProto::TAuthConfig& config);
NKikimrScheme::TEvLogin CreatePlainLdapLoginRequest(const TString& username,
     const TString& peerName, const NKikimrProto::TAuthConfig& config);
NKikimrScheme::TEvLogin CreateScramLoginRequest(const TString& username, NLoginProto::EHashType::HashType hashType,
    const TString& clientProof, const TString& authMessage,  const TString& peerName, const NKikimrProto::TAuthConfig& config);

} // namespace NKikimr
