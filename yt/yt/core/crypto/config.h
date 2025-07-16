#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

//! Either an inlined value, environment variable, or a file reference.
struct TPemBlobConfig
    : public NYTree::TYsonStruct
{
    std::optional<TString> EnvironmentVariable;
    std::optional<TString> FileName;
    std::optional<TString> Value;

    TString LoadBlob() const;

    static TPemBlobConfigPtr CreateFileReference(const TString& fileName);

    REGISTER_YSON_STRUCT(TPemBlobConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPemBlobConfig)

////////////////////////////////////////////////////////////////////////////////

//! FIXME: Enabled during migration, because this code has always been broken.
constexpr bool DefaultInsecureSkipVerify = true;

struct TSslContextCommand
    : public NYTree::TYsonStruct
{
    std::string Name;
    std::string Value;

    static TSslContextCommandPtr Create(std::string name, std::string value);

    REGISTER_YSON_STRUCT(TSslContextCommand);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSslContextCommand)

////////////////////////////////////////////////////////////////////////////////

struct TSslContextConfig
    : public virtual NYTree::TYsonStruct
{
    NCrypto::TPemBlobConfigPtr CertificateAuthority;
    NCrypto::TPemBlobConfigPtr CertificateChain;
    NCrypto::TPemBlobConfigPtr PrivateKey;

    //! Commands for SSL context configuration handled by SSL_CONF_cmd.
    std::vector<TSslContextCommandPtr> SslConfigurationCommands;

    //! Trust everybody, never verify certificate, issue warning - for testing purpose.
    bool InsecureSkipVerify;

    REGISTER_YSON_STRUCT(TSslContextConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSslContextConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
