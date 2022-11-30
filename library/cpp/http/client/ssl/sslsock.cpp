#include "sslsock.h"

#include <library/cpp/openssl/init/init.h>

#include <util/datetime/base.h>
#include <util/draft/holder_vector.h>
#include <util/generic/singleton.h>
#include <util/stream/str.h>
#include <util/system/mutex.h>

#include <contrib/libs/openssl/include/openssl/conf.h>
#include <contrib/libs/openssl/include/openssl/crypto.h>
#include <contrib/libs/openssl/include/openssl/err.h>
#include <contrib/libs/openssl/include/openssl/x509v3.h>

#include <contrib/libs/libc_compat/string.h>

namespace NHttpFetcher {
    namespace {
        struct TSslInit {
            inline TSslInit() {
                InitOpenSSL();
            }
        } SSL_INIT;
    }

    TString TSslSocketBase::GetSslErrors() {
        TStringStream ss;
        unsigned long err = 0;
        while (err = ERR_get_error()) {
            if (ss.Str().size()) {
                ss << "\n";
            }
            ss << ERR_error_string(err, nullptr);
        }
        return ss.Str();
    }

    class TSslSocketBase::TSslCtx {
    public:
        SSL_CTX* Ctx;

        TSslCtx() {
            const SSL_METHOD* method = SSLv23_method();
            if (!method) {
                TString err = GetSslErrors();
                Y_FAIL("SslSocket StaticInit: SSLv23_method failed: %s", err.data());
            }
            Ctx = SSL_CTX_new(method);
            if (!Ctx) {
                TString err = GetSslErrors();
                Y_FAIL("SSL_CTX_new: %s", err.data());
            }

            SSL_CTX_set_options(Ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);
            SSL_CTX_set_verify(Ctx, SSL_VERIFY_NONE, nullptr);
        }

        ~TSslCtx() {
            SSL_CTX_free(Ctx);
        }
    };

    SSL_CTX* TSslSocketBase::SslCtx() {
        return Singleton<TSslCtx>()->Ctx;
    }

    void TSslSocketBase::LoadCaCerts(const TString& caFile, const TString& caPath) {
        if (1 != SSL_CTX_load_verify_locations(SslCtx(),
                                               !!caFile ? caFile.data() : nullptr,
                                               !!caPath ? caPath.data() : nullptr))
        {
            ythrow yexception() << "Error loading CA certs: " << GetSslErrors();
        }
        SSL_CTX_set_verify(SslCtx(), SSL_VERIFY_PEER, nullptr);
    }

    namespace {
        enum EMatchResult {
            MATCH_FOUND,
            NO_MATCH,
            NO_EXTENSION,
            ERROR
        };

        bool EqualNoCase(TStringBuf a, TStringBuf b) {
            return (a.size() == b.size()) && (strncasecmp(a.data(), b.data(), a.size()) == 0);
        }

        bool MatchDomainName(TStringBuf tmpl, TStringBuf name) {
            // match wildcards only in the left-most part
            // do not support (optional according to RFC) partial wildcards (ww*.yandex.ru)
            // see RFC-6125
            TStringBuf tmplRest = tmpl;
            TStringBuf tmplFirst = tmplRest.NextTok('.');
            if (tmplFirst == "*") {
                tmpl = tmplRest;
                name.NextTok('.');
            }
            return EqualNoCase(tmpl, name);
        }

        EMatchResult MatchCertCommonName(X509* cert, TStringBuf hostname) {
            int commonNameLoc = X509_NAME_get_index_by_NID(X509_get_subject_name(cert), NID_commonName, -1);
            if (commonNameLoc < 0) {
                return ERROR;
            }

            X509_NAME_ENTRY* commonNameEntry = X509_NAME_get_entry(X509_get_subject_name(cert), commonNameLoc);
            if (!commonNameEntry) {
                return ERROR;
            }

            ASN1_STRING* commonNameAsn1 = X509_NAME_ENTRY_get_data(commonNameEntry);
            if (!commonNameAsn1) {
                return ERROR;
            }

            TStringBuf commonName((const char*)ASN1_STRING_get0_data(commonNameAsn1), ASN1_STRING_length(commonNameAsn1));

            return MatchDomainName(commonName, hostname)
                       ? MATCH_FOUND
                       : NO_MATCH;
        }

        EMatchResult MatchCertAltNames(X509* cert, TStringBuf hostname) {
            EMatchResult result = NO_MATCH;
            STACK_OF(GENERAL_NAME)* names = (STACK_OF(GENERAL_NAME)*)X509_get_ext_d2i(cert, NID_subject_alt_name, nullptr, NULL);
            if (!names) {
                return NO_EXTENSION;
            }

            int namesCt = sk_GENERAL_NAME_num(names);
            for (int i = 0; i < namesCt; ++i) {
                const GENERAL_NAME* name = sk_GENERAL_NAME_value(names, i);

                if (name->type == GEN_DNS) {
                    TStringBuf dnsName((const char*)ASN1_STRING_get0_data(name->d.dNSName), ASN1_STRING_length(name->d.dNSName));
                    if (MatchDomainName(dnsName, hostname)) {
                        result = MATCH_FOUND;
                        break;
                    }
                }
            }
            sk_GENERAL_NAME_pop_free(names, GENERAL_NAME_free);
            return result;
        }
    }

    bool TSslSocketBase::CheckCertHostname(X509* cert, TStringBuf hostname) {
        switch (MatchCertAltNames(cert, hostname)) {
            case MATCH_FOUND:
                return true;
                break;
            case NO_EXTENSION:
                return MatchCertCommonName(cert, hostname) == MATCH_FOUND;
                break;
            default:
                return false;
        }
    }

}
