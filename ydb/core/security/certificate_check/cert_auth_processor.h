#pragma once
#include <openssl/x509.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <unordered_map>
#include <vector>

namespace NKikimr {

struct TCertificateAuthorizationParams {
    struct TRDN {
        TString Attribute;
        TVector<TString> Values;
        TVector<TString> Suffixes;

        TRDN(const TString& Attribute);
        TRDN& AddValue(const TString& val);
        TRDN& AddSuffix(const TString& suffix);
    };

    struct TDN {
        TVector<TRDN> RDNs;

        TDN& AddRDN(const TRDN& rdn);
        operator bool () const;
    };

    TCertificateAuthorizationParams(const TDN& dn = TDN(), bool requireSameIssuer = true, const std::vector<TString>& groups = {});
    TCertificateAuthorizationParams(TDN&& dn, bool requireSameIssuer = true, std::vector<TString>&& groups = {});

    operator bool () const;
    bool CheckSubject(const std::unordered_map<TString, std::vector<TString>>& subjectDescription) const;
    void SetSubjectDn(const TDN& subjectDn) {
        SubjectDn = subjectDn;
    }

    bool IsHostMatchAttributeCN(const TString&) const {
        return true;
    }

    bool CanCheckNodeByAttributeCN = false;
    TDN SubjectDn;
    bool RequireSameIssuer = true;
    std::vector<TString> Groups;
};


struct X509CertificateReader {
    template <auto fn>
    struct deleter_from_fn {
        template <typename T>
        constexpr void operator()(T* arg) const {
            fn(arg);
        }
    };

    using X509Ptr = std::unique_ptr<X509, deleter_from_fn<&::X509_free>>;
    using BIOPtr = std::unique_ptr<BIO, deleter_from_fn<&::BIO_free>>;

    static X509Ptr ReadCertAsPEM(const TStringBuf& cert);
    static TVector<std::pair<TString, TString>> ReadSubjectTerms(const X509Ptr& x509);
    static TVector<std::pair<TString, TString>> ReadAllSubjectTerms(const X509Ptr& x509);
    static TVector<std::pair<TString, TString>> ReadIssuerTerms(const X509Ptr& x509);
    static TString GetFingerprint(const X509Ptr& x509);
private:
    static std::pair<TString, TString> GetTermFromX509Name(X509_NAME* name, int nid);
    static TVector<std::pair<TString, TString>> ReadTerms(X509_NAME* name);
};

}  //namespace NKikimr
