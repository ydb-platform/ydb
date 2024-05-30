#pragma once
#include <openssl/x509.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <unordered_map>
#include <vector>

namespace NKikimr {

struct TDynamicNodeAuthorizationParams {
    struct TRelativeDistinguishedName {
        TString Attribute;
        TVector<TString> Values;
        TVector<TString> Suffixes;

        TRelativeDistinguishedName(const TString& Attribute);
        TRelativeDistinguishedName& AddValue(const TString& val);
        TRelativeDistinguishedName& AddSuffix(const TString& suffix);
    };

    struct TDistinguishedName {
        TVector<TRelativeDistinguishedName> RelativeDistinguishedNames;

        TDistinguishedName& AddRelativeDistinguishedName(TRelativeDistinguishedName name);
    };

    operator bool () const;
    bool IsSubjectDescriptionMatched(const std::unordered_map<TString, std::vector<TString>>& subjectDescription) const;
    TDynamicNodeAuthorizationParams& AddCertSubjectDescription(const TDistinguishedName& description) {
        CertSubjectsDescriptions.push_back(description);
        return *this;
    }

    bool IsHostMatchAttributeCN(const TString&) const {
        return true;
    }

    bool CanCheckNodeByAttributeCN = false;
    TVector<TDistinguishedName> CertSubjectsDescriptions;
    bool NeedCheckIssuer = true;
    TString SidName;
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
private:
    static std::pair<TString, TString> GetTermFromX509Name(X509_NAME* name, int nid);
};

}  //namespace NKikimr
