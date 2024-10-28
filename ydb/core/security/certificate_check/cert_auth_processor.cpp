#include "cert_auth_processor.h"

#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/objects.h>
#include <openssl/obj_mac.h>
#include <openssl/sha.h>

#include <util/generic/yexception.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/string/hex.h>

namespace NKikimr {

X509CertificateReader::X509Ptr X509CertificateReader::ReadCertAsPEM(const TStringBuf& cert) {
    auto bio = BIOPtr(BIO_new_mem_buf(cert.data(), cert.size()));
    if (!bio) {
        return {};
    }

    auto x509 = X509Ptr(PEM_read_bio_X509(bio.get(), NULL, NULL, NULL));
    if (!x509) {
        return {};
    }

    return x509;
}

std::pair<TString, TString> X509CertificateReader::GetTermFromX509Name(X509_NAME* name, int nid) {
    if (name == nullptr) {
        return {};
    }

    const char* sn = OBJ_nid2sn(nid);
    if (sn == nullptr) {
        return {};
    }

    char cnbuf[1024];
    int name_len = X509_NAME_get_text_by_NID(name, nid, cnbuf, sizeof(cnbuf));
    if (name_len <= 0) {
        return {};
    }

    return std::make_pair(TString(sn, strlen(sn)), TString(cnbuf, name_len));
}

TVector<std::pair<TString, TString>> X509CertificateReader::ReadSubjectTerms(const X509Ptr& x509) {
    X509_NAME* name = X509_get_subject_name(x509.get()); // return internal pointer

    TVector<std::pair<TString, TString>> extractions = {
        GetTermFromX509Name(name, NID_countryName),
        GetTermFromX509Name(name, NID_stateOrProvinceName),
        GetTermFromX509Name(name, NID_localityName),
        GetTermFromX509Name(name, NID_organizationName),
        GetTermFromX509Name(name, NID_organizationalUnitName),
        GetTermFromX509Name(name, NID_commonName)
    };

    auto newEnd = std::remove(extractions.begin(), extractions.end(), std::pair<TString, TString>{});
    extractions.erase(newEnd, extractions.end());

    return extractions;
}

TVector<std::pair<TString, TString>> X509CertificateReader::ReadTerms(X509_NAME* name) {
    TVector<std::pair<TString, TString>> subjectTerms;
    int entryCount = X509_NAME_entry_count(name);
    subjectTerms.reserve(entryCount);
    for (int i = 0; i < entryCount; i++) {
        const X509_NAME_ENTRY* entry = X509_NAME_get_entry(name, i);
        if (!entry) {
            continue;
        }
        const ASN1_STRING* data = X509_NAME_ENTRY_get_data(entry);
        if (!data) {
            continue;
        }

        const ASN1_OBJECT* object = X509_NAME_ENTRY_get_object(entry);
        if (!object) {
            continue;
        }
        const int nid = OBJ_obj2nid(object);
        const char* sn = OBJ_nid2sn(nid);
        subjectTerms.push_back(std::make_pair(TString(sn, std::strlen(sn)), TString(reinterpret_cast<char*>(data->data), data->length)));
    }
    return subjectTerms;
}

TVector<std::pair<TString, TString>> X509CertificateReader::ReadAllSubjectTerms(const X509Ptr& x509) {
    X509_NAME* name = X509_get_subject_name(x509.get()); // return internal pointer
    return ReadTerms(name);
}

TVector<std::pair<TString, TString>> X509CertificateReader::ReadIssuerTerms(const X509Ptr& x509) {
    X509_NAME* name = X509_get_issuer_name(x509.get()); // return internal pointer
    return ReadTerms(name);
}

static void FreeList(GENERAL_NAMES* list) {
    sk_GENERAL_NAME_pop_free(list, GENERAL_NAME_free);
}

TVector<TString> X509CertificateReader::ReadSubjectDns(const X509Ptr& x509, const std::vector<std::pair<TString, TString>>& subjectTerms) {
    TVector<TString> result;
    // 1. Subject's common name (CN) must be a subject DNS name, so add it to DNS names of subject first
    for (const auto& [k, v] : subjectTerms) {
        if (k == "CN") {
            result.emplace_back(v);
        }
    }

    using TGeneralNamesPtr = std::unique_ptr<GENERAL_NAMES, deleter_from_fn<&FreeList>>;
    TGeneralNamesPtr subjectAltNames((GENERAL_NAMES*)X509_get_ext_d2i(x509.get(), NID_subject_alt_name, NULL, NULL));
    if (!subjectAltNames) {
        return result;
    }
    const int subjectAltNamesCount = sk_GENERAL_NAME_num(subjectAltNames.get());
    if (subjectAltNamesCount <= 0) {
        return result;
    }

    result.reserve(static_cast<size_t>(subjectAltNamesCount) + result.size());
    // 2. Additionally find subject alternative names with type=DNS
    for (int i = 0; i < subjectAltNamesCount; ++i) {
        const GENERAL_NAME* name = sk_GENERAL_NAME_value(subjectAltNames.get(), i);
        if (!name) {
            continue;
        }
        if (name->type == GEN_DNS) {
            const ASN1_STRING* value = name->d.dNSName;
            if (!value) {
                continue;
            }

            const char* data = reinterpret_cast<const char*>(ASN1_STRING_get0_data(value));
            if (!data) {
                continue;
            }
            int size = ASN1_STRING_length(value);
            if (size <= 0) {
                continue;
            }
            result.emplace_back(data, static_cast<size_t>(size));
        }
    }
    return result;
}

TString X509CertificateReader::GetFingerprint(const X509Ptr& x509) {
    static constexpr size_t FINGERPRINT_LENGTH = SHA_DIGEST_LENGTH;
    unsigned char fingerprint[FINGERPRINT_LENGTH];
    if (X509_digest(x509.get(), EVP_sha1(), fingerprint, nullptr) <= 0) {
        return "";
    }
    return HexEncode(fingerprint, FINGERPRINT_LENGTH);
}

TCertificateAuthorizationParams::TCertificateAuthorizationParams(const TDN& dn, const std::optional<TRDN>& subjectDns, bool requireSameIssuer, const std::vector<TString>& groups)
    : SubjectDn(dn)
    , SubjectDns(subjectDns)
    , RequireSameIssuer(requireSameIssuer)
    , Groups(groups)
{}

TCertificateAuthorizationParams::TCertificateAuthorizationParams(TDN&& dn, std::optional<TRDN>&& subjectDns, bool requireSameIssuer, std::vector<TString>&& groups)
    : SubjectDn(std::move(dn))
    , SubjectDns(std::move(subjectDns))
    , RequireSameIssuer(requireSameIssuer)
    , Groups(std::move(groups))
{}

TCertificateAuthorizationParams::TDN& TCertificateAuthorizationParams::TDN::AddRDN(const TRDN& rdn) {
    RDNs.push_back(rdn);
    return *this;
}

TCertificateAuthorizationParams::operator bool() const {
    return SubjectDn || SubjectDns;
}

bool TCertificateAuthorizationParams::CheckSubject(const std::unordered_map<TString, std::vector<TString>>& subjectDescription, const std::vector<TString>& subjectDns) const {
    for (const TRDN& rdn: SubjectDn.RDNs) {
        auto fieldIt = subjectDescription.find(rdn.Attribute);
        if (fieldIt == subjectDescription.cend()) {
            return false;
        }

        const auto& attributeValues = fieldIt->second;
        if (!rdn.Match(attributeValues)) {
            return false;
        }
    }

    if (SubjectDns) {
        bool dnsMatched = false;
        for (const TString& dns : subjectDns) {
            if (SubjectDns->Match(dns)) {
                dnsMatched = true;
                break;
            }
        }
        if (!dnsMatched) {
            return false;
        }
    }

    return true;
}

TCertificateAuthorizationParams::TDN::operator bool() const {
    return !RDNs.empty();
}

TCertificateAuthorizationParams::TRDN::TRDN(const TString& attribute)
    : Attribute(attribute)
{}

TCertificateAuthorizationParams::TRDN& TCertificateAuthorizationParams::TRDN::AddValue(const TString& val)
{
    Values.push_back(val);
    return *this;
}

TCertificateAuthorizationParams::TRDN& TCertificateAuthorizationParams::TRDN::AddSuffix(const TString& suffix)
{
    Suffixes.push_back(suffix);
    return *this;
}

bool TCertificateAuthorizationParams::TRDN::Match(const TString& value) const
{
    for (const auto& v : Values) {
        if (value == v) {
            return true;
        }
    }
    for (const auto& s : Suffixes) {
        if (value.EndsWith(s)) {
            return true;
        }
    }

    return false;
}

bool TCertificateAuthorizationParams::TRDN::Match(const std::vector<TString>& values) const
{
    for (const auto& value : values) {
        if (!Match(value)) {
            return false;
        }
    }
    return true;
}

}  //namespace NKikimr {
