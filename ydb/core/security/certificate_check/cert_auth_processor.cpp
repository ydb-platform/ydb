#include "cert_auth_processor.h"

#include <openssl/x509.h>
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

TString X509CertificateReader::GetFingerprint(const X509Ptr& x509) {
    static constexpr size_t FINGERPRINT_LENGTH = SHA_DIGEST_LENGTH;
    unsigned char fingerprint[FINGERPRINT_LENGTH];
    if (X509_digest(x509.get(), EVP_sha1(), fingerprint, nullptr) <= 0) {
        return "";
    }
    return HexEncode(fingerprint, FINGERPRINT_LENGTH);
}

TCertificateAuthorizationParams::TCertificateAuthorizationParams(const TDN& dn, bool requireSameIssuer, const std::vector<TString>& groups)
    : SubjectDn(dn)
    , RequireSameIssuer(requireSameIssuer)
    , Groups(groups)
{}

TCertificateAuthorizationParams::TCertificateAuthorizationParams(TDN&& dn, bool requireSameIssuer, std::vector<TString>&& groups)
    : SubjectDn(std::move(dn))
    , RequireSameIssuer(requireSameIssuer)
    , Groups(std::move(groups))
{}

TCertificateAuthorizationParams::TDN& TCertificateAuthorizationParams::TDN::AddRDN(const TRDN& rdn) {
    RDNs.push_back(rdn);
    return *this;
}

TCertificateAuthorizationParams::operator bool() const {
    return SubjectDn;
}

bool TCertificateAuthorizationParams::CheckSubject(const std::unordered_map<TString, std::vector<TString>>& subjectDescription) const {
    bool isDescriptionMatched = false;
    for (const auto& rdn: SubjectDn.RDNs) {
        isDescriptionMatched = false;
        auto fieldIt = subjectDescription.find(rdn.Attribute);
        if (fieldIt == subjectDescription.cend()) {
            break;
        }

        const auto& attributeValues = fieldIt->second;
        bool attributeMatched = false;
        for (const auto& attributeValue : attributeValues) {
            attributeMatched = false;
            for (const auto& value: rdn.Values) {
                if (value == attributeValue) {
                    attributeMatched = true;
                    break;
                }
            }
            if (!attributeMatched) {
                for (const auto& suffix: rdn.Suffixes) {
                    if (attributeValue.EndsWith(suffix)) {
                        attributeMatched = true;
                        break;
                    }
                }
            }
            if (!attributeMatched) {
                break;
            }
        }
        if (!attributeMatched) {
            isDescriptionMatched = false;
            break;
        }
        isDescriptionMatched = true;
    }

    if (isDescriptionMatched) {
        return true;
    }
    return false;
}

TCertificateAuthorizationParams::TDN::operator bool() const {
    return !RDNs.empty();
}

TCertificateAuthorizationParams::TRDN::TRDN(const TString& Attribute)
    :Attribute(Attribute)
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

}  //namespace NKikimr {
