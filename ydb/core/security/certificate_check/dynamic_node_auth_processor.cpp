#include "dynamic_node_auth_processor.h"

#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/objects.h>
#include <openssl/obj_mac.h>

#include <util/generic/yexception.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

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

TVector<std::pair<TString, TString>> X509CertificateReader::ReadAllSubjectTerms(const X509Ptr& x509) {
    TVector<std::pair<TString, TString>> subjectTerms;
    X509_NAME* name = X509_get_subject_name(x509.get()); // return internal pointer

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

TVector<std::pair<TString, TString>> X509CertificateReader::ReadIssuerTerms(const X509Ptr& x509) {
    TVector<std::pair<TString, TString>> issuerTerms;
    X509_NAME* name = X509_get_issuer_name(x509.get()); // return internal pointer

    int entryCount = X509_NAME_entry_count(name);
    issuerTerms.reserve(entryCount);
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
        issuerTerms.push_back(std::make_pair(TString(sn, std::strlen(sn)), TString(reinterpret_cast<char*>(data->data), data->length)));
    }
    return issuerTerms;
}

TDynamicNodeAuthorizationParams::TDistinguishedName& TDynamicNodeAuthorizationParams::TDistinguishedName::AddRelativeDistinguishedName(TRelativeDistinguishedName name) {
    RelativeDistinguishedNames.push_back(std::move(name));
    return *this;
}

TDynamicNodeAuthorizationParams::operator bool() const {
    return !CertSubjectsDescriptions.empty();
}

bool TDynamicNodeAuthorizationParams::IsSubjectDescriptionMatched(const std::unordered_map<TString, std::vector<TString>>& subjectDescription) const {
    for (const auto& description: CertSubjectsDescriptions) {
        bool isDescriptionMatched = false;
        for (const auto& name: description.RelativeDistinguishedNames) {
            Cerr << "+++ Check " << name.Attribute << Endl;
            isDescriptionMatched = false;
            auto fieldIt = subjectDescription.find(name.Attribute);
            if (fieldIt == subjectDescription.cend()) {
                break;
            }

            const auto& attributeValues = fieldIt->second;
            bool attributeMatched = false;
            for (const auto& attributeValue : attributeValues) {
                attributeMatched = false;
                for (const auto& value: name.Values) {
                    Cerr << value << " <-> " << attributeValue << Endl;
                    if (value == attributeValue) {
                        attributeMatched = true;
                        break;
                    }
                }
                if (!attributeMatched) {
                    for (const auto& suffix: name.Suffixes) {
                        Cerr << suffix << " <-> " << attributeValue << Endl;
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
    }

    return false;
}

TDynamicNodeAuthorizationParams::TRelativeDistinguishedName::TRelativeDistinguishedName(const TString& Attribute)
    :Attribute(Attribute)
{}

TDynamicNodeAuthorizationParams::TRelativeDistinguishedName& TDynamicNodeAuthorizationParams::TRelativeDistinguishedName::AddValue(const TString& val)
{
    Values.push_back(val);
    return *this;
}

TDynamicNodeAuthorizationParams::TRelativeDistinguishedName& TDynamicNodeAuthorizationParams::TRelativeDistinguishedName::AddSuffix(const TString& suffix)
{
    Suffixes.push_back(suffix);
    return *this;
}

}  //namespace NKikimr {
