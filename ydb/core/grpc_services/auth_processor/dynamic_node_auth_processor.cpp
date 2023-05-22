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
    if (name_len == 0) {
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

TDynamicNodeAuthorizationParams::TDistinguishedName& TDynamicNodeAuthorizationParams::TDistinguishedName::AddRelativeDistinguishedName(TRelativeDistinguishedName name) {
    RelativeDistinguishedNames.push_back(std::move(name));
    return *this;
}

TDynamicNodeAuthorizationParams::operator bool() const {
    return bool(CertSubjectsDescriptions);
}

bool TDynamicNodeAuthorizationParams::IsSubjectDescriptionMatched(const TMap<TString, TString>& subjectDescription) const {
    for (const auto& description: CertSubjectsDescriptions) {
        bool isDescriptionMatched = false;
        for (const auto& name: description.RelativeDistinguishedNames) {
            isDescriptionMatched = false;
            auto fieldIt = subjectDescription.find(name.Attribute);
            if (fieldIt == subjectDescription.cend()) {
                break;
            }

            const auto& attributeValue = fieldIt->second;
            for (const auto& value: name.Values) {
                if (value == attributeValue) {
                    isDescriptionMatched = true;
                    break;
                }
            }
            for (const auto& suffix: name.Suffixes) {
                if (attributeValue.EndsWith(suffix)) {
                    isDescriptionMatched = true;
                    break;
                }
            }
            if (!isDescriptionMatched) {
                break;
            }
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
