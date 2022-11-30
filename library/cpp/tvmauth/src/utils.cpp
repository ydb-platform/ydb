#include "utils.h"

#include "parser.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

#include <array>

namespace {
    constexpr const unsigned char b64_encode[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    constexpr std::array<unsigned char, 256> B64Init() {
        std::array<unsigned char, 256> buf{};
        for (auto& i : buf)
            i = 0xff;

        for (int i = 0; i < 64; ++i)
            buf[b64_encode[i]] = i;

        return buf;
    }
    constexpr std::array<unsigned char, 256> b64_decode = B64Init();
}

namespace NTvmAuth::NUtils {
    TString Bin2base64url(TStringBuf buf) {
        if (!buf) {
            return TString();
        }

        TString res;
        res.resize(((buf.size() + 2) / 3) << 2, 0);

        const unsigned char* pB = (const unsigned char*)buf.data();
        const unsigned char* pE = (const unsigned char*)buf.data() + buf.size();
        unsigned char* p = (unsigned char*)res.data();
        for (; pB + 2 < pE; pB += 3) {
            const unsigned char a = *pB;
            *p++ = b64_encode[(a >> 2) & 0x3F];
            const unsigned char b = *(pB + 1);
            *p++ = b64_encode[((a & 0x3) << 4) | ((b & 0xF0) >> 4)];
            const unsigned char c = *(pB + 2);
            *p++ = b64_encode[((b & 0xF) << 2) | ((c & 0xC0) >> 6)];
            *p++ = b64_encode[c & 0x3F];
        }

        if (pB < pE) {
            const unsigned char a = *pB;
            *p++ = b64_encode[(a >> 2) & 0x3F];

            if (pB == (pE - 1)) {
                *p++ = b64_encode[((a & 0x3) << 4)];
            } else {
                const unsigned char b = *(pB + 1);
                *p++ = b64_encode[((a & 0x3) << 4) |
                                  ((int)(b & 0xF0) >> 4)];
                *p++ = b64_encode[((b & 0xF) << 2)];
            }
        }

        res.resize(p - (unsigned char*)res.data());
        return res;
    }

    TString Base64url2bin(TStringBuf buf) {
        const unsigned char* bufin = (const unsigned char*)buf.data();
        if (!buf || b64_decode[*bufin] > 63) {
            return TString();
        }
        const unsigned char* bufend = (const unsigned char*)buf.data() + buf.size();
        while (++bufin < bufend && b64_decode[*bufin] < 64)
            ;
        int nprbytes = (bufin - (const unsigned char*)buf.data());
        int nbytesdecoded = ((nprbytes + 3) / 4) * 3;

        if (nprbytes < static_cast<int>(buf.size())) {
            int left = buf.size() - nprbytes;
            while (left--) {
                if (*(bufin++) != '=')
                    return TString();
            }
        }

        TString res;
        res.resize(nbytesdecoded);

        unsigned char* bufout = (unsigned char*)res.data();
        bufin = (const unsigned char*)buf.data();

        while (nprbytes > 4) {
            unsigned char a = b64_decode[*bufin];
            unsigned char b = b64_decode[bufin[1]];
            *(bufout++) = (unsigned char)(a << 2 | b >> 4);
            unsigned char c = b64_decode[bufin[2]];
            *(bufout++) = (unsigned char)(b << 4 | c >> 2);
            unsigned char d = b64_decode[bufin[3]];
            *(bufout++) = (unsigned char)(c << 6 | d);
            bufin += 4;
            nprbytes -= 4;
        }

        if (nprbytes == 1) {
            return {}; // Impossible
        }
        if (nprbytes > 1) {
            *(bufout++) = (unsigned char)(b64_decode[*bufin] << 2 | b64_decode[bufin[1]] >> 4);
        }
        if (nprbytes > 2) {
            *(bufout++) = (unsigned char)(b64_decode[bufin[1]] << 4 | b64_decode[bufin[2]] >> 2);
        }
        if (nprbytes > 3) {
            *(bufout++) = (unsigned char)(b64_decode[bufin[2]] << 6 | b64_decode[bufin[3]]);
        }

        int diff = (4 - nprbytes) & 3;
        if (diff) {
            nbytesdecoded -= (4 - nprbytes) & 3;
            res.resize(nbytesdecoded);
        }

        return res;
    }

    TString SignCgiParamsForTvm(TStringBuf secret, TStringBuf ts, TStringBuf dstTvmId, TStringBuf scopes) {
        TString data;
        data.reserve(ts.size() + dstTvmId.size() + scopes.size() + 3);
        const char DELIM = '|';
        data.append(ts).push_back(DELIM);
        data.append(dstTvmId).push_back(DELIM);
        data.append(scopes).push_back(DELIM);

        TString value(EVP_MAX_MD_SIZE, 0);
        unsigned macLen = 0;

        if (!::HMAC(EVP_sha256(), secret.data(), secret.size(), (unsigned char*)data.data(), data.size(),
                    (unsigned char*)value.data(), &macLen))
        {
            return {};
        }

        if (macLen != EVP_MAX_MD_SIZE) {
            value.resize(macLen);
        }
        return Bin2base64url(value);
    }
}

namespace NTvmAuth::NInternal {
    TMaybe<TInstant> TCanningKnife::GetExpirationTime(TStringBuf ticket) {
        const TParserTickets::TRes res = TParserTickets::ParseV3(ticket, {}, TParserTickets::ServiceFlag());

        return res.Status == ETicketStatus::MissingKey || res.Status == ETicketStatus::Expired
                   ? TInstant::Seconds(res.Ticket.expirationtime())
                   : TMaybe<TInstant>();
    }
}
