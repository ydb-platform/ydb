#include <util/stream/output.h>
#include <util/stream/format.h>
#include "ber.h"

namespace LdapMock {

TString EncodeInt(int num) {
    char octets[4];
    octets[0] = static_cast<char>((num >> 24) & 0xFF);
    octets[1] = static_cast<char>((num >> 16) & 0xFF);
    octets[2] = static_cast<char>((num >> 8) & 0xFF);
    octets[3] = static_cast<char>(num & 0xFF);

    char octetNum = 4;
    if (octets[0] == 0 || octets[0] == 1) {
        octetNum--;
        if (octets[1] == 0 || octets[1] == 1) {
            octetNum--;
            if (octets[2] == 0 || octets[2] == 1) {
                octetNum--;
            }
        }
    }
    TString result;
    result += static_cast<char>(0x02);
    result += octetNum;

    for (int i = 4 - octetNum; i < 4; i++) {
        result += octets[i];
    }
    return result;
}

TString EncodeEnum(int num) {
    TString result = EncodeInt(num);
    result[0] = static_cast<char>(0xA);
    return result;
}

TString EncodeSize(size_t size) {
    if (size < 0x80) {
        return TString(1, static_cast<char>(size));
    }
    char octets[4];
    octets[0] = static_cast<char>((size >> 24) & 0xFF);
    octets[1] = static_cast<char>((size >> 16) & 0xFF);
    octets[2] = static_cast<char>((size >> 8) & 0xFF);
    octets[3] = static_cast<char>(size & 0xFF);

    char octetNum = 4;
    if (octets[0] == 0 || octets[0] == 1) {
        octetNum--;
        if (octets[1] == 0 || octets[1] == 1) {
            octetNum--;
            if (octets[2] == 0 || octets[2] == 1) {
                octetNum--;
            }
        }
    }
    TString result;
    result += static_cast<char>(0x80 + octetNum);

    for (int i = 4 - octetNum; i < 4; i++) {
        result += octets[i];
    }
    return result;
}

TString EncodeString(const TString& str) {
    TString result;
    result += static_cast<char>(0x04);
    result += EncodeSize(str.size());
    result += str;
    return result;
}

TString EncodeSequence(const TString& sequenceBody) {
    TString result;
    result += static_cast<char>(0x30);
    result += EncodeSize(sequenceBody.size());
    result += sequenceBody;
    return result;
}

}
