#include <Common/Base64.h>

#include <CHDBPoco/Base64Decoder.h>
#include <CHDBPoco/Base64Encoder.h>
#include <CHDBPoco/MemoryStream.h>
#include <CHDBPoco/StreamCopier.h>

#include <sstream>

namespace DB_CHDB
{

std::string base64Encode(const std::string & decoded, bool url_encoding)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    CHDBPoco::Base64Encoder encoder(ostr, url_encoding ? CHDBPoco::BASE64_URL_ENCODING : 0);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

std::string base64Decode(const std::string & encoded, bool url_encoding)
{
    std::string decoded;
    CHDBPoco::MemoryInputStream istr(encoded.data(), encoded.size());
    CHDBPoco::Base64Decoder decoder(istr, url_encoding ? CHDBPoco::BASE64_URL_ENCODING : 0);
    CHDBPoco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

}
