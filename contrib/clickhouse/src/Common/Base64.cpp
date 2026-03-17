#include <Common/Base64.h>

#include <DBPoco/Base64Decoder.h>
#include <DBPoco/Base64Encoder.h>
#include <DBPoco/MemoryStream.h>
#include <DBPoco/StreamCopier.h>

#include <sstream>

namespace DB
{

std::string base64Encode(const std::string & decoded, bool url_encoding)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    DBPoco::Base64Encoder encoder(ostr, url_encoding ? DBPoco::BASE64_URL_ENCODING : 0);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

std::string base64Decode(const std::string & encoded, bool url_encoding)
{
    std::string decoded;
    DBPoco::MemoryInputStream istr(encoded.data(), encoded.size());
    DBPoco::Base64Decoder decoder(istr, url_encoding ? DBPoco::BASE64_URL_ENCODING : 0);
    DBPoco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

}
