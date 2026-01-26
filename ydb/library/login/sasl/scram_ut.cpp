#include "scram.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>

using namespace NLogin::NSasl;

Y_UNIT_TEST_SUITE(TScramParseFirstClientMsgTests) {
    Y_UNIT_TEST(ParseValidMinimalMessage) {
        // Minimal valid message: n,,n=user,r=clientnonce
        std::string msg = "n,,n=user,r=clientnonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::NotSupported);
        UNIT_ASSERT(parsed.GS2Header.AuthzId.empty());
        UNIT_ASSERT(parsed.Mext.empty());
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "clientnonce");
        UNIT_ASSERT(parsed.Extensions.empty());
        UNIT_ASSERT_EQUAL(parsed.ClientFirstMessageBare, "n=user,r=clientnonce");
    }

    Y_UNIT_TEST(ParseMessageWithChannelBindingSupported) {
        // Client supports channel binding but thinks server does not
        std::string msg = "y,,n=user,r=nonce123";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::Supported);
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce123");
    }

    Y_UNIT_TEST(ParseMessageWithChannelBindingRequired) {
        // Client requires channel binding
        std::string msg = "p=tls-unique,,n=user,r=nonce456";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::Required);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.RequestedChannelBinding, "tls-unique");
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce456");
    }

    Y_UNIT_TEST(ParseMessageWithAuthzId) {
        // Message with authorization identity
        std::string msg = "n,a=admin,n=user,r=nonce789";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.AuthzId, "admin");
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce789");
    }

    Y_UNIT_TEST(ParseMessageWithEscapedUsername) {
        // Username with escaped characters (=2C for comma, =3D for equals)
        std::string msg = "n,,n=user=2Cname=3Dtest,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user,name=test");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce");
    }

    Y_UNIT_TEST(ParseMessageWithMext) {
        // Message with mandatory extension
        std::string msg = "n,,m=ext-value,n=user,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.Mext, "ext-value");
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce");
    }

    Y_UNIT_TEST(ParseMessageWithExtensions) {
        // Message with additional extensions
        std::string msg = "n,,n=user,r=nonce,x=ext1,y=ext2";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce");
        UNIT_ASSERT_EQUAL(parsed.Extensions.size(), 2);
        UNIT_ASSERT_EQUAL(parsed.Extensions['x'], "ext1");
        UNIT_ASSERT_EQUAL(parsed.Extensions['y'], "ext2");
    }

    Y_UNIT_TEST(ParseMessageWithComplexNonce) {
        // Nonce with various allowed characters
        std::string msg = "n,,n=user,r=abc123-XYZ.!@#$%^&*()_+=";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.Nonce, "abc123-XYZ.!@#$%^&*()_+=");
    }

    Y_UNIT_TEST(FailOnEmptyMessage) {
        std::string msg = "";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnInvalidChannelBindingFlag) {
        // Invalid channel binding flag
        std::string msg = "x,,n=user,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingCommaAfterChannelBinding) {
        std::string msg = "n,n=user,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingUsername) {
        std::string msg = "n,,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingNonce) {
        std::string msg = "n,,n=user";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnInvalidUsernameEncoding) {
        // Invalid escaping in username (=2X instead of =2C or =3D)
        // DecodeSaslname will return empty string, leading to InvalidUsernameEncoding
        std::string msg = "n,,n=user=2X,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        // DecodeSaslname returns empty string on invalid escaping,
        // which then leads to InvalidUsernameEncoding
        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidUsernameEncoding);
    }

    Y_UNIT_TEST(FailOnNonUTF8Username) {
        // Invalid UTF-8 in username (after decoding)
        std::string msg = "n,,n=user\xFF\xFE,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidUsernameEncoding);
    }

    Y_UNIT_TEST(FailOnNonUTF8AuthzId) {
        // Invalid UTF-8 in authzid
        std::string msg = "n,a=admin\xFF\xFE,n=user,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidUsernameEncoding);
    }

    Y_UNIT_TEST(FailOnNonPrintableNonce) {
        // Nonce with invalid characters (comma)
        std::string msg = "n,,n=user,r=nonce,invalid";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        // Comma in nonce will lead to incorrect parsing
        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnReservedExtensionName) {
        // Using reserved extension name
        std::string msg = "n,,n=user,r=nonce,a=reserved";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnDuplicateExtension) {
        // Duplicate extension
        std::string msg = "n,,n=user,r=nonce,x=ext1,x=ext2";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnInvalidChannelBindingName) {
        // Invalid channel binding name (contains invalid characters)
        std::string msg = "p=tls@unique,,n=user,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnMissingCommaAfterUsername) {
        std::string msg = "n,,n=user r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        // ReadAttribute reads "user r=nonce" as username value
        // DecodeSaslname encounters unescaped '=' and returns empty string
        // Empty string leads to InvalidUsernameEncoding
        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidUsernameEncoding);
    }

    Y_UNIT_TEST(FailOnEmptyAttributeValue) {
        std::string msg = "n,,n=,r=nonce";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(ParseRFC5802Example) {
        // Example from RFC 5802
        std::string msg = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        TFirstClientMsg parsed;

        auto rc = ParseFirstClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.AuthcId, "user");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "fyko+d2lbbFgONRv9qkxdawL");
        UNIT_ASSERT_EQUAL(parsed.ClientFirstMessageBare, "n=user,r=fyko+d2lbbFgONRv9qkxdawL");
    }
}

Y_UNIT_TEST_SUITE(TScramParseFinalClientMsgTests) {
    Y_UNIT_TEST(ParseValidMinimalMessage) {
        // Minimal valid final message
        // c= base64(n,,) = biws
        std::string msg = "c=biws,r=clientservernonce,p=dGVzdHByb29m";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::NotSupported);
        UNIT_ASSERT(parsed.ChannelBindingData.empty());
        UNIT_ASSERT_EQUAL(parsed.Nonce, "clientservernonce");
        UNIT_ASSERT_EQUAL(parsed.Proof, "dGVzdHByb29m");
        UNIT_ASSERT(parsed.Extensions.empty());
        UNIT_ASSERT_EQUAL(parsed.ClientFinalMessageWithoutProof, "c=biws,r=clientservernonce");
    }

    Y_UNIT_TEST(ParseMessageWithChannelBindingSupported) {
        // c= base64(y,,) = eSws
        std::string msg = "c=eSws,r=nonce123,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::Supported);
        UNIT_ASSERT(parsed.ChannelBindingData.empty());
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce123");
    }

    Y_UNIT_TEST(ParseMessageWithChannelBindingData) {
        // c= base64(p=tls-unique,,<channel-binding-data>)
        std::string cbData = "p=tls-unique,,channeldata";
        std::string encodedCb = Base64Encode(cbData);
        std::string msg = "c=" + encodedCb + ",r=nonce456,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.ChannelBindingFlag, EClientChannelBindingFlag::Required);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.RequestedChannelBinding, "tls-unique");
        UNIT_ASSERT_EQUAL(parsed.ChannelBindingData, "channeldata");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce456");
    }

    Y_UNIT_TEST(ParseMessageWithAuthzId) {
        // c= base64(n,a=admin,)
        std::string cbData = "n,a=admin,";
        std::string encodedCb = Base64Encode(cbData);
        std::string msg = "c=" + encodedCb + ",r=nonce789,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.GS2Header.AuthzId, "admin");
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce789");
    }

    Y_UNIT_TEST(ParseMessageWithExtensions) {
        // Message with extensions before proof
        std::string msg = "c=biws,r=nonce,x=ext1,y=ext2,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.Nonce, "nonce");
        UNIT_ASSERT_EQUAL(parsed.Extensions.size(), 2);
        UNIT_ASSERT_EQUAL(parsed.Extensions['x'], "ext1");
        UNIT_ASSERT_EQUAL(parsed.Extensions['y'], "ext2");
        UNIT_ASSERT_EQUAL(parsed.Proof, "cHJvb2Y=");
        // ClientFinalMessageWithoutProof should include extensions
        UNIT_ASSERT_EQUAL(parsed.ClientFinalMessageWithoutProof, "c=biws,r=nonce,x=ext1,y=ext2");
    }

    Y_UNIT_TEST(ParseMessageWithComplexNonce) {
        // Nonce with various allowed characters
        std::string msg = "c=biws,r=abc123-XYZ.!@#$%^&*()_+=,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.Nonce, "abc123-XYZ.!@#$%^&*()_+=");
    }

    Y_UNIT_TEST(FailOnEmptyMessage) {
        std::string msg = "";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingChannelBinding) {
        std::string msg = "r=nonce,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnInvalidBase64ChannelBinding) {
        // Invalid base64 in channel binding
        std::string msg = "c=invalid!!!,r=nonce,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnMissingNonce) {
        std::string msg = "c=biws,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingProof) {
        std::string msg = "c=biws,r=nonce";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnInvalidBase64Proof) {
        // Invalid base64 in proof
        std::string msg = "c=biws,r=nonce,p=invalid!!!";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnReservedExtensionName) {
        // Using reserved extension name
        std::string msg = "c=biws,r=nonce,a=reserved,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnDuplicateExtension) {
        // Duplicate extension
        std::string msg = "c=biws,r=nonce,x=ext1,x=ext2,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnMissingCommaAfterChannelBinding) {
        std::string msg = "c=biws r=nonce,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        // Space in base64 value leads to InvalidEncoding
        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnMissingCommaAfterNonce) {
        std::string msg = "c=biws,r=nonce p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        // Space in base64 proof value leads to InvalidEncoding
        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnExtraDataAfterProof) {
        // Data after proof is not allowed
        std::string msg = "c=biws,r=nonce,p=cHJvb2Y=,x=extra";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnChannelBindingDataWithoutRequired) {
        // Channel binding data present but flag is not Required
        std::string cbData = "n,,extradata";
        std::string encodedCb = Base64Encode(cbData);
        std::string msg = "c=" + encodedCb + ",r=nonce,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(FailOnEmptyAttributeValue) {
        std::string msg = "c=biws,r=,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidFormat);
    }

    Y_UNIT_TEST(ParseRFC5802Example) {
        // Example from RFC 5802 (simplified)
        std::string msg = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::Success);
        UNIT_ASSERT_EQUAL(parsed.Nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
        UNIT_ASSERT_EQUAL(parsed.Proof, "v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=");
        UNIT_ASSERT_EQUAL(parsed.ClientFinalMessageWithoutProof, "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
    }

    Y_UNIT_TEST(FailOnNonUTF8Extension) {
        // Invalid UTF-8 in extension
        std::string msg = "c=biws,r=nonce,x=ext\xFF\xFE,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }

    Y_UNIT_TEST(FailOnNonPrintableNonce) {
        // Nonce with invalid characters
        std::string msg = "c=biws,r=nonce\x01invalid,p=cHJvb2Y=";
        TFinalClientMsg parsed;

        auto rc = ParseFinalClientMsg(msg, parsed);

        UNIT_ASSERT_EQUAL(rc, EParseMsgReturnCodes::InvalidEncoding);
    }
}

Y_UNIT_TEST_SUITE(TScramGenerateSecretsTests) {
    Y_UNIT_TEST(GenerateSecretsWithSHA256) {
        // Test with SCRAM-SHA-256
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "pencil";
        std::string salt = "QSXCR+Q6sek8bf92";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!storedKey.empty());
        UNIT_ASSERT(!serverKey.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-256 produces 32-byte hashes
        UNIT_ASSERT_EQUAL(storedKey.size(), 32);
        UNIT_ASSERT_EQUAL(serverKey.size(), 32);
    }

    Y_UNIT_TEST(GenerateSecretsWithSHA1) {
        // Test with SCRAM-SHA-1
        std::string hashType = "SCRAM-SHA-1";
        std::string password = "password";
        std::string salt = "salt";
        ui32 iterationsCount = 1000;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!storedKey.empty());
        UNIT_ASSERT(!serverKey.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-1 produces 20-byte hashes
        UNIT_ASSERT_EQUAL(storedKey.size(), 20);
        UNIT_ASSERT_EQUAL(serverKey.size(), 20);
    }

    Y_UNIT_TEST(GenerateSecretsWithDifferentIterations) {
        // Test with different iteration counts
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string salt = "testsalt";
        std::string storedKey1, serverKey1, errorText1;
        std::string storedKey2, serverKey2, errorText2;

        bool result1 = GenerateScramSecrets(hashType, password, salt, 1000,
                                           storedKey1, serverKey1, errorText1);
        bool result2 = GenerateScramSecrets(hashType, password, salt, 5000,
                                           storedKey2, serverKey2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different iteration counts should produce different keys
        UNIT_ASSERT(storedKey1 != storedKey2);
        UNIT_ASSERT(serverKey1 != serverKey2);
    }

    Y_UNIT_TEST(GenerateSecretsWithDifferentPasswords) {
        // Test with different passwords
        std::string hashType = "SCRAM-SHA-256";
        std::string salt = "commonsalt";
        ui32 iterationsCount = 4096;
        std::string storedKey1, serverKey1, errorText1;
        std::string storedKey2, serverKey2, errorText2;

        bool result1 = GenerateScramSecrets(hashType, "password1", salt, iterationsCount,
                                           storedKey1, serverKey1, errorText1);
        bool result2 = GenerateScramSecrets(hashType, "password2", salt, iterationsCount,
                                           storedKey2, serverKey2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different passwords should produce different keys
        UNIT_ASSERT(storedKey1 != storedKey2);
        UNIT_ASSERT(serverKey1 != serverKey2);
    }

    Y_UNIT_TEST(GenerateSecretsWithDifferentSalts) {
        // Test with different salts
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "commonpass";
        ui32 iterationsCount = 4096;
        std::string storedKey1, serverKey1, errorText1;
        std::string storedKey2, serverKey2, errorText2;

        bool result1 = GenerateScramSecrets(hashType, password, "salt1", iterationsCount,
                                           storedKey1, serverKey1, errorText1);
        bool result2 = GenerateScramSecrets(hashType, password, "salt2", iterationsCount,
                                           storedKey2, serverKey2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different salts should produce different keys
        UNIT_ASSERT(storedKey1 != storedKey2);
        UNIT_ASSERT(serverKey1 != serverKey2);
    }

    Y_UNIT_TEST(GenerateSecretsStoredAndServerKeysDifferent) {
        // Verify that StoredKey and ServerKey are different
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string salt = "testsalt";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(storedKey != serverKey);
    }

    Y_UNIT_TEST(FailOnUnsupportedHashType) {
        // Test with unsupported hash type
        std::string hashType = "SCRAM-INVALID";
        std::string password = "password";
        std::string salt = "salt";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(!result);
        UNIT_ASSERT(!errorText.empty());
        UNIT_ASSERT(errorText.find("Unsupported hash type") != std::string::npos);
    }

    Y_UNIT_TEST(FailOnInvalidHashTypeFormat) {
        // Test with invalid hash type format
        std::string hashType = "SHA-256";  // Missing "SCRAM-" prefix
        std::string password = "password";
        std::string salt = "salt";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(!result);
        UNIT_ASSERT(!errorText.empty());
    }

    Y_UNIT_TEST(FailOnEmptyPassword) {
        // Test with empty password - should fail per RFC 5802
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "";
        std::string salt = "salt";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(!result);
        UNIT_ASSERT(!errorText.empty());
        UNIT_ASSERT(errorText.find("Unsupported password format") != std::string::npos);
    }

    Y_UNIT_TEST(GenerateSecretsWithBinarySalt) {
        // Test with binary salt (non-printable characters)
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "password";
        std::string salt = std::string("\x00\x01\x02\x03\xFF\xFE", 6);
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!storedKey.empty());
        UNIT_ASSERT(!serverKey.empty());
    }

    Y_UNIT_TEST(GenerateSecretsWithUnicodePassword) {
        // Test with Unicode password
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "пароль";  // Russian word for "password"
        std::string salt = "salt";
        ui32 iterationsCount = 4096;
        std::string storedKey;
        std::string serverKey;
        std::string errorText;

        bool result = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                          storedKey, serverKey, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!storedKey.empty());
        UNIT_ASSERT(!serverKey.empty());
    }

    Y_UNIT_TEST(GenerateSecretsExactValues) {
        // Test with exact values for different passwords
        std::string hashType = "SCRAM-SHA-256";
        std::string saltBase64 = "s0QSrrFVkMTh3k2TTk860A==";
        std::string salt = Base64Decode(saltBase64);
        ui32 iterationsCount = 4096;

        // Test password: "password"
        {
            std::string storedKey, serverKey, errorText;
            bool result = GenerateScramSecrets(hashType, "password", salt, iterationsCount,
                                              storedKey, serverKey, errorText);
            UNIT_ASSERT(result);
            std::string expectedStoredKey = Base64Decode("iQJajN1d48LB02V6Z4QhzqLocK9Glume1LMQ00yCq74=");
            std::string expectedServerKey = Base64Decode("Jtls9JP+UJ+Jkd9wopls6XVZC6vWgMShkQTYeR9xzww=");
            UNIT_ASSERT_EQUAL(storedKey, expectedStoredKey);
            UNIT_ASSERT_EQUAL(serverKey, expectedServerKey);
        }

        // Test password: "password1"
        {
            std::string storedKey, serverKey, errorText;
            bool result = GenerateScramSecrets(hashType, "password1", salt, iterationsCount,
                                              storedKey, serverKey, errorText);
            UNIT_ASSERT(result);
            std::string expectedStoredKey = Base64Decode("LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=");
            std::string expectedServerKey = Base64Decode("eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc=");
            UNIT_ASSERT_EQUAL(storedKey, expectedStoredKey);
            UNIT_ASSERT_EQUAL(serverKey, expectedServerKey);
        }

        // Test password: "password2"
        {
            std::string storedKey, serverKey, errorText;
            bool result = GenerateScramSecrets(hashType, "password2", salt, iterationsCount,
                                              storedKey, serverKey, errorText);
            UNIT_ASSERT(result);
            std::string expectedStoredKey = Base64Decode("VFQm/CQ5yn8sQqbhybLGd9vvR1fYNfXubh1hDHJPnHo=");
            std::string expectedServerKey = Base64Decode("3b/5VlB7KIia72vvoBOIdOvgwctzdJR7iKaeQKxCzmU=");
            UNIT_ASSERT_EQUAL(storedKey, expectedStoredKey);
            UNIT_ASSERT_EQUAL(serverKey, expectedServerKey);
        }

        // Test password: "flower"
        {
            std::string storedKey, serverKey, errorText;
            bool result = GenerateScramSecrets(hashType, "flower", salt, iterationsCount,
                                              storedKey, serverKey, errorText);
            UNIT_ASSERT(result);
            std::string expectedStoredKey = Base64Decode("dlrMXRd689TE9zB6WnHY+tsM7iPE4k83aKAYb1uyT6A=");
            std::string expectedServerKey = Base64Decode("g7bjRnKbDOdaJ0u4vzgMg5xi6aFeC22pCNQ/3VxncWQ=");
            UNIT_ASSERT_EQUAL(storedKey, expectedStoredKey);
            UNIT_ASSERT_EQUAL(serverKey, expectedServerKey);
        }

        // Test password: "qwerty"
        {
            std::string storedKey, serverKey, errorText;
            bool result = GenerateScramSecrets(hashType, "qwerty", salt, iterationsCount,
                                              storedKey, serverKey, errorText);
            UNIT_ASSERT(result);
            std::string expectedStoredKey = Base64Decode("BcGgBQdwufS6QnbHC2Cbd6TWJjCVa+L+eXdBW6spmuE=");
            std::string expectedServerKey = Base64Decode("QP6ts109Iqje8uOqQMqs6qKIIgSwUaqBCY/kZ9hD4F8=");
            UNIT_ASSERT_EQUAL(storedKey, expectedStoredKey);
            UNIT_ASSERT_EQUAL(serverKey, expectedServerKey);
        }
    }
}

Y_UNIT_TEST_SUITE(TScramComputeServerKeyTests) {
    Y_UNIT_TEST(ComputeServerKeyMatchesGenerateSecrets) {
        // Verify that ComputeServerKey produces the same result as GenerateScramSecrets
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string salt = "testsalt";
        ui32 iterationsCount = 4096;

        std::string storedKey, serverKeyFromGenerate, errorText1;
        bool result1 = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                           storedKey, serverKeyFromGenerate, errorText1);

        std::string serverKeyFromCompute, errorText2;
        bool result2 = ComputeServerKey(hashType, password, salt, iterationsCount,
                                       serverKeyFromCompute, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        UNIT_ASSERT_EQUAL(serverKeyFromGenerate, serverKeyFromCompute);
    }
}

Y_UNIT_TEST_SUITE(TScramComputeServerSignatureTests) {
    Y_UNIT_TEST(ComputeServerSignatureWithSHA256) {
        // Test with SCRAM-SHA-256
        std::string hashType = "SCRAM-SHA-256";
        std::string serverKey = std::string(32, 'k');  // 32-byte key for SHA-256
        std::string authMessage = "client-first,server-first,client-final";
        std::string serverSignature;
        std::string errorText;

        bool result = ComputeServerSignature(hashType, serverKey, authMessage,
                                            serverSignature, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!serverSignature.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-256 produces 32-byte signatures
        UNIT_ASSERT_EQUAL(serverSignature.size(), 32);
    }

    Y_UNIT_TEST(ComputeServerSignatureWithSHA1) {
        // Test with SCRAM-SHA-1
        std::string hashType = "SCRAM-SHA-1";
        std::string serverKey = std::string(20, 'k');  // 20-byte key for SHA-1
        std::string authMessage = "client-first,server-first,client-final";
        std::string serverSignature;
        std::string errorText;

        bool result = ComputeServerSignature(hashType, serverKey, authMessage,
                                            serverSignature, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!serverSignature.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-1 produces 20-byte signatures
        UNIT_ASSERT_EQUAL(serverSignature.size(), 20);
    }

    Y_UNIT_TEST(ComputeServerSignatureWithDifferentAuthMessages) {
        // Test with different auth messages
        std::string hashType = "SCRAM-SHA-256";
        std::string serverKey = std::string(32, 'k');
        std::string authMessage1 = "auth-message-1";
        std::string authMessage2 = "auth-message-2";
        std::string serverSignature1, errorText1;
        std::string serverSignature2, errorText2;

        bool result1 = ComputeServerSignature(hashType, serverKey, authMessage1,
                                             serverSignature1, errorText1);
        bool result2 = ComputeServerSignature(hashType, serverKey, authMessage2,
                                             serverSignature2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different auth messages should produce different signatures
        UNIT_ASSERT(serverSignature1 != serverSignature2);
    }

    Y_UNIT_TEST(ComputeServerSignatureWithLongAuthMessage) {
        // Test with long auth message
        std::string hashType = "SCRAM-SHA-256";
        std::string serverKey = std::string(32, 'k');
        std::string authMessage = std::string(10000, 'x');
        std::string serverSignature;
        std::string errorText;

        bool result = ComputeServerSignature(hashType, serverKey, authMessage,
                                            serverSignature, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!serverSignature.empty());
    }

    Y_UNIT_TEST(FailOnUnsupportedHashType) {
        // Test with unsupported hash type
        std::string hashType = "SCRAM-INVALID";
        std::string serverKey = std::string(16, 'k');
        std::string authMessage = "test-message";
        std::string serverSignature;
        std::string errorText;

        bool result = ComputeServerSignature(hashType, serverKey, authMessage,
                                            serverSignature, errorText);

        UNIT_ASSERT(!result);
        UNIT_ASSERT(!errorText.empty());
        UNIT_ASSERT(errorText.find("Unsupported hash type") != std::string::npos);
    }

    Y_UNIT_TEST(ComputeServerSignatureWithRFC5802Example) {
        // Test with values similar to RFC 5802 example
        std::string hashType = "SCRAM-SHA-1";
        std::string password = "pencil";
        std::string salt = "QSXCR+Q6sek8bf92";
        ui32 iterationsCount = 4096;
        std::string serverKey, errorText1;

        bool keyResult = ComputeServerKey(hashType, password, salt, iterationsCount,
                                         serverKey, errorText1);
        UNIT_ASSERT(keyResult);

        std::string authMessage = "n=user,r=fyko+d2lbbFgONRv9qkxdawL,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096,c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j";
        std::string serverSignature, errorText2;

        bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                               serverSignature, errorText2);

        UNIT_ASSERT(sigResult);
        UNIT_ASSERT(!serverSignature.empty());
        UNIT_ASSERT_EQUAL(serverSignature.size(), 20);
    }

    Y_UNIT_TEST(ComputeServerSignatureWithUnicodeAuthMessage) {
        // Test with Unicode characters in auth message
        std::string hashType = "SCRAM-SHA-256";
        std::string serverKey = std::string(32, 'k');
        std::string authMessage = "пользователь,сообщение";
        std::string serverSignature;
        std::string errorText;

        bool result = ComputeServerSignature(hashType, serverKey, authMessage,
                                            serverSignature, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!serverSignature.empty());
    }

    Y_UNIT_TEST(ComputeServerSignatureExactValues) {
        // Test with exact values for different passwords
        std::string hashType = "SCRAM-SHA-256";
        std::string saltBase64 = "s0QSrrFVkMTh3k2TTk860A==";
        std::string salt = Base64Decode(saltBase64);
        ui32 iterationsCount = 4096;
        std::string authMessage = "n=user,r=clientnonce,r=clientservernonce,s=s0QSrrFVkMTh3k2TTk860A==,i=4096,c=biws,r=clientservernonce";

        // Test password: "password"
        {
            std::string serverKey, errorText1;
            bool keyResult = ComputeServerKey(hashType, "password", salt, iterationsCount,
                                             serverKey, errorText1);
            UNIT_ASSERT(keyResult);

            std::string serverSignature, errorText2;
            bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                                   serverSignature, errorText2);
            UNIT_ASSERT(sigResult);
            std::string expectedSignature = Base64Decode("s3GAQlyS26TTt7ciWfskjqwnOhxhd2WXPxe9Nk8Ik8o=");
            UNIT_ASSERT_EQUAL(serverSignature, expectedSignature);
        }

        // Test password: "password1"
        {
            std::string serverKey, errorText1;
            bool keyResult = ComputeServerKey(hashType, "password1", salt, iterationsCount,
                                             serverKey, errorText1);
            UNIT_ASSERT(keyResult);

            std::string serverSignature, errorText2;
            bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                                   serverSignature, errorText2);
            UNIT_ASSERT(sigResult);
            std::string expectedSignature = Base64Decode("RBEDP7XfP9zTpxx+++HZSiw7kB7MDtfZ5mlBcMSxRQY=");
            UNIT_ASSERT_EQUAL(serverSignature, expectedSignature);
        }

        // Test password: "password2"
        {
            std::string serverKey, errorText1;
            bool keyResult = ComputeServerKey(hashType, "password2", salt, iterationsCount,
                                             serverKey, errorText1);
            UNIT_ASSERT(keyResult);

            std::string serverSignature, errorText2;
            bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                                   serverSignature, errorText2);
            UNIT_ASSERT(sigResult);
            std::string expectedSignature = Base64Decode("EYCvKFjKpBAUDCUdoDwrdjpXVqGBJjEpreLzBduflE0=");
            UNIT_ASSERT_EQUAL(serverSignature, expectedSignature);
        }

        // Test password: "flower"
        {
            std::string serverKey, errorText1;
            bool keyResult = ComputeServerKey(hashType, "flower", salt, iterationsCount,
                                             serverKey, errorText1);
            UNIT_ASSERT(keyResult);

            std::string serverSignature, errorText2;
            bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                                   serverSignature, errorText2);
            UNIT_ASSERT(sigResult);
            std::string expectedSignature = Base64Decode("gcNz0LwjjlrFjjXHsKT5VaJ/8NtOcTBvm4cN3235qdI=");
            UNIT_ASSERT_EQUAL(serverSignature, expectedSignature);
        }

        // Test password: "qwerty"
        {
            std::string serverKey, errorText1;
            bool keyResult = ComputeServerKey(hashType, "qwerty", salt, iterationsCount,
                                             serverKey, errorText1);
            UNIT_ASSERT(keyResult);

            std::string serverSignature, errorText2;
            bool sigResult = ComputeServerSignature(hashType, serverKey, authMessage,
                                                   serverSignature, errorText2);
            UNIT_ASSERT(sigResult);
            std::string expectedSignature = Base64Decode("8EbLQIDBV37LbPFD8yfLfr0Rr5irxWAAgloq3Qr9/bw=");
            UNIT_ASSERT_EQUAL(serverSignature, expectedSignature);
        }
    }
}

Y_UNIT_TEST_SUITE(TScramComputeClientProofTests) {
    Y_UNIT_TEST(ComputeClientProofWithSHA256) {
        // Test with SCRAM-SHA-256
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "pencil";
        std::string saltBase64 = "QSXCR+Q6sek8bf92";
        std::string salt = Base64Decode(saltBase64);
        ui32 iterationsCount = 4096;
        std::string authMessage = "n=user,r=fyko+d2lbbFgONRv9qkxdawL,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096,c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j";
        std::string clientProof;
        std::string errorText;

        bool result = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                        clientProof, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!clientProof.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-256 produces 32-byte proofs
        UNIT_ASSERT_EQUAL(clientProof.size(), 32);
    }

    Y_UNIT_TEST(ComputeClientProofWithSHA1) {
        // Test with SCRAM-SHA-1
        std::string hashType = "SCRAM-SHA-1";
        std::string password = "password";
        std::string salt = "salt";
        ui32 iterationsCount = 1000;
        std::string authMessage = "n=user,r=nonce,r=nonce123,s=c2FsdA==,i=1000,c=biws,r=nonce123";
        std::string clientProof;
        std::string errorText;

        bool result = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                        clientProof, errorText);

        UNIT_ASSERT(result);
        UNIT_ASSERT(!clientProof.empty());
        UNIT_ASSERT(errorText.empty());
        // SHA-1 produces 20-byte proofs
        UNIT_ASSERT_EQUAL(clientProof.size(), 20);
    }

    Y_UNIT_TEST(ComputeClientProofWithDifferentPasswords) {
        // Test with different passwords
        std::string hashType = "SCRAM-SHA-256";
        std::string salt = "commonsalt";
        ui32 iterationsCount = 4096;
        std::string authMessage = "test-auth-message";
        std::string clientProof1, errorText1;
        std::string clientProof2, errorText2;

        bool result1 = ComputeClientProof(hashType, "password1", salt, iterationsCount, authMessage,
                                         clientProof1, errorText1);
        bool result2 = ComputeClientProof(hashType, "password2", salt, iterationsCount, authMessage,
                                         clientProof2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different passwords should produce different proofs
        UNIT_ASSERT(clientProof1 != clientProof2);
    }

    Y_UNIT_TEST(ComputeClientProofWithDifferentAuthMessages) {
        // Test with different auth messages
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string salt = "testsalt";
        ui32 iterationsCount = 4096;
        std::string clientProof1, errorText1;
        std::string clientProof2, errorText2;

        bool result1 = ComputeClientProof(hashType, password, salt, iterationsCount, "auth-message-1",
                                         clientProof1, errorText1);
        bool result2 = ComputeClientProof(hashType, password, salt, iterationsCount, "auth-message-2",
                                         clientProof2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Different auth messages should produce different proofs
        UNIT_ASSERT(clientProof1 != clientProof2);
    }

    Y_UNIT_TEST(FailOnUnsupportedHashType) {
        // Test with unsupported hash type
        std::string hashType = "SCRAM-INVALID";
        std::string password = "password";
        std::string salt = "salt";
        ui32 iterationsCount = 4096;
        std::string authMessage = "test-message";
        std::string clientProof;
        std::string errorText;

        bool result = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                        clientProof, errorText);

        UNIT_ASSERT(!result);
        UNIT_ASSERT(!errorText.empty());
        UNIT_ASSERT(errorText.find("Unsupported hash type") != std::string::npos);
    }

    Y_UNIT_TEST(ComputeClientProofConsistency) {
        // Test that same inputs produce same output
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string salt = "testsalt";
        ui32 iterationsCount = 4096;
        std::string authMessage = "consistent-message";
        std::string clientProof1, errorText1;
        std::string clientProof2, errorText2;

        bool result1 = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                         clientProof1, errorText1);
        bool result2 = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                         clientProof2, errorText2);

        UNIT_ASSERT(result1);
        UNIT_ASSERT(result2);
        // Same inputs should produce same output
        UNIT_ASSERT_EQUAL(clientProof1, clientProof2);
    }

    Y_UNIT_TEST(ComputeClientProofVerifyRoundtrip) {
        // Test that ComputeClientProof and VerifyClientProof work together
        std::string hashType = "SCRAM-SHA-256";
        std::string password = "testpass";
        std::string saltBase64 = "s0QSrrFVkMTh3k2TTk860A==";
        std::string salt = Base64Decode(saltBase64);
        ui32 iterationsCount = 4096;
        std::string authMessage = "n=user,r=nonce,r=nonce123,s=s0QSrrFVkMTh3k2TTk860A==,i=4096,c=biws,r=nonce123";

        // Compute client proof
        std::string clientProof, errorText1;
        bool computeResult = ComputeClientProof(hashType, password, salt, iterationsCount, authMessage,
                                               clientProof, errorText1);
        UNIT_ASSERT(computeResult);

        // Generate stored key for verification
        std::string storedKey, serverKey, errorText2;
        bool generateResult = GenerateScramSecrets(hashType, password, salt, iterationsCount,
                                                   storedKey, serverKey, errorText2);
        UNIT_ASSERT(generateResult);

        // Verify the proof
        std::string errorText3;
        bool verifyResult = VerifyClientProof(hashType, clientProof, storedKey, authMessage, errorText3);
        UNIT_ASSERT(verifyResult);
    }

    Y_UNIT_TEST(ComputeClientProofExactValues) {
        // Test with exact values for different passwords
        std::string hashType = "SCRAM-SHA-256";
        std::string saltBase64 = "s0QSrrFVkMTh3k2TTk860A==";
        std::string salt = Base64Decode(saltBase64);
        ui32 iterationsCount = 4096;
        std::string authMessage = "n=user,r=clientnonce,r=clientservernonce,s=s0QSrrFVkMTh3k2TTk860A==,i=4096,c=biws,r=clientservernonce";

        // Test password: "password"
        {
            std::string clientProof, errorText;
            bool result = ComputeClientProof(hashType, "password", salt, iterationsCount, authMessage,
                                            clientProof, errorText);
            UNIT_ASSERT(result);
            std::string expectedProof = Base64Decode("kzu3fSX2fsmE4tN2uQW9sK39UpQN39mhOaHgM6+21GA=");
            UNIT_ASSERT_EQUAL(clientProof, expectedProof);
        }

        // Test password: "password1"
        {
            std::string clientProof, errorText;
            bool result = ComputeClientProof(hashType, "password1", salt, iterationsCount, authMessage,
                                            clientProof, errorText);
            UNIT_ASSERT(result);
            std::string expectedProof = Base64Decode("AJgthTHWf0jz/bMHwrWDOHk9SQPpPpvGx937mEzFnCQ=");
            UNIT_ASSERT_EQUAL(clientProof, expectedProof);
        }

        // Test password: "password2"
        {
            std::string clientProof, errorText;
            bool result = ComputeClientProof(hashType, "password2", salt, iterationsCount, authMessage,
                                            clientProof, errorText);
            UNIT_ASSERT(result);
            std::string expectedProof = Base64Decode("onCT9KAMiTb4vvJzBQM0w1nXLW3hJiZIJuc9Jz71pV8=");
            UNIT_ASSERT_EQUAL(clientProof, expectedProof);
        }

        // Test password: "flower"
        {
            std::string clientProof, errorText;
            bool result = ComputeClientProof(hashType, "flower", salt, iterationsCount, authMessage,
                                            clientProof, errorText);
            UNIT_ASSERT(result);
            std::string expectedProof = Base64Decode("31KTIm9By4mSJlun2/QeQGLfv7jehj4ldcnf0IViCe8=");
            UNIT_ASSERT_EQUAL(clientProof, expectedProof);
        }

        // Test password: "qwerty"
        {
            std::string clientProof, errorText;
            bool result = ComputeClientProof(hashType, "qwerty", salt, iterationsCount, authMessage,
                                            clientProof, errorText);
            UNIT_ASSERT(result);
            std::string expectedProof = Base64Decode("Xd4qonWM/RIaQ/4JaET2XOqNBcTqO5H3gYRsuv1gk2I=");
            UNIT_ASSERT_EQUAL(clientProof, expectedProof);
        }
    }
}
