#include "encryption.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(EncryptedFileSerializerTest) {
    Y_UNIT_TEST(SerializeWholeFileAtATime) {
        TEncryptionKey key("cool random key!");
        TEncryptionIV iv = TEncryptionIV::Generate();
        TEncryptedFileSerializer::EncryptFile("aes-128_gcm", key, iv, "short data file");
    }

    Y_UNIT_TEST(WrongParameters) {
        TEncryptionKey key16("Cool random key!");
        TEncryptionKey key32("Key is big enough to be 32 bytes");
        TEncryptionIV iv = TEncryptionIV::Generate();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("", key16, iv), yexception, "No cipher algorithm specified");

        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("EAS-256_gcm", key16, iv), yexception, "Unknown cipher algorithm: \"EAS-256_gcm\"");

        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("chacha20_poly1305", key16, iv), yexception, "Invalid key length 16. Expected: 32");

        TEncryptionIV badIv = iv;
        badIv.IV.push_back(42);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TEncryptedFileSerializer("aes_256_gcm", key32, badIv), yexception, "Invalid IV length 13. Expected: 12");

        TEncryptedFileSerializer serializer("aes-256-gcm", key32, iv);
        UNIT_ASSERT_EXCEPTION_CONTAINS(serializer.AddBlock("", false), yexception, "Empty data block");
    }
}

} // namespace NKikimr::NBackup
