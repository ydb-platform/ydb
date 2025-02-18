#include "encryption.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(EncryptedFileSerializerTest) {
    Y_UNIT_TEST(SerializeWholeFileAtATime) {
        TEncryptionKey key("cool random key!");
        TEncryptionIV iv = TEncryptionIV::Generate();
        TEncryptedFileSerializer::EncryptFile("aes-128_gcm", key, iv, "short data file", true);
    }
}

} // namespace NKikimr::NBackup
