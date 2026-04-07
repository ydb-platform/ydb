#include <library/cpp/openssl/io/stream.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/gtest.h>

#include <util/system/env.h>

#include <openssl/x509.h>

int GetObjectsCount(const TOpenSslX509StorePtr& store) {
    return sk_X509_OBJECT_num(X509_STORE_get0_objects(store.Get()));
}

Y_UNIT_TEST_SUITE(Builtin) {
    Y_UNIT_TEST(Builtin) {
        UNIT_ASSERT_NO_EXCEPTION(GetBuiltinOpenSslX509Store());
        UNIT_ASSERT_NO_EXCEPTION(GetBuiltinOpenSslX509Store());
        EXPECT_GT(GetObjectsCount(GetBuiltinOpenSslX509Store()), 0);
    }
    Y_UNIT_TEST(Default) {
        UNIT_ASSERT_NO_EXCEPTION(GetDefaultOpenSslX509Store());
        UNIT_ASSERT_NO_EXCEPTION(GetDefaultOpenSslX509Store());
        EXPECT_GT(GetObjectsCount(GetDefaultOpenSslX509Store()), 0);
    }
    Y_UNIT_TEST(OpensslDefaults) {
#if defined(_win_)
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_file(), "C:\\Program Files\\Common Files\\SSL/cert.pem");
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_dir(), "C:\\Program Files\\Common Files\\SSL/certs");
#else
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_file(), "/usr/local/ssl/cert.pem");
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_dir(), "/usr/local/ssl/certs");
#endif
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_file_env(), "SSL_CERT_FILE");
        UNIT_ASSERT_STRINGS_EQUAL(X509_get_default_cert_dir_env(), "SSL_CERT_DIR");
    }
    Y_UNIT_TEST(EnvironmentOptions) {
        SetEnv("SSL_CERT_FILE", "_non_existing_file_");
        SetEnv("SSL_CERT_DIR", "_non_existing_dir_");
        UNIT_ASSERT_NO_EXCEPTION(GetBuiltinOpenSslX509Store());
        UNIT_ASSERT_NO_EXCEPTION(GetDefaultOpenSslX509Store());
        EXPECT_GT(GetObjectsCount(GetBuiltinOpenSslX509Store()), 0);
        EXPECT_EQ(GetObjectsCount(GetDefaultOpenSslX509Store()), 0);
    }
}
