#include "keys.h" 
 
#include "rw.h" 
 
#include <library/cpp/openssl/init/init.h>

#include <contrib/libs/openssl/include/openssl/evp.h> 
 
#include <util/generic/strbuf.h> 
#include <util/generic/yexception.h>
 
namespace {
    struct TInit {
        TInit() {
            InitOpenSSL();
        }
    } INIT;
}

namespace NTvmAuth {
    namespace NRw { 
        namespace NPrivate {
            void TRwDestroyer::Destroy(TRwInternal* o) {
                RwFree(o);
            }

            class TArrayDestroyer {
            public:
                static void Destroy(unsigned char* o) {
                    free(o);
                }
            };
        }

        static TString SerializeRW(TRwKey* rw, int (*func)(const TRwKey*, unsigned char**)) {
            unsigned char* buf = nullptr; 
            int size = func(rw, &buf); 
            THolder<unsigned char, NPrivate::TArrayDestroyer> guard(buf);
            return TString((char*)buf, size); 
        } 
 
        TKeyPair GenKeyPair(size_t size) {
            TRw rw(RwNew());
            RwGenerateKey(rw.Get(), size);
 
            TRw skey(RwPrivateKeyDup(rw.Get()));
            TRw vkey(RwPublicKeyDup(rw.Get()));
 
            TKeyPair res;
            res.Private = SerializeRW(skey.Get(), &i2d_RWPrivateKey);
            res.Public = SerializeRW(vkey.Get(), &i2d_RWPublicKey);
 
            TRwPrivateKey prKey(res.Private, 0);
            TRwPublicKey pubKey(res.Public);

            const TStringBuf msg = "Test test test test test";

            Y_ENSURE(pubKey.CheckSign(msg, prKey.SignTicket(msg)), "Failed to gen keys");

            return res;
        } 
 
        TRwPrivateKey::TRwPrivateKey(TStringBuf body, TKeyId id) 
            : Id_(id)
            , Rw_(Deserialize(body))
            , SignLen_(RwModSize(Rw_.Get()))
        { 
            Y_ENSURE(SignLen_ > 0, "Private key has bad len: " << SignLen_);
        } 
 
        TKeyId TRwPrivateKey::GetId() const { 
            return Id_;
        } 
 
        TString TRwPrivateKey::SignTicket(TStringBuf ticket) const { 
            TString res(SignLen_, 0x00);

            int len = RwPssrSignMsg(ticket.size(),
                                    (const unsigned char*)ticket.data(),
                                    (unsigned char*)res.data(),
                                    Rw_.Get(),
                                    (EVP_MD*)EVP_sha256());
 
            Y_ENSURE(len > 0 && len <= SignLen_, "Signing failed. len: " << len);
 
            res.resize(len);
            return res;
        } 
 
        TRw TRwPrivateKey::Deserialize(TStringBuf key) { 
            TRwKey* rw = nullptr;
            auto data = reinterpret_cast<const unsigned char*>(key.data()); 
            if (!d2i_RWPrivateKey(&rw, &data, key.size())) { 
                ythrow yexception() << "Private key is malformed";
            } 
            return TRw(rw);
        } 
 
        TRwPublicKey::TRwPublicKey(TStringBuf body) 
            : Rw_(Deserialize(body))
        { 
        } 
 
        bool TRwPublicKey::CheckSign(TStringBuf ticket, TStringBuf sign) const { 
            int result = RwPssrVerifyMsg(ticket.size(),
                                         (const unsigned char*)ticket.data(),
                                         (unsigned char*)sign.data(),
                                         sign.size(),
                                         Rw_.Get(),
                                         (EVP_MD*)EVP_sha256());
 
            Y_ENSURE(result >= 0, "Failed to check sign: " << result);
            return result; 
        } 
 
        TRw TRwPublicKey::Deserialize(TStringBuf key) { 
            TRwKey* rw = nullptr;
            auto data = reinterpret_cast<const unsigned char*>(key.data()); 
            auto status = d2i_RWPublicKey(&rw, &data, key.size());

            TRw res(rw);
            Y_ENSURE(status, "Public key is malformed: " << key);
            return res;
        } 

        TSecureHeap::TSecureHeap(size_t totalSize, int minChunkSize) {
            CRYPTO_secure_malloc_init(totalSize, minChunkSize);
        }

        TSecureHeap::~TSecureHeap() {
            CRYPTO_secure_malloc_done();
        }

        void TSecureHeap::Init(size_t totalSize, int minChunkSize) {
            Singleton<TSecureHeap>(totalSize, minChunkSize);
        }
    } 
} 
