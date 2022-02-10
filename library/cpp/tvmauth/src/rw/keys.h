#pragma once

#include <util/generic/ptr.h> 
#include <util/generic/string.h> 

#include <unordered_map>

struct TRwInternal; 

namespace NTvmAuth { 
    namespace NRw {
        namespace NPrivate { 
            class TRwDestroyer { 
            public: 
                static void Destroy(TRwInternal* o); 
            }; 
        } 

        using TRw = THolder<TRwInternal, NPrivate::TRwDestroyer>; 
        using TKeyId = ui32; 
 
        struct TKeyPair {
            TString Private; 
            TString Public; 
        };
        TKeyPair GenKeyPair(size_t size); 

        class TRwPrivateKey {
        public:
            TRwPrivateKey(TStringBuf body, TKeyId id);

            TKeyId GetId() const;
            TString SignTicket(TStringBuf ticket) const;

        private:
            static TRw Deserialize(TStringBuf key);

            TKeyId Id_; 
            TRw Rw_; 
            int SignLen_; 
        };

        class TRwPublicKey {
        public:
            TRwPublicKey(TStringBuf body);

            bool CheckSign(TStringBuf ticket, TStringBuf sign) const;

        private:
            static TRw Deserialize(TStringBuf key);

            TRw Rw_; 
        };

        using TPublicKeys = std::unordered_map<TKeyId, TRwPublicKey>;
 
        class TSecureHeap { 
        public: 
            TSecureHeap(size_t totalSize, int minChunkSize); 
            ~TSecureHeap(); 
 
            static void Init(size_t totalSize = 16 * 1024 * 1024, int minChunkSize = 16); 
        }; 
    }
}
