#pragma once

#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <library/cpp/digest/argonish/blake2b.h>

namespace NArgonish {
    const ui32 BLAKE2B_BLOCKBYTES = 128;
    const ui32 BLAKE2B_BLOCKQWORDS = BLAKE2B_BLOCKBYTES / 8;
    const ui32 BLAKE2B_OUTBYTES = 64;
    const ui32 BLAKE2B_KEYBYTES = 64;
    const ui32 BLAKE2B_SALTBYTES = 16;
    const ui32 BLAKE2B_PERSONALBYTES = 16;

    template <NArgonish::EInstructionSet instructionSet>
    class TBlake2B final: public IBlake2Base {
    public:
        virtual ~TBlake2B<instructionSet>() {
            SecureZeroMemory_((void*)&State_, sizeof(State_));
            SecureZeroMemory_((void*)&Param_, sizeof(Param_));
        }

        EInstructionSet GetInstructionSet() {
            return instructionSet;
        }

    protected:
        struct TBlake2BState {
            ui64 H[8];
            ui64 T[2];
            ui64 F[2];
            ui64 Buf[BLAKE2B_BLOCKQWORDS];
            size_t BufLen;
            size_t OutLen;
            ui8 LastNode;
        };

        struct TBlake2BParam {
            ui8 DigestLen;                       /* 1 */
            ui8 KeyLen;                          /* 2 */
            ui8 Fanout;                          /* 3 */
            ui8 Depth;                           /* 4 */
            ui32 LeafLength;                     /* 8 */
            ui32 NodeOffset;                     /* 12 */
            ui32 XofLength;                      /* 16 */
            ui8 NodeDepth;                       /* 17 */
            ui8 InnerLength;                     /* 18 */
            ui8 Reserved[14];                    /* 32 */
            ui8 Salt[BLAKE2B_SALTBYTES];         /* 48 */
            ui8 Personal[BLAKE2B_PERSONALBYTES]; /* 64 */
        } Y_PACKED;

        TBlake2BState State_;
        TBlake2BParam Param_;

    protected:
        void Compress_(const ui64 block[BLAKE2B_BLOCKQWORDS]);
        void InitialXor_(ui8* h, const ui8* p);
        void* GetIV_() const;

        static void SecureZeroMemory_(void* src, size_t len) {
            static void* (*const volatile memsetv)(void*, int, size_t) = &memset;
            memsetv(src, 0, len);
        }

        void InitParam_() {
            memset(&State_, 0, sizeof(State_));
            InitialXor_((ui8*)(State_.H), (const ui8*)(&Param_));
            State_.OutLen = Param_.DigestLen;
        }

        void IncrementCounter_(const ui64 inc) {
            State_.T[0] += inc;
            State_.T[1] += (State_.T[0] < inc) ? 1 : 0;
        }

        bool IsLastBlock_() {
            return State_.F[0] != 0;
        }

        void SetLastNode_() {
            State_.F[1] = (ui64)-1;
        }

        void SetLastBlock_() {
            if (State_.LastNode)
                SetLastNode_();

            State_.F[0] = (ui64)-1;
        }

    public:
        TBlake2B(size_t outlen) {
            /*
             * Note that outlen check was moved to proxy class
             */

            Param_.DigestLen = (ui8)outlen;
            Param_.KeyLen = 0;
            Param_.Fanout = 1;
            Param_.Depth = 1;
            Param_.LeafLength = 0;
            Param_.NodeOffset = 0;
            Param_.XofLength = 0;
            Param_.NodeDepth = 0;
            Param_.InnerLength = 0;

            memset(Param_.Reserved, 0, sizeof(Param_.Reserved));
            memset(Param_.Salt, 0, sizeof(Param_.Salt));
            memset(Param_.Personal, 0, sizeof(Param_.Personal));

            InitParam_();
        }

        TBlake2B(size_t outlen, const void* key, size_t keylen) {
            /**
             * Note that key and outlen checks were moved to proxy classes
             */
            Param_.DigestLen = (ui8)outlen;
            Param_.KeyLen = (ui8)keylen;
            Param_.Fanout = 1;
            Param_.Depth = 1;

            Param_.LeafLength = 0;
            Param_.NodeOffset = 0;
            Param_.XofLength = 0;
            Param_.NodeDepth = 0;
            Param_.InnerLength = 0;

            memset(Param_.Reserved, 0, sizeof(Param_.Reserved));
            memset(Param_.Salt, 0, sizeof(Param_.Salt));
            memset(Param_.Personal, 0, sizeof(Param_.Personal));

            InitParam_();
            ui8 block[BLAKE2B_BLOCKBYTES] = {0};
            memcpy(block, key, keylen);
            Update(block, BLAKE2B_BLOCKBYTES);
            SecureZeroMemory_(block, BLAKE2B_BLOCKBYTES);
        }

        void Update(ui32 in) override {
            Update((const void*)&in, sizeof(in));
        }

        void Update(const void* pin, size_t inlen) override {
            const ui8* in = (ui8*)pin;
            if (inlen > 0) {
                size_t left = State_.BufLen;
                size_t fill = BLAKE2B_BLOCKBYTES - left;
                if (inlen > fill) {
                    State_.BufLen = 0;
                    memcpy((ui8*)State_.Buf + left, in, fill); /* Fill buffer */
                    IncrementCounter_(BLAKE2B_BLOCKBYTES);
                    Compress_(State_.Buf); /* Compress */
                    in += fill;
                    inlen -= fill;
                    while (inlen > BLAKE2B_BLOCKBYTES) {
                        /* to fix ubsan's unaligned report */
                        ui64 tmpbuf[BLAKE2B_BLOCKQWORDS];
                        memcpy(tmpbuf, in, BLAKE2B_BLOCKBYTES);

                        IncrementCounter_(BLAKE2B_BLOCKBYTES);
                        Compress_(tmpbuf);
                        in += BLAKE2B_BLOCKBYTES;
                        inlen -= BLAKE2B_BLOCKBYTES;
                    }
                }
                memcpy((ui8*)State_.Buf + State_.BufLen, in, inlen);
                State_.BufLen += inlen;
            }
        }

        void Final(void* out, size_t outlen) override {
            if (out == nullptr || outlen < State_.OutLen)
                ythrow yexception() << "out is null or outlen is too long";

            if (IsLastBlock_())
                ythrow yexception() << "Final can't be called several times";

            IncrementCounter_(State_.BufLen);
            SetLastBlock_();
            memset((ui8*)State_.Buf + State_.BufLen, 0, BLAKE2B_BLOCKBYTES - State_.BufLen);
            Compress_(State_.Buf);
            memcpy(out, (void*)&State_.H[0], outlen);
        }
    };
}
