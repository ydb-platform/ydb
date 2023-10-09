#pragma once

#include <library/cpp/bit_io/bitinput.h>
#include <library/cpp/scheme/scheme.h>

#include <util/generic/deque.h>
#include <util/generic/maybe.h>
#include <util/generic/variant.h>
#include <util/string/builder.h>

namespace NSc::NUt {

    enum EVMAction {
        VMA_JMP__POS                    /* "pos=POS" */,

        VMA_CREATE_BACK                 /* "TValue()->emplace_back" */,
        VMA_CREATE_BACK__SRC            /* "TValue(SRC)->emplace_back" */,
        VMA_CREATE_FRONT                /* "TValue()->emplace_front,pos+=1" */,
        VMA_CREATE_FRONT__SRC           /* "TValue(SRC)->emplace_front,pos+=1" */,

        VMA_DESTROY_BACK                /* "pop_back" */,
        VMA_DESTROY_FRONT               /* "pop_front,pos-=1" */,

        VMA_ASSIGN__SRC                 /* "operator=(SRC)" */,

        VMA_SET_STRING__IDX             /* "SetString(str[IDX])" */,
        VMA_SET_INT_NUMBER__IDX         /* "SetIntNumber(IDX)" */,
        VMA_SET_DICT                    /* "SetDict()" */,
        VMA_SET_ARRAY                   /* "SetArray()" */,
        VMA_SET_NULL                    /* "SetNull()" */,

        VMA_GET_JSON                    /* "ToJson()" */,

        VMA_ARRAY_CLEAR                 /* "ClearArray()" */,
        VMA_ARRAY_PUSH                  /* "Push()" */,
        VMA_ARRAY_PUSH__SRC             /* "Push()=SRC" */,
        VMA_ARRAY_PUSH__DST             /* "Push()->DST" */,
        VMA_ARRAY_POP                   /* "Pop()" */,
        VMA_ARRAY_POP__REF              /* "Pop()->REF" */,
        VMA_ARRAY_INSERT__IDX           /* "Insert(IDX)" */,
        VMA_ARRAY_INSERT__IDX_SRC       /* "Insert(IDX)=SRC" */,
        VMA_ARRAY_INSERT__IDX_DST       /* "Insert(IDX)->DST" */,
        VMA_ARRAY_DELETE__IDX           /* "Delete(IDX)" */,
        VMA_ARRAY_DELETE__IDX_REF       /* "Delete(IDX)->REF" */,
        VMA_ARRAY_GET__IDX_REF          /* "Get(IDX)->REF" */,
        VMA_ARRAY_GET_OR_ADD__IDX       /* "GetOrAdd(IDX)" */,
        VMA_ARRAY_GET_OR_ADD__IDX_SRC   /* "GetOrAdd(IDX)=SRC" */,
        VMA_ARRAY_GET_OR_ADD__IDX_DST   /* "GetOrAdd(IDX)->DST" */,
        VMA_ARRAY_GET_NO_ADD__IDX_REF   /* "GetNoAdd(IDX)->REF" */,

        VMA_DICT_CLEAR                  /* "ClearDict()" */,
        VMA_DICT_DELETE__IDX            /* "Delete(str[IDX])" */,
        VMA_DICT_DELETE__IDX_REF        /* "Delete(str[IDX])->REF" */,
        VMA_DICT_GET__IDX_REF           /* "Get(str[IDX])->REF" */,
        VMA_DICT_GET_OR_ADD__IDX        /* "GetOrAdd(str[IDX])" */,
        VMA_DICT_GET_OR_ADD__IDX_SRC    /* "GetOrAdd(str[IDX])=SRC" */,
        VMA_DICT_GET_OR_ADD__IDX_DST    /* "GetOrAdd(str[IDX])->DST" */,
        VMA_DICT_GET_NO_ADD__IDX_REF    /* "GetNoAdd(str[IDX])->REF" */,

        VMA_MERGE_UPDATE__POS           /* "MergeUpdate(POS)" */,
        VMA_MERGE_REVERSE__POS          /* "ReverseMerge(POS)" */,
        VMA_MERGE_COPY_FROM__POS        /* "CopyFrom(POS)" */,

        VMA_SWAP__POS                   /* "Swap(POS)" */,
        VMA_EQUAL__POS_POS              /* "Equal(POS, POS)" */,

        VMA_SELECT_NO_ADD__PATH_REF      /* "TrySelect(PATH)->REF" */,
        VMA_SELECT_OR_ADD__PATH_REF      /* "TrySelectOrAdd(PATH)->REF" */,
        VMA_SELECT_AND_DELETE__PATH_REF  /* "TrySelectAndDelete(PATH)->REF" */,

        VMA_COUNT,
    };


    struct TVMState {
        NBitIO::TBitInput Input;
        TDeque<TValue> Memory { 1 };
        ui32 Pos = 0;

        static const ui32 MaxMemory = 16;

    public:
        explicit TVMState(TStringBuf wire, ui32 memSz, ui32 pos);

        TValue& Current() {
            return Memory[Pos];
        }

        TValue& LRef(ui32 pos) {
            return Memory[pos];
        }

        const TValue& CRef(ui32 pos) {
            return Memory[pos];
        }

        TValue&& RRef(ui32 pos) {
            return std::move(Memory[pos]);
        }

        [[nodiscard]]
        bool TryPushBack();

        template <class T>
        [[nodiscard]]
        bool TryPushBack(T&& t) {
            if (MayAddMoreMemory()) {
                Memory.emplace_back(std::forward<T>(t));
                return true;
            } else {
                return false;
            }
        }

        [[nodiscard]]
        bool TryPushFront();

        template <class T>
        [[nodiscard]]
        bool TryPushFront(T&& t) {
            if (MayAddMoreMemory()) {
                Pos += 1;
                Memory.emplace_front(std::forward<T>(t));
                return true;
            } else {
                return false;
            }
        }

        [[nodiscard]]
        bool TryPopBack();

        [[nodiscard]]
        bool TryPopFront();

        TString ToString() const;

    private:
        [[nodiscard]]
        bool MayAddMoreMemory() const noexcept {
            return Memory.size() < MaxMemory;
        }
    };


    struct TIdx {
        ui32 Idx = -1;
        static const ui32 ValueCount = 4;

    public:
        TString ToString() const;
    };


    struct TPos {
        ui32 Pos = -1;
        static const ui32 ValueCount = TVMState::MaxMemory;

    public:
        TString ToString() const;
    };


    struct TRef : public TPos {
        enum EType {
            T_CREATE_FRONT  /* "emplace_front(RES),pos+=1" */,
            T_CREATE_BACK   /* "emplace_back(RES)" */,
            T_REF__POS      /* "pos@(RES)" */,
            T_COUNT
        };

        EType Type = T_COUNT;
        static const ui32 TypeCount = T_COUNT;

    public:
        TString ToString() const;
    };


    struct TSrc : public TPos {
        enum EType {
            T_LREF__POS     /* "pos@(TValue&)" */,
            T_CREF__POS     /* "pos@(const TValue&)" */,
            T_RREF__POS     /* "pos@(TValue&&)" */,
            T_COUNT
        };

        EType Type = T_COUNT;
        static const ui32 TypeCount = T_COUNT;

    public:
        TString ToString() const;
    };


    struct TDst : public TPos {
        enum EType {
            T_CREATE_FRONT_LREF     /* "emplace_front(TValue&),pos+=1" */,
            T_CREATE_FRONT_CREF     /* "emplace_front(const TValue&),pos+=1" */,
            T_CREATE_FRONT_RREF     /* "emplace_front(TValue&&),pos+=1" */,
            T_CREATE_BACK_LREF      /* "emplace_back(TValue&)" */,
            T_CREATE_BACK_CREF      /* "emplace_back(TValue&),pos+=1" */,
            T_CREATE_BACK_RREF      /* "emplace_back(TValue&),pos+=1" */,
            T_LREF__POS             /* "pos@(TValue&)" */,
            T_CREF__POS             /* "pos@(const TValue&)" */,
            T_RREF__POS             /* "pos@(TValue&&)" */,
            T_COUNT
        };

        EType Type = T_COUNT;
        static const ui32 TypeCount = T_COUNT;

    public:
        TString ToString() const;
    };


    struct TPath {
        TString Path;
        static const ui32 MaxLength = 32;

    public:
        TString ToString() const;
    };

    using TArg = std::variant<std::monostate, TIdx, TPos, TRef, TSrc, TDst, TPath>;

    struct TVMAction {
        using EType = EVMAction;
        EVMAction Type = VMA_COUNT;
        static const ui32 TypeCount = VMA_COUNT;

    public:
        template <class T>
        bool SetArg(TMaybe<T> arg) {
            Y_ABORT_UNLESS(CurrArg < Y_ARRAY_SIZE(Arg));
            if (arg) {
                Arg[CurrArg++] = *arg;
                return true;
            } else {
                return false;
            }
        }

    public:
        TRef GetRef(ui32 arg) const noexcept;

        TSrc GetSrc(ui32 arg) const noexcept;

        TDst GetDst(ui32 arg) const noexcept;


        ui32 GetPos(ui32 arg) const noexcept;

        ui32 GetIdx(ui32 arg) const noexcept;

        TStringBuf GetKey(ui32 arg) const noexcept;

        TStringBuf GetString(ui32 arg) const noexcept;

        i64 GetIntNumber(ui32 arg) const noexcept;

        TStringBuf GetPath(ui32 arg) const noexcept;

        TString ToString() const;

    private:
        TArg Arg[2];
        ui32 CurrArg = 0;
    };

    [[nodiscard]]
    inline ui32 GetCountWidth(ui32 cnt) {
        return MostSignificantBit(cnt - 1) + 1;
    }

}
