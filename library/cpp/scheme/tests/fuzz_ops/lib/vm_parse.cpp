#include "vm_parse.h"

namespace NSc::NUt {
#define Y_TRY_READ_BITS(state, out, bits, res) do { if (!state.Input.Read(out, bits)) { return res; } } while (false)
#define Y_TRY_READ_BITS_BOOL(state, out, bits) Y_TRY_READ_BITS(state, out, bits, false)
#define Y_TRY_READ_BITS_MAYBE(state, out, bits) Y_TRY_READ_BITS(state, out, bits, Nothing())

    TMaybe<TIdx> ParseIdx(TVMState& st) {
        static_assert(IsPowerOf2(TIdx::ValueCount));
        static const auto bits = GetCountWidth(TIdx::ValueCount);

        TIdx idx;
        if (!st.Input.Read(idx.Idx, bits)) {
            return Nothing();
        }
        return idx;
    }

    namespace {
        bool DoParsePos(TVMState& st, TPos& pos) {
            static const auto bits = GetCountWidth(TPos::ValueCount);
            const ui32 sz = st.Memory.size();

            ui32 neg = -1;
            Y_TRY_READ_BITS_BOOL(st, neg, 1);

            ui32 delta = -1;
            Y_TRY_READ_BITS_BOOL(st, delta, bits);

            if (neg) {
                if (st.Pos < delta) {
                    return false;
                }
                pos.Pos = st.Pos - delta;
            } else {
                if (st.Pos + delta >= sz) {
                    return false;
                }
                pos.Pos = st.Pos + delta;
            }

            return true;
        }

        template <class T>
        bool DoParseType(TVMState& state, T& res) {
            static const auto bits = GetCountWidth(T::TypeCount);

            ui32 type = -1;
            if (state.Input.Read(type, bits) && type < T::TypeCount) {
                res.Type = (typename T::EType)(type);
                return true;
            } else {
                return false;
            }
        }
    }

    TMaybe<TPos> ParsePos(TVMState& state) {
        TPos res;
        if (DoParsePos(state, res)) {
            return res;
        }
        return Nothing();
    }

    TMaybe<TRef> ParseRef(TVMState& state) {
        TRef ref;
        if (!DoParseType(state, ref)) {
            return Nothing();
        }

        switch (ref.Type) {
        case TRef::T_REF__POS:
            if (!DoParsePos(state, ref)) {
                return Nothing();
            }
            [[fallthrough]];
        case TRef::T_CREATE_FRONT:
        case TRef::T_CREATE_BACK:
            return ref;
        default:
            Y_ABORT();
        }
    }

    TMaybe<TSrc> ParseSrc(TVMState& state) {
        TSrc src;
        if (!DoParseType(state, src)) {
            return Nothing();
        }

        switch (src.Type) {
        case TSrc::T_LREF__POS:
        case TSrc::T_CREF__POS:
        case TSrc::T_RREF__POS:
            if (!DoParsePos(state, src)) {
                return Nothing();
            }
            return src;
        default:
            Y_ABORT();
        }
    }

    TMaybe<TDst> ParseDst(TVMState& state) {
        TDst dst;
        if (!DoParseType(state, dst)) {
            return Nothing();
        }

        switch (dst.Type) {
        case TDst::T_LREF__POS:
        case TDst::T_CREF__POS:
        case TDst::T_RREF__POS:
            if (!DoParsePos(state, dst)) {
                return Nothing();
            }
            [[fallthrough]];
        case TDst::T_CREATE_FRONT_LREF:
        case TDst::T_CREATE_FRONT_CREF:
        case TDst::T_CREATE_FRONT_RREF:
        case TDst::T_CREATE_BACK_LREF:
        case TDst::T_CREATE_BACK_CREF:
        case TDst::T_CREATE_BACK_RREF:
            return dst;
        default:
            Y_ABORT();
        }
    }

    TMaybe<TPath> ParsePath(TVMState& state) {
        static const ui32 bits = GetCountWidth(TPath::MaxLength);
        TPath path;

        ui32 len = -1;
        Y_TRY_READ_BITS_MAYBE(state, len, bits);
        while (len--) {
            ui8 c;
            Y_TRY_READ_BITS_MAYBE(state, c, 8);
            path.Path.push_back(c);
        }
        return path;
    }

    TMaybe<TVMAction> ParseNextAction(TVMState& state) {
        TVMAction res;

        if (!DoParseType(state, res)) {
            return Nothing();
        }

        switch (res.Type) {
        case VMA_CREATE_BACK:
        case VMA_CREATE_FRONT:
        case VMA_DESTROY_BACK:
        case VMA_DESTROY_FRONT:

        case VMA_SET_DICT:
        case VMA_SET_ARRAY:
        case VMA_SET_NULL:
        case VMA_GET_JSON:

        case VMA_ARRAY_CLEAR:
        case VMA_ARRAY_PUSH:
        case VMA_DICT_CLEAR:
            return res;

        case VMA_JMP__POS:
        case VMA_MERGE_UPDATE__POS:
        case VMA_MERGE_REVERSE__POS:
        case VMA_MERGE_COPY_FROM__POS:
        case VMA_SWAP__POS:
            if (res.SetArg(ParsePos(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_ARRAY_POP__REF:
            if (res.SetArg(ParseRef(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_SET_STRING__IDX:
        case VMA_SET_INT_NUMBER__IDX:
        case VMA_ARRAY_INSERT__IDX:
        case VMA_ARRAY_GET_OR_ADD__IDX:
        case VMA_DICT_GET_OR_ADD__IDX:
            if (res.SetArg(ParseIdx(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_CREATE_BACK__SRC:
        case VMA_CREATE_FRONT__SRC:
        case VMA_ASSIGN__SRC:
        case VMA_ARRAY_PUSH__SRC:
            if (res.SetArg(ParseSrc(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_ARRAY_PUSH__DST:
            if (res.SetArg(ParseDst(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_ARRAY_DELETE__IDX_REF:
        case VMA_ARRAY_GET__IDX_REF:
        case VMA_ARRAY_GET_NO_ADD__IDX_REF:
        case VMA_DICT_DELETE__IDX_REF:
        case VMA_DICT_GET__IDX_REF:
        case VMA_DICT_GET_NO_ADD__IDX_REF:
            if (res.SetArg(ParseIdx(state)) && res.SetArg(ParseRef(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_ARRAY_INSERT__IDX_SRC:
        case VMA_ARRAY_GET_OR_ADD__IDX_SRC:
        case VMA_DICT_GET_OR_ADD__IDX_SRC:
            if (res.SetArg(ParseIdx(state)) && res.SetArg(ParseSrc(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_ARRAY_INSERT__IDX_DST:
        case VMA_ARRAY_GET_OR_ADD__IDX_DST:
        case VMA_DICT_GET_OR_ADD__IDX_DST:
            if (res.SetArg(ParseIdx(state)) && res.SetArg(ParseDst(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_EQUAL__POS_POS:
            if (res.SetArg(ParsePos(state)) && res.SetArg(ParsePos(state))) {
                return res;
            } else {
                return Nothing();
            }

        case VMA_SELECT_NO_ADD__PATH_REF:
        case VMA_SELECT_OR_ADD__PATH_REF:
        case VMA_SELECT_AND_DELETE__PATH_REF:
            if (res.SetArg(ParsePath(state)) && res.SetArg(ParseRef(state))) {
                return res;
            } else {
                return Nothing();
            }

        default:
            return Nothing();
        }
    }
}
