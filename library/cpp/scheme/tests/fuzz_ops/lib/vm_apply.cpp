#include "vm_apply.h"

namespace NSc::NUt {

#define Y_GEN_TRY_OP(op) \
    if (!op) { return false; }

#define Y_GEN_SRC_OP(op, arg, st, act) \
    switch (act.GetSrc(arg).Type) { \
    case TSrc::T_LREF__POS: \
        op(st.LRef(act.GetSrc(arg).Pos)); \
        break; \
    case TSrc::T_CREF__POS: \
        op(st.CRef(act.GetSrc(arg).Pos)); \
        break; \
    case TSrc::T_RREF__POS: \
        op(st.RRef(act.GetSrc(arg).Pos)); \
        break; \
    default: \
        Y_ABORT(); \
    }


#define Y_GEN_SRC_TRY_OP(op, arg, st, act) \
    if (st.CRef(act.GetSrc(arg).Pos).IsSameOrAncestorOf(st.Current())) { \
        return false; \
    } \
    Y_GEN_SRC_OP(op, arg, st, act);


#define Y_GEN_DST_OP(op, arg, st, act) \
    switch (act.GetDst(arg).Type) { \
    case TDst::T_CREATE_BACK_LREF: \
        Y_GEN_TRY_OP(st.TryPushBack(op)) \
        break; \
    case TDst::T_CREATE_BACK_CREF: \
        Y_GEN_TRY_OP(st.TryPushBack(std::as_const(op))) \
        break; \
    case TDst::T_CREATE_BACK_RREF: \
        Y_GEN_TRY_OP(st.TryPushBack(std::move(op))) \
        break; \
    case TDst::T_CREATE_FRONT_LREF: \
        Y_GEN_TRY_OP(st.TryPushFront(op)) \
        break; \
    case TDst::T_CREATE_FRONT_CREF: \
        Y_GEN_TRY_OP(st.TryPushFront(std::as_const(op))) \
        break; \
    case TDst::T_CREATE_FRONT_RREF: \
        Y_GEN_TRY_OP(st.TryPushFront(std::move(op))) \
        break; \
    case TDst::T_LREF__POS: \
        st.LRef(act.GetDst(arg).Pos) = op; \
        break; \
    case TDst::T_CREF__POS: \
        st.LRef(act.GetDst(arg).Pos) = std::as_const(op); \
        break; \
    case TDst::T_RREF__POS: \
        st.LRef(act.GetDst(arg).Pos) = std::move(op); \
        break; \
    default: \
        Y_ABORT(); \
    }


#define Y_GEN_REF_OP(op, arg, st, act) \
    switch (act.GetRef(arg).Type) { \
    case TRef::T_CREATE_BACK: \
        Y_GEN_TRY_OP(st.TryPushBack(op)) \
        break; \
    case TRef::T_CREATE_FRONT: \
        Y_GEN_TRY_OP(st.TryPushFront(op)) \
        break; \
    case TRef::T_REF__POS: \
        st.LRef(act.GetRef(arg).Pos) = op; \
        break; \
    default: \
        Y_ABORT(); \
    }

#define Y_GEN_PTR_OP(op, arg, st, act) \
    if (auto* r = (op)) { \
        switch (act.GetRef(arg).Type) { \
        case TRef::T_CREATE_BACK: \
            Y_GEN_TRY_OP(st.TryPushBack(*r)) \
            break; \
        case TRef::T_CREATE_FRONT: \
            Y_GEN_TRY_OP(st.TryPushFront(*r)) \
            break; \
        case TRef::T_REF__POS: \
            st.LRef(act.GetRef(arg).Pos) = *r; \
            break; \
        default: \
            Y_ABORT(); \
        } \
    }

    bool ApplyNextAction(TVMState& st, TVMAction act) {
        switch (act.Type) {
        case VMA_JMP__POS:
            st.Pos = act.GetPos(0);
            return true;

        case VMA_CREATE_BACK:
            Y_GEN_TRY_OP(st.TryPushBack())
            return true;

        case VMA_CREATE_BACK__SRC:
            switch (act.GetSrc(0).Type) {
            case TSrc::T_LREF__POS:
                Y_GEN_TRY_OP(st.TryPushBack(st.LRef(act.GetSrc(0).Pos)))
                break;
            case TSrc::T_CREF__POS:
                Y_GEN_TRY_OP(st.TryPushBack(st.CRef(act.GetSrc(0).Pos)))
                break;
            case TSrc::T_RREF__POS:
                Y_GEN_TRY_OP(st.TryPushBack(st.RRef(act.GetSrc(0).Pos)))
                break;
            default:
                Y_ABORT();
            }

            return true;

        case VMA_CREATE_FRONT:
            Y_GEN_TRY_OP(st.TryPushFront())
            return true;

        case VMA_CREATE_FRONT__SRC:
            switch (act.GetSrc(0).Type) {
            case TSrc::T_LREF__POS:
                Y_GEN_TRY_OP(st.TryPushFront(st.LRef(act.GetSrc(0).Pos)))
                break;
            case TSrc::T_CREF__POS:
                Y_GEN_TRY_OP(st.TryPushFront(st.CRef(act.GetSrc(0).Pos)))
                break;
            case TSrc::T_RREF__POS:
                Y_GEN_TRY_OP(st.TryPushFront(st.RRef(act.GetSrc(0).Pos)))
                break;
            default:
                Y_ABORT();
            }
            return true;

        case VMA_DESTROY_BACK:
            return st.TryPopBack();

        case VMA_DESTROY_FRONT:
            return st.TryPopFront();

        case VMA_ASSIGN__SRC:
            Y_GEN_SRC_OP(st.Current() = , 0, st, act);
            return true;

        case VMA_SET_STRING__IDX:
            st.Current().SetString(act.GetString(0));
            return true;

        case VMA_SET_INT_NUMBER__IDX:
            st.Current().SetIntNumber(act.GetIntNumber(0));
            return true;

        case VMA_SET_DICT:
            st.Current().SetDict();
            return true;

        case VMA_SET_ARRAY:
            st.Current().SetArray();
            return true;

        case VMA_SET_NULL:
            st.Current().SetNull();
            return true;

        case VMA_GET_JSON:
            st.Current().ToJson();
            return true;

        case VMA_ARRAY_CLEAR:
            st.Current().ClearArray();
            return true;

        case VMA_ARRAY_PUSH:
            st.Current().Push();
            return true;

        case VMA_ARRAY_PUSH__SRC:
            Y_GEN_SRC_TRY_OP(st.Current().Push, 0, st, act);
            return true;

        case VMA_ARRAY_PUSH__DST:
            Y_GEN_DST_OP(st.Current().Push(), 0, st, act);
            return true;

        case VMA_ARRAY_POP__REF:
            Y_GEN_REF_OP(st.Current().Pop(), 0, st, act);
            return true;

        case VMA_ARRAY_INSERT__IDX:
            st.Current().Insert(act.GetIdx(0));
            return true;

        case VMA_ARRAY_INSERT__IDX_SRC:
            Y_GEN_SRC_TRY_OP(st.Current().Insert(act.GetIdx(0)) = , 1, st, act);
            return true;

        case VMA_ARRAY_INSERT__IDX_DST:
            Y_GEN_DST_OP(st.Current().Insert(act.GetIdx(0)), 1, st, act);
            return true;

        case VMA_ARRAY_DELETE__IDX_REF:
            Y_GEN_REF_OP(st.Current().Delete(act.GetIdx(0)), 1, st, act);
            return true;

        case VMA_ARRAY_GET__IDX_REF:
            Y_GEN_REF_OP(st.Current().Get(act.GetIdx(0)), 1, st, act);
            return true;

        case VMA_ARRAY_GET_OR_ADD__IDX:
            st.Current().GetOrAdd(act.GetIdx(0));
            return true;

        case VMA_ARRAY_GET_OR_ADD__IDX_SRC:
            Y_GEN_SRC_TRY_OP(st.Current().GetOrAdd(act.GetIdx(0)) = , 1, st, act);
            return true;

        case VMA_ARRAY_GET_OR_ADD__IDX_DST:
            Y_GEN_DST_OP(st.Current().GetOrAdd(act.GetIdx(0)), 1, st, act);
            return true;

        case VMA_ARRAY_GET_NO_ADD__IDX_REF:
            Y_GEN_PTR_OP(st.Current().GetNoAdd(act.GetIdx(0)), 1, st, act);
            return true;

        case VMA_DICT_CLEAR:
            st.Current().ClearDict();
            return true;

        case VMA_DICT_DELETE__IDX:
            st.Current().Delete(act.GetKey(0));
            return true;

        case VMA_DICT_DELETE__IDX_REF:
            Y_GEN_REF_OP(st.Current().Delete(act.GetKey(0)), 1, st, act);
            return true;

        case VMA_DICT_GET__IDX_REF:
            Y_GEN_REF_OP(st.Current().Get(act.GetKey(0)), 1, st, act);
            return true;

        case VMA_DICT_GET_OR_ADD__IDX:
            st.Current().GetOrAdd(act.GetKey(0));
            return true;

        case VMA_DICT_GET_OR_ADD__IDX_SRC:
            Y_GEN_SRC_TRY_OP(st.Current().GetOrAdd(act.GetKey(0)) = , 1, st, act);
            return true;

        case VMA_DICT_GET_OR_ADD__IDX_DST:
            Y_GEN_DST_OP(st.Current().GetOrAdd(act.GetKey(0)), 1, st, act);
            return true;

        case VMA_DICT_GET_NO_ADD__IDX_REF:
            Y_GEN_PTR_OP(st.Current().GetNoAdd(act.GetKey(0)), 1, st, act);
            return true;

        case VMA_MERGE_UPDATE__POS:
            st.Current().MergeUpdate(st.LRef(act.GetPos(0)));
            return true;

        case VMA_MERGE_REVERSE__POS:
            st.Current().ReverseMerge(st.LRef(act.GetPos(0)));
            return true;

        case VMA_MERGE_COPY_FROM__POS:
            st.Current().CopyFrom(st.LRef(act.GetPos(0)));
            return true;

        case VMA_SWAP__POS:
            st.Current().Swap(st.LRef(act.GetPos(0)));
            return true;

        case VMA_EQUAL__POS_POS:
            TValue::Equal(st.CRef(act.GetPos(0)), st.CRef(act.GetPos(1)));
            return true;

        case VMA_SELECT_NO_ADD__PATH_REF:
            Y_GEN_REF_OP(st.Current().TrySelect(act.GetPath(0)), 1, st, act);
            return true;

        case VMA_SELECT_OR_ADD__PATH_REF:
            Y_GEN_PTR_OP(st.Current().TrySelectOrAdd(act.GetPath(0)), 1, st, act);
            return true;

        case VMA_SELECT_AND_DELETE__PATH_REF:
            Y_GEN_REF_OP(st.Current().TrySelectAndDelete(act.GetPath(0)), 1, st, act);
            return true;

        default:
            Y_ABORT();
        }
    }
}
