#include "vm_defs.h"

#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NSc::NUt {
    namespace {
        TStringBuf GetStringByIdx(ui32 idx) {
            static const TStringBuf strings[TIdx::ValueCount] {{}, "a", "aa", "aaa"};
            return strings[idx % TIdx::ValueCount];
        }
    }


    TVMState::TVMState(TStringBuf wire, ui32 memSz, ui32 pos)
        : Input(wire)
        , Memory(memSz)
        , Pos(pos)
    {}

    bool TVMState::TryPushBack() {
        if (MayAddMoreMemory()) {
            Memory.emplace_back();
            return true;
        } else {
            return false;
        }
    }

    bool TVMState::TryPushFront() {
        if (MayAddMoreMemory()) {
            Pos += 1;
            Memory.emplace_front();
            return true;
        } else {
            return false;
        }
    }

    bool TVMState::TryPopBack() {
        if (Memory.size() > 1 && Pos < Memory.size() - 1) {
            Memory.pop_back();
            return true;
        } else {
            return false;
        }
    }

    bool TVMState::TryPopFront() {
        if (Memory.size() > 1 && Pos > 0) {
            Memory.pop_front();
            Pos -= 1;
            return true;
        } else {
            return false;
        }
    }

    TString TVMState::ToString() const {
        TStringBuilder b;
        b << "pos=" << Pos << ";";
        for (const auto i : xrange(Memory.size())) {
            b << " " << i << (i == Pos ? "@" : ".") << Memory[i].ToJson().Quote();
        }

        return b;
    }

    TString TIdx::ToString() const {
        return TStringBuilder() << "IDX:" << Idx;
    }

    TString TPos::ToString() const {
        return TStringBuilder() << "POS:" << Pos;
    }

    template <class T>
    TString RenderToString(TStringBuf name, T type, ui32 pos) {
        TStringBuilder b;
        b << name << ":" << type;
        if ((ui32)-1 != pos) {
            b << "," << pos;
        }
        return b;
    }

    TString TRef::ToString() const {
        return RenderToString("REF", Type, Pos);
    }

    TString TSrc::ToString() const {
        return RenderToString("SRC", Type, Pos);
    }

    TString TDst::ToString() const {
        return RenderToString("DST", Type, Pos);
    }

    TString TPath::ToString() const {
        return TStringBuilder() << "PATH:" << Path.Quote();
    }


    TRef TVMAction::GetRef(ui32 arg) const noexcept {
        return std::get<TRef>(Arg[arg]);
    }

    TSrc TVMAction::GetSrc(ui32 arg) const noexcept {
        return std::get<TSrc>(Arg[arg]);
    }

    TDst TVMAction::GetDst(ui32 arg) const noexcept {
        return std::get<TDst>(Arg[arg]);
    }

    ui32 TVMAction::GetPos(ui32 arg) const noexcept {
        return std::get<TPos>(Arg[arg]).Pos;
    }

    ui32 TVMAction::GetIdx(ui32 arg) const noexcept {
        return std::get<TIdx>(Arg[arg]).Idx;
    }

    TStringBuf TVMAction::GetKey(ui32 arg) const noexcept {
        return GetString(arg);
    }

    TStringBuf TVMAction::GetString(ui32 arg) const noexcept {
        return GetStringByIdx(GetIdx(arg));
    }

    i64 TVMAction::GetIntNumber(ui32 arg) const noexcept {
        return GetIdx(arg);
    }

    TStringBuf TVMAction::GetPath(ui32 arg) const noexcept {
        return std::get<TPath>(Arg[arg]).Path;
    }


    struct TActionPrinter {
        TActionPrinter(IOutputStream& out)
            : Out(out)
        {}

        bool operator()(const std::monostate&) const {
            return true;
        }
        //TIdx, TPos, TRef, TSrc, TDst, TPath
        template <class T>
        bool operator()(const T& t) const {
            Out << "; " << t.ToString();
            return false;
        }
        IOutputStream& Out;
    };

    TString TVMAction::ToString() const {
        TStringBuilder out;
        out << Type;
        for (const auto& arg : Arg) {
            if (std::visit(TActionPrinter(out.Out), arg)) {
                break;
            }
        }
        return out;
    }
}
