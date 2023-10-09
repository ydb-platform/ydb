#pragma once

#include "test_steps.h"
#include "test_pretty.h"
#include "test_part.h"
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<typename TWrap, typename TEggs>
    class TChecker: public TSteps<TChecker<TWrap, TEggs>> {
    public:
        using TBase = TSteps<TChecker<TWrap, TEggs>>;
        using TSub = decltype(std::declval<TWrap>().Get());

        struct TConf {
            TConf() = default;

            TConf(TAutoPtr<IPages> env, ui64 retry = 0, bool erased = true)
                : Env(env)
                , Retry(retry)
                , Erased(erased)
            {

            }

            TAutoPtr<IPages> Env;
            ui64 Retry = 0;
            bool Erased = true; /* do not hide ERowOp::Erase */
        };

        template<typename ... TArgs>
        TChecker(const TEggs &eggs, TConf conf, TArgs && ...args)
            : Retries(Max(conf.Retry, ui64(1) + conf.Retry))
            , Erased(conf.Erased)
            , Env(conf.Env ? conf.Env : new TTestEnv)
            , Wrap(eggs, std::forward<TArgs>(args)...)
            , Scheme(*Wrap.Scheme)
        {

        }

        const TSub operator->() const noexcept
        {
            return Wrap.Get();
        }

        TWrap& operator*() noexcept
        {
            return Wrap;
        }

        template<typename TEnv>
        TAutoPtr<TEnv> Displace(TAutoPtr<IPages> env) noexcept
        {
            auto *origin = std::exchange(Env, env).Release();
            auto *casted = dynamic_cast<TEnv*>(origin);

            Y_ABORT_UNLESS(!origin || casted, "Cannot cast IPages to given env");

            return casted;
        }

        TChecker& ReplaceEnv(TAutoPtr<IPages> env)
        {
            return Displace<IPages>(env), *this;
        }

        template<typename TEnv>
        TEnv* GetEnv() {
            auto *casted = dynamic_cast<TEnv*>(Env.Get());

            Y_ABORT_UNLESS(!Env || casted, "Cannot cast IPages to given env");

            return casted;
        }

        template<typename ...TArgs>
        inline TChecker& IsN(TArgs&&...args)
        {
            return IsOpN(ERowOp::Absent, std::forward<TArgs>(args)...);
        }

        template<typename ...TArgs>
        inline TChecker& IsOpN(ERowOp op, TArgs&&...args)
        {
            auto row = *TSchemedCookRow(Scheme).Col(std::forward<TArgs>(args)...);

            return Is(row, true, op);
        }

        template<typename ...TArgs>
        inline TChecker& HasN(TArgs&&...args)
        {
            return Has(*TSchemedCookRow(Scheme).Col(std::forward<TArgs>(args)...));
        }

        template<typename ...TArgs>
        TChecker& Has(const TRow &row, const TArgs& ... left)
        {
            return Has(row), Has(left...);
        }

        TChecker& Has(const TRow &row)
        {
            return Seek(row, ESeek::Exact).Is(row, true, ERowOp::Absent);
        }

        TChecker& NoVal(const TRow &row)
        {
            return Seek(row, ESeek::Exact).Is(row, false, ERowOp::Absent);
        }

        template<typename ...TArgs>
        TChecker& NoKey(const TRow &row, const TArgs& ... left)
        {
            return NoKey(row, true), NoKey(left...);
        }

        TChecker& NoKey(const TRow &row, bool erased = true)
        {
            Seek(row, ESeek::Exact);

            return erased ? Is(EReady::Gone) : Is(row, true, ERowOp::Erase);
        }

        template<typename ...TArgs>
        inline TChecker& NoKeyN(TArgs&&... args)
        {
            return NoKey(*TSchemedCookRow(Scheme).Col(std::forward<TArgs>(args)...));
        }

        template<typename TListType>
        TChecker& IsTheSame(const TListType &list)
        {
            Seek({ } , ESeek::Lower);

            return Is(list.begin(), list.end());
        }

        template<typename TIter>
        TChecker& IsTheSame(const TIter begin, const TIter end)
        {
            Seek({ }, ESeek::Lower);

            return Is(begin, end);
        }

        template<typename TIter>
        TChecker& Is(TIter it, const TIter end)
        {
            for (; it != end; ++it) Is(*it, true, ERowOp::Absent).Next();

            return *this;
        }

        TChecker& Seek(const TRow &tagged, ESeek seek)
        {
            return Make(TRowTool(Scheme).LookupKey(tagged), seek);
        }

        TChecker& SeekAgain(const TRow &tagged, ESeek seek)
        {
            return Make(TRowTool(Scheme).LookupKey(tagged), seek, /* make = */ false);
        }

        TChecker& Make(const TRawVals rawkey, const ESeek seek, bool make = true)
        {
            if (make) {
                Wrap.Make(Env.Get());
            }

            for (bool first = true; ; first = false)  {
                for (Hoped = 0; Hoped < Retries; Hoped++) {
                    Ready = first ? Wrap.Seek(rawkey, seek) : Wrap.Next();

                    if (Ready != EReady::Page) break;
                }

                if (Ready != EReady::Data || Erased) {
                    break;
                } else if (Wrap.Apply().GetRowState() != ERowOp::Erase) {
                    break;  /* Skip technical rows      */
                } else if (seek == ESeek::Exact) {
                    Ready = EReady::Gone;

                    break;  /* emulation of absent row  */
                }
            }

            return *this;
        }

        TChecker& StopAfter(TArrayRef<const TCell> key) {
            Wrap.StopAfter(key);
            return *this;
        }

        TChecker& Next()
        {
            while (true) {
                for (Hoped = 0; Hoped < Retries; Hoped++) {
                    Ready = Wrap.Next();

                    if (Ready != EReady::Page) break;
                }

                if (Ready != EReady::Data || Erased) {
                    break;
                } else if (Wrap.Apply().GetRowState() != ERowOp::Erase) {
                    break;  /* Skip technical rows      */
                }
            }

            return *this;
        }

        TChecker& Ver(TRowVersion rowVersion)
        {
            Y_ABORT_UNLESS(Erased, "Working with versions needs Erased == true");

            for (Hoped = 0; Hoped < Retries; Hoped++) {
                Ready = Wrap.SkipToRowVersion(rowVersion);

                if (Ready != EReady::Page) break;
            }

            if (Ready == EReady::Data) {
                Wrap.Apply();
            }

            return *this;
        }

        TChecker& IsVer(TRowVersion expected) {
            if (EReady::Gone == Ready) {
                TBase::Log()
                    << "Row not found, expected " << expected << Endl;

                UNIT_ASSERT(false);
            }

            TRowVersion current = Wrap.GetRowVersion();

            bool success = true;

            if (current != expected) {
                TBase::Log()
                    << "Row version is " << current
                    << ", expected " << expected
                    << Endl;

                success = false;
            }

            UNIT_ASSERT(success);

            return *this;
        }

        TChecker& IsOp(ERowOp op, const TRow &row)
        {
            return Is(row, true, op);
        }

        TChecker& Is(const TRow &row, bool same = true, ERowOp op = ERowOp::Absent)
        {
            if (EReady::Gone == Ready) {
                TBase::Log()
                    << "No row: " << TRowTool(Scheme).Describe(row) << Endl;

                UNIT_ASSERT(false);
            }

            Is(EReady::Data);

            const auto &state = Wrap.Apply();
            const auto &remap = Wrap.Remap();
            const bool keys = (op == ERowOp::Erase);

            bool success = true;

            if (op != ERowOp::Absent && op != state.GetRowState()) {
                TBase::Log()
                    << "Row state is " << (int)state.GetRowState()
                    << ", expected ERowOp " << (int)op
                    << Endl;

                success = false;

            } else if (TRowTool(Scheme).Cmp(row, state, remap, keys) != same) {
                TBase::Log()
                    << "Row missmatch, got "
                    << (int)state.GetRowState()
                    << ": " << NFmt::TCells(*state, remap, DbgRegistry())
                    << ", expect " << TRowTool(Scheme).Describe(row)
                    << Endl;

                success = false;
            }

            UNIT_ASSERT(success);

            return *this;
        }

        TChecker& Is(EReady ready)
        {
            if (ready != Ready) {
                if (Ready == EReady::Data) {
                    const auto &state = Wrap.Apply();
                    const auto &remap = Wrap.Remap();
                    TBase::Log()
                        << "Unexpected row "
                        << (int)state.GetRowState()
                        << ": " << NFmt::TCells(*state, remap, DbgRegistry())
                        << Endl;
                }

                TBase::Log()
                    << "Iterator has "
                    << " EReady " << static_cast<int>(Ready)
                    << ", expected " << static_cast<int>(ready)
                    << " after " << Hoped << " hopes of " << Retries
                    << Endl;

                UNIT_ASSERT(false);
            }

            return *this;
        }

        EReady GetReady() const noexcept
        {
            return Ready;
        }

    private:
        const ui64 Retries = 1;
        const ui64 Erased = true;
        ui64 Hoped = 0;
        EReady Ready = EReady::Gone;
        TAutoPtr<IPages> Env;
        TWrap Wrap;
        const TRowScheme &Scheme;
    };

}
}
}
