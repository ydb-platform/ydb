#pragma once

#include "colons.h"
#include "do_zero.h"
#include "do_mem.h"
#include "do_part.h"
#include "do_iter.h"
#include "logger.h"
#include "sponge.h"
#include "runner.h"

#include <ydb/core/tablet_flat/util_fmt_desc.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>
#include <ydb/core/tablet_flat/test/libs/table/model/small.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>

namespace NKikimr {
namespace NTable {
namespace NPerf {

    class TApp {
    public:
        using ESponge = NTest::ESponge;

        int Execute()
        {
            Logger = new TLogger(LLev, LLev, nullptr);

            if (Mass == nullptr) SetMass("32768:2cols");

            NTest::TSponge sponge(Sponge);

            if (auto logl = Logger->Log(NKikiSched::ELnLev::INFO)) {
                logl << "Using " << NFmt::Do(*Mass);
            }

            if (auto logl = Logger->Log(NKikiSched::ELnLev::INFO)) {
                logl << "Modelled by " << NFmt::Do(*Mass->Model);
            }

            TRunner env(Logger.Get(), *Mass, sponge, Conf);

            if (Test & 0x0001) {
                Cout << "zero       "; env.Do<TDoZero>(); Cout << Endl;
            }
            if (Test & 0x0002) {
                Cout << "warm-tree  "; env.Do<TDoMem>(); Cout << Endl;
            }
            if (Test & 0x0004) {
                Cout << "flat-32.lz4"; env.Do<TDoPart>(true); Cout << Endl;
            }
            if (Test & 0x0008) {
                Cout << "flat-7KiB  "; env.Do<TDoPart>(false); Cout << Endl;
            }
            if (Test & 0x0010) {
                Cout << "it-1.0.seq "; env.Do<TDoIter>(0, false); Cout << Endl;
            }
            if (Test & 0x0020) {
                Cout << "it-0.2.rnd "; env.Do<TDoIter>(2, true); Cout << Endl;
            }
            if (Test & 0x0040) {
                Cout << "it-0.4.rnd "; env.Do<TDoIter>(4, true); Cout << Endl;
            }
            if (Test & 0x0080) {
                Cout << "it-0.8.rnd "; env.Do<TDoIter>(8, true); Cout << Endl;
            }
            if (Test & 0x0100) {
                Cout << "it-0.8.seq "; env.Do<TDoIter>(8, false); Cout << Endl;
            }

            if (auto logl = Logger->Log(NKikiSched::ELnLev::INFO)) {
                logl << NFmt::Do(sponge);
            }

            return 0;
        }

        void SetLogger(const TString line)
        {
            NKikiSched::TColons args(line);

            args.Put(LLev);
        }

        void SetTests(const TString line)
        {
            Test = 0;

            NKikiSched::TColons args(line);

            while (auto name = args.Next()) {
                if (name == "zero") { Test |= 0x0001;
                } else if (name == "warm") { Test |= 0x0002;
                } else if (name == "flz4") { Test |= 0x0004;
                } else if (name == "7kib") { Test |= 0x0008;
                } else if (name == "s1.0") { Test |= 0x0010;
                } else if (name == "r0.2") { Test |= 0x0020;
                } else if (name == "r0.4") { Test |= 0x0040;
                } else if (name == "r0.4") { Test |= 0x0080;
                } else if (name == "s0.8") { Test |= 0x0100;
                } else {
                    throw yexception() << "Unknown test code " << name;
                }
            }
        }

        void SetMass(const TString line)
        {
            NKikiSched::TColons args(line);

            auto rows = args.Value<ui64>(32768);
            const TString kind = args.Token("small");

            TAutoPtr<NTest::IModel> model;

            if (kind == "2cols") {
                model = new NTest::TModel2Cols;
            } else if (kind == "mixed") {
                model = new NTest::TModelStd(false);
            } else {
                throw yexception() << "Unknown rows model " << kind;
            }

            Mass = new NTest::TMass(model, rows);
        }

        void SetSponge(const TString line)
        {
            Sponge = ESpongeBy(line);
        }

        void SetSampling(const TString line)
        {
            NKikiSched::TColons args(line);

            Conf.Least = args.Large(Conf.Least);
            Conf.Limit = args.Large(Conf.Limit);
            args.Put(Conf.Spent);
        }

    private:
        static ESponge ESpongeBy(const TString &sponge)
        {
            if (sponge == "none") return ESponge::None;
            if (sponge == "mur")  return ESponge::Murmur;
            if (sponge == "city") return ESponge::City;
            if (sponge == "fnv")  return ESponge::Fnv;
            if (sponge == "xor")  return ESponge::Xor;

            throw yexception() << "Unknown sponge " << sponge;
        }

    private:
        int LLev = 1;
        TAutoPtr<TLogger> Logger;
        ui64 Test = 0xffffffff; /* Bitmask of tests to perform  */
        NPerf::TConf Conf;
        TAutoPtr<NTest::TMass> Mass;
        NTest::ESponge Sponge = ESponge::Murmur;
    };

}
}
}
