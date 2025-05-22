#pragma once

#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/tx_proxy.pb.h>

namespace NKikimr {
namespace NMiniKQL {

    struct TLocalMiniKQLProgram {
        struct TProgramFragment {
            TString Binary;
            TString Text;
        };

        TProgramFragment Program;
        TProgramFragment Params;

        bool CompileOnly;

        static TLocalMiniKQLProgram TextText(const TString &textPgm, const TString &textParams) {
            return {{TString(), textPgm}, {TString(), textParams}, false};
        }
        static TLocalMiniKQLProgram TextBin(const TString &textPgm, const TString &binParams) {
            return {{TString(), textPgm},{binParams, TString()}, false};
        }
        static TLocalMiniKQLProgram BinText(const TString &binPgm, const TString &textParams) {
            return {{binPgm, TString()},{TString(), textParams}, false};
        }
        static TLocalMiniKQLProgram BinBin(const TString &binPgm, const TString &binParams) {
            return {{binPgm, TString()},{binParams, TString()}, false};
        }
        static TLocalMiniKQLProgram Compile(const TString &textPgm, const TString &textParams) {
            return {{TString(), textPgm},{TString(), textParams}, true};
        }

        TLocalMiniKQLProgram()
            : CompileOnly(false)
        {}

        TLocalMiniKQLProgram(const TProgramFragment &program, const TProgramFragment &params, bool compileOnly)
            : Program(program)
            , Params(params)
            , CompileOnly(compileOnly)
        {}

        TLocalMiniKQLProgram(const TEvTablet::TEvLocalMKQL &msg) {
            const auto &pgm = msg.Record.GetProgram();
            if (pgm.HasProgram()) {
                const auto &x = pgm.GetProgram();
                if (x.HasBin())
                    Program.Binary = x.GetBin();
                else if (x.HasText())
                    Program.Text = x.GetText();
            }
            if (pgm.HasParams()) {
                const auto &x = pgm.GetParams();
                if (x.HasBin())
                    Params.Binary = x.GetBin();
                else if (x.HasText())
                    Params.Text = x.GetText();
            }

            CompileOnly = pgm.HasMode() && (pgm.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction_EMode_COMPILE);
        }
    };

}
}
