#include "fold.h"

namespace NUF {
    TNormalizer::TNormalizer(ELanguage lmain, ELanguage laux)
        : DoRenyxa()
        , DoLowerCase()
        , DoSimpleCyr()
        , FillOffsets()
    {
        Reset();
        SetLanguages(lmain, laux);
    }

    TNormalizer::TNormalizer(const TLanguages& langs)
        : DoRenyxa()
        , DoLowerCase()
        , DoSimpleCyr()
        , FillOffsets()
    {
        Reset();
        SetLanguages(langs);
    }

    void TNormalizer::SetLanguages(ELanguage lmain, ELanguage laux) {
        Languages.reset();
        Scripts.reset();
        Languages.set(lmain);
        Languages.set(laux);
        Scripts.set(ScriptByLanguage(lmain));
        Scripts.set(ScriptByLanguage(laux));
    }

    void TNormalizer::SetLanguages(const TLanguages& langs) {
        Languages = langs;
        Scripts.reset();

        for (ui32 i = 0; i < langs.size(); ++i) {
            if (langs.test(i))
                Scripts.set(ScriptByLanguage(ELanguage(i)));
        }
    }

    void TNormalizer::SetDoRenyxa(bool da) {
        DoRenyxa = da;
    }

    void TNormalizer::SetDoLowerCase(bool da) {
        DoLowerCase = da;
    }

    void TNormalizer::SetDoSimpleCyr(bool da) {
        DoSimpleCyr = da;
    }

    void TNormalizer::SetFillOffsets(bool da) {
        FillOffsets = da;
    }

    void TNormalizer::Reset() {
        CDBuf.clear();
        OutBuf.clear();
        CDOffsets.clear();
        TmpBuf.clear();
        p = p0 = pe = eof = ts = te = ret = nullptr;
        cs = act = 0;
    }

    void TNormalizer::SetInput(TWtringBuf b) {
        Reset();
        CDBuf.reserve(2 * b.size());
        OutBuf.reserve(2 * b.size());

        Decomposer.Normalize(b.data(), b.size(), CDBuf);
        p = p0 = CDBuf.begin();
        pe = eof = CDBuf.end();
    }

}
