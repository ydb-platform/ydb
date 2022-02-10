#include "regexp.h"

#include <util/generic/string.h>
#include <util/string/ascii.h>
#include <util/system/defaults.h>

#include <cstdlib>
#include <util/generic/noncopyable.h>

class TGlobalImpl : TNonCopyable {
private:
    const char* Str;
    regmatch_t* Pmatch;
    int Options;
    int StrLen;
    int StartOffset, NotEmptyOpts, MatchPos;
    int MatchBuf[NMATCHES * 3];
    pcre* PregComp;

    enum StateCode {
        TGI_EXIT,
        TGI_CONTINUE,
        TGI_WALKTHROUGH
    };

private:
    void CopyResults(int count) {
        for (int i = 0; i < count; i++) {
            Pmatch[MatchPos].rm_so = MatchBuf[2 * i];
            Pmatch[MatchPos].rm_eo = MatchBuf[2 * i + 1];
            MatchPos++;
            if (MatchPos >= NMATCHES) {
                ythrow yexception() << "TRegExBase::Exec(): Not enough space in internal buffer.";
            }
        }
    }

    int DoPcreExec(int opts) {
        int rc = pcre_exec(
            PregComp,    /* the compiled pattern */
            nullptr,     /* no extra data - we didn't study the pattern */
            Str,         /* the subject string */
            StrLen,      /* the length of the subject */
            StartOffset, /* start at offset 0 in the subject */
            opts,        /* default options */
            MatchBuf,    /* output vector for substring information */
            NMATCHES);   /* number of elements in the output vector */

        if (rc == 0) {
            ythrow yexception() << "TRegExBase::Exec(): Not enough space in internal buffer.";
        }

        return rc;
    }

    StateCode CheckEmptyCase() {
        if (MatchBuf[0] == MatchBuf[1]) { // founded an empty string
            if (MatchBuf[0] == StrLen) {  // at the end
                return TGI_EXIT;
            }
            NotEmptyOpts = PCRE_NOTEMPTY | PCRE_ANCHORED; // trying to find non empty string
        }
        return TGI_WALKTHROUGH;
    }

    StateCode CheckNoMatch(int rc) {
        if (rc == PCRE_ERROR_NOMATCH) {
            if (NotEmptyOpts == 0) {
                return TGI_EXIT;
            }

            MatchBuf[1] = StartOffset + 1; // we have failed to find non-empty-string. trying to find again shifting "previous match offset"
            return TGI_CONTINUE;
        }
        return TGI_WALKTHROUGH;
    }

public:
    TGlobalImpl(const char* st, regmatch_t& pma, int opts, pcre* pc_re)
        : Str(st)
        , Pmatch(&pma)
        , Options(opts)
        , StartOffset(0)
        , NotEmptyOpts(0)
        , MatchPos(0)
        , PregComp(pc_re)
    {
        memset(Pmatch, -1, sizeof(regmatch_t) * NMATCHES);
        StrLen = strlen(Str);
    }

    int ExecGlobal() {
        StartOffset = 0;
        int rc = DoPcreExec(Options);

        if (rc < 0) {
            return rc;
        }
        CopyResults(rc);
        do {
            NotEmptyOpts = 0;
            StartOffset = MatchBuf[1];

            if (CheckEmptyCase() == TGI_EXIT) {
                return 0;
            }

            rc = DoPcreExec(NotEmptyOpts | Options);

            switch (CheckNoMatch(rc)) {
                case TGI_CONTINUE:
                    continue;
                case TGI_EXIT:
                    return 0;
                case TGI_WALKTHROUGH:
                default:
                    break;
            }

            if (rc < 0) {
                return rc;
            }

            CopyResults(rc);
        } while (true);

        return 0;
    }

private:
};

class TRegExBaseImpl: public TAtomicRefCount<TRegExBaseImpl> {
    friend class TRegExBase;

protected:
    int CompileOptions;
    TString RegExpr;
    regex_t Preg;

public:
    TRegExBaseImpl()
        : CompileOptions(0)
    {
        memset(&Preg, 0, sizeof(Preg));
    }

    TRegExBaseImpl(const TString& re, int cflags)
        : CompileOptions(cflags)
        , RegExpr(re)
    {
        int rc = regcomp(&Preg, re.data(), cflags);
        if (rc) {
            const size_t ERRBUF_SIZE = 100;
            char errbuf[ERRBUF_SIZE];
            regerror(rc, &Preg, errbuf, ERRBUF_SIZE);
            Error = "Error: regular expression " + re + " is wrong: " + errbuf;
            ythrow yexception() << "RegExp " << re << ": " << Error.data();
        }
    }

    int Exec(const char* str, regmatch_t pmatch[], int eflags, int nmatches) const {
        if (!RegExpr) {
            ythrow yexception() << "Regular expression is not compiled";
        }
        if (!str) {
            ythrow yexception() << "Empty string is passed to TRegExBaseImpl::Exec";
        }
        if ((eflags & REGEXP_GLOBAL) == 0) {
            return regexec(&Preg, str, nmatches, pmatch, eflags);
        } else {
            int options = 0;
            if ((eflags & REG_NOTBOL) != 0)
                options |= PCRE_NOTBOL;
            if ((eflags & REG_NOTEOL) != 0)
                options |= PCRE_NOTEOL;

            return TGlobalImpl(str, pmatch[0], options, (pcre*)Preg.re_pcre).ExecGlobal();
        }
    }

    bool IsCompiled() {
        return Preg.re_pcre;
    }

    ~TRegExBaseImpl() {
        regfree(&Preg);
    }

private:
    TString Error;
};

bool TRegExBase::IsCompiled() const {
    return Impl && Impl->IsCompiled();
}

TRegExBase::TRegExBase(const char* re, int cflags) {
    if (re) {
        Compile(re, cflags);
    }
}

TRegExBase::TRegExBase(const TString& re, int cflags) {
    Compile(re, cflags);
}

TRegExBase::~TRegExBase() {
}

void TRegExBase::Compile(const TString& re, int cflags) {
    Impl = new TRegExBaseImpl(re, cflags);
}

int TRegExBase::Exec(const char* str, regmatch_t pmatch[], int eflags, int nmatches) const {
    if (!Impl)
        ythrow yexception() << "!Regular expression is not compiled";
    return Impl->Exec(str, pmatch, eflags, nmatches);
}

int TRegExBase::GetCompileOptions() const {
    if (!Impl)
        ythrow yexception() << "!Regular expression is not compiled";
    return Impl->CompileOptions;
}

TString TRegExBase::GetRegExpr() const {
    if (!Impl)
        ythrow yexception() << "!Regular expression is not compiled";
    return Impl->RegExpr;
}

TRegExMatch::TRegExMatch(const char* re, int cflags)
    : TRegExBase(re, cflags)
{
}

TRegExMatch::TRegExMatch(const TString& re, int cflags)
    : TRegExBase(re, cflags)
{
}

bool TRegExMatch::Match(const char* str) const {
    return Exec(str, nullptr, 0, 0) == 0;
}

TRegExSubst::TRegExSubst(const char* re, int cflags)
    : TRegExBase(re, cflags)
    , Replacement(nullptr)
{
    memset(Brfs, 0, sizeof(TBackReferences) * NMATCHES);
}

TString TRegExSubst::Replace(const char* str, int eflags) {
    TString s;
    if (BrfsCount) {
        if (Exec(str, PMatch, eflags) == 0) {
            int i;
            for (i = 0; i < BrfsCount; i++) {
                s += TString(Replacement, Brfs[i].Beg, Brfs[i].End - Brfs[i].Beg);
                if (Brfs[i].Refer >= 0 && Brfs[i].Refer < NMATCHES)
                    s += TString(str, PMatch[Brfs[i].Refer].rm_so, int(PMatch[Brfs[i].Refer].rm_eo - PMatch[Brfs[i].Refer].rm_so));
            }
            s += TString(Replacement, Brfs[i].Beg, Brfs[i].End - Brfs[i].Beg);
        }
    } else {
        s = Replacement;
    }
    return s;
}

//***
// ��� ������������ ������ aaa.$1.$$$$.$2.bbb.$$$ccc Brfs ����� �����:
// {beg = 0,  end = 4,  Refer =  1} => "aaa." + $1_match
// {beg = 6,  end = 8,  Refer = -1} => ".$"
// {beg = 9,  end = 10, Refer = -1} => "$"
// {beg = 11, end = 12, Refer =  2} => "." + $2_match
// {beg = 14, end = 20, Refer = -1} => ".bbb.$"
// {beg = 21, end = 22, Refer = -1} => "$"
// {beg = 22, end = 25, Refer = -1} => "ccc"
// {beg = 0,  end = 0,  Refer =  0}
//***
int TRegExSubst::ParseReplacement(const char* repl) {
    Replacement = repl;
    if (!Replacement || *Replacement == 0)
        return 0;
    char* pos = (char*)Replacement;
    char* pos1 = nullptr;
    char* pos2 = nullptr;
    int i = 0;
    while (pos && *pos && i < NMATCHES) {
        pos1 = strchr(pos, '$');
        Brfs[i].Refer = -1;
        pos2 = pos1;
        if (pos1) {
            pos2 = pos1 + 1;
            while (IsAsciiDigit(*pos2))
                pos2++;
            if (pos2 > pos1 + 1) {
                Brfs[i].Refer = atol(TString(Replacement, pos1 + 1 - Replacement, pos2 - (pos1 + 1)).data());
            } else {
                pos1++;
                if (*pos2 == '$')
                    pos2++;
                Brfs[i].Refer = -1;
            }
        }
        Brfs[i].Beg = int(pos - (char*)Replacement);
        Brfs[i].End = (pos1 == nullptr ? (int)strlen(Replacement) : int(pos1 - Replacement));
        pos = pos2;
        i++;
    }
    Brfs[i].Beg = Brfs[i].End = 0;
    Brfs[i].Refer = -1;
    BrfsCount = i;
    return BrfsCount;
}
