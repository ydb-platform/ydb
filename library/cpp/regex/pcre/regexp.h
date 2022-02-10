#pragma once

#include <sys/types.h>

#include <util/system/defaults.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <contrib/libs/pcre/pcre.h>
#include <contrib/libs/pcre/pcreposix.h>

//THIS CODE LOOKS LIKE A TRASH, BUT WORKS.

#define NMATCHES 100
#define REGEXP_GLOBAL 0x0080 // use this if you want to find all occurences

class TRegExBaseImpl;

class TRegExBase {
protected:
    TSimpleIntrusivePtr<TRegExBaseImpl> Impl;

public:
    TRegExBase(const char* regExpr = nullptr, int cflags = REG_EXTENDED);
    TRegExBase(const TString& regExpr, int cflags = REG_EXTENDED);

    virtual ~TRegExBase();

    int Exec(const char* str, regmatch_t pmatch[], int eflags, int nmatches = NMATCHES) const;
    void Compile(const TString& regExpr, int cflags = REG_EXTENDED);
    bool IsCompiled() const;
    int GetCompileOptions() const;
    TString GetRegExpr() const;
};

class TRegExMatch: public TRegExBase {
public:
    TRegExMatch(const char* regExpr = nullptr, int cflags = REG_NOSUB | REG_EXTENDED);
    TRegExMatch(const TString& regExpr, int cflags = REG_NOSUB | REG_EXTENDED);

    bool Match(const char* str) const;
};

struct TBackReferences {
    int Beg;
    int End;
    int Refer;
};

class TRegExSubst: public TRegExBase {
private:
    const char* Replacement;
    regmatch_t PMatch[NMATCHES];

    TBackReferences Brfs[NMATCHES];
    int BrfsCount;

public:
    TRegExSubst(const char* regExpr = nullptr, int cflags = REG_EXTENDED);

    TString Replace(const char* str, int eflags = 0);
    int ParseReplacement(const char* replacement);
};
