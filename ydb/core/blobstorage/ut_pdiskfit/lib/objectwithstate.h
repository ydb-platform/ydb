#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>
#include <util/generic/intrlist.h>

class TObjectWithState
    : public TIntrusiveListItem<TObjectWithState>
{
    TString Name;

public:
    TObjectWithState(TString name);
    virtual ~TObjectWithState();
    virtual TString SerializeState() = 0;

    // call register after full initialization of derived class
    void Register();

    TString GetState();

    static TString SerializeCommonState();
    static void DeserializeCommonState(const TString& data);

private:
    static TMutex Mutex;
    static TIntrusiveList<TObjectWithState> ObjectsWithState;
    static THashMap<TString, TString> States;
};
