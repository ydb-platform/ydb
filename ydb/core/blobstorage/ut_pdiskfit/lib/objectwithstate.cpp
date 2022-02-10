#include "objectwithstate.h"
#include <ydb/core/protos/pdiskfit.pb.h>

TMutex TObjectWithState::Mutex;
TIntrusiveList<TObjectWithState> TObjectWithState::ObjectsWithState;
THashMap<TString, TString> TObjectWithState::States;

TObjectWithState::TObjectWithState(TString name)
    : Name(std::move(name))
{}

TObjectWithState::~TObjectWithState() {
    TGuard<TMutex> lock(Mutex);
    TIntrusiveListItem<TObjectWithState>::Unlink();
}

void TObjectWithState::Register() {
    TGuard<TMutex> lock(Mutex);
    ObjectsWithState.PushBack(this);
}

TString TObjectWithState::SerializeCommonState() {
    TGuard<TMutex> lock(Mutex);
    for (TObjectWithState& object : ObjectsWithState) {
        States[object.Name] = object.SerializeState();
    }
    NPDiskFIT::TObjectWithStateDict dict;
    for (const auto& kv : States) {
        auto *item = dict.AddItems();
        item->SetKey(kv.first);
        item->SetValue(kv.second);

    }
    TString data;
    bool status = dict.SerializeToString(&data);
    Y_VERIFY(status);
    return data;
}

void TObjectWithState::DeserializeCommonState(const TString& data) {
    NPDiskFIT::TObjectWithStateDict dict;
    bool status = dict.ParseFromString(data);
    Y_VERIFY(status);
    for (const auto& item : dict.GetItems()) {
        Y_VERIFY(item.HasKey());
        Y_VERIFY(item.HasValue());
        States.emplace(item.GetKey(), item.GetValue());
    }
}

TString TObjectWithState::GetState() {
    auto it = States.find(Name);
    return it != States.end() ? it->second : TString();
}
