#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

class ISimpleDb {
public:
    virtual void Erase(const TString &key) = 0;
    virtual void Update(const TString &key, const TString &value) = 0;
    virtual void AddTrash(const TLogoBlobID& id) = 0;
};

} // NKeyValue
} // NKikimr
