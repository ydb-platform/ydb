#pragma once

#include "defs.h"

namespace NKikimr::NPDisk {

class TLogReaderBase : public TThrRefBase {
public:
    virtual void Exec(ui64 offsetRead, TVector<ui64> &badOffsets, TActorSystem *actorSystem) = 0;
    virtual void NotifyError(ui64 offsetRead, TString& errorReason) = 0;
    virtual ~TLogReaderBase() {}
    virtual bool GetIsReplied() const = 0;
};

} // namespace NKikimr::NPDisk
