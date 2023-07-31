#include "abstract.h"
#include <library/cpp/actors/core/monotonic.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NConveyor {
bool ITask::Execute(std::shared_ptr<TTaskSignals> signals) {
    bool result = false;
    const TMonotonic start = TMonotonic::Now();
    try {
        result = DoExecute();
        if (!result) {
            if (signals) {
                signals->Fails->Add(1);
                signals->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
            if (!ErrorMessage) {
                ErrorMessage = "cannot execute task (not specified error message)";
            }
        } else {
            if (signals) {
                signals->Success->Add(1);
                signals->SuccessDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
        }
    } catch (...) {
        if (signals) {
            signals->Fails->Add(1);
            signals->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
        }
        TStringBuilder sbLocalMessage;
        sbLocalMessage << "exception: " << CurrentExceptionMessage();
        if (!ErrorMessage) {
            ErrorMessage = sbLocalMessage;
        } else {
            ErrorMessage += sbLocalMessage;
        }
    }
    return result;
}

}
