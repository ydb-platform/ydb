#include "abstract.h"
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NConveyor {
bool ITask::Execute() {
    bool result = false;
    try {
        result = DoExecute();
        if (!result) {
            if (!ErrorMessage) {
                ErrorMessage = "cannot execute task (not specified error message)";
            }
        }
    } catch (...) {
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
