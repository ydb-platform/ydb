#include "text.h"

namespace NKikimr {
namespace NText {

bool OutFlag(bool isFirst, bool isPresent, const char* name, TStringStream &stream) {
    if (isPresent) {
        if (!isFirst) {
            stream << " | " << name;
            return false;
        } else {
            stream  << name;
            return false;
        }
    } else {
        return isFirst;
    }
}

}  // namespace NText
}  // namespace NKikimr

