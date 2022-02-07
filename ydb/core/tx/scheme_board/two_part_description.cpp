#include "two_part_description.h"

namespace NKikimr {
namespace NSchemeBoard {

bool TTwoPartDescription::Empty() const {
    return PreSerialized.empty() && !Record.ByteSizeLong();
}

TTwoPartDescription::operator bool() const {
    return !Empty();
}

} // NSchemeBoard
} // NKikimr
