#include "highlighting.h"

namespace NSQLHighlight {

    bool IsCaseInsensitive(const THighlighting& highlighting) {
        return AnyOf(highlighting.Units, [](const TUnit& unit) {
            return AnyOf(unit.Patterns, [](const NSQLTranslationV1::TRegexPattern& p) {
                return p.IsCaseInsensitive;
            });
        });
    }

} // namespace NSQLHighlight
