#include "generator_json.h"

#include "json.h"

#include <yql/essentials/sql/v1/highlight/sql_highlight_json.h>

namespace NSQLHighlight {

IGenerator::TPtr MakeJsonGenerator() {
    return MakeOnlyFileGenerator([](IOutputStream& out, const THighlighting& highlighting, bool /* ansi */) {
        Print(out, ToJson(highlighting));
    });
}

} // namespace NSQLHighlight
