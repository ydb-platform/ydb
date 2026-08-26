#pragma once

#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSqlWindow final: public TSqlTranslation {
public:
    explicit TSqlWindow(const TSqlTranslation& that);

    bool Build(const TRule_window_clause& node, TWinSpecs& winSpecs);
    bool Build(const TRule_window_definition& node, TWinSpecs& winSpecs);
};

} // namespace NSQLTranslationV1
