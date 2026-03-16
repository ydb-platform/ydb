#include <stan/lang/grammars/statement_2_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct statement_2_grammar<pos_iterator_t>;
  }
}
