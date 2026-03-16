#include <stan/lang/grammars/expression_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct expression_grammar<pos_iterator_t>;
  }
}
