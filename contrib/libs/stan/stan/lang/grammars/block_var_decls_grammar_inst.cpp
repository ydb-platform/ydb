#include <stan/lang/grammars/block_var_decls_grammar_def.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>

namespace stan {
  namespace lang {
    template struct block_var_decls_grammar<pos_iterator_t>;
  }
}
