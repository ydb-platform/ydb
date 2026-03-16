#ifndef STAN_LANG_AST_PRINTABLE_HPP
#define STAN_LANG_AST_PRINTABLE_HPP

#include <boost/variant/recursive_variant.hpp>
#include <string>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * A printable object is either an expression or a string.
     */
    struct printable {
      /**
       * Variant type for member variable to store.
       */
      typedef boost::variant<boost::recursive_wrapper<std::string>,
                             boost::recursive_wrapper<expression> >
      printable_t;

      /**
       * Construct a printable object with an empty string.
       */
      printable();

      /**
       * Construct a printable object with the specified expression.
       *
       * @param expr expression to store
       */
      printable(const expression& expr);  // NOLINT(runtime/explicit)

      /**
       * Construct a printable object with the specified string.
       *
       * @param msg message to store
       */
      printable(const std::string& msg);  // NOLINT(runtime/explicit)

      /**
       * Construct a printable object with an object of its variant
       * type. 
       *
       * @param printable variant string or expression
       */
      printable(const printable_t& printable);  // NOLINT(runtime/explicit)

      /**
       * Copy constructor to construct printable from a printable.
       *
       * @param printable printable to copy
       */
      printable(const printable& printable);  // NOLINT(runtime/explicit)

      /**
       * The stored printable object.
       */
      printable_t printable_;
    };

  }
}
#endif
