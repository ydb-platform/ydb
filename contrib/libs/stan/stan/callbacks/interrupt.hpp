#ifndef STAN_CALLBACKS_INTERRUPT_HPP
#define STAN_CALLBACKS_INTERRUPT_HPP

namespace stan {
  namespace callbacks {

    /**
     * <code>interrupt</code> is a base class defining the interface
     * for Stan interrupt callbacks.
     *
     * The interrupt is called from within Stan algorithms to allow
     * for the interfaces to handle interrupt signals (ctrl-c).
     */
    class interrupt {
    public:
      /**
       * Callback function.
       *
       * This function is called by the algorithms allowing the interfaces
       * to break when necessary.
       */
      virtual void operator()() {
      }

      /**
       * Virtual destructor.
       */
      virtual ~interrupt() {}
    };

  }
}
#endif
