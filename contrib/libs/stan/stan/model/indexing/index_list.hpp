#ifndef STAN_MODEL_INDEXING_INDEX_LIST_HPP
#define STAN_MODEL_INDEXING_INDEX_LIST_HPP


namespace stan {
  namespace model {

    /**
     * Structure for an empty (size zero) index list.
     */
    struct nil_index_list {
    };


    /**
     * Template structure for an index list consisting of a head and
     * tail index.  
     *
     * @tparam H type of index stored as the head of the list.
     * @tparam T type of index list stored as the tail of the list.
     */
    template <typename H, typename T>
    struct cons_index_list {
      const H head_;
      const T tail_;

      /**
       * Construct a non-empty index list with the specified index for
       * a head and specified index list for a tail.
       *
       * @param head Index for head.
       * @param tail Index list for tail.
       */
      explicit cons_index_list(const H& head, const T& tail)
        : head_(head),
          tail_(tail) {
      }
    };

    // factory-like function does type inference for I and T
    template <typename I, typename T>
    inline cons_index_list<I, T>
    cons_list(const I& idx1, const T& t) {
      return cons_index_list<I, T>(idx1, t);
    }

    inline nil_index_list
    index_list() {
      return nil_index_list();
    }

    template <typename I>
    inline cons_index_list<I, nil_index_list>
    index_list(const I& idx) {
      return cons_list(idx, index_list());
    }

    template <typename I1, typename I2>
    inline cons_index_list<I1, cons_index_list<I2, nil_index_list> >
    index_list(const I1& idx1, const I2& idx2) {
      return cons_list(idx1, index_list(idx2));
    }

    template <typename I1, typename I2, typename I3>
    inline
    cons_index_list<I1,
                    cons_index_list<I2,
                                    cons_index_list<I3,
                                                    nil_index_list> > >
    index_list(const I1& idx1, const I2& idx2, const I3& idx3) {
      return cons_list(idx1, index_list(idx2, idx3));
    }

  }
}
#endif
