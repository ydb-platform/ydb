//! `UnionFind<K>` is a disjoint-set data structure.

use super::graph::IndexType;
use alloc::{collections::TryReserveError, vec, vec::Vec};
use core::cmp::Ordering;

/// `UnionFind<K>` is a disjoint-set data structure. It tracks set membership of *n* elements
/// indexed from *0* to *n - 1*. The scalar type is `K` which must be an unsigned integer type.
///
/// <http://en.wikipedia.org/wiki/Disjoint-set_data_structure>
///
/// Too awesome not to quote:
///
/// “The amortized time per operation is **O(α(n))** where **α(n)** is the
/// inverse of **f(x) = A(x, x)** with **A** being the extremely fast-growing Ackermann function.”
#[derive(Debug, Clone)]
pub struct UnionFind<K> {
    // For element at index *i*, store the index of its parent; the representative itself
    // stores its own index. This forms equivalence classes which are the disjoint sets, each
    // with a unique representative.
    parent: Vec<K>,
    // It is a balancing tree structure,
    // so the ranks are logarithmic in the size of the container -- a byte is more than enough.
    //
    // Rank is separated out both to save space and to save cache in when searching in the parent
    // vector.
    rank: Vec<u8>,
}

impl<K> Default for UnionFind<K> {
    fn default() -> Self {
        Self {
            parent: Vec::new(),
            rank: Vec::new(),
        }
    }
}

#[inline]
unsafe fn get_unchecked<K>(xs: &[K], index: usize) -> &K {
    debug_assert!(index < xs.len());
    xs.get_unchecked(index)
}

#[inline]
unsafe fn get_unchecked_mut<K>(xs: &mut [K], index: usize) -> &mut K {
    debug_assert!(index < xs.len());
    xs.get_unchecked_mut(index)
}

impl<K> UnionFind<K>
where
    K: IndexType,
{
    /// Create a new `UnionFind` of `n` disjoint sets.
    pub fn new(n: usize) -> Self {
        let rank = vec![0; n];
        let parent = (0..n).map(K::new).collect::<Vec<K>>();

        UnionFind { parent, rank }
    }

    /// Create a new `UnionFind` with no elements.
    pub const fn new_empty() -> Self {
        Self {
            parent: Vec::new(),
            rank: Vec::new(),
        }
    }

    /// Returns the number of elements in the union-find data-structure.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Returns true if there are no elements in the union-find data-structure.
    pub fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }

    /// Adds a new disjoint set and returns the index of the new set.
    ///
    /// The new disjoint set is always added to the end, so the returned
    /// index is the same as the number of elements before calling this function.
    ///
    /// **Time Complexity**
    /// Takes amortized O(1) time.
    pub fn new_set(&mut self) -> K {
        let retval = K::new(self.parent.len());
        self.rank.push(0);
        self.parent.push(retval);
        retval
    }

    /// Return the representative for `x`.
    ///
    /// **Panics** if `x` is out of bounds.
    #[track_caller]
    pub fn find(&self, x: K) -> K {
        self.try_find(x).expect("The index is out of bounds")
    }

    /// Return the representative for `x` or `None` if `x` is out of bounds.
    pub fn try_find(&self, mut x: K) -> Option<K> {
        if x.index() >= self.len() {
            return None;
        }

        loop {
            // Use unchecked indexing because we can trust the internal set ids.
            let xparent = unsafe { *get_unchecked(&self.parent, x.index()) };
            if xparent == x {
                break;
            }
            x = xparent;
        }

        Some(x)
    }

    /// Return the representative for `x`.
    ///
    /// Write back the found representative, flattening the internal
    /// datastructure in the process and quicken future lookups.
    ///
    /// **Panics** if `x` is out of bounds.
    #[track_caller]
    pub fn find_mut(&mut self, x: K) -> K {
        assert!(x.index() < self.len());
        unsafe { self.find_mut_recursive(x) }
    }

    /// Return the representative for `x` or `None` if `x` is out of bounds.
    ///
    /// Write back the found representative, flattening the internal
    /// datastructure in the process and quicken future lookups.
    pub fn try_find_mut(&mut self, x: K) -> Option<K> {
        if x.index() >= self.len() {
            return None;
        }
        Some(unsafe { self.find_mut_recursive(x) })
    }

    unsafe fn find_mut_recursive(&mut self, mut x: K) -> K {
        let mut parent = *get_unchecked(&self.parent, x.index());
        while parent != x {
            let grandparent = *get_unchecked(&self.parent, parent.index());
            *get_unchecked_mut(&mut self.parent, x.index()) = grandparent;
            x = parent;
            parent = grandparent;
        }
        x
    }

    /// Returns `true` if the given elements belong to the same set, and returns
    /// `false` otherwise.
    ///
    /// **Panics** if `x` or `y` is out of bounds.
    #[track_caller]
    pub fn equiv(&self, x: K, y: K) -> bool {
        self.find(x) == self.find(y)
    }

    /// Returns `Ok(true)` if the given elements belong to the same set, and returns
    /// `Ok(false)` otherwise.
    ///
    /// If `x` or `y` are out of bounds, it returns `Err` with the first bad index found.
    pub fn try_equiv(&self, x: K, y: K) -> Result<bool, K> {
        let xrep = self.try_find(x).ok_or(x)?;
        let yrep = self.try_find(y).ok_or(y)?;
        Ok(xrep == yrep)
    }

    /// Unify the two sets containing `x` and `y`.
    ///
    /// Return `false` if the sets were already the same, `true` if they were unified.
    ///
    /// **Panics** if `x` or `y` is out of bounds.
    #[track_caller]
    pub fn union(&mut self, x: K, y: K) -> bool {
        self.try_union(x, y).unwrap()
    }

    /// Unify the two sets containing `x` and `y`.
    ///
    /// Return `Ok(false)` if the sets were already the same, `Ok(true)` if they were unified.
    ///
    /// If `x` or `y` are out of bounds, it returns `Err` with first found bad index.
    /// But if `x == y`, the result will be `Ok(false)` even if the indexes go out of bounds.
    pub fn try_union(&mut self, x: K, y: K) -> Result<bool, K> {
        if x == y {
            return Ok(false);
        }
        let xrep = self.try_find_mut(x).ok_or(x)?;
        let yrep = self.try_find_mut(y).ok_or(y)?;

        if xrep == yrep {
            return Ok(false);
        }

        let xrepu = xrep.index();
        let yrepu = yrep.index();
        let xrank = self.rank[xrepu];
        let yrank = self.rank[yrepu];

        // The rank corresponds roughly to the depth of the treeset, so put the
        // smaller set below the larger
        match xrank.cmp(&yrank) {
            Ordering::Less => self.parent[xrepu] = yrep,
            Ordering::Greater => self.parent[yrepu] = xrep,
            Ordering::Equal => {
                self.parent[yrepu] = xrep;
                self.rank[xrepu] += 1;
            }
        }
        Ok(true)
    }

    /// Return a vector mapping each element to its representative.
    pub fn into_labeling(mut self) -> Vec<K> {
        // write in the labeling of each element
        unsafe {
            for ix in 0..self.len() {
                let k = *get_unchecked(&self.parent, ix);
                let xrep = self.find_mut_recursive(k);
                *self.parent.get_unchecked_mut(ix) = xrep;
            }
        }
        self.parent
    }
}

impl<K> UnionFind<K> {
    /// Constructs a new, empty `UnionFind<K>` with at least the specified capacity.
    ///
    /// This acts similarly to [`Vec::with_capacity`].
    pub fn with_capacity(capacity: usize) -> Self {
        UnionFind {
            parent: Vec::with_capacity(capacity),
            rank: Vec::with_capacity(capacity),
        }
    }

    /// Returns the total number of elements the `UnionFind` can hold without reallocating.
    ///
    /// # Examples
    ///
    /// ```
    /// use petgraph::unionfind::UnionFind;
    ///
    /// let mut uf: UnionFind<u32> = UnionFind::with_capacity(10);
    /// uf.new_set();
    /// assert!(uf.capacity() >= 10);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.parent.capacity().min(self.rank.capacity())
    }

    /// Reserves capacity for at least `additional` more elements to be inserted
    /// in the given `UnionFind<K>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_.
    ///
    /// # Examples
    ///
    /// ```
    /// use petgraph::unionfind::UnionFind;
    ///
    /// let mut uf: UnionFind<u32> = UnionFind::new(3);
    /// uf.reserve(10);
    /// assert!(uf.capacity() >= 13);
    /// ```
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.parent.reserve(additional);
        self.rank.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more elements to
    /// be inserted in the given `UnionFind<K>`. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: UnionFind::reserve
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_.
    ///
    /// # Examples
    ///
    /// ```
    /// use petgraph::unionfind::UnionFind;
    ///
    /// let mut uf: UnionFind<u32> =  UnionFind::new_empty();
    /// uf.reserve_exact(10);
    /// assert!(uf.capacity() >= 10);
    /// ```
    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.parent.reserve_exact(additional);
        self.rank.reserve_exact(additional);
    }

    /// Tries to reserve capacity for at least `additional` more elements to be inserted
    /// in the given `UnionFind<K>`. The collection may reserve more space to speculatively avoid
    /// frequent reallocations. After calling `try_reserve`, capacity will be
    /// greater than or equal to `self.len() + additional` if it returns
    /// `Ok(())`. Does nothing if capacity is already sufficient. This method
    /// preserves the contents even if an error occurs.
    ///
    /// # Errors
    ///
    /// If the capacity overflows, or the allocator reports a failure, then an error
    /// is returned.
    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.parent
            .try_reserve(additional)
            .and_then(|_| self.rank.try_reserve(additional))
    }

    /// Tries to reserve the minimum capacity for at least `additional`
    /// elements to be inserted in the given `UnionFind<K>`. Unlike [`try_reserve`],
    /// this will not deliberately over-allocate to speculatively avoid frequent
    /// allocations. After calling `try_reserve_exact`, capacity will be greater
    /// than or equal to `self.len() + additional` if it returns `Ok(())`.
    /// Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`try_reserve`] if future insertions are expected.
    ///
    /// [`try_reserve`]: UnionFind::try_reserve
    ///
    /// # Errors
    ///
    /// If the capacity overflows, or the allocator reports a failure, then an error
    /// is returned.
    #[inline]
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.parent
            .try_reserve_exact(additional)
            .and_then(|_| self.rank.try_reserve_exact(additional))
    }

    /// Shrinks the capacity of the `UnionFind` as much as possible.
    ///
    /// The behavior of this method depends on the allocator, which may either shrink the
    /// collection in-place or reallocate. The resulting `UnionFind` might still have some excess capacity, just as
    /// is the case for [`with_capacity`]. See [`Vec::shrink_to_fit`] for more details, since the implementation is based on this method.
    ///
    /// [`with_capacity`]: UnionFind::with_capacity
    ///
    /// # Examples
    ///
    /// ```
    /// use petgraph::unionfind::UnionFind;
    ///
    /// let mut uf: UnionFind<u32> = UnionFind::with_capacity(10);
    ///
    /// for _ in 0..3 {
    ///     uf.new_set();
    /// }
    ///
    /// assert!(uf.capacity() >= 10);
    /// uf.shrink_to_fit();
    /// assert!(uf.capacity() >= 3);
    /// ```
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.parent.shrink_to_fit();
        self.rank.shrink_to_fit();
    }

    /// Shrinks the capacity of the `UnionFind` with a lower bound.
    ///
    /// The capacity will remain at least as large as both the length
    /// and the supplied value.
    ///
    /// If the current capacity is less than the lower limit, this is a no-op.
    ///
    /// # Examples
    ///
    /// ```
    /// use petgraph::unionfind::UnionFind;
    ///
    /// let mut uf: UnionFind<u32> = UnionFind::with_capacity(10);
    ///
    /// for _ in 0..3 {
    ///     uf.new_set();
    /// }
    ///
    /// assert!(uf.capacity() >= 10);
    /// uf.shrink_to(4);
    /// assert!(uf.capacity() >= 4);
    /// uf.shrink_to(0);
    /// assert!(uf.capacity() >= 3);
    /// ```
    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.parent.shrink_to(min_capacity);
        self.rank.shrink_to(min_capacity);
    }
}
