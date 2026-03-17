//! Abstraction layer for dealing with timezone data on the file system
use crate::{
    py::*,
    tz::tzif::{self, TZif, is_valid_key},
};
use ahash::AHashMap;
use pyo3_ffi::*;
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    ptr::NonNull,
};

/// A manually reference-counted handle to a `TZif` object.
/// Since it's just a thin wrapper around a pointer, and
/// meant to be used in a single-threaded context, it's safe to share and copy
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct TzRef {
    inner: NonNull<Inner>,
}

struct Inner {
    value: TZif,
    refcnt: std::cell::UnsafeCell<usize>,
}

impl TzRef {
    /// Creates a new instance with a refcount of 1
    fn new(value: TZif) -> Self {
        let inner = Box::new(Inner {
            refcnt: std::cell::UnsafeCell::new(1),
            value,
        });
        Self {
            inner: NonNull::new(Box::into_raw(inner)).unwrap(),
        }
    }

    /// Increments the reference count.
    pub(crate) fn incref(&self) {
        unsafe {
            let refcnt = self.inner.as_ref().refcnt.get();
            *refcnt += 1;
        }
    }

    /// Decrement the reference count manually and return true if it drops to zero.
    #[inline]
    pub(crate) fn decref<'a, F>(self, get_cache: F)
    where
        // Passing the cache lazily ensures we only get it if we need it,
        // i.e. if the refcount drops to zero.
        F: FnOnce() -> &'a TzStore,
    {
        let refcnt = unsafe {
            let refcnt = self.inner.as_ref().refcnt.get();
            *refcnt -= 1;
            *refcnt
        };
        if refcnt == 0 {
            get_cache().clear_weakref(self);
        }
    }

    /// Gets the current reference count (for debugging purposes).
    #[allow(dead_code)]
    pub(crate) fn ref_count(&self) -> usize {
        unsafe { *self.inner.as_ref().refcnt.get() }
    }

    unsafe fn drop_in_place(&self) {
        // SAFETY: the pointer was allocated with Box::new,
        // and the caller should know when to drop it.
        drop(unsafe { Box::from_raw(self.inner.as_ptr()) });
    }
}

impl std::ops::Deref for TzRef {
    type Target = TZif;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.inner.as_ref().value }
    }
}

/// Access layer for timezone data
#[derive(Debug)]
pub(crate) struct TzStore {
    cache: Cache,
    // The paths to search for zoneinfo files.
    pub(crate) paths: Vec<PathBuf>,
    // The path to the tzdata package contents, if any.
    tzdata_path: Option<PathBuf>,
}

/// Timezone cache meant for single-threaded use.
/// It's designed to be used by the ZonedDateTime class,
/// which only calls it from a single thread while holding the GIL.
/// This avoids the need for synchronization.
/// It is based on the cache approach of zoneinfo in Python's standard library.
#[derive(Debug)]
struct Cache {
    inner: UnsafeCell<CacheInner>,
}

impl Cache {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(CacheInner {
                lru: VecDeque::with_capacity(LRU_CAPACITY),
                lookup: AHashMap::with_capacity(8), // a reasonable default size
            }),
        }
    }

    /// Get an entry from the cache, or try to load it from the supplied function
    /// and insert it into the cache.
    fn get_or_load<F>(&self, key: &str, load: F) -> Option<TzRef>
    where
        F: FnOnce() -> Option<TZif>,
    {
        // SAFETY: this is safe because we only access the cache from a single thread
        // while holding the GIL. The UnsafeCell is only used to allow mutable access
        // to the inner cache.
        let CacheInner { lookup, lru } = unsafe { self.inner.get().as_mut().unwrap() };

        match lookup.get(key) {
            // Found in cache. Mark it as recently used
            Some(&tz) => {
                Self::promote_lru(tz, lru, lookup);
                Some(tz)
            }
            // Not found in cache. Load it and insert it into the cache
            None => load().map(TzRef::new).inspect(|&tz| {
                Self::new_to_lru(tz, lru, lookup);
                lookup.insert(tz.key.clone(), tz);
            }),
        }
    }

    fn decref<F>(tz: TzRef, cleanup: F)
    where
        F: FnOnce(),
    {
        if unsafe {
            let refcnt = tz.inner.as_ref().refcnt.get();
            *refcnt -= 1;
            *refcnt
        } == 0
        {
            cleanup();
            // SAFETY: this is safe because we are the last strong reference
            // to the TZif object.
            unsafe {
                tz.drop_in_place();
            }
        }
    }

    pub(crate) fn clear_weakref(&self, tz: TzRef) {
        let lookup = &mut unsafe { self.inner.get().as_mut().unwrap() }.lookup;
        lookup.remove(&tz.key);
    }

    fn new_to_lru(tz: TzRef, lru: &mut Lru, lookup: &mut Lookup) {
        debug_assert!(!lru.contains(&tz));
        debug_assert!(tz.ref_count() > 0);
        // If the LRU exceeds capacity, remove the least recently used entry
        if lru.len() == LRU_CAPACITY {
            let least_used = lru.back().copied().unwrap();
            Self::decref(least_used, || {
                lookup.remove(&least_used.key);
            });
        }
        // Now add the new entry to the front
        lru.push_front(tz);
    }

    /// Register the given TZif was "used recently", moving it to the front of the LRU.
    fn promote_lru(tz: TzRef, lru: &mut Lru, lookup: &mut Lookup) {
        match lru.iter().position(|&ptr| ptr == tz) {
            Some(0) => {} // Already at the front
            Some(i) => {
                // Move it to the front. Note we don't need to increment the refcount,
                // since it's already in the LRU.
                lru.remove(i);
                lru.push_front(tz);
            }
            None => {
                tz.incref(); // LRU needs a strong refence
                Self::new_to_lru(tz, lru, lookup);
            }
        }
    }

    /// Clear the cache, dropping all entries.
    fn clear_all(&self) {
        let CacheInner { lookup, lru } = unsafe { self.inner.get().as_mut().unwrap() };

        // Clear all weak references. Note that strong references may still exist
        // both in the LRU and in ZonedDateTime objects.
        lookup.clear();

        // Clear the LRU
        let mut lru_old = std::mem::replace(lru, VecDeque::with_capacity(LRU_CAPACITY));
        for tz in lru_old.drain(..) {
            Self::decref(tz, || {
                // No cleanup needed: the lookup table is already cleared
            });
        }
    }

    /// Clear specific entries from the cache.
    fn clear_only(&self, keys: &[String]) {
        let CacheInner { lookup, lru } = unsafe { self.inner.get().as_mut().unwrap() };
        for k in keys {
            lookup.remove(k); // Always remove, regardless of refcount
            if let Some(i) = lru.iter().position(|tz| tz.key == *k) {
                Self::decref(lru.remove(i).unwrap(), || {
                    // No cleanup needed: the lookup table is already cleared
                });
            };
        }
    }
}

impl Drop for Cache {
    /// Drop the cache, clearing all entries. This should only trigger during module unloading,
    /// and there should be no ZonedDateTime objects left.
    fn drop(&mut self) {
        let CacheInner { lookup, lru } = unsafe { self.inner.get().as_mut().unwrap() };
        // At this point, the only strong references should be in the LRU.
        let mut lru = std::mem::take(lru);
        for tz in lru.drain(..) {
            Self::decref(tz, || {
                // Remove the weak reference too
                lookup.remove(&tz.key);
            });
        }
        // By now, the lookup table should be empty (it contains only weak references)
        debug_assert!(lookup.is_empty());
    }
}

type Lru = VecDeque<TzRef>;
type Lookup = AHashMap<String, TzRef>;

struct CacheInner {
    // Weak references to the `TZif` objects, keyed by TZ ID.
    // ZonedDateTime objects hold strong references to the `TZif` objects,
    // along with the cache's LRU.
    //
    // Choice of data structure:
    // "Ahash" works significantly faster than the standard hashing algorithm.
    // We don't need the cryptographic security of the standard algorithm,
    // since the keys are trusted (they are limited to valid zoneinfo keys).
    // Other alternatives that benchmarked *slower* are `BTreeMap`, gxhash, fxhash, and phf.
    //
    // Cleanup strategy:
    // Removal of 0-refcount entries is done by the `decref` method of the `TZRef` handle.
    lookup: Lookup,
    // Keeps the most recently used entries alive, to prevent over-eager dropping.
    //
    // For example, if constantly creating and dropping ZonedDateTimes
    // with a particular TZ ID, we don't want to keep reloading the same file.
    // Thus, we keep the most recently used entries in the cache.
    //
    // Choice of data structure:
    // A VecDeque is great for push/popping from both ends, and is simple to use,
    // although a Vec wasn't much slower in benchmarks.
    lru: Lru,
}

const LRU_CAPACITY: usize = 8; // this value seems to work well for Python's zoneinfo

impl TzStore {
    pub(crate) fn new() -> PyResult<Self> {
        Ok(Self {
            cache: Cache::new(),
            tzdata_path: get_tzdata_path()?,
            // Empty. The actual search paths are patched in at module import
            paths: Vec::with_capacity(4),
        })
    }

    /// Fetches a `TZif` for the given IANA time zone ID.
    /// If not already cached, reads the file from the filesystem.
    /// Returns a *borrowed* reference to the `TZif` object.
    /// Its reference count is *not* incremented.
    pub(crate) fn get(&self, key: &str, exc_notfound: PyObj) -> PyResult<TzRef> {
        self.cache
            .get_or_load(key, || self.load_tzif(key))
            .ok_or_else_raise(exc_notfound.as_ptr(), || {
                format!("No time zone found with key {key}")
            })
    }

    /// The `get` function, but accepts a Python Object as the key.
    pub(crate) fn obj_get(&self, tz_obj: PyObj, exc_notfound: PyObj) -> PyResult<TzRef> {
        self.get(
            tz_obj
                .cast::<PyStr>()
                .ok_or_type_err("tz must be a string")?
                .as_str()?,
            exc_notfound,
        )
    }

    /// Load a TZif file by key, assuming the key is untrusted input.
    fn load_tzif(&self, tzid: &str) -> Option<TZif> {
        if !is_valid_key(tzid) {
            return None;
        }
        self.load_tzif_from_tzpath(tzid)
            .or_else(|| self.load_tzif_from_tzdata(tzid))
    }

    /// Load a TZif from the TZPATH directory, assuming a benign TZ ID.
    fn load_tzif_from_tzpath(&self, tzid: &str) -> Option<TZif> {
        self.paths
            .iter()
            .find_map(|base| self.read_tzif_at_path(&base.join(tzid), tzid))
    }

    /// Load a TZif from the tzdata package, assuming a benign TZ ID.
    fn load_tzif_from_tzdata(&self, tzid: &str) -> Option<TZif> {
        self.tzdata_path
            .as_ref()
            .and_then(|base| self.read_tzif_at_path(&base.join(tzid), tzid))
    }

    /// Read a TZif file from the given path, returning None if it doesn't exist
    /// or otherwise cannot be read.
    fn read_tzif_at_path(&self, path: &Path, tzid: &str) -> Option<TZif> {
        if path.is_file() {
            fs::read(path).ok().and_then(|d| tzif::parse(&d, tzid).ok())
        } else {
            None
        }
    }

    pub(crate) fn clear_all(&self) {
        self.cache.clear_all();
    }

    pub(crate) fn clear_only(&self, keys: &[String]) {
        self.cache.clear_only(keys);
    }

    fn clear_weakref(&self, tz: TzRef) {
        self.cache.clear_weakref(tz);
    }
}

fn get_tzdata_path() -> PyResult<Option<PathBuf>> {
    Ok(Some(PathBuf::from({
        let __path__ = match import(c"tzdata.zoneinfo") {
            Ok(obj) => Ok(obj),
            _ if unsafe { PyErr_ExceptionMatches(PyExc_ImportError) } == 1 => {
                unsafe { PyErr_Clear() };
                return Ok(None);
            }
            e => e,
        }?
        .getattr(c"__path__")?;
        // __path__ is a list of paths. It will only have one element,
        // unless somebody is doing something strange.
        let py_str = __path__
            .getitem((0).to_py()?.borrow())?
            .cast::<PyStr>()
            .ok_or_type_err("tzdata module path must be a string")?;

        // SAFETY: Python guarantees that the string is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(py_str.as_utf8()?) }.to_owned()
    })))
}
