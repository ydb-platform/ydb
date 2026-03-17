# distutils: language = c++
# distutils: libraries = leveldb

from libc.stdint cimport uint64_t
from libc.string cimport const_char
from libcpp cimport bool
from libcpp.string cimport string


cdef extern from "leveldb/db.h" namespace "leveldb":

    int kMajorVersion
    int kMinorVersion

    cdef cppclass Snapshot:
        pass

    cdef cppclass Range:
        Slice start
        Slice limit
        Range() nogil
        Range(Slice& s, Slice& l) nogil

    cdef cppclass DB:
        Status Put(WriteOptions& options, Slice& key, Slice& value) nogil
        Status Delete(WriteOptions& options, Slice& key) nogil
        Status Write(WriteOptions& options, WriteBatch* updates) nogil
        Status Get(ReadOptions& options, Slice& key, string* value) nogil
        Iterator* NewIterator(ReadOptions& options) nogil
        Snapshot* GetSnapshot() nogil
        void ReleaseSnapshot(Snapshot* snapshot) nogil
        bool GetProperty(Slice& property, string* value) nogil
        void GetApproximateSizes(Range* range, int n, uint64_t* sizes) nogil
        void CompactRange(Slice* begin, Slice* end) nogil

    # The DB::open() method is static, and hence not a member of the DB
    # class defined above
    Status DB_Open "leveldb::DB::Open"(Options& options, string& name, DB** dbptr) nogil

    cdef Status DestroyDB(string& name, Options& options) nogil
    cdef Status RepairDB(string& dbname, Options& options) nogil


cdef extern from "leveldb/status.h" namespace "leveldb":

    cdef cppclass Status:
        bool ok() nogil
        bool IsNotFound() nogil
        bool IsCorruption() nogil
        bool IsIOError() nogil
        string ToString() nogil


cdef extern from "leveldb/options.h" namespace "leveldb":

    cdef enum CompressionType:
        kNoCompression
        kSnappyCompression

    cdef cppclass Options:
        Comparator* comparator
        bool create_if_missing
        bool error_if_exists
        bool paranoid_checks
        # Env* env
        # Logger* info_log
        size_t write_buffer_size
        int max_open_files
        Cache* block_cache
        size_t block_size
        int block_restart_interval
        #size_t max_file_size
        CompressionType compression
        FilterPolicy* filter_policy
        Options() nogil

    cdef cppclass ReadOptions:
        bool verify_checksums
        bool fill_cache
        Snapshot* snapshot
        ReadOptions() nogil

    cdef cppclass WriteOptions:
        bool sync
        WriteOptions() nogil


cdef extern from "leveldb/slice.h" namespace "leveldb":

    cdef cppclass Slice:
        Slice() nogil
        Slice(const_char* d, size_t n) nogil
        Slice(string& s) nogil
        Slice(const_char* s) nogil
        const_char* data() nogil
        size_t size() nogil
        bool empty() nogil
        # char operator[](size_t n) nogil
        void clear() nogil
        void remove_prefix(size_t n) nogil
        string ToString() nogil
        int compare(Slice& b) nogil
        bool starts_with(Slice& x) nogil


cdef extern from "leveldb/write_batch.h" namespace "leveldb":

    cdef cppclass WriteBatch:
        WriteBatch() nogil
        void Put(Slice& key, Slice& value) nogil
        void Delete(Slice& key) nogil
        void Clear() nogil
        # Status Iterate(Handler* handler) const


cdef extern from "leveldb/iterator.h" namespace "leveldb":

    cdef cppclass Iterator:
        Iterator() nogil
        bool Valid() nogil
        void SeekToFirst() nogil
        void SeekToLast() nogil
        void Seek(Slice& target) nogil
        void Next() nogil
        void Prev() nogil
        Slice key() nogil
        Slice value() nogil
        Status status() nogil
        # void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);


cdef extern from "leveldb/comparator.h" namespace "leveldb":

    cdef cppclass Comparator:
        int Compare(Slice& a, Slice&b) nogil
        const_char* Name() nogil

    Comparator* BytewiseComparator() nogil


cdef extern from "leveldb/filter_policy.h" namespace "leveldb":

    cdef cppclass FilterPolicy:
        const_char* Name() nogil
        void CreateFilter(Slice* keys, int n, string* dst) nogil
        bool KeyMayMatch(Slice& key, Slice& filter) nogil

    FilterPolicy* NewBloomFilterPolicy(int bits_per_key) nogil


cdef extern from "leveldb/cache.h" namespace "leveldb":

    cdef cppclass Cache:
        # Treat as opaque structure
        pass

    Cache* NewLRUCache(size_t capacity) nogil
