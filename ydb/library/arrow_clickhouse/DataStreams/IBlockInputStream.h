// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <DataStreams/IBlockStream_fwd.h>

#include <atomic>
#include <shared_mutex>


namespace CH
{

/** The stream interface for reading data by blocks from the database.
  * Relational operations are supposed to be done also as implementations of this interface.
  * Watches out at how the source of the blocks works.
  * Lets you get information for profiling: rows per second, blocks per second, megabytes per second, etc.
  * Allows you to stop reading data (in nested sources).
  */
class IBlockInputStream
{
public:
    IBlockInputStream() {}
    virtual ~IBlockInputStream() {}

    IBlockInputStream(const IBlockInputStream &) = delete;
    IBlockInputStream & operator=(const IBlockInputStream &) = delete;

    /// To output the data stream transformation tree (query execution plan).
    virtual String getName() const = 0;

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * It is guaranteed that method "read" returns blocks of exactly that structure.
      */
    virtual Header getHeader() const = 0;

    /** Read next block.
      * If there are no more blocks, return an empty block (for which operator `bool` returns false).
      * NOTE: Only one thread can read from one instance of IBlockInputStream simultaneously.
      * This also applies for readPrefix, readSuffix.
      */
    Block read();

    /** Read something before starting all data or after the end of all data.
      * In the `readSuffix` function, you can implement a finalization that can lead to an exception.
      * readPrefix() must be called before the first call to read().
      * readSuffix() should be called after read() returns an empty block, or after a call to cancel(), but not during read() execution.
      */

    /** The default implementation calls readPrefixImpl() on itself, and then readPrefix() recursively for all children.
      * There are cases when you do not want `readPrefix` of children to be called synchronously, in this function,
      *  but you want them to be called, for example, in separate threads (for parallel initialization of children).
      * Then overload `readPrefix` function.
      */
    virtual void readPrefix();

    /** The default implementation calls recursively readSuffix() on all children, and then readSuffixImpl() on itself.
      * If this stream calls read() in children in a separate thread, this behavior is usually incorrect:
      * readSuffix() of the child can not be called at the moment when the same child's read() is executed in another thread.
      * In this case, you need to override this method so that readSuffix() in children is called, for example, after connecting streams.
      */
    virtual void readSuffix();

    /** Ask to abort the receipt of data as soon as possible.
      * By default - just sets the flag is_cancelled and asks that all children be interrupted.
      * This function can be called several times, including simultaneously from different threads.
      * Have two modes:
      *  with kill = false only is_cancelled is set - streams will stop silently with returning some processed data.
      *  with kill = true also is_killed set - queries will stop with exception.
      */
    virtual void cancel(bool kill = false);

    bool isCancelled() const;
    bool isCancelledOrThrowIfKilled() const;

protected:
    BlockInputStreams children;
    std::shared_mutex children_mutex;

    std::atomic<bool> is_cancelled{false};

    void addChild(const BlockInputStreamPtr & child)
    {
        std::unique_lock lock(children_mutex);
        children.push_back(child);
    }

private:
    /// Derived classes must implement this function.
    virtual Block readImpl() = 0;

    /// Here you can do a preliminary initialization.
    virtual void readPrefixImpl() {}

    /// Here you need to do a finalization, which can lead to an exception.
    virtual void readSuffixImpl() {}

    template <typename F>
    void forEachChild(F && f)
    {
        /// NOTE: Acquire a read lock, therefore f() should be thread safe
        std::shared_lock lock(children_mutex);

        // Reduce lock scope and avoid recursive locking since that is undefined for shared_mutex.
        const auto children_copy = children;
        lock.unlock();

        for (auto & child : children_copy)
            if (f(*child))
                return;
    }
};

}
