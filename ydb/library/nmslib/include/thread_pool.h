/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>
#include <exception>

namespace similarity {

  // See sample usage below
  template <class T>
  bool GetNextQueueObj(std::mutex &mtx, std::queue<T>& queue, T& obj) {
    std::unique_lock<std::mutex> lock(mtx);
    if (queue.empty()) {
      return false;
    }
    obj = queue.front();
    queue.pop();
    return true;
  }
/* 
   Sample usage of helper function GetNextQueueObj:
   
    queue<MSWNode*> toPatchQueue; // the job queue
    for (MSWNode* node : toPatchNodes) toPatchQueue.push(node);
    mutex mtx;
    vector<thread> threads;
    for (int i = 0; i < indexThreadQty_; ++i) {
      threads.push_back(thread(
          [&]() {
            MSWNode* node = nullptr;
            // get the next job from the queue
            while(GetNextQueueObj(mtx, toPatchQueue, node)) {
              node->removeGivenFriends(delNodesBitset);
            }
          }
      ));
    }
    // Don't forget to join!
    for (auto& thread : threads) thread.join();

*/

  /* 
   * replacement for the openmp '#pragma omp parallel for' directive
   * only handles a subset of functionality (no reductions etc)
   * Process ids from start (inclusive) to end (EXCLUSIVE)
   */
  template <class Function>
  inline void ParallelFor(size_t start, size_t end, size_t numThreads, Function fn) {
    if (numThreads <= 0) {
      numThreads = std::thread::hardware_concurrency();
    }

    if (numThreads == 1) {
      for (size_t id = start; id < end; id++) {
        fn(id, 0);
      }
    } else {
      std::vector<std::thread>  threads;
      std::atomic<size_t>       current(start);

      // keep track of exceptions in threads
      // https://stackoverflow.com/a/32428427/1713196
      std::exception_ptr lastException = nullptr;
      std::mutex         lastExceptMutex;

      for (size_t threadId = 0; threadId < numThreads; ++threadId) {
        threads.push_back(std::thread([&, threadId] {
          while (true) {
            size_t id = current.fetch_add(1);

            if ((id >= end)) {
              break;
            }

            try {
              fn(id, threadId);
            } catch (...) {
              std::unique_lock<std::mutex> lastExcepLock(lastExceptMutex);
              lastException = std::current_exception();
              /* 
               * This will work even when current is the largest value that
               * size_t can fit, because fetch_add returns the previous value
               * before the increment (what will result in overflow 
               * and produce 0 instead of current + 1).
               */
              current = end;
              break;
            }
          }
        }));
      }
      for (auto & thread : threads) {
        thread.join();
      }
      if (lastException) {
        std::rethrow_exception(lastException);
      }
    }


  }
};
