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
#ifndef _KNN_QUEUE_H_
#define _KNN_QUEUE_H_

#include <queue>
#include <vector>
#include <utility>
#include <limits>

#include "global.h"
#include "object.h"

namespace similarity {

template <typename dist_t>
class KNNQueue {
 public:
  KNNQueue(unsigned K) : K_(K) {}

  ~KNNQueue() {}

  void Reset() { QueueType empty; std::swap(empty, queue_); }

  bool Empty() const { return queue_.empty(); }

  size_t Size() const { return queue_.size(); }

  dist_t TopDistance() const {
    return queue_.empty()
        ? std::numeric_limits<dist_t>::max()
        : queue_.top().first;
  }

  const Object* TopObject() const { return queue_.top().second; }

  const Object* Pop() {
    const Object* top_object = TopObject();
    queue_.pop();
    return top_object;
  }

  void Push(const dist_t distance, const Object* object) {
    if (Size() < K_) {
      queue_.push(QueueElement(distance, object));
    } else {
      if (TopDistance() > distance) {
        Pop();
        queue_.push(QueueElement(distance, object));
      }
    }
  }

  KNNQueue* Clone() const {
    KNNQueue* clone = new KNNQueue(K_);
    clone->queue_ = queue_;
    return clone;
  }

 private:
  typedef std::pair<dist_t, const Object*> QueueElement;
  typedef std::priority_queue<QueueElement> QueueType;

  QueueType queue_;
  unsigned K_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(KNNQueue);
};

}   // namespace similarity

#endif   // _KNN_QUEUE_H_
