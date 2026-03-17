// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.

#ifndef FST_SCRIPT_SHORTEST_PATH_H_
#define FST_SCRIPT_SHORTEST_PATH_H_

#include <cstdint>
#include <memory>
#include <vector>

#include <fst/shortest-path.h>
#include <fst/script/arcfilter-impl.h>
#include <fst/script/fst-class.h>
#include <fst/script/shortest-distance.h>
#include <fst/script/weight-class.h>

namespace fst {
namespace script {

// Slightly simplified interface: `has_distance` and `first_path` are disabled.

struct ShortestPathOptions : public ShortestDistanceOptions {
  const int32_t nshortest;
  const bool unique;
  const WeightClass &weight_threshold;
  const int64_t state_threshold;

  ShortestPathOptions(QueueType queue_type, int32_t nshortest, bool unique,
                      float delta, const WeightClass &weight_threshold,
                      int64_t state_threshold = kNoStateId)
      : ShortestDistanceOptions(queue_type, ArcFilterType::ANY, kNoStateId,
                                delta),
        nshortest(nshortest),
        unique(unique),
        weight_threshold(weight_threshold),
        state_threshold(state_threshold) {}
};

namespace internal {

// Code to implement switching on queue types.

template <class Arc, class Queue>
void ShortestPath(const Fst<Arc> &ifst, MutableFst<Arc> *ofst,
                  std::vector<typename Arc::Weight> *distance,
                  const ShortestPathOptions &opts) {
  using ArcFilter = AnyArcFilter<Arc>;
  using Weight = typename Arc::Weight;
  if constexpr (IsPath<Weight>::value) {
    const std::unique_ptr<Queue> queue(
        QueueConstructor<Arc, Queue, ArcFilter>::Construct(ifst, distance));
    const fst::ShortestPathOptions<Arc, Queue, ArcFilter> sopts(
        queue.get(), ArcFilter(), opts.nshortest, opts.unique,
        /* has_distance=*/false, opts.delta, /* first_path=*/false,
        *opts.weight_threshold.GetWeight<Weight>(), opts.state_threshold);
    ShortestPath(ifst, ofst, distance, sopts);
  } else {
    FSTERROR() << "ShortestPath: Weight needs to have the path property: "
               << Arc::Weight::Type();
    ofst->SetProperties(kError, kError);
  }
}

template <class Arc>
void ShortestPath(const Fst<Arc> &ifst, MutableFst<Arc> *ofst,
                  const ShortestPathOptions &opts) {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;
  std::vector<Weight> distance;
  switch (opts.queue_type) {
    case AUTO_QUEUE: {
      ShortestPath<Arc, AutoQueue<StateId>>(ifst, ofst, &distance, opts);
      return;
    }
    case FIFO_QUEUE: {
      ShortestPath<Arc, FifoQueue<StateId>>(ifst, ofst, &distance, opts);
      return;
    }
    case LIFO_QUEUE: {
      ShortestPath<Arc, LifoQueue<StateId>>(ifst, ofst, &distance, opts);
      return;
    }
    case SHORTEST_FIRST_QUEUE: {
      if constexpr (IsIdempotent<Weight>::value) {
        ShortestPath<Arc, NaturalShortestFirstQueue<StateId, Weight>>(
            ifst, ofst, &distance, opts);
      } else {
        FSTERROR() << "ShortestPath: Bad queue type SHORTEST_FIRST_QUEUE for"
                   << " non-idempotent Weight " << Weight::Type();
        ofst->SetProperties(kError, kError);
      }
      return;
    }
    case STATE_ORDER_QUEUE: {
      ShortestPath<Arc, StateOrderQueue<StateId>>(ifst, ofst, &distance, opts);
      return;
    }
    case TOP_ORDER_QUEUE: {
      ShortestPath<Arc, TopOrderQueue<StateId>>(ifst, ofst, &distance, opts);
      return;
    }
    default: {
      FSTERROR() << "ShortestPath: Unknown queue type: " << opts.queue_type;
      ofst->SetProperties(kError, kError);
      return;
    }
  }
}

}  // namespace internal

using FstShortestPathArgs = std::tuple<const FstClass &, MutableFstClass *,
                                       const ShortestPathOptions &>;

template <class Arc>
void ShortestPath(FstShortestPathArgs *args) {
  const Fst<Arc> &ifst = *std::get<0>(*args).GetFst<Arc>();
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  const ShortestPathOptions &opts = std::get<2>(*args);
  internal::ShortestPath(ifst, ofst, opts);
}

void ShortestPath(const FstClass &ifst, MutableFstClass *ofst,
                  const ShortestPathOptions &opts);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_SHORTEST_PATH_H_
