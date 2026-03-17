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

#ifndef FST_SCRIPT_SHORTEST_DISTANCE_H_
#define FST_SCRIPT_SHORTEST_DISTANCE_H_

#include <cstdint>
#include <tuple>
#include <vector>

#include <fst/queue.h>
#include <fst/shortest-distance.h>
#include <fst/script/arcfilter-impl.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>
#include <fst/script/prune.h>
#include <fst/script/script-impl.h>
#include <fst/script/weight-class.h>

namespace fst {
namespace script {

struct ShortestDistanceOptions {
  const QueueType queue_type;
  const ArcFilterType arc_filter_type;
  const int64_t source;
  const float delta;

  ShortestDistanceOptions(QueueType queue_type, ArcFilterType arc_filter_type,
                          int64_t source, float delta)
      : queue_type(queue_type),
        arc_filter_type(arc_filter_type),
        source(source),
        delta(delta) {}
};

namespace internal {

// Code to implement switching on queue and arc filter types.

template <class Arc, class Queue, class ArcFilter>
struct QueueConstructor {
  using Weight = typename Arc::Weight;

  static std::unique_ptr<Queue> Construct(const Fst<Arc> &,
                                          const std::vector<Weight> *) {
    return std::make_unique<Queue>();
  }
};

// Specializations to support queues with different constructors.

template <class Arc, class ArcFilter>
struct QueueConstructor<Arc, AutoQueue<typename Arc::StateId>, ArcFilter> {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  //  template<class Arc, class ArcFilter>
  static std::unique_ptr<AutoQueue<StateId>> Construct(
      const Fst<Arc> &fst, const std::vector<Weight> *distance) {
    return std::make_unique<AutoQueue<StateId>>(fst, distance, ArcFilter());
  }
};

template <class Arc, class ArcFilter>
struct QueueConstructor<
    Arc, NaturalShortestFirstQueue<typename Arc::StateId, typename Arc::Weight>,
    ArcFilter> {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  static std::unique_ptr<NaturalShortestFirstQueue<StateId, Weight>> Construct(
      const Fst<Arc> &, const std::vector<Weight> *distance) {
    return std::make_unique<NaturalShortestFirstQueue<StateId, Weight>>(
        *distance);
  }
};

template <class Arc, class ArcFilter>
struct QueueConstructor<Arc, TopOrderQueue<typename Arc::StateId>, ArcFilter> {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  static std::unique_ptr<TopOrderQueue<StateId>> Construct(
      const Fst<Arc> &fst, const std::vector<Weight> *) {
    return std::make_unique<TopOrderQueue<StateId>>(fst, ArcFilter());
  }
};

template <class Arc, class Queue, class ArcFilter>
void ShortestDistance(const Fst<Arc> &fst,
                      std::vector<typename Arc::Weight> *distance,
                      const ShortestDistanceOptions &opts) {
  std::unique_ptr<Queue> queue(
      QueueConstructor<Arc, Queue, ArcFilter>::Construct(fst, distance));
  const fst::ShortestDistanceOptions<Arc, Queue, ArcFilter> sopts(
      queue.get(), ArcFilter(), opts.source, opts.delta);
  ShortestDistance(fst, distance, sopts);
}

template <class Arc, class Queue>
void ShortestDistance(const Fst<Arc> &fst,
                      std::vector<typename Arc::Weight> *distance,
                      const ShortestDistanceOptions &opts) {
  switch (opts.arc_filter_type) {
    case ArcFilterType::ANY: {
      ShortestDistance<Arc, Queue, AnyArcFilter<Arc>>(fst, distance, opts);
      return;
    }
    case ArcFilterType::EPSILON: {
      ShortestDistance<Arc, Queue, EpsilonArcFilter<Arc>>(fst, distance, opts);
      return;
    }
    case ArcFilterType::INPUT_EPSILON: {
      ShortestDistance<Arc, Queue, InputEpsilonArcFilter<Arc>>(fst, distance,
                                                               opts);
      return;
    }
    case ArcFilterType::OUTPUT_EPSILON: {
      ShortestDistance<Arc, Queue, OutputEpsilonArcFilter<Arc>>(fst, distance,
                                                                opts);
      return;
    }
    default: {
      FSTERROR() << "ShortestDistance: Unknown arc filter type: "
                 << static_cast<std::underlying_type_t<ArcFilterType>>(
                        opts.arc_filter_type);
      distance->clear();
      distance->resize(1, Arc::Weight::NoWeight());
      return;
    }
  }
}

}  // namespace internal

using FstShortestDistanceArgs1 =
    std::tuple<const FstClass &, std::vector<WeightClass> *,
               const ShortestDistanceOptions &>;

template <class Arc>
void ShortestDistance(FstShortestDistanceArgs1 *args) {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;
  const Fst<Arc> &fst = *std::get<0>(*args).GetFst<Arc>();
  const auto &opts = std::get<2>(*args);
  std::vector<Weight> typed_distance;
  switch (opts.queue_type) {
    case AUTO_QUEUE: {
      internal::ShortestDistance<Arc, AutoQueue<StateId>>(fst, &typed_distance,
                                                          opts);
      break;
    }
    case FIFO_QUEUE: {
      internal::ShortestDistance<Arc, FifoQueue<StateId>>(fst, &typed_distance,
                                                          opts);
      break;
    }
    case LIFO_QUEUE: {
      internal::ShortestDistance<Arc, LifoQueue<StateId>>(fst, &typed_distance,
                                                          opts);
      break;
    }
    case SHORTEST_FIRST_QUEUE: {
      if constexpr (IsIdempotent<Weight>::value) {
        internal::ShortestDistance<Arc,
                                   NaturalShortestFirstQueue<StateId, Weight>>(
            fst, &typed_distance, opts);
      } else {
        FSTERROR() << "ShortestDistance: Bad queue type SHORTEST_FIRST_QUEUE"
                   << " for non-idempotent Weight " << Weight::Type();
      }
      break;
    }
    case STATE_ORDER_QUEUE: {
      internal::ShortestDistance<Arc, StateOrderQueue<StateId>>(
          fst, &typed_distance, opts);
      break;
    }
    case TOP_ORDER_QUEUE: {
      internal::ShortestDistance<Arc, TopOrderQueue<StateId>>(
          fst, &typed_distance, opts);
      break;
    }
    default: {
      FSTERROR() << "ShortestDistance: Unknown queue type: " << opts.queue_type;
      typed_distance.clear();
      typed_distance.resize(1, Arc::Weight::NoWeight());
      break;
    }
  }
  internal::CopyWeights(typed_distance, std::get<1>(*args));
}

using FstShortestDistanceArgs2 =
    std::tuple<const FstClass &, std::vector<WeightClass> *, bool, double>;

template <class Arc>
void ShortestDistance(FstShortestDistanceArgs2 *args) {
  using Weight = typename Arc::Weight;
  const Fst<Arc> &fst = *std::get<0>(*args).GetFst<Arc>();
  std::vector<Weight> typed_distance;
  ShortestDistance(fst, &typed_distance, std::get<2>(*args),
                   std::get<3>(*args));
  internal::CopyWeights(typed_distance, std::get<1>(*args));
}

using FstShortestDistanceInnerArgs3 = std::tuple<const FstClass &, double>;

using FstShortestDistanceArgs3 =
    WithReturnValue<WeightClass, FstShortestDistanceInnerArgs3>;

template <class Arc>
void ShortestDistance(FstShortestDistanceArgs3 *args) {
  const Fst<Arc> &fst = *std::get<0>(args->args).GetFst<Arc>();
  args->retval = WeightClass(ShortestDistance(fst, std::get<1>(args->args)));
}

void ShortestDistance(const FstClass &fst, std::vector<WeightClass> *distance,
                      const ShortestDistanceOptions &opts);

void ShortestDistance(const FstClass &ifst, std::vector<WeightClass> *distance,
                      bool reverse = false,
                      double delta = fst::kShortestDelta);

WeightClass ShortestDistance(const FstClass &ifst,
                             double delta = fst::kShortestDelta);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_SHORTEST_DISTANCE_H_
