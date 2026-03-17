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
//
// This file defines the registration mechanism for new operations.
// These operations are designed to enable scripts to work with FST classes
// at a high level.
//
// If you have a new arc type and want these operations to work with FSTs
// with that arc type, see below for the registration steps
// you must take.
//
// These methods are only recommended for use in high-level scripting
// applications. Most users should use the lower-level templated versions
// corresponding to these.
//
// If you have a new arc type you'd like these operations to work with,
// use the REGISTER_FST_OPERATIONS macro defined in fstscript.h.
//
// If you have a custom operation you'd like to define, you need four
// components. In the following, assume you want to create a new operation
// with the signature
//
//    void Foo(const FstClass &ifst, MutableFstClass *ofst);
//
//  You need:
//
//  1) A way to bundle the args that your new Foo operation will take, as
//     a single struct. The template structs in arg-packs.h provide a handy
//     way to do this. In Foo's case, that might look like this:
//
//       using FooArgs = std::pair<const FstClass &, MutableFstClass *>;
//
//     Note: this package of args is going to be passed by non-const pointer.
//
//  2) A function template that is able to perform Foo, given the args and
//     arc type. Yours might look like this:
//
//       template<class Arc>
//       void Foo(FooArgs *args) {
//          // Pulls out the actual, arc-templated FSTs.
//          const Fst<Arc> &ifst = std::get<0>(*args).GetFst<Arc>();
//          MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
//          // Actually perform Foo on ifst and ofst.
//       }
//
//  3) a client-facing function for your operation. This would look like
//     the following:
//
//     void Foo(const FstClass &ifst, MutableFstClass *ofst) {
//       // Check that the arc types of the FSTs match
//       if (!ArcTypesMatch(ifst, *ofst, "Foo")) return;
//       // package the args
//       FooArgs args(ifst, ofst);
//       // Finally, call the operation
//       Apply<Operation<FooArgs>>("Foo", ifst->ArcType(), &args);
//     }
//
//  The Apply<> function template takes care of the link between 2 and 3,
//  provided you also have:
//
//  4) A registration for your new operation, on the arc types you care about.
//     This can be provided easily by the REGISTER_FST_OPERATION macro:
//
//       REGISTER_FST_OPERATION(Foo, StdArc, FooArgs);
//       REGISTER_FST_OPERATION(Foo, MyArc, FooArgs);
//       // .. etc
//
//     You can also use REGISTER_FST_OPERATION_3ARCS macro to register an
//     operation for StdArc, LogArc, and Log64Arc:
//
//       REGISTER_FST_OPERATION_3ARCS(Foo, FooArcs);
//
//  That's it! Now when you call Foo(const FstClass &, MutableFstClass *),
//  it dispatches (in #3) via the Apply<> function to the correct
//  instantiation of the template function in #2.
//

#ifndef FST_SCRIPT_SCRIPT_IMPL_H_
#define FST_SCRIPT_SCRIPT_IMPL_H_

// This file contains general-purpose templates which are used in the
// implementation of the operations.

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fst/log.h>
#include <fst/generic-register.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

enum class RandArcSelection : uint8_t { UNIFORM, LOG_PROB, FAST_LOG_PROB };

// A generic register for operations with various kinds of signatures.
// Needed since every function signature requires a new registration class.
// The std::pair<std::string, std::string> is understood to be the operation
// name and arc type; subclasses (or typedefs) need only provide the operation
// signature.
template <class OperationSignature>
class GenericOperationRegister
    : public GenericRegister<std::pair<std::string, std::string>,
                             OperationSignature,
                             GenericOperationRegister<OperationSignature>> {
 public:
  OperationSignature GetOperation(const std::string &operation_name,
                                  const std::string &arc_type) {
    return this->GetEntry(std::make_pair(operation_name, arc_type));
  }

 protected:
  std::string ConvertKeyToSoFilename(
      const std::pair<std::string, std::string> &key) const final {
    // Uses the old-style FST for now.
    std::string legal_type(key.second);  // The arc type.
    ConvertToLegalCSymbol(&legal_type);
    legal_type.append("-arc.so");
    return legal_type;
  }
};

// Operation package: everything you need to register a new type of operation.
// The ArgPack should be the type that's passed into each wrapped function;
// for instance, it might be a struct containing all the args. It's always
// passed by pointer, so const members should be used to enforce constness where
// it's needed. Return values should be implemented as a member of ArgPack as
// well.

template <class Args>
struct Operation {
  using ArgPack = Args;

  using OpType = void (*)(ArgPack *args);

  // The register (hash) type.
  using Register = GenericOperationRegister<OpType>;

  // The register-er type.
  using Registerer = GenericRegisterer<Register>;
};

// Macro for registering new types of operations.
#define REGISTER_FST_OPERATION(Op, Arc, ArgPack)               \
  static fst::script::Operation<ArgPack>::Registerer       \
      arc_dispatched_operation_##ArgPack##Op##Arc##_registerer \
      ({#Op, Arc::Type()}, Op<Arc>)

// A macro that calls REGISTER_FST_OPERATION for widely-used arc types.
#define REGISTER_FST_OPERATION_3ARCS(Op, ArgPack) \
  REGISTER_FST_OPERATION(Op, StdArc, ArgPack);    \
  REGISTER_FST_OPERATION(Op, LogArc, ArgPack);    \
  REGISTER_FST_OPERATION(Op, Log64Arc, ArgPack)

// Template function to apply an operation by name.
template <class OpReg>
void Apply(const std::string &op_name, const std::string &arc_type,
           typename OpReg::ArgPack *args) {
  const auto op =
      OpReg::Register::GetRegister()->GetOperation(op_name, arc_type);
  if (!op) {
    FSTERROR() << op_name << ": No operation found on arc type " << arc_type;
    return;
  }
  op(args);
}

namespace internal {

// Helper that logs to ERROR if the arc types of m and n don't match,
// assuming that both m and n implement .ArcType(). The op_name argument is
// used to construct the error message.
template <class M, class N>
bool ArcTypesMatch(const M &m, const N &n, const std::string &op_name) {
  if (m.ArcType() != n.ArcType()) {
    FSTERROR() << op_name << ": Arguments with non-matching arc types "
               << m.ArcType() << " and " << n.ArcType();
    return false;
  }
  return true;
}

// From untyped to typed weights.
template <class Weight>
void CopyWeights(const std::vector<WeightClass> &weights,
                 std::vector<Weight> *typed_weights) {
  typed_weights->clear();
  typed_weights->reserve(weights.size());
  for (const auto &weight : weights) {
    typed_weights->emplace_back(*weight.GetWeight<Weight>());
  }
}

// From typed to untyped weights.
template <class Weight>
void CopyWeights(const std::vector<Weight> &typed_weights,
                 std::vector<WeightClass> *weights) {
  weights->clear();
  weights->reserve(typed_weights.size());
  for (const auto &typed_weight : typed_weights) {
    weights->emplace_back(typed_weight);
  }
}

}  // namespace internal

// Used for Replace operations.
inline std::vector<std::pair<int64_t, const FstClass *>> BorrowPairs(
    const std::vector<std::pair<int64_t, std::unique_ptr<const FstClass>>>
        &pairs) {
  std::vector<std::pair<int64_t, const FstClass *>> borrowed_pairs;
  borrowed_pairs.reserve(pairs.size());
  for (const auto &pair : pairs) {
    borrowed_pairs.emplace_back(pair.first, pair.second.get());
  }
  return borrowed_pairs;
}

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_SCRIPT_IMPL_H_
