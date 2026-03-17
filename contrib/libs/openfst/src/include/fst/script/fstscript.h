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
// The FST script interface permits users to interact with FSTs without knowing
// their arc type. It does this by mapping compile-time polymorphism (in the
// form of a arc-templated FST types) onto a shared virtual interface. It also
// supports arc extension via a DSO interface. Due to the overhead of virtual
// dispatch and registered function lookups, the script API is somewhat slower
// then library API provided by types like StdVectorFst, but has the advantage
// that it is designed not to crash (and to provide useful debugging
// information) upon common user errors like passing invalid indices or
// attempting comparison of incompatible FSTs. It is used both by the FST
// binaries and the Python extension.
//
// This header includes all of the FST script functionality.

#ifndef FST_SCRIPT_FSTSCRIPT_H_
#define FST_SCRIPT_FSTSCRIPT_H_

// Major classes
#include <fst/script/arciterator-class.h>
#include <fst/script/encodemapper-class.h>
#include <fst/script/fst-class.h>
#include <fst/script/stateiterator-class.h>
#include <fst/script/text-io.h>
#include <fst/script/weight-class.h>

// Flag-to-enum parsers.
#include <fst/script/getters.h>
// Templates like Operation<> and Apply<>.
#include <fst/script/script-impl.h>

// Operations.
#include <fst/script/arcsort.h>
#include <fst/script/closure.h>
#include <fst/script/compile.h>
#include <fst/script/compose.h>
#include <fst/script/concat.h>
#include <fst/script/connect.h>
#include <fst/script/convert.h>
#include <fst/script/decode.h>
#include <fst/script/determinize.h>
#include <fst/script/difference.h>
#include <fst/script/disambiguate.h>
#include <fst/script/draw.h>
#include <fst/script/encode.h>
#include <fst/script/epsnormalize.h>
#include <fst/script/equal.h>
#include <fst/script/equivalent.h>
#include <fst/script/info.h>
#include <fst/script/intersect.h>
#include <fst/script/invert.h>
#include <fst/script/isomorphic.h>
#include <fst/script/map.h>
#include <fst/script/minimize.h>
#include <fst/script/print.h>
#include <fst/script/project.h>
#include <fst/script/prune.h>
#include <fst/script/push.h>
#include <fst/script/randequivalent.h>
#include <fst/script/randgen.h>
#include <fst/script/relabel.h>
#include <fst/script/replace.h>
#include <fst/script/reverse.h>
#include <fst/script/reweight.h>
#include <fst/script/rmepsilon.h>
#include <fst/script/shortest-distance.h>
#include <fst/script/shortest-path.h>
#include <fst/script/synchronize.h>
#include <fst/script/topsort.h>
#include <fst/script/union.h>
#include <fst/script/verify.h>

// This class is necessary because registering each of the operations
// separately overfills the stack, as there's so many of them.
namespace fst {
namespace script {

template <class Arc>
class AllFstOperationsRegisterer {
 public:
  AllFstOperationsRegisterer() {
    RegisterBatch1();
    RegisterBatch2();
  }

 private:
  void RegisterBatch1() {
    REGISTER_FST_OPERATION(ArcSort, Arc, FstArcSortArgs);
    REGISTER_FST_OPERATION(Closure, Arc, FstClosureArgs);
    REGISTER_FST_OPERATION(CompileInternal, Arc, FstCompileArgs);
    REGISTER_FST_OPERATION(Compose, Arc, FstComposeArgs);
    REGISTER_FST_OPERATION(Concat, Arc, FstConcatArgs1);
    REGISTER_FST_OPERATION(Concat, Arc, FstConcatArgs2);
    REGISTER_FST_OPERATION(Concat, Arc, FstConcatArgs3);
    REGISTER_FST_OPERATION(Connect, Arc, MutableFstClass);
    REGISTER_FST_OPERATION(Convert, Arc, FstConvertArgs);
    REGISTER_FST_OPERATION(Decode, Arc, FstDecodeArgs);
    REGISTER_FST_OPERATION(Determinize, Arc, FstDeterminizeArgs);
    REGISTER_FST_OPERATION(Difference, Arc, FstDifferenceArgs);
    REGISTER_FST_OPERATION(Disambiguate, Arc, FstDisambiguateArgs);
    REGISTER_FST_OPERATION(Draw, Arc, FstDrawArgs);
    REGISTER_FST_OPERATION(Encode, Arc, FstEncodeArgs);
    REGISTER_FST_OPERATION(EpsNormalize, Arc, FstEpsNormalizeArgs);
    REGISTER_FST_OPERATION(Equal, Arc, FstEqualArgs);
    REGISTER_FST_OPERATION(Equivalent, Arc, FstEquivalentArgs);
    REGISTER_FST_OPERATION(Info, Arc, FstInfoArgs);
    REGISTER_FST_OPERATION(InitArcIteratorClass, Arc, InitArcIteratorClassArgs);
    REGISTER_FST_OPERATION(InitMutableArcIteratorClass, Arc,
                           InitMutableArcIteratorClassArgs);
    REGISTER_FST_OPERATION(InitStateIteratorClass, Arc,
                           InitStateIteratorClassArgs);
    REGISTER_FST_OPERATION(Intersect, Arc, FstIntersectArgs);
    REGISTER_FST_OPERATION(Invert, Arc, MutableFstClass);
    REGISTER_FST_OPERATION(Isomorphic, Arc, FstIsomorphicArgs);
  }

  void RegisterBatch2() {
    REGISTER_FST_OPERATION(Map, Arc, FstMapArgs);
    REGISTER_FST_OPERATION(Minimize, Arc, FstMinimizeArgs);
    REGISTER_FST_OPERATION(Print, Arc, FstPrintArgs);
    REGISTER_FST_OPERATION(Project, Arc, FstProjectArgs);
    REGISTER_FST_OPERATION(Prune, Arc, FstPruneArgs1);
    REGISTER_FST_OPERATION(Prune, Arc, FstPruneArgs2);
    REGISTER_FST_OPERATION(Push, Arc, FstPushArgs1);
    REGISTER_FST_OPERATION(Push, Arc, FstPushArgs2);
    REGISTER_FST_OPERATION(RandEquivalent, Arc, FstRandEquivalentArgs);
    REGISTER_FST_OPERATION(RandGen, Arc, FstRandGenArgs);
    REGISTER_FST_OPERATION(Relabel, Arc, FstRelabelArgs1);
    REGISTER_FST_OPERATION(Relabel, Arc, FstRelabelArgs2);
    REGISTER_FST_OPERATION(Replace, Arc, FstReplaceArgs);
    REGISTER_FST_OPERATION(Reverse, Arc, FstReverseArgs);
    REGISTER_FST_OPERATION(Reweight, Arc, FstReweightArgs);
    REGISTER_FST_OPERATION(RmEpsilon, Arc, FstRmEpsilonArgs);
    REGISTER_FST_OPERATION(ShortestDistance, Arc, FstShortestDistanceArgs1);
    REGISTER_FST_OPERATION(ShortestDistance, Arc, FstShortestDistanceArgs2);
    REGISTER_FST_OPERATION(ShortestDistance, Arc, FstShortestDistanceArgs3);
    REGISTER_FST_OPERATION(ShortestPath, Arc, FstShortestPathArgs);
    REGISTER_FST_OPERATION(Synchronize, Arc, FstSynchronizeArgs);
    REGISTER_FST_OPERATION(TopSort, Arc, FstTopSortArgs);
    REGISTER_FST_OPERATION(Union, Arc, FstUnionArgs1);
    REGISTER_FST_OPERATION(Union, Arc, FstUnionArgs2);
    REGISTER_FST_OPERATION(Verify, Arc, FstVerifyArgs);
  }
};

}  // namespace script
}  // namespace fst

#define REGISTER_FST_OPERATIONS(Arc) \
  AllFstOperationsRegisterer<Arc> register_all_fst_operations##Arc;

#endif  // FST_SCRIPT_FSTSCRIPT_H_
