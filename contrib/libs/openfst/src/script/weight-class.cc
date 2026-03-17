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

#include <fst/script/weight-class.h>

#include <string_view>

namespace fst {
namespace script {

REGISTER_FST_WEIGHT(StdArc::Weight);
REGISTER_FST_WEIGHT(LogArc::Weight);
REGISTER_FST_WEIGHT(Log64Arc::Weight);

WeightClass::WeightClass(std::string_view weight_type,
                         std::string_view weight_str) {
  static const auto *reg = WeightClassRegister::GetRegister();
  const auto stw = reg->GetEntry(weight_type);
  if (!stw) {
    FSTERROR() << "WeightClass: Unknown weight type: " << weight_type;
    impl_.reset();
    return;
  }
  impl_ = stw(weight_str);
}

WeightClass WeightClass::Zero(std::string_view weight_type) {
  return WeightClass(weight_type, __ZERO__);
}

WeightClass WeightClass::One(std::string_view weight_type) {
  return WeightClass(weight_type, __ONE__);
}

WeightClass WeightClass::NoWeight(std::string_view weight_type) {
  return WeightClass(weight_type, __NOWEIGHT__);
}

bool WeightClass::WeightTypesMatch(const WeightClass &lhs,
                                   const WeightClass &rhs,
                                   std::string_view op_name) {
  if (lhs.Type() != rhs.Type()) {
    FSTERROR() << op_name << ": Weights with non-matching types: " << lhs.Type()
               << " and " << rhs.Type();
    return false;
  }
  return true;
}

bool operator==(const WeightClass &lhs, const WeightClass &rhs) {
  const auto *lhs_impl = lhs.GetImpl();
  const auto *rhs_impl = rhs.GetImpl();
  if (!(lhs_impl && rhs_impl &&
        WeightClass::WeightTypesMatch(lhs, rhs, "operator=="))) {
    return false;
  }
  return *lhs_impl == *rhs_impl;
}

bool operator!=(const WeightClass &lhs, const WeightClass &rhs) {
  return !(lhs == rhs);
}

WeightClass Plus(const WeightClass &lhs, const WeightClass &rhs) {
  const auto *rhs_impl = rhs.GetImpl();
  if (!(lhs.GetImpl() && rhs_impl &&
        WeightClass::WeightTypesMatch(lhs, rhs, "Plus"))) {
    return WeightClass();
  }
  WeightClass result(lhs);
  result.GetImpl()->PlusEq(*rhs_impl);
  return result;
}

WeightClass Times(const WeightClass &lhs, const WeightClass &rhs) {
  const auto *rhs_impl = rhs.GetImpl();
  if (!(lhs.GetImpl() && rhs_impl &&
        WeightClass::WeightTypesMatch(lhs, rhs, "Times"))) {
    return WeightClass();
  }
  WeightClass result(lhs);
  result.GetImpl()->TimesEq(*rhs_impl);
  return result;
}

WeightClass Divide(const WeightClass &lhs, const WeightClass &rhs) {
  const auto *rhs_impl = rhs.GetImpl();
  if (!(lhs.GetImpl() && rhs_impl &&
        WeightClass::WeightTypesMatch(lhs, rhs, "Divide"))) {
    return WeightClass();
  }
  WeightClass result(lhs);
  result.GetImpl()->DivideEq(*rhs_impl);
  return result;
}

WeightClass Power(const WeightClass &weight, size_t n) {
  if (!weight.GetImpl()) return WeightClass();
  WeightClass result(weight);
  result.GetImpl()->PowerEq(n);
  return result;
}

std::ostream &operator<<(std::ostream &ostrm, const WeightClass &weight) {
  weight.impl_->Print(&ostrm);
  return ostrm;
}

}  // namespace script
}  // namespace fst
