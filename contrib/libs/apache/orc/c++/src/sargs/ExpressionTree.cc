/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ExpressionTree.hh"

#include <cassert>
#include <sstream>

namespace orc {

  ExpressionTree::ExpressionTree(Operator op)
      : operator_(op), leaf_(UNUSED_LEAF), constant_(TruthValue::YES_NO_NULL) {}

  ExpressionTree::ExpressionTree(Operator op, std::initializer_list<TreeNode> children)
      : operator_(op),
        children_(children.begin(), children.end()),
        leaf_(UNUSED_LEAF),
        constant_(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(size_t leaf)
      : operator_(Operator::LEAF), children_(), leaf_(leaf), constant_(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(TruthValue constant)
      : operator_(Operator::CONSTANT), children_(), leaf_(UNUSED_LEAF), constant_(constant) {
    // PASS
  }

  ExpressionTree::ExpressionTree(const ExpressionTree& other)
      : operator_(other.operator_), leaf_(other.leaf_), constant_(other.constant_) {
    for (TreeNode child : other.children_) {
      children_.emplace_back(std::make_shared<ExpressionTree>(*child));
    }
  }

  ExpressionTree::Operator ExpressionTree::getOperator() const {
    return operator_;
  }

  const std::vector<TreeNode>& ExpressionTree::getChildren() const {
    return children_;
  }

  std::vector<TreeNode>& ExpressionTree::getChildren() {
    return const_cast<std::vector<TreeNode>&>(
        const_cast<const ExpressionTree*>(this)->getChildren());
  }

  const TreeNode ExpressionTree::getChild(size_t i) const {
    return children_.at(i);
  }

  TreeNode ExpressionTree::getChild(size_t i) {
    return std::const_pointer_cast<ExpressionTree>(
        const_cast<const ExpressionTree*>(this)->getChild(i));
  }

  TruthValue ExpressionTree::getConstant() const {
    assert(operator_ == Operator::CONSTANT);
    return constant_;
  }

  size_t ExpressionTree::getLeaf() const {
    assert(operator_ == Operator::LEAF);
    return leaf_;
  }

  void ExpressionTree::setLeaf(size_t leaf) {
    assert(operator_ == Operator::LEAF);
    leaf_ = leaf;
  }

  void ExpressionTree::addChild(TreeNode child) {
    children_.push_back(child);
  }

  TruthValue ExpressionTree::evaluate(const std::vector<TruthValue>& leaves) const {
    TruthValue result;
    switch (operator_) {
      case Operator::OR: {
        result = children_.at(0)->evaluate(leaves);
        for (size_t i = 1; i < children_.size() && !isNeeded(result); ++i) {
          result = children_.at(i)->evaluate(leaves) || result;
        }
        return result;
      }
      case Operator::AND: {
        result = children_.at(0)->evaluate(leaves);
        for (size_t i = 1; i < children_.size() && isNeeded(result); ++i) {
          result = children_.at(i)->evaluate(leaves) && result;
        }
        return result;
      }
      case Operator::NOT:
        if (children_.size() != 1) {
          throw std::invalid_argument("NOT operator must have exactly one child");
        }
        return !children_.at(0)->evaluate(leaves);
      case Operator::LEAF:
        return leaves[leaf_];
      case Operator::CONSTANT:
        return constant_;
      default:
        throw std::invalid_argument("Unknown operator!");
    }
  }

  std::string to_string(TruthValue truthValue) {
    switch (truthValue) {
      case TruthValue::YES:
        return "YES";
      case TruthValue::NO:
        return "NO";
      case TruthValue::IS_NULL:
        return "IS_NULL";
      case TruthValue::YES_NULL:
        return "YES_NULL";
      case TruthValue::NO_NULL:
        return "NO_NULL";
      case TruthValue::YES_NO:
        return "YES_NO";
      case TruthValue::YES_NO_NULL:
        return "YES_NO_NULL";
      default:
        throw std::invalid_argument("unknown TruthValue!");
    }
  }

  std::string ExpressionTree::toString() const {
    std::ostringstream sstream;
    switch (operator_) {
      case Operator::OR:
        sstream << "(or";
        for (const auto& child : children_) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::AND:
        sstream << "(and";
        for (const auto& child : children_) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::NOT:
        if (children_.size() != 1) {
          throw std::invalid_argument("NOT operator must have exactly one child");
        }
        sstream << "(not " << children_.at(0)->toString() << ')';
        break;
      case Operator::LEAF:
        sstream << "leaf-" << leaf_;
        break;
      case Operator::CONSTANT:
        sstream << to_string(constant_);
        break;
      default:
        throw std::invalid_argument("unknown operator!");
    }
    return sstream.str();
  }

}  // namespace orc
