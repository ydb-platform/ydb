// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/linalg_ops.h"

namespace tensorflow {
namespace ops {

Cholesky::Cholesky(const ::tensorflow::Scope& scope, ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Cholesky");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Cholesky")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

CholeskyGrad::CholeskyGrad(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input l, ::tensorflow::Input grad) {
  if (!scope.ok()) return;
  auto _l = ::tensorflow::ops::AsNodeOut(scope, l);
  if (!scope.ok()) return;
  auto _grad = ::tensorflow::ops::AsNodeOut(scope, grad);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("CholeskyGrad");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "CholeskyGrad")
                     .Input(_l)
                     .Input(_grad)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

LogMatrixDeterminant::LogMatrixDeterminant(const ::tensorflow::Scope& scope,
                                           ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogMatrixDeterminant");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogMatrixDeterminant")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sign = Output(ret, _outputs_range["sign"].first);
  this->log_abs_determinant = Output(ret, _outputs_range["log_abs_determinant"].first);
}

MatrixDeterminant::MatrixDeterminant(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixDeterminant");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixDeterminant")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixExponential::MatrixExponential(const ::tensorflow::Scope& scope,
                                     ::tensorflow::Input input) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixExponential");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixExponential")
                     .Input(_input)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixInverse::MatrixInverse(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input input, const
                             MatrixInverse::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixInverse");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixInverse")
                     .Input(_input)
                     .Attr("adjoint", attrs.adjoint_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixInverse::MatrixInverse(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input input)
  : MatrixInverse(scope, input, MatrixInverse::Attrs()) {}

MatrixSolve::MatrixSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         matrix, ::tensorflow::Input rhs, const
                         MatrixSolve::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _matrix = ::tensorflow::ops::AsNodeOut(scope, matrix);
  if (!scope.ok()) return;
  auto _rhs = ::tensorflow::ops::AsNodeOut(scope, rhs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixSolve");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixSolve")
                     .Input(_matrix)
                     .Input(_rhs)
                     .Attr("adjoint", attrs.adjoint_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixSolve::MatrixSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         matrix, ::tensorflow::Input rhs)
  : MatrixSolve(scope, matrix, rhs, MatrixSolve::Attrs()) {}

MatrixSolveLs::MatrixSolveLs(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input matrix, ::tensorflow::Input
                             rhs, ::tensorflow::Input l2_regularizer, const
                             MatrixSolveLs::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _matrix = ::tensorflow::ops::AsNodeOut(scope, matrix);
  if (!scope.ok()) return;
  auto _rhs = ::tensorflow::ops::AsNodeOut(scope, rhs);
  if (!scope.ok()) return;
  auto _l2_regularizer = ::tensorflow::ops::AsNodeOut(scope, l2_regularizer);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixSolveLs");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixSolveLs")
                     .Input(_matrix)
                     .Input(_rhs)
                     .Input(_l2_regularizer)
                     .Attr("fast", attrs.fast_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixSolveLs::MatrixSolveLs(const ::tensorflow::Scope& scope,
                             ::tensorflow::Input matrix, ::tensorflow::Input
                             rhs, ::tensorflow::Input l2_regularizer)
  : MatrixSolveLs(scope, matrix, rhs, l2_regularizer, MatrixSolveLs::Attrs()) {}

MatrixTriangularSolve::MatrixTriangularSolve(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input matrix,
                                             ::tensorflow::Input rhs, const
                                             MatrixTriangularSolve::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _matrix = ::tensorflow::ops::AsNodeOut(scope, matrix);
  if (!scope.ok()) return;
  auto _rhs = ::tensorflow::ops::AsNodeOut(scope, rhs);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("MatrixTriangularSolve");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "MatrixTriangularSolve")
                     .Input(_matrix)
                     .Input(_rhs)
                     .Attr("lower", attrs.lower_)
                     .Attr("adjoint", attrs.adjoint_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

MatrixTriangularSolve::MatrixTriangularSolve(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input matrix,
                                             ::tensorflow::Input rhs)
  : MatrixTriangularSolve(scope, matrix, rhs, MatrixTriangularSolve::Attrs()) {}

Qr::Qr(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
       Qr::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Qr");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Qr")
                     .Input(_input)
                     .Attr("full_matrices", attrs.full_matrices_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->q = Output(ret, _outputs_range["q"].first);
  this->r = Output(ret, _outputs_range["r"].first);
}

Qr::Qr(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Qr(scope, input, Qr::Attrs()) {}

SelfAdjointEig::SelfAdjointEig(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input, const
                               SelfAdjointEig::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("SelfAdjointEig");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "SelfAdjointEigV2")
                     .Input(_input)
                     .Attr("compute_v", attrs.compute_v_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->e = Output(ret, _outputs_range["e"].first);
  this->v = Output(ret, _outputs_range["v"].first);
}

SelfAdjointEig::SelfAdjointEig(const ::tensorflow::Scope& scope,
                               ::tensorflow::Input input)
  : SelfAdjointEig(scope, input, SelfAdjointEig::Attrs()) {}

Svd::Svd(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
         Svd::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Svd");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Svd")
                     .Input(_input)
                     .Attr("compute_uv", attrs.compute_uv_)
                     .Attr("full_matrices", attrs.full_matrices_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->s = Output(ret, _outputs_range["s"].first);
  this->u = Output(ret, _outputs_range["u"].first);
  this->v = Output(ret, _outputs_range["v"].first);
}

Svd::Svd(const ::tensorflow::Scope& scope, ::tensorflow::Input input)
  : Svd(scope, input, Svd::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
