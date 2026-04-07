#include "printer.h"

#include <util/string/builder.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NPrinter {

namespace {

TString ToString(const NKikimrSSA::TProgram::TParameter& parameter) {
    return TStringBuilder{} << "Param " << parameter.GetName();
}

TString ToString(const NKikimrSSA::TProgram::TConstant& constant) {
    return TStringBuilder{} << "Const " << constant.ShortDebugString();
}

TString ToString(const NKikimrSSA::TProgram::TColumn& column) {
    return TStringBuilder{} << "$" << column.GetId();
}

TString ToString(const NKikimrSSA::TProgram::TAssignment::TExternalFunction& externalFunction) {
    TStringBuilder result;
    result << externalFunction.GetName();
    for (const auto& column: externalFunction.GetArguments()) {
        result << " " << ToString(column);
    }
    return result;
}

TString ToString(const NKikimrSSA::TProgram::TProjection& projection) {
    TStringBuilder result;
    result << "Projection";
    for (const auto& column: projection.GetColumns()) {
        result << " " << ToString(column);
    }
    return result;
}

TString ToString(const NKikimrSSA::TProgram::TFilter& filter) {
    return TStringBuilder{} << "Filter " << ToString(filter.GetPredicate());
}

TString ToString(const NKikimrSSA::TProgram::TAssignment::TFunction& function) {
    TStringBuilder result;
    if (function.HasKernelName()) {
        result << function.GetKernelName();
    } else if (function.HasId()) {
        result << NKikimrSSA::TProgram::TAssignment::EFunction_Name(NKikimrSSA::TProgram::TAssignment::EFunction(function.GetId()));
    } else if (function.HasYqlOperationId()) {
        result << NYql::TKernelRequestBuilder::EBinaryOp(function.GetYqlOperationId());
    } else {
        result << "UnknownFunction";
    }
    for (const auto& column: function.GetArguments()) {
        result << " " << ToString(column);
    }
    return result;
}

TString ToString(const NKikimrSSA::TProgram::TAssignment& assignment) {
    TStringBuilder result;
    result << ToString(assignment.GetColumn()) << " = ";
    switch (assignment.expression_case()) {
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::kConstant:
            return result << ToString(assignment.GetConstant());
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::kFunction:
            return result << ToString(assignment.GetFunction());
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::kExternalFunction:
            return result << ToString(assignment.GetExternalFunction());
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::kNull:
            return result << assignment.ShortDebugString();
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::kParameter:
            return result << ToString(assignment.GetParameter());
        case NKikimrSSA::TProgram::TAssignment::ExpressionCase::EXPRESSION_NOT_SET:
            return result;
    }
}

TString ToString(const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& aggregateFunction) {
    TStringBuilder result;
    if (aggregateFunction.HasId()) {
        result << NKikimrSSA::TProgram::TAggregateAssignment::EAggregateFunction_Name(NKikimrSSA::TProgram::TAggregateAssignment::EAggregateFunction(aggregateFunction.GetId()));
    } else {
        result << "UnknownFunction";
    }
    for (const auto& column: aggregateFunction.GetArguments()) {
        result << " " << ToString(column);
    }
    return result;
}

TString ToString(const NKikimrSSA::TProgram::TAggregateAssignment& aggregateAssignment) {
    return TStringBuilder{} << ToString(aggregateAssignment.GetColumn()) << " = " << ToString(aggregateAssignment.GetFunction());
}

TString ToString(const NKikimrSSA::TProgram::TGroupBy& groupBy) {
    TStringBuilder result;
    result << "GroupBy";
    for (const auto& column: groupBy.GetKeyColumns()) {
        result << " " << ToString(column);
    }

    for (const auto& aggregateAssignment: groupBy.GetAggregates()) {
        result << " " << ToString(aggregateAssignment);
    }
    return result;
}

}

TString SSAToPrettyString(const NKikimrSSA::TProgram& program) {
    TStringBuilder result;
    for (const auto& command: program.GetCommand()) {
        switch (command.line_case()) {
            case NKikimrSSA::TProgram::TCommand::LineCase::kAssign: {
                result << ToString(command.GetAssign()) << Endl;
                break;
            }
            case NKikimrSSA::TProgram::TCommand::LineCase::kFilter: {
                result << ToString(command.GetFilter()) << Endl;
                break;
            }
            case NKikimrSSA::TProgram::TCommand::LineCase::kProjection: {
                result << ToString(command.GetProjection()) << Endl;
                break;
            }
            case NKikimrSSA::TProgram::TCommand::LineCase::kGroupBy: {
                result << ToString(command.GetGroupBy()) << Endl;
                break;
            }
            case NKikimrSSA::TProgram::TCommand::LineCase::LINE_NOT_SET: {
                break;
            }
        }
    }
    return result;
}


}   // namespace NKikimr::NArrow::NPrinter
