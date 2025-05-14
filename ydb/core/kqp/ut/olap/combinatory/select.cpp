#include "select.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr::NKqp {

TConclusionStatus TSelectCommand::DoExecute(TKikimrRunner& kikimr) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    const i64 indexSkipStart = controller->GetIndexesSkippingOnSelect().Val();
    const i64 indexApproveStart = controller->GetIndexesApprovedOnSelect().Val();
    const i64 indexNoDataStart = controller->GetIndexesSkippedNoData().Val();

    const i64 headerSkipStart = controller->GetHeadersSkippingOnSelect().Val();
    const i64 headerApproveStart = controller->GetHeadersApprovedOnSelect().Val();
    const i64 headerNoDataStart = controller->GetHeadersSkippedNoData().Val();

    Cerr << "EXECUTE: " << Command << Endl;
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto it = kikimr.GetQueryClient().StreamExecuteQuery(Command, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
    TString output = StreamResultToYson(it);
    if (Compare) {
        Cerr << "COMPARE: " << Compare << Endl;
        Cerr << "OUTPUT: " << output << Endl;
        CompareYson(output, Compare);
    }
    const ui32 iSkip = controller->GetIndexesSkippingOnSelect().Val() - indexSkipStart;
    const ui32 iNoData = controller->GetIndexesSkippedNoData().Val() - indexNoDataStart;
    const ui32 iApproves = controller->GetIndexesApprovedOnSelect().Val() - indexApproveStart;
    Cerr << "INDEX:" << iNoData << "/" << iSkip << "/" << iApproves << Endl;

    const ui32 hSkip = controller->GetHeadersSkippingOnSelect().Val() - headerSkipStart;
    const ui32 hNoData = controller->GetHeadersSkippedNoData().Val() - headerNoDataStart;
    const ui32 hApproves = controller->GetHeadersApprovedOnSelect().Val() - headerApproveStart;
    Cerr << "HEADER:" << hNoData << "/" << hSkip << "/" << hApproves << Endl;
    if (ExpectIndexSkip) {
        AFL_VERIFY(iSkip + hSkip == *ExpectIndexSkip)("expect", ExpectIndexSkip)("ireal", iSkip)("hreal", hSkip)(
                                    "current", controller->GetIndexesSkippingOnSelect().Val())("pred", indexSkipStart);
    }
    if (ExpectIndexNoData) {
        AFL_VERIFY(iNoData == *ExpectIndexNoData)("expect", ExpectIndexNoData)("real", iNoData)(
                                "current", controller->GetIndexesSkippedNoData().Val())("pred", indexNoDataStart);
    }
    if (ExpectIndexApprove) {
        AFL_VERIFY(iApproves == *ExpectIndexApprove)("expect", ExpectIndexApprove)("real", iApproves)(
                                  "current", controller->GetIndexesApprovedOnSelect().Val())("pred", indexApproveStart);
    }
    return TConclusionStatus::Success();
}

bool TSelectCommand::DeserializeFromString(const TString& info) {
    auto lines = StringSplitter(info).SplitBySet("\n").ToList<TString>();
    std::optional<ui32> state;
    for (auto&& l : lines) {
        l = Strip(l);
        if (l.StartsWith("READ:")) {
            l = l.substr(5);
            state = 0;
        } else if (l.StartsWith("EXPECTED:")) {
            l = l.substr(9);
            state = 1;
        } else if (l.StartsWith("IDX_ND_SKIP_APPROVE:")) {
            state = 2;
            l = l.substr(20);
        } else {
            AFL_VERIFY(state)("line", l);
        }

        if (*state == 0) {
            Command += l;
        } else if (*state == 1) {
            Compare += l;
        } else if (*state == 2) {
            auto idxExpectations = StringSplitter(l).SplitBySet(" ,.;").SkipEmpty().ToList<TString>();
            AFL_VERIFY(idxExpectations.size() == 3)("size", idxExpectations.size())("string", l);
            if (idxExpectations[0] != "{}") {
                ui32 res;
                AFL_VERIFY(TryFromString<ui32>(idxExpectations[0], res))("string", l);
                ExpectIndexNoData = res;
            }
            if (idxExpectations[1] != "{}") {
                ui32 res;
                AFL_VERIFY(TryFromString<ui32>(idxExpectations[1], res))("string", l);
                ExpectIndexSkip = res;
            }
            if (idxExpectations[2] != "{}") {
                ui32 res;
                AFL_VERIFY(TryFromString<ui32>(idxExpectations[2], res))("string", l);
                ExpectIndexApprove = res;
            }
        } else {
            AFL_VERIFY(false)("line", l);
        }
    }
    return true;
}

}   // namespace NKikimr::NKqp
