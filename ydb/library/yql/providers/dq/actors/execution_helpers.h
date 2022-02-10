#pragma once

#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>

#include <util/generic/guid.h>

namespace NYql::NDqs::NExecutionHelpers {
    TString PrettyPrintWorkerInfo(const Yql::DqsProto::TWorkerInfo& workerInfo, ui64 stageId);

    template<typename T>
    TGUID GuidFromProto(const T& t) {
        TGUID guid;
        guid.dw[0] = t.GetDw0();
        guid.dw[1] = t.GetDw1();
        guid.dw[2] = t.GetDw2();
        guid.dw[3] = t.GetDw3();
        return guid;
    }

    template<typename T>
    void GuidToProto(T& t, const TGUID& g) {
        t.SetDw0(g.dw[0]);
        t.SetDw1(g.dw[1]);
        t.SetDw2(g.dw[2]);
        t.SetDw3(g.dw[3]);
    }

} // namespace NYql::NDqs::NExecutionHelpers
