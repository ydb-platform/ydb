#pragma once

#include "defs.h"
#include "dsproxy_fault_tolerance_ut_base.h"

namespace NKikimr {

class TPutFaultToleranceTest : public TFaultToleranceTestBase<TPutFaultToleranceTest> {
public:
    using TFaultToleranceTestBase::TFaultToleranceTestBase;

    void RunTestAction() {
        ui32 index = 1;

        for (const TEvBlobStorage::TEvPut::ETactic tactic : {TEvBlobStorage::TEvPut::TacticMinLatency,
                TEvBlobStorage::TEvPut::TacticMaxThroughput}) {
            auto makeBlob = [&index] {
                TString data = Sprintf("#%" PRIu32, index);
                while (data.size() < 4096) {
                    data += data;
                }
                TLogoBlobID id(1, 1, index++, 0, data.size(), 0);
                return std::make_tuple(id, data);
            };

            THashMap<TLogoBlobID, TString> dataMap;

            auto tryOption = [&](const auto& faultyDisks, bool fitsFailModel) {
                if (IsVerbose) {
                    TStringStream str;
                    faultyDisks.Output(str << "faultyDisks# ");
                    str << " fitsFailModel# " << (fitsFailModel ? "true" : "false");
                    Cerr << str.Str() << Endl;
                }

                SetFailedDisks(faultyDisks);
                TLogoBlobID id;
                TString data;
                std::tie(id, data) = makeBlob();
                TAutoPtr<TEvBlobStorage::TEvPutResult> result = PutWithResult(id, data, tactic);
                switch (result->Status) {
                    case NKikimrProto::OK:
                        // whenever the PUT request finishes with success, the blob must be actually written and should
                        // be restored in any case while fitting failure model
                        dataMap[id] = data;
                        break;

                    case NKikimrProto::ERROR:
                        // ERROR can only be generated if the failure model is exceeded
                        Y_ABORT_UNLESS(!fitsFailModel);
                        UNIT_ASSERT_VALUES_UNEQUAL("", result->ErrorReason);
                        break;

                    default:
                        Y_ABORT("unexpected status %s", NKikimrProto::EReplyStatus_Name(result->Status).data());
                }
            };

            // now write blobs under normal conditions -- not exceeding failure model
            for (const auto& faultyDisks : FaultsFittingFailModel) {
                tryOption(faultyDisks, true);
            }

            // perform writes while exceeding the failure model
            for (const auto& faultyDisks : FaultsExceedingFailModel) {
                tryOption(faultyDisks, false);
            }

            // ensure we can read all the confirmed, thus successfully written blobs
            for (const auto& faultyDisks : FaultsFittingFailModel) {
                SetFailedDisks(faultyDisks);
                for (const auto& kv : dataMap) {
                    CheckBlob(kv.first, false, NKikimrProto::OK, kv.second);
                }
            }
        }
    }
};

} // NKikimr
