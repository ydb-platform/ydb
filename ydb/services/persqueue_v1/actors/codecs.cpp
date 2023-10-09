#include "codecs.h"

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/generic/algorithm.h>
#include <util/generic/fwd.h>
#include <util/string/builder.h>

namespace NKikimr::NGRpcProxy {
    bool ValidateWriteWithCodec(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const ui32 codecID, TString& error) {
        error.clear();

        if (pqTabletConfig.has_codecs() /* empty codecs that any codec is allowed for migration purposes */) {
            const auto& ids = pqTabletConfig.codecs().ids();
            if (!ids.empty() && Find(ids, codecID) == ids.end()) {
                const auto& names = pqTabletConfig.codecs().codecs();
                Y_ABORT_UNLESS(ids.size() == names.size(), "PQ tabled supported codecs configuration is invalid");
                TStringBuilder errorBuilder;
                errorBuilder << "given codec (id " << static_cast<i32>(codecID) << ") is not configured for the topic. Configured codecs are " << names[0] << " (id " << ids[0] << ")";
                for (i32 i = 1; i != ids.size(); ++i) {
                    errorBuilder << ", " << names[i] << " (id " << ids[i] << ")";
                }
                error = errorBuilder;
                return false;
            }
        }

        return true;
    }
}
