#include "codecs.h"

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NGRpcProxy {
    namespace {
        // Well-known codec names as stored in the tablet config (Codec_Name lowercased without the
        // "CODEC_" prefix) mapped to their protocol-independent codec number (CODEC_RAW == 1, ...).
        // Custom codecs are stored as the name "CUSTOM" which is intentionally absent here: such
        // entries cannot be expressed by name and must be recovered from the numeric ids list.
        i32 CodecNumberByName(const TString& name) {
            static const THashMap<TString, i32> byName{
                {"raw", 1}, {"gzip", 2}, {"lzop", 3}, {"zstd", 4},
            };
            const auto* number = byName.FindPtr(name);
            return number ? *number : 0;
        }
    }

    TVector<i32> BuildSupportedCodecs(const NKikimrPQ::TPQTabletConfig& pqTabletConfig) {
        const auto& codecs = pqTabletConfig.codecs();
        const auto& names = codecs.codecs();
        const auto& ids = codecs.ids();

        TVector<i32> result;
        result.reserve(Max(names.size(), ids.size()));

        // Use the string-name list as the base (legacy behavior), falling back to the numeric id at
        // the same position whenever a name cannot be resolved (e.g. custom codecs stored as "CUSTOM").
        for (i32 i = 0; i < names.size(); ++i) {
            i32 number = CodecNumberByName(names[i]);
            if (number == 0 && i < ids.size()) {
                number = ids[i] + 1;
            }
            result.push_back(number);
        }

        // Append any trailing numeric ids that the (shorter) name list did not cover.
        for (i32 i = names.size(); i < ids.size(); ++i) {
            result.push_back(ids[i] + 1);
        }

        return result;
    }

    bool ValidateWriteWithCodec(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const ui32 codecID, TString& error) {
        error.clear();

        if (pqTabletConfig.has_codecs() /* empty codecs that any codec is allowed for migration purposes */) {
            const auto& ids = pqTabletConfig.codecs().ids();
            if (!ids.empty() && Find(ids, codecID) == ids.end()) {
                const auto& names = pqTabletConfig.codecs().codecs();
                AFL_ENSURE(ids.size() == names.size())("reason", "PQ tablet supported codecs configuration is invalid");
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
