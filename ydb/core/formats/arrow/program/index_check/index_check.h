#pragma once

#include <ydb/library/accessor/accessor.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow::NSSA {

    class TIndexCheckOperation {
    public:
        enum class EOperation: ui32 {
            Equals,
            StartsWith,
            EndsWith,
            Contains,
            Less,
            Greater,
            LessOrEqual,
            GreaterOrEqual,
        };

    private:
        const EOperation Operation;
        YDB_READONLY(bool, CaseSensitive, true);

    public:
        TString GetSignalId() const {
            return TStringBuilder() << Operation << "::" << (CaseSensitive ? 1 : 0);
        }

        TString DebugString() const {
            return TStringBuilder() << "{" << Operation << "," << CaseSensitive << "}";
        }

        EOperation GetOperation() const {
            return Operation;
        }

        TIndexCheckOperation(const EOperation op, const bool caseSensitive)
            : Operation(op)
            , CaseSensitive(caseSensitive)
        {
        }

        explicit operator size_t() const {
            return (size_t)Operation;
        }

        bool operator==(const TIndexCheckOperation& op) const = default;
    };

} // namespace NKikimr::NArrow::NSSA
