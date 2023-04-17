#pragma once
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/types.h>
#include <util/generic/utility.h>

#include <array>

namespace NKikimr {

template <ui32 S>
class TOperationLog {
private:
    class const_iterator {
        friend class TOperationLog;
    public:
        const_iterator(ui64 position, const TOperationLog& container)
            : Container(container)
            , Position(position) {
        }

        bool operator==(const const_iterator& other) const {
            return other.Position == Position;
        }

        bool operator!=(const const_iterator& other) const {
            return other.Position != Position;
        }

        const_iterator& operator++() {
            Position--;
            return *this;
        }

        const_iterator& operator++(int) {
            Position--;
            return *this;
        }

        const TString& operator*() const {
            const TString& str = Container.GetByPosition(Position);
            return str;
        }

    private:
        ui64 GetPosition() {
            return Position;
        }

        const TOperationLog& Container;
        ui64 Position;
    };

    friend class const_iterator;

private:
    constexpr static ui32 Capacity = S;
    std::array<TString, S> Records;
    // RecordIdx is shifted to prevent underflow on decrement
    std::atomic<ui64> NextRecordPosition = Capacity;

public:
    TOperationLog() = default;

    TOperationLog(const TOperationLog& other) = delete;
    TOperationLog& operator=(const TOperationLog& other) = delete;

    TOperationLog(TOperationLog&& other) = default;
    TOperationLog& operator=(TOperationLog&& other) = default;

    const_iterator begin() const {
        return const_iterator(NextRecordPosition.load() - 1, *this);
    }

    const_iterator end() const {
        return const_iterator(NextRecordPosition.load() - 1 - Size(), *this);
    }

    const TString& GetByPosition(ui64 position) const {
        return Records[position % Capacity];
    }

    const TString& operator[](ui32 idx) const {
        Y_VERIFY(idx < Size());
        return Records[(NextRecordPosition.load() - 1 - idx) % Capacity];
    }

    ui32 Size() const {
        return Min(NextRecordPosition.load() - Capacity, static_cast<ui64>(Capacity));
    }

    void AddRecord(const TString& value) {
        Records[NextRecordPosition.fetch_add(1) % Capacity] = value;
    }

    TString ToString() const {
        TStringStream str;
        str << "[ " ;
        /* Print OperationLog records from newer to older */ 
        ui32 ctr = 0;
        for (auto it = begin(); it != end(); ++it, ++ctr) {
            str << "Record# " << ctr <<  " : { " << GetByPosition(it.GetPosition()) << " }, ";
        }
        str << " ]";
        return str.Str();
    }
};

} // NKikimr

#define ADD_RECORD_TO_OPERATION_LOG(log, record)    \
    do {                                            \
        log.AddRecord(TStringBuilder() << record);  \
    } while (false)


#define ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(log, record)                         \
    do {                                                                                \
        log.AddRecord(TStringBuilder() << TInstant::Now().ToString() << " " << record); \
    } while (false)
