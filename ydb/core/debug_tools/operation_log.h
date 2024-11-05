#pragma once
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/types.h>
#include <util/generic/utility.h>

#include <array>

namespace NKikimr {

template <ui32 S>
class TOperationLog {
public:
    struct BorrowedRecord {
        std::unique_ptr<TString> Record;
        ui32 Position;

        const TString& operator*() const {
            return *Record;
        }

        operator bool() const {
            return (bool)Record;
        }
    };

private:
    constexpr static ui32 Capacity = S;
    std::array<std::atomic<TString*>, S> Records;
    // RecordPosition is shifted to prevent underflow on decrement
    std::atomic<ui64> NextRecordPosition = Capacity;

public:
    TOperationLog(bool initializeRecords = true, TString defaultValue = "Race occured, try again") {
        if (initializeRecords) {
            InitializeRecords(defaultValue);
        }
    };

    TOperationLog(TOperationLog&& other) = default;
    TOperationLog& operator=(TOperationLog&& other) = default;

    ~TOperationLog() {
        for (auto& record : Records) {
            delete record.load();
        }
    }

    void InitializeRecords(TString value = "Race occured, try again") {
        for (auto& record : Records) {
            record.store(new TString(value));
        }
    }

    BorrowedRecord BorrowByIdx(ui32 idx) {
        ui32 position = (NextRecordPosition.load() - 1 - idx) % Capacity;
        if (idx >= Size()) {
            return { nullptr, position };
        }

        std::unique_ptr<TString> ptr(Records[position].exchange(nullptr));
        return { 
            .Record = std::move(ptr),
            .Position = position
        };
    }

    void ReturnBorrowedRecord(BorrowedRecord& borrowed) {
        TString* pRecord = borrowed.Record.release();
        TString* expected = nullptr;
        if (!Records[borrowed.Position].compare_exchange_strong(expected, pRecord)) {
            // This cell is taken by newer value, delete an old one
            delete pRecord;
        }
    }

    ui32 Size() const {
        return Min(NextRecordPosition.load() - Capacity, static_cast<ui64>(Capacity));
    }

    void AddRecord(std::unique_ptr<TString>& value) {
        TString* prev = Records[NextRecordPosition.fetch_add(1) % Capacity].exchange(value.release());
        if (prev) {
            delete prev;
        }
    }

    TString ToString() {
        TStringStream str;
        str << "[ " ;
        /* Print OperationLog records from newer to older */ 
        ui32 logSize = Size();
        for (ui32 i = 0; i < logSize; ++i) {
            auto record = BorrowByIdx(i);
            if (record) {
                str << "Record# " << i <<  " : { " << *record << " }, ";
                ReturnBorrowedRecord(record);
            } else {
                str << "Record# " << i <<  " : { null }, ";
            }
        }
        str << " ]";
        return str.Str();
    }
};

} // NKikimr

#define ADD_RECORD_TO_OPERATION_LOG(log, record)                                \
    do {                                                                        \
        auto recordPtr = std::make_unique<TString>(TStringBuilder() << record); \
        log.AddRecord(recordPtr);                                               \
    } while (false)

#define ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(log, record)                                                         \
    do {                                                                                                                \
        auto recordPtr = std::make_unique<TString>(TStringBuilder() << TInstant::Now().ToString() << " " << record);    \
        log.AddRecord(recordPtr);                                                                                       \
    } while (false)   
