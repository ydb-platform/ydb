#include "restore_import_data.h"

#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/util/log.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/bucket_quoter/bucket_quoter.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/info.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>

namespace NYdb::NDump {

using namespace NImport;
using namespace NTable;

namespace {

class TValue {
    struct TSpecialType {
        bool operator<(const TSpecialType&) const {
            return false; // to make std::variant's comparator happy
        }
    };

    struct TInf: public TSpecialType {}; // we need just the positive one, it'll be used as a last boundary
    struct TNull: public TSpecialType {};

    enum class EType {
        Inf = 0,
        Null,
        String,
        Pod,
    };

    inline EType GetType() const {
        switch (Value.index()) {
        case 0:
            return EType::Inf;
        case 1:
            return EType::Null;
        case 2:
            return EType::String;
        default:
            return EType::Pod;
        }
    }

public:
    template <typename T>
    explicit TValue(T value)
        : Value(value)
    {
    }

    static TValue Inf() {
        return TValue(TInf());
    }

    static TValue Null() {
        return TValue(TNull());
    }

    bool operator<(const TValue& rhs) const {
        const EType lType = GetType();
        const EType rType = rhs.GetType();

        if (lType == EType::Inf) {
            return false;
        } else if (rType == EType::Inf) {
            return true;
        } else if (lType == EType::Null) {
            return lType != rType;
        } else if (rType == EType::Null) {
            return false;
        } else {
            return Value < rhs.Value;
        }
    }

    ui64 MemSize() const {
        switch (GetType()) {
        case EType::String:
            return sizeof(Value) + std::get<std::string>(Value).size();
        default:
            return sizeof(Value);
        }
    }

private:
    std::variant<
        TInf,
        TNull,
        std::string,
        bool,
        ui8,
        i32,
        ui32,
        i64,
        ui64
    > Value;

}; // TValue

template <typename TParser>
class TValueConverter {
    TValue Convert(EPrimitiveType type) const {
        switch (type) {
        case EPrimitiveType::Bool:
            return TValue(Parser.GetBool());
        case EPrimitiveType::Uint8:
            return TValue(Parser.GetUint8());
        case EPrimitiveType::Int32:
            return TValue(Parser.GetInt32());
        case EPrimitiveType::Uint32:
            return TValue(Parser.GetUint32());
        case EPrimitiveType::Int64:
            return TValue(Parser.GetInt64());
        case EPrimitiveType::Uint64:
            return TValue(Parser.GetUint64());
        case EPrimitiveType::DyNumber:
            return TValue(Parser.GetDyNumber());
        case EPrimitiveType::Date:
            return TValue(Parser.GetDate().GetValue());
        case EPrimitiveType::Datetime:
            return TValue(Parser.GetDatetime().GetValue());
        case EPrimitiveType::Timestamp:
            return TValue(Parser.GetTimestamp().GetValue());
        case EPrimitiveType::Date32:
            return TValue(Parser.GetDate32());
        case EPrimitiveType::Datetime64:
            return TValue(Parser.GetDatetime64());
        case EPrimitiveType::Timestamp64:
            return TValue(Parser.GetTimestamp64());
        case EPrimitiveType::String:
            return TValue(Parser.GetString());
        case EPrimitiveType::Utf8:
            return TValue(Parser.GetUtf8());
        default:
            Y_ENSURE(false, "Unexpected primitive type: " << type);
        }
    }

    void Convert(TVector<TValue>& value) {
        switch (Parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            value.emplace_back(Convert(Parser.GetPrimitiveType()));
            break;

        case TTypeParser::ETypeKind::Optional:
            Parser.OpenOptional();
            if (Parser.IsNull()) {
                value.emplace_back(TValue::Null());
            } else {
                Convert(value);
            }
            Parser.CloseOptional();
            break;

        case TTypeParser::ETypeKind::Tuple:
            Parser.OpenTuple();
            while (Parser.TryNextElement()) {
                Convert(value);
            }
            Parser.CloseTuple();
            break;

        default:
            Y_ENSURE(false, "Unexpected type kind: " << Parser.GetKind());
        }
    }

public:
    explicit TValueConverter(TParser& parser)
        : Parser(parser)
    {
    }

    TVector<TValue> ConvertAll() {
        TVector<TValue> value;
        Convert(value);
        return value;
    }

    TValue ConvertSingle() {
        auto value = ConvertAll();
        Y_ENSURE(value.size() == 1, "Unexpected value size: " << value.size());
        return value[0];
    }

private:
    TParser& Parser;

}; // TValueConverter

class TYdbDumpValueParser {
    TString CheckedUnescape() const {
        Y_ENSURE(Value.size() >= 2 || Value.front() == '"' || Value.back() == '"');
        return CGIUnescapeRet(TStringBuf(Value).Skip(1).Chop(1));
    }

public:
    explicit TYdbDumpValueParser(TStringBuf value, EPrimitiveType type)
        : Value(value)
        , Type(type)
        , OptionalOpened(false)
    {
    }

    TTypeParser::ETypeKind GetKind() const {
        return OptionalOpened
            ? TTypeParser::ETypeKind::Primitive
            : TTypeParser::ETypeKind::Optional;
    }

    EPrimitiveType GetPrimitiveType() const {
        return Type;
    }

    bool GetBool() const {
        return FromString<bool>(Value);
    }

    ui8 GetUint8() const {
        return FromString<ui8>(Value);
    }

    i32 GetInt32() const {
        return FromString<i32>(Value);
    }

    ui32 GetUint32() const {
        return FromString<ui32>(Value);
    }

    i64 GetInt64() const {
        return FromString<i64>(Value);
    }

    ui64 GetUint64() const {
        return FromString<ui64>(Value);
    }

    TString GetDyNumber() const {
        return ToString(Value);
    }

    TInstant GetDate() const {
        return TInstant::ParseIso8601(Value);
    }

    TInstant GetDatetime() const {
        return TInstant::ParseIso8601(Value);
    }

    TInstant GetTimestamp() const {
        return TInstant::ParseIso8601(Value);
    }

    i32 GetDate32() const {
        return FromString<i32>(Value);
    }

    i64 GetDatetime64() const {
        return FromString<i64>(Value);
    }

    i64 GetTimestamp64() const {
        return FromString<i64>(Value);
    }

    TString GetString() const {
        return CheckedUnescape();
    }

    TString GetUtf8() const {
        return CheckedUnescape();
    }

    bool IsNull() const {
        return Value == "null";
    }

    void OpenOptional() {
        OptionalOpened = true;
    }

    void CloseOptional() {
        OptionalOpened = false;
    }

    bool TryNextElement() {
        return false;
    }

    void OpenTuple() {}
    void CloseTuple() {}

private:
    const TStringBuf Value;
    const EPrimitiveType Type;

    bool OptionalOpened;

}; // TYdbDumpValueParser

class TKey: public TVector<TValue> {
public:
    using TVector<TValue>::TVector;

    TKey(TVector<TValue>&& values)
        : TVector<TValue>(std::move(values))
    {
    }

    bool operator<(const TKey& rhs) const {
        for (ui32 i = 0; i < Min(size(), rhs.size()); ++i) {
            if (at(i) < rhs.at(i)) {
                return true;
            }

            if (rhs.at(i) < at(i)) {
                return false;
            }
        }

        return size() < rhs.size();
    }

    ui64 MemSize() const {
        return Accumulate(begin(), end(), ui64(0), [](ui64 s, const auto& v) {
            return s + v.MemSize();
        });
    }

}; // TKey

using TSplitPoint = TKey;

class TKeyBuilder {
    static auto MakeKeyColumnIds(const std::vector<std::string>& keyColumns) {
        THashMap<TString, ui32> keyColumnIds;

        for (ui32 i = 0; i < keyColumns.size(); ++i) {
            Y_ENSURE(keyColumnIds.emplace(keyColumns.at(i), i).second);
        }

        return keyColumnIds;
    }

    static EPrimitiveType GetPrimitiveType(TTypeParser& parser) {
        switch (parser.GetKind()) {
        case TTypeParser::ETypeKind::Optional:
            parser.OpenOptional();
            return GetPrimitiveType(parser);
        case TTypeParser::ETypeKind::Primitive:
            return parser.GetPrimitive();
        default:
            Y_ENSURE(false, "Unexpected type kind: " << parser.GetKind());
        }
    }

    static EPrimitiveType GetPrimitiveType(const TType& type) {
        TTypeParser parser(type);
        return GetPrimitiveType(parser);
    }

public:
    explicit TKeyBuilder(
            const std::vector<TColumn>& columns,
            const std::vector<std::string>& keyColumns,
            const std::shared_ptr<TLog>& log)
        : Columns(columns)
        , KeyColumnIds(MakeKeyColumnIds(keyColumns))
        , Log(log)
    {
    }

    TKey Build(const NPrivate::TLine& line) const {
        TMap<ui32, TValue> values;

        TStringBuf buf = line;
        for (const auto& column : Columns) {
            TStringBuf value = buf.NextTok(',');
            if (!value) {
                LOG_E("Empty token: " << line.GetLocation());
                return {};
            }

            auto it = KeyColumnIds.find(column.Name);
            if (it == KeyColumnIds.end()) {
                continue;
            }

            TYdbDumpValueParser parser(value, GetPrimitiveType(column.Type));
            try {
                values.emplace(it->second, TValueConverter<TYdbDumpValueParser>(parser).ConvertSingle());
            } catch (const TFromStringException& e) {
                auto loc = TStringBuilder() << line.GetLocation();
                throw NStatusHelpers::TYdbErrorException(Result<TStatus>(loc, EStatus::SCHEME_ERROR, e.what()));
            } catch (...) {
                std::rethrow_exception(std::current_exception());
            }
        }

        TKey key;
        for (auto& [_, value] : values) {
            key.push_back(std::move(value));
        }

        return key;
    }

private:
    const std::vector<TColumn> Columns;
    const THashMap<TString, ui32> KeyColumnIds;
    const std::shared_ptr<TLog> Log;

}; // TKeyBuilder

ui64 AdjustedRecordSize(ui64 recordSize) {
    return recordSize + 1 /* \n */;
}

class TPartitionRows {
    void IncSize(ui64 keySize, ui64 recordSize) {
        MemSizeCounter += (keySize + recordSize);
        RecordsSizeCounter += AdjustedRecordSize(recordSize);
    }

    void DecSize(ui64 keySize, ui64 recordSize) {
        MemSizeCounter -= (keySize + recordSize);
        RecordsSizeCounter -= AdjustedRecordSize(recordSize);
    }

public:
    TPartitionRows()
        : MemSizeCounter(0)
        , RecordsSizeCounter(0)
    {
    }

    ui64 MemSize() const {
        return MemSizeCounter;
    }

    ui64 RecordsSize() const {
        return RecordsSizeCounter;
    }

    bool Add(TKey&& key, NPrivate::TLine&& record) {
        auto ret = Rows.emplace(std::move(key), std::move(record));
        if (!ret.second) {
            return ret.second;
        }

        IncSize(ret.first->first.MemSize(), ret.first->second.size());
        return ret.second;
    }

    bool Empty() const {
        return Rows.empty();
    }

    auto Pop() {
        Y_ENSURE(!Empty());
        auto ret = Rows.extract(Rows.begin());

        DecSize(ret.key().MemSize(), ret.mapped().size());
        return ret;
    }

    NPrivate::TBatch Serialize(ui64 maxSize) {
        NPrivate::TBatch result;

        if (Empty()) {
            return result;
        }

        auto handle = Pop();
        do {
            result.Add(handle.mapped());

            if (Empty()) {
                return result;
            }

            handle = Pop();
        } while (result.size() + handle.mapped().size() < maxSize);

        Add(std::move(handle.key()), std::move(handle.mapped()));
        return result;
    }

private:
    TMap<TKey, NPrivate::TLine> Rows;
    ui64 MemSizeCounter;
    ui64 RecordsSizeCounter;

}; // TPartitionRows

class TTableRows {
    template <typename TIterator>
    struct TIteratorHash {
        size_t operator()(const TIterator& x) const {
            return THash<intptr_t>()(reinterpret_cast<intptr_t>(&*x));
        }
    };

    // key is a right-side boundary of partition
    using TRows = TMap<TSplitPoint, TPartitionRows>;
    using TRowsBy = TMap<ui64, THashSet<TRows::iterator, TIteratorHash<TRows::iterator>>, TGreater<ui64>>;

    static auto MakeSplitPoints(const std::vector<TKeyRange>& keyRanges) {
        Y_ENSURE(!keyRanges.empty());

        TVector<TSplitPoint> splitPoints;

        auto it = keyRanges.begin();
        while (++it != keyRanges.end()) {
            const auto& from = it->From();

            Y_ENSURE(from.has_value());
            Y_ENSURE(from->IsInclusive());

            TValueParser parser(from->GetValue());
            splitPoints.push_back(TValueConverter<TValueParser>(parser).ConvertAll());
        }

        return splitPoints;
    }

    static auto MakeEmptyRows(TVector<TSplitPoint>&& splitPoints) {
        TRows rows;

        for (auto& splitPoint : splitPoints) {
            rows.emplace(std::move(splitPoint), TPartitionRows());
        }

        // last boundary is +inf
        rows.emplace(TSplitPoint{TValue::Inf()}, TPartitionRows());

        return rows;
    }

    template <typename T>
    static auto FindPartition(T& rows, const TKey& key) {
        Y_ENSURE(!rows.empty());
        auto it = rows.begin();

        while (it != rows.end() && !(key < it->first)) {
            ++it;
        }

        return it;
    }

    static auto Add(TRows& emplaceTo, TKey&& key, NPrivate::TLine&& record) {
        auto it = FindPartition(emplaceTo, key);
        Y_ENSURE(it != emplaceTo.end());

        auto& rows = it->second;
        return std::make_pair(it, rows.Add(std::move(key), std::move(record)));
    }

    static void RemoveFromSizeTracker(TRowsBy& container, ui64 prevSize, TRows::iterator rowIt) {
        auto bucket = container.find(prevSize);
        Y_ENSURE(bucket != container.end());

        auto it = bucket->second.find(rowIt);
        Y_ENSURE(it != bucket->second.end());

        bucket->second.erase(it);

        if (bucket->second.empty()) {
            container.erase(bucket);
        }
    }

public:
    explicit TTableRows(const std::vector<TKeyRange>& keyRanges)
        : ByPartition(MakeEmptyRows(MakeSplitPoints(keyRanges)))
        , MemSize(0)
    {
    }

    bool CanAdd(const TKey& key, const TString& record, ui64 memLimit, ui64 batchSize) const {
        if (MemSize + key.MemSize() + record.size() >= memLimit) {
            return MemSize == 0;
        }

        auto it = FindPartition(ByPartition, key);
        Y_ENSURE(it != ByPartition.end());

        if (it->second.RecordsSize() + record.size() >= batchSize) {
            return it->second.RecordsSize() == 0;
        }

        return true;
    }

    void Add(TKey&& key, NPrivate::TLine&& record) {
        Y_ENSURE(key);

        const ui64 recordSize = record.size();
        const ui64 memSize = key.MemSize() + recordSize;

        auto ret = Add(ByPartition, std::move(key), std::move(record));

        if (ret.second) {
            auto update = [it = ret.first](TRowsBy& container, ui64 prevSize, ui64 newSize) {
                if (prevSize) {
                    RemoveFromSizeTracker(container, prevSize, it);
                }

                Y_ENSURE(container[newSize].insert(it).second);
            };

            const auto& rows = ret.first->second;
            update(ByMemSize, (rows.MemSize() - memSize), rows.MemSize());
            update(ByRecordsSize, (rows.RecordsSize() - AdjustedRecordSize(recordSize)), rows.RecordsSize());

            MemSize += memSize;
        }
    }

    void Reshard(const std::vector<TKeyRange>& keyRanges) {
        auto newByPartition = MakeEmptyRows(MakeSplitPoints(keyRanges));

        for (auto& [_, rows] : ByPartition) {
            while (!rows.Empty()) {
                auto handle = rows.Pop();
                Y_ENSURE(Add(newByPartition, std::move(handle.key()), std::move(handle.mapped())).second);
            }
        }

        ByMemSize.clear();
        ByRecordsSize.clear();
        ByPartition = std::move(newByPartition);

        for (auto it = ByPartition.begin(); it != ByPartition.end(); ++it) {
            Y_ENSURE(ByMemSize[it->second.MemSize()].insert(it).second);
            Y_ENSURE(ByRecordsSize[it->second.RecordsSize()].insert(it).second);
        }
    }

    bool HasData(ui64 memLimit, ui64 batchSize, bool force = false) const {
        if (ByMemSize.empty()) {
            Y_ENSURE(ByRecordsSize.empty());
            return false;
        }

        Y_ENSURE(!ByMemSize.begin()->second.empty());
        Y_ENSURE(!ByRecordsSize.begin()->second.empty());

        if (MemSize >= memLimit) {
            return true;
        }

        auto it = ByRecordsSize.begin()->second.begin();
        auto& rows = (*it)->second;

        if (rows.RecordsSize() >= batchSize || (force && !rows.Empty())) {
            return true;
        }

        return false;
    }

    NPrivate::TBatch GetData(ui64 memLimit, ui64 batchSize, bool force = false) {
        Y_ENSURE(HasData(memLimit, batchSize, force));
        Y_ENSURE(!ByMemSize.empty());
        Y_ENSURE(!ByRecordsSize.empty());
        Y_ENSURE(!ByMemSize.begin()->second.empty());
        Y_ENSURE(!ByRecordsSize.begin()->second.empty());

        auto get = [this, batchSize](TRowsBy& from) {
            auto it = *from.begin()->second.begin();
            auto& rows = it->second;

            RemoveFromSizeTracker(ByMemSize, rows.MemSize(), it);
            RemoveFromSizeTracker(ByRecordsSize, rows.RecordsSize(), it);

            MemSize -= rows.MemSize();
            auto ret = rows.Serialize(batchSize);
            MemSize += rows.MemSize();

            if (rows.MemSize()) {
                Y_ENSURE(ByMemSize[rows.MemSize()].insert(it).second);
            }
            if (rows.RecordsSize()) {
                Y_ENSURE(ByRecordsSize[rows.RecordsSize()].insert(it).second);
            }

            return ret;
        };

        if (MemSize >= memLimit) {
            return get(ByMemSize);
        }

        return get(ByRecordsSize);
    }

private:
    TRows ByPartition;
    TRowsBy ByMemSize;
    TRowsBy ByRecordsSize;

    ui64 MemSize;

}; // TTableRows

class TDataAccumulator: public NPrivate::IDataAccumulator {
public:
    explicit TDataAccumulator(
            const TTableDescription& dumpedDesc,
            const TTableDescription& actualDesc,
            const TRestoreSettings& settings,
            const std::shared_ptr<TLog>& log)
        : KeyBuilder(dumpedDesc.GetColumns(), dumpedDesc.GetPrimaryKeyColumns(), log)
        , MemLimit(settings.MemLimit_)
        , BatchSize(settings.BytesPerRequest_)
        , Rows(actualDesc.GetKeyRanges())
    {
    }

    EStatus Check(const NPrivate::TLine& line) const override {
        TGuard<TMutex> lock(Mutex);
        if (const auto key = KeyBuilder.Build(line)) {
            return Rows.CanAdd(key, line, MemLimit, BatchSize) ? OK : FULL;
        } else {
            return ERROR;
        }
    }

    void Feed(NPrivate::TLine&& line) override {
        TGuard<TMutex> lock(Mutex);
        Rows.Add(KeyBuilder.Build(line), std::move(line));
    }

    void Feed(const NPrivate::TBatch& data) {
        TGuard<TMutex> lock(Mutex);

        TStringInput input(data.GetData());
        TString line;

        ui64 idx = 0;
        while (input.ReadLine(line)) {
            auto l = NPrivate::TLine(std::move(line), data.GetLocation(idx++));
            Rows.Add(KeyBuilder.Build(l), std::move(l));
        }
    }

    bool Ready(bool force) const override {
        TGuard<TMutex> lock(Mutex);
        return Rows.HasData(MemLimit, BatchSize, force);
    }

    NPrivate::TBatch GetData(bool force) override {
        TGuard<TMutex> lock(Mutex);
        auto batch = Rows.GetData(MemLimit, BatchSize, force);
        batch.SetOriginAccumulator(this);
        return batch;
    }

    void Reshard(const std::vector<TKeyRange>& keyRanges) {
        TGuard<TMutex> lock(Mutex);
        Rows.Reshard(keyRanges);
    }

private:
    const TKeyBuilder KeyBuilder;
    const ui64 MemLimit;
    const ui64 BatchSize;

    TTableRows Rows;
    TMutex Mutex;

}; // TDataAccumulator

class TDataWriter: public NPrivate::IDataWriter {
    static auto MakeSettings(const TRestoreSettings& settings, const TTableDescription& desc) {
        auto importDataSettings = TImportYdbDumpDataSettings(settings)
            .RequestType(DOC_API_REQUEST_TYPE);

        for (const auto& column : desc.GetColumns()) {
            importDataSettings.AppendColumns(column.Name);
        }

        return importDataSettings;
    }

    bool Write(const NPrivate::TBatch& data) {
        const ui32 maxRetries = 10;
        TDuration retrySleep = TDuration::MilliSeconds(500);

        for (ui32 retryNumber = 0; retryNumber <= maxRetries; ++retryNumber) {
            while (!RequestLimiter.IsAvail()) {
                Sleep(Min(TDuration::MicroSeconds(RequestLimiter.GetWaitTime()), RateLimiterSettings.ReactionTime_));
                if (IsStopped()) {
                    return false;
                }
            }

            if (IsStopped()) {
                return false;
            }

            RequestLimiter.Use(1);

            auto importResult = ImportClient.ImportData(Path, TString{data}, Settings).GetValueSync();

            if (importResult.IsSuccess()) {
                return true;
            }

            if (retryNumber == maxRetries) {
                LOG_E("There is no retries left, last result: " << importResult);
                SetError(std::move(importResult));
                return false;
            }

            switch (importResult.GetStatus()) {
                case EStatus::PRECONDITION_FAILED: {
                    LOG_D("Partitioning of " << Path.Quote() << " has been changed while importing: " << data.GetLocation());
                    TMaybe<TTableDescription> desc;
                    auto descResult = DescribeTable(TableClient, Path, desc);
                    if (!descResult.IsSuccess()) {
                        LOG_E("Error describing table " << Path.Quote() << ": " << descResult.GetIssues().ToOneLineString());
                        SetError(std::move(descResult));
                        return false;
                    }

                    for (auto* acc : Accumulators) {
                        acc->Reshard(desc->GetKeyRanges());
                    }

                    auto* originAcc = dynamic_cast<TDataAccumulator*>(data.GetOriginAccumulator());
                    Y_ENSURE(originAcc);
                    originAcc->Feed(data);

                    return true;
                }

                case EStatus::ABORTED:
                    break;

                case EStatus::OVERLOADED:
                case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                    NConsoleClient::ExponentialBackoff(retrySleep);
                    break;

                case EStatus::UNAVAILABLE:
                    NConsoleClient::ExponentialBackoff(retrySleep);
                    break;

                case EStatus::TRANSPORT_UNAVAILABLE:
                    NConsoleClient::ExponentialBackoff(retrySleep);
                    break;

                default:
                    LOG_E("Can't import data to " << Path.Quote()
                          << " at location " << data.GetLocation() 
                          << ", result: " << importResult);
                    SetError(std::move(importResult));
                    return false;
            }
        }

        return false;
    }

    void SetError(TStatus&& error) {
        TGuard<TMutex> lock(Mutex);
        if (!Error) {
            Error = std::move(error);
        }
    }

    void Stop() {
        AtomicSet(Stopped, 1);
    }

    bool IsStopped() const {
        return AtomicGet(Stopped) == 1;
    }

public:
    explicit TDataWriter(
            const TString& path,
            const TTableDescription& desc,
            ui32 partitionCount,
            const TRestoreSettings& settings,
            TImportClient& importClient,
            TTableClient& tableClient,
            const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators,
            const std::shared_ptr<TLog>& log)
        : Path(path)
        , Settings(MakeSettings(settings, desc))
        , ImportClient(importClient)
        , TableClient(tableClient)
        , Accumulators(accumulators.size())
        , Log(log)
        , RateLimiterSettings(settings.RateLimiterSettings_)
        , RequestLimiter(RateLimiterSettings.GetRps(), RateLimiterSettings.GetRps())
        , Stopped(0)
    {
        Y_ENSURE(!accumulators.empty());
        for (size_t i = 0; i < accumulators.size(); ++i) {
            Accumulators[i] = dynamic_cast<TDataAccumulator*>(accumulators[i].Get());
            Y_ENSURE(Accumulators[i]);
        }

        TasksQueue = MakeHolder<TThreadPool>(TThreadPool::TParams().SetBlocking(true).SetCatching(true));

        size_t threadCount = settings.MaxInFlight_;
        if (!threadCount) {
            threadCount = Min<size_t>(partitionCount, NSystemInfo::CachedNumberOfCpus());
        }

        TasksQueue->Start(threadCount, threadCount + 1);
    }

    bool Push(NPrivate::TBatch&& data) override {
        if (data.size() > TRestoreSettings::MaxImportDataBytesPerRequest) {
            LOG_E("Too much data: " << data.GetLocation());
            return false;
        }

        if (IsStopped()) {
            return false;
        }

        auto func = [this, data = std::move(data)]() {
            if (!Write(data)) {
                Stop();
            }
        };

        return TasksQueue->AddFunc(std::move(func));
    }

    void Wait() override {
        TasksQueue->Stop();
        if (Error) {
            throw NStatusHelpers::TYdbErrorException(std::move(*Error));
        }
    }

private:
    const TString Path;
    const TImportYdbDumpDataSettings Settings;
    TImportClient& ImportClient;
    TTableClient& TableClient;
    TVector<TDataAccumulator*> Accumulators;
    const std::shared_ptr<TLog> Log;

    const TRateLimiterSettings RateLimiterSettings;

    using TRpsLimiter = TBucketQuoter<ui64>;
    TRpsLimiter RequestLimiter;

    THolder<IThreadPool> TasksQueue;
    TAtomic Stopped;

    TMaybe<TStatus> Error;
    TMutex Mutex;

}; // TDataWriter

} // anonymous

NPrivate::IDataAccumulator* CreateImportDataAccumulator(
        const TTableDescription& dumpedDesc,
        const TTableDescription& actualDesc,
        const TRestoreSettings& settings,
        const std::shared_ptr<TLog>& log)
{
    return new TDataAccumulator(dumpedDesc, actualDesc, settings, log);
}

NPrivate::IDataWriter* CreateImportDataWriter(
        const TString& path,
        const TTableDescription& desc,
        ui32 partitionCount,
        TImportClient& importClient,
        TTableClient& tableClient,
        const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators,
        const TRestoreSettings& settings,
        const std::shared_ptr<TLog>& log)
{
    return new TDataWriter(path, desc, partitionCount, settings, importClient, tableClient, accumulators, log);
}

} // NYdb::NDump
