#include "yt_codec_io.h"

#include <ydb/library/yql/providers/common/codec/yql_restricted_yson.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <exception>
#ifndef MKQL_DISABLE_CODEGEN
#include <ydb/library/yql/providers/yt/codec/codegen/yt_codec_cg.h>
#endif
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/swap_bytes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/varint.h>
#include <library/cpp/streams/brotli/brotli.h>

#include <util/generic/yexception.h>
#include <util/generic/string.h>
#include <util/generic/queue.h>
#include <util/generic/xrange.h>
#include <util/system/guard.h>
#include <util/system/yassert.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/stream/file.h>

#include <functional>

namespace NYql {

using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYT;
using namespace ::NYson::NDetail;

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TStatKey InputDecodeTime("Job_InputDecodeTime", true);
TStatKey InputReadTime("Job_InputReadTime", true);
TStatKey OutputEncodeTime("Job_OutputEncodeTime", true);
TStatKey OutputWriteTime("Job_OutputWriteTime", true);

struct TBlock : public TThrRefBase {
    size_t Avail_ = 0;
    std::optional<size_t> LastRecordBoundary_;
    const size_t Capacity_;
    TArrayHolder<char> Buffer_;
    TMaybe<yexception> Error_;

    explicit TBlock(size_t capacity)
        : Capacity_(capacity)
        , Buffer_(new char[Capacity_])
    {}
};

using TBlockPtr = TIntrusivePtr<TBlock>;

struct TBufferManager {
    const size_t BlockCount_;
    const size_t BlockSize_;
    TQueue<TBlockPtr> FreeBlocks_;
    TQueue<TBlockPtr> FilledBlocks_;

    TBufferManager(size_t blockCount, size_t blockSize)
        : BlockCount_(blockCount)
        , BlockSize_(blockSize)
    {
        // mark all blocks as free
        for (size_t i = 0; i < BlockCount_; ++i) {
            FreeBlocks_.push(MakeIntrusive<TBlock>(BlockSize_));
        }
    }
};

size_t LoadWithTimeout(IInputStream& source, char* buffer_in, size_t len, const TMaybe<TInstant>& deadline) {
    char* buf = buffer_in;
    while (len) {
        const size_t ret = source.Read(buf, len);

        buf += ret;
        len -= ret;

        if (ret == 0) {
            break;
        }

        if (deadline && Now() > deadline) {
            ythrow TTimeoutException() << "Read timeout";
        }
    }

    return buf - buffer_in;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TAsyncBlockReader: public IBlockReader {
public:
    TAsyncBlockReader(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize)
        : Source_(source)
        , BufMan_(blockCount, blockSize)
        , Thread_(TThread::TParams(&ThreadFunc, this).SetName("reader"))
    {
        Thread_.Start();
    }

    ~TAsyncBlockReader() {
        with_lock(Mutex_) {
            Shutdown_ = true;
            CondRestore_.Signal();
            CondFill_.Signal();
        }

        Thread_.Join();
    }

    void SetDeadline(TInstant deadline) override {
        Deadline_ = deadline;
    }

    std::pair<const char*, const char*> NextFilledBlock() override {
        TBlockPtr firstBlock;
        with_lock(Mutex_) {
            while (BufMan_.FilledBlocks_.empty()) {
                CondRead_.WaitI(Mutex_);
            }

            firstBlock = BufMan_.FilledBlocks_.front();
        }

        if (firstBlock->Error_.Defined()) {
            YQL_LOG_CTX_THROW *firstBlock->Error_;
        }
        return { firstBlock->Buffer_.Get(), firstBlock->Buffer_.Get() + firstBlock->Avail_ };
    }

    void ReturnBlock() override {
        auto guard = Guard(Mutex_);

        auto firstBlock = BufMan_.FilledBlocks_.front();
        firstBlock->Avail_ = 0;
        firstBlock->Error_.Clear();
        BufMan_.FilledBlocks_.pop();
        BufMan_.FreeBlocks_.push(firstBlock);

        CondFill_.Signal();
    }

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override {
        auto guard = Guard(Mutex_);

        // Clean all filled blocks
        while (!BufMan_.FilledBlocks_.empty()) {
            auto block = BufMan_.FilledBlocks_.front();
            block->Avail_ = 0;
            block->Error_.Clear();
            BufMan_.FilledBlocks_.pop();
            BufMan_.FreeBlocks_.push(block);
        }

        if (!Source_.Retry(rangeIndex, rowIndex, error)) {
            Shutdown_ = true;
            CondRestore_.Signal();
            return false;
        }
        CondRestore_.Signal();
        return true;
    }

private:
    static void* ThreadFunc(void* param) {
        static_cast<TAsyncBlockReader*>(param)->Fetch();
        return nullptr;
    }

    void Fetch() {
        for (;;) {
            TBlockPtr freeBlock;
            with_lock(Mutex_) {
                while (BufMan_.FreeBlocks_.empty() && !Shutdown_) {
                    CondFill_.WaitI(Mutex_);
                }

                if (Shutdown_) {
                    return;
                }

                freeBlock = BufMan_.FreeBlocks_.front();
                BufMan_.FreeBlocks_.pop();
            }

            bool hasError = false;
            try {
                freeBlock->Avail_ = LoadWithTimeout(Source_, freeBlock->Buffer_.Get(), freeBlock->Capacity_, Deadline_);
            }
            catch (yexception& e) {
                freeBlock->Error_ = std::move(e);
                hasError = true;
            }
            catch (...) {
                freeBlock->Error_ = (yexception() << CurrentExceptionMessage());
                hasError = true;
            }

            const bool lastBlock = !freeBlock->Avail_;
            with_lock(Mutex_) {
                BufMan_.FilledBlocks_.push(freeBlock);
                CondRead_.Signal();
                if (hasError) {
                    CondRestore_.WaitI(Mutex_);
                    if (Shutdown_) {
                        return;
                    }
                } else if (lastBlock) {
                    break;
                }
            }
        }
    }

private:
    TMaybe<TInstant> Deadline_;
    NYT::TRawTableReader& Source_;
    TBufferManager BufMan_;
    TMutex Mutex_;
    TCondVar CondFill_;
    TCondVar CondRead_;
    TCondVar CondRestore_;
    bool Shutdown_ = false;
    TThread Thread_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TSyncBlockReader: public IBlockReader {
public:
    TSyncBlockReader(NYT::TRawTableReader& source, size_t blockSize)
        : Source_(source)
        , Block_(blockSize)
    {
    }

    ~TSyncBlockReader() = default;

    void SetDeadline(TInstant deadline) override {
        Deadline_ = deadline;
    }

    std::pair<const char*, const char*> NextFilledBlock() override {
        Block_.Avail_ = LoadWithTimeout(Source_, Block_.Buffer_.Get(), Block_.Capacity_, Deadline_);
        return { Block_.Buffer_.Get(), Block_.Buffer_.Get() + Block_.Avail_ };
    }

    void ReturnBlock() override {
        Block_.Avail_ = 0;
    }

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override {
        Block_.Avail_ = 0;
        return Source_.Retry(rangeIndex, rowIndex, error);
    }

private:
    NYT::TRawTableReader& Source_;
    TBlock Block_;
    TMaybe<TInstant> Deadline_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

THolder<IBlockReader> MakeBlockReader(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize) {
    THolder<IBlockReader> res;
    if (blockCount > 1) {
        res.Reset(new TAsyncBlockReader(source, blockCount, blockSize));
    } else {
        res.Reset(new TSyncBlockReader(source, blockSize));
    }
    return res;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TAsyncBlockWriter: public IBlockWriter {
public:
    TAsyncBlockWriter(IOutputStream& target, size_t blockCount, size_t blockSize)
        : Target_(target)
        , BufMan_(blockCount, blockSize)
        , Thread_(TThread::TParams(&ThreadFunc, this).SetName("writer"))
    {
        Thread_.Start();
    }

    ~TAsyncBlockWriter() {
        Stop();
    }

    void SetRecordBoundaryCallback(std::function<void()> callback) override {
        OnRecordBoundaryCallback_ = std::move(callback);
    }

    std::pair<char*, char*> NextEmptyBlock() override {
        TBlockPtr firstBlock;
        with_lock(Mutex_) {
            while (BufMan_.FreeBlocks_.empty()) {
                CondFill_.WaitI(Mutex_);
            }
            if (Error_.Defined()) {
                YQL_LOG_CTX_THROW *Error_;
            }

            firstBlock = BufMan_.FreeBlocks_.front();
        }

        firstBlock->Avail_ = 0;
        return { firstBlock->Buffer_.Get(), firstBlock->Buffer_.Get() + firstBlock->Capacity_ };
    }

    void ReturnBlock(size_t avail, std::optional<size_t> lastRecordBoundary) override {
        if (!avail) {
            return;
        }

        with_lock(Mutex_) {
            auto firstBlock = BufMan_.FreeBlocks_.front();
            firstBlock->Avail_ = avail;
            firstBlock->LastRecordBoundary_ = lastRecordBoundary;
            BufMan_.FilledBlocks_.push(firstBlock);
            BufMan_.FreeBlocks_.pop();
        }

        CondWrite_.Signal();
    }

    void Finish() override {
        Stop();
        if (Error_.Defined()) {
            YQL_LOG_CTX_THROW *Error_;
        }
    }

private:
    static void* ThreadFunc(void* param) {
        static_cast<TAsyncBlockWriter*>(param)->Write();
        return nullptr;
    }

    void Stop() {
        with_lock(Mutex_) {
            Shutdown_ = true;
        }

        CondWrite_.Signal();
        Thread_.Join();
    }

    void Write() {
        for (;;) {
            TBlockPtr firstBlock;
            bool needShutdown = false;
            with_lock(Mutex_) {
                while (BufMan_.FilledBlocks_.empty()) {
                    if (Shutdown_) {
                        needShutdown = true;
                        break;
                    }

                    CondWrite_.WaitI(Mutex_);
                }

                if (!needShutdown) {
                    firstBlock = BufMan_.FilledBlocks_.front();
                    BufMan_.FilledBlocks_.pop();
                }
            }

            if (needShutdown) {
                try {
                    Target_.Finish();
                }
                catch (yexception& e) {
                    auto guard = Guard(Mutex_);
                    Error_ = std::move(e);
                }
                catch (...) {
                    auto guard = Guard(Mutex_);
                    Error_ = (yexception() << CurrentExceptionMessage());
                }
                return;
            }

            try {
                Target_.Write(firstBlock->Buffer_.Get(), firstBlock->LastRecordBoundary_.value_or(firstBlock->Avail_));
                if (firstBlock->LastRecordBoundary_) {
                    if (OnRecordBoundaryCallback_) {
                        OnRecordBoundaryCallback_();
                    }
                    if (firstBlock->Avail_ > *firstBlock->LastRecordBoundary_) {
                        Target_.Write(firstBlock->Buffer_.Get() + *firstBlock->LastRecordBoundary_, firstBlock->Avail_ - *firstBlock->LastRecordBoundary_);
                    }
                }

            }
            catch (yexception& e) {
                auto guard = Guard(Mutex_);
                Error_ = std::move(e);
            }
            catch (...) {
                auto guard = Guard(Mutex_);
                Error_ = (yexception() << CurrentExceptionMessage());
            }

            with_lock(Mutex_) {
                firstBlock->Avail_ = 0;
                BufMan_.FreeBlocks_.push(firstBlock);
            }

            CondFill_.Signal();
        }
    }

private:
    IOutputStream& Target_;
    TBufferManager BufMan_;
    TMutex Mutex_;
    TCondVar CondFill_;
    TCondVar CondWrite_;
    bool Shutdown_ = false;
    TThread Thread_;
    TMaybe<yexception> Error_;
    std::function<void()> OnRecordBoundaryCallback_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TSyncBlockWriter: public IBlockWriter {
public:
    TSyncBlockWriter(IOutputStream& target, size_t blockSize)
        : Target_(target)
        , Block_(blockSize)
    {
    }

    void SetRecordBoundaryCallback(std::function<void()> callback) override {
        OnRecordBoundaryCallback_ = std::move(callback);
    }

    std::pair<char*, char*> NextEmptyBlock() override {
        Block_.Avail_ = 0;
        return { Block_.Buffer_.Get(), Block_.Buffer_.Get() + Block_.Capacity_ };
    }

    void ReturnBlock(size_t avail, std::optional<size_t> lastRecordBoundary) override {
        if (!avail) {
            return;
        }

        Target_.Write(Block_.Buffer_.Get(), lastRecordBoundary.value_or(avail));
        if (lastRecordBoundary) {
            if (OnRecordBoundaryCallback_) {
                OnRecordBoundaryCallback_();
            }
            if (avail > *lastRecordBoundary) {
                Target_.Write(Block_.Buffer_.Get() + *lastRecordBoundary, avail - *lastRecordBoundary);
            }
        }
    }

    void Finish() override {
        Target_.Finish();
        Finished_ = true;
    }

private:
    IOutputStream& Target_;
    TBlock Block_;
    bool Finished_ = false;
    std::function<void()> OnRecordBoundaryCallback_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: share buffer manager between all writers (with at least <out table count> + 1 blocks)
THolder<IBlockWriter> MakeBlockWriter(IOutputStream& target, size_t blockCount, size_t blockSize) {
    THolder<IBlockWriter> res;
    if (blockCount > 1) {
        res.Reset(new TAsyncBlockWriter(target, blockCount, blockSize));
    } else {
        res.Reset(new TSyncBlockWriter(target, blockSize));
    }
    return res;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

inline void WriteWithLength(IOutputStream& out, TStringBuf val) {
    ::NYson::WriteVarInt32(&out, val.size());
    out.Write(val.data(), val.size());
}

inline void WriteWithLength(IOutputStream& out, const NUdf::TUnboxedValue& val) {
    WriteWithLength(out, val.AsStringRef());
}

void WriteRowItems(TMkqlIOCache& specsCache, size_t tableIndex,
    const TVector<TString>& items,
    const TMaybe<THashMap<TString, TString>>& others,
    IOutputStream& out)
{
    auto& decoder = *specsCache.GetSpecs().Inputs[tableIndex];
    // Output
    out.Write(BeginListSymbol);
    for (ui32 index = 0; index < decoder.StructSize; ++index) {
        if (!items[index].empty()) {
            out.Write(items[index].data(), items[index].size());
            out.Write(ListItemSeparatorSymbol);
            continue;
        }

        if (decoder.OthersStructIndex && *decoder.OthersStructIndex == index) {
            specsCache.UpdateMaxOthersFields(tableIndex, others->size());

            out.Write(BeginListSymbol);
            for (const auto& oField : *others) {
                out.Write(BeginListSymbol);
                out.Write(StringMarker);
                WriteWithLength(out, oField.first);
                out.Write(ListItemSeparatorSymbol);
                out.Write(StringMarker);
                WriteWithLength(out, oField.second);
                out.Write(EndListSymbol);

                out.Write(ListItemSeparatorSymbol);
            }
            out.Write(EndListSymbol);
            out.Write(ListItemSeparatorSymbol);
            continue;
        }

        // missing value
        const auto& field = decoder.FieldsVec[index];
        if (field.Type->IsOptional() || field.Type->IsVoid() || field.Type->IsNull()) {
            out.Write(EntitySymbol);
            out.Write(ListItemSeparatorSymbol);
            continue;
        }

        const auto& defVal = decoder.DefaultValues[index];
        YQL_ENSURE(defVal, "Missing field data: " << field.Name);

        out.Write(StringMarker);
        WriteWithLength(out, defVal);
        out.Write(ListItemSeparatorSymbol);
    }
    out.Write(EndListSymbol);
    out.Write(ListItemSeparatorSymbol);
}

} // unnamed

//////////////////////////////////////////////////////////////////////////////////////////////////////////
struct TMkqlReaderImpl::TDecoder {
    TDecoder(TInputBuf& buf, const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : Buf_(buf)
        , SpecsCache_(specs, holderFactory)
    {
    }

    virtual ~TDecoder() = default;
    virtual bool DecodeNext(NKikimr::NUdf::TUnboxedValue*& items, TMaybe<NKikimr::NMiniKQL::TValuesDictHashMap>& others) = 0;

    NUdf::TUnboxedValue ReadOtherField(char cmd) {
        switch (cmd) {
        case EntitySymbol:
            return NUdf::TUnboxedValuePod::Embedded(TStringBuf("#"));

        case TrueMarker:
            return NUdf::TUnboxedValuePod::Embedded(TStringBuf("%true"));

        case FalseMarker:
            return NUdf::TUnboxedValuePod::Embedded(TStringBuf("%false"));

        case Int64Marker: {
            auto val = Buf_.ReadVarI64();
            char format[100];
            auto len = ToString(val, format, Y_ARRAY_SIZE(format));
            return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(format, len)));
        }

        case Uint64Marker: {
            auto val = Buf_.ReadVarUI64();
            char format[100];
            auto len = ToString(val, format, Y_ARRAY_SIZE(format) - 1);
            format[len] = 'u';
            return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(format, len + 1)));
        }

        case StringMarker: {
            const i32 length = Buf_.ReadVarI32();
            CHECK_STRING_LENGTH(length);
            auto val = NUdf::TUnboxedValue(MakeStringNotFilled(length));
            const auto& buf = val.AsStringRef();
            Buf_.ReadMany(buf.Data(), buf.Size());
            return val;
        }

        case DoubleMarker: {
            double val;
            Buf_.ReadMany((char*)&val, sizeof(val));
            char format[100];
            auto len = ToString(val, format, Y_ARRAY_SIZE(format));
            return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(format, len)));
        }

        }

        auto& yson = Buf_.YsonBuffer();
        yson.clear();
        CopyYsonWithAttrs(cmd, Buf_, yson);
        return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(yson.data(), yson.size())));
    }

    void EndStream() {
        Finished_ = true;
        Valid_ = false;
        KeySwitch_ = false;
    }

    void Reset(bool hasRangeIndices, ui32 tableIndex, bool ignoreStreamTableIndex) {
        HasRangeIndices_ = hasRangeIndices;
        TableIndex_ = tableIndex;
        AtStart_ = true;
        Valid_ = true;
        Finished_ = false;
        RowAlreadyRead_ = false;
        RowIndex_.Clear();
        RangeIndex_.Clear();
        Row_.Clear();
        IgnoreStreamTableIndex = ignoreStreamTableIndex;
        // Don't reset KeySwitch_ here, because this method is called when switching to a next table. It shouldn't touch key switch flag
    }

public:
    bool HasRangeIndices_ = false;
    bool AtStart_ = true;
    bool Valid_ = true;
    bool Finished_ = false;
    bool RowAlreadyRead_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    NKikimr::NUdf::TUnboxedValue Row_;
    bool IgnoreStreamTableIndex = false;
    bool KeySwitch_ = true;

protected:
    TInputBuf& Buf_;
    TMkqlIOCache SpecsCache_;
};


class TYsonDecoder: public TMkqlReaderImpl::TDecoder {
public:
    TYsonDecoder(TInputBuf& buf, const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : TMkqlReaderImpl::TDecoder(buf, specs, holderFactory)
    {
    }

    virtual ~TYsonDecoder() = default;

    bool DecodeNext(NUdf::TUnboxedValue*& items, TMaybe<TValuesDictHashMap>& others) final {
        char cmd;
        if (Finished_ || !Buf_.TryRead(cmd)) {
            EndStream();
            return false;
        }

        cmd = ReadAttrs(cmd);
        if (!Valid_) {
            return false;
        }

        CHECK_EXPECTED(cmd, BeginMapSymbol);
        AtStart_ = false;
        auto& decoder = *SpecsCache_.GetSpecs().Inputs[TableIndex_];
        auto& lastFields = SpecsCache_.GetLastFields(TableIndex_);
        Row_.Clear();
        Row_ = SpecsCache_.NewRow(TableIndex_, items);
        if (decoder.OthersStructIndex) {
            others.ConstructInPlace(SpecsCache_.GetMaxOthersFields(TableIndex_),
                TValueHasher(decoder.OthersKeyTypes, false, nullptr),
                TValueEqual(decoder.OthersKeyTypes, false, nullptr));
        }

        ui32 fieldNo = 0;
        cmd = Buf_.Read();

        for (;;) {
            if (cmd == EndMapSymbol) {
                break;
            }
            CHECK_EXPECTED(cmd, StringMarker);
            auto name = Buf_.ReadYtString(2); // KeyValueSeparatorSymbol + subsequent cmd
            EXPECTED(Buf_, KeyValueSeparatorSymbol);
            const TMkqlIOSpecs::TDecoderSpec::TDecodeField* field = nullptr;
            if (!decoder.OthersStructIndex || name != YqlOthersColumnName) {
                if (fieldNo < lastFields.size()) {
                    auto lastField = lastFields[fieldNo];
                    if (lastField->Name == name) {
                        field = lastField;
                    }
                }

                if (!field) {
                    auto it = decoder.Fields.find(name);
                    if (it != decoder.Fields.end()) {
                        field = &it->second;
                    }
                }
            }

            if (!field) {
                YQL_ENSURE(decoder.OthersStructIndex, "Unexpected field: " << name << " in strict scheme");
                cmd = Buf_.Read();
                // 'name' buffer may be invalidated in ReadOtherField()
                auto key = NUdf::TUnboxedValue(MakeString(name));
                auto isStringKey = (cmd != StringMarker) ? NUdf::TUnboxedValue() :
                    NUdf::TUnboxedValue(MakeString(TStringBuilder() << "_yql_" << name));
                try {
                    auto value = ReadOtherField(cmd);
                    if (isStringKey) {
                        auto empty = NUdf::TUnboxedValue::Zero();
                        others->emplace(std::move(isStringKey), std::move(empty));
                    }
                    others->emplace(std::move(key), std::move(value));
                }
                catch (const TYqlPanic& e) {
                    ythrow TYqlPanic() << "Failed to read field: '" << TStringBuf(key.AsStringRef()) << "'\n" << e.what();
                }
                catch (...) {
                    ythrow yexception() << "Failed to read field: '" << TStringBuf(key.AsStringRef()) << "'\n" << CurrentExceptionMessage();
                }
            } else {
                try {
                    if (Y_LIKELY(field->StructIndex != Max<ui32>())) {
                        items[field->StructIndex] = ReadField(field->Type);
                    } else {
                        SkipField(field->Type);
                    }
                }
                catch (const TYqlPanic& e) {
                    ythrow TYqlPanic() << "Failed to read field: '" << name << "'\n" << e.what();
                }
                catch (...) {
                    ythrow yexception() << "Failed to read field: '" << name << "'\n" << CurrentExceptionMessage();
                }

                if (fieldNo < lastFields.size()) {
                    lastFields[fieldNo] = field;
                }

                ++fieldNo;
            }

            cmd = Buf_.Read();
            if (cmd == KeyedItemSeparatorSymbol) {
                cmd = Buf_.Read();
            }
        }

        if (!Buf_.TryRead(cmd)) {
            // don't EndStream here as it would throw away last record
            // (GetRow throws on invalidated stream)
            // instead it would be invalidated on next call
            // Finished_ is set to not call TryRead next time
            // because else TryRead causes an infinite loop
            Finished_ = true;
            return true;
        }

        CHECK_EXPECTED(cmd, ListItemSeparatorSymbol);

        if (others) {
            SpecsCache_.UpdateMaxOthersFields(TableIndex_, others->size());
        }

        return true;
    }

protected:
    char ReadAttrs(char cmd) {
        while (cmd == BeginAttributesSymbol) {
            TMaybe<ui64> rowIndex;
            TMaybe<ui32> rangeIndex;
            cmd = Buf_.Read();

            for (;;) {
                if (cmd == EndAttributesSymbol) {
                    EXPECTED(Buf_, EntitySymbol);
                    // assume there's no control record at the end of the stream
                    EXPECTED(Buf_, ListItemSeparatorSymbol);

                    if (Valid_) {
                        if (!Buf_.TryRead(cmd)) {
                            EndStream();
                        }
                    }

                    break;
                }

                CHECK_EXPECTED(cmd, StringMarker);
                auto name = Buf_.ReadYtString();
                if (name == TStringBuf("row_index")) {
                    EXPECTED(Buf_, KeyValueSeparatorSymbol);
                    EXPECTED(Buf_, Int64Marker);
                    rowIndex = Buf_.ReadVarI64();
                }
                else if (name == TStringBuf("table_index")) {
                    EXPECTED(Buf_, KeyValueSeparatorSymbol);
                    EXPECTED(Buf_, Int64Marker);
                    const auto tableIndex = Buf_.ReadVarI64();
                    if (!IgnoreStreamTableIndex) {
                        TableIndex_ = tableIndex;
                        YQL_ENSURE(TableIndex_ < SpecsCache_.GetSpecs().Inputs.size());
                    }
                }
                else if (name == TStringBuf("key_switch")) {
                    EXPECTED(Buf_, KeyValueSeparatorSymbol);
                    EXPECTED(Buf_, TrueMarker);
                    if (!AtStart_) {
                        Valid_ = false;
                        KeySwitch_ = true;
                    }
                }
                else if (name == TStringBuf("range_index")) {
                    EXPECTED(Buf_, KeyValueSeparatorSymbol);
                    EXPECTED(Buf_, Int64Marker);
                    rangeIndex = Buf_.ReadVarI64();
                }
                else {
                    YQL_ENSURE(false, "Unsupported annotation:" << name);
                }

                cmd = Buf_.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }
            }
            if (rowIndex) {
                if (HasRangeIndices_) {
                    if (rangeIndex) {
                        RowIndex_ = rowIndex;
                        RangeIndex_ = rangeIndex;
                    }
                } else {
                    RowIndex_ = rowIndex;
                }
            }

        }
        return cmd;
    }

    NUdf::TUnboxedValue ReadField(TType* type) {
        auto cmd = Buf_.Read();
        if (type->IsNull()) {
            return NUdf::TUnboxedValue();
        }

        const bool isOptional = type->IsOptional();
        if (isOptional) {
            TType* uwrappedType = static_cast<TOptionalType*>(type)->GetItemType();
            if (cmd == EntitySymbol) {
                return NUdf::TUnboxedValue();
            }
            auto& decoder = *SpecsCache_.GetSpecs().Inputs[TableIndex_];
            auto val = ReadYsonValue((decoder.NativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) ? type : uwrappedType, decoder.NativeYtTypeFlags, SpecsCache_.GetHolderFactory(), cmd, Buf_, true);
            return (decoder.NativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) ? val : val.Release().MakeOptional();
        } else {
            if (Y_LIKELY(cmd != EntitySymbol)) {
                auto& decoder = *SpecsCache_.GetSpecs().Inputs[TableIndex_];
                return ReadYsonValue(type, decoder.NativeYtTypeFlags, SpecsCache_.GetHolderFactory(), cmd, Buf_, true);
            }

            if (type->GetKind() == TType::EKind::Data && static_cast<TDataType*>(type)->GetSchemeType() == NUdf::TDataType<NUdf::TYson>::Id) {
                return NUdf::TUnboxedValue::Embedded(TStringBuf("#"));
            }

            // Don't fail right here because field can be filled from DefaultValues
            return NUdf::TUnboxedValue();
        }
    }

    void SkipField(TType* type) {
        auto cmd = Buf_.Read();
        if (cmd != EntitySymbol) {
            SkipValue(type->IsOptional() ? static_cast<TOptionalType*>(type)->GetItemType() : type, cmd);
        }
    }

    void SkipValue(TType* type, char cmd) {
        switch (type->GetKind()) {
        case TType::EKind::Variant: {
            auto varType = static_cast<TVariantType*>(type);
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = Buf_.Read();
            YQL_ENSURE(cmd == Int64Marker || cmd == Uint64Marker, "Excepted [U]Int64 marker, but got: " << int(cmd));
            auto underlyingType = varType->GetUnderlyingType();
            YQL_ENSURE(underlyingType->IsTuple() || underlyingType->IsStruct(), "Wrong underlying type");
            TType* itemType;
            i64 index;

            if (cmd == Int64Marker) {
                index = Buf_.ReadVarI64();
            } else {
                index = Buf_.ReadVarUI64();
            }

            YQL_ENSURE(index > -1 && index < varType->GetAlternativesCount(), "Bad variant alternative: " << index << ", only " <<
                varType->GetAlternativesCount() << " are available");
            if (underlyingType->IsTuple()) {
                itemType = static_cast<TTupleType*>(underlyingType)->GetElementType(index);
            } else {
                itemType = static_cast<TStructType*>(underlyingType)->GetMemberType(index);
            }


            EXPECTED(Buf_, ListItemSeparatorSymbol);
            cmd = Buf_.Read();
            SkipValue(itemType, cmd);

            cmd = Buf_.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = Buf_.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            break;
        }

        case TType::EKind::Data: {
            auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
            switch (schemeType) {
            case NUdf::TDataType<bool>::Id:
                YQL_ENSURE(cmd == FalseMarker || cmd == TrueMarker, "Expected either true or false, but got: " << cmd);
                break;

            case NUdf::TDataType<ui8>::Id:
            case NUdf::TDataType<ui16>::Id:
            case NUdf::TDataType<ui32>::Id:
            case NUdf::TDataType<ui64>::Id:
            case NUdf::TDataType<NUdf::TDate>::Id:
            case NUdf::TDataType<NUdf::TDatetime>::Id:
            case NUdf::TDataType<NUdf::TTimestamp>::Id:
                CHECK_EXPECTED(cmd, Uint64Marker);
                Buf_.ReadVarUI64();
                break;

            case NUdf::TDataType<i8>::Id:
            case NUdf::TDataType<i16>::Id:
            case NUdf::TDataType<i32>::Id:
            case NUdf::TDataType<i64>::Id:
            case NUdf::TDataType<NUdf::TInterval>::Id:
            case NUdf::TDataType<NUdf::TDate32>::Id:
            case NUdf::TDataType<NUdf::TDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            case NUdf::TDataType<NUdf::TInterval64>::Id:
                CHECK_EXPECTED(cmd, Int64Marker);
                Buf_.ReadVarI64();
                break;

            case NUdf::TDataType<float>::Id:
            case NUdf::TDataType<double>::Id:
                CHECK_EXPECTED(cmd, DoubleMarker);
                Buf_.SkipMany(sizeof(double));
                break;

            case NUdf::TDataType<NUdf::TDecimal>::Id:
            case NUdf::TDataType<NUdf::TUtf8>::Id:
            case NUdf::TDataType<char*>::Id:
            case NUdf::TDataType<NUdf::TJson>::Id:
            case NUdf::TDataType<NUdf::TTzDate>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            case NUdf::TDataType<NUdf::TTzDate32>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp64>::Id:
            case NUdf::TDataType<NUdf::TDyNumber>::Id:
            case NUdf::TDataType<NUdf::TUuid>::Id:
            case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
                CHECK_EXPECTED(cmd, StringMarker);
                const i32 length = Buf_.ReadVarI32();
                CHECK_STRING_LENGTH(length);
                Buf_.SkipMany(length);
                break;
            }

            case NUdf::TDataType<NUdf::TYson>::Id: {
                auto& yson = Buf_.YsonBuffer();
                yson.clear();
                CopyYsonWithAttrs(cmd, Buf_, yson);
                break;
            }

            default:
                YQL_ENSURE(false, "Unsupported data type: " << schemeType);
            }
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType*>(type);
            YQL_ENSURE(cmd == BeginMapSymbol || cmd == BeginListSymbol);
            if (cmd == BeginListSymbol) {
                cmd = Buf_.Read();
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    SkipValue(structType->GetMemberType(i), cmd);

                    cmd = Buf_.Read();
                    if (cmd == ListItemSeparatorSymbol) {
                        cmd = Buf_.Read();
                    }
                }
                CHECK_EXPECTED(cmd, EndMapSymbol);
                break;
            }

            cmd = Buf_.Read();
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                CHECK_EXPECTED(cmd, StringMarker);
                const i32 length = Buf_.ReadVarI32();
                CHECK_STRING_LENGTH(length);
                TString name(length, '\0');
                Buf_.ReadMany((char*)name.data(), length);
                cmd = Buf_.Read();
                CHECK_EXPECTED(cmd, KeyValueSeparatorSymbol);

                auto idx = structType->FindMemberIndex(name);
                YQL_ENSURE(idx);
                cmd = Buf_.Read();
                SkipValue(structType->GetMemberType(*idx), cmd);

                cmd = Buf_.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }
            }
            CHECK_EXPECTED(cmd, EndMapSymbol);
            break;
        }

        case TType::EKind::List: {
            auto itemType = static_cast<TListType*>(type)->GetItemType();
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = Buf_.Read();

            for (;;) {
                if (cmd == EndListSymbol) {
                    break;
                }

                SkipValue(itemType, cmd);
                cmd = Buf_.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }
            }
            break;
        }

        case TType::EKind::Optional: {
            if (cmd == EntitySymbol) {
                return;
            }

            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = Buf_.Read();
            if (cmd == EndListSymbol) {
                return;
            }

            SkipValue(static_cast<TOptionalType*>(type)->GetItemType(), cmd);

            cmd = Buf_.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = Buf_.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            break;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = Buf_.Read();

            for (;;) {
                if (cmd == EndListSymbol) {
                    break;
                }

                CHECK_EXPECTED(cmd, BeginListSymbol);
                cmd = Buf_.Read();
                SkipValue(keyType, cmd);
                EXPECTED(Buf_, ListItemSeparatorSymbol);
                cmd = Buf_.Read();
                SkipValue(payloadType, cmd);

                cmd = Buf_.Read();
                // skip inner list separator
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }

                CHECK_EXPECTED(cmd, EndListSymbol);

                cmd = Buf_.Read();
                // skip outer list separator
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(type);
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = Buf_.Read();

            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                SkipValue(tupleType->GetElementType(i), cmd);

                cmd = Buf_.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = Buf_.Read();
                }

            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            break;
        }

        case TType::EKind::Void: {
            if (cmd == EntitySymbol) {
                return;
            }

            CHECK_EXPECTED(cmd, StringMarker);
            i32 length = Buf_.ReadVarI32();
            YQL_ENSURE(length == 4, "Expected Void");
            char buffer[4];
            Buf_.ReadMany(buffer, 4);
            YQL_ENSURE(TStringBuf(buffer, 4) == TStringBuf("Void"), "Expected Void");
            break;
        }

        default:
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }
};

class TSkiffDecoderBase: public TMkqlReaderImpl::TDecoder {
public:
    TSkiffDecoderBase(TInputBuf& buf, const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : TMkqlReaderImpl::TDecoder(buf, specs, holderFactory)
    {
    }

    bool DecodeNext(NUdf::TUnboxedValue*& items, TMaybe<TValuesDictHashMap>& others) final {
        char byte1;
        if (!Buf_.TryRead(byte1)) {
            EndStream();
            return false;
        }

        char byte2 = Buf_.Read();
        if (!IgnoreStreamTableIndex) {
            ui16 tableIndex = (ui16(ui8(byte2)) << 8) | ui8(byte1);
            TableIndex_ = tableIndex;
        }
        auto& spec = SpecsCache_.GetSpecs();
        YQL_ENSURE(TableIndex_ < spec.Inputs.size());

        auto& decoder = *spec.Inputs[TableIndex_];

        if (spec.SystemFields_.HasFlags(TMkqlIOSpecs::ESystemField::RangeIndex)) {
            auto cmd = Buf_.Read();
            if (cmd) {
                i64 value;
                Buf_.ReadMany((char*)&value, sizeof(value));
                YQL_ENSURE(HasRangeIndices_);
                RangeIndex_ = value;
            }
        }

        if (decoder.Dynamic) {
            RowIndex_.Clear();
        } else if (spec.SystemFields_.HasFlags(TMkqlIOSpecs::ESystemField::RowIndex)) {
            auto cmd = Buf_.Read();
            if (cmd) {
                i64 value;
                Buf_.ReadMany((char*)&value, sizeof(value));
                RowIndex_ = value;
            }
        }

        if (spec.SystemFields_.HasFlags(TMkqlIOSpecs::ESystemField::KeySwitch)) {
            auto cmd = Buf_.Read();
            if (!AtStart_ && cmd) {
                Valid_ = false;
                RowAlreadyRead_ = true;
                KeySwitch_ = true;
            }
        }

        AtStart_ = false;

        Row_ = NUdf::TUnboxedValue();
        Row_ = SpecsCache_.NewRow(TableIndex_, items);

        DecodeItems(items);

        if (decoder.OthersStructIndex) {
            // parse yson with other fields
            try {
                others.ConstructInPlace(SpecsCache_.GetMaxOthersFields(TableIndex_),
                    TValueHasher(decoder.OthersKeyTypes, false, nullptr),
                    TValueEqual(decoder.OthersKeyTypes, false, nullptr));
                ReadOtherSkiffFields(*others);
                SpecsCache_.UpdateMaxOthersFields(TableIndex_, others->size());
            } catch (const TYqlPanic& e) {
                ythrow TYqlPanic() << "Failed to read others\n" << e.what();
            } catch (...) {
                ythrow yexception() << "Failed to read others\n" << CurrentExceptionMessage();
            }
        }
        return true;
    }

protected:
    virtual void DecodeItems(NUdf::TUnboxedValue* items) = 0;

    void ReadOtherSkiffFields(TValuesDictHashMap& others) {
        ui32 size;
        Buf_.ReadMany((char*)&size, sizeof(size));
        YQL_ENSURE(size > 0);
        EXPECTED(Buf_, BeginMapSymbol);
        char cmd = Buf_.Read();

        for (;;) {
            if (cmd == EndMapSymbol) {
                break;
            }

            auto name = Buf_.ReadYtString(2); // KeyValueSeparatorSymbol + subsequent cmd
            EXPECTED(Buf_, KeyValueSeparatorSymbol);
            cmd = Buf_.Read();
            // 'name' buffer may be invalidated in ReadOtherField()
            auto key = NUdf::TUnboxedValue(MakeString(name));
            auto isStringKey = (cmd != StringMarker) ? NUdf::TUnboxedValue() :
                NUdf::TUnboxedValue(MakeString(TStringBuilder() << "_yql_" << name));
            try {
                auto value = ReadOtherField(cmd);
                if (isStringKey) {
                    auto empty = NUdf::TUnboxedValue::Zero();
                    others.emplace(std::move(isStringKey), std::move(empty));
                }
                others.emplace(std::move(key), std::move(value));
            } catch (const TYqlPanic& e) {
                ythrow TYqlPanic() << "Failed to read field: '" << TStringBuf(key.AsStringRef()) << "'\n" << e.what();
            } catch (...) {
                ythrow yexception() << "Failed to read field: '" << TStringBuf(key.AsStringRef()) << "'\n" << CurrentExceptionMessage();
            }

            cmd = Buf_.Read();
            if (cmd == KeyedItemSeparatorSymbol) {
                cmd = Buf_.Read();
            }
        }
    }
};

class TSkiffDecoder: public TSkiffDecoderBase {
public:
    TSkiffDecoder(TInputBuf& buf, const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : TSkiffDecoderBase(buf, specs, holderFactory)
    {
    }

protected:
    void DecodeItems(NUdf::TUnboxedValue* items) final {
        auto& decoder = *SpecsCache_.GetSpecs().Inputs[TableIndex_];
        for (ui32 i = 0; i < decoder.SkiffSize; ++i) {
            if (decoder.OthersStructIndex && i == *decoder.OthersStructIndex) {
                continue;
            }

            if (decoder.FieldsVec[i].Virtual) {
                items[i] = NUdf::TUnboxedValuePod::Zero();
                continue;
            }

            try {
                if (Y_UNLIKELY(decoder.FieldsVec[i].StructIndex == Max<ui32>())) {
                    SkipSkiffField(decoder.FieldsVec[i].Type, decoder.NativeYtTypeFlags);
                } else if (decoder.NativeYtTypeFlags && !decoder.FieldsVec[i].ExplicitYson) {
                    items[i] = ReadSkiffFieldNativeYt(decoder.FieldsVec[i].Type, decoder.NativeYtTypeFlags);
                } else if (decoder.DefaultValues[i]) {
                    auto val = ReadSkiffField(decoder.FieldsVec[i].Type, true);
                    items[i] = val ? NUdf::TUnboxedValue(val.Release().GetOptionalValue()) : decoder.DefaultValues[i];
                } else {
                    items[i] = ReadSkiffField(decoder.FieldsVec[i].Type, false);
                }
            } catch (const TYqlPanic& e) {
                ythrow TYqlPanic() << "Failed to read field: '" << decoder.FieldsVec[i].Name << "'\n" << e.what();
            } catch (...) {
                ythrow yexception() << "Failed to read field: '" << decoder.FieldsVec[i].Name << "'\n" << CurrentExceptionMessage();
            }
        }
    }

    NUdf::TUnboxedValue ReadSkiffField(TType* type, bool withDefVal) {
        const bool isOptional = withDefVal || type->IsOptional();
        TType* uwrappedType = type;
        if (type->IsOptional()) {
            uwrappedType = static_cast<TOptionalType*>(type)->GetItemType();
        }

        if (isOptional) {
            auto marker = Buf_.Read();
            if (!marker) {
                return NUdf::TUnboxedValue();
            }
        }

        if (uwrappedType->IsData()) {
            return NCommon::ReadSkiffData(uwrappedType, 0, Buf_);
        } else if (!isOptional && uwrappedType->IsPg()) {
            return NCommon::ReadSkiffPg(static_cast<TPgType*>(uwrappedType), Buf_);
        } else {
            // yson content
            ui32 size;
            Buf_.ReadMany((char*)&size, sizeof(size));
            CHECK_STRING_LENGTH_UNSIGNED(size);
            // parse binary yson...
            YQL_ENSURE(size > 0);
            char cmd = Buf_.Read();
            auto value = ReadYsonValue(uwrappedType, 0, SpecsCache_.GetHolderFactory(), cmd, Buf_, true);
            return isOptional ? value.Release().MakeOptional() : value;
        }
    }

    NUdf::TUnboxedValue ReadSkiffFieldNativeYt(TType* type, ui64 nativeYtTypeFlags) {
        return NCommon::ReadSkiffNativeYtValue(type, nativeYtTypeFlags, SpecsCache_.GetHolderFactory(), Buf_);
    }

    void SkipSkiffField(TType* type, ui64 nativeYtTypeFlags) {
        return NCommon::SkipSkiffField(type, nativeYtTypeFlags, Buf_);
    }
};

#ifndef MKQL_DISABLE_CODEGEN

class TSkiffLLVMDecoder: public TSkiffDecoderBase {
public:
    TSkiffLLVMDecoder(TInputBuf& buf, const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : TSkiffDecoderBase(buf, specs, holderFactory)
    {
        Codegen_ = NCodegen::ICodegen::Make(NCodegen::ETarget::Native);
        Codegen_->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TVector<llvm::Function*> funcs;
        THashMap<const TMkqlIOSpecs::TDecoderSpec*, llvm::Function*> processedDecoders;
        for (auto x : specs.Inputs) {
            llvm::Function*& func = processedDecoders[x];
            if (!func) {
                auto rowReaderBuilder = MakeYtCodecCgReader(Codegen_, holderFactory, x);
                for (ui32 i = 0; i < x->SkiffSize; ++i) {
                    if (x->OthersStructIndex && i == *x->OthersStructIndex) {
                        rowReaderBuilder->SkipOther();
                        continue;
                    }

                    if (x->FieldsVec[i].Virtual) {
                        rowReaderBuilder->SkipVirtual();
                        continue;
                    }

                    if (x->FieldsVec[i].StructIndex != Max<ui32>()) {
                        rowReaderBuilder->AddField(x->FieldsVec[i].Type, x->DefaultValues[i], x->FieldsVec[i].ExplicitYson ? 0 : x->NativeYtTypeFlags);
                    } else {
                        rowReaderBuilder->SkipField(x->FieldsVec[i].Type, x->NativeYtTypeFlags);
                    }
                }

                func = rowReaderBuilder->Build();
            }
            funcs.push_back(func);
        }

        Codegen_->Verify();
        YtCodecAddMappings(*Codegen_);
        Codegen_->Compile();
        for (const auto& func : funcs) {
            RowReaders_.push_back((TRowReader)Codegen_->GetPointerToFunction(func));
        }
    }

protected:
    void DecodeItems(NUdf::TUnboxedValue* items) final {
        RowReaders_.at(TableIndex_)(items, Buf_);
    }

    NCodegen::ICodegen::TPtr Codegen_;
    typedef void(*TRowReader)(NKikimr::NUdf::TUnboxedValue*, TInputBuf&);
    TVector<TRowReader> RowReaders_;
};

#endif

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TMkqlReaderImpl::TMkqlReaderImpl(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize, ui32 tableIndex, bool ignoreStreamTableIndex)
    : TMkqlReaderImpl()
{
    SetReader(source, blockCount, blockSize, tableIndex, ignoreStreamTableIndex);
}

TMkqlReaderImpl::TMkqlReaderImpl()
    : TimerDecode_(InputDecodeTime, 100)
    , TimerRead_(InputReadTime, 100)
    , Buf_(&TimerRead_)
{
}

TMkqlReaderImpl::~TMkqlReaderImpl() {
}

void TMkqlReaderImpl::SetReader(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize, ui32 tableIndex, bool ignoreStreamTableIndex, TMaybe<ui64> currentRow, TMaybe<ui32> currentRange) {
    Reader_ = MakeBlockReader(source, blockCount, blockSize);
    Buf_.SetSource(*Reader_);
    HasRangeIndices_ = source.HasRangeIndices();
    InitialTableIndex_ = tableIndex;
    IgnoreStreamTableIndex_ = ignoreStreamTableIndex;
    if (Decoder_) {
        Decoder_->Reset(HasRangeIndices_, InitialTableIndex_, IgnoreStreamTableIndex_);
        if (currentRow) {
            Decoder_->RowIndex_ = currentRow;
        }
        if (currentRange) {
            Decoder_->RangeIndex_ = currentRange;
        }
    }
}

void TMkqlReaderImpl::SetSpecs(const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory) {
    Specs_ = &specs;
    HolderFactoryPtr = &holderFactory;
    JobStats_ = specs.JobStats_;
    Buf_.SetStats(JobStats_);
    if (Specs_->UseSkiff_) {
#ifndef MKQL_DISABLE_CODEGEN
        if (Specs_->OptLLVM_ != "OFF" && NCodegen::ICodegen::IsCodegenAvailable()) {
            Decoder_.Reset(new TSkiffLLVMDecoder(Buf_, *Specs_, holderFactory));
        }
        else
#endif
        {
            Decoder_.Reset(new TSkiffDecoder(Buf_, *Specs_, holderFactory));
        }
    } else {
        Decoder_.Reset(new TYsonDecoder(Buf_, *Specs_, holderFactory));
    }
    Decoder_->TableIndex_ = InitialTableIndex_;
    Decoder_->HasRangeIndices_ = HasRangeIndices_;
    Decoder_->IgnoreStreamTableIndex = IgnoreStreamTableIndex_;
}

void TMkqlReaderImpl::Finish() {
    if (Decoder_) {
        Decoder_->Row_.Clear();
    }
    TimerDecode_.Report(JobStats_);
    TimerRead_.Report(JobStats_);
}

void TMkqlReaderImpl::OnError(const std::exception_ptr& error, TStringBuf msg) {
    YQL_LOG(ERROR) << "Reader error: " << msg;
    Buf_.Reset();
    if (!Reader_->Retry(Decoder_->RangeIndex_, Decoder_->RowIndex_, error)) {
        ythrow yexception() << "Failed to read row, table index: " << Decoder_->TableIndex_ << ", row index: " <<
            (Decoder_->RowIndex_.Defined() ? ToString(*Decoder_->RowIndex_) : "?") << "\n" << msg;
    }
}

NUdf::TUnboxedValue TMkqlReaderImpl::GetRow() const {
    CheckValidity();
    CheckReadRow();
    return Decoder_->Row_;
}

bool TMkqlReaderImpl::IsValid() const {
    return Reader_ && Decoder_ && Decoder_->Valid_;
}

TMaybe<ui64> TMkqlReaderImpl::GetRowIndexUnchecked() const {
    return Decoder_ ? Decoder_->RowIndex_ : TMaybe<ui64>();
}

TMaybe<ui32> TMkqlReaderImpl::GetRangeIndexUnchecked() const {
    return Decoder_ ? Decoder_->RangeIndex_ : TMaybe<ui32>();
}

ui32 TMkqlReaderImpl::GetTableIndex() const {
    CheckValidity();
    CheckReadRow();
    return Decoder_->TableIndex_;
}

ui32 TMkqlReaderImpl::GetRangeIndex() const {
    CheckValidity();
    CheckReadRow();
    return Decoder_->RangeIndex_.GetOrElse(0);
}

ui64 TMkqlReaderImpl::GetRowIndex() const {
    CheckValidity();
    CheckReadRow();
    return Decoder_->RowIndex_.GetOrElse(0UL);
}

void TMkqlReaderImpl::Next() {
    auto guard = Guard(TimerDecode_);
    CheckValidity();

    if (Decoder_->RowAlreadyRead_) {
        Decoder_->RowAlreadyRead_ = false;
        return;
    }

    if (Decoder_->RowIndex_) {
        ++*Decoder_->RowIndex_;
    }

    NUdf::TUnboxedValue* items = nullptr;
    TMaybe<TValuesDictHashMap> others;

    // Retrieable part
    while (true) {
        try {
            if (Decoder_->DecodeNext(items, others)) {
                break; // Retry loop
            } else {
                return;
            }
        } catch (const TYqlPanic& e) {
            ythrow TYqlPanic() << "Failed to read row, table index: " << Decoder_->TableIndex_ << ", row index: " <<
                (Decoder_->RowIndex_.Defined() ? ToString(*Decoder_->RowIndex_) : "?") << "\n" << e.what();
        } catch (const TTimeoutException&) {
            throw;
        } catch (const yexception& e) {
            OnError(std::make_exception_ptr(e), e.AsStrBuf());
        } catch (...) {
            OnError(std::current_exception(), CurrentExceptionMessage());
        }
    }

    // Unretrieable part
    auto& decoder = *Specs_->Inputs[Decoder_->TableIndex_];
    if (Specs_->UseSkiff_) {
        if (decoder.OthersStructIndex && *decoder.OthersStructIndex != Max<ui32>()) {
            items[*decoder.OthersStructIndex] = BuildOthers(decoder, *others);
        }
    } else {
        for (ui32 index = 0; index < decoder.StructSize; ++index) {
            if (items[index]) {
                continue;
            }

            if (decoder.OthersStructIndex && *decoder.OthersStructIndex == index) {
                items[index] = BuildOthers(decoder, *others);
                continue;
            }

            // missing value
            const auto& field = decoder.FieldsVec[index];
            if (field.Type->IsOptional() || field.Type->IsNull() || field.Type->IsPg()) {
                items[index] = NUdf::TUnboxedValuePod();
                continue;
            }

            if (field.Type->IsVoid()) {
                items[index] = NUdf::TUnboxedValue::Void();
                continue;
            }

            if (field.Virtual) {
                items[index] = NUdf::TUnboxedValue::Zero();
                continue;
            }

            const auto& defVal = decoder.DefaultValues[index];
            YQL_ENSURE(defVal, "Failed to read row, table index: " << Decoder_->TableIndex_ << ", row index: " <<
                (Decoder_->RowIndex_.Defined() ? ToString(*Decoder_->RowIndex_) : "?") << ": missing field data: " << field.Name);
            items[index] = defVal;
        }
    }
    if (decoder.FillSysColumnPath) {
        items[*decoder.FillSysColumnPath] = Specs_->TableNames.at(Decoder_->TableIndex_);
    }
    if (decoder.FillSysColumnIndex) {
        items[*decoder.FillSysColumnIndex] = NUdf::TUnboxedValuePod(Decoder_->TableIndex_);
    }
    if (decoder.FillSysColumnKeySwitch) {
        items[*decoder.FillSysColumnKeySwitch] = NUdf::TUnboxedValuePod(Decoder_->KeySwitch_);
    }
    if (auto row = Decoder_->RowIndex_) {
        if (decoder.FillSysColumnRecord) {
            items[*decoder.FillSysColumnRecord] = NUdf::TUnboxedValuePod(*row + 1);
        }
        if (decoder.FillSysColumnNum) {
            items[*decoder.FillSysColumnNum] = NUdf::TUnboxedValuePod(Specs_->TableOffsets.at(Decoder_->TableIndex_) + *row + 1);
        }
    }
    Decoder_->KeySwitch_ = false;
}

NUdf::TUnboxedValue TMkqlReaderImpl::BuildOthers(const TMkqlIOSpecs::TDecoderSpec& decoder, TValuesDictHashMap& others) {
    if (others.empty()) {
        return HolderFactoryPtr->GetEmptyContainerLazy();
    } else {
        auto filler = [&others](TValuesDictHashMap& map) {
            map.swap(others);
        };

        return HolderFactoryPtr->CreateDirectHashedDictHolder(filler, decoder.OthersKeyTypes, false, true, nullptr, nullptr, nullptr);
    }
}

void TMkqlReaderImpl::NextKey() {
    while (Decoder_->Valid_) {
        Next();
    }

    if (Decoder_->Finished_) {
        return;
    }

    Decoder_->Valid_ = true;
    if (Decoder_->RowIndex_ && !Decoder_->RowAlreadyRead_) {
        --*Decoder_->RowIndex_;
    }
}

void TMkqlReaderImpl::CheckValidity() const {
    YQL_ENSURE(Decoder_ && Decoder_->Valid_, "Iterator is not valid");
}

void TMkqlReaderImpl::CheckReadRow() const {
    YQL_ENSURE(Decoder_ && !Decoder_->AtStart_, "Next() must be called");
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TMkqlWriterImpl::TEncoder {
    TEncoder(TOutputBuf& buf, const TMkqlIOSpecs& specs)
        : Buf_(buf)
        , Specs_(specs)
    {
    }
    virtual ~TEncoder() = default;

    virtual void EncodeNext(const NUdf::TUnboxedValuePod row) = 0;
    virtual void EncodeNext(const NUdf::TUnboxedValuePod* row) = 0;

protected:
    struct TField {
        TStringBuf Name;
        TType* Type;
        bool Optional;
    };

    static TVector<TField> GetFields(TStructType* type, const TVector<TString>& columns = {}) {
        TVector<TField> res;

        THashMap<TString, ui32> columnsOrder;
        if (columns.size() > 0) {
            for (ui32 index = 0; index < columns.size(); index++) {
                columnsOrder[columns[index]] = index;
            }
            res.resize(columns.size());
        } else {
            res.reserve(type->GetMembersCount());
        }

        for (ui32 index = 0; index < type->GetMembersCount(); ++index) {
            auto name = type->GetMemberName(index);
            auto fieldType = type->GetMemberType(index);
            const bool isOptional = fieldType->IsOptional();
            if (isOptional) {
                fieldType = static_cast<TOptionalType*>(fieldType)->GetItemType();
            }

            if (!columnsOrder.empty() && columnsOrder.contains(name)) {
                res[columnsOrder[name]] = TField{name, fieldType, isOptional};
            } else {
                res.push_back(TField{name, fieldType, isOptional});
            }
        }
        return res;
    }

protected:
    TOutputBuf& Buf_;
    const TMkqlIOSpecs& Specs_;
};

class TYsonEncoder: public TMkqlWriterImpl::TEncoder {
public:
    TYsonEncoder(TOutputBuf& buf, const TMkqlIOSpecs& specs, size_t tableIndex)
        : TMkqlWriterImpl::TEncoder(buf, specs)
    {
        Fields_ = GetFields(Specs_.Outputs[tableIndex].RowType);
        NativeYtTypeFlags_ = Specs_.Outputs[tableIndex].NativeYtTypeFlags;
    }

    void EncodeNext(const NUdf::TUnboxedValuePod row) final {
        Buf_.Write(BeginMapSymbol);

        for (size_t index = 0; index < Fields_.size(); ++index) {
            const TField& field = Fields_[index];
            auto value = row.GetElement(index);
            if (field.Optional || field.Type->GetKind() == TTypeBase::EKind::Pg) {
                if (!value) {
                    continue;
                }
                value = value.Release().GetOptionalValue();
            }

            Buf_.Write(StringMarker);
            Buf_.WriteVarI32(field.Name.size());
            Buf_.WriteMany(field.Name.data(), field.Name.size());
            Buf_.Write(KeyValueSeparatorSymbol);
            
            bool isOptionalFieldTypeV3 = field.Optional && (NativeYtTypeFlags_ & ENativeTypeCompatFlags::NTCF_COMPLEX);
            bool wrapOptionalTypeV3 = isOptionalFieldTypeV3 &&
                (field.Type->GetKind() == TTypeBase::EKind::Optional || field.Type->GetKind() == TTypeBase::EKind::Pg);
            if (wrapOptionalTypeV3) {
                Buf_.Write(BeginListSymbol);
            }

            WriteYsonValueInTableFormat(Buf_, field.Type, NativeYtTypeFlags_, std::move(value), true);

            if (wrapOptionalTypeV3) {
                Buf_.Write(ListItemSeparatorSymbol);
                Buf_.Write(EndListSymbol);
            }

            Buf_.Write(KeyedItemSeparatorSymbol);
        }
        Buf_.Write(EndMapSymbol);
        Buf_.Write(ListItemSeparatorSymbol);
        Buf_.OnRecordBoundary();
    }

    void EncodeNext(const NUdf::TUnboxedValuePod* row) final {
        Buf_.Write(BeginMapSymbol);

        for (size_t index = 0; index < Fields_.size(); ++index) {
            const TField& field = Fields_[index];
            auto value = row[index];
            if (field.Optional) {
                if (!value) {
                    continue;
                }
                value = value.GetOptionalValue();
            }

            Buf_.Write(StringMarker);
            Buf_.WriteVarI32(field.Name.size());
            Buf_.WriteMany(field.Name.data(), field.Name.size());
            Buf_.Write(KeyValueSeparatorSymbol);

            WriteYsonValueInTableFormat(Buf_, field.Type, NativeYtTypeFlags_, std::move(value), true);

            Buf_.Write(KeyedItemSeparatorSymbol);
        }
        Buf_.Write(EndMapSymbol);
        Buf_.Write(ListItemSeparatorSymbol);
        Buf_.OnRecordBoundary();
    }
private:
    TVector<TField> Fields_;
    ui64 NativeYtTypeFlags_;
};

class TSkiffEncoderBase: public TMkqlWriterImpl::TEncoder {
public:
    TSkiffEncoderBase(TOutputBuf& buf, const TMkqlIOSpecs& specs)
        : TMkqlWriterImpl::TEncoder(buf, specs)
    {
    }

    void EncodeNext(const NUdf::TUnboxedValuePod row) final {
        const ui16 tableIndexVal = 0; // Always should be zero
        Buf_.WriteMany((const char*)&tableIndexVal, sizeof(tableIndexVal));
        EncodeData(row);
        Buf_.OnRecordBoundary();
    }

    void EncodeNext(const NUdf::TUnboxedValuePod* row) final {
        const ui16 tableIndexVal = 0; // Always should be zero
        Buf_.WriteMany((const char*)&tableIndexVal, sizeof(tableIndexVal));
        EncodeData(row);
        Buf_.OnRecordBoundary();
    }

protected:
    virtual void EncodeData(const NUdf::TUnboxedValuePod row) = 0;
    virtual void EncodeData(const NUdf::TUnboxedValuePod* row) = 0;
};

class TSkiffEncoder: public TSkiffEncoderBase {
public:
    TSkiffEncoder(TOutputBuf& buf, const TMkqlIOSpecs& specs, size_t tableIndex, const TVector<TString>& columns)
        : TSkiffEncoderBase(buf, specs)
    {
        Fields_ = GetFields(Specs_.Outputs[tableIndex].RowType, columns);
        NativeYtTypeFlags_ = Specs_.Outputs[tableIndex].NativeYtTypeFlags;
    }

protected:
    void EncodeData(const NUdf::TUnboxedValuePod row) final {
        for (size_t index = 0; index < Fields_.size(); ++index) {
            const TField& field = Fields_[index];
            auto value = row.GetElement(index);
            if (field.Optional) {
                if (!value) {
                    Buf_.Write('\0');
                    continue;
                }
                Buf_.Write('\1');
                value = value.Release().GetOptionalValue();
            }
            WriteSkiffValue(field.Type, value, field.Optional);
        }
    }

    void EncodeData(const NUdf::TUnboxedValuePod* row) final {
        for (size_t index = 0; index < Fields_.size(); ++index) {
            const TField& field = Fields_[index];
            auto value = row[index];
            if (field.Optional) {
                if (!value) {
                    Buf_.Write('\0');
                    continue;
                }
                Buf_.Write('\1');
                value = value.GetOptionalValue();
            }
            WriteSkiffValue(field.Type, value, field.Optional);
        }
    }

    void WriteSkiffValue(TType* type, const NUdf::TUnboxedValuePod& value, bool wasOptional) {
        if (NativeYtTypeFlags_) {
            NCommon::WriteSkiffNativeYtValue(type, NativeYtTypeFlags_, value, Buf_);
        } else if (type->IsData()) {
            NCommon::WriteSkiffData(type, 0, value, Buf_);
        } else if (!wasOptional && type->IsPg()) {
            NCommon::WriteSkiffPg(static_cast<TPgType*>(type), value, Buf_);
        } else {
            WriteYsonContainerValue(type, 0, value, Buf_);
        }
    }

protected:
    TVector<TField> Fields_;
    ui64 NativeYtTypeFlags_;
};

class TSkiffEmptySchemaEncoder: public TSkiffEncoderBase {
public:
    TSkiffEmptySchemaEncoder(TOutputBuf& buf, const TMkqlIOSpecs& specs)
        : TSkiffEncoderBase(buf, specs)
    {
    }

protected:
    void EncodeData(const NUdf::TUnboxedValuePod row) final {
        Y_UNUSED(row);
        Buf_.Write('\0'); // Empty optional "_yql_fake_column"
    }

    void EncodeData(const NUdf::TUnboxedValuePod* row) final {
        Y_UNUSED(row);
        Buf_.Write('\0'); // Empty optional "_yql_fake_column"
    }
};

class TSkiffLLVMEncoder: public TSkiffEncoderBase {
public:
    typedef void (*TRowWriter)(const NUdf::TUnboxedValuePod, TOutputBuf&);
    typedef void (*TRowFlatWriter)(const NUdf::TUnboxedValuePod*, TOutputBuf&);

    TSkiffLLVMEncoder(TOutputBuf& buf, const TMkqlIOSpecs& specs, TRowWriter rowWriter, TRowFlatWriter flatWriter)
        : TSkiffEncoderBase(buf, specs)
        , RowWriter_(rowWriter), RowFlatWriter_(flatWriter)
    {
    }

protected:
    void EncodeData(const NUdf::TUnboxedValuePod row) final {
        RowWriter_(row, Buf_);
    }

    void EncodeData(const NUdf::TUnboxedValuePod* row) final {
        RowFlatWriter_(row, Buf_);
    }
private:
    TRowWriter RowWriter_;
    TRowFlatWriter RowFlatWriter_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TMkqlWriterImpl::TOutput::TOutput(IOutputStream& stream, size_t blockCount, size_t blockSize, TStatTimer& timerWrite)
    : Writer_(MakeBlockWriter(stream, blockCount, blockSize))
    , Buf_(*Writer_, &timerWrite)
{
}

TMkqlWriterImpl::TOutput::TOutput(NYT::TRawTableWriterPtr stream, size_t blockSize, NKikimr::NMiniKQL::TStatTimer& timerWrite)
    : Writer_(MakeBlockWriter(*stream, 0, blockSize))
    , Buf_(*Writer_, &timerWrite)
{
    Writer_->SetRecordBoundaryCallback([stream](){ stream->NotifyRowEnd(); });
}

TMkqlWriterImpl::TMkqlWriterImpl(const TVector<IOutputStream*>& streams, size_t blockCount, size_t blockSize)
    : TimerEncode_(OutputEncodeTime)
    , TimerWrite_(OutputWriteTime)
{
    for (IOutputStream* stream: streams) {
        Outputs_.push_back(MakeHolder<TOutput>(*stream, blockCount, blockSize, TimerWrite_));
    }
}

TMkqlWriterImpl::TMkqlWriterImpl(IOutputStream& stream, size_t blockCount, size_t blockSize)
    : TimerEncode_(OutputEncodeTime)
    , TimerWrite_(OutputWriteTime)
{
    Outputs_.push_back(MakeHolder<TOutput>(stream, blockCount, blockSize, TimerWrite_));
}

TMkqlWriterImpl::TMkqlWriterImpl(const TVector<NYT::TRawTableWriterPtr>& streams, size_t blockSize)
    : TimerEncode_(OutputEncodeTime)
    , TimerWrite_(OutputWriteTime)
{
    for (auto& stream: streams) {
        Outputs_.push_back(MakeHolder<TOutput>(stream, blockSize, TimerWrite_));
    }
}

TMkqlWriterImpl::TMkqlWriterImpl(NYT::TRawTableWriterPtr stream, size_t blockSize)
    : TimerEncode_(OutputEncodeTime)
    , TimerWrite_(OutputWriteTime)
{
    Outputs_.push_back(MakeHolder<TOutput>(stream, blockSize, TimerWrite_));
}

TMkqlWriterImpl::~TMkqlWriterImpl() {
}

void TMkqlWriterImpl::SetSpecs(const TMkqlIOSpecs& specs, const TVector<TString>& columns) {
    // In the case of the "skiff" format, the ordering of columns in data and in spec are assumed to be identical during encoding
    // To provide this, a "columns" field is used to change the alphabetical order of columns in spec

    Specs_ = &specs;
    JobStats_ = specs.JobStats_;

#ifndef MKQL_DISABLE_CODEGEN
    THashMap<TStructType*, std::pair<llvm::Function*, llvm::Function*>> llvmFunctions;
    if (Specs_->UseSkiff_ && Specs_->OptLLVM_ != "OFF" && NCodegen::ICodegen::IsCodegenAvailable()) {
        for (size_t i: xrange(Specs_->Outputs.size())) {
            auto rowType = Specs_->Outputs[i].RowType;
            if (rowType->GetMembersCount() != 0 && !llvmFunctions.contains(rowType)) {
                if (!Codegen_) {
                    Codegen_ = NCodegen::ICodegen::Make(NCodegen::ETarget::Native);
                    Codegen_->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
                }

                const auto writer1 = MakeYtCodecCgWriter<false>(Codegen_, rowType);
                const auto writer2 = MakeYtCodecCgWriter<true>(Codegen_, rowType);

                for (ui32 index = 0; index < rowType->GetMembersCount(); ++index) {
                    ui32 column = index;
                    if (columns) {
                        column = rowType->GetMemberIndex(columns[index]);
                    }

                    auto fieldType = rowType->GetMemberType(column);
                    writer1->AddField(fieldType, Specs_->Outputs[i].NativeYtTypeFlags);
                    writer2->AddField(fieldType, Specs_->Outputs[i].NativeYtTypeFlags);
                }

                llvmFunctions.emplace(rowType, std::make_pair(writer1->Build(), writer2->Build()));
            }
        }
        if (!llvmFunctions.empty()) {
            Codegen_->Verify();
            YtCodecAddMappings(*Codegen_);
            Codegen_->Compile();
        }
        else {
            Codegen_.reset();
        }
    }
#endif

    for (size_t i: xrange(Outputs_.size())) {
        auto& out = Outputs_[i];
        out->Buf_.SetStats(JobStats_);
        if (Specs_->UseSkiff_) {
            if (Specs_->Outputs[i].RowType->GetMembersCount() == 0) {
                YQL_ENSURE(columns.empty());
                Encoders_.emplace_back(new TSkiffEmptySchemaEncoder(out->Buf_, *Specs_));
            }
#ifndef MKQL_DISABLE_CODEGEN
            else if (auto p = llvmFunctions.FindPtr(Specs_->Outputs[i].RowType)) {
                Encoders_.emplace_back(new TSkiffLLVMEncoder(out->Buf_, *Specs_,
                    (TSkiffLLVMEncoder::TRowWriter)Codegen_->GetPointerToFunction(p->first),
                    (TSkiffLLVMEncoder::TRowFlatWriter)Codegen_->GetPointerToFunction(p->second)
                ));
            }
#endif
            else {
                Encoders_.emplace_back(new TSkiffEncoder(out->Buf_, *Specs_, i, columns));
            }
        } else {
            YQL_ENSURE(columns.empty());
            Encoders_.emplace_back(new TYsonEncoder(out->Buf_, *Specs_, i));
        }
    }
}

void TMkqlWriterImpl::AddRow(const NUdf::TUnboxedValuePod row) {
    const auto guard = Guard<TStatTimer>(TimerEncode_);
    if (Encoders_.size() == 1U) {
        Encoders_.front()->EncodeNext(row);
    } else {
        const auto tableIndex = row.GetVariantIndex();
        YQL_ENSURE(tableIndex < Encoders_.size(), "Wrong table index: " << tableIndex
            << ", there are only " << Encoders_.size() << " outputs");
        const auto item = row.GetVariantItem().Release();
        Encoders_[tableIndex]->EncodeNext(item);
    }

    if (WriteLimit) {
        ui64 res = 0;
        for (auto& x : Outputs_) {
            res += x->Buf_.GetWrittenBytes();
        }
        if (res > *WriteLimit) {
            throw TMemoryLimitExceededException();
        }
    }
}

void TMkqlWriterImpl::AddFlatRow(const NUdf::TUnboxedValuePod* row) {
    YQL_ENSURE(Encoders_.size() == 1U, "Expected single table.");
    const auto guard = Guard<TStatTimer>(TimerEncode_);
    Encoders_.front()->EncodeNext(row);
    if (WriteLimit) {
        ui64 res = 0;
        for (auto& x : Outputs_) {
            res += x->Buf_.GetWrittenBytes();
        }
        if (res > *WriteLimit) {
            throw TMemoryLimitExceededException();
        }
    }
}

void TMkqlWriterImpl::Finish() {
    if (!IsFinished_) {
        IsFinished_ = true;
        DoFinish(false);
    }
}

void TMkqlWriterImpl::Abort() {
    if (!IsFinished_) {
        IsFinished_ = true;
        DoFinish(true);
    }
}

void TMkqlWriterImpl::DoFinish(bool abort) {
    auto guard = Guard<TStatTimer>(TimerEncode_);
    if (!abort) {
        for (auto& x : Outputs_) {
            x->Buf_.Finish();
        }
    }

    Report();
}

void TMkqlWriterImpl::Report() {
    TimerEncode_.Report(JobStats_);
    TimerWrite_.Report(JobStats_);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

THolder<IInputStream> MakeStringInput(const TString& str, bool decompress) {
    if (decompress) {
        return MakeHolder<THoldingStream<TBrotliDecompress, TStringInput>>(MakeHolder<TStringInput>(str));
    } else {
        return MakeHolder<TStringInput>(str);
    }
}

THolder<IInputStream> MakeFileInput(const TString& file, bool decompress) {
    if (decompress) {
        return MakeHolder<THoldingStream<TBrotliDecompress, TUnbufferedFileInput>>(MakeHolder<TUnbufferedFileInput>(file));
    } else {
        return MakeHolder<TUnbufferedFileInput>(file);
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

NUdf::TUnboxedValue DecodeYamr(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TYaMRRow& row) {
    NUdf::TUnboxedValue* items;
    auto ret = specsCache.NewRow(tableIndex, items);
    auto& decoder = *specsCache.GetSpecs().Inputs.at(tableIndex);
    auto& lastFields = specsCache.GetLastFields(tableIndex);

    TVector<TStringBuf> values{row.Key, row.SubKey, row.Value};
    for (size_t i = 0; i < YAMR_FIELDS.size(); ++i) {
        const TMkqlIOSpecs::TDecoderSpec::TDecodeField* field = nullptr;
        if (i < lastFields.size()) {
            auto lastField = lastFields[i];
            if (lastField->Name == YAMR_FIELDS[i]) {
                field = lastField;
            }
        }
        if (!field) {
            auto it = decoder.Fields.find(YAMR_FIELDS[i]);
            if (it != decoder.Fields.end()) {
                field = &it->second;
            }
        }
        if (field) {
            items[field->StructIndex] = MakeString(NUdf::TStringRef(values[i].data(), values[i].size()));
        }
    }

    return ret;
}

void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TNode& value, IOutputStream& ysonOut) {
    auto& decoder = *specsCache.GetSpecs().Inputs.at(tableIndex);

    TVector<TString> items(decoder.StructSize);
    TMaybe<THashMap<TString, TString>> others;
    if (decoder.OthersStructIndex) {
        others.ConstructInPlace();
        others->reserve(specsCache.GetMaxOthersFields(tableIndex));
    }

    for (auto& node: value.AsMap()) {
        auto& name = node.first;
        const TMkqlIOSpecs::TDecoderSpec::TDecodeField* field = nullptr;
        if (!decoder.OthersStructIndex || name != YqlOthersColumnName) {
            auto it = decoder.Fields.find(name);
            if (it != decoder.Fields.end()) {
                field = &it->second;
            }
        }

        if (!field && decoder.OthersStructIndex) {
            if (node.second.IsString()) {
                others->emplace(name, node.second.AsString());
            } else if (node.second.IsEntity() || node.second.IsBool() || node.second.IsInt64() || node.second.IsUint64() || node.second.IsDouble()) {
                others->emplace(name, NYT::NodeToYsonString(node.second, NYT::NYson::EYsonFormat::Text));
            } else {
                others->emplace(name, NYT::NodeToYsonString(node.second, NYT::NYson::EYsonFormat::Binary));
            }
            continue;
        }

        if (field->StructIndex != Max<ui32>()) {
            NYT::TNode res = node.second;
            auto dataType = field->Type;
            if (field->Type->IsOptional()) {
                dataType = static_cast<TOptionalType*>(field->Type)->GetItemType();
                if (res.IsEntity()) {
                    res = NYT::TNode::CreateList();
                } else {
                    res = NYT::TNode::CreateList().Add(node.second);
                }
            } else if (res.IsEntity()) {
                res = NYT::TNode();
            }
            if (res.GetType() != NYT::TNode::Undefined) {
                if (dataType->GetKind() == TType::EKind::Data && static_cast<TDataType*>(dataType)->GetSchemeType() == NUdf::TDataType<NUdf::TYson>::Id) {
                    items[field->StructIndex] = NCommon::EncodeRestrictedYson(res, NYT::NYson::EYsonFormat::Binary);
                } else {
                    items[field->StructIndex] = NYT::NodeToYsonString(res, NYT::NYson::EYsonFormat::Binary);
                }
            }
        }
    }

    WriteRowItems(specsCache, tableIndex, items, others, ysonOut);
}

void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TYaMRRow& value, IOutputStream& ysonOut) {
    auto& decoder = *specsCache.GetSpecs().Inputs.at(tableIndex);
    auto& lastFields = specsCache.GetLastFields(tableIndex);

    TVector<TString> items(decoder.StructSize);
    TVector<TStringBuf> values{value.Key, value.SubKey, value.Value};
    for (size_t i = 0; i < YAMR_FIELDS.size(); ++i) {
        const TMkqlIOSpecs::TDecoderSpec::TDecodeField* field = nullptr;
        if (i < lastFields.size()) {
            auto lastField = lastFields[i];
            if (lastField->Name == YAMR_FIELDS[i]) {
                field = lastField;
            }
        }
        if (!field) {
            auto it = decoder.Fields.find(YAMR_FIELDS[i]);
            if (it != decoder.Fields.end()) {
                field = &it->second;
            }
        }
        if (field) {
            items[field->StructIndex] = NYT::NodeToYsonString(NYT::TNode(values[i]), NYT::NYson::EYsonFormat::Binary);
        }
    }

    WriteRowItems(specsCache, tableIndex, items, {}, ysonOut);
}

void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NUdf::TUnboxedValuePod& value, IOutputStream& ysonOut) {
    auto& decoder = *specsCache.GetSpecs().Inputs.at(tableIndex);
    TVector<TString> items(decoder.StructSize);
    for (ui32 i = 0; i < decoder.StructSize; ++i) {
        items[i] = NCommon::WriteYsonValue(value.GetElement(decoder.FieldsVec[i].StructIndex), decoder.FieldsVec[i].Type, nullptr, NYT::NYson::EYsonFormat::Binary);
    }
    WriteRowItems(specsCache, tableIndex, items, {}, ysonOut);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

} // NYql
