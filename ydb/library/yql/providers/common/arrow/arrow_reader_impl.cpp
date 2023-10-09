#include "interface/arrow_reader.h"
#include <util/thread/pool.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <parquet/arrow/reader.h>
#include <arrow/io/api.h>
#include <arrow/util/future.h>
#include <parquet/file_reader.h>

#include <iostream>
#include <sstream>

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

namespace NYql {

class TArrowReader : public IArrowReader {
public:
    TArrowReader(const TArrowReaderSettings& settings) {
        ThreadPool = CreateThreadPool(settings.PoolSize);    
    }

    NThreading::TFuture<TSchemaResponse> GetSchema(const TArrowFileDesc& desc) const override final;
    NThreading::TFuture<std::shared_ptr<arrow::Table>> ReadRowGroup(const TArrowFileDesc& desc, int rowGroupIndex, const std::vector<int>& columnIndices) const override final;
    virtual ~TArrowReader() {

    }
private:
    THolder<IThreadPool> ThreadPool;                    
};

IArrowReader::TPtr MakeArrowReader(const TArrowReaderSettings& settings) {
    return std::make_shared<TArrowReader>(settings);
}


using ArrowFileReaderPtr = std::unique_ptr<parquet::arrow::FileReader>;

class TS3RandomAccessFile : public arrow::io::RandomAccessFile {
public:
    explicit TS3RandomAccessFile(const TArrowFileDesc& desc) : Gateway(desc.Gateway), Headers(desc.Headers), RetryPolicy(desc.RetryPolicy), Url(desc.Url), FileSize(desc.Size) {

    }

    virtual arrow::Result<int64_t> GetSize() override {
        return FileSize;
    }

    virtual arrow::Result<int64_t> Tell() const override {
        return InnerPos;
    }
    
    virtual arrow::Status Seek(int64_t position) override {
        InnerPos = position;
        return {};
    }

    virtual arrow::Status Close() override {
        return {};
    }

    virtual bool closed() const override {
        return false;
    }

    virtual arrow::Result<int64_t> Read(int64_t, void* ) override {
        Y_ABORT_UNLESS(0);
        return arrow::Result<int64_t>();
    }

    virtual arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override {
        Y_ABORT_UNLESS(0);
        return arrow::Result<std::shared_ptr<arrow::Buffer>>();
    }

    virtual arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        try {
            auto promise = NThreading::NewPromise<TString>();
            Gateway->Download(Url, Headers,
                                position,
                                nbytes,
                                std::bind(&OnResult, promise, std::placeholders::_1),
                                {},
                                RetryPolicy);
            return arrow::Buffer::FromString(promise.GetFuture().ExtractValueSync());
        } catch (const std::exception& e) {
            return arrow::Status::UnknownError(e.what());
        }
    }

    virtual arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext&, int64_t,
                                                    int64_t) override
    {
        Y_ABORT_UNLESS(0);
        return arrow::Future<std::shared_ptr<arrow::Buffer>>();
    }


private:
    static void OnResult(NThreading::TPromise<TString> promise, IHTTPGateway::TResult&& result) {
        try {
            promise.SetValue(result.Content.Extract());
        } catch (const std::exception& e) {
            promise.SetException(std::current_exception());
        }
    }

    IHTTPGateway::TPtr Gateway;
    IHTTPGateway::THeaders Headers;
    IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    const TString Url;
    const size_t FileSize;
    int64_t InnerPos = 0;
};


struct TFileReaderWrapper {
public:
    TFileReaderWrapper(const TArrowFileDesc& desc) {
        if (desc.Contents) {
            ArrowFile = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(*desc.Contents));
        } else {
            if (desc.IsLocal) {
                ArrowFile = arrow::io::ReadableFile::Open(desc.Url.substr(7), arrow::default_memory_pool()).ValueOrDie();
            } else {
                ArrowFile = std::make_shared<TS3RandomAccessFile>(desc);
            }
        }

        THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(ArrowFile, arrow::default_memory_pool(), &FileReader));
    }

    ArrowFileReaderPtr FileReader;
    std::shared_ptr<arrow::io::RandomAccessFile> ArrowFile;
};

struct TArrowFileCookie {
    TFileReaderWrapper Wrapper;

    TArrowFileCookie(const TArrowFileDesc& desc)
        : Wrapper(desc)
    {}
};

NThreading::TFuture<IArrowReader::TSchemaResponse> TArrowReader::GetSchema(const TArrowFileDesc& desc) const
{
    auto promise = NThreading::NewPromise<TSchemaResponse>();
    auto future = promise.GetFuture();
    
    YQL_ENSURE(desc.Format == "parquet");

    if (!ThreadPool->AddFunc([desc, promise] () mutable {
        YQL_ENSURE(desc.Format == "parquet");    
        try {
            auto cookie = desc.Cookie;
            if (!cookie) {
                cookie = std::make_shared<TArrowFileCookie>(desc);
            }
            
            std::shared_ptr<arrow::Schema> schema;
            
            THROW_ARROW_NOT_OK(cookie->Wrapper.FileReader->GetSchema(&schema));
            
            promise.SetValue(TSchemaResponse(schema, cookie->Wrapper.FileReader->num_row_groups(), cookie));
        } catch (const std::exception&) {
            promise.SetException(std::current_exception());
        }
    }))
    {
        promise.SetException("AddFunc to ThreadPool failed");
    }

    return future;
}

NThreading::TFuture<std::shared_ptr<arrow::Table>> TArrowReader::ReadRowGroup(const TArrowFileDesc& desc, int rowGroupIndex, 
                                                    const std::vector<int>& columnIndices) const
{
    auto promise = NThreading::NewPromise<std::shared_ptr<arrow::Table>>();
    auto future = promise.GetFuture();
    
    if (!ThreadPool->AddFunc([desc, promise, rowGroupIndex, columnIndices] () mutable {
        YQL_ENSURE(desc.Format == "parquet");
        try {
            auto cookie = desc.Cookie;
            if (!cookie) {
                cookie = std::make_shared<TArrowFileCookie>(desc);
            }

            std::shared_ptr<arrow::Table> currentTable;

            THROW_ARROW_NOT_OK(cookie->Wrapper.FileReader->ReadRowGroup(rowGroupIndex, columnIndices, &currentTable));

            promise.SetValue(currentTable);
        } catch (const std::exception&) {
            promise.SetException(std::current_exception());
        }
    }))
    {
        promise.SetException("AddFunc to ThreadPool failed");
    }

    return future;
}
}
