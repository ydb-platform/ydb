#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpcBaseWorkloadParams: public TWorkloadBaseParams {
public:
    enum class EFloatMode {
        FLOAT /* "float" */,
        DECIMAL /* "decimal" */,
        DECIMAL_YDB /* "decimal_ydb" */
    };
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    YDB_READONLY(EFloatMode, FloatMode, EFloatMode::FLOAT);
    YDB_READONLY(EQuerySyntax, Syntax, EQuerySyntax::YQL);
    YDB_READONLY(ui64, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
    YDB_READONLY_DEF(TFsPath, ExternalQueriesDir);
    YDB_READONLY_PROTECT(bool, CheckCanonical, false);
};

class TTpcBaseWorkloadGenerator: public TWorkloadGeneratorBase {
public:
    explicit TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override final;
    TQueryInfoList GetInitialData() override final;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override final;

protected:
    virtual void PatchQuery(TString& query) const;
    virtual TVector<TString> GetTablesList() const = 0;

private:
    const TTpcBaseWorkloadParams& Params;
    void FilterHeader(IOutputStream& result, TStringBuf header, const TString& query) const;
    TString GetHeader(const TString& query) const;
};

template<class T>
class TCsvItemWriter {
public:
    using TItem = T;
    using TWriteFunction = std::function<void(const TItem&, IOutputStream&)>;
    explicit TCsvItemWriter(IOutputStream& out)
        : Out(out)
    {}

    void RegisterField(TStringBuf name, TWriteFunction writeFunc) {
        Fields.emplace_back(name, writeFunc);
    }

    void WriteHeader() {
        if (HeaderWritten) {
            return;
        }
        for(const auto& field: Fields) {
            Out << field.Name;
            if (&field + 1 != Fields.end()) {
                Out << TWorkloadGeneratorBase::PsvDelimiter;
            }
        }
        Out << Endl;
        HeaderWritten = true;
    }

    void Write(const TItem& item) {
        WriteHeader();
        for(const auto& field: Fields) {
            field.WriteFunction(item, Out);
            if (&field + 1 != Fields.end()) {
                Out << TWorkloadGeneratorBase::PsvDelimiter;
            }
        }
        Out << Endl;
    }

    template<class TContainer>
    void Write(const TContainer& items) {
        for(const auto& item: items) {
            Write(item);
        }
    }

    void Write(const TItem* items, size_t count) {
        for(size_t i = 0; i < count; ++i) {
            Write(items[i]);
        }
    }

private:
    struct TField {
        TField(TStringBuf name, TWriteFunction func)
            : Name(name)
            , WriteFunction(func)
        {}
        TStringBuf Name;
        TWriteFunction WriteFunction;
    };
    TVector<TField> Fields;
    IOutputStream& Out;
    bool HeaderWritten = false;
};

#define CSV_WRITER_REGISTER_FIELD(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << item.record_field; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name, column_name);

} // namespace NYdbWorkload
