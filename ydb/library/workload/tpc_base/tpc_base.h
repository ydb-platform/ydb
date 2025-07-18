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
    YDB_READONLY(double, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
};

class TTpcBaseWorkloadGenerator: public TWorkloadGeneratorBase {
public:
    explicit TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params);
    TQueryInfoList GetWorkload(int type) override final;
    TQueryInfoList GetInitialData() override final;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override final;

private:
    const TTpcBaseWorkloadParams& Params;
    void PatchQuery(TString& query) const;
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

    virtual ~TCsvItemWriter() = default;

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

    void Write(const TItem& item, size_t itemIndex) {
        WriteHeader();
        for(size_t field = 0; field < Fields.size(); ++field) {
            WriteImpl(item, itemIndex, field);
            if (field + 1 < Fields.size()) {
                Out << TWorkloadGeneratorBase::PsvDelimiter;
            }
        }
        Out << Endl;
    }

    template<class TContainer>
    void Write(const TContainer& items) {
        size_t i = 0;
        for(const auto& item: items) {
            Write(item, i++);
        }
    }

    void Write(const TItem* items, size_t count) {
        for(size_t i = 0; i < count; ++i) {
            Write(items[i], i);
        }
    }

protected:
    virtual void WriteImpl(const TItem& item, size_t itemIndex, const size_t fieldIndex) {
        Y_UNUSED(itemIndex);
        Fields[fieldIndex].WriteFunction(item, Out);
    }

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

} // namespace NYdbWorkload
