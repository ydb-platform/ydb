#pragma once

#include <library/cpp/string_utils/csv/csv.h>
#include <util/stream/length.h>

namespace NYdb {
namespace NConsoleClient {

class IParamStream {
public:
    virtual TString ReadAll() = 0;
    virtual size_t ReadLine(TString& res) = 0;
    virtual ~IParamStream() = default;
};

class TSimpleParamStream : public IParamStream {
public:
    virtual TString ReadAll() override;
    virtual size_t ReadLine(TString& res) override;
};

class TCsvParamStream : public IParamStream {
public:
    TCsvParamStream();
    virtual TString ReadAll() override;
    virtual size_t ReadLine(TString& res) override;

private:
    TCountingInput Input;
    NCsvFormat::TLinesSplitter Splitter;
    ui64 CurrentCount = 0;
};

}
}