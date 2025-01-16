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
    TSimpleParamStream(IInputStream* input);
    virtual TString ReadAll() override;
    virtual size_t ReadLine(TString& res) override;
private:
    IInputStream* Input;
};

class TCsvParamStream : public IParamStream {
public:
    TCsvParamStream(IInputStream* input);
    virtual TString ReadAll() override;
    virtual size_t ReadLine(TString& res) override;

private:
    IInputStream* Input;
    TCountingInput CountingInput;
    NCsvFormat::TLinesSplitter Splitter;
    ui64 CurrentCount = 0;
};

}
}