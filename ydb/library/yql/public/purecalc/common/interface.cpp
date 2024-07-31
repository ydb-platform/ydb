#include "interface.h"

#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/public/purecalc/common/logger_init.h>
#include <ydb/library/yql/public/purecalc/common/program_factory.h>

using namespace NYql;
using namespace NYql::NPureCalc;

TLoggingOptions::TLoggingOptions()
    : LogLevel_(ELogPriority::TLOG_ERR)
    , LogDestination(&Clog)
{
}

TLoggingOptions& TLoggingOptions::SetLogLevel(ELogPriority logLevel) {
    LogLevel_ = logLevel;
    return *this;
}

TLoggingOptions& TLoggingOptions::SetLogDestination(IOutputStream* logDestination) {
    LogDestination = logDestination;
    return *this;
}

TProgramFactoryOptions::TProgramFactoryOptions()
    : UdfsDir_("")
    , UserData_()
    , LLVMSettings("OFF")
    , BlockEngineSettings("disable")
    , ExprOutputStream(nullptr)
    , CountersProvider(nullptr)
    , NativeYtTypeFlags(0)
    , UseSystemColumns(false)
    , UseWorkerPool(true)
{
}

TProgramFactoryOptions& TProgramFactoryOptions::SetUDFsDir(TStringBuf dir) {
    UdfsDir_ = dir;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddLibrary(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData_.emplace_back();

    ref.Type_ = NUserData::EType::LIBRARY;
    ref.Disposition_ = disposition;
    ref.Name_ = name;
    ref.Content_ = content;

    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddFile(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData_.emplace_back();

    ref.Type_ = NUserData::EType::FILE;
    ref.Disposition_ = disposition;
    ref.Name_ = name;
    ref.Content_ = content;

    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddUDF(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData_.emplace_back();

    ref.Type_ = NUserData::EType::UDF;
    ref.Disposition_ = disposition;
    ref.Name_ = name;
    ref.Content_ = content;

    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetLLVMSettings(TStringBuf llvm_settings) {
    LLVMSettings = llvm_settings;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetBlockEngineSettings(TStringBuf blockEngineSettings) {
    BlockEngineSettings = blockEngineSettings;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetExprOutputStream(IOutputStream* exprOutputStream) {
    ExprOutputStream = exprOutputStream;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetCountersProvider(NKikimr::NUdf::ICountersProvider* countersProvider) {
    CountersProvider = countersProvider;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetUseNativeYtTypes(bool useNativeTypes) {
    NativeYtTypeFlags = useNativeTypes ? NTCF_PRODUCTION : NTCF_NONE;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetNativeYtTypeFlags(ui64 nativeTypeFlags) {
    NativeYtTypeFlags = nativeTypeFlags;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetDeterministicTimeProviderSeed(TMaybe<ui64> seed) {
    DeterministicTimeProviderSeed = seed;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetUseSystemColumns(bool useSystemColumns) {
    UseSystemColumns = useSystemColumns;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetUseWorkerPool(bool useWorkerPool) {
    UseWorkerPool = useWorkerPool;
    return *this;
}

void NYql::NPureCalc::ConfigureLogging(const TLoggingOptions& options) {
    InitLogging(options);
}

IProgramFactoryPtr NYql::NPureCalc::MakeProgramFactory(const TProgramFactoryOptions& options) {
    return new TProgramFactory(options);
}
