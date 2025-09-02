#include "interface.h"

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/public/purecalc/common/logger_init.h>
#include <yql/essentials/public/purecalc/common/program_factory.h>

using namespace NYql;
using namespace NYql::NPureCalc;

TLoggingOptions::TLoggingOptions()
    : LogLevel(ELogPriority::TLOG_ERR)
    , LogDestination(&Clog)
{
}

TLoggingOptions& TLoggingOptions::SetLogLevel(ELogPriority logLevel) {
    LogLevel = logLevel;
    return *this;
}

TLoggingOptions& TLoggingOptions::SetLogDestination(IOutputStream* logDestination) {
    LogDestination = logDestination;
    return *this;
}

TProgramFactoryOptions::TProgramFactoryOptions()
    : UdfsDir("")
    , UserData()
    , LLVMSettings("OFF")
    , BlockEngineSettings("disable")
    , ExprOutputStream(nullptr)
    , CountersProvider(nullptr)
    , NativeYtTypeFlags(0)
    , UseSystemColumns(false)
    , UseWorkerPool(true)
    , UseAntlr4(true)
    , LangVer(MinLangVersion)
{
}

TProgramFactoryOptions& TProgramFactoryOptions::SetLanguageVersion(TLangVersion langver) {
    LangVer = langver;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::SetUDFsDir(TStringBuf dir) {
    UdfsDir = dir;
    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddLibrary(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData.emplace_back();

    ref.Type = NUserData::EType::LIBRARY;
    ref.Disposition = disposition;
    ref.Name = name;
    ref.Content = content;

    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddFile(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData.emplace_back();

    ref.Type = NUserData::EType::FILE;
    ref.Disposition = disposition;
    ref.Name = name;
    ref.Content = content;

    return *this;
}

TProgramFactoryOptions& TProgramFactoryOptions::AddUDF(NUserData::EDisposition disposition, TStringBuf name, TStringBuf content) {
    auto& ref = UserData.emplace_back();

    ref.Type = NUserData::EType::UDF;
    ref.Disposition = disposition;
    ref.Name = name;
    ref.Content = content;

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
