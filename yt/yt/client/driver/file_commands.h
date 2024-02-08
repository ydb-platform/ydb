#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadFileCommand
    : public TTypedCommand<NApi::TFileReaderOptions>
{
    REGISTER_YSON_STRUCT_LITE(TReadFileCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileReader;
    TString Etag;

    void DoExecute(ICommandContextPtr context) override;
    bool HasResponseParameters() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteFileCommand
    : public TTypedCommand<NApi::TFileWriterOptions>
{
    REGISTER_YSON_STRUCT_LITE(TWriteFileCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileWriter;
    bool ComputeMD5;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetFileFromCacheCommand
    : public TTypedCommand<NApi::TGetFileFromCacheOptions>
{
    REGISTER_YSON_STRUCT_LITE(TGetFileFromCacheCommand);

    static void Register(TRegistrar registrar);

private:
    TString MD5;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPutFileToCacheCommand
    : public TTypedCommand<NApi::TPutFileToCacheOptions>
{
    REGISTER_YSON_STRUCT_LITE(TPutFileToCacheCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;
    TString MD5;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
