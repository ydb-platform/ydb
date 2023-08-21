#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadFileCommand
    : public TTypedCommand<NApi::TFileReaderOptions>
{
public:
    TReadFileCommand();

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
public:
    TWriteFileCommand();

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
public:
    TGetFileFromCacheCommand();

private:
    TString MD5;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPutFileToCacheCommand
    : public TTypedCommand<NApi::TPutFileToCacheOptions>
{
public:
    TPutFileToCacheCommand();

private:
    NYPath::TYPath Path;
    TString MD5;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

