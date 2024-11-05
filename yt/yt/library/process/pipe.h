#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TNamedPipe
    : public TRefCounted
{
public:
    ~TNamedPipe();
    static TNamedPipePtr Create(const TString& path, int permissions = 0660, std::optional<int> capacity = {});
    static TNamedPipePtr FromPath(const TString& path);

    NNet::IConnectionReaderPtr CreateAsyncReader();
    NNet::IConnectionWriterPtr CreateAsyncWriter(bool useDeliveryFence = false);

    TString GetPath() const;

private:
    const TString Path_;
    const std::optional<int> Capacity_;

    //! Whether pipe was created by this class
    //! and should be removed in destructor.
    const bool Owning_;

    explicit TNamedPipe(const TString& path, std::optional<int> capacity, bool owning);
    void Open(int permissions);
    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TNamedPipe)

////////////////////////////////////////////////////////////////////////////////

class TNamedPipeConfig
    : public NYTree::TYsonStruct
{
public:
    TString Path;
    int FD = 0;
    bool Write = false;

    static TNamedPipeConfigPtr Create(TString path, int fd, bool write);

    REGISTER_YSON_STRUCT(TNamedPipeConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TPipe
    : public TNonCopyable
{
public:
    static const int InvalidFD = -1;

    TPipe();
    TPipe(TPipe&& pipe);
    ~TPipe();

    void operator=(TPipe&& other);

    void CloseReadFD();
    void CloseWriteFD();

    NNet::IConnectionReaderPtr CreateAsyncReader();
    NNet::IConnectionWriterPtr CreateAsyncWriter();

    int ReleaseReadFD();
    int ReleaseWriteFD();

    int GetReadFD() const;
    int GetWriteFD() const;

private:
    int ReadFD_ = InvalidFD;
    int WriteFD_ = InvalidFD;

    TPipe(int fd[2]);
    void Init(TPipe&& other);

    friend class TPipeFactory;
};

void FormatValue(TStringBuilderBase* builder, const TPipe& pipe, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

class TPipeFactory
{
public:
    explicit TPipeFactory(int minFD = 0);
    ~TPipeFactory();

    TPipe Create();

    void Clear();

private:
    const int MinFD_;
    std::vector<int> ReservedFDs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
