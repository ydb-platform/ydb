#pragma once

///
/// @file yt/cpp/mapreduce/interface/fwd.h
///
/// Header containing mostly forward declarations of types.


#include <util/generic/fwd.h>
#include <util/system/types.h>

#include <variant>

/// @cond Doxygen_Suppress
namespace google::protobuf {
    class Message;
}

namespace NYT {

    ////////////////////////////////////////////////////////////////////////////////
    // batch_request.h
    ////////////////////////////////////////////////////////////////////////////////

    class IBatchRequest;
    using TBatchRequestPtr = ::TIntrusivePtr<IBatchRequest>;

    ////////////////////////////////////////////////////////////////////////////////
    // client.h
    ////////////////////////////////////////////////////////////////////////////////

    enum ELockMode : int;

    struct TStartTransactionOptions;

    struct TLockOptions;

    template <class TDerived>
    struct TTabletOptions;

    struct TMountTableOptions;

    struct TUnmountTableOptions;

    struct TRemountTableOptions;

    struct TReshardTableOptions;

    struct TAlterTableOptions;

    struct TLookupRowsOptions;

    struct TSelectRowsOptions;

    struct TCreateClientOptions;

    struct TAlterTableReplicaOptions;

    struct TGetFileFromCacheOptions;

    struct TPutFileToCacheOptions;

    struct TCheckPermissionResult;
    struct TCheckPermissionResponse;
    struct TCheckPermissionOptions;

    struct TTabletInfo;

    class ILock;
    using ILockPtr = ::TIntrusivePtr<ILock>;

    class ITransaction;
    using ITransactionPtr = ::TIntrusivePtr<ITransaction>;

    class ITransactionPinger;
    using ITransactionPingerPtr = ::TIntrusivePtr<ITransactionPinger>;

    struct IOperation;
    using IOperationPtr = ::TIntrusivePtr<IOperation>;

    class IClientBase;

    class IClient;

    using IClientPtr = ::TIntrusivePtr<IClient>;
    using IClientBasePtr = ::TIntrusivePtr<IClientBase>;

    ////////////////////////////////////////////////////////////////////////////////
    // config.h
    ////////////////////////////////////////////////////////////////////////////////

    struct TConfig;
    using TConfigPtr = ::TIntrusivePtr<TConfig>;

    ////////////////////////////////////////////////////////////////////////////////
    // cypress.h
    ////////////////////////////////////////////////////////////////////////////////

    enum ENodeType : int;

    struct TCreateOptions;

    struct TRemoveOptions;

    struct TGetOptions;

    struct TSetOptions;

    struct TMultisetAttributesOptions;

    struct TListOptions;

    struct TCopyOptions;

    struct TMoveOptions;

    struct TLinkOptions;

    struct TConcatenateOptions;

    struct TInsertRowsOptions;

    struct TDeleteRowsOptions;

    struct TTrimRowsOptions;

    class ICypressClient;

    ////////////////////////////////////////////////////////////////////////////////
    // errors.h
    ////////////////////////////////////////////////////////////////////////////////

    class TApiUsageError;

    class TYtError;

    class TErrorResponse;

    struct TFailedJobInfo;

    class TOperationFailedError;

    ////////////////////////////////////////////////////////////////////////////////
    // node.h
    ////////////////////////////////////////////////////////////////////////////////

    class TNode;

    ////////////////////////////////////////////////////////////////////////////////
    // common.h
    ////////////////////////////////////////////////////////////////////////////////

    using TTransactionId = TGUID;
    using TNodeId = TGUID;
    using TLockId = TGUID;
    using TOperationId = TGUID;
    using TTabletCellId = TGUID;
    using TReplicaId = TGUID;
    using TJobId = TGUID;

    using TYPath = TString;
    using TLocalFilePath = TString;

    template <class T, class TDerived = void>
    struct TOneOrMany;

    // key column values
    using TKey = TOneOrMany<TNode>;

    class TSortColumn;

    // column names
    using TColumnNames = TOneOrMany<TString>;

    // key column descriptors.
    class TSortColumns;

    enum EValueType : int;

    enum ESortOrder : int;

    enum EOptimizeForAttr : i8;

    enum EErasureCodecAttr : i8;

    enum ESchemaModificationAttr : i8;

    enum class EMasterReadKind : int;

    class TColumnSchema;

    class TTableSchema;

    enum class ERelation;

    struct TKeyBound;

    struct TReadLimit;

    struct TReadRange;

    struct TRichYPath;

    struct TAttributeFilter;

    ////////////////////////////////////////////////////////////////////////////////
    // io.h
    ////////////////////////////////////////////////////////////////////////////////

    enum class EFormatType : int;

    struct TFormat;

    class IFileReader;

    using IFileReaderPtr = ::TIntrusivePtr<IFileReader>;

    class IFileWriter;

    using IFileWriterPtr = ::TIntrusivePtr<IFileWriter>;

    class IBlobTableReader;
    using IBlobTableReaderPtr = ::TIntrusivePtr<IBlobTableReader>;

    class TRawTableReader;

    using TRawTableReaderPtr = ::TIntrusivePtr<TRawTableReader>;

    class TRawTableWriter;

    using TRawTableWriterPtr = ::TIntrusivePtr<TRawTableWriter>;

    template <class T, class = void>
    class TTableReader;

    template <class T, class = void>
    class TTableRangesReader;

    template <typename T>
    using TTableRangesReaderPtr = ::TIntrusivePtr<TTableRangesReader<T>>;

    template <class T>
    using TTableReaderPtr = ::TIntrusivePtr<TTableReader<T>>;

    template <class T, class = void>
    class TTableWriter;

    template <class T>
    using TTableWriterPtr = ::TIntrusivePtr<TTableWriter<T>>;

    struct TYaMRRow;

    using ::google::protobuf::Message;

    class ISkiffRowParser;

    using ISkiffRowParserPtr = ::TIntrusivePtr<ISkiffRowParser>;

    class ISkiffRowSkipper;

    using ISkiffRowSkipperPtr = ::TIntrusivePtr<ISkiffRowSkipper>;

    namespace NDetail {

    class TYdlGenericRowType;

    } // namespace NDetail

    template<class... TYdlRowTypes>
    class TYdlOneOf;

    template<class... TProtoRowTypes>
    class TProtoOneOf;

    template<class... TSkiffRowTypes>
    class TSkiffRowOneOf;

    using TYaMRReader = TTableReader<TYaMRRow>;
    using TYaMRWriter = TTableWriter<TYaMRRow>;
    using TNodeReader = TTableReader<TNode>;
    using TNodeWriter = TTableWriter<TNode>;
    using TMessageReader = TTableReader<Message>;
    using TMessageWriter = TTableWriter<Message>;
    using TYdlTableWriter = TTableWriter<NDetail::TYdlGenericRowType>;

    template <class TDerived>
    struct TIOOptions;

    struct TFileReaderOptions;

    struct TFileWriterOptions;

    struct TTableReaderOptions;

    class TSkiffRowHints;

    struct TTableWriterOptions;

    ////////////////////////////////////////////////////////////////////////////////
    // job_statistics.h
    ////////////////////////////////////////////////////////////////////////////////

    class TJobStatistics;

    template <typename T>
    class TJobStatisticsEntry;

    ////////////////////////////////////////////////////////////////////////////////
    // operation.h
    ////////////////////////////////////////////////////////////////////////////////

    class TFormatHints;

    struct TUserJobSpec;

    struct TMapOperationSpec;

    struct TRawMapOperationSpec;

    struct TReduceOperationSpec;

    struct TMapReduceOperationSpec;

    struct TJoinReduceOperationSpec;

    struct TSortOperationSpec;

    class IIOperationPreparationContext;

    class IJob;
    using IJobPtr = ::TIntrusivePtr<IJob>;

    class IRawJob;
    using IRawJobPtr = ::TIntrusivePtr<IRawJob>;

    enum EMergeMode : int;

    struct TMergeOperationSpec;

    struct TEraseOperationSpec;

    template <class TR, class TW>
    class IMapper;

    template <class TR, class TW>
    class IReducer;

    template <class TR, class TW>
    class IAggregatorReducer;

    struct TSuspendOperationOptions;

    struct TResumeOperationOptions;

    enum class EOperationBriefState : int;

    struct TOperationAttributes;

    struct TOperationOptions;

    enum class EOperationAttribute : int;

    struct TOperationAttributeFilter;

    struct TGetOperationOptions;

    struct TListOperationsOptions;

    struct TGetJobOptions;

    struct TListJobsOptions;

    struct IOperationClient;

    enum class EFinishedJobState : int;

    enum class EJobType : int;
    enum class EJobState : int;
    enum class ETaskName : int;
    class TTaskName;

    struct TJobBinaryDefault;

    struct TJobBinaryLocalPath;

    struct TJobBinaryCypressPath;

    using TJobBinaryConfig = std::variant<
        TJobBinaryDefault,
        TJobBinaryLocalPath,
        TJobBinaryCypressPath>;

    struct TRetryConfig;
    class IRetryConfigProvider;
    using IRetryConfigProviderPtr = ::TIntrusivePtr<IRetryConfigProvider>;
}
/// @endcond
