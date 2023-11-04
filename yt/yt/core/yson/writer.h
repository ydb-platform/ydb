#pragma once

#include "public.h"
#include "consumer.h"
#include "token_writer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! A canonical implementation of YSON data stream writer.
class TYsonWriter
    : public TYsonConsumerBase
    , public virtual IFlushableYsonConsumer
    , private TNonCopyable
{
public:
    static constexpr int DefaultIndent = 4;

    //! Initializes an instance.
    /*!
     *  \param stream A stream for writing the YSON data to.
     *  \param format A format used for encoding the data.
     *  \param enableRaw Enables inserting raw portions of YSON as-is, without reparse.
     */
    TYsonWriter(
        IOutputStream* stream,
        EYsonFormat format = EYsonFormat::Binary,
        EYsonType type = EYsonType::Node,
        bool enableRaw = false,
        int indent = DefaultIndent,
        bool passThroughUtf8Characters = false);

    // IYsonConsumer overrides.
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    void Flush() override;

    int GetDepth() const;

protected:
    IOutputStream* const Stream_;
    const EYsonFormat Format_;
    const EYsonType Type_;
    const bool EnableRaw_;
    const int IndentSize_;
    const bool PassThroughUtf8Characters_;

    int Depth_ = 0;
    bool EmptyCollection_ = true;


    void WriteIndent();
    void WriteStringScalar(TStringBuf value);

    void BeginCollection(char ch);
    void CollectionItem();
    void EndCollection(char ch);

    void EndNode();
};

////////////////////////////////////////////////////////////////////////////////

//! An optimized version of YSON stream writer.
/*!
 *  This writer buffers its output so don't forget to call #Flush.
 *  Only binary YSON format is supported.
 */
class TBufferedBinaryYsonWriter
    : public TYsonConsumerBase
    , public virtual IFlushableYsonConsumer
    , private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param stream A stream for writing the YSON data to.
     *  \param format A format used for encoding the data.
     *  \param enableRaw Enables inserting raw portions of YSON as-is, without reparse.
     */
    TBufferedBinaryYsonWriter(
        IZeroCopyOutput* stream,
        EYsonType type = EYsonType::Node,
        bool enableRaw = true,
        std::optional<int> nestingLevelLimit = std::nullopt);

    // IYsonConsumer overrides.
    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override;

    void Flush() override;

    int GetDepth() const;

    ui64 GetTotalWrittenSize() const;

protected:
    const EYsonType Type_;
    const bool EnableRaw_;

    std::optional<TUncheckedYsonTokenWriter> TokenWriter_;

    int NestingLevelLimit_;
    int Depth_ = 0;

    void WriteStringScalar(TStringBuf value);
    void BeginCollection();
    void EndNode();

};

////////////////////////////////////////////////////////////////////////////////

//! Creates either TYsonWriter or TBufferedBinaryYsonWriter, depending on #format.
std::unique_ptr<IFlushableYsonConsumer> CreateYsonWriter(
    IZeroCopyOutput* output,
    EYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent = TYsonWriter::DefaultIndent,
    bool passThroughUtf8Characters = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

