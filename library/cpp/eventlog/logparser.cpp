#include "logparser.h"
#include "evdecoder.h"

#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/digest/murmur.h>
#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/hash_set.h>
#include <util/string/split.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/builder.h>

#include <contrib/libs/re2/re2/re2.h>

#include <algorithm>
#include <array>

namespace {
    bool FastforwardUntilSyncHeader(IInputStream* in) {
        // Usually this function finds the correct header at the first hit
        std::array<char, COMPRESSED_LOG_FRAME_SYNC_DATA.size()> buffer;
        if (in->Load(buffer.data(), buffer.size()) != buffer.size()) {
            return false;
        }

        auto begin = buffer.begin();

        for (;;) {
            if (std::mismatch(
                    begin, buffer.end(),
                    COMPRESSED_LOG_FRAME_SYNC_DATA.begin()).first == buffer.end() &&
                std::mismatch(
                    buffer.begin(), begin,
                    COMPRESSED_LOG_FRAME_SYNC_DATA.begin() + (buffer.end() - begin)).first == begin) {
                return true;
            }
            if (!in->ReadChar(*begin)) {
                return false;
            }
            ++begin;
            if (begin == buffer.end()) {
                begin = buffer.begin();
            }
        }
    }

    bool HasCorrectChecksum(const TFrameHeader& header) {
        // Calculating hash over all the fields of the read header except for the field with the hash of the header itself.
        const size_t baseSize = sizeof(TCompressedFrameBaseHeader) + sizeof(TCompressedFrameHeader2) - sizeof(ui32);
        const ui32 checksum = MurmurHash<ui32>(&header.Basehdr, baseSize);
        return checksum == header.Framehdr.HeaderChecksum;
    }

    TMaybe<TFrameHeader> FindNextFrameHeader(IInputStream* in) {
        for (;;) {
            if (FastforwardUntilSyncHeader(in)) {
                try {
                    return TFrameHeader(*in);
                } catch (const TFrameLoadError& err) {
                    Cdbg << err.what() << Endl;
                    in->Skip(err.SkipAfter);
                }
            } else {
                return Nothing();
            }
        }
    }

    std::pair<TMaybe<TFrameHeader>, TStringBuf> FindNextFrameHeader(TStringBuf span) {
        for (;;) {
            auto iter = std::search(
                    span.begin(), span.end(),
                    COMPRESSED_LOG_FRAME_SYNC_DATA.begin(), COMPRESSED_LOG_FRAME_SYNC_DATA.end());
            const size_t offset = iter - span.begin();

            if (offset != span.size()) {
                span = span.substr(offset);
                try {
                    TMemoryInput in(
                            span.data() + COMPRESSED_LOG_FRAME_SYNC_DATA.size(),
                            span.size() - COMPRESSED_LOG_FRAME_SYNC_DATA.size());
                    return {TFrameHeader(in), span};
                } catch (const TFrameLoadError& err) {
                    Cdbg << err.what() << Endl;
                    span = span.substr(err.SkipAfter);
                }
            } else {
                return {Nothing(), {}};
            }
        }
    }

    size_t FindFrames(const TStringBuf span, ui64 start, ui64 end, ui64 maxRequestDuration) {
        Y_ENSURE(start <= end);

        const auto leftTimeBound = start - Min(start, maxRequestDuration);
        const auto rightTimeBound = end + Min(maxRequestDuration, Max<ui64>() - end);

        TStringBuf subspan = span;
        TMaybe<TFrameHeader> maybeLeftFrame;
        std::tie(maybeLeftFrame, subspan) = FindNextFrameHeader(subspan);

        if (!maybeLeftFrame || maybeLeftFrame->EndTime() > rightTimeBound) {
            return span.size();
        }

        if (maybeLeftFrame->StartTime() > leftTimeBound) {
            return 0;
        }

        while (subspan.size() > maybeLeftFrame->FullLength()) {
            const auto mid = subspan.data() + subspan.size() / 2;
            auto [midFrame, rightHalfSpan] = FindNextFrameHeader({mid, subspan.data() + subspan.size()});
            if (!midFrame) {
                // If mid is in the middle of the last frame, here we will lose it meaning that
                // we will find previous frame as the result.
                // This is fine because we will iterate frames starting from that.
                subspan = subspan.substr(0, subspan.size() / 2);
                continue;
            }
            if (midFrame->StartTime() <= leftTimeBound) {
                maybeLeftFrame = midFrame;
                subspan = rightHalfSpan;
            } else {
                subspan = subspan.substr(0, subspan.size() / 2);
            }
        }

        return subspan.data() - span.data();
    }
}

TFrameHeader::TFrameHeader(IInputStream& in) {
    try {
        ::Load(&in, Basehdr);

        Y_ENSURE(Basehdr.Length, "Empty frame additional data");

        ::Load(&in, Framehdr);
        switch (LogFormat()) {
            case COMPRESSED_LOG_FORMAT_V1:
                break;

            case COMPRESSED_LOG_FORMAT_V2:
            case COMPRESSED_LOG_FORMAT_V3:
            case COMPRESSED_LOG_FORMAT_V4:
            case COMPRESSED_LOG_FORMAT_V5:
                Y_ENSURE(!Framehdr.CompressorVersion, "Wrong compressor");

                Y_ENSURE(HasCorrectChecksum(*this), "Wrong header checksum");
                break;

            default:
                ythrow yexception() << "Unsupported log structure format";
        };

        Y_ENSURE(Framehdr.StartTimestamp <= Framehdr.EndTimestamp, "Wrong start/end timestamps");

        // Each frame must contain at least one event.
        Y_ENSURE(Framehdr.UncompressedDatalen, "Empty frame payload");
    } catch (...) {
        TString location = "";
        if (const auto* cnt = dynamic_cast<TCountingInput *>(&in)) {
            location = "@ " + ToString(cnt->Counter());
        }
        ythrow TFrameLoadError(FrameLength()) << "Frame Load Error" << location << ": " << CurrentExceptionMessage();
    }
}

TFrame::TFrame(IInputStream& in, TFrameHeader header, IEventFactory* fac)
    : TFrameHeader(header)
    , Limiter_(MakeHolder<TLengthLimitedInput>(&in, header.FrameLength()))
    , Fac_(fac)
{
    if (auto* cnt = dynamic_cast<TCountingInput *>(&in)) {
        Address_ = cnt->Counter() - sizeof(TFrameHeader);
    } else {
        Address_ = 0;
    }
}

TFrame::TIterator TFrame::GetIterator(TIntrusiveConstPtr<TEventFilter> eventFilter) const {
    if (EventsCache_.empty()) {
        for (TFrameDecoder decoder{*this, eventFilter.Get()}; decoder.Avail(); decoder.Next()) {
            EventsCache_.emplace_back(*decoder);
        }
    }

    return TIterator(*this, eventFilter);
}

void TFrame::ClearEventsCache() const {
    EventsCache_.clear();
}

TString TFrame::GetCompressedFrame() const {
    const auto left = Limiter_->Left();
    TString payload = Limiter_->ReadAll();
    Y_ENSURE(payload.size() == left, "Could not read frame payload: premature end of stream");
    const ui32 checksum = MurmurHash<ui32>(payload.data(), payload.size());
    Y_ENSURE(checksum == Framehdr.PayloadChecksum, "Invalid frame checksum");

    return payload;
}

TString TFrame::GetRawFrame() const {
    TString frameBuf = GetCompressedFrame();
    TStringInput sin(frameBuf);
    return TZLibDecompress{&sin}.ReadAll();
}

TFrame::TIterator::TIterator(const TFrame& frame, TIntrusiveConstPtr<TEventFilter> filter)
    : Frame_(frame)
    , Size_(frame.EventsCache_.size())
    , Filter_(filter)
    , Index_(0)
{
    SkipToValidEvent();
}

TConstEventPtr TFrame::TIterator::operator*() const {
    return Frame_.GetEvent(Index_);
}

bool TFrame::TIterator::Next() {
    Index_++;
    SkipToValidEvent();
    return Index_ < Size_;
}

void TFrame::TIterator::SkipToValidEvent() {
    if (!Filter_) {
        return;
    }

    for (; Index_ < Size_; ++Index_) {
        if (Filter_->EventAllowed(Frame_.GetEvent(Index_)->Class)) {
            break;
        }
    }
}

TMaybe<TFrame> FindNextFrame(IInputStream* in, IEventFactory* eventFactory) {
    if (auto header = FindNextFrameHeader(in)) {
        return TFrame{*in, *header, eventFactory};
    } else {
        return Nothing();
    }
}

TContainsEventFrameFilter::TContainsEventFrameFilter(const TString& unparsedMatchGroups, const IEventFactory* eventFactory) {
    TVector<TStringBuf> tokens;

    SplitWithEscaping(tokens, unparsedMatchGroups, "/");

    // Amount of match groups
    size_t size = tokens.size();
    MatchGroups.resize(size);

    for (size_t i = 0; i < size; i++) {
        TMatchGroup& group = MatchGroups[i];
        TVector<TStringBuf> groupTokens;
        SplitWithEscaping(groupTokens, tokens[i], ":");

        Y_ENSURE(groupTokens.size() == 3);

        try {
            group.EventID = eventFactory->ClassByName(groupTokens[0]);
        } catch (yexception& e) {
            if (!TryFromString<TEventClass>(groupTokens[0], group.EventID)) {
                e << "\nAppend:\n" << "Cannot derive EventId from EventType: " << groupTokens[0];
                throw e;
            }
        }

        group.FieldName = groupTokens[1];
        group.ValueToMatch = UnescapeCharacters(groupTokens[2], "/:");
    }
}

bool TContainsEventFrameFilter::FrameAllowed(const TFrame& frame) const {
    THashSet<size_t> toMatchSet;
    for (size_t i = 0; i < MatchGroups.size(); i++) {
        toMatchSet.insert(i);
    }

    for (auto it = frame.GetIterator(); it.Avail(); it.Next()) {
        TConstEventPtr event(*it);
        TVector<size_t> indicesToErase;

        if (!toMatchSet.empty()) {
            const NProtoBuf::Message* message = event->GetProto();
            const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
            const google::protobuf::Reflection* reflection = message->GetReflection();

            Y_ENSURE(descriptor);
            Y_ENSURE(reflection);

            for (size_t groupIndex : toMatchSet) {
                const TMatchGroup& group = MatchGroups[groupIndex];

                if (event->Class == group.EventID) {
                    TVector<TString> parts = StringSplitter(group.FieldName).Split('.').ToList<TString>();
                    TString lastPart = std::move(parts.back());
                    parts.pop_back();

                    for (auto part : parts) {
                        auto fieldDescriptor = descriptor->FindFieldByName(part);
                        Y_ENSURE(fieldDescriptor, "Cannot find field \"" + part + "\". Full fieldname is \"" + group.FieldName + "\".");

                        message = &reflection->GetMessage(*message, fieldDescriptor);
                        descriptor = message->GetDescriptor();
                        reflection = message->GetReflection();

                        Y_ENSURE(descriptor);
                        Y_ENSURE(reflection);
                    }

                    const google::protobuf::FieldDescriptor* fieldDescriptor = descriptor->FindFieldByName(lastPart);
                    Y_ENSURE(fieldDescriptor, "Cannot find field \"" + lastPart + "\". Full fieldname is \"" + group.FieldName + "\".");

                    TString fieldValue = GetEventFieldAsString(message, fieldDescriptor, reflection);
                    if (re2::RE2::FullMatch(fieldValue, group.ValueToMatch)) {
                        indicesToErase.push_back(groupIndex);
                    }
                }
            }

            for (size_t idx : indicesToErase) {
                toMatchSet.erase(idx);
            }

            if (toMatchSet.empty()) {
                return true;
            }
        }
    }

    return toMatchSet.empty();
}

void SplitWithEscaping(TVector<TStringBuf>& tokens, const TStringBuf& stringToSplit, const TStringBuf& externalCharacterSet) {
    size_t tokenStart = 0;
    const TString characterSet = TString::Join("\\", externalCharacterSet);

    for (size_t position = stringToSplit.find_first_of(characterSet); position != TString::npos; position = stringToSplit.find_first_of(characterSet, position + 1)) {
        if (stringToSplit[position] == '\\') {
            position++;
        } else {
            if (tokenStart != position) {
                tokens.push_back(TStringBuf(stringToSplit, tokenStart, position - tokenStart));
            }
            tokenStart = position + 1;
        }
    }

    if (tokenStart < stringToSplit.size()) {
        tokens.push_back(TStringBuf(stringToSplit, tokenStart, stringToSplit.size() - tokenStart));
    }
}

TString UnescapeCharacters(const TStringBuf& stringToUnescape, const TStringBuf& characterSet) {
    TStringBuilder stringBuilder;
    size_t tokenStart = 0;

    for (size_t position = stringToUnescape.find('\\', 0u); position != TString::npos; position = stringToUnescape.find('\\', position + 2)) {
        if (position + 1 < stringToUnescape.size() && characterSet.find(stringToUnescape[position + 1]) != TString::npos) {
            stringBuilder << TStringBuf(stringToUnescape, tokenStart, position - tokenStart);
            tokenStart = position + 1;
        }
    }

    if (tokenStart < stringToUnescape.size()) {
        stringBuilder << TStringBuf(stringToUnescape, tokenStart, stringToUnescape.size() - tokenStart);
    }

    return stringBuilder;
}

TString GetEventFieldAsString(const NProtoBuf::Message* message, const google::protobuf::FieldDescriptor* fieldDescriptor, const google::protobuf::Reflection* reflection) {
    Y_ENSURE(message);
    Y_ENSURE(fieldDescriptor);
    Y_ENSURE(reflection);

    TString result;
    switch (fieldDescriptor->type()) {
        case google::protobuf::FieldDescriptor::Type::TYPE_DOUBLE:
            result = ToString(reflection->GetDouble(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_FLOAT:
            result = ToString(reflection->GetFloat(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_BOOL:
            result = ToString(reflection->GetBool(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_INT32:
            result = ToString(reflection->GetInt32(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_UINT32:
            result = ToString(reflection->GetUInt32(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_INT64:
            result = ToString(reflection->GetInt64(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_UINT64:
            result = ToString(reflection->GetUInt64(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_STRING:
            result = ToString(reflection->GetString(*message, fieldDescriptor));
            break;
        case google::protobuf::FieldDescriptor::Type::TYPE_ENUM:
            {
                const NProtoBuf::EnumValueDescriptor* enumValueDescriptor = reflection->GetEnum(*message, fieldDescriptor);
                result = ToString(enumValueDescriptor->name());
            }
            break;
        default:
            throw yexception() << "GetEventFieldAsString for type " << fieldDescriptor->type_name() << " is not implemented.";
    }
    return result;
}

TFrameStreamer::TFrameStreamer(IInputStream& s, IEventFactory* fac, IFrameFilterRef ff)
    : In_(&s)
    , FrameFilter_(ff)
    , EventFactory_(fac)
{
    Frame_ = FindNextFrame(&In_, EventFactory_);

    SkipToAllowedFrame();
}

TFrameStreamer::TFrameStreamer(
        const TString& fileName,
        ui64 startTime,
        ui64 endTime,
        ui64 maxRequestDuration,
        IEventFactory* fac,
        IFrameFilterRef ff)
    : File_(TBlob::FromFile(fileName))
    , MemoryIn_(File_.Data(), File_.Size())
    , In_(&MemoryIn_)
    , StartTime_(startTime)
    , EndTime_(endTime)
    , CutoffTime_(endTime + Min(maxRequestDuration, Max<ui64>() - endTime))
    , FrameFilter_(ff)
    , EventFactory_(fac)
{
    In_.Skip(FindFrames(File_.AsStringBuf(), startTime, endTime, maxRequestDuration));
    Frame_ = FindNextFrame(&In_, fac);
    SkipToAllowedFrame();
}

TFrameStreamer::~TFrameStreamer() = default;

bool TFrameStreamer::Avail() const {
    return Frame_.Defined();
}

const TFrame& TFrameStreamer::operator*() const {
    Y_ENSURE(Frame_, "Frame streamer depleted");

    return *Frame_;
}

bool TFrameStreamer::Next() {
    DoNext();
    SkipToAllowedFrame();

    return Frame_.Defined();
}

bool TFrameStreamer::AllowedTimeRange(const TFrame& frame) const {
    const bool allowedStartTime = (StartTime_ == 0) || ((StartTime_ <= frame.StartTime()) && (frame.StartTime() <= EndTime_));
    const bool allowedEndTime = (EndTime_ == 0) || ((StartTime_ <= frame.EndTime()) && (frame.EndTime() <= EndTime_));
    return allowedStartTime || allowedEndTime;
}

bool TFrameStreamer::DoNext() {
    if (!Frame_) {
        return false;
    }
    In_.Skip(Frame_->Limiter_->Left());
    Frame_ = FindNextFrame(&In_, EventFactory_);

    if (Frame_ && CutoffTime_ > 0 && Frame_->EndTime() > CutoffTime_) {
        Frame_.Clear();
    }

    return Frame_.Defined();
}

namespace {
    struct TDecodeBuffer {
        TDecodeBuffer(const TString codec, IInputStream& src, size_t bs) {
            TBuffer from(bs);

            {
                TBufferOutput b(from);
                TransferData(&src, &b);
            }

            NBlockCodecs::Codec(codec)->Decode(from, DecodeBuffer);
        }

        explicit TDecodeBuffer(IInputStream& src) {
            TBufferOutput b(DecodeBuffer);
            TransferData(&src, &b);
        }

        TBuffer DecodeBuffer;
    };

    class TBlockCodecStream: private TDecodeBuffer, public TBufferInput {
    public:
        TBlockCodecStream(const TString codec, IInputStream& src, size_t bs)
            : TDecodeBuffer(codec, src, bs)
            , TBufferInput(DecodeBuffer)
        {}

        explicit TBlockCodecStream(IInputStream& src)
            : TDecodeBuffer(src)
            , TBufferInput(DecodeBuffer)
        {}
    };
}

TFrameDecoder::TFrameDecoder(const TFrame& fr, const TEventFilter* const filter, bool strict, bool withRawData)
    : Frame_(fr)
    , Event_(nullptr)
    , Flt_(filter)
    , Fac_(fr.Fac_)
    , EndOfFrame_(new TEndOfFrameEvent(Frame_.EndTime()))
    , Strict_(strict)
    , WithRawData_(withRawData)
{
    switch (fr.LogFormat()) {
        case COMPRESSED_LOG_FORMAT_V2:
        case COMPRESSED_LOG_FORMAT_V3:
        case COMPRESSED_LOG_FORMAT_V4:
        case COMPRESSED_LOG_FORMAT_V5: {
            const auto payload = fr.GetCompressedFrame();
            TMemoryInput payloadInput{payload};

            if (fr.LogFormat() == COMPRESSED_LOG_FORMAT_V5) {
                Decompressor_.Reset(new TBlockCodecStream("zstd_1", payloadInput, payload.size()));
            } else {
                TZLibDecompress zlib(&payloadInput);
                Decompressor_.Reset(new TBlockCodecStream(zlib));
                if (fr.LogFormat() == COMPRESSED_LOG_FORMAT_V4) {
                    Decompressor_.Reset(new TBlockCodecStream("lz4hc", *Decompressor_, payload.size()));
                }
            }

            break;
        }

        default:
            ythrow yexception() << "unsupported log format: " << fr.LogFormat() << Endl;
            break;
    };

    if (WithRawData_) {
        TBufferOutput out(UncompressedData_);
        TLengthLimitedInput limiter(Decompressor_.Get(), fr.Framehdr.UncompressedDatalen);

        TransferData(&limiter, &out);
        Decompressor_.Reset(new TMemoryInput(UncompressedData_.data(), UncompressedData_.size()));
    }

    Limiter_.Reset(new TLengthLimitedInput(Decompressor_.Get(), fr.Framehdr.UncompressedDatalen));

    Decode();
}

TFrameDecoder::~TFrameDecoder() = default;

bool TFrameDecoder::Avail() const {
    return HaveData();
}

TConstEventPtr TFrameDecoder::operator*() const {
    Y_ENSURE(HaveData(), "Decoder depleted");

    return Event_;
}

bool TFrameDecoder::Next() {
    if (HaveData()) {
        Decode();
    }

    return HaveData();
}

void TFrameDecoder::Decode() {
    Event_ = nullptr;
    const bool framed = (Frame_.LogFormat() == COMPRESSED_LOG_FORMAT_V3) || (Frame_.LogFormat() == COMPRESSED_LOG_FORMAT_V4 || Frame_.LogFormat() == COMPRESSED_LOG_FORMAT_V5);

    size_t evBegin = 0;
    size_t evEnd = 0;
    if (WithRawData_)
        evBegin = UncompressedData_.Size() - Limiter_->Left();

    while (Limiter_->Left() && !(Event_ = DecodeEvent(*Limiter_, framed, Frame_.Address(), Flt_, Fac_, Strict_).Release())) {
    }

    if (WithRawData_) {
        evEnd = UncompressedData_.Size() - Limiter_->Left();
        RawEventData_ = TStringBuf(UncompressedData_.data() + evBegin, UncompressedData_.data() + evEnd);
    }

    if (!Event_ && (!Flt_ || (Flt_->EventAllowed(TEndOfFrameEvent::EventClass)))) {
        Event_ = EndOfFrame_.Release();
    }

    if (!!Event_) {
        Event_->FrameId = Frame_.FrameId();
    }
}

const TStringBuf TFrameDecoder::GetRawEvent() const {
    return RawEventData_;
}

TEventStreamer::TEventStreamer(TFrameStream& fs, ui64 s, ui64 e, bool strongOrdering, TIntrusivePtr<TEventFilter> filter, bool losslessStrongOrdering)
    : Frames_(fs)
    , Start_(s)
    , End_(e)
    , MaxEndTimestamp_(0)
    , Frontier_(0)
    , StrongOrdering_(strongOrdering)
    , LosslessStrongOrdering_(losslessStrongOrdering)
    , EventFilter_(filter)
{

    if (Start_ > End_) {
        ythrow yexception() << "Wrong main interval";
    }

    TEventStreamer::Next();
}

TEventStreamer::~TEventStreamer() = default;

bool TEventStreamer::Avail() const {
    return Events_.Avail() && (*Events_)->Timestamp <= Frontier_;
}

TConstEventPtr TEventStreamer::operator*() const {
    Y_ENSURE(TEventStreamer::Avail(), "Event streamer depleted");

    return *Events_;
}

bool TEventStreamer::Next() {
    if (Events_.Avail() && Events_.Next() && (*Events_)->Timestamp <= Frontier_) {
        return true;
    }

    for (;;) {
        if (!LoadMoreEvents()) {
            return false;
        }

        if (TEventStreamer::Avail()) {
            return true;
        }
    }
}

/*
Two parameters are used in the function:
Frontier - the moment of time up to which inclusively all the log events made their way
 into the buffer (and might have been already extracted out of it).
Horizon - the moment of time, that equals to Frontier + MAX_REQUEST_DURATION.
In order to get all the log events up to the Frontier inclusively,
 frames need to be read until "end time" of the current frame exceeds the Horizon.
*/
bool TEventStreamer::LoadMoreEvents() {
    if (!Frames_.Avail()) {
        return false;
    }

    const TFrame& fr1 = *Frames_;
    const ui64 maxRequestDuration = (StrongOrdering_ ? MAX_REQUEST_DURATION : 0);

    if (fr1.EndTime() <= Frontier_ + maxRequestDuration) {
        ythrow yexception() << "Wrong frame stream state";
    }

    if (Frontier_ >= End_) {
        return false;
    }

    const ui64 old_frontier = Frontier_;
    Frontier_ = fr1.EndTime();

    {
        Y_DEFER {
            Events_.Reorder(StrongOrdering_);
        };

        for (; Frames_.Avail(); Frames_.Next()) {
            const TFrame& fr2 = *Frames_;

            // Frames need to start later than the Frontier.
            if (StrongOrdering_ && fr2.StartTime() <= old_frontier) {
                Cdbg << "Invalid frame encountered" << Endl;
                continue;
            }

            if (fr2.EndTime() > MaxEndTimestamp_) {
                MaxEndTimestamp_ = fr2.EndTime();
            }

            if (fr2.EndTime() > Frontier_ + maxRequestDuration && !LosslessStrongOrdering_) {
                return true;
            }

            // Checking for the frame to be within the main time borders.
            if (fr2.EndTime() >= Start_ && fr2.StartTime() <= End_) {
                TransferEvents(fr2);
            }
        }
    }

    Frontier_ = MaxEndTimestamp_;

    return true;
}

void TEventStreamer::TransferEvents(const TFrame& fr) {
    Events_.SetCheckpoint();

    try {
        for (auto it = fr.GetIterator(EventFilter_); it.Avail(); it.Next()) {
            TConstEventPtr ev = *it;

            if (ev->Timestamp > fr.EndTime() || ev->Timestamp < fr.StartTime()) {
                ythrow TInvalidEventTimestamps() << "Event timestamp out of frame range";
            }

            if (ev->Timestamp >= Start_ && ev->Timestamp <= End_) {
                Events_.Append(ev, StrongOrdering_);
            }
        }
    } catch (const TInvalidEventTimestamps& err) {
        Events_.Rollback();
        Cdbg << "EventsTransfer error: InvalidEventTimestamps: " << err.what() << Endl;
    } catch (const TFrameLoadError& err) {
        Events_.Rollback();
        Cdbg << "EventsTransfer error: " << err.what() << Endl;
    } catch (const TEventDecoderError& err) {
        Events_.Rollback();
        Cdbg << "EventsTransfer error: EventDecoder error: " << err.what() << Endl;
    } catch (const TZLibDecompressorError& err) {
        Events_.Rollback();
        Cdbg << "EventsTransfer error: ZLibDecompressor error: " << err.what() << Endl;
    } catch (...) {
        Events_.Rollback();
        throw;
    }
}

void TEventStreamer::TEventBuffer::SetCheckpoint() {
    BufLen_ = Buffer_.size();
}

void TEventStreamer::TEventBuffer::Rollback() {
    Buffer_.resize(BufLen_);
}

void TEventStreamer::TEventBuffer::Reorder(bool strongOrdering) {
    SetCheckpoint();

    std::reverse(Buffer_.begin(), Buffer_.end());

    if (strongOrdering) {
        StableSort(Buffer_.begin(), Buffer_.end(), [&](const auto& a, const auto& b) {
            return (a->Timestamp > b->Timestamp) ||
                   ((a->Timestamp == b->Timestamp) && !a->Class && b->Class);
        });
    }
}

void TEventStreamer::TEventBuffer::Append(TConstEventPtr ev, bool strongOrdering) {
    // Events in buffer output must be in an ascending order.
    Y_ENSURE(!strongOrdering || ev->Timestamp >= LastTimestamp_, "Trying to append out-of-order event");

    Buffer_.push_back(std::move(ev));
}

bool TEventStreamer::TEventBuffer::Avail() const {
    return !Buffer_.empty();
}

TConstEventPtr TEventStreamer::TEventBuffer::operator*() const {
    Y_ENSURE(!Buffer_.empty(), "Event buffer is empty");

    return Buffer_.back();
}

bool TEventStreamer::TEventBuffer::Next() {
    if (!Buffer_.empty()) {
        LastTimestamp_ = Buffer_.back()->Timestamp;
        Buffer_.pop_back();
        return !Buffer_.empty();
    } else {
        return false;
    }
}
