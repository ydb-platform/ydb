#pragma once

#include <library/cpp/unified_agent_client/f_maybe.h>
#include <library/cpp/unified_agent_client/proto_weighing.h>

namespace NUnifiedAgent::NPW {
    struct TMessageMetaItem: public TMessage {
        TMessageMetaItem()
            : TMessage()
            , Key(this)
            , Value(this)
            , SkipStart(this)
            , SkipLength(this)
        {
        }

        explicit TMessageMetaItem(TMessage* parent)
            : TMessage(parent)
            , Key(this)
            , Value(this)
            , SkipStart(this)
            , SkipLength(this)
        {
        }

        explicit TMessageMetaItem(const NUnifiedAgent::TFMaybe<TFieldLink>& link)
            : TMessage(link)
            , Key(this)
            , Value(this)
            , SkipStart(this)
            , SkipLength(this)
        {
        }

        TStringField Key;
        TRepeatedPtrField<TStringField> Value;
        TRepeatedField<ui32> SkipStart;
        TRepeatedField<ui32> SkipLength;
    };

    struct TDataBatch: public TMessage {
        TDataBatch()
            : TMessage()
            , SeqNo(this)
            , Timestamp(this)
            , Payload(this, 2)
            , Meta(this, 2)
        {
        }

        TDataBatch(TMessage* parent)
            : TMessage(parent)
            , SeqNo(this)
            , Timestamp(this)
            , Payload(this, 2)
            , Meta(this, 2)
        {
        }

        TRepeatedField<ui64> SeqNo;  // 1
        TRepeatedField<ui64> Timestamp; // 2
        TRepeatedPtrField<TStringField> Payload; // 100
        TRepeatedPtrField<TMessageMetaItem> Meta; // 101
    };

    struct TRequest: public TMessage {
        TRequest()
            : TMessage()
            , DataBatch(this)
        {
        }

        TDataBatch DataBatch;
    };
}
