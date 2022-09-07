#pragma once

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>


namespace NKikimr::NGRpcProxy {

    struct TTopicHolder {
        ui64 TabletID;
        TActorId PipeClient;
        bool ACLRequestInfly;
        TString CloudId;
        TString DbId;
        TString FolderId;
        NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode;
        NPersQueue::TDiscoveryConverterPtr DiscoveryConverter;
        NPersQueue::TTopicConverterPtr FullConverter;
        TMaybe<TString> CdcStreamPath;

        TVector<ui32> Groups;
        TMap<ui64, ui64> Partitions;

        TTopicHolder()
                : TabletID(0)
                , PipeClient()
                , ACLRequestInfly(false)
        {}
    };

    struct TTopicInitInfo {
        NPersQueue::TTopicConverterPtr TopicNameConverter;
        ui64 TabletID;
        TString CloudId;
        TString DbId;
        TString FolderId;
        NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode;
    };

    using TTopicInitInfoMap = THashMap<TString, TTopicInitInfo>;

} //    namespace NKikimr::NGRpcProxy
