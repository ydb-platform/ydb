#pragma once

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/keys.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView {

    template<typename TDerived, typename TEvResponse>
    class TStorageScanBase : public TScanActorBase<TStorageScanBase<TDerived, TEvResponse>> {
        using TBase = TScanActorBase<TStorageScanBase<TDerived, TEvResponse>>;

    public:
        using TScanActorBase<TStorageScanBase>::TScanActorBase;

        STRICT_STFUNC(StateScan,
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, TBase::HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, TBase::HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        )

    protected:
        using TFieldMap = std::unordered_map<NTable::TTag, std::vector<int>>;

    private:
        void ProceedToScan() override {
            TBase::Become(&TStorageScanBase::StateScan);
            if (TBase::AckReceived) {
                StartScan();
            }
        }

        void StartScan() {
            ui64 bsControllerId = TBase::GetBSControllerId();
            if (!bsControllerId) {
                return;
            }

            auto pipeCache = MakePipePerNodeCacheID(false);
            TBase::Send(pipeCache, new TEvPipeCache::TEvForward(static_cast<TDerived&>(*this).CreateQuery(),
                bsControllerId, true), IEventHandle::FlagTrackDelivery);
        }

        void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
            StartScan();
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Delivery problem in NSysView::"
                << static_cast<TDerived&>(*this).GetName());
        }

        template<typename TTo, typename TFrom>
        TCell Convert(const TFrom& value) {
            const TTo tmp(value);
            Y_DEBUG_ABORT_UNLESS(TFrom(tmp) == value);
            return TCell::Make<TTo>(tmp);
        }

        template<typename T>
        TCell MakeCellFrom(const T& value, NScheme::TTypeId type) {
            switch (type) {
                case NScheme::NTypeIds::Int32:     return Convert<i32>(value);
                case NScheme::NTypeIds::Uint32:    return Convert<ui32>(value);
                case NScheme::NTypeIds::Int64:     return Convert<i64>(value);
                case NScheme::NTypeIds::Uint64:    return Convert<ui64>(value);
                case NScheme::NTypeIds::Bool:      return Convert<bool>(value);
                case NScheme::NTypeIds::Int8:      return Convert<i8>(value);
                case NScheme::NTypeIds::Uint8:     return Convert<ui8>(value);
                case NScheme::NTypeIds::Int16:     return Convert<i16>(value);
                case NScheme::NTypeIds::Uint16:    return Convert<ui16>(value);
                case NScheme::NTypeIds::Double:    return Convert<double>(value);
                case NScheme::NTypeIds::Float:     return Convert<float>(value);
                case NScheme::NTypeIds::Interval:  return Convert<i64>(value);
                case NScheme::NTypeIds::Timestamp: return Convert<ui64>(value);
                default: Y_ABORT();
            }
        }

        TCell MakeCellFrom(const TString& value, NScheme::TTypeId type) {
            Y_ABORT_UNLESS(type == NScheme::NTypeIds::String || type == NScheme::NTypeIds::String4k ||
                type == NScheme::NTypeIds::String2m || type == NScheme::NTypeIds::Utf8);
            return TCell(value.data(), value.size());
        }

        TCell ExtractCell(const NProtoBuf::Message *m, const NProtoBuf::FieldDescriptor *f, NScheme::TTypeId type) {
            const NProtoBuf::Reflection *r = m->GetReflection();
            if (!r->HasField(*m, f)) {
                return TCell();
            }

            using E = NProtoBuf::FieldDescriptor::Type;
            switch (f->type()) {
                case E::TYPE_DOUBLE:
                    return MakeCellFrom(r->GetDouble(*m, f), type);

                case E::TYPE_FLOAT:
                    return MakeCellFrom(r->GetFloat(*m, f), type);

                case E::TYPE_INT64:
                case E::TYPE_SFIXED64:
                case E::TYPE_SINT64:
                    return MakeCellFrom(r->GetInt64(*m, f), type);

                case E::TYPE_INT32:
                case E::TYPE_SFIXED32:
                case E::TYPE_SINT32:
                    return MakeCellFrom(r->GetInt32(*m, f), type);

                case E::TYPE_UINT64:
                case E::TYPE_FIXED64:
                    return MakeCellFrom(r->GetUInt64(*m, f), type);

                case E::TYPE_UINT32:
                case E::TYPE_FIXED32:
                    return MakeCellFrom(r->GetUInt32(*m, f), type);

                case E::TYPE_BOOL:
                    return MakeCellFrom(r->GetBool(*m, f), type);

                case E::TYPE_STRING:
                case E::TYPE_BYTES: {
                    TString str;
                    const TString& res = r->GetStringReference(*m, f, &str);
                    if (&res == &str) {
                        OwnedStringWarehouse.push_back(std::move(str));
                        return MakeCellFrom(OwnedStringWarehouse.back(), type);
                    } else {
                        return MakeCellFrom(res, type);
                    }
                }

                case E::TYPE_GROUP:
                case E::TYPE_MESSAGE:
                case E::TYPE_ENUM:
                    Y_ABORT();
            }
        }

        void Handle(typename TEvResponse::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(TBase::ScanId);

            const auto& fieldMap = TDerived::GetFieldMap();
            TVector<TCell> cells;
            for (const auto& entry : record.GetEntries()) {
                for (auto column : TBase::Columns) {
                    if (const auto it = fieldMap.find(column.Tag); it != fieldMap.end()) {
                        const auto& path = it->second;
                        const NProtoBuf::Message *m = &entry;
                        for (auto it = path.begin(); it != path.end(); ++it) {
                            const NProtoBuf::Descriptor *desc = m->GetDescriptor();
                            const NProtoBuf::FieldDescriptor *fdesc = desc->FindFieldByNumber(*it);
                            if (std::next(it) == path.end()) { // terminal entry
                                cells.push_back(ExtractCell(m, fdesc, column.Type.GetTypeId()));
                            } else { // submessage
                                Y_ABORT_UNLESS(fdesc->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE);
                                m = &m->GetReflection()->GetMessage(*m, fdesc);
                            }
                        }
                    } else {
                        cells.emplace_back();
                    }
                }

                TArrayRef<const TCell> ref(cells);
                batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
                cells.clear();
            }

            batch->Finished = true;
            TBase::SendBatch(std::move(batch));
        }

        void PassAway() override {
            TBase::Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
            TBase::PassAway();
        }

    private:
        std::deque<TString> OwnedStringWarehouse;
    };

} // NKikimr::NSysView
