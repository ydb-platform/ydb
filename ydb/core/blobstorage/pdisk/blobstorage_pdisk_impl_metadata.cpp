#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_completion_impl.h"

namespace NKikimr::NPDisk {

    namespace {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TCompletionReadMetadata handles single slot read during metadata loading on a formatted device

        class TCompletionReadMetadata : public TCompletionAction {
            TPDisk* const PDisk;
            std::unique_ptr<TInitialReadMetadataResult> Req;
            TRcBuf Buffer;

        public:
            TCompletionReadMetadata(TPDisk *pdisk, size_t bytesToRead, std::unique_ptr<TInitialReadMetadataResult> req)
                : PDisk(pdisk)
                , Req(std::move(req))
                , Buffer(TRcBuf::UninitializedPageAligned(bytesToRead))
            {}

            void *GetBuffer() {
                return Buffer.GetDataMut();
            }

            bool CanHandleResult() const override { return true; }

            void Exec(TActorSystem *actorSystem) override {
                if (Result != EIoResult::Ok) {
                    Req->ErrorReason = std::move(ErrorReason);
                    PDisk->InputRequest(Req.release());
                    return Release(actorSystem);
                }

                Req->ErrorReason = "unspecified error"; // we assume error

                TPDiskStreamCypher cypher(PDisk->Cfg->EnableSectorEncryption);
                cypher.SetKey(PDisk->Format.ChunkKey);

                // obtain the header and decrypt its encrypted part, then validate the hash
                TMetadataHeader *header = reinterpret_cast<TMetadataHeader*>(GetBuffer());
                header->Encrypt(cypher);
                if (header->CheckHash()) {
                    // check we have read it all
                    const ui32 total = sizeof(TMetadataHeader) + header->Length;
                    if (total <= Buffer.size()) {
                        // already have all the buffer
                        header->EncryptData(cypher);
                        if (header->CheckDataHash()) {
                            Req->ErrorReason = {}; 
                            Req->Header = *header;
                            Req->Payload = TRcBuf(TRcBuf::Piece, reinterpret_cast<const char*>(header + 1), header->Length, Buffer);
                        } else {
                            Req->ErrorReason = "metadata slot header passes validation, data does not -- corrupt data on disk";
                        }
                    } else {
                        // have only the part of the buffer -- read more
                        const size_t bytesToRead = PDisk->Format.RoundUpToSectorSize(sizeof(TMetadataHeader) + header->Length);
                        Buffer = TRcBuf::UninitializedPageAligned(bytesToRead); // __header is not valid anymore__
                        const ui64 offset = PDisk->Format.Offset(Req->Key.ChunkIdx, Req->Key.OffsetInSectors);
                        PDisk->BlockDevice->PreadAsync(GetBuffer(), bytesToRead, offset, this, Req->ReqId, nullptr);
                        return;
                    }
                } else {
                    Req->ErrorReason = "header checksum does not pass validation";
                }

                PDisk->InputRequest(Req.release());
                Release(actorSystem);
            }

            void Release(TActorSystem * /*actorSystem*/) override {
                delete this;
            }
        };

        class TCompletionWriteMetadata : public TCompletionAction {
            TPDisk* const PDisk;
            const TActorId Sender;
            std::deque<std::tuple<NMeta::TSlotKey, TRcBuf>> WriteQueue;

        public:
            TCompletionWriteMetadata(TPDisk *pdisk, TActorId sender)
                : PDisk(pdisk)
                , Sender(sender)
            {}

            bool CanHandleResult() const override { return true; }

            void AddQuery(NMeta::TSlotKey key, TRcBuf&& buffer) {
                WriteQueue.emplace_back(key, std::move(buffer));
            }

            void IssueQuery(TActorSystem *actorSystem) {
                Y_ABORT_UNLESS(!WriteQueue.empty());
                auto& [key, buffer] = WriteQueue.front();
                const ui64 writeOffset = PDisk->Format.Offset(key.ChunkIdx, key.OffsetInSectors);
                LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " TCompletionWriteMetadata::IssueQuery"
                    << " Buffer.size# " << buffer.size()
                    << " WriteOffset# " << writeOffset
                    << " ChunkIdx# " << key.ChunkIdx
                    << " OffsetInSectors# " << key.OffsetInSectors);
                PDisk->BlockDevice->PwriteAsync(buffer.data(), buffer.size(), writeOffset, this, {}, nullptr);
            }

            void Exec(TActorSystem *actorSystem) override {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " TCompletionWriteMetadata::Exec"
                    << " Result# " << Result);
                Y_ABORT_UNLESS(!WriteQueue.empty());
                WriteQueue.pop_front();
                if (Result != EIoResult::Ok) {
                    PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TWriteMetadataResult>(false, Sender));
                } else if (WriteQueue.empty()) {
                    PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TWriteMetadataResult>(true, Sender));
                } else {
                    return IssueQuery(actorSystem);
                }
                delete this;
            }

            void Release(TActorSystem *actorSystem) override {
                actorSystem->Send(Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
                delete this;
            }
        };

        class TCompletionReadUnformattedMetadata : public TCompletionAction {
            TPDisk* const PDisk;
            const TMetadataFormatSector Format;
            const bool WantEvent;
            TRcBuf Buffer;

        public:
            TCompletionReadUnformattedMetadata(TPDisk *pdisk, const TMetadataFormatSector& format, bool wantEvent)
                : PDisk(pdisk)
                , Format(format)
                , WantEvent(wantEvent)
            {}

            bool CanHandleResult() const override { return true; }

            void IssueQuery() {
                const size_t bytesToRead = PDisk->Format.RoundUpToSectorSize(Format.Length);
                Buffer = TRcBuf::UninitializedPageAligned(bytesToRead);
                PDisk->BlockDevice->PreadAsync(Buffer.GetDataMut(), Buffer.size(), Format.Offset, this, {}, nullptr);
            }

            void Exec(TActorSystem *actorSystem) override {
                std::unique_ptr<TInitialReadMetadataResult> req(
                    PDisk->ReqCreator.CreateFromArgs<TInitialReadMetadataResult>(NMeta::TSlotKey()));
                if (Result != EIoResult::Ok) {
                    req->ErrorReason = "read failed";
                } else if (Buffer.size() < sizeof(TMetadataHeader)) {
                    req->ErrorReason = "buffer is too small to hold TMetadataHeader";
                } else {
                    TPDiskStreamCypher cypher(true);
                    cypher.SetKey(Format.DataKey);

                    auto *header = reinterpret_cast<TMetadataHeader*>(Buffer.GetDataMut());
                    header->Encrypt(cypher);
                    if (!header->CheckHash()) {
                        req->ErrorReason = "header has is not valid";
                    } else if (header->TotalRecords != 1 || header->RecordIndex != 0 || header->SequenceNumber != 0) {
                        req->ErrorReason = "header fields are filled incorrectly";
                    } else if (Buffer.size() < sizeof(TMetadataHeader) + header->Length) {
                        req->ErrorReason = "payload does not fit";
                    } else {
                        header->EncryptData(cypher);
                        if (!header->CheckDataHash()) {
                            req->ErrorReason = "data hash is not valid";
                        } else {
                            req->Header = *header;
                            req->Payload = {TRcBuf::Piece, reinterpret_cast<const char*>(header + 1), header->Length, Buffer};
                        }
                    }
                }
                if (WantEvent) {
                    actorSystem->Send(PDisk->PDiskActor, new TEvPDiskMetadataLoaded(req->ErrorReason ? std::nullopt :
                        std::make_optional(req->Payload)));
                }
                PDisk->InputRequest(req.release());
                Release(actorSystem);
            }

            void Release(TActorSystem* /*actorSystem*/) override {
                delete this;
            }
        };

        class TCompletionWriteUnformattedMetadata : public TCompletionAction {
            TPDisk* const PDisk;
            const TActorId Sender;
            const TMetadataFormatSector Format;
            TRcBuf Payload;
            const TMainKey MainKey;
            int FormatIndex = -1; // -1 for payload
            ui32 BadSectors = 0;

        public:
            TCompletionWriteUnformattedMetadata(TPDisk *pdisk, TActorId sender, const TMetadataFormatSector& format,
                    TRcBuf&& payload, const TMainKey& mainKey)
                : PDisk(pdisk)
                , Sender(sender)
                , Format(format)
                , Payload(std::move(payload))
                , MainKey(mainKey)
            {}

            void IssueQuery() {
                const ui64 offset = FormatIndex == -1
                    ? Format.Offset
                    : FormatIndex * FormatSectorSize;

                if (FormatIndex != -1) {
                    Y_ABORT_UNLESS(static_cast<ui32>(FormatIndex) < ReplicationFactor);
                    Payload = TRcBuf::UninitializedPageAligned(FormatSectorSize);
                    TPDisk::MakeMetadataFormatSector(reinterpret_cast<ui8*>(Payload.GetDataMut()), MainKey, Format);
                }

                LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " TCompletionWriteUnformattedMetadata::IssueQuery"
                    << " FormatIndex# " << FormatIndex
                    << " Payload.size# " << Payload.size()
                    << " Offset# " << offset);

                PDisk->BlockDevice->PwriteAsync(Payload.data(), Payload.size(), offset, this, {}, nullptr);
            }

            bool CanHandleResult() const override { return true; }

            void Exec(TActorSystem * /*actorSystem*/) override {
                LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDisk->PDiskId
                    << " TCompletionWriteUnformattedMetadata::Exec"
                    << " Result# " << Result
                    << " FormatIndex# " << FormatIndex
                    << " BadSectors# " << BadSectors
                    << " ReplicationFactor# " << ReplicationFactor);
                if (Result != EIoResult::Ok) {
                    if (FormatIndex == -1) {
                        return Finish(false);
                    } else {
                        ++BadSectors;
                    }
                }
                if (++FormatIndex == ReplicationFactor) { // finish query
                    Finish(BadSectors < (ReplicationFactor + 1) / 2);
                } else {
                    IssueQuery();
                }
            }

            void Release(TActorSystem *actorSystem) override {
                actorSystem->Send(Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
                delete this;
            }

            void Finish(bool success) {
                PDisk->InputRequest(PDisk->ReqCreator.CreateFromArgs<TWriteMetadataResult>(success, Sender));
                delete this;
            }
        };

    } // anonymous

    void TPDisk::InitFormattedMetadata() {
        Y_ABORT_UNLESS(std::holds_alternative<std::monostate>(Meta.State));
        auto& formatted = Meta.State.emplace<NMeta::TFormatted>();

        std::vector<TChunkIdx> metadataChunks;

        with_lock (StateMutex) {
            // collect all existing metadata chunks
            for (size_t chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
                if (TChunkState& state = ChunkState[chunkIdx]; state.OwnerId == OwnerMetadata) {
                    metadataChunks.push_back(chunkIdx);
                }
            }

            // calculate amount of megabytes we need to have
            const ui32 chunkSize = Format.ChunkSize;
            ui64 metadataBytes = static_cast<ui64>(metadataChunks.size()) * chunkSize;
            const ui64 requiredBytes = 2 * Cfg->MaxMetadataMegabytes * 1_MB; // double the number because we have to store old and current meta

            // allocate more metadata chunks to satisfy config requirements
            size_t chunkIdx = ChunkState.size();
            while (chunkIdx > 0 && metadataBytes < requiredBytes) {
                if (TChunkState& state = ChunkState[--chunkIdx]; !IsOwnerAllocated(state.OwnerId)) {
                    state.OwnerId = OwnerMetadata;
                    state.CommitState = TChunkState::DATA_RESERVED;
                    metadataChunks.push_back(chunkIdx);
                    metadataBytes += chunkSize;
                }
            }
        }

        // prepare metadata slots for reading
        const ui32 half = Format.ChunkSize / (2 * Format.SectorSize);
        for (TChunkIdx chunk : metadataChunks) {
            const NMeta::TSlotKey slot1(chunk, 0);
            formatted.Slots.emplace(slot1, NMeta::ESlotState::READ_PENDING);
            formatted.ReadPending.push_back(slot1);
            const NMeta::TSlotKey slot2(chunk, half);
            formatted.Slots.emplace(slot2, NMeta::ESlotState::READ_PENDING);
            formatted.ReadPending.push_back(slot2);
        }

        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " InitMetadata"
            << " MetadataChunks# " << FormatList(metadataChunks));
    }

    void TPDisk::ReadFormattedMetadataIfNeeded() {
        Y_ABORT_UNLESS(std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata));
        auto& formatted = GetFormattedMeta();

        Y_ABORT_UNLESS(formatted.NumReadsInFlight < formatted.MaxReadsInFlight);

        while (!formatted.ReadPending.empty()) {
            // find the slot we have to read
            const NMeta::TSlotKey& key = formatted.ReadPending.front();
            const auto it = formatted.Slots.find(key);
            Y_ABORT_UNLESS(it != formatted.Slots.end());
            Y_ABORT_UNLESS(it->second == NMeta::ESlotState::READ_PENDING);

            // make completion object and the request that will be pushed back to PDisk thread when the request is complete
            const size_t bytesToRead = Format.RoundUpToSectorSize(sizeof(TMetadataHeader));
            std::unique_ptr<TInitialReadMetadataResult> req(ReqCreator.CreateFromArgs<TInitialReadMetadataResult>(key));
            const TReqId reqId = req->ReqId;
            auto completion = std::make_unique<TCompletionReadMetadata>(this, bytesToRead, std::move(req));
            completion->CostNs = DriveModel.TimeForSizeNs(bytesToRead, key.ChunkIdx, TDriveModel::OP_TYPE_READ);
            void *buffer = completion->GetBuffer();
            const ui64 readOffset = Format.Offset(key.ChunkIdx, key.OffsetInSectors);

            LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " ReadMetadataIfNeeded: initiating read"
                << " ChunkIdx# " << key.ChunkIdx
                << " OffsetInSectors# " << key.OffsetInSectors
                << " ReadOffset# " << readOffset
                << " BytesToRead# " << bytesToRead
                << " ReqId# " << reqId);

            // issue the request
            BlockDevice->PreadAsync(buffer, bytesToRead, readOffset, completion.release(), reqId, nullptr);

            // switch state and remove slot from the read pending queue
            it->second = NMeta::ESlotState::READ_IN_PROGRESS;
            formatted.ReadPending.pop_front();

            if (++formatted.NumReadsInFlight == formatted.MaxReadsInFlight) { // at full capacity
                return;
            }
        }

        if (!formatted.NumReadsInFlight) {
            FinishReadingFormattedMetadata();
        }
    }

    void TPDisk::ProcessInitialReadMetadataResult(TInitialReadMetadataResult& request) {
        std::visit(TOverloaded{
            [](std::monostate&) { Y_ABORT("incorrect case"); },
            [&](NMeta::TUnformatted&) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                    << " ProcessInitialReadMetadataResult (unformatted)"
                    << " ErrorReason# " << request.ErrorReason
                    << " Payload.size# " << request.Payload.size());

                if (request.ErrorReason) {
                    Meta.StoredMetadata = NMeta::TError{.Description = std::move(*request.ErrorReason)};
                } else {
                    Meta.StoredMetadata = std::move(request.Payload);
                }
                ProcessMetadataRequestQueue();
            },
            [&](NMeta::TFormatted& formatted) {
                const auto it = formatted.Slots.find(request.Key);
                Y_ABORT_UNLESS(it != formatted.Slots.end());
                Y_ABORT_UNLESS(it->second == NMeta::ESlotState::READ_IN_PROGRESS);
                Y_ABORT_UNLESS(formatted.NumReadsInFlight);
                --formatted.NumReadsInFlight;

                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                    << " ProcessInitialReadMetadataResult (formatted)"
                    << " ChunkIdx# " << request.Key.ChunkIdx
                    << " OffsetInSectors# " << request.Key.OffsetInSectors
                    << " ErrorReason# " << request.ErrorReason
                    << " Payload.size# " << request.Payload.size());

                if (request.ErrorReason) { // we couldn't read the slot -- mark it as a free one
                    it->second = NMeta::ESlotState::FREE;
                } else {
                    formatted.Parts.emplace_back(request.Key, request.Header, std::move(request.Payload));
                    it->second = NMeta::ESlotState::PROCESSED;
                }

                ReadFormattedMetadataIfNeeded(); // issue next query
            },
        }, Meta.State);
    }

    void TPDisk::FinishReadingFormattedMetadata() {
        auto& formatted = GetFormattedMeta();

        Y_ABORT_UNLESS(formatted.ReadPending.empty());
        for (auto& [_, state] : formatted.Slots) {
            Y_ABORT_UNLESS(state == NMeta::ESlotState::FREE || state == NMeta::ESlotState::PROCESSED);
        }

        std::sort(formatted.Parts.begin(), formatted.Parts.end());
        Meta.StoredMetadata.emplace<NMeta::TNoMetadata>(); // for the case when we find nothing
        Meta.NextSequenceNumber = formatted.Parts.empty() ? 1 : formatted.Parts.back().Header.SequenceNumber + 1;

        auto markSlots = [&](auto begin, auto end, NMeta::ESlotState newState) {
            for (auto it = begin; it != end; ++it) {
                const NMeta::ESlotState prev = std::exchange(formatted.Slots[it->Key], newState);
                Y_ABORT_UNLESS(prev == NMeta::ESlotState::PROCESSED);
            }
        };

        for (auto it = formatted.Parts.rbegin(); it != formatted.Parts.rend(); ) {
            NMeta::TPart& part = *it;
            const ui64 sequenceNumber = part.Header.SequenceNumber;

            // find the ending range for this sequence number
            decltype(it) endIt;
            for (endIt = it; endIt != formatted.Parts.rend() && endIt->Header.SequenceNumber == sequenceNumber; ++endIt) {}

            // check the validness
            const ui32 totalParts = part.Header.TotalRecords;
            TRope buffer;
            ui32 expectedRecordIndex = totalParts - 1;
            bool success = std::distance(it, endIt) == totalParts;
            for (auto temp = it; temp != endIt; ++temp) {
                Y_ABORT_UNLESS(temp->Header.SequenceNumber == sequenceNumber);
                if (success && temp->Header.TotalRecords == totalParts && temp->Header.RecordIndex == expectedRecordIndex) {
                    --expectedRecordIndex;
                    buffer.Insert(buffer.Begin(), std::move(temp->Payload));
                } else {
                    success = false;
                }
            }
            if (success) { // yes, the sequence is valid, we can apply it
                markSlots(it, endIt, NMeta::ESlotState::OCCUPIED);
                markSlots(endIt, formatted.Parts.rend(), NMeta::ESlotState::FREE);
                Meta.StoredMetadata.emplace<TRcBuf>(buffer);
                break;
            } else { // no, we have to drop this sequence and proceed with the lower one
                markSlots(it, endIt, NMeta::ESlotState::FREE);
            }
        }
        formatted.Parts.clear();

        // start processing any pending metadata requests
        Y_ABORT_UNLESS(!Meta.WriteInFlight);
        ProcessMetadataRequestQueue();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TPDisk::ProcessPushUnformattedMetadataSector(TPushUnformattedMetadataSector& request) {
        Y_ABORT_UNLESS(std::holds_alternative<std::monostate>(Meta.State));
        auto& unformatted = Meta.State.emplace<NMeta::TUnformatted>();
        unformatted.Format = request.Format;
        if (unformatted.Format) {
            auto *completion = new TCompletionReadUnformattedMetadata(this, *unformatted.Format, request.WantEvent);
            completion->IssueQuery();
            Meta.NextSequenceNumber = unformatted.Format->SequenceNumber + 1;
        } else {
            Meta.StoredMetadata.emplace<NMeta::TNoMetadata>();
            ProcessMetadataRequestQueue();
            if (request.WantEvent) {
                ActorSystem->Send(PDiskActor, new TEvPDiskMetadataLoaded(std::nullopt));
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TPDisk::ProcessMetadataRequestQueue() {
        Y_ABORT_UNLESS(!std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata));
        while (!Meta.Requests.empty() && !Meta.WriteInFlight) {
            const size_t sizeBefore = Meta.Requests.size();
            switch (auto& front = Meta.Requests.front(); front->GetType()) {
                case ERequestType::RequestReadMetadata:
                    HandleNextReadMetadata();
                    break;

                case ERequestType::RequestWriteMetadata:
                    HandleNextWriteMetadata();
                    break;

                default:
                    Y_ABORT();
            }
            Y_ABORT_UNLESS(Meta.Requests.size() < sizeBefore || Meta.WriteInFlight);
        }
    }

    void TPDisk::ProcessReadMetadata(std::unique_ptr<TRequestBase> req) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " ProcessReadMetadata: new request"
            << " Sender# " << req->Sender
            << " ScanInProgress# " << std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata)
            << " Requests.size# " << Meta.Requests.size());
        Meta.Requests.push_back(std::move(req));
        if (std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata) || Meta.Requests.size() > 1) {
            return;
        }
        HandleNextReadMetadata();
    }

    void TPDisk::HandleNextReadMetadata() {
        auto& front = Meta.Requests.front();
        Y_ABORT_UNLESS(front->GetType() == ERequestType::RequestReadMetadata);
        Y_ABORT_UNLESS(!Meta.WriteInFlight);
        auto guid = std::visit<std::optional<ui64>>(TOverloaded{
            [](std::monostate&) -> std::nullopt_t { Y_ABORT("incorrect case"); },
            [&](NMeta::TFormatted&) { return Format.Guid; },
            [&](NMeta::TUnformatted&) { return std::nullopt; },
        }, Meta.State);
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " HandleNextReadMetadata"
            << " Guid# " << guid
            << " Sender# " << front->Sender
            << " StoredMetadata# " << NMeta::ToString(&Meta.StoredMetadata));
        auto *response = std::visit<IEventBase*>(TOverloaded{
            [](NMeta::TScanInProgress) -> std::nullptr_t { Y_ABORT("incorrect case"); },
            [&](NMeta::TNoMetadata&) { return new TEvReadMetadataResult(EPDiskMetadataOutcome::NO_METADATA, guid); },
            [&](NMeta::TError&) { return new TEvReadMetadataResult(EPDiskMetadataOutcome::ERROR, guid); },
            [&](TRcBuf& buffer) { return new TEvReadMetadataResult(TRcBuf(buffer), guid); },
        }, Meta.StoredMetadata);
        ActorSystem->Send(front->Sender, response);
        Meta.Requests.pop_front();
    }

    void TPDisk::ProcessWriteMetadata(std::unique_ptr<TRequestBase> req) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " ProcessWriteMetadata: new request"
            << " Sender# " << req->Sender
            << " ScanInProgress# " << std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata)
            << " Requests.size# " << Meta.Requests.size());
        Meta.Requests.push_back(std::move(req));
        if (std::holds_alternative<NMeta::TScanInProgress>(Meta.StoredMetadata) || Meta.Requests.size() > 1) {
            return; // gonna handle incoming requests in order
        }
        HandleNextWriteMetadata();
    }

    void TPDisk::HandleNextWriteMetadata() {
        Y_ABORT_UNLESS(!Meta.Requests.empty());
        const auto& front = Meta.Requests.front();
        Y_ABORT_UNLESS(front->GetType() == ERequestType::RequestWriteMetadata);
        auto& write = static_cast<TWriteMetadata&>(*front);
        Y_ABORT_UNLESS(!Meta.WriteInFlight);

        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " HandleNextWriteMetadata"
            << " Metadata.size# " << write.Metadata.size());

        std::visit(TOverloaded{
            [](std::monostate&) { Y_ABORT("incorrect case"); },
            [&](NMeta::TFormatted& formatted) {
                // calculate number of slots required to store provided meta
                const ui64 metadataSize = write.Metadata.size();
                const ui32 slotSize = Format.ChunkSize / (2 * Format.SectorSize) * Format.SectorSize - sizeof(TMetadataHeader);
                const ui32 numSlotsRequired = Max<ui32>(1, (metadataSize + slotSize - 1) / slotSize);

                // find free slots to store metadata
                std::vector<NMeta::TSlotKey> freeSlotKeys;
                for (const auto& [slotKey, state] : formatted.Slots) {
                    if (state == NMeta::ESlotState::FREE) {
                        freeSlotKeys.push_back(slotKey);
                        if (freeSlotKeys.size() == numSlotsRequired) {
                            break;
                        }
                    }
                }

                // check we have them enough
                if (freeSlotKeys.size() < numSlotsRequired) {
                    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDisk# " << PDiskId
                        << " ProcessWriteMetadata (formatted): not enough free slots"
                        << " Required# " << numSlotsRequired
                        << " Available# " << freeSlotKeys.size());
                    ActorSystem->Send(write.Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, Format.Guid));
                    Meta.Requests.pop_front();
                    return;
                }

                // generate write queue
                auto completion = std::make_unique<TCompletionWriteMetadata>(this, write.Sender);
                size_t offset = 0;
                for (ui32 i = 0; i < numSlotsRequired; ++i, offset += slotSize) {
                    const NMeta::TSlotKey key = freeSlotKeys[i];

                    const size_t payloadSize = Min<size_t>(slotSize, metadataSize - offset);
                    TRcBuf payload = CreateMetadataPayload(write.Metadata, offset, payloadSize, Format.SectorSize,
                        Cfg->EnableSectorEncryption, Format.ChunkKey, Meta.NextSequenceNumber, i, numSlotsRequired);

                    completion->AddQuery(key, std::move(payload));
                    completion->CostNs += DriveModel.TimeForSizeNs(payload.size(), key.ChunkIdx, TDriveModel::OP_TYPE_WRITE);

                    const auto it = formatted.Slots.find(key);
                    Y_ABORT_UNLESS(it != formatted.Slots.end());
                    Y_ABORT_UNLESS(it->second == NMeta::ESlotState::FREE);
                    it->second = NMeta::ESlotState::BEING_WRITTEN;
                }

                completion.release()->IssueQuery(ActorSystem);
            },
            [&](NMeta::TUnformatted& unformatted) {
                TMetadataFormatSector& fmt = unformatted.FormatInFlight;
                memset(&fmt, 0, sizeof(fmt));

                if (unformatted.Format) {
                    fmt.DataKey = unformatted.Format->DataKey;
                } else {
                    fmt.DataKey = RandomNumber<TKey>();
                }

                TRcBuf payload = CreateMetadataPayload(write.Metadata, 0, write.Metadata.size(), DefaultSectorSize,
                    true, fmt.DataKey, 0, 0, 1);
                const size_t bytesToWrite = payload.size();

                const ui64 deviceSizeInBytes = DriveData.Size & ~ui64(DefaultSectorSize - 1);
                auto& cur = unformatted.Format;
                const ui64 endOffset = !cur || cur->Offset + cur->Length + bytesToWrite <= deviceSizeInBytes
                    ? deviceSizeInBytes
                    : cur->Offset;

                if (endOffset < bytesToWrite + FormatSectorSize * ReplicationFactor) { // way too large metadata
                    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDisk# " << PDiskId
                        << " ProcessWriteMetadata (unformatted): not enough free space"
                        << " EndOffset# " << endOffset
                        << " RawDeviceSize# " << DriveData.Size
                        << " DeviceSizeInBytes# " << deviceSizeInBytes
                        << " BytesToWrite# " << bytesToWrite);
                    ActorSystem->Send(write.Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
                    Meta.Requests.pop_front();
                    return;
                }

                fmt.Offset = endOffset - bytesToWrite;
                fmt.Length = bytesToWrite;
                fmt.SequenceNumber = Meta.NextSequenceNumber;

                auto *completion = new TCompletionWriteUnformattedMetadata(this, write.Sender, fmt, std::move(payload),
                    write.MainKey);
                completion->IssueQuery();
            },
        }, Meta.State);

        ++Meta.NextSequenceNumber;
        Meta.WriteInFlight = true;
    }

    void TPDisk::ProcessWriteMetadataResult(TWriteMetadataResult& request) {
        Y_ABORT_UNLESS(Meta.WriteInFlight);
        Meta.WriteInFlight = false;

        Y_ABORT_UNLESS(!Meta.Requests.empty());
        auto& front = Meta.Requests.front();
        Y_ABORT_UNLESS(front->GetType() == ERequestType::RequestWriteMetadata);
        auto& write = static_cast<TWriteMetadata&>(*front);

        std::optional<ui64> guid;

        std::visit(TOverloaded{
            [](std::monostate&) { Y_ABORT_UNLESS("incorrect case"); },
            [&](NMeta::TFormatted& formatted) {
                for (auto& [_, state] : formatted.Slots) {
                    if (request.Success) {
                        if (state == NMeta::ESlotState::BEING_WRITTEN) {
                            state = NMeta::ESlotState::OCCUPIED;
                        } else if (state == NMeta::ESlotState::OCCUPIED) {
                            state = NMeta::ESlotState::FREE;
                        }
                    } else {
                        if (state == NMeta::ESlotState::BEING_WRITTEN) {
                            state = NMeta::ESlotState::FREE;
                        }
                    }
                }
                guid = Format.Guid;
            },
            [&](NMeta::TUnformatted& unformatted) {
                if (request.Success) {
                    unformatted.Format.emplace(unformatted.FormatInFlight);
                }
            },
        }, Meta.State);

        if (request.Success) { // update persisted metadata cache in memory
            Meta.StoredMetadata.emplace<TRcBuf>(std::move(write.Metadata));
        }

        ActorSystem->Send(write.Sender, new TEvWriteMetadataResult(request.Success ? EPDiskMetadataOutcome::OK :
            EPDiskMetadataOutcome::ERROR, guid));

        Meta.Requests.pop_front();

        ProcessMetadataRequestQueue();
    }

    void TPDisk::DropAllMetadataRequests() {
        for (auto& item : std::exchange(Meta.Requests, {})) {
            TRequestBase::AbortDelete(item.release(), ActorSystem);
        }
    }

    TRcBuf TPDisk::CreateMetadataPayload(TRcBuf& metadata, size_t offset, size_t payloadSize, ui32 sectorSize,
            bool encryption, const TKey& key, ui64 sequenceNumber, ui32 recordIndex, ui32 totalRecords) {
        Y_ABORT_UNLESS(offset + payloadSize <= metadata.size());

        Y_DEBUG_ABORT_UNLESS(IsPowerOf2(sectorSize));
        const size_t dataSize = sizeof(TMetadataHeader) + payloadSize;
        const size_t bytesToWrite = (dataSize + sectorSize - 1) & ~size_t(sectorSize - 1);

        TPDiskStreamCypher cypher(encryption);
        cypher.SetKey(key);

        auto buffer = TRcBuf::UninitializedPageAligned(bytesToWrite);

        Y_ABORT_UNLESS(recordIndex <= Max<ui16>());
        Y_ABORT_UNLESS(totalRecords <= Max<ui16>());
        Y_ABORT_UNLESS(payloadSize <= Max<ui32>());

        auto *header = reinterpret_cast<TMetadataHeader*>(buffer.GetDataMut());
        void *data = header + 1;
        memcpy(data, metadata.data() + offset, payloadSize);
        TPDiskHashCalculator dataHasher;
        dataHasher.Hash(data, payloadSize);

        *header = {
            .Nonce = RandomNumber<ui64>(),
            .SequenceNumber = sequenceNumber,
            .RecordIndex = static_cast<ui16>(recordIndex),
            .TotalRecords = static_cast<ui16>(totalRecords),
            .Length = static_cast<ui32>(payloadSize),
            .DataHash = dataHasher.GetHashResult(),
        };

        header->SetHash();
        header->EncryptData(cypher);
        header->Encrypt(cypher);

        return buffer;
    }

    bool TPDisk::WriteMetadataSync(TRcBuf&& metadata, const TDiskFormat& format) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " WriteMetadataSync: transferring metadata"
            << " Metadata.size# " << metadata.size());

        // calculate number of slots required to store provided meta
        const ui64 metadataSize = metadata.size();
        const ui32 half = format.ChunkSize / (2 * format.SectorSize);
        const ui32 slotSize = half * format.SectorSize - sizeof(TMetadataHeader);
        const ui32 numSlotsRequired = Max<ui32>(1, (metadataSize + slotSize - 1) / slotSize);

        // find free slots to store metadata
        std::vector<NMeta::TSlotKey> freeSlotKeys;
        for (ui32 chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
            if (ChunkState[chunkIdx].OwnerId == OwnerMetadata) {
                freeSlotKeys.emplace_back(chunkIdx, 0);
                if (freeSlotKeys.size() < numSlotsRequired) {
                    freeSlotKeys.emplace_back(chunkIdx, half);
                }
                if (freeSlotKeys.size() == numSlotsRequired) {
                    break;
                }
            }
        }

        // check we have them enough
        if (freeSlotKeys.size() < numSlotsRequired) {
            return false;
        }

        // generate write queue
        size_t offset = 0;
        for (ui32 i = 0; i < numSlotsRequired; ++i, offset += slotSize) {
            const NMeta::TSlotKey key = freeSlotKeys[i];
            const size_t payloadSize = Min<size_t>(slotSize, metadataSize - offset);
            TRcBuf payload = CreateMetadataPayload(metadata, offset, payloadSize, format.SectorSize,
                Cfg->EnableSectorEncryption, format.ChunkKey, 1, i, numSlotsRequired);
            BlockDevice->PwriteSync(payload.data(), payload.size(), format.Offset(key.ChunkIdx, key.OffsetInSectors), {}, nullptr);
        }

        return true;
    }

    std::optional<TMetadataFormatSector> TPDisk::CheckMetadataFormatSector(const ui8 *data, size_t len, const TMainKey& mainKey) {
        if (len != FormatSectorSize * ReplicationFactor) {
            Y_DEBUG_ABORT("unexpected metadata format sector size");
            return {}; // definitely not correct
        }

        constexpr ui32 usefulDataSize = FormatSectorSize - sizeof(TDataSectorFooter);
        static_assert(sizeof(TMetadataFormatSector) <= usefulDataSize);

        TPDiskStreamCypher cypher(true);
        auto decrypted = TRcBuf::Uninitialized(usefulDataSize);
        auto& decryptedSector = *reinterpret_cast<TMetadataFormatSector*>(decrypted.GetDataMut());

        std::optional<TMetadataFormatSector> winner;

        for (ui32 i = 0; i < ReplicationFactor; ++i, data += FormatSectorSize) {
            auto& footer = *reinterpret_cast<const TDataSectorFooter*>(data + usefulDataSize);
            for (const auto& key : mainKey.Keys) {
                cypher.SetKey(key);
                cypher.StartMessage(footer.Nonce);
                cypher.Encrypt(decrypted.GetDataMut(), data, decrypted.size());
                TPDiskHashCalculator hasher;
                hasher.Hash(decrypted.data(), decrypted.size());
                if (hasher.GetHashResult() == footer.Hash && decryptedSector.Magic == MagicMetadataFormatSector) {
                    if (!winner || winner->SequenceNumber < decryptedSector.SequenceNumber) {
                        winner.emplace(decryptedSector);
                    }
                }
            }
        }

        return winner;
    }

    void TPDisk::MakeMetadataFormatSector(ui8 *data, const TMainKey& mainKey, const TMetadataFormatSector& format) {
        TReallyFastRng32 rng(RandomNumber<ui64>());
        for (ui32 *p = reinterpret_cast<ui32*>(data); static_cast<void*>(p) < data + FormatSectorSize; ++p) {
            *p = rng();
        }

        TMetadataFormatSector *p = reinterpret_cast<TMetadataFormatSector*>(data);
        *p = format;
        p->Magic = MagicMetadataFormatSector;

        TPDiskHashCalculator hasher;
        constexpr ui32 usefulDataSize = FormatSectorSize - sizeof(TDataSectorFooter);
        hasher.Hash(data, usefulDataSize);

        auto& footer = *reinterpret_cast<TDataSectorFooter*>(data + usefulDataSize);
        memset(&footer, 0, sizeof(footer));
        footer.Nonce = RandomNumber<ui64>();
        footer.Version = PDISK_DATA_VERSION;
        footer.Hash = hasher.GetHashResult();

        TPDiskStreamCypher cypher(true);
        cypher.SetKey(mainKey.Keys.back());
        cypher.StartMessage(footer.Nonce);
        cypher.InplaceEncrypt(data, usefulDataSize);
    }

    NMeta::TFormatted& TPDisk::GetFormattedMeta() {
        Y_ABORT_UNLESS(std::holds_alternative<NMeta::TFormatted>(Meta.State));
        return std::get<NMeta::TFormatted>(Meta.State);
    }

} // NKikimr::NPDisk
