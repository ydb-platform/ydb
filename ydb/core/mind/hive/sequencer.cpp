
#include "sequencer.h"

namespace NKikimr {
namespace NHive {

TSequencer::TSequence TSequencer::NO_SEQUENCE;

bool TSequenceGenerator::AddFreeSequence(TOwnerType owner, TSequencer::TSequence sequence) {
    Y_ABORT_UNLESS(sequence.Begin != NO_ELEMENT);
    auto [it, inserted] = SequenceByOwner.emplace(owner, sequence);
    if (inserted) {
        if (!sequence.Empty()) {
            FreeSequences.emplace_back(owner);
            FreeSize_ += sequence.Size();
        }
        ++FreeSequencesIndex;
    }
    return inserted;
}

void TSequenceGenerator::AddAllocatedSequence(TOwnerType owner, TSequence sequence) {
    Y_ABORT_UNLESS(owner.first != NO_OWNER);
    Y_ABORT_UNLESS(sequence.Begin != NO_ELEMENT);
    {
        auto [it, inserted] = AllocatedSequences.emplace(sequence, owner);
        Y_ABORT_UNLESS(inserted);
        AllocatedSize_ += sequence.Size();
    }
    {
        auto [it, inserted] = SequenceByOwner.emplace(owner, sequence);
        Y_ABORT_UNLESS(inserted);
    }
}

bool TSequenceGenerator::AddSequence(TOwnerType owner, TSequence sequence) {
    if (owner.first == NO_OWNER) {
        return AddFreeSequence(owner, sequence);
    } else {
        AddAllocatedSequence(owner, sequence);
        return true;
    }
}

TSequencer::TElementType TSequenceGenerator::AllocateElement(std::vector<TOwnerType>& modified) {
    return AllocateSequence({NO_OWNER, NO_ELEMENT}, 1, modified).GetNext();
}

TSequencer::TSequence TSequenceGenerator::AllocateSequence(TSequencer::TOwnerType owner, size_t size, std::vector<TOwnerType>& modified) {
    if (size > 1) {
        Y_ABORT_UNLESS(owner.first != NO_OWNER);
        auto it = SequenceByOwner.find(owner);
        if (it != SequenceByOwner.end()) {
            return it->second;
        }
    }
    if (FreeSequences.empty()) {
        return NO_SEQUENCE;
    }
    TOwnerType freeOwner = FreeSequences.front();
    auto itSequence = SequenceByOwner.find(freeOwner);
    if (itSequence == SequenceByOwner.end()) {
        return NO_SEQUENCE;
    }
    TSequence result = itSequence->second.BiteOff(size);
    if (itSequence->second.Empty()) {
        FreeSequences.pop_front();
    }
    FreeSize_ -= result.Size();
    if (size > 1) {
        AddAllocatedSequence(owner, result);
        modified.emplace_back(owner);
    }
    modified.emplace_back(freeOwner);
    return result;
}

TSequencer::TSequence TSequenceGenerator::GetSequence(TSequencer::TOwnerType owner) {
    auto it = SequenceByOwner.find(owner);
    if (it != SequenceByOwner.end()) {
        return it->second;
    } else {
        return NO_SEQUENCE;
    }
}

TSequencer::TOwnerType TSequenceGenerator::GetOwner(TSequencer::TElementType element) {
    auto it = AllocatedSequences.lower_bound(element);
    if (it != AllocatedSequences.end()) {
        // check for "equal"
        if (it->first.Contains(element)) {
            return it->second;
        }
    }
    if (it != AllocatedSequences.begin()) {
        --it;
        if (it->first.Contains(element)) {
            return it->second;
        }
    }

    return {NO_OWNER, 0};
}

size_t TSequenceGenerator::FreeSize() const {
    return FreeSize_;
}

size_t TSequenceGenerator::AllocatedSequencesSize() const {
    return AllocatedSize_;
}

size_t TSequenceGenerator::AllocatedSequencesCount() const {
    return AllocatedSequences.size();
}

size_t TSequenceGenerator::NextFreeSequenceIndex() const {
    return FreeSequencesIndex;
}

TSequencer::TElementType TSequenceGenerator::GetNextElement() const {
    if (FreeSequences.empty()) {
        return NO_ELEMENT;
    }
    TOwnerType freeOwner = FreeSequences.front();
    auto itSequence = SequenceByOwner.find(freeOwner);
    if (itSequence == SequenceByOwner.end()) {
        return NO_ELEMENT;
    }
    return itSequence->second.GetNext();
}

void TSequenceGenerator::Clear() {
    SequenceByOwner.clear();
    AllocatedSequences.clear();
    FreeSequences.clear();
    FreeSequencesIndex = 0;
}

bool TOwnershipKeeper::AddOwnedSequence(TOwnerType owner, TSequence sequence) {
    Y_ABORT_UNLESS(owner != NO_OWNER);
    Y_ABORT_UNLESS(sequence.Begin != NO_ELEMENT);

    auto [it, inserted] = OwnedSequences.emplace(sequence, owner);
    Y_ABORT_UNLESS(inserted || it->second == owner);

    return inserted;
}

TOwnershipKeeper::TOwnerType TOwnershipKeeper::GetOwner(TSequencer::TElementType element) {
    auto it = OwnedSequences.lower_bound(element);
    if (it != OwnedSequences.end()) {
        // check for "equal"
        if (it->first.Contains(element)) {
            return it->second;
        }
    }
    if (it != OwnedSequences.begin()) {
        --it;
        if (it->first.Contains(element)) {
            return it->second;
        }
    }

    return NO_OWNER;
}

void TOwnershipKeeper::GetOwnedSequences(TOwnerType owner, std::vector<TSequence>& sequences) const {
    sequences.clear();
    for (const auto& [seq, own] : OwnedSequences) {
        if (own == owner) {
            sequences.emplace_back(seq);
        }
    }
}

void TOwnershipKeeper::Clear() {
    OwnedSequences.clear();
}

}
}
