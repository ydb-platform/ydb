#pragma once 
#include "defs.h" 
 
#include <util/generic/hash.h> 
#include <util/generic/ptr.h> 
 
#include <unordered_map> 
#include <unordered_set> 
 
namespace NKikimr { 
namespace NTable { 
 
    /** 
     * A simple copy-on-write data structure for a TxId -> TValue map 
     */ 
    template<class TValue> 
    class TTransactionMap { 
    private: 
        using TTxMap = std::unordered_map<ui64, TValue>; 
 
        struct TState : public TThrRefBase, public TTxMap { 
        }; 
 
    public: 
        using const_iterator = typename TState::const_iterator; 
 
    public: 
        TTransactionMap() = default; 
 
        explicit operator bool() const { 
            return State_ && !State_->empty(); 
        } 
 
        bool Contains(ui64 txId) const { 
            return State_ && State_->contains(txId); 
        } 
 
        const TValue* Find(ui64 txId) const { 
            if (State_) { 
                auto it = State_->find(txId); 
                if (it != State_->end()) { 
                    return &it->second; 
                } 
            } 
            return nullptr; 
        } 
 
        void Clear() { 
            State_.Reset(); 
        } 
 
        void Add(ui64 txId, const TValue& value) { 
            Unshare()[txId] = value; 
        } 
 
        void Remove(ui64 txId) { 
            if (State_ && State_->contains(txId)) { 
                Unshare().erase(txId); 
            } 
        } 
 
    public: 
        const_iterator begin() const { 
            if (State_) { 
                const TState& state = *State_; 
                return state.begin(); 
            } else { 
                return { }; 
            } 
        } 
 
        const_iterator end() const { 
            if (State_) { 
                const TState& state = *State_; 
                return state.end(); 
            } else { 
                return { }; 
            } 
        } 
 
    private: 
        TState& Unshare() { 
            if (!State_) { 
                State_ = MakeIntrusive<TState>(); 
            } else if (State_->RefCount() > 1) { 
                State_ = MakeIntrusive<TState>(*State_); 
            } 
            return *State_; 
        } 
 
    private: 
        TIntrusivePtr<TState> State_; 
    }; 
 
    /** 
     * A simple copy-on-write data structure for a TxId set 
     */ 
    class TTransactionSet { 
    private: 
        using TTxSet = std::unordered_set<ui64>; 
 
        struct TState : public TThrRefBase, TTxSet { 
        }; 
 
    public: 
        using const_iterator = TState::const_iterator; 
 
    public: 
        TTransactionSet() = default; 
 
        explicit operator bool() const { 
            return State_ && !State_->empty(); 
        } 
 
        bool Contains(ui64 txId) const { 
            return State_ && State_->contains(txId); 
        } 
 
        void Clear() { 
            State_.Reset(); 
        } 
 
        void Add(ui64 txId) { 
            Unshare().insert(txId); 
        } 
 
        void Remove(ui64 txId) { 
            if (State_ && State_->contains(txId)) { 
                Unshare().erase(txId); 
            } 
        } 
 
    public: 
        const_iterator begin() const { 
            if (State_) { 
                const TState& state = *State_; 
                return state.begin(); 
            } else { 
                return { }; 
            } 
        } 
 
        const_iterator end() const { 
            if (State_) { 
                const TState& state = *State_; 
                return state.end(); 
            } else { 
                return { }; 
            } 
        } 
 
    private: 
        TState& Unshare() { 
            if (!State_) { 
                State_ = MakeIntrusive<TState>(); 
            } else if (State_->RefCount() > 1) { 
                State_ = MakeIntrusive<TState>(*State_); 
            } 
            return *State_; 
        } 
 
    private: 
        TIntrusivePtr<TState> State_; 
    }; 
 
} // namespace NTable 
} // namespace NKikimr 
