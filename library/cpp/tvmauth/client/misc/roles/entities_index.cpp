#include "entities_index.h" 
 
#include <util/stream/str.h> 
 
#include <set> 
 
namespace NTvmAuth::NRoles { 
    TEntitiesIndex::TStage::TStage(const std::set<TString>& k) 
        : Keys_(k.begin(), k.end()) 
    { 
    } 
 
    // TODO TStringBuf 
    bool TEntitiesIndex::TStage::GetNextKeySet(std::vector<TString>& out) { 
        out.clear(); 
        out.reserve(Keys_.size()); 
 
        ++Id_; 
        for (size_t idx = 0; idx < Keys_.size(); ++idx) { 
            bool need = (Id_ >> idx) & 0x01; 
 
            if (need) { 
                out.push_back(Keys_[idx]); 
            } 
        } 
 
        return !out.empty(); 
    } 
 
    TEntitiesIndex::TEntitiesIndex(const std::vector<TEntityPtr>& entities) { 
        const std::set<TString> uniqueKeys = GetUniqueSortedKeys(entities); 
        Idx_.Entities = entities; 
        Idx_.SubTree.reserve(uniqueKeys.size() * entities.size()); 
 
        TStage stage(uniqueKeys); 
        std::vector<TString> keyset; 
        while (stage.GetNextKeySet(keyset)) { 
            for (const TEntityPtr& e : entities) { 
                TSubTree* currentBranch = &Idx_; 
 
                for (const TString& key : keyset) { 
                    auto it = e->find(key); 
                    if (it == e->end()) { 
                        continue; 
                    } 
 
                    auto [i, ok] = currentBranch->SubTree.emplace( 
                        TKeyValue{it->first, it->second}, 
                        TSubTree()); 
 
                    currentBranch = &i->second; 
                    currentBranch->Entities.push_back(e); 
                } 
            } 
        } 
 
        MakeUnique(Idx_); 
    } 
 
    std::set<TString> TEntitiesIndex::GetUniqueSortedKeys(const std::vector<TEntityPtr>& entities) { 
        std::set<TString> res; 
 
        for (const TEntityPtr& e : entities) { 
            for (const auto& [key, value] : *e) { 
                res.insert(key); 
            } 
        } 
 
        return res; 
    } 
 
    void TEntitiesIndex::MakeUnique(TSubTree& branch) { 
        auto& vec = branch.Entities; 
        std::sort(vec.begin(), vec.end()); 
        vec.erase(std::unique(vec.begin(), vec.end()), vec.end()); 
 
        for (auto& [_, restPart] : branch.SubTree) { 
            MakeUnique(restPart); 
        } 
    } 
 
    static void Print(const TEntitiesIndex::TSubTree& part, IOutputStream& out, size_t offset = 0) { 
        std::vector<std::pair<TKeyValue, const TEntitiesIndex::TSubTree*>> vec; 
        vec.reserve(part.SubTree.size()); 
 
        for (const auto& [key, value] : part.SubTree) { 
            vec.push_back({key, &value}); 
        } 
 
        std::sort(vec.begin(), vec.end(), [](const auto& l, const auto& r) { 
            if (l.first.Key < r.first.Key) { 
                return true; 
            } 
            if (l.first.Value < r.first.Value) { 
                return true; 
            } 
            return false; 
        }); 
 
        for (const auto& [key, value] : vec) { 
            out << TString(offset, ' ') << "\"" << key.Key << "/" << key.Value << "\"" << Endl; 
            Print(*value, out, offset + 4); 
        } 
    } 
 
    TString TEntitiesIndex::PrintDebugString() const { 
        TStringStream res; 
        res << Endl; 
 
        Print(Idx_, res); 
 
        return res.Str(); 
    } 
} 
