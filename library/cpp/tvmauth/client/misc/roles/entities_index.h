#pragma once

#include "types.h"

#include <library/cpp/tvmauth/client/exception.h>

#include <set>
#include <vector>

namespace NTvmAuth::NRoles {
    class TEntitiesIndex: TMoveOnly {
    public:
        struct TSubTree;
        using TIdxByAttrs = THashMap<TKeyValue, TSubTree>;

        struct TSubTree {
            std::vector<TEntityPtr> Entities;
            TIdxByAttrs SubTree;
        };

        class TStage {
        public:
            TStage(const std::set<TString>& k);

            bool GetNextKeySet(std::vector<TString>& out);

        private:
            std::vector<TString> Keys_;
            size_t Id_ = 0;
        };

    public:
        TEntitiesIndex(const std::vector<TEntityPtr>& entities);

        /**
         * Iterators must be to sorted unique key/value
         */
        template <typename Iterator>
        bool ContainsExactEntity(Iterator begin, Iterator end) const;

        /**
         * Iterators must be to sorted unique key/value
         */
        template <typename Iterator>
        const std::vector<TEntityPtr>& GetEntitiesWithAttrs(Iterator begin, Iterator end) const;

    public: // for tests
        static std::set<TString> GetUniqueSortedKeys(const std::vector<TEntityPtr>& entities);
        static void MakeUnique(TEntitiesIndex::TSubTree& branch);

        TString PrintDebugString() const;

    private:
        template <typename Iterator>
        const TSubTree* FindSubtree(Iterator begin, Iterator end, size_t& size) const;

    private:
        TSubTree Idx_;
        std::vector<TEntityPtr> EmptyResult_;
    };

    template <typename Iterator>
    bool TEntitiesIndex::ContainsExactEntity(Iterator begin, Iterator end) const {
        size_t size = 0;
        const TSubTree* subtree = FindSubtree(begin, end, size);
        if (!subtree) {
            return false;
        }

        auto res = std::find_if(
            subtree->Entities.begin(),
            subtree->Entities.end(),
            [size](const auto& e) { return size == e->size(); });
        return res != subtree->Entities.end();
    }

    template <typename Iterator>
    const std::vector<TEntityPtr>& TEntitiesIndex::GetEntitiesWithAttrs(Iterator begin, Iterator end) const {
        size_t size = 0;
        const TSubTree* subtree = FindSubtree(begin, end, size);
        if (!subtree) {
            return EmptyResult_;
        }

        return subtree->Entities;
    }

    template <typename Iterator>
    const TEntitiesIndex::TSubTree* TEntitiesIndex::FindSubtree(Iterator begin,
                                                                Iterator end,
                                                                size_t& size) const {
        const TSubTree* subtree = &Idx_;
        size = 0;

        for (auto attr = begin; attr != end; ++attr) {
            auto it = subtree->SubTree.find(TKeyValueView{attr->first, attr->second});
            if (it == subtree->SubTree.end()) {
                return nullptr;
            }

            ++size;
            subtree = &it->second;
        }

        return subtree;
    }
}
