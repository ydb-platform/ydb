#pragma once

#include <util/generic/noncopyable.h>

template <class T, class C>
struct TAvlTreeItem;

template <class T, class C>
class TAvlTree: public TNonCopyable {
    using TTreeItem = TAvlTreeItem<T, C>;
    friend struct TAvlTreeItem<T, C>;

    static inline const T* AsT(const TTreeItem* item) noexcept {
        return (const T*)item;
    }

    static inline T* AsT(TTreeItem* item) noexcept {
        return (T*)item;
    }

    template <class TTreeItem, class TValue>
    class TIteratorBase {
    public:
        inline TIteratorBase(TTreeItem* p, const TAvlTree* t) noexcept
            : Ptr_(p)
            , Tree_(t)
        {
        }

        inline bool IsEnd() const noexcept {
            return Ptr_ == nullptr;
        }

        inline bool IsBegin() const noexcept {
            return Ptr_ == nullptr;
        }

        inline bool IsFirst() const noexcept {
            return Ptr_ && Ptr_ == Tree_->Head_;
        }

        inline bool IsLast() const noexcept {
            return Ptr_ && Ptr_ == Tree_->Tail_;
        }

        inline TValue& operator*() const noexcept {
            return *AsT(Ptr_);
        }

        inline TValue* operator->() const noexcept {
            return AsT(Ptr_);
        }

        inline TTreeItem* Inc() noexcept {
            return Ptr_ = FindNext(Ptr_);
        }

        inline TTreeItem* Dec() noexcept {
            return Ptr_ = FindPrev(Ptr_);
        }

        inline TIteratorBase& operator++() noexcept {
            Inc();
            return *this;
        }

        inline TIteratorBase operator++(int) noexcept {
            TIteratorBase ret(*this);
            Inc();
            return ret;
        }

        inline TIteratorBase& operator--() noexcept {
            Dec();
            return *this;
        }

        inline TIteratorBase operator--(int) noexcept {
            TIteratorBase ret(*this);
            Dec();
            return ret;
        }

        inline TIteratorBase Next() const noexcept {
            return ConstructNext(*this);
        }

        inline TIteratorBase Prev() const noexcept {
            return ConstructPrev(*this);
        }

        inline bool operator==(const TIteratorBase& r) const noexcept {
            return Ptr_ == r.Ptr_;
        }

        inline bool operator!=(const TIteratorBase& r) const noexcept {
            return Ptr_ != r.Ptr_;
        }

    private:
        inline static TIteratorBase ConstructNext(const TIteratorBase& i) noexcept {
            return TIterator(FindNext(i.Ptr_), i.Tree_);
        }

        inline static TIteratorBase ConstructPrev(const TIteratorBase& i) noexcept {
            return TIterator(FindPrev(i.Ptr_), i.Tree_);
        }

        inline static TIteratorBase FindPrev(TTreeItem* el) noexcept {
            if (el->Left_ != nullptr) {
                el = el->Left_;

                while (el->Right_ != nullptr) {
                    el = el->Right_;
                }
            } else {
                while (true) {
                    TTreeItem* last = el;
                    el = el->Parent_;

                    if (el == nullptr || el->Right_ == last) {
                        break;
                    }
                }
            }

            return el;
        }

        static TTreeItem* FindNext(TTreeItem* el) {
            if (el->Right_ != nullptr) {
                el = el->Right_;

                while (el->Left_) {
                    el = el->Left_;
                }
            } else {
                while (true) {
                    TTreeItem* last = el;
                    el = el->Parent_;

                    if (el == nullptr || el->Left_ == last) {
                        break;
                    }
                }
            }

            return el;
        }

    private:
        TTreeItem* Ptr_;
        const TAvlTree* Tree_;
    };

    using TConstIterator = TIteratorBase<const TTreeItem, const T>;
    using TIterator = TIteratorBase<TTreeItem, T>;

    static inline TConstIterator ConstructFirstConst(const TAvlTree* t) noexcept {
        return TConstIterator(t->Head_, t);
    }

    static inline TIterator ConstructFirst(const TAvlTree* t) noexcept {
        return TIterator(t->Head_, t);
    }

    static inline TConstIterator ConstructLastConst(const TAvlTree* t) noexcept {
        return TConstIterator(t->Tail_, t);
    }

    static inline TIterator ConstructLast(const TAvlTree* t) noexcept {
        return TIterator(t->Tail_, t);
    }

    static inline bool Compare(const TTreeItem& l, const TTreeItem& r) {
        return C::Compare(*AsT(&l), *AsT(&r));
    }

public:
    using const_iterator = TConstIterator;
    using iterator = TIterator;

    inline TAvlTree() noexcept
        : Root_(nullptr)
        , Head_(nullptr)
        , Tail_(nullptr)
    {
    }

    inline ~TAvlTree() noexcept {
        Clear();
    }

    inline void Clear() noexcept {
        for (iterator it = Begin(); it != End();) {
            (it++)->TTreeItem::Unlink();
        }
    }

    inline T* Insert(TTreeItem* el, TTreeItem** lastFound = nullptr) noexcept {
        el->Unlink();
        el->Tree_ = this;

        TTreeItem* curEl = Root_;
        TTreeItem* parentEl = nullptr;
        TTreeItem* lastLess = nullptr;

        while (true) {
            if (curEl == nullptr) {
                AttachRebal(el, parentEl, lastLess);

                if (lastFound != nullptr) {
                    *lastFound = el;
                }

                return AsT(el);
            }

            if (Compare(*el, *curEl)) {
                parentEl = lastLess = curEl;
                curEl = curEl->Left_;
            } else if (Compare(*curEl, *el)) {
                parentEl = curEl;
                curEl = curEl->Right_;
            } else {
                if (lastFound != nullptr) {
                    *lastFound = curEl;
                }

                return nullptr;
            }
        }
    }

    inline T* Find(const TTreeItem* el) const noexcept {
        TTreeItem* curEl = Root_;

        while (curEl) {
            if (Compare(*el, *curEl)) {
                curEl = curEl->Left_;
            } else if (Compare(*curEl, *el)) {
                curEl = curEl->Right_;
            } else {
                return AsT(curEl);
            }
        }

        return nullptr;
    }

    inline T* LowerBound(const TTreeItem* el) const noexcept {
        TTreeItem* curEl = Root_;
        TTreeItem* lowerBound = nullptr;

        while (curEl) {
            if (Compare(*el, *curEl)) {
                lowerBound = curEl;
                curEl = curEl->Left_;
            } else if (Compare(*curEl, *el)) {
                curEl = curEl->Right_;
            } else {
                return AsT(curEl);
            }
        }

        return AsT(lowerBound);
    }

    inline T* Erase(TTreeItem* el) noexcept {
        if (el->Tree_ == this) {
            return this->EraseImpl(el);
        }

        return nullptr;
    }

    inline T* EraseImpl(TTreeItem* el) noexcept {
        el->Tree_ = nullptr;

        TTreeItem* replacement;
        TTreeItem* fixfrom;
        long lheight, rheight;

        if (el->Right_) {
            replacement = el->Right_;

            while (replacement->Left_) {
                replacement = replacement->Left_;
            }

            if (replacement->Parent_ == el) {
                fixfrom = replacement;
            } else {
                fixfrom = replacement->Parent_;
            }

            if (el == Head_) {
                Head_ = replacement;
            }

            RemoveEl(replacement, replacement->Right_);
            ReplaceEl(el, replacement);
        } else if (el->Left_) {
            replacement = el->Left_;

            while (replacement->Right_) {
                replacement = replacement->Right_;
            }

            if (replacement->Parent_ == el) {
                fixfrom = replacement;
            } else {
                fixfrom = replacement->Parent_;
            }

            if (el == Tail_) {
                Tail_ = replacement;
            }

            RemoveEl(replacement, replacement->Left_);
            ReplaceEl(el, replacement);
        } else {
            fixfrom = el->Parent_;

            if (el == Head_) {
                Head_ = el->Parent_;
            }

            if (el == Tail_) {
                Tail_ = el->Parent_;
            }

            RemoveEl(el, nullptr);
        }

        if (fixfrom == nullptr) {
            return AsT(el);
        }

        RecalcHeights(fixfrom);

        TTreeItem* ub = FindFirstUnbalEl(fixfrom);

        while (ub) {
            lheight = ub->Left_ ? ub->Left_->Height_ : 0;
            rheight = ub->Right_ ? ub->Right_->Height_ : 0;

            if (rheight > lheight) {
                ub = ub->Right_;
                lheight = ub->Left_ ? ub->Left_->Height_ : 0;
                rheight = ub->Right_ ? ub->Right_->Height_ : 0;

                if (rheight > lheight) {
                    ub = ub->Right_;
                } else if (rheight < lheight) {
                    ub = ub->Left_;
                } else {
                    ub = ub->Right_;
                }
            } else {
                ub = ub->Left_;
                lheight = ub->Left_ ? ub->Left_->Height_ : 0;
                rheight = ub->Right_ ? ub->Right_->Height_ : 0;
                if (rheight > lheight) {
                    ub = ub->Right_;
                } else if (rheight < lheight) {
                    ub = ub->Left_;
                } else {
                    ub = ub->Left_;
                }
            }

            fixfrom = Rebalance(ub);
            ub = FindFirstUnbalEl(fixfrom);
        }

        return AsT(el);
    }

    inline const_iterator First() const noexcept {
        return ConstructFirstConst(this);
    }

    inline const_iterator Last() const noexcept {
        return ConstructLastConst(this);
    }

    inline const_iterator Begin() const noexcept {
        return First();
    }

    inline const_iterator End() const noexcept {
        return const_iterator(nullptr, this);
    }

    inline const_iterator begin() const noexcept {
        return Begin();
    }

    inline const_iterator end() const noexcept {
        return End();
    }

    inline const_iterator cbegin() const noexcept {
        return Begin();
    }

    inline const_iterator cend() const noexcept {
        return End();
    }

    inline iterator First() noexcept {
        return ConstructFirst(this);
    }

    inline iterator Last() noexcept {
        return ConstructLast(this);
    }

    inline iterator Begin() noexcept {
        return First();
    }

    inline iterator End() noexcept {
        return iterator(nullptr, this);
    }

    inline iterator begin() noexcept {
        return Begin();
    }

    inline iterator end() noexcept {
        return End();
    }

    inline bool Empty() const noexcept {
        return const_cast<TAvlTree*>(this)->Begin() == const_cast<TAvlTree*>(this)->End();
    }

    inline explicit operator bool() const noexcept {
        return !this->Empty();
    }

    template <class Functor>
    inline void ForEach(Functor& f) {
        iterator it = Begin();

        while (!it.IsEnd()) {
            iterator next = it;
            ++next;
            f(*it);
            it = next;
        }
    }

private:
    inline TTreeItem* Rebalance(TTreeItem* n) noexcept {
        long lheight, rheight;

        TTreeItem* a;
        TTreeItem* b;
        TTreeItem* c;
        TTreeItem* t1;
        TTreeItem* t2;
        TTreeItem* t3;
        TTreeItem* t4;

        TTreeItem* p = n->Parent_;
        TTreeItem* gp = p->Parent_;
        TTreeItem* ggp = gp->Parent_;

        if (gp->Right_ == p) {
            if (p->Right_ == n) {
                a = gp;
                b = p;
                c = n;
                t1 = gp->Left_;
                t2 = p->Left_;
                t3 = n->Left_;
                t4 = n->Right_;
            } else {
                a = gp;
                b = n;
                c = p;
                t1 = gp->Left_;
                t2 = n->Left_;
                t3 = n->Right_;
                t4 = p->Right_;
            }
        } else {
            if (p->Right_ == n) {
                a = p;
                b = n;
                c = gp;
                t1 = p->Left_;
                t2 = n->Left_;
                t3 = n->Right_;
                t4 = gp->Right_;
            } else {
                a = n;
                b = p;
                c = gp;
                t1 = n->Left_;
                t2 = n->Right_;
                t3 = p->Right_;
                t4 = gp->Right_;
            }
        }

        if (ggp == nullptr) {
            Root_ = b;
        } else if (ggp->Left_ == gp) {
            ggp->Left_ = b;
        } else {
            ggp->Right_ = b;
        }

        b->Parent_ = ggp;
        b->Left_ = a;
        a->Parent_ = b;
        b->Right_ = c;
        c->Parent_ = b;
        a->Left_ = t1;

        if (t1 != nullptr) {
            t1->Parent_ = a;
        }

        a->Right_ = t2;

        if (t2 != nullptr) {
            t2->Parent_ = a;
        }

        c->Left_ = t3;

        if (t3 != nullptr) {
            t3->Parent_ = c;
        }

        c->Right_ = t4;

        if (t4 != nullptr) {
            t4->Parent_ = c;
        }

        lheight = a->Left_ ? a->Left_->Height_ : 0;
        rheight = a->Right_ ? a->Right_->Height_ : 0;
        a->Height_ = (lheight > rheight ? lheight : rheight) + 1;

        lheight = c->Left_ ? c->Left_->Height_ : 0;
        rheight = c->Right_ ? c->Right_->Height_ : 0;
        c->Height_ = (lheight > rheight ? lheight : rheight) + 1;

        lheight = a->Height_;
        rheight = c->Height_;
        b->Height_ = (lheight > rheight ? lheight : rheight) + 1;

        RecalcHeights(ggp);

        return ggp;
    }

    inline void RecalcHeights(TTreeItem* el) noexcept {
        long lheight, rheight, new_height;

        while (el) {
            lheight = el->Left_ ? el->Left_->Height_ : 0;
            rheight = el->Right_ ? el->Right_->Height_ : 0;

            new_height = (lheight > rheight ? lheight : rheight) + 1;

            if (new_height == el->Height_) {
                return;
            } else {
                el->Height_ = new_height;
            }

            el = el->Parent_;
        }
    }

    inline TTreeItem* FindFirstUnbalGP(TTreeItem* el) noexcept {
        long lheight, rheight, balanceProp;
        TTreeItem* gp;

        if (el == nullptr || el->Parent_ == nullptr || el->Parent_->Parent_ == nullptr) {
            return nullptr;
        }

        gp = el->Parent_->Parent_;

        while (gp != nullptr) {
            lheight = gp->Left_ ? gp->Left_->Height_ : 0;
            rheight = gp->Right_ ? gp->Right_->Height_ : 0;
            balanceProp = lheight - rheight;

            if (balanceProp < -1 || balanceProp > 1) {
                return el;
            }

            el = el->Parent_;
            gp = gp->Parent_;
        }

        return nullptr;
    }

    inline TTreeItem* FindFirstUnbalEl(TTreeItem* el) noexcept {
        if (el == nullptr) {
            return nullptr;
        }

        while (el) {
            const long lheight = el->Left_ ? el->Left_->Height_ : 0;
            const long rheight = el->Right_ ? el->Right_->Height_ : 0;
            const long balanceProp = lheight - rheight;

            if (balanceProp < -1 || balanceProp > 1) {
                return el;
            }

            el = el->Parent_;
        }

        return nullptr;
    }

    inline void ReplaceEl(TTreeItem* el, TTreeItem* replacement) noexcept {
        TTreeItem* parent = el->Parent_;
        TTreeItem* left = el->Left_;
        TTreeItem* right = el->Right_;

        replacement->Left_ = left;

        if (left) {
            left->Parent_ = replacement;
        }

        replacement->Right_ = right;

        if (right) {
            right->Parent_ = replacement;
        }

        replacement->Parent_ = parent;

        if (parent) {
            if (parent->Left_ == el) {
                parent->Left_ = replacement;
            } else {
                parent->Right_ = replacement;
            }
        } else {
            Root_ = replacement;
        }

        replacement->Height_ = el->Height_;
    }

    inline void RemoveEl(TTreeItem* el, TTreeItem* filler) noexcept {
        TTreeItem* parent = el->Parent_;

        if (parent) {
            if (parent->Left_ == el) {
                parent->Left_ = filler;
            } else {
                parent->Right_ = filler;
            }
        } else {
            Root_ = filler;
        }

        if (filler) {
            filler->Parent_ = parent;
        }

        return;
    }

    inline void AttachRebal(TTreeItem* el, TTreeItem* parentEl, TTreeItem* lastLess) {
        el->Parent_ = parentEl;
        el->Left_ = nullptr;
        el->Right_ = nullptr;
        el->Height_ = 1;

        if (parentEl != nullptr) {
            if (lastLess == parentEl) {
                parentEl->Left_ = el;
            } else {
                parentEl->Right_ = el;
            }

            if (Head_->Left_ == el) {
                Head_ = el;
            }

            if (Tail_->Right_ == el) {
                Tail_ = el;
            }
        } else {
            Root_ = el;
            Head_ = Tail_ = el;
        }

        RecalcHeights(parentEl);

        TTreeItem* ub = FindFirstUnbalGP(el);

        if (ub != nullptr) {
            Rebalance(ub);
        }
    }

private:
    TTreeItem* Root_;
    TTreeItem* Head_;
    TTreeItem* Tail_;
};

template <class T, class C>
struct TAvlTreeItem: public TNonCopyable {
public:
    using TTree = TAvlTree<T, C>;
    friend class TAvlTree<T, C>;
    friend typename TAvlTree<T, C>::TConstIterator;
    friend typename TAvlTree<T, C>::TIterator;

    inline TAvlTreeItem() noexcept
        : Left_(nullptr)
        , Right_(nullptr)
        , Parent_(nullptr)
        , Height_(0)
        , Tree_(nullptr)
    {
    }

    inline ~TAvlTreeItem() noexcept {
        Unlink();
    }

    inline void Unlink() noexcept {
        if (Tree_) {
            Tree_->EraseImpl(this);
        }
    }

private:
    TAvlTreeItem* Left_;
    TAvlTreeItem* Right_;
    TAvlTreeItem* Parent_;
    long Height_;
    TTree* Tree_;
};
