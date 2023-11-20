#pragma once

#include "defs.h"
#include "blobstorage_groupinfo.h"

namespace NKikimr {

    template<typename TBaseIter, typename TParentIter, typename TDerived>
    class TBlobStorageGroupInfo::TIteratorBase
        : public TParentIter
    {
        using TValue = std::remove_reference_t<decltype(*std::declval<TBaseIter>())>;

        TBaseIter It; // iterator to current element

    public:
        TIteratorBase()
        {}

        template<typename... TArgs>
        TIteratorBase(const TBaseIter& it, TArgs&&... args)
            : TParentIter(std::forward<TArgs>(args)...)
            , It(it)
        {}

        TIteratorBase(const TIteratorBase& it)
            : TParentIter(it.GetParentIter())
            , It(it.It)
        {}

        const TValue& operator *() const {
            return *It;
        }

        const TValue *operator ->() const {
            return &*It;
        }

        TDerived& operator ++() {
            // if we have reached end of our range, we have to advance parent iterator and set up new ranges unless
            // it have reached the end too
            if (++It == TParentIter::NestedEnd()) {
                TParentIter::operator ++();
                if (!TParentIter::IsAtEnd()) {
                    It = TParentIter::NestedBegin();
                }
            }

            return static_cast<TDerived&>(*this);
        }

        TDerived& operator --() {
            // check if we are at end; if so, we have nothing to do but to move parent iterator one item back; otherwise
            // see if we are at begin and have to move parent iterator too
            if (IsAtEnd() || It == TParentIter::NestedBegin()) {
                TParentIter::operator --();
                It = TParentIter::NestedEnd();
            }
            --It;
            return static_cast<TDerived&>(*this);
        }

        TDerived operator ++(int) {
            TDerived current(static_cast<TDerived&>(*this));
            ++*this;
            return current;
        }

        TDerived operator --(int) {
            TDerived current(static_cast<TDerived&>(*this));
            --*this;
            return current;
        }

        friend bool operator ==(const TIteratorBase& x, const TIteratorBase& y) {
            return x.It == y.It && static_cast<const TParentIter&>(x) == static_cast<const TParentIter&>(y);
        }

        friend bool operator !=(const TIteratorBase& x, const TIteratorBase& y) {
            return x.It != y.It || static_cast<const TParentIter&>(x) != static_cast<const TParentIter&>(y);
        }

    protected:
        bool IsAtEnd() const {
            return TParentIter::IsAtEnd() || It == TParentIter::NestedEnd();
        }

        ui32 IndexInParent() const {
            return It - TParentIter::NestedBegin();
        }

        const TParentIter& GetParentIter() const {
            return *this;
        }
    };

    class TBlobStorageGroupInfo::TRootIteratorBase {
        const TBlobStorageGroupInfo::TTopology *Topology = nullptr;
        bool AtEnd = false;

    public:
        TRootIteratorBase()
        {}

        TRootIteratorBase(const TBlobStorageGroupInfo::TTopology *topology, bool atEnd)
            : Topology(topology)
            , AtEnd(atEnd)
        {}

        TRootIteratorBase& operator ++() {
            Y_ABORT_UNLESS(!AtEnd);
            AtEnd = true;
            return *this;
        }

        TRootIteratorBase& operator --() {
            Y_ABORT_UNLESS(AtEnd);
            AtEnd = false;
            return *this;
        }

        auto NestedBegin() const {
            Y_ABORT_UNLESS(!AtEnd && Topology);
            return Topology->FailRealms.begin();
        }

        auto NestedEnd() const {
            Y_ABORT_UNLESS(!AtEnd && Topology);
            return Topology->FailRealms.end();
        }

        bool IsAtEnd() const {
            return AtEnd;
        }

        friend bool operator ==(const TRootIteratorBase& x, const TRootIteratorBase& y) {
            Y_DEBUG_ABORT_UNLESS(x.Topology == y.Topology);
            return x.AtEnd == y.AtEnd;
        }

        friend bool operator !=(const TRootIteratorBase& x, const TRootIteratorBase& y) {
            Y_DEBUG_ABORT_UNLESS(x.Topology == y.Topology);
            return x.AtEnd != y.AtEnd;
        }
    };

    class TBlobStorageGroupInfo::TFailRealmIterator
        : public TBlobStorageGroupInfo::TIteratorBase<TVector<TFailRealm>::const_iterator, TRootIteratorBase, TFailRealmIterator>
        , public std::iterator<std::bidirectional_iterator_tag, TFailRealm>
    {
    public:
        using TIteratorBase::TIteratorBase;

        ui32 GetFailRealmIdx() const {
            return TIteratorBase::IndexInParent();
        }

        ui32 GetNumFailDomainsPerFailRealm() const {
            const auto& failRealm = **this;
            return failRealm.FailDomains.size();
        }

        TFailDomainIterator FailRealmFailDomainsBegin() const;
        TFailDomainIterator FailRealmFailDomainsEnd() const;
        TFailDomainRange GetFailRealmFailDomains() const;

        TVDiskIterator FailRealmVDisksBegin() const;
        TVDiskIterator FailRealmVDisksEnd() const;
        TVDiskRange GetFailRealmVDisks() const;

    protected:
        auto NestedBegin() const {
            const TFailRealm& failRealm = **this;
            return failRealm.FailDomains.begin();
        }

        auto NestedEnd() const {
            const TFailRealm& failRealm = **this;
            return failRealm.FailDomains.end();
        }
    };

    class TBlobStorageGroupInfo::TFailDomainIterator
        : public TBlobStorageGroupInfo::TIteratorBase<TVector<TFailDomain>::const_iterator, TFailRealmIterator, TFailDomainIterator>
        , public std::iterator<std::bidirectional_iterator_tag, TFailDomain>
    {
    public:
        using TIteratorBase::TIteratorBase;

        ui32 GetFailDomainIdx() const {
            return TIteratorBase::IndexInParent();
        }

        ui32 GetNumVDisksPerFailDomain() const {
            const TFailDomain& domain = **this;
            return domain.VDisks.size();
        }

        const TBlobStorageGroupInfo::TFailRealmIterator& GetFailRealmIt() const {
            return TIteratorBase::GetParentIter();
        }

        TVDiskIterator FailDomainVDisksBegin() const;
        TVDiskIterator FailDomainVDisksEnd() const;
        TVDiskRange GetFailDomainVDisks() const;

    protected:
        auto NestedBegin() const {
            const TFailDomain& domain = **this;
            return domain.VDisks.begin();
        }

        auto NestedEnd() const {
            const TFailDomain& domain = **this;
            return domain.VDisks.end();
        }
    };

    class TBlobStorageGroupInfo::TVDiskIterator
        : public TBlobStorageGroupInfo::TIteratorBase<TVector<TVDiskInfo>::const_iterator, TFailDomainIterator, TVDiskIterator>
        , public std::iterator<std::bidirectional_iterator_tag, TVDiskInfo>
    {
    public:
        using TIteratorBase::TIteratorBase;

        ui32 GetVDiskIdx() const {
            return TIteratorBase::IndexInParent();
        }

        const TBlobStorageGroupInfo::TFailDomainIterator& GetFailDomainIt() const {
            return TIteratorBase::GetParentIter();
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlobStorageGroupInfo implementation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::FindVDisk(const TVDiskID& vdisk) const {
        Y_ABORT_UNLESS(vdisk.GroupID == GroupID && vdisk.GroupGeneration == GroupGeneration);
        const auto realmIt = Topology->FailRealms.begin() + vdisk.FailRealm;
        const auto domainIt = realmIt->FailDomains.begin() + vdisk.FailDomain;
        const auto vdiskIt = domainIt->VDisks.begin() + vdisk.VDisk;
        return TVDiskIterator(vdiskIt, domainIt, realmIt, Topology.get(), false);
    }

    template<typename TBaseIter>
    class TBlobStorageGroupInfo::TRangeBase {
        const TBaseIter Begin;
        const TBaseIter End;

    public:
        TRangeBase(const TBaseIter& begin, const TBaseIter& end)
            : Begin(begin)
            , End(end)
        {}

        const TBaseIter& begin() const {
            return Begin;
        }

        const TBaseIter& end() const {
            return End;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TFailRealmIterator implementation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::TFailRealmIterator::FailRealmFailDomainsBegin() const {
        return TFailDomainIterator((*this)->FailDomains.begin(), *this);
    }

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::TFailRealmIterator::FailRealmFailDomainsEnd() const {
        return ++TFailDomainIterator(std::prev((*this)->FailDomains.end()), *this);
    }

    inline TBlobStorageGroupInfo::TFailDomainRange TBlobStorageGroupInfo::TFailRealmIterator::GetFailRealmFailDomains() const {
        return TFailDomainRange(FailRealmFailDomainsBegin(), FailRealmFailDomainsEnd());
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TFailRealmIterator::FailRealmVDisksBegin() const {
        return FailRealmFailDomainsBegin().FailDomainVDisksBegin();
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TFailRealmIterator::FailRealmVDisksEnd() const {
        return (--FailRealmFailDomainsEnd()).FailDomainVDisksEnd();
    }

    inline TBlobStorageGroupInfo::TVDiskRange TBlobStorageGroupInfo::TFailRealmIterator::GetFailRealmVDisks() const {
        return TVDiskRange(FailRealmVDisksBegin(), FailRealmVDisksEnd());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TFailDomainIterator implementation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TFailDomainIterator::FailDomainVDisksBegin() const {
        return TVDiskIterator((*this)->VDisks.begin(), *this);
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TFailDomainIterator::FailDomainVDisksEnd() const {
        return ++TVDiskIterator(std::prev((*this)->VDisks.end()), *this);
    }

    inline TBlobStorageGroupInfo::TVDiskRange TBlobStorageGroupInfo::TFailDomainIterator::GetFailDomainVDisks() const {
        return TVDiskRange(FailDomainVDisksBegin(), FailDomainVDisksEnd());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlobStorageGroupInfo iterator builders
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    inline TBlobStorageGroupInfo::TFailRealmIterator TBlobStorageGroupInfo::FailRealmsBegin() const {
        return TFailRealmIterator(Topology->FailRealms.begin(), TRootIteratorBase(Topology.get(), false));
    }

    inline TBlobStorageGroupInfo::TFailRealmIterator TBlobStorageGroupInfo::FailRealmsEnd() const {
        return ++TFailRealmIterator(std::prev(Topology->FailRealms.end()), TRootIteratorBase(Topology.get(), false));
    }

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::FailDomainsBegin() const {
        return FailRealmsBegin().FailRealmFailDomainsBegin();
    }

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::FailDomainsEnd() const {
        return (--FailRealmsEnd()).FailRealmFailDomainsEnd();
    }

    inline TBlobStorageGroupInfo::TFailDomainRange TBlobStorageGroupInfo::GetFailDomains() const {
        return TFailDomainRange(FailDomainsBegin(), FailDomainsEnd());
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::VDisksBegin() const {
        return FailRealmsBegin().FailRealmVDisksBegin();
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::VDisksEnd() const {
        return (--FailRealmsEnd()).FailRealmVDisksEnd();
    }

    inline TBlobStorageGroupInfo::TVDiskRange TBlobStorageGroupInfo::GetVDisks() const {
        return TVDiskRange(VDisksBegin(), VDisksEnd());
    }



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlobStorageGroupInfo::TTopology iterator builders
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    inline TBlobStorageGroupInfo::TFailRealmIterator TBlobStorageGroupInfo::TTopology::FailRealmsBegin() const {
        return TFailRealmIterator(FailRealms.begin(), TRootIteratorBase(this, false));
    }

    inline TBlobStorageGroupInfo::TFailRealmIterator TBlobStorageGroupInfo::TTopology::FailRealmsEnd() const {
        return ++TFailRealmIterator(std::prev(FailRealms.end()), TRootIteratorBase(this, false));
    }

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::TTopology::FailDomainsBegin() const {
        return FailRealmsBegin().FailRealmFailDomainsBegin();
    }

    inline TBlobStorageGroupInfo::TFailDomainIterator TBlobStorageGroupInfo::TTopology::FailDomainsEnd() const {
        return (--FailRealmsEnd()).FailRealmFailDomainsEnd();
    }

    inline TBlobStorageGroupInfo::TFailDomainRange TBlobStorageGroupInfo::TTopology::GetFailDomains() const {
        return TFailDomainRange(FailDomainsBegin(), FailDomainsEnd());
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TTopology::VDisksBegin() const {
        return FailRealmsBegin().FailRealmVDisksBegin();
    }

    inline TBlobStorageGroupInfo::TVDiskIterator TBlobStorageGroupInfo::TTopology::VDisksEnd() const {
        return (--FailRealmsEnd()).FailRealmVDisksEnd();
    }

    inline TBlobStorageGroupInfo::TVDiskRange TBlobStorageGroupInfo::TTopology::GetVDisks() const {
        return TVDiskRange(VDisksBegin(), VDisksEnd());
    }

} // NKikimr
