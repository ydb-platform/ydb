#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/util/iterator.h>

#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <type_traits>
#include <iterator>


namespace NKikimr {

    namespace NSync {

        ////////////////////////////////////////////////////////////////////////////
        // TVDiskInfo
        ////////////////////////////////////////////////////////////////////////////
        template <class T>
        class TVDiskInfo {
        public:
            const TVDiskIdShort VDiskIdShort;
            const bool MyFailDomain;
            const bool Myself;
            const ui32 OrderNumber;
            const ui32 DomainOrderNumber;

        private:
            T Payload;

        public:
            TVDiskInfo(const TVDiskIdShort &vdiskShort,
                       bool myFailDomain,
                       bool myself,
                       const ui32 orderNumber,
                       const ui32 domainOrderNumber)
                : VDiskIdShort(vdiskShort)
                , MyFailDomain(myFailDomain)
                , Myself(myself)
                , OrderNumber(orderNumber)
                , DomainOrderNumber(domainOrderNumber)
            {}

            TString ToString() const {
                TStringStream str;
                str << "{" << "VDiskIdShort: " << VDiskIdShort
                    << " " << Payload.ToString()
                    << " MyFailDomain# " << MyFailDomain << "}";
                return str.Str();
            }

            T &Get() { return Payload; }
            const T &Get() const { return Payload; }
        };


        ////////////////////////////////////////////////////////////////////////////
        // VDISK NEIGHBORS
        ////////////////////////////////////////////////////////////////////////////
        template <class T>
        class TVDiskNeighbors : public TThrRefBase {
        public:
            using TValue = TVDiskInfo<T>;
            using TThis = TVDiskNeighbors<T>;

            // TNeighbors contains nested vectors of FailRealms, FailDomains and VDisks inside them; e.g.
            // [ FailRealm0:[ FailDomain0:[ VDisk_0_0_0 VDisk_0_0_1 ] FailDomain1:[ VDisk_0_1_0 ... ] ] ]
            // that is TNeighbors[vdisk.FailRealm][vdisk.FailDomain][vdisk.VDisk] contains per-VDisk metadata
            using TNeighbors = TVector<TVector<TVector<TValue>>>;

            template<bool Const>
            class TFailDomainIteratorImpl;

            TVDiskNeighbors(const TVDiskIdShort &self,
                            std::shared_ptr<TBlobStorageGroupInfo::TTopology> top)
                : Self(self)
                , Erasure(top->GType.GetErasure())
            {
                Setup(top);
            }

            TValue &operator [](const TVDiskIdShort &vdisk) {
                return Neighbors.at(vdisk.FailRealm).at(vdisk.FailDomain).at(vdisk.VDisk);
            }

            const TValue &operator [](const TVDiskIdShort &vdisk) const {
                return Neighbors.at(vdisk.FailRealm).at(vdisk.FailDomain).at(vdisk.VDisk);
            }

            ui32 GetTotalDisks() const {
                return TotalDisks;
            }

            template<bool Const>
            bool IsMyFailDomain(const TFailDomainIteratorImpl<Const>& it) {
                return it == GetMyFailDomainIter();
            }

            template<bool Const>
            bool IsMyFailDomain(const TFailDomainIteratorImpl<Const>& it) const {
                return it == GetMyFailDomainIter();
            }

            ///////////////////////////////////////////////////////////////////////////
            // VDisk iterator; iterates over all VDisks within all fail domains
            ///////////////////////////////////////////////////////////////////////////
            template<bool Const>
            class TIteratorImpl
                : public TIteratorFacade<TIteratorImpl<Const>,
                                         typename std::conditional<Const,
                                             const TValue,
                                             TValue>::type>
            {
                using TNeighbors = typename std::conditional<Const,
                    const typename TVDiskNeighbors::TNeighbors,
                    typename TVDiskNeighbors::TNeighbors>::type;

                using TValue = typename std::conditional<Const,
                    const typename TVDiskNeighbors::TVDiskNeighbors::TValue,
                    typename TVDiskNeighbors::TValue>::type;

                using TFailDomainIterator = TFailDomainIteratorImpl<Const>;

                TFailDomainIterator FailDomainIter;
                ui32 VDisk;

            public:
                using value_type = TValue;
                using reference = value_type&;
                using pointer = value_type*;
                using difference_type = std::size_t;
                using iterator_category = std::forward_iterator_tag;
                TIteratorImpl(const TFailDomainIterator& failDomainIter, ui32 vdisk)
                    : FailDomainIter(failDomainIter)
                    , VDisk(vdisk)
                {}

            private:
                friend struct TIteratorFacade<TIteratorImpl, TValue>;

                TValue& Dereference() const {
                    auto& vec = FailDomainIter.GetVDiskVector();
                    return vec[VDisk];
                }

                void MoveNext() {
                    const auto& vec = FailDomainIter.GetVDiskVector();
                    if (++VDisk == vec.size()) {
                        VDisk = 0;
                        ++FailDomainIter;
                    }
                }

                template<bool OtherConst>
                bool EqualTo(const TIteratorImpl<OtherConst>& other) const {
                    return FailDomainIter == other.FailDomainIter && VDisk == other.VDisk;
                }

                size_t DistanceTo(const TIteratorImpl& other) const {
                    size_t rv = 0;
                    ui32 offset = VDisk;
                    TFailDomainIterator iter(FailDomainIter);
                    while (iter != other.FailDomainIter) {
                        rv += iter.GetVDiskVector().size() - offset;
                        ++iter;
                        offset = 0;
                    }
                    rv += other.VDisk - offset;
                    return rv;
                }
            };

            ///////////////////////////////////////////////////////////////////////////
            // VDisk range suitable for use in ranged-for
            ///////////////////////////////////////////////////////////////////////////
            template<bool Const>
            using TVDiskRangeImpl = TIteratorRange<TIteratorImpl<Const>>;

            ///////////////////////////////////////////////////////////////////////////
            // Fail domain iterator
            ///////////////////////////////////////////////////////////////////////////
            template<bool Const>
            class TFailDomainIteratorImpl
                : public TIteratorFacade<TFailDomainIteratorImpl<Const>, TVDiskRangeImpl<Const>, TVDiskRangeImpl<Const>>
            {
                using TNeighbors = typename std::conditional<Const,
                    const typename TVDiskNeighbors::TNeighbors,
                    typename TVDiskNeighbors::TNeighbors>::type;

                using TValue = typename std::conditional<Const,
                    const typename TVDiskNeighbors::TValue,
                    typename TVDiskNeighbors::TValue>::type;

                // alias for VDisk range class
                using TVDiskRange = TVDiskRangeImpl<Const>;

                // alias for VDisk iterator
                using TIterator = TIteratorImpl<Const>;

                TNeighbors &Ref;
                ui32 FailRealm;
                ui32 FailDomain;

            public:
                using value_type = TValue;
                using reference = value_type&;
                using pointer = value_type*;
                using difference_type = std::size_t;
                using iterator_category = std::forward_iterator_tag;
                TFailDomainIteratorImpl(TNeighbors &ref, ui32 ring, ui32 failDomain)
                    : Ref(ref)
                    , FailRealm(ring)
                    , FailDomain(failDomain)
                {}

            private:
                friend class TVDiskNeighbors;

                ui32 GetFailRealmIndex() const {
                    return FailRealm;
                }

                ui32 GetFailDomainIndex() const {
                    return FailDomain;
                }

            private:
                template<bool OtherConst>
                friend class TIteratorImpl;

                using TVDiskVector = typename std::conditional<Const,
                      const typename TNeighbors::value_type::value_type,
                      typename TNeighbors::value_type::value_type>::type;

                TVDiskVector& GetVDiskVector() const {
                    return Ref[FailRealm][FailDomain];
                }

            private:
                friend struct TIteratorFacade<TFailDomainIteratorImpl, TVDiskRangeImpl<Const>, TVDiskRangeImpl<Const>>;

                TVDiskRange Dereference() const {
                    TIterator begin(*this, 0);
                    TIterator end(++TFailDomainIteratorImpl(*this), 0);
                    return TVDiskRange(begin, end);
                }

                void MoveNext() {
                    if (++FailDomain == Ref[FailRealm].size()) {
                        FailDomain = 0;
                        ++FailRealm;
                    }
                }

                template<bool OtherConst>
                bool EqualTo(const TFailDomainIteratorImpl<OtherConst>& other) const {
                    Y_ABORT_UNLESS(&Ref == &other.Ref);
                    return FailRealm == other.FailRealm && FailDomain == other.FailDomain;
                }
            };

            ///////////////////////////////////////////////////////////////////////////
            // Fail domain range for use in ranged-for
            ///////////////////////////////////////////////////////////////////////////
            template<bool Const>
            using TFailDomainRangeImpl = TIteratorRange<TFailDomainIteratorImpl<Const>>;

            using TIterator = TIteratorImpl<false>;
            using TConstIterator = TIteratorImpl<true>;

            using TFailDomainIterator = TFailDomainIteratorImpl<false>;
            using TConstFailDomainIterator = TFailDomainIteratorImpl<true>;

            TFailDomainIterator GetMyFailDomainIter() {
                return TFailDomainIterator(Neighbors, Self.FailRealm, Self.FailDomain);
            }

            TConstFailDomainIterator GetMyFailDomainIter() const {
                return TConstFailDomainIterator(Neighbors, Self.FailRealm, Self.FailDomain);
            }

            TFailDomainRangeImpl<true> GetFailDomains() const {
                return TFailDomainRangeImpl<true>(TConstFailDomainIterator(Neighbors, 0, 0),
                                                  TConstFailDomainIterator(Neighbors, Neighbors.size(), 0));
            }

            TFailDomainRangeImpl<false> GetFailDomains() {
                return TFailDomainRangeImpl<false>(TFailDomainIterator(Neighbors, 0, 0),
                                                   TFailDomainIterator(Neighbors, Neighbors.size(), 0));
            }

            TIterator Begin() {
                return TIterator(TFailDomainIterator(Neighbors, 0, 0), 0);
            }

            TIterator begin() {
                return Begin();
            }

            TConstIterator Begin() const {
                return TConstIterator(TConstFailDomainIterator(Neighbors, 0, 0), 0);
            }

            TConstIterator begin() const {
                return Begin();
            }

            TIterator End() {
                return TIterator(TFailDomainIterator(Neighbors, Neighbors.size(), 0), 0);
            }

            TIterator end() {
                return End();
            }

            TConstIterator End() const {
                return TConstIterator(TConstFailDomainIterator(Neighbors, Neighbors.size(), 0), 0);
            }

            TConstIterator end() const {
                return End();
            }

            TString ToString(char sep = '\0') const {
                TStringStream s;
                s << "{";
                for (TConstIterator it = Begin(), e = End(); it != e; ++it) {
                    s << it->ToString();
                    if (sep)
                        s << sep;
                }
                s << "}";
                return s.Str();
            }

            template <class TPrinter>
            void OutputHtml(IOutputStream &str, TPrinter &printer, const TString &name, const TString &divClass) const {
                str << "\n";
                HTML(str) {
                    DIV_CLASS (divClass) {
                        DIV_CLASS("panel-heading") {str << name;}
                        DIV_CLASS("panel-body") {
                            DIV_CLASS("row") {
                                OutputHtmlTable<TPrinter>(str, printer);
                            }
                        }
                    }
                }
                str << "\n";
            }

            template <class TPrinter>
            void OutputHtmlTable(IOutputStream &str, TPrinter &printer) const {
                TVector<TConstIterator> its;
                HTML(str) {
                    TABLE_CLASS ("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                const auto& domains = GetFailDomains();
                                for (auto it = domains.begin(); it != domains.end(); ++it) {
                                    TABLEH() {str << "Domain";}

                                    const auto& vdisks = *it;
                                    its.emplace_back(vdisks.begin());
                                }
                            }
                        }
                        OutputHtmlTableBody<TPrinter>(str, printer, its);
                    }
                }
            }

        public:
            const TVDiskIdShort Self;
            const TErasureType::EErasureSpecies Erasure;

        protected:
            TNeighbors Neighbors;
            ui32 TotalDisks = 0;
            ui32 DisksInDomain = 0;

            void Setup(std::shared_ptr<TBlobStorageGroupInfo::TTopology> top) {
                Neighbors.resize(top->GetTotalFailRealmsNum());
                for (auto realmIt = top->FailRealmsBegin(), realmEnd = top->FailRealmsEnd(); realmIt != realmEnd; ++realmIt) {
                    auto& realm = Neighbors[realmIt.GetFailRealmIdx()];
                    realm.resize(realmIt.GetNumFailDomainsPerFailRealm());
                    for (auto domIt = realmIt.FailRealmFailDomainsBegin(), domEnd = realmIt.FailRealmFailDomainsEnd();
                            domIt != domEnd; ++domIt) {
                        auto& domain = realm[domIt.GetFailDomainIdx()];
                        const bool myFailDomain = top->GetFailDomainOrderNumber(Self) == domIt->FailDomainOrderNumber;
                        for (const auto& vdisk : domIt.GetFailDomainVDisks()) {
                            const bool myself = Self == vdisk.VDiskIdShort;
                            domain.emplace_back(vdisk.VDiskIdShort, myFailDomain, myself,
                                                vdisk.OrderNumber, vdisk.FailDomainOrderNumber);
                            TotalDisks++;
                        }
                    }
                }

                bool first = true;
                for (const auto& vdisks : GetFailDomains()) {
                    const ui32 numDisks = vdisks.end() - vdisks.begin();
                    if (first) {
                        DisksInDomain = numDisks;
                    } else {
                        Y_ABORT_UNLESS(DisksInDomain == numDisks);
                    }
                }

                Y_ABORT_UNLESS(top->GType.GetErasure() == TBlobStorageGroupType::ErasureNone || TotalDisks > 2);
            }

            template <class TPrinter>
            void OutputHtmlTableBody(IOutputStream &str, TPrinter &printer, TVector<TConstIterator> &its) const {
                HTML(str) {
                    TABLEBODY() {
                        for (ui32 row = 0; row < DisksInDomain; ++row) {
                            TABLER() {
                                // column iterator -- one entry for each domain in "its"
                                typename TVector<TConstIterator>::iterator colIt = its.begin();

                                for (const auto& vdisks : GetFailDomains()) {
                                    TABLED() {
                                        TConstIterator& x = *colIt++;
                                        printer(str, x++);
                                        if (row + 1 == DisksInDomain) {
                                            Y_ABORT_UNLESS(x == vdisks.end());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        ////////////////////////////////////////////////////////////////////////////
        // VDISK NEIGHBORS with serialize/deserialize
        ////////////////////////////////////////////////////////////////////////////
        template <class T>
        class TVDiskNeighborsSerializable : public TVDiskNeighbors<T> {
        public:
            using TBase = TVDiskNeighbors<T>;
            using TConstIterator = typename TBase::TConstIterator;
            using TIterator = typename TBase::TIterator;
            using TValue = typename TBase::TValue;

            using TBase::Begin;
            using TBase::End;


            TVDiskNeighborsSerializable(const TVDiskIdShort &self,
                                        std::shared_ptr<TBlobStorageGroupInfo::TTopology> top)
                : TBase(self, top)
            {}

            // TSer must implement the following interface:
            // void operator() (const TValue &val);
            // void Finish();
            template <class TSer>
            void GenericSerialize(TSer &ser) const {
                for (TConstIterator cur = Begin(), end = End(); cur != end; ++cur) {
                    ser(*cur);
                }
                ser.Finish();
            }

            // TDes must implement the following interface:
            // void operator() (TValue &val);
            // void Finish();
            template <class TDes>
            void GenericParse(TDes &des) {
                for (TIterator cur = Begin(), end = End(); cur != end; ++cur) {
                    des(*cur);
                }
                des.Finish();
            }
        };

    } // NSync

} // NKikimr


