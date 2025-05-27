namespace NKikimr::NPQ {

template<class M, class I = typename M::const_iterator>
std::pair<I, I> MapSubrange(const M& map,
                            const typename M::key_type& begin, bool includeBegin,
                            const typename M::key_type& end, bool includeEnd)
{
    if (map.empty()) {
        return {map.end(), map.end()};
    }

    if (begin == end) {
        if ((includeBegin != includeEnd) || !includeBegin) {
            return {map.end(), map.end()};
        }
    }

    auto leftBorder = includeBegin ? map.lower_bound(begin) : map.upper_bound(begin);
    auto rightBorder = includeEnd ? map.upper_bound(end) : map.lower_bound(end);

    return {leftBorder, rightBorder};
}

}
