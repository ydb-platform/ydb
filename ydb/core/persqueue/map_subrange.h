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

    auto lowerBound = map.lower_bound(begin);
    if ((lowerBound != map.end()) && (lowerBound->first == begin) && !includeBegin) {
        // The `lower_bound` function returns the iterator to a value equal to or greater than
        // the desired value. If we have found the key in the map and the `IncludeBegin` flag is
        // reset, then we need to move the iterator forward.
        ++lowerBound;
    }

    auto upperBound = map.upper_bound(end);
    if ((upperBound != map.begin()) && (std::prev(upperBound)->first == end) && !includeEnd) {
        // The 'upper_bound` function returns the iterator by a value greater than the desired
        // value. If we found the key in the map and the `IncludeEnd` flag is reset, then we
        // need to move the iterator back.
        //
        // For example: there are keys [1, 2, 3, 4, 5] and we need to remove the keys from
        // the range [2, 4). The `upper_bound` function returns an iterator pointing to 5.
        // For a half-open interval, we need to move it 1 step back.
        --upperBound;
    }

    return {lowerBound, upperBound};
}

}
