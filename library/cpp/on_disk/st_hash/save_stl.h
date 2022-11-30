#pragma once

#include <util/generic/hash.h>
#include <util/system/yassert.h>
#include <util/stream/output.h>

// this structure might be replaced with sthashtable class
template <class HF, class Eq, class size_type>
struct sthashtable_nvm_sv {
    sthashtable_nvm_sv() {
        if (sizeof(sthashtable_nvm_sv) != sizeof(HF) + sizeof(Eq) + 3 * sizeof(size_type)) {
            memset(this, 0, sizeof(sthashtable_nvm_sv));
        }
    }

    sthashtable_nvm_sv(const HF& phf, const Eq& peq, const size_type& pnb, const size_type& pne, const size_type& pnd)
        : sthashtable_nvm_sv()
    {
        hf = phf;
        eq = peq;
        num_buckets = pnb;
        num_elements = pne;
        data_end_off = pnd;
    }

    HF hf;
    Eq eq;
    size_type num_buckets;
    size_type num_elements;
    size_type data_end_off;
};

/**
 * Some hack to save both THashMap and sthash.
 * Working with stHash does not depend on the template parameters, because the content of stHash is not used inside this method.
 */
template <class V, class K, class HF, class Ex, class Eq, class A>
template <class KeySaver>
inline int THashTable<V, K, HF, Ex, Eq, A>::save_for_st(IOutputStream* stream, KeySaver& ks, sthash<int, int, THash<int>, TEqualTo<int>, typename KeySaver::TSizeType>* stHash) const {
    Y_ASSERT(!stHash || stHash->bucket_count() == bucket_count());
    typedef sthashtable_nvm_sv<HF, Eq, typename KeySaver::TSizeType> sv_type;
    sv_type sv = {this->_get_hash_fun(), this->_get_key_eq(), static_cast<typename KeySaver::TSizeType>(buckets.size()), static_cast<typename KeySaver::TSizeType>(num_elements), 0};
    // to do: m.b. use just the size of corresponding object?
    typename KeySaver::TSizeType cur_off = sizeof(sv_type) +
                                           (sv.num_buckets + 1) * sizeof(typename KeySaver::TSizeType);
    sv.data_end_off = cur_off;
    const_iterator n;
    for (n = begin(); n != end(); ++n) {
        sv.data_end_off += static_cast<typename KeySaver::TSizeType>(ks.GetRecordSize(*n));
    }
    typename KeySaver::TSizeType* sb = stHash ? (typename KeySaver::TSizeType*)(stHash->buckets()) : nullptr;
    if (stHash)
        sv.data_end_off += static_cast<typename KeySaver::TSizeType>(sb[buckets.size()] - sb[0]);
    //saver.Align(sizeof(char*));
    stream->Write(&sv, sizeof(sv));

    size_type i;
    //save vector
    for (i = 0; i < buckets.size(); ++i) {
        node* cur = buckets[i];
        stream->Write(&cur_off, sizeof(cur_off));
        if (cur) {
            while (!((uintptr_t)cur & 1)) {
                cur_off += static_cast<typename KeySaver::TSizeType>(ks.GetRecordSize(cur->val));
                cur = cur->next;
            }
        }
        if (stHash)
            cur_off += static_cast<typename KeySaver::TSizeType>(sb[i + 1] - sb[i]);
    }
    stream->Write(&cur_off, sizeof(cur_off)); // end mark
    for (i = 0; i < buckets.size(); ++i) {
        node* cur = buckets[i];
        if (cur) {
            while (!((uintptr_t)cur & 1)) {
                ks.SaveRecord(stream, cur->val);
                cur = cur->next;
            }
        }
        if (stHash)
            stream->Write((const char*)stHash + sb[i], sb[i + 1] - sb[i]);
    }
    return 0;
}
