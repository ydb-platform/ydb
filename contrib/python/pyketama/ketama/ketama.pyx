from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

DEF SLOT_LEN = 37

cdef extern from "ketama.h":
    ctypedef struct continuum_t:
        pass

    ctypedef struct domain_t:
        char slot[SLOT_LEN]
        unsigned long weight

    ctypedef struct mcs:
        unsigned int point
        domain_t *domain

    int ketama_create(continuum_t *contptr, const domain_t *domains, unsigned int num_domains)
    int ketama_add(continuum_t*, const domain_t*)
    void ketama_init(continuum_t*)
    void ketama_free(continuum_t *contptr)
    mcs* ketama_get(char*, continuum_t*)
    char* ketama_error()


class KetamaError(Exception):
    pass


def _bytes(s):
    if isinstance(s, str):
        return s.encode("utf-8")
    return s


cdef class Continuum:
    cdef continuum_t _continum
    cdef dict _values

    def __cinit__(self, config=()):
        '''
        :param config: list, element can be a string, a tuple like (key,  weight) or like (key, value, weight), key should be a string
        '''
        cdef continuum_t continum
        cdef domain_t *domains

        ketama_init(&continum)

        self._continum = continum
        self._values = {}

        size = len(config)

        if config:
            domains = <domain_t*>malloc(sizeof(domain_t) * size)
            try:
                for i, c in enumerate(config):
                    if isinstance(config[0], str):
                        key, value, weight = c, c, 100
                    elif len(config[0]) == 2:
                        key, weight = c
                        value = key
                    elif len(config[0]) == 3:
                        key, value, weight = c

                    if weight <= 0:
                        raise KetamaError(
                            "Invalid weight value {}".format(weight))

                    slot = _bytes(key)
                    slot_len = len(slot)

                    if slot_len > SLOT_LEN - 1:
                        raise KetamaError("Key '{}' too long. "
                                          "Max key length is {}".format(
                            key, SLOT_LEN - 1))

                    memcpy(domains[i].slot, <char*>slot, slot_len)
                    domains[i].slot[slot_len] = '\0'
                    domains[i].weight = weight
                    self._values[slot] = value

                r = ketama_create(&self._continum, domains, size)
            finally:
                free(domains)

            if not r:
                raise KetamaError(ketama_error().decode("ascii"))

    def __dealloc__(self):
        ketama_free(&self._continum)

    def __setitem__(self, key, val):
        cdef domain_t domain

        slot, weight = key if isinstance(key, tuple) else (key, 100)

        slot = _bytes(slot)
        slot_len = len(slot)

        if slot_len > SLOT_LEN - 1:
            raise KetamaError("Key '{}' too long. Max key length is {}".format(
                key, SLOT_LEN - 1))

        memcpy(domain.slot, <char*>slot, slot_len)
        domain.slot[slot_len] = '\0'
        domain.weight = weight
        r = ketama_add(&self._continum, &domain)
        if not r:
            raise KetamaError(ketama_error().decode("ascii"))

        self._values[slot] = val

    def __getitem__(self, k):
        cdef bytes key = _bytes(k)
        cdef mcs *mcsarr = ketama_get(<char*>key, &self._continum)

        if not mcsarr:
            raise KetamaError(ketama_error().decode("ascii"))

        return self._values[mcsarr[0].domain[0].slot]
