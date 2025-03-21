#include <library/cpp/resource/resource.h>
#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/string/cast.h>
#include <util/stream/mem.h>

#ifdef _unix_
#include <netinet/in.h>
#else
#include <winsock2.h>
#endif

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/dist.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/genrand.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/dcomp.h>
}

#define FL_LOADED    0x01

struct TDist: public dist_t {
    TVector<int> TypeVector;
    TVector<int> WeightSets;
    TVector<int*> WeightSetIndex;
    TVector<int> Maximums;
    TVector<int> ValueSets;
    TVector<int*> ValueSetIndex;
    TString Strings;
    TString Names;
};

struct TDIdx: public d_idx_t {
    explicit TDIdx(IInputStream& mi) {
        memset(this, 0, sizeof(d_idx_t));
        mi.Read(name, D_NAME_LEN);
        name[D_NAME_LEN] = '\0';
        index = ReadInt(mi);
        offset = ReadInt(mi);
        str_space = ReadInt(mi);
        length = ReadInt(mi);
        w_width = ReadInt(mi);
        v_width = ReadInt(mi);
        name_space = ReadInt(mi);
        dist = nullptr;
    }

    void Load() {
        dist = &Dist;
        auto resource = NResource::Find("tpcds.idx");
        TStringInput mi(resource);
        mi.Skip(offset);
        Dist.TypeVector.resize(v_width);
        Dist.type_vector = Dist.TypeVector.data();
        for (int i = 0; i < v_width; i++) {
            Dist.TypeVector[i] = ReadInt(mi);
        }
        Dist.WeightSetIndex.resize(w_width);
        Dist.WeightSets.resize(w_width * length);
        Dist.weight_sets = Dist.WeightSetIndex.data();
        Dist.Maximums.resize(w_width);
        Dist.maximums = Dist.Maximums.data();
        for (int i = 0; i < w_width; i++) {
            Dist.Maximums[i] = 0;
            Dist.WeightSetIndex[i] = Dist.WeightSets.data() + i * length;
            for (int j = 0; j < length; j++) {
                Dist.maximums[i] += ReadInt(mi);
                Dist.weight_sets[i][j] = Dist.maximums[i];
            }
        }

        Dist.ValueSetIndex.resize(v_width);
        Dist.ValueSets.resize(v_width * length);
        Dist.value_sets = Dist.ValueSetIndex.data();
        for (int i = 0; i < v_width; i++) {
            Dist.ValueSetIndex[i] = Dist.ValueSets.data() + i * length;
            for (int j = 0; j < length; j++) {
                Dist.value_sets[i][j] = ReadInt(mi);
            }
        }

        if (name_space) {
            Dist.Names.resize(name_space);
            Dist.names = Dist.Names.begin();
            mi.Read(Dist.names, name_space * sizeof(char));
        }

        Dist.Strings.resize(str_space);
        Dist.strings = Dist.Strings.begin();
        mi.Read(Dist.strings, str_space * sizeof(char));
        flags = FL_LOADED;
    }

    static inline int ReadInt(IInputStream& mi) {
        int temp;
        mi.Read(&temp, sizeof(int32_t));
        return ntohl(temp);
    };
    TDist Dist;
};


class TDists {
public:
    TDists() {
        int temp;
        auto resource = NResource::Find("tpcds.idx");
        memcpy(&temp, resource.data(), sizeof(int32_t));
        int entry_count = ntohl(temp);
        TMemoryInput mi(resource.end() - entry_count * IDX_SIZE, entry_count * IDX_SIZE);
        for (auto i = 0; i < entry_count; ++i) {
            TDIdx current_idx(mi);
            TString name(current_idx.name);
            name.to_lower();
            Idxs.emplace(name, std::move(current_idx));
        }
    }

    d_idx_t* operator[](TString name) {
        name.to_lower();
        auto id = MapFindPtr(Idxs, name);
        if (id != NULL && id->flags != FL_LOADED) {
            id->Load();
        }
        return id;
    }

private:
    TMap<TString, TDIdx> Idxs;
};

extern "C" d_idx_t* find_dist(char *name) {
    return (*Singleton<TDists>())[name];
}

extern "C" int dist_weight(int *dest, char *d, int index, int wset) {
    d_idx_t *d_idx;
    dist_t *dist;
    int res;
    
    if ((d_idx = find_dist(d)) == NULL) {
        Cerr << "Invalid distribution name '" << d << "'" << Endl;
    }
    
    dist = d_idx->dist;
    
    res = dist->weight_sets[wset - 1][index - 1];
    /* reverse the accumulation of weights */
    if (index > 1)
        res -= dist->weight_sets[wset - 1][index - 2];
    
    if (dest == NULL)
        return(res);
    
    *dest = res;
    
    return(0);
}

extern "C" int distsize(char *name) {
    d_idx_t *dist;

    dist = find_dist(name);

    if (dist == NULL)
        return(-1);

    return(dist->length);
}

extern "C" int dist_op(void *dest, int op, char *d_name, int vset, int wset, int stream) {
    d_idx_t *d;
    dist_t *dist;
    int index = 0,
        dt;
    char *char_val;
    int i_res = 1;

    if ((d = find_dist(d_name)) == NULL) {
        Cerr << "Invalid distribution name '" << d_name << "'" << Endl;
        assert(d != NULL);
    }

    dist = d->dist;

    if (op == 0) {
        int level;
        genrand_integer(&level, DIST_UNIFORM, 1, 
            dist->maximums[wset - 1], 0, stream);
        while (level > dist->weight_sets[wset - 1][index] && 
            index < d->length)
            index += 1;
        dt = vset - 1;
        if ((index >= d->length) || (dt > d->v_width))
            INTERNAL("Distribution overrun");
        char_val = dist->strings + dist->value_sets[dt][index];
    } else { 
        index = vset - 1;
        dt = wset - 1;
        if (index >= d->length || index < 0)
        {
            fprintf(stderr, "Runtime ERROR: Distribution over-run/under-run\n");
            fprintf(stderr, "Check distribution definitions and usage for %s.\n",
            d->name);
            fprintf(stderr, "index = %d, length=%d.\n",
                index, d->length);
            exit(1);
        }
        char_val = dist->strings + dist->value_sets[dt][index];
    }

    switch(dist->type_vector[dt]) {
    case TKN_VARCHAR:
        if (dest) {
            *(char **)dest = (char *)char_val;
        }
        break;
    case TKN_INT:
        i_res = atoi(char_val);
        if (dest) {
            *(int *)dest = i_res;
        }
        break;
    case TKN_DATE:
        if (dest == NULL) {
            dest = (date_t *)malloc(sizeof(date_t));
        }
        strtodt((date_t *)dest, char_val);
        break;
    case TKN_DECIMAL:
        if (dest == NULL) {
            dest = (decimal_t *)malloc(sizeof(decimal_t));
        }
        strtodec((decimal_t *)dest,char_val);
        break;
    }

    return((dest == NULL)?i_res:index + 1);    /* shift back to the 1-based indexing scheme */
}
