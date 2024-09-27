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

extern "C" int di_compare(const void *op1, const void *op2) {
    d_idx_t *ie1 = (d_idx_t *)op1,
        *ie2 = (d_idx_t *)op2;
    
    return strcasecmp(ie1->name, ie2->name);
}

int load_dist(d_idx_t *di) {
    int res = 0;
    int32_t temp;

    if (di->flags != FL_LOADED) {
        auto resource = NResource::Find("tpcds.idx");
        TStringInput mi(resource);
        mi.Skip(di->offset);
        di->dist = (dist_t *)malloc(sizeof(struct DIST_T));
        auto d = di->dist;
        d->type_vector = (int *)malloc(sizeof(int32_t) * di->v_width);
        for (int i = 0; i < di->v_width; i++) {
            mi.Read(&temp, sizeof(int32_t));
            d->type_vector[i] = ntohl(temp);
        }
        
        d->weight_sets = (int **)malloc(sizeof(int *) * di->w_width);
        d->maximums = (int *)malloc(sizeof(int32_t) * di->w_width);
        for (int i = 0; i < di->w_width; i++) {
            *(d->weight_sets + i) = (int *)malloc(di->length * sizeof(int32_t));
            d->maximums[i] = 0;
            for (int j = 0; j < di->length; j++) {
                mi.Read(&temp, sizeof(int32_t));
                *(*(d->weight_sets + i) + j) = ntohl(temp);
                d->maximums[i] += d->weight_sets[i][j];
                d->weight_sets[i][j] = d->maximums[i];
            }
        }
        
        d->value_sets = (int **)malloc(sizeof(int *) * di->v_width);
        MALLOC_CHECK(d->value_sets);
        for (int i = 0; i < di->v_width; i++) {
            *(d->value_sets + i) = (int *)malloc(di->length * sizeof(int32_t));
            for (int j = 0; j < di->length; j++) {
                mi.Read(&temp, sizeof(int32_t));
                *(*(d->value_sets + i) + j) = ntohl(temp);
            }
        }

        if (di->name_space) {
            d->names = (char *)malloc(di->name_space);
            mi.Read(d->names, di->name_space * sizeof(char));
        }

        d->strings = (char *)malloc(sizeof(char) * di->str_space);
        mi.Read(d->strings, di->str_space * sizeof(char));
        di->flags = FL_LOADED;
    }
    return(res);
}


class TDists {
public:
    TDists() {
        int temp;
        auto resource = NResource::Find("tpcds.idx");
        memcpy(&temp, resource.data(), sizeof(int32_t));
        int entry_count = ntohl(temp);
        TMemoryInput mi(resource.end() - entry_count * IDX_SIZE, entry_count * IDX_SIZE);
        Idxs.resize(entry_count);
        for (auto& current_idx: Idxs) {
            memset(&current_idx, 0, sizeof(d_idx_t));
            mi.Read(current_idx.name, D_NAME_LEN);
            current_idx.name[D_NAME_LEN] = '\0';
            mi.Read(&temp, sizeof(int32_t));
            current_idx.index = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.offset = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.str_space = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.length = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.w_width = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.v_width = ntohl(temp);
            mi.Read(&temp, sizeof(int32_t));
            current_idx.name_space = ntohl(temp);
            current_idx.dist = NULL;
        }
        qsort(Idxs.begin(), entry_count, sizeof(d_idx_t), di_compare);
    }

    d_idx_t* operator[](char* name) {
        d_idx_t key;
        strcpy(key.name, name);
        auto id = (d_idx_t *)bsearch(&key, Idxs.begin(), Idxs.size(), sizeof(d_idx_t), di_compare);
        if (id != NULL) {
            if (id->flags != FL_LOADED) {
                load_dist(id);
            }
        }
        return id;
    }

private:
    TVector<d_idx_t> Idxs;
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
        strtodt(*(date_t **)dest, char_val);
        break;
    case TKN_DECIMAL:
        if (dest == NULL) {
            dest = (decimal_t *)malloc(sizeof(decimal_t));
        }
        strtodec(*(decimal_t **)dest,char_val);
        break;
    }

    return((dest == NULL)?i_res:index + 1);    /* shift back to the 1-based indexing scheme */
}
