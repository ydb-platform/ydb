#include "ketama.h"
#include "md5.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>


char k_error[255] = "";

static void set_error(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vsnprintf(k_error, sizeof(k_error), format, ap);
    va_end(ap);
}

void ketama_md5_digest(char *key, unsigned char md5pword[16])
{
    md5_state_t md5state;

    md5_init(&md5state);
    md5_append(&md5state, (unsigned char *)key, strlen(key));
    md5_finish(&md5state, md5pword);
}


unsigned int ketama_hashi(char *key)
{
    unsigned char digest[16];

    ketama_md5_digest(key, digest);
    return (unsigned int)((digest[3] << 24)
                        | (digest[2] << 16)
                        | (digest[1] <<  8)
                        |  digest[0]);
}


mcs* ketama_get(char* key, continuum_t *cont)
{
    unsigned int h = ketama_hashi(key);
    unsigned int highp = cont->num_points;
    mcs (*mcsarr)[cont->num_points] = cont->array;
    unsigned int lowp = 0, midp;
    unsigned int midval, midval1;

    if (cont->num_domains <= 0) {
        set_error("Continuum is empty");
        return NULL;
    }

    // divide and conquer array search to find server with next biggest
    // point after what this key hashes to
    while (1) {
        midp = (int)((lowp + highp) / 2);

        if (midp == cont->num_points)
            return &((*mcsarr)[0]); // if at the end, roll back to zeroth

        midval = (*mcsarr)[midp].point;
        midval1 = midp == 0 ? 0 : (*mcsarr)[midp-1].point;

        if ( h <= midval && h > midval1 )
            return &((*mcsarr)[midp]);

        if ( midval < h )
            lowp = midp + 1;
        else
            highp = midp - 1;

        if ( lowp > highp )
            return &((*mcsarr)[0]);
    }
}


void create_continuum(continuum_t *contptr)
{
    mcs continuum[contptr->num_domains * 160];
    unsigned int i, k, digits, num, cont = 0;

    for(i = 0; i < contptr->num_domains; i++) {
        float pct = (float)(contptr->domains[i].weight) / (float)contptr->weight;
        unsigned int ks = floorf(pct * 40.0 * (float)contptr->num_domains);
        num = ks;

        for (digits = 0; num > 0; digits++) {
            num /= 10;
        }

        char ss[SLOT_LEN + 1 + digits];

        for(k = 0; k < ks; k++) {
            /* 40 hashes, 4 numbers per hash = 160 points per server */
            unsigned char digest[16];

            sprintf(ss, "%s-%d", contptr->domains[i].slot, k);
            ketama_md5_digest(ss, digest);

            /* Use successive 4-bytes from hash as numbers
             * for the points on the circle: */
            int h;
            for(h = 0; h < 4; h++) {
                continuum[cont].point = (digest[3+h*4] << 24)
                                      | (digest[2+h*4] << 16)
                                      | (digest[1+h*4] <<  8)
                                      |  digest[h*4];

                continuum[cont].domain = &contptr->domains[i];
                cont++;
            }
        }
    }

    /* Sorts in ascending order of "point" */
    qsort((void*)&continuum, cont, sizeof(mcs), (compfn)ketama_compare);

    unsigned int bytes = sizeof(mcs) * cont;
    contptr->num_points = cont;
    contptr->array = malloc(bytes);
    memcpy(contptr->array, &continuum, bytes);
}


int ketama_add(continuum_t *contptr, const domain_t *domain)
{
    if (domain->weight <= 0) {
        set_error("Invalid weight value %d", domain->weight);
        return 0;
    }

    contptr->num_domains++;
    contptr->weight += domain->weight;

    contptr->domains = (domain_t*)realloc(contptr->domains, sizeof(domain_t) * contptr->num_domains);
    contptr->domains[contptr->num_domains - 1] = *domain;

    free(contptr->array);
    contptr->array = NULL;

    create_continuum(contptr);
    return 1;
}


int ketama_create(continuum_t *contptr, const domain_t domains[], unsigned int num_domains)
{
    unsigned long total_weight = 0;
    unsigned int i;
    for (i = 0; i < num_domains; ++i) {
        total_weight += domains[i].weight;
    }

    if (num_domains < 1) {
        set_error("No valid domains");
        return 0;
    }

    contptr->num_domains = num_domains;
    contptr->weight = total_weight;

    unsigned int bytes = sizeof(domain_t) * num_domains;
    contptr->domains = (domain_t*)malloc(bytes);
    memcpy(contptr->domains, domains, bytes);

    create_continuum(contptr);

    return 1;
}

void ketama_init(continuum_t *contptr)
{
    contptr->num_points = 0;
    contptr->num_domains = 0;
    contptr->weight = 0;
    contptr->array = NULL;
    contptr->domains = NULL;
}


void ketama_free(continuum_t *contptr)
{
    free(contptr->domains);
    free(contptr->array);
}


int ketama_compare(mcs *a, mcs *b)
{
    return (a->point < b->point) ? -1 : ((a->point > b->point) ? 1 : 0);
}


char* ketama_error()
{
    return k_error;
}
