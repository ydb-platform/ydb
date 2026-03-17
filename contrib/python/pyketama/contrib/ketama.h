#ifndef KETAMA_H__
#define KETAMA_H__

#define SLOT_LEN 37

typedef int (*compfn)(const void*, const void*);

typedef struct
{
    char slot[SLOT_LEN];
    unsigned long weight;
} domain_t;

typedef struct
{
    unsigned int point;  // point on circle
    domain_t *domain;
} mcs;

typedef struct
{
    unsigned int num_points;
    unsigned int num_domains;
    int weight;
    domain_t *domains;
    void* array; //array of mcs structs
} continuum_t;


void ketama_init(continuum_t*);

int ketama_add(continuum_t*, const domain_t*);

int ketama_create(continuum_t *contptr, const domain_t *domains, unsigned int num_domains);

void ketama_free(continuum_t *contptr);

mcs* ketama_get(char*, continuum_t*);

int ketama_compare(mcs*, mcs*);

unsigned int ketama_hashi(char* inString);

void ketama_md5_digest(char* inString, unsigned char md5pword[16]);

char* ketama_error();

#endif // KETAMA_H__
