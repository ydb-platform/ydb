#ifndef INTEGER_CONSTANT_INITIALIZED_VECTOR
#define INTEGER_CONSTANT_INITIALIZED_VECTOR

typedef struct int_CIVector_
{
  int init_value, size, top, *to, *from, *vec;
} int_CIVector;

int intCIV_isInitialized(int_CIVector * v, int i);
void intCIV_init(int_CIVector * v, int size, int init_value);
void intCIV_exit(int_CIVector * v);
int intCIV_set(int_CIVector * v, int i, int val);
int intCIV_get(int_CIVector * v, int i);

#endif /*INTEGER_CONSTANT_INITIALIZED_VECTOR*/
