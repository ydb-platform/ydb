#include <stdlib.h>
#include <stdio.h>
#include "IntConstantInitializedVector.h"

int intCIV_isInitialized(int_CIVector * v, int i)
{
  if(v->top == 0)
    return 0;
  if(v->from[i] >= 0)
    if(v->from[i] < v->top && v->to[v->from[i]] == i) 
    return 1;
  return 0;
}



void intCIV_init(int_CIVector * v, int size, int init_value)
{
  v->init_value = init_value;
  v->size = size;
  v->top = 0;
  v->to = malloc(sizeof(int)*size);
  v->from = malloc(sizeof(int)*size);
  v->vec = malloc(sizeof(int)*size);
}

void intCIV_exit(int_CIVector * v)
{
  free(v->to);
  free(v->from);
  free(v->vec);
 }

int intCIV_set(int_CIVector * v, int i, int val)
{
  if(v == NULL)
    return -1;
  if(i < 0 || i >= v->size)
    return -1;
  if(!intCIV_isInitialized(v,i))
    {
      v->from[i] = v->top;
      v->to[v->top] = i;
      v->top++;
    }
  v->vec[i] = val;
  return 0;  
}

int intCIV_get(int_CIVector * v, int i)
{
  if(v == NULL)
    return -1;
  if(i < 0 || i >= v->size)
    return -1;
  if(intCIV_isInitialized(v,i))
    return v->vec[i];
  return v->init_value;
}

