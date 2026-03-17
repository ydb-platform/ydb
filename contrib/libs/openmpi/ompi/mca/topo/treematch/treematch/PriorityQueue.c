#include <stdlib.h>
#include "PriorityQueue.h"

/*
  This comparison function is used to sort elements in key descending order.
*/
static int compFunc(const FiboNode * const node1, const FiboNode * const node2)
{
  return 
    ( ( ((QueueElement*)(node1))->key > ((QueueElement*)(node2))->key ) ? -1 : 1); 
}

int PQ_init(PriorityQueue * const q, int size)
{
  int i;
  q->size = size;
  q->elements = malloc(sizeof(QueueElement *) * size);
  for(i=0; i < size; i++)
    q->elements[i]=NULL;
  return fiboTreeInit((FiboTree *)q, compFunc);
}

void PQ_exit(PriorityQueue * const q)
{
  
  int i;
  for(i = 0; i < q->size; i++)
    {
      if(q->elements[i] != NULL)
	free(q->elements[i]);
    }
  if(q->elements != NULL)
    free(q->elements);
  fiboTreeExit((FiboTree *)q);
}
void PQ_free(PriorityQueue * const q)
{
  int i;
  for(i = 0; i < q->size; i++)
    {
      if(q->elements[i] != NULL)
	free(q->elements[i]);
    }
  fiboTreeFree((FiboTree *)q);
}

int PQ_isEmpty(PriorityQueue * const q)
{
  FiboTree * tree = (FiboTree *)q;
/* if the tree root is linked to itself then the tree is empty */
  if(&(tree->rootdat) == (tree->rootdat.linkdat.nextptr)) 
    return 1;
  return 0;
}

void PQ_insertElement(PriorityQueue * const q, QueueElement * const e)
{
  if(e->value >= 0 && e->value < q->size)
    {
      fiboTreeAdd((FiboTree *)q, (FiboNode *)(e));
      q->elements[e->value] = e;
      e->isInQueue = 1;
    }
}
void PQ_deleteElement(PriorityQueue * const q, QueueElement * const e)
{
  fiboTreeDel((FiboTree *)q, (FiboNode *)(e));
  q->elements[e->value] = NULL;
  e->isInQueue = 0;
}

void PQ_insert(PriorityQueue * const q, int val, double key)
{
  if( val >= 0 && val < q->size)
    {
      QueueElement * e = malloc(sizeof(QueueElement));
      e->value = val;
      e->key = key;
      PQ_insertElement(q, e);
    }
}

void PQ_delete(PriorityQueue * const q, int val)
{
  QueueElement * e = q->elements[val];
  PQ_deleteElement(q, e);
  free(e);
}

QueueElement * PQ_findMaxElement(PriorityQueue * const q)
{
  QueueElement * e = (QueueElement *)(fiboTreeMin((FiboTree *)q));
  return e;
}
QueueElement * PQ_deleteMaxElement(PriorityQueue * const q)
{
  QueueElement * e = (QueueElement *)(fiboTreeMin((FiboTree *)q));
  if(e != NULL)
    {
      PQ_deleteElement(q, e);
    }
  return e;
}

double PQ_findMaxKey(PriorityQueue * const q)
{
  QueueElement * e = PQ_findMaxElement(q);
  if(e!=NULL)
    return e->key;
  return 0;
}

int PQ_deleteMax(PriorityQueue * const q)
{
  QueueElement * e = PQ_deleteMaxElement(q);
  int res = -1;
  if(e != NULL)
    res = e->value;
  free(e);
  return res;
}

void PQ_increaseElementKey(PriorityQueue * const q, QueueElement * const e, double i)
{
  if(e->isInQueue)
    {
      PQ_deleteElement(q, e);
      e->key += i;
      PQ_insertElement(q, e);
    }
}
void PQ_decreaseElementKey(PriorityQueue * const q, QueueElement * const e, double i)
{
  if(e->isInQueue)
    {
      PQ_deleteElement(q, e);
      e->key -= i;
      PQ_insertElement(q, e);
    }
}
void PQ_adjustElementKey(PriorityQueue * const q, QueueElement * const e, double i)
{
  if(e->isInQueue)
    {    
      PQ_deleteElement(q, e);
      e->key = i;
      PQ_insertElement(q, e);
    }
}

void PQ_increaseKey(PriorityQueue * const q, int val, double i)
{
  QueueElement * e = q->elements[val];
  if(e != NULL)
    PQ_increaseElementKey(q, e, i);
}

void PQ_decreaseKey(PriorityQueue * const q, int val, double i)
{
  QueueElement * e = q->elements[val];
  if(e != NULL)
    PQ_decreaseElementKey(q, e, i);
}

void PQ_adjustKey(PriorityQueue * const q, int val, double i)
{
  QueueElement * e = q->elements[val];
  if(e != NULL)
    PQ_adjustElementKey(q, e, i);
}
