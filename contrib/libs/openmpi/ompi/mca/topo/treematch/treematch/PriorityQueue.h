#ifndef PRIORITY_QUEUE
#define PRIORITY_QUEUE

#include "fibo.h"

/*
  This is the struct for our elements in a PriorityQueue.
  The node is at first place so we only have to use a cast to switch between QueueElement's pointer and Fibonode's pointer.
*/
typedef struct QueueElement_
{
  FiboNode node; /*the node used to insert the element in a FiboTree*/
  double key; /*the key of the element,  elements are sorted in a descending order according to their key*/
  int value;
  int isInQueue;
} QueueElement;

typedef struct PriorityQueue_
{
  FiboTree tree;
  QueueElement ** elements; /*a vector of element with their value as key so we can easily retreive an element from its value */
  int size; /*the size allocated to the elements vector*/
} PriorityQueue;


/*
  PQ_init initiates a PriorityQueue with a size given in argument and sets compFunc as comparison function. Note that you have to allocate memory to the PriorityQueue pointer before calling this function.
  Returns : 
    0 if success
    !0 if failed

  PQ_free simply empties the PriorityQueue but does not free the memory used by its elements.
  PQ_exit destroys the PriorityQueue without freeing elements. The PriorityQueue is no longer usable without using PQ_init again.
Note that the PriorityQueue pointer is not deallocated.
*/
int PQ_init(PriorityQueue * const, int size);
void PQ_free(PriorityQueue * const);
void PQ_exit(PriorityQueue * const);

/*
  PQ_isEmpty returns 1 if the PriorityQueue is empty, 0 otherwise.
*/
int PQ_isEmpty(PriorityQueue * const);

/*
  PQ_insertElement inserts the given QueueElement in the given PriorityQueue
*/
void PQ_insertElement(PriorityQueue * const, QueueElement * const); 
/*
  PQ_deleteElement delete the element given in argument from the PriorityQueue.
*/
void PQ_deleteElement(PriorityQueue * const, QueueElement * const);

/*
  PQ_insert inserts an element in the PriorityQueue with the value and key given in argument.
*/
void PQ_insert(PriorityQueue * const, int val, double key);
/*
  PQ_delete removes the first element found with the value given in argument and frees it.
*/
void PQ_delete(PriorityQueue * const, int val);


/*
  PQ_findMaxElement returns the QueueElement with the greatest key in the given PriorityQueue
*/
QueueElement * PQ_findMaxElement(PriorityQueue * const);
/*
  PQ_deleteMaxElement returns the QueueElement with the geatest key in the given PriorityQueue and removes it from the queue.
*/
QueueElement * PQ_deleteMaxElement(PriorityQueue * const);

/*
  PQ_findMax returns the key of the element with the geatest key in the given PriorityQueue
*/
double PQ_findMaxKey(PriorityQueue * const);
/*
  PQ_deleteMax returns the value of the element with the greatest key in the given PriorityQueue and removes it from the queue.
*/
int PQ_deleteMax(PriorityQueue * const);

/*
  PQ_increaseElementKey adds the value of i to the key of the given QueueElement
*/
void PQ_increaseElementKey(PriorityQueue * const, QueueElement * const, double i);
/*
  PQ_decreaseElementKey substracts the value of i from the key of the given QueueElement
*/
void PQ_decreaseElementKey(PriorityQueue * const, QueueElement * const, double i);
/*
  PQ_adjustElementKey sets to i the key of the given QueueElement.
*/
void PQ_adjustElementKey(PriorityQueue * const, QueueElement * const, double i);

/*
  PQ_increaseKey adds i to the key of the first element found with a value equal to val in the PriorityQueue.
*/
void PQ_increaseKey(PriorityQueue * const, int val, double i);
/*
  PQ_decreaseKey substracts i from the key of the first element found with a value equal to val in the PriorityQueue.
*/
void PQ_decreaseKey(PriorityQueue * const, int val, double i);
/*
  PQ_adjustKey sets to i the key of the first element found with a value equal to val in the PriorityQueue.
*/
void PQ_adjustKey(PriorityQueue * const, int val, double i);

#endif /*PRIORITY_QUEUE*/
