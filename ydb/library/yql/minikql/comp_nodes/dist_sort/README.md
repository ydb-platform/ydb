# Distributed Sort Algorithm
*In development...*

## Description
* The purpose of this code is to sort strings in a distributed way;
* This algorithm is based on the [research](https://panthema.net/2019/publications/2019_Schimek_Distributed_String_Sorting_Algorithms.pdf) (pp. 25-31);
* Algorithms implemented are in the *algorithms* folder:
  * MSD String Radix Sort and Multiway Merge algorithms are taken from the [tlx library](https://github.com/bingmann/tlx) (BSL-1.0 license); 
* The preliminary results are in the *results* folder:
  * **The best speed is achieved for 24 nodes (~11 GB/s)**;
* More detailed description can be found on the [slides](https://docs.google.com/presentation/d/15iPLxKMe7wtphQoU5S4sZQXTm3NkLemxW_4ITsH5SgE/edit?usp=sharing).

## Run
* To run the algorithm, execute:
```
make distributed_sort && ./distributed_sort
```