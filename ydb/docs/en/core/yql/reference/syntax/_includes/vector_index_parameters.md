  * common parameters for all vector indexes:
    * `vector_dimension` - embedding vector dimensionality (should be between 1 and 16384)
    * `vector_type` - vector value type (`float`, `uint8`, or `int8`)
    * `distance` - distance function (`cosine`, `manhattan`, or `euclidean`), mutually exclusive with `similarity`
	  * `similarity` - similarity function (`inner_product` or `cosine`), mutually exclusive with `distance`
  * specific parameters for `vector_kmeans_tree` (see [the reference](../../../../dev/vector-indexes.md#kmeans-tree-type)):
    * `clusters` - number of centroids for k-means algorithm (should be between 2 and 2048)
    * `levels` - number of levels in the tree (should be between 1 and 16)
    * the total number of nodes in the tree, calculated as `clusters` raised to the power of `levels`, should be no more than 1073741824
    * the product of `vector_dimension` and `clusters` should be no more than 4194304
