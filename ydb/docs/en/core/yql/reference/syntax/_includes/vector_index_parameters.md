  * common parameters for all vector indexes:
    * `vector_dimension` - embedding vector dimensionality (16384 or less)
    * `vector_type` - vector value type (`float`, `uint8`, `int8`, or `bit`)
    * `distance` - distance function (`cosine`, `manhattan`, or `euclidean`), mutually exclusive with `similarity`
	  * `similarity` - similarity function (`inner_product` or `cosine`), mutually exclusive with `distance`
  * specific parameters for `vector_kmeans_tree` (see [{#T}](../../../../dev/vector-indexes.md#kmeans-tree-type)):
    * `clusters` - number of centroids for k-means algorithm (values greater than 1000 may degrade performance)
    * `levels` - number of levels in the tree