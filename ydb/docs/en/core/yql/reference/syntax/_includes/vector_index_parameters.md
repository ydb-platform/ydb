  * common parameters for all vector indexes:
    * `vector_dimension` - embedding vector dimensionality (16384 or less)
    * `vector_type` - vector value type (`float`, `uint8`, `int8`, `bit`)
    * `distance` - distance function (`cosine`, `manhattan`, `euclidean`) or `similarity` - similarity function (`inner_product`, `cosine`)
  * specific parameters for `vector_kmeans_tree`:
    * `clusters` - number of centroids for k-means algorithm (values greater than 1000 may degrade performance)
    * `levels` - number of levels in the tree