  * общие параметры для всех векторных индексов:
    * `vector_dimension` - размерность вектора эмбеддинга (16384 или меньше);
    * `vector_type` - тип значений вектора (`float`, `uint8`, `int8` или `bit`);
    * `distance` - функция расстояния (`cosine`, `manhattan` или `euclidean`), взаимосключающий с `similarity`;
    * `similarity` - функция схожести (`inner_product` или `cosine`), взаимосключающий с `distance`;
  * специфичные параметры для `vector_kmeans_tree` (см. [{#T}](../../../../dev/vector-indexes.md#kmeans-tree-type)):
    * `clusters` - количество центроидов для алгоритма k-means (значения более 1000 могут ухудшить производительность);
    * `levels` - количество уровней в дереве;
