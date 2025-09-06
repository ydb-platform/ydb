  * общие параметры для всех векторных индексов:
    * `vector_dimension` - размерность вектора эмбеддинга (значение от 1 до 16384);
    * `vector_type` - тип значений вектора (`float`, `uint8` или `int8`);
    * `distance` - функция расстояния (`cosine`, `manhattan` или `euclidean`), взаимосключающий с `similarity`;
    * `similarity` - функция схожести (`inner_product` или `cosine`), взаимосключающий с `distance`;
  * специфичные параметры для `vector_kmeans_tree`{% if backend_name == "YDB" and oss == true %} (см. [документацию](../../../../dev/vector-indexes.md#kmeans-tree-type)){% endif %}:
    * `clusters` - количество центроидов для алгоритма k-means (значение от 2 до 2048);
    * `levels` - количество уровней в дереве (значение от 1 до 16);
    * общее количество узлов в дереве, рассчитываемое как `clusters` в степени `levels`, должно быть не более чем 1073741824;
    * произведение `vector_dimension` на `clusters` должно быть не более чем 4194304.
