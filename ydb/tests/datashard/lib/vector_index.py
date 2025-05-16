def get_vector(type, numb, size_vector):
    if type == "Float":
        values = [float(i) for i in range(size_vector - 1)]
        values.append(float(numb))
        return ",".join(f'{val}f' for val in values)

    values = [i for i in range(size_vector - 1)]
    values.append(numb)
    return ",".join(str(val) for val in values)


targets = {
    "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
    "distance": {
        "cosine": "Knn::CosineDistance",
        "manhattan": "Knn::ManhattanDistance",
        "euclidean": "Knn::EuclideanDistance",
    },
}
