$bitvector = Knn::ToBinaryStringBit([-1.0f, 1.0f]);
select Knn::FloatFromBinaryString($bitvector);
