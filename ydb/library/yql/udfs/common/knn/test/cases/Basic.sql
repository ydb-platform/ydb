--lazy list
select Knn::ToBinaryString(ListFromRange(3.0, 8.0));

--normal list
select Knn::ToBinaryString([1.0, 2.0]);

select Knn::FromBinaryString("12");
