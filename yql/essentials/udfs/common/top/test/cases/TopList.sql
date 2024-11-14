/* syntax version 1 */
select TOP(x,10) from 
(select [[1,2],[1],[1,2,3],[1],[1,2],[1]] as x)
flatten list by x;
