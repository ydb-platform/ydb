/* postgres can not */
select
ListFromRange(3,0,-1),
ListFromRange(3,0),
ListFromRange(0,3,-1),
ListFromRange(0,3),
ListFromRange(0,3,-0.5),
ListFromRange(0,3,+0.5),
ListFromRange(0u,7u,2u),
ListFromRange(7u,0u,-2);
