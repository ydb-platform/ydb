/* postgres can not */
select * from plato.Input where cast(key as Uint8) in
(
    1u,
    3l,
    23ul,
    255, -- out of Uint8
    0,
)
