select
    1 between 2 and 4,
    3 between 2 and 4,
    5 between 2 and 4,
    3 between 4 and 2,
    5 between 4 and 2,
    null between 2 and 4,
    1 between null and 4,
    1 between 4 and null,
    1 between 0 and null,
    1 between null and null,
    null between null and null;

select
    1 between asymmetric 2 and 4,
    3 between asymmetric 2 and 4,
    5 between asymmetric 2 and 4,
    3 between asymmetric 4 and 2,
    5 between asymmetric 4 and 2,
    null between asymmetric 2 and 4,
    1 between asymmetric null and 4,
    1 between asymmetric 4 and null,
    1 between asymmetric 0 and null,
    1 between asymmetric null and null,
    null between asymmetric null and null;

select
    1 between symmetric 2 and 4,
    3 between symmetric 2 and 4,
    5 between symmetric 2 and 4,
    1 between symmetric 4 and 2,
    3 between symmetric 4 and 2,
    5 between symmetric 4 and 2,
    null between symmetric 2 and 4,
    1 between symmetric null and 4,
    1 between symmetric 4 and null,
    1 between symmetric 0 and null,
    1 between symmetric null and null,
    null between symmetric null and null;
