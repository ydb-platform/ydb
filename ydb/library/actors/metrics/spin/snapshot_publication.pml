/*
 * TRawLineFrontend publication with one writer and one snapshot reader.
 * SNAPSHOT_PREFIX=1 captures the release-published length once; 0 models
 * reading the mutable RecordsCount header on each traversal.
 * Pinning/reuse is outside this model: the chunk remains owned by this line.
 * Payload stores, header stores, commit and reads are separate SC steps.
 * The fixed model abstracts release/acquire as publication of the initialized
 * prefix; it does not prove the C++ memory model or absence of data races.
 */
#ifndef SNAPSHOT_PREFIX
#define SNAPSHOT_PREFIX 1
#endif
#ifndef RECORDS
#define RECORDS 3
#endif

byte payload[RECORDS];
byte header_count = 0;
byte committed = 0;

proctype Writer() {
    byte i = 0;

    /* The original frontend initializes its first header before the record. */
    header_count = 1;
    do
    :: i < RECORDS ->
        payload[i] = i + 1;
        header_count = i + 1;
        committed = i + 1;
        i++
    :: else ->
        break
    od
}

proctype SnapshotReader() {
    byte captured;
    byte count;
    byte i;
    byte pass = 0;
    byte first_count;

#if SNAPSHOT_PREFIX
    captured = committed;
#endif
    do
    :: pass < 2 ->
#if SNAPSHOT_PREFIX
        count = captured;
#else
        count = header_count;
#endif
        if
        :: pass == 0 ->
            first_count = count
        :: else ->
            assert(count == first_count)
        fi;
        i = 0;
        do
        :: i < count ->
            /* No partially initialized record may become visible. */
            assert(payload[i] == i + 1);
            i++
        :: else ->
            break
        od;
        pass++
    :: else ->
        break
    od
}

init {
    atomic {
        run Writer();
        run SnapshotReader()
    }
}
