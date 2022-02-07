
mkdir -p out

echo "
    LogIntModFilter 
    SleepCheck 
    MultipleActionsCheck 
    KillCheck 
    InMemoryLog 
    InMemoryLogCheck 
    NoExec 
    Log 
    LogTs 
    FalseIntFilter 
    LogIntAfterFilter 
    LogInt 
    FalseStringFilter 
    FalseStringFilterPartialMatch 
    LogStringAfterFilter 
    LogString 
    LogCheck 
" | awk NF | ( while read l; do ./tests --unsafe-lwtrace 1 $l > out/out_$l.txt 2>out/err_$l.txt && echo done $l & done ; wait )


grep . out/out_*.txt
grep . out/err_*.txt --color=always



