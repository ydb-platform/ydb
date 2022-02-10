#!/usr/bin/env perl 
 
# 
# Takes profile file as an input and prints out annotated disassmebly 
# Usage: 
#    ./annotate_profile.pl <binary_name> <profile_name> 
# 
 
 
# Function to draw bar of the specified length filled up to specified length 
sub DRAW_BAR($$) { 
    my ($length, $filled) = @_; 
    my $bar = ""; 
    --$filled; 
    while ($filled > 0) { 
        $bar = $bar . "X"; 
        $length--; 
        $filled--; 
    } 
    while ($length > 0) { 
        $bar = $bar . " "; 
        $length--; 
    } 
    return $bar; 
} 
 
my $curFunc = ""; 
my $curModule = ""; 
my $allHits = 0; 
my %moduleHits; 
my %funcModule; 
my %funcHits; 
my %funcHottestCount; 
my %funcStart; 
my %funcEnd; 
my %funcNames; 
my %funcBaseAddrs; 
my %funcSizes; 
my %addrHits; 
my %addrFunc; 
my %moduleBaseAddr; 
my @funcSortByAddr; 
my %demangledNames; 
my %srcLineHits; 
my %srcFileHits; 
 
# Demagles C++ function name 
sub DEMANGLE($) { 
    my ($name) = @_; 
    if (exists $demangledNames{$name}) { 
        return $demangledNames{$name}; 
    } 
    if ($name =~ /^_Z/) { 
        my $cmd = "c++filt -p \'$name\' |"; 
        open(my $RES, $cmd ) || die "No c++filt"; 
        my $demangled_name = <$RES>; 
        chomp($demangled_name); 
        close $RES; 
        if (length($demangled_name) !=0) { 
            $name = $demangled_name; 
        } 
    } 
    return $name; 
} 
 
# Saves function info 
sub AddFunc($$$$$) 
{ 
    my ($func, $bin_file, $baseAddr, $size, $name) = @_; 
    $funcModule{$func} = $bin_file; 
    $funcBaseAddrs{$func} = $baseAddr; 
    # A function with the same base address can be mentioned multiple times with different sizes (0, and non-0, WTF??) 
    if ((! exists $funcSizes{$func}) || ($funcSizes{$func} < $size)) { 
        $funcSizes{$func} = $size; 
    } 
    $funcNames{$func} = $name; 
    $funcStart{$func} = $func; 
#    printf "%08x\t%08x\t%016x\t%s\t%s\n", 
#        $funcBaseAddrs{$func}, $funcSizes{$func}, $moduleBaseAddr, $funcModule{$func}, $funcNames{$func}; 
} 
 
# Reads list of all functions in a module 
sub ReadFunctionList($$) { 
    my ($bin_file, $moduleBaseAddr) = @_; 
    if (! -e $bin_file) { 
        return; 
    } 
    my $readelf_cmd = "readelf -W -s $bin_file |"; 
#    print "$readelf_cmd\n"; 
    my $IN_FILE; 
    open($IN_FILE, $readelf_cmd) || die "couldn't open the file!"; 
    while (my $line = <$IN_FILE>) { 
        chomp($line); 
        # "    33: 00000000000a0fc0   433 FUNC    GLOBAL DEFAULT   10 getipnodebyaddr@@FBSD_1.0" 
        if ($line =~ m/^\s*\d+:\s+([0-9a-fA-F]+)\s+(\d+)\s+FUNC\s+\w+\s+DEFAULT\s+\d+\s+(.*)$/) { 
            # Read function info 
            my $name = $3; 
            my $baseAddr = hex($1) + $moduleBaseAddr; 
            my $func = $baseAddr; 
            my $size = $2; 
            AddFunc($func, $bin_file, $baseAddr, $size, $name); 
        } 
    } 
    close($IN_FILE); 
    @funcSortByAddr = sort {$funcBaseAddrs{$a} <=> $funcBaseAddrs{$b} } keys %funcBaseAddrs; 
#    printf "%016x\t%s\t%d\n", $moduleBaseAddr, $bin_file, $#funcSortByAddr+1; 
} 
 
# Reads the profile and attributes address hits to the functions 
sub ReadSamples() { 
    # First pass saves all samples in a hash-table 
    my $samples_file = $ARGV[1]; 
    my $IN_FILE; 
    open($IN_FILE, $samples_file)|| die "couldn't open the file!"; 
    my $curFuncInd = 0; 
    my $curFunc = 0; 
    my $curFuncBegin = 0; 
    my $curFuncEnd = 0; 
    my $curModule = ""; 
    my $curModuleBase = 0; 
    my $read_samples = 0; 
    my $samplesStarted = 0; 
    while (my $line = <$IN_FILE>) { 
        chomp($line); 
 
        if ($line =~ m/^samples:\s+(\d+)\s+unique:\s+(\d+)\s+dropped:\s+(\d+)\s+searchskips:\s+(\d+)$/) { 
            $total_samples = $1; 
            $unique_samples = $2; 
            $dropped_samples = $3; 
            $search_skips = $4; 
            next; 
        } 
 
        if ($line =~ m/^Samples:$/) { 
            $samplesStarted = 1; 
            next; 
        } elsif (!$samplesStarted) { 
            print "$line\n"; 
            next; 
        } 
 
#        print "$line\n"; 
        if  ($line =~ m/^Func\t\d+/) { 
            # "Func 2073  0x803323000 0x803332fd0 /lib/libthr.so.3 pthread_cond_init" 
            my @cols = split(/\t/, $line); 
            $curModule = $cols[4]; 
            $curModuleBase = hex($cols[2]); 
            if (0x400000 == $curModuleBase) { 
                $curModuleBase = 0; 
            } 
            $curFunc = hex($cols[3]); 
            if (! exists $moduleBaseAddr{$curModule}) { 
                $moduleBaseAddr{$curModule} = $curModuleBase; 
                ReadFunctionList($curModule, $curModuleBase); 
            } 
            if (! exists $funcNames{$curFunc}) { 
                my $name = sprintf("unknown_0x%08x", $curFunc); 
                AddFunc($curFunc, $curModule, $curFunc, 0, $name); 
            } 
        } elsif ($line =~ m/^\d+\t0x([0-9,a-f,A-F]+)\t(\d+)/) { 
            # Read one sample for the current function 
            $read_samples++; 
            my $addr = hex($1); 
#            print "$addr\n"; 
            if ($addr >= $curFuncEnd) { 
                # Find the function the current address belongs to 
                while ($curFuncInd <= $#funcSortByAddr) { 
                    my $f = $funcSortByAddr[$curFuncInd]; 
                    my $begin = $funcBaseAddrs{$f}; 
                    my $end = $funcBaseAddrs{$f} + $funcSizes{$f}; 
                    if ($begin <= $addr and $addr < $end) { 
                        $curFunc = $f; 
                        $funcStart{$curFunc} = $addr; 
                        $curFuncBegin = $begin; 
                        $curFuncEnd = $end; 
                        last; 
                    } elsif ($addr < $begin) { 
#                        printf "X3: func:%08x\tname:%s\tbase:%08x\tsize:%08x\t%s\nline:%s\n", 
#                            $curFunc, $funcNames{$curFunc}, $funcBaseAddrs{$curFunc}, $funcSizes{$curFunc}, $curModule, $line; 
                        last; 
                    } 
                    ++$curFuncInd; 
                } 
            } 
 
            $funcHits{$curFunc} += $2; 
            if ($funcHottestCount{$curFunc} < $2) { 
                $funcHottestCount{$curFunc} = $2; 
            } 
            $addrHits{$addr} = $2; 
            $addrFunc{$addr} = $curFunc; 
            $funcEnd{$curFunc} = $addr; 
            $allHits += $2; 
	    $moduleHits{$curModule} += $2; 
 
#    	    printf "%08x\t%08x\t%08x\t%08x\t%s\n", $addr, $curFunc, $curFuncBegin, $curFuncEnd, $funcNames{$curFunc}; 
        } 
    } 
    close($IN_FILE); 
     
    printf "\nsamples: %d    unique: %d   dropped: %d   searchskips: %d\n",  $total_samples, $unique_samples, $dropped_samples, $search_skips; 
    if ($read_samples != $unique_samples) { 
        printf "\n-----------------------------------------------------------------------------------------------------\n"; 
        printf "!!!!WARNING: read %d samples, expected %d samples, profiling results might be not acqurate!!!!", $read_samples, $unique_samples; 
        printf "\n-----------------------------------------------------------------------------------------------------\n"; 
    } 
} 
 
# Dumps module stats 
sub DumpModules() { 
    # Sort functions by hit counts and dump the list 
    my @modules = sort {$a <=> $b } keys %moduleHits; 
    for (my $i = 0; $i <= $#modules; ++$i) { 
        my $m = $modules[$i]; 
        my $cnt = $moduleHits{$m}; 
        my $perc = 100.0 * $cnt / $allHits; 
        printf "%12d\t%6.2f%% |%s  %s\n", $cnt, $perc, DRAW_BAR(20, 20*$cnt/$allHits), $m; 
    } 
} 
 
# Dumps top N hot functions 
sub DumpHotFunc($) { 
    my ($maxCnt) = @_; 
    # Sort functions by hit counts and dump the list 
    my @hotFunc = sort {$funcHits{$b} <=> $funcHits{$a} } keys %funcHits; 
#    print $#hotFunc; 
    for (my $i = 0; $i <= $#hotFunc && $i < $maxCnt; ++$i) { 
        my $f = $hotFunc[$i]; 
        my $cnt = $funcHits{$f}; 
        my $perc = 100.0 * $cnt / $allHits; 
        printf "%12d\t%6.2f%% |%s  %s\n", $cnt, $perc, DRAW_BAR(20, 20*$cnt/$allHits), DEMANGLE($funcNames{$f}); 
    } 
} 
 
# Dumps top N hotspots (hot addresses) 
sub DumpHotSpots($) { 
    my ($maxCnt) = @_; 
    # Sort addresses by hit counts and dump the list 
    my @hotSpots = sort {$addrHits{$b} <=> $addrHits{$a} } keys %addrHits; 
    for (my $i = 0; $i <= $#hotSpots && $i < $maxCnt; ++$i) { 
        my $s = $hotSpots[$i]; 
        my $cnt = $addrHits{$s}; 
        my $perc = 100.0 * $cnt / $allHits; 
        my $f = $addrFunc{$s}; 
        my $fname = $funcNames{$f}; 
        printf "%12d\t%6.2f%% |%s  0x%016x\t%s + 0x%x\n", 
            $cnt, $perc, DRAW_BAR(20, 20*$cnt/$allHits), $s, DEMANGLE($fname), $s - $funcBaseAddrs{$f}; 
    } 
} 
 
# Adds hit informations to a disassembly line 
sub ANNOTATE_DISASSM($$$$) { 
    my ($address, $disassm, $max_hit_count, $func_hit_count) = @_; 
    my $hit_count = $addrHits{$address}; 
    my $perc = sprintf("% 7.2f%%", 100*$hit_count/$func_hit_count); 
    $address = sprintf("% 8x", $address); 
    print $address . " " . $hit_count . "\t" . $perc . " |" . 
        DRAW_BAR(20, 20*$hit_count/$max_hit_count) . "\t" . $disassm . "\n"; 
} 
 
# Dumps annotated disassembly of the specified function (actually not the whole function but 
# just the addresses between the first and last hit) 
sub DumpDisasm($) { 
    my ($name) = @_; 
    if (exists $funcStart{$name} && exists $funcEnd{$name} && $funcStart{$name}!=0) { 
        my $module = $funcModule{$name}; 
        my $modBase = $moduleBaseAddr{$module}; 
        my $start_address = $funcStart{$name} - $modBase; 
        my $stop_address = $funcEnd{$name} - $modBase + 1; 
#        print " " . $funcStart{$name} . " " . $funcEnd{$name} . " $modBase ---"; 
        my $max_hit_count = $funcHits{$name}; 
        my $objdump_cmd = "objdump -C -d -l --start-address=" . $start_address . 
            " --stop-address=" . $stop_address . " " . $module . " |"; 
        if ($stop_address - $start_address < 10000000) { # don't try to disaassemble more than 10MB, because most likely it's a bug 
#        print STDERR $objdump_cmd . "\n"; 
        open(my $OBJDUMP, $objdump_cmd) || die "No objdump"; 
        my $srcLine = "func# ". $name; 
        my $srcFile = $module; 
        while (my $objdump_line = <$OBJDUMP>) { 
            # filter disassembly lines 
            if ($objdump_line =~ /^Disassembly of section/) { 
            } elsif ($objdump_line =~ m/^\s*([0-9,a-f,A-F]+):\s*(.*)/) { 
                my $addr = hex($1); 
                my $hit_count = $addrHits{$addr}; 
                if ($hit_count > 0) { 
                    $srcLineHits{$srcLine} += $hit_count; 
                    $srcFileHits{$srcFile} += $hit_count; 
                } 
                ANNOTATE_DISASSM($addr + $modBase, $2, $funcHottestCount{$name}, $max_hit_count); 
            } elsif ($objdump_line =~ m/^(\/.*):(\d+)$/) { 
                $srcLine = $objdump_line; 
                $srcFile = $1; 
                chomp($srcLine); 
                print $objdump_line; 
            } else { 
                print $objdump_line; 
            } 
        } 
        close $OBJDUMP; 
        } 
    } 
} 
 
# Dumps disassemlby for top N hot functions 
sub DumpFuncDissasm($) { 
    (my $maxCnt) = @_; 
    my @funcs = sort {$funcHits{$b} <=> $funcHits{$a} } keys %funcHits; 
    print $#funcs . "\n"; 
    for (my $i = 0; $i <= $#funcs && $i < $maxCnt; ++$i) { 
        my $f = $funcs[$i]; 
        print "\n--------------------------------------------------------------------------------------------------------------\n"; 
        printf "hits:%d\t%7.2f%%\tbase:%08x\tstart:%08x\tend:%08x\t%s\n", 
            $funcHits{$f}, 100*$funcHits{$f}/$allHits, $funcBaseAddrs{$f}, $funcStart{$f}, $funcEnd{$f}, DEMANGLE($funcNames{$f}); 
        print "--------------------------------------------------------------------------------------------------------------\n"; 
        DumpDisasm($f); 
    } 
} 
 
sub DumpSrcFiles($) { 
    (my $maxCnt) = @_; 
    my @srcFiles = sort {$srcFileHits{$b} <=> $srcFileHits{$a} } keys %srcFileHits; 
    for (my $i = 0; $i <= $#srcFiles && $i < $maxCnt; ++$i) { 
        my $f = $srcFiles[$i]; 
        my $cnt = $srcFileHits{$f}; 
        printf "%12d\t%6.2f%% |%s %s\n", $cnt, 100*$cnt/$allHits, DRAW_BAR(20, 20*$cnt/$allHits), $f; 
    } 
} 
 
sub DumpSrcLines($) { 
    (my $maxCnt) = @_; 
    my @srcLines = sort {$srcLineHits{$b} <=> $srcLineHits{$a} } keys %srcLineHits; 
    for (my $i = 0; $i <= $#srcLines && $i < $maxCnt; ++$i) { 
        my $l = $srcLines[$i]; 
        my $cnt = $srcLineHits{$l}; 
        printf "%12d\t%6.2f%% |%s %s\n", $cnt, 100*$cnt/$allHits, DRAW_BAR(20, 20*$cnt/$allHits), $l; 
    } 
} 
 
ReadFunctionList($ARGV[0], 0); 
ReadSamples(); 
print "\nModules:\n"; 
DumpModules(); 
print "\nHot functions:\n"; 
DumpHotFunc(100); 
print "\nHotspots:\n"; 
DumpHotSpots(100); 
DumpFuncDissasm(100); 
print "\nHot src files:\n"; 
DumpSrcFiles(100); 
print "\nHot src lines:\n"; 
DumpSrcLines(100); 
 
# my @funcs = sort {$funcBaseAddrs{$a} <=> $funcBaseAddrs{$b} } keys %funcHits; 
#    printf "%d\n", $#funcs; 
#    for (my $i = 0; $i <= $#funcs; ++$i) { 
#        my $f = $funcs[$i]; 
#        printf "%s\t%d\tbase:%08x\tstart:%08x\tend:%08x\t%s\n", 
#            $funcNames{$f}, $funcHits{$f}, $funcBaseAddrs{$f}, $funcStart{$f}, $funcEnd{$f}, $funcModule{$f}; 
#        #DumpDisasm($f); 
#    } 
