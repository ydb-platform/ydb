pragma WarnUntypedStringLiterals;
pragma UnicodeLiterals;
$f = ()->{
    return (
	    "a"s,
	    "b"b,
	    "c"t,
	    "d"u,
	    "e");
};

select $f();

pragma DisableWarnUntypedStringLiterals;
pragma DisableUnicodeLiterals;
$g = ()->{
    return (
	    "a"s,
	    "b"b,
	    "c"t,
	    "d"u,
	    "e");
};

select $g();

