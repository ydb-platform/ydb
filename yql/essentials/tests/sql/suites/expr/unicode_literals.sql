pragma WarnUntypedStringLiterals;
pragma UnicodeLiterals;
$f = ()->{
    return (
	    "a"s,
	    "b"u,
	    "c");
};

select $f();

pragma DisableWarnUntypedStringLiterals;
pragma DisableUnicodeLiterals;
$g = ()->{
    return (
	    "a"s,
	    "b"u,
	    "c");
};

select $g();

