pkgs: attrs: with pkgs; with attrs; rec {
  version = "4.15.2";

  src = fetchurl {
    url = "https://github.com/Z3Prover/z3/archive/refs/tags/z3-${version}.tar.gz";
    hash = "sha256-NIa/WzWxhZgcqwsKgfhwVHZIocpDMIWqea/RfESVl1E=";
  };

  patches = [
    ./001-fix-dll-open.patch
  ];

  sourceRoot = "z3-z3-${version}";
}
