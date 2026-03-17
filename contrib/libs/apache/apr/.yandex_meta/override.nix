self: super: with self; rec {
  name = "apr-${version}";
  version = "1.7.0";

  src = fetchurl {
    url = "mirror://apache/apr/${name}.tar.bz2";
    hash = "sha256-4uFI8LLpm45cbKoJ9tT7TdPoP3RKpyqVL5T1oUQ29+o=";
  };

  patches = [];
}
