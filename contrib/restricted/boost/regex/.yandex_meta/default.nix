self: super: with self; {
  boost_regex = stdenv.mkDerivation rec {
    pname = "boost_regex";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "regex";
      rev = "boost-${version}";
      hash = "sha256-Ju4R36bLmkH98MUp0fq774TpYnv43NzqRZdfSYHUv/k=";
    };
  };
}
