self: super: with self; {
  boost_random = stdenv.mkDerivation rec {
    pname = "boost_random";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "random";
      rev = "boost-${version}";
      hash = "sha256-doKJOuC2biQAyDX96eAPoBq3cjjns95hB56moN3uweI=";
    };
  };
}
