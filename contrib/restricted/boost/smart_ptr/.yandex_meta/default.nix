self: super: with self; {
  boost_smart_ptr = stdenv.mkDerivation rec {
    pname = "boost_smart_ptr";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "smart_ptr";
      rev = "boost-${version}";
      hash = "sha256-JbqPxNwjwS1edYyCGhmgsM8miTQzPcF59P+ib6k3zIg=";
    };
  };
}
