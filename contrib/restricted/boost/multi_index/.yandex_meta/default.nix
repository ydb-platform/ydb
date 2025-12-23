self: super: with self; {
  boost_multi_index = stdenv.mkDerivation rec {
    pname = "boost_multi_index";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multi_index";
      rev = "boost-${version}";
      hash = "sha256-qJ9m7Ak1kI+obRGhYbSqHGjPR7eyq0iL3ndRIqZA1E8=";
    };
  };
}
