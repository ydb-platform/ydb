self: super: with self; {
  boost_utility = stdenv.mkDerivation rec {
    pname = "boost_utility";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "utility";
      rev = "boost-${version}";
      hash = "sha256-hRw1rmJlFCm5/NZoyIhm5SqST5K7fhCidsq+JlWl5zI=";
    };
  };
}
