self: super: with self; {
  boost_container = stdenv.mkDerivation rec {
    pname = "boost_container";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container";
      rev = "boost-${version}";
      hash = "sha256-8kSlNjczI6GjwDdYnnrc7au2DTpw7HmVxhK64TJb/eM=";
    };
  };
}
