self: super: with self; {
  boost_serialization = stdenv.mkDerivation rec {
    pname = "boost_serialization";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "serialization";
      rev = "boost-${version}";
      hash = "sha256-JaYkZ4ACmmaiUkNzdPSZWzWRzb9KCZYgZtgLjpiektM=";
    };
  };
}
