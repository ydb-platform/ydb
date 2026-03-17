self: super: with self; {
  boost_heap = stdenv.mkDerivation rec {
    pname = "boost_heap";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "heap";
      rev = "boost-${version}";
      hash = "sha256-qz+I1aUSUMD83xHavQK2bbeUAQdNqcz7s58ZzYui7WI=";
    };
  };
}
