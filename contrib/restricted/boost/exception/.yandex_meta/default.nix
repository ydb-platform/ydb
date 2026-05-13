self: super: with self; {
  boost_exception = stdenv.mkDerivation rec {
    pname = "boost_exception";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "exception";
      rev = "boost-${version}";
      hash = "sha256-QFbbUzWoZSOxQ7HsncJ3sfN9RbytpTmt3ZNkt+JIlf4=";
    };
  };
}
