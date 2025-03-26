self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-u1KPJNJtAOPOfHmrdw7WQVRmuTWQg6KtLhkINr2L8JM=";
    };
  };
}
