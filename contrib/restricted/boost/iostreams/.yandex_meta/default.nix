self: super: with self; {
  boost_iostreams = stdenv.mkDerivation rec {
    pname = "boost_iostreams";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iostreams";
      rev = "boost-${version}";
      hash = "sha256-6CIySEbQ00iEnQ/dtkNlq1SJVShOUxm68ylY9E8bPPs=";
    };
  };
}
