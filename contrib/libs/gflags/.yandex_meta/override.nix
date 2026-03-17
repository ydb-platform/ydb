self: super: with self; rec {
  pname = "gflags";
  version = "2.2.2";

  src = fetchFromGitHub {
    owner = "gflags";
    repo = "gflags";
    rev = "v${version}";
    sha256 = "147i3md3nxkjlrccqg4mq1kyzc7yrhvqv5902iibc7znkvzdvlp0";
  };
}
