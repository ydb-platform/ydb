self: super: with self; rec {
  pname = "tree-sitter-c-sharp";
  version = "0.23.1";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = pname;
    rev = "v${version}";
    hash = "sha256-weH0nyLpvVK/OpgvOjTuJdH2Hm4a1wVshHmhUdFq3XA=";
  };
}
