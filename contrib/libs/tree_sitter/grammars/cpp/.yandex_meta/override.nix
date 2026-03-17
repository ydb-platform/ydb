self: super: with self; rec {
  pname = "tree-sitter-cpp";
  version = "0.20.5";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = pname;
    rev = "v${version}";
    hash = "sha256-CdNCVDMAmeJrHgPb2JLxFHj/tHnUYC8flmxj+UaVXTo=";
  };
}
