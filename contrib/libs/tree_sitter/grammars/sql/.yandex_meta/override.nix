self: super: with self; rec {
  pname = "tree-sitter-sql";
  version = "0.3.11";

  src = fetchFromGitHub {
    owner = "DerekStride";
    hash = "sha256-efeDAUgCwV9UBXbLyZ1a4Rwcvr/+wke8IzkxRUQnddM=";
    repo = "${pname}";
    rev = "v${version}";
  };
  generated_src = fetchzip {
      url = "https://github.com/DerekStride/tree-sitter-sql/archive/refs/heads/gh-pages.tar.gz";
      hash = "sha256-UmGvjtN0Pi7uH8+Sb6JbvdV60gow7KQCbDRcKo3nMYw=";
    };
  postUnpack = ''
      cp -r $generated_src/src/parser.c source/src
    '';
}
