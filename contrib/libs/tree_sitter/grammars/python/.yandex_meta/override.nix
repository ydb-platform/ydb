self: super: with self; rec {
  pname = "tree-sitter-python";
  version = "0.23.6";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-71Od4sUsxGEvTwmXX8hBvzqD55hnXkVJublrhp1GICg=";
  };
}
