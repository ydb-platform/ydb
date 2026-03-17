// included from constructor for function_signatures() in src/stan/lang/ast.hpp

std::vector<bare_expr_type> bare_types;
bare_types.push_back(int_type());
bare_types.push_back(double_type());
bare_types.push_back(vector_type());
bare_types.push_back(row_vector_type());
bare_types.push_back(matrix_type());

std::vector<bare_expr_type> vector_types;
vector_types.push_back(double_type());  // scalar
vector_types.push_back(bare_array_type(double_type(), 1));  // std vector
vector_types.push_back(vector_type());  // Eigen vector
vector_types.push_back(row_vector_type());  // Eigen row vector

std::vector<bare_expr_type> int_vector_types;
int_vector_types.push_back(int_type());  // scalar
int_vector_types.push_back(bare_array_type(int_type()));  // std vector

std::vector<bare_expr_type> primitive_types;
primitive_types.push_back(int_type());
primitive_types.push_back(double_type());

std::vector<bare_expr_type> all_vector_types;
all_vector_types.push_back(bare_expr_type(double_type()));  // scalar
all_vector_types.push_back(bare_expr_type(bare_array_type(double_type())));  // std vector
all_vector_types.push_back(bare_expr_type(vector_type()));  // Eigen vector
all_vector_types.push_back(bare_expr_type(row_vector_type()));  // Eigen row vector
all_vector_types.push_back(bare_expr_type(int_type()));  // scalar
all_vector_types.push_back(bare_expr_type(bare_array_type(int_type())));  // std vector

add("abs", bare_expr_type(int_type()), bare_expr_type(int_type()));
add("abs", bare_expr_type(double_type()), bare_expr_type(double_type()));
add_unary_vectorized("acos");
add_unary_vectorized("acosh");
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("add", bare_types[i], bare_types[i], bare_types[i]);
 }
add("add", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("add", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add("add", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("add", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("add", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("add", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("add", bare_types[i], bare_types[i]);
 }
for (size_t i = 1; i < 8; ++i) {
  add("append_array", bare_expr_type(bare_array_type(int_type(), i)), bare_expr_type(bare_array_type(int_type(), i)), bare_expr_type(bare_array_type(int_type(), i)));
  add("append_array", bare_expr_type(bare_array_type(double_type(), i)), bare_expr_type(bare_array_type(double_type(), i)), bare_expr_type(bare_array_type(double_type(), i)));
  add("append_array", bare_expr_type(bare_array_type(vector_type(), i)), bare_expr_type(bare_array_type(vector_type(), i)), bare_expr_type(bare_array_type(vector_type(), i)));
  add("append_array", bare_expr_type(bare_array_type(row_vector_type(), i)), bare_expr_type(bare_array_type(row_vector_type(), i)), bare_expr_type(bare_array_type(row_vector_type(), i)));
  add("append_array", bare_expr_type(bare_array_type(matrix_type(), i)), bare_expr_type(bare_array_type(matrix_type(), i)), bare_expr_type(bare_array_type(matrix_type(), i)));
 }
add_unary_vectorized("asin");
add_unary_vectorized("asinh");
add_unary_vectorized("atan");
add_binary("atan2");
add_unary_vectorized("atanh");
for (size_t i = 0; i < int_vector_types.size(); ++i)
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("bernoulli_ccdf_log", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_cdf", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_cdf_log", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_log", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_lccdf", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_lcdf", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_lpmf", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
  }
for (const auto& t : all_vector_types) {
  add("bernoulli_rng", rng_return_type<int_type>(t), t);
 }
for (const auto& t : all_vector_types) {
  add("bernoulli_logit_rng", rng_return_type<int_type>(t), t);
 }
for (size_t i = 0; i < int_vector_types.size(); ++i)
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("bernoulli_logit_log", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
    add("bernoulli_logit_lpmf", bare_expr_type(double_type()), int_vector_types[i],
	vector_types[j]);
  }
add("bernoulli_logit_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()));
add("bernoulli_logit_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(vector_type()));
add("bessel_first_kind", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add("bessel_second_kind", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
for (size_t i = 0; i < int_vector_types.size(); i++)
  for (size_t j = 0; j < int_vector_types.size(); j++)
    for (size_t k = 0; k < vector_types.size(); k++)
      for (size_t l = 0; l < vector_types.size(); l++) {
        add("beta_binomial_ccdf_log", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
        add("beta_binomial_cdf", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
        add("beta_binomial_cdf_log", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
        add("beta_binomial_log", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
        add("beta_binomial_lccdf", bare_expr_type(double_type()), int_vector_types[i],
	    int_vector_types[j], vector_types[k], vector_types[l]);
        add("beta_binomial_lcdf", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
        add("beta_binomial_lpmf", bare_expr_type(double_type()),
            int_vector_types[i], int_vector_types[j],
	    vector_types[k], vector_types[l]);
      }
for (const auto& t : int_vector_types) {
  for (const auto& u : all_vector_types) {
    for (const auto& v : all_vector_types) {
      add("beta_binomial_rng", rng_return_type<int_type>(t, u, v), t, u, v);
    }
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("beta_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("beta_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("beta_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("beta_log", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("beta_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("beta_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("beta_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("beta_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
for (const auto& t : vector_types) {
  for (const auto& u : vector_types) {
    for (const auto& v : all_vector_types) {
      add("beta_proportion_ccdf_log", bare_expr_type(double_type()), t, u, v);
      add("beta_proportion_cdf_log", bare_expr_type(double_type()), t, u, v);
      add("beta_proportion_log", bare_expr_type(double_type()), t, u, v);
      add("beta_proportion_lccdf", bare_expr_type(double_type()), t, u, v);
      add("beta_proportion_lcdf", bare_expr_type(double_type()), t, u, v);
      add("beta_proportion_lpdf", bare_expr_type(double_type()), t, u, v);
    }
  }
 }
for (const auto& t : vector_types) {
  for (const auto& u : all_vector_types) {
    add("beta_proportion_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("binary_log_loss", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  for (size_t j = 0; j < int_vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("binomial_ccdf_log", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_cdf", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_cdf_log", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_log", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_lccdf", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_lcdf", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_lpmf", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
    }
  }
}
for (const auto& t : int_vector_types) {
  for (const auto& u : all_vector_types) {
    add("binomial_rng", rng_return_type<int_type>(t, u), t, u);
  }
 }
add_binary("binomial_coefficient_log");
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  for (size_t j = 0; j < int_vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("binomial_logit_log", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
      add("binomial_logit_lpmf", bare_expr_type(double_type()),
          int_vector_types[i], int_vector_types[j], vector_types[k]);
    }
  }
 }
add("block", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  add("categorical_log", bare_expr_type(double_type()), int_vector_types[i], bare_expr_type(vector_type()));
  add("categorical_logit_log", bare_expr_type(double_type()), int_vector_types[i],
      bare_expr_type(vector_type()));
  add("categorical_lpmf", bare_expr_type(double_type()), int_vector_types[i], bare_expr_type(vector_type()));
  add("categorical_logit_lpmf", bare_expr_type(double_type()), int_vector_types[i],
      bare_expr_type(vector_type()));
 }
add("categorical_rng", bare_expr_type(int_type()), bare_expr_type(vector_type()));
add("categorical_logit_rng", bare_expr_type(int_type()), bare_expr_type(vector_type()));
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("cauchy_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("cauchy_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("cauchy_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("cauchy_log", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("cauchy_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("cauchy_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("cauchy_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("cauchy_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("append_col", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("append_col", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("append_col", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("append_col", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("append_col", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("append_col", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("append_col", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add_unary_vectorized("cbrt");
add_unary_vectorized("ceil");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("chi_square_ccdf_log", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_cdf", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_cdf_log", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_log", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_lccdf", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_lcdf", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
    add("chi_square_lpdf", bare_expr_type(double_type()), vector_types[i],
        vector_types[j]);
  }
 }
for (const auto& t : all_vector_types) {
  add("chi_square_rng", rng_return_type<double_type>(t), t);
 }
add("cholesky_decompose", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("choose", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("col", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()));
add("cols", bare_expr_type(int_type()), bare_expr_type(vector_type()));
add("cols", bare_expr_type(int_type()), bare_expr_type(row_vector_type()));
add("cols", bare_expr_type(int_type()), bare_expr_type(matrix_type()));
add("columns_dot_product", bare_expr_type(row_vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("columns_dot_product", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("columns_dot_product", bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("columns_dot_self", bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("columns_dot_self", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("columns_dot_self", bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("cos");
add_unary_vectorized("cosh");
add("cov_exp_quad", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("cov_exp_quad", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("cov_exp_quad", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("cov_exp_quad", bare_expr_type(matrix_type()),
    bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(bare_array_type(double_type(), 1)),bare_expr_type(double_type()), bare_expr_type(double_type()));
add("cov_exp_quad", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("cov_exp_quad", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("crossprod", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("csr_matrix_times_vector", bare_expr_type(vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()),
    bare_expr_type(vector_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("csr_to_dense_matrix", bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()),
    bare_expr_type(vector_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)));
add("csr_extract_w", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("csr_extract_v", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(matrix_type()));
add("csr_extract_u", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(matrix_type()));
add("cumulative_sum", bare_expr_type(bare_array_type(double_type(), 1)),
    bare_expr_type(bare_array_type(double_type(), 1)));
add("cumulative_sum", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("cumulative_sum", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("determinant", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("diag_matrix", bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("diag_post_multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("diag_post_multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()));
add("diag_pre_multiply", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("diag_pre_multiply", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("diagonal", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("digamma");

add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(int_type()));
add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(double_type()));
add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(row_vector_type()));
add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(matrix_type()));

for (size_t i = 0; i < 8; ++i) {
  add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), i + 1)));
  add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(double_type(), i + 1)));
  add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(vector_type(), i + 1)));
  add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(row_vector_type(), i + 1)));
  add("dims", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(matrix_type(), i + 1)));
 }

add("dirichlet_log", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("dirichlet_lpdf", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("dirichlet_rng", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("distance", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("distance", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("distance", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("distance", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("divide", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("divide", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("divide", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("divide", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add("divide", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("dot_product", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("dot_product", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("dot_product", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("dot_product", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("dot_product", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)),
    bare_expr_type(bare_array_type(double_type(), 1)));
add("dot_self", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("dot_self", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("double_exponential_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_cdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("double_exponential_lpdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("double_exponential_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add_nullary("e");
add("eigenvalues_sym", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("eigenvectors_sym", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("qr_Q", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("qr_R", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("qr_thin_Q", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("qr_thin_R", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("elt_divide", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("elt_divide", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("elt_divide", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("elt_divide", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("elt_divide", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add("elt_divide", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("elt_divide", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("elt_divide", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("elt_divide", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("elt_multiply", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("elt_multiply", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("elt_multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("erf");
add_unary_vectorized("erfc");
add_unary_vectorized("exp");
add_unary_vectorized("exp2");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      for (size_t l = 0; l < vector_types.size(); ++l) {
        add("exp_mod_normal_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_cdf", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_cdf_log", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_log", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_lccdf", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_lcdf", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
        add("exp_mod_normal_lpdf", bare_expr_type(double_type()), vector_types[i],
	    vector_types[j], vector_types[k], vector_types[l]);
      }
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    for (const auto& v : all_vector_types) {
      add("exp_mod_normal_rng", rng_return_type<double_type>(t, u, v), t, u, v);
    }
  }
 }
add_unary_vectorized("expm1");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("exponential_ccdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_cdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_lccdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_lcdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("exponential_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
  }
 }
for (const auto& t : all_vector_types) {
  add("exponential_rng", rng_return_type<double_type>(t), t);
 }
add_unary_vectorized("fabs");
add("falling_factorial", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(int_type()));
add("falling_factorial", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add_binary("fdim");
add_unary_vectorized("floor");
add_ternary("fma");
add_binary("fmax");
add_binary("fmin");
add_binary("fmod");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("frechet_ccdf_log", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_cdf", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_cdf_log", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_log", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_lccdf", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_lcdf", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
      add("frechet_lpdf", bare_expr_type(double_type()), vector_types[i],
          vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("frechet_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("gamma_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gamma_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("gamma_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gamma_log", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("gamma_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gamma_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gamma_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
    }
  }
 }
add_binary("gamma_p");
add_binary("gamma_q");
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("gamma_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("gaussian_dlm_obs_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()),
    bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("gaussian_dlm_obs_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("gaussian_dlm_obs_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()),
    bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("gaussian_dlm_obs_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add_nullary("get_lp");  // special handling in term_grammar_def
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("gumbel_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gumbel_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("gumbel_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gumbel_log", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
      add("gumbel_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gumbel_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("gumbel_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j],
	  vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("gumbel_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("head", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()));
add("head", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()));
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("head", bare_expr_type(bare_array_type(bare_types[i], 1)),
      bare_expr_type(bare_array_type(bare_types[i], 1)), bare_expr_type(int_type()));
  add("head", bare_expr_type(bare_array_type(bare_types[i], 2)),
      bare_expr_type(bare_array_type(bare_types[i], 2)), bare_expr_type(int_type()));
  add("head", bare_expr_type(bare_array_type(bare_types[i], 3)),
      bare_expr_type(bare_array_type(bare_types[i], 3)), bare_expr_type(int_type()));
 }
add("hypergeometric_log", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("hypergeometric_lpmf", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("hypergeometric_rng", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add_binary("hypot");
add("if_else", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("inc_beta", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("int_step", bare_expr_type(int_type()), bare_expr_type(double_type()));
add("int_step", bare_expr_type(int_type()), bare_expr_type(int_type()));
add_unary_vectorized("inv");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("inv_chi_square_ccdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_cdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_lccdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_lcdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("inv_chi_square_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
  }
 }
for (const auto& t : all_vector_types) {
  add("inv_chi_square_rng", rng_return_type<double_type>(t), t);
 }
add_unary_vectorized("inv_cloglog");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("inv_gamma_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_cdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("inv_gamma_lpdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("inv_gamma_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add_unary_vectorized("inv_logit");
add_unary_vectorized("inv_Phi");
add_unary_vectorized("inv_sqrt");
add_unary_vectorized("inv_square");
add("inv_wishart_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("inv_wishart_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("inv_wishart_rng", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("inverse", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("inverse_spd", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("is_inf", bare_expr_type(int_type()), bare_expr_type(double_type()));
add("is_nan", bare_expr_type(int_type()), bare_expr_type(double_type()));
add_binary("lbeta");
add_binary("lchoose");
add_unary_vectorized("lgamma");
add("lkj_corr_cholesky_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("lkj_corr_cholesky_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("lkj_corr_cholesky_rng", bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add("lkj_corr_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("lkj_corr_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("lkj_corr_rng", bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add("lkj_cov_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("lmgamma", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add_binary("lmultiply");
add_unary_vectorized("log");
add_nullary("log10");
add_unary_vectorized("log10");
add_unary_vectorized("log1m");
add_unary_vectorized("log1m_exp");
add_unary_vectorized("log1m_inv_logit");
add_unary_vectorized("log1p");
add_unary_vectorized("log1p_exp");
add_nullary("log2");
add_unary_vectorized("log2");
add("log_determinant", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add_binary("log_diff_exp");
add_binary("log_falling_factorial");
add_ternary("log_mix");    // adds fn over double, double, double
for (size_t i = 1; i < vector_types.size(); ++i) {
  for (size_t j = 1; j < vector_types.size(); ++j) {
    add("log_mix", bare_expr_type(double_type()), bare_expr_type(vector_types[i]), bare_expr_type(vector_types[j]));
  }
  add("log_mix", bare_expr_type(double_type()), bare_expr_type(vector_types[i]), bare_expr_type(bare_array_type(vector_type(), 1)));
  add("log_mix", bare_expr_type(double_type()), bare_expr_type(vector_types[i]), bare_expr_type(bare_array_type(row_vector_type(), 1)));
 }
add_binary("log_rising_factorial");
add_unary_vectorized("log_inv_logit");
add("log_softmax", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("log_sum_exp", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("log_sum_exp", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("log_sum_exp", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("log_sum_exp", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add_binary("log_sum_exp");
for (size_t i = 0; i < primitive_types.size(); ++i) {
  add("logical_negation", bare_expr_type(int_type()), primitive_types[i]);
  for (size_t j = 0; j < primitive_types.size(); ++j) {
    add("logical_or", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_and", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_eq", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_neq", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_lt", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_lte", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_gt", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
    add("logical_gte", bare_expr_type(int_type()), primitive_types[i],
	primitive_types[j]);
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("logistic_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_cdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("logistic_lpdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("logistic_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add_unary_vectorized("logit");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("lognormal_ccdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_cdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_cdf_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_log", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_lccdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_lcdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
      add("lognormal_lpdf", bare_expr_type(double_type()), vector_types[i],
	  vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("lognormal_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add_nullary("machine_precision");
add("matrix_exp", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("matrix_exp_multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("max", bare_expr_type(int_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("max", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("max", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("max", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("max", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("max", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("mdivide_left", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("mdivide_left", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mdivide_left_spd", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("mdivide_left_spd", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mdivide_left_tri_low", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mdivide_left_tri_low", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("mdivide_right", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("mdivide_right_spd", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mdivide_right_spd", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("mdivide_right", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mdivide_right_tri_low", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("mdivide_right_tri_low", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("mean", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("mean", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("mean", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("mean", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("min", bare_expr_type(int_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("min", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("min", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("min", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("min", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("min", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("minus", bare_expr_type(double_type()), bare_expr_type(double_type()));
add("minus", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("minus", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("minus", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("modified_bessel_first_kind", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add("modified_bessel_second_kind", bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(double_type()));
add("modulus", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("multi_gp_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("multi_gp_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("multi_gp_cholesky_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("multi_gp_cholesky_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
{
  std::vector<bare_expr_type> eigen_vector_types;
  eigen_vector_types.push_back(vector_type());
  eigen_vector_types.push_back(bare_array_type(vector_type()));
  eigen_vector_types.push_back(row_vector_type());
  eigen_vector_types.push_back(bare_array_type(row_vector_type()));
  for (size_t k = 0; k < 4; ++k) {
    for (size_t l = 0; l < 4; ++l) {
      add("multi_normal_cholesky_log", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_normal_cholesky_lpdf", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_normal_log", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_normal_lpdf", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_normal_prec_log", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_normal_prec_lpdf", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_student_t_log", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]), bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));

      add("multi_student_t_lpdf", bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[k]), bare_expr_type(double_type()),
          bare_expr_type(eigen_vector_types[l]), bare_expr_type(matrix_type()));
    }
  }
}
add("multi_normal_rng", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("multi_normal_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(matrix_type()));
add("multi_normal_rng", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("multi_normal_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(matrix_type()));

add("multi_normal_cholesky_rng", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("multi_normal_cholesky_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(matrix_type()));
add("multi_normal_cholesky_rng", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("multi_normal_cholesky_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(matrix_type()));

add("multi_student_t_rng", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("multi_student_t_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(matrix_type()));
add("multi_student_t_rng", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("multi_student_t_rng", bare_expr_type(bare_array_type(vector_type(), 1)), bare_expr_type(double_type()), bare_expr_type(bare_array_type(row_vector_type(), 1)), bare_expr_type(matrix_type()));

add("multinomial_log", bare_expr_type(double_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("multinomial_lpmf", bare_expr_type(double_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("multinomial_rng", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()), bare_expr_type(int_type()));
add("multiply", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("multiply", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("multiply", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add("multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("multiply", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("multiply", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("multiply", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("multiply", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("multiply", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("multiply", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("multiply", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("multiply", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add_binary("multiply_log");
add("multiply_lower_tri_self_transpose", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("neg_binomial_ccdf_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_cdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_cdf_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_lccdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_lcdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_lpmf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);

      add("neg_binomial_2_ccdf_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_cdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_cdf_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_lccdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_lcdf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_lpmf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);

      add("neg_binomial_2_log_log", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
      add("neg_binomial_2_log_lpmf", bare_expr_type(double_type()),
          int_vector_types[i], vector_types[j], vector_types[k]);
    }
  }
}
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("neg_binomial_rng", rng_return_type<int_type>(t, u), t, u);
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("neg_binomial_2_rng", rng_return_type<int_type>(t, u), t, u);
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("neg_binomial_2_log_rng", rng_return_type<int_type>(t, u), t, u);
  }
 }
add("neg_binomial_2_log_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(double_type()));
add("neg_binomial_2_log_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(double_type()));
add_nullary("negative_infinity");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("normal_ccdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_cdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_cdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_lccdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_lcdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("normal_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("normal_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("normal_id_glm_lpdf",
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(matrix_type()),
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(double_type()));
add("normal_id_glm_lpdf",
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(double_type()));
add_nullary("not_a_number");
add("num_elements", bare_expr_type(int_type()), bare_expr_type(matrix_type()));
add("num_elements", bare_expr_type(int_type()), bare_expr_type(vector_type()));
add("num_elements", bare_expr_type(int_type()), bare_expr_type(row_vector_type()));
for (size_t i=1; i < 10; i++) {
  add("num_elements", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(int_type(), i))));
  add("num_elements", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(double_type(), i))));
  add("num_elements", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(matrix_type(), i))));
  add("num_elements", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(row_vector_type(), i))));
  add("num_elements", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(vector_type(), i))));
 }
add("ordered_logistic_log", bare_expr_type(double_type()), bare_expr_type(int_type()),
    bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("ordered_logistic_log", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(vector_type()));
add("ordered_logistic_log", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(bare_array_type(vector_type(), 1)));

add("ordered_logistic_lpmf", bare_expr_type(double_type()), bare_expr_type(int_type()),
    bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("ordered_logistic_lpmf", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(vector_type()));
add("ordered_logistic_lpmf", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(bare_array_type(vector_type(), 1)));

add("ordered_logistic_rng", bare_expr_type(int_type()), bare_expr_type(double_type()),
    bare_expr_type(vector_type()));

add("ordered_probit_log", bare_expr_type(double_type()), bare_expr_type(int_type()),
    bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("ordered_probit_log", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(vector_type()));
add("ordered_probit_log", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()),
    bare_expr_type(bare_array_type(vector_type(), 1)));

add("ordered_probit_lpmf", bare_expr_type(double_type()), bare_expr_type(int_type()),
    bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("ordered_probit_lpmf", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(double_type()),
    bare_expr_type(vector_type()));
add("ordered_probit_lpmf", bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(vector_type(), 1)));

add("ordered_probit_rng", bare_expr_type(int_type()), bare_expr_type(double_type()),
    bare_expr_type(vector_type()));


add_binary("owens_t");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("pareto_ccdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_cdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_cdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_lccdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_lcdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("pareto_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("pareto_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      for (size_t l = 0; l < vector_types.size(); ++l) {
        add("pareto_type_2_ccdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_cdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_cdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_lccdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_lcdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("pareto_type_2_lpdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
      }
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    for (const auto& v : all_vector_types) {
      add("pareto_type_2_rng", rng_return_type<double_type>(t, u, v), t, u, v);
    }
  }
 }
add_unary_vectorized("Phi");
add_unary_vectorized("Phi_approx");
add_nullary("pi");
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("poisson_ccdf_log", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_cdf", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_cdf_log", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_log", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_lccdf", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_lcdf", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_lpmf", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
  }
}
for (const auto& t : all_vector_types) {
  add("poisson_rng", rng_return_type<int_type>(t), t);
 }
for (size_t i = 0; i < int_vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("poisson_log_log", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
    add("poisson_log_lpmf", bare_expr_type(double_type()), int_vector_types[i],
        vector_types[j]);
  }
}
for (const auto& t : all_vector_types) {
  add("poisson_log_rng", rng_return_type<int_type>(t), t);
 }
add("poisson_log_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(double_type()),
    bare_expr_type(vector_type()));
add("poisson_log_glm_lpmf",
    bare_expr_type(double_type()),
    bare_expr_type(bare_array_type(int_type(), 1)),
    bare_expr_type(matrix_type()),
    bare_expr_type(vector_type()),
    bare_expr_type(vector_type()));
add_nullary("positive_infinity");
add_binary("pow");
add("prod", bare_expr_type(int_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("prod", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("prod", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("prod", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("prod", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("quad_form", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("quad_form", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("quad_form_sym", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("quad_form_sym", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("quad_form_diag", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("quad_form_diag", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()));
add("rank", bare_expr_type(int_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(int_type()));
add("rank", bare_expr_type(int_type()), bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(int_type()));
add("rank", bare_expr_type(int_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()));
add("rank", bare_expr_type(int_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()));
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    add("rayleigh_ccdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_cdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_cdf_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_log", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_lccdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_lcdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
    add("rayleigh_lpdf", bare_expr_type(double_type()), vector_types[i], vector_types[j]);
  }
 }
for (const auto& t : all_vector_types) {
  add("rayleigh_rng", rng_return_type<double_type>(t), t);
 }
add("append_row", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("append_row", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("append_row", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()));
add("append_row", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("append_row", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("append_row", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("append_row", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("rep_array", bare_expr_type(bare_array_type(bare_types[i], 1)), bare_types[i], bare_expr_type(int_type()));
  add("rep_array", bare_expr_type(bare_array_type(bare_types[i], 2)), bare_types[i], bare_expr_type(int_type()), bare_expr_type(int_type()));
  add("rep_array", bare_expr_type(bare_array_type(bare_types[i], 3)), bare_types[i],
      bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
  for (size_t j = 1; j <= 3; ++j) {
    add("rep_array", bare_expr_type(bare_array_type(bare_types[i], j + 1)),
        bare_expr_type(bare_array_type(bare_types[i], j)),  bare_expr_type(int_type()));
    add("rep_array", bare_expr_type(bare_array_type(bare_types[i], j + 2)),
        bare_expr_type(bare_array_type(bare_types[i], j)),  bare_expr_type(int_type()), bare_expr_type(int_type()));
    add("rep_array", bare_expr_type(bare_array_type(bare_types[i], j + 3)),
        bare_expr_type(bare_array_type(bare_types[i], j)),  bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
  }
 }
add("rep_matrix", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("rep_matrix", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()));
add("rep_matrix", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()));
add("rep_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(int_type()));
add("rep_vector", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(int_type()));
add("rising_factorial", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(int_type()));
add("rising_factorial", bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add_unary_vectorized("round");
add("row", bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()));
add("rows", bare_expr_type(int_type()), bare_expr_type(vector_type()));
add("rows", bare_expr_type(int_type()), bare_expr_type(row_vector_type()));
add("rows", bare_expr_type(int_type()), bare_expr_type(matrix_type()));
add("rows_dot_product", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("rows_dot_product", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("rows_dot_product", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("rows_dot_self", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("rows_dot_self", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("rows_dot_self", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("scale_matrix_exp_multiply", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("scaled_inv_chi_square_ccdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_cdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_cdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_lccdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_lcdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("scaled_inv_chi_square_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("scaled_inv_chi_square_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("sd", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("sd", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("sd", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("sd", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("segment", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("segment", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("segment", bare_expr_type(bare_array_type(bare_types[i], 1)),
      bare_expr_type(bare_array_type(bare_types[i], 1)), bare_expr_type(int_type()), bare_expr_type(int_type()));
  add("segment", bare_expr_type(bare_array_type(bare_types[i], 2)),
      bare_expr_type(bare_array_type(bare_types[i], 2)), bare_expr_type(int_type()), bare_expr_type(int_type()));
  add("segment", bare_expr_type(bare_array_type(bare_types[i], 3)),
      bare_expr_type(bare_array_type(bare_types[i], 3)), bare_expr_type(int_type()), bare_expr_type(int_type()));
 }
add_unary_vectorized("sin");
add("singular_values", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("sinh");
// size() is polymorphic over arrays, so start i at 1
for (size_t i = 1; i < 8; ++i) {
  add("size", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(int_type(), i))));
  add("size", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(double_type(), i))));
  add("size", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(vector_type(), i))));
  add("size", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(row_vector_type(), i))));
  add("size", bare_expr_type(int_type()), bare_expr_type(bare_array_type(bare_array_type(matrix_type(), i))));
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      for (size_t l = 0; l < vector_types.size(); ++l) {
        add("skew_normal_ccdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_cdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_cdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_lccdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_lcdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("skew_normal_lpdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
      }
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    for (const auto& v : all_vector_types) {
      add("skew_normal_rng", rng_return_type<double_type>(t, u, v), t, u, v);
    }
  }
 }
add("softmax", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("sort_asc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)));
add("sort_asc", bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(bare_array_type(double_type(), 1)));
add("sort_asc", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("sort_asc", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("sort_desc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)));
add("sort_desc", bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(bare_array_type(double_type(), 1)));
add("sort_desc", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("sort_desc", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("sort_indices_asc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)));
add("sort_indices_asc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(double_type(), 1)));
add("sort_indices_asc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("sort_indices_asc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(row_vector_type()));
add("sort_indices_desc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(int_type(), 1)));
add("sort_indices_desc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(double_type(), 1)));
add("sort_indices_desc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(vector_type()));
add("sort_indices_desc", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(row_vector_type()));
add("squared_distance", bare_expr_type(double_type()), bare_expr_type(double_type()), bare_expr_type(double_type()));
add("squared_distance", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("squared_distance", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("squared_distance", bare_expr_type(double_type()), bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("squared_distance", bare_expr_type(double_type()), bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add_unary_vectorized("sqrt");
add_nullary("sqrt2");
add_unary_vectorized("square");
for (size_t i = 0; i < vector_types.size(); ++i)
{
  add("std_normal_log", bare_expr_type(double_type()), vector_types[i]);
  add("std_normal_lpdf", bare_expr_type(double_type()), vector_types[i]);
}
add_unary("step");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      for (size_t l = 0; l < vector_types.size(); ++l) {
        add("student_t_ccdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_cdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_cdf_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_log", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_lccdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_lcdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
        add("student_t_lpdf", bare_expr_type(double_type()), vector_types[i],
            vector_types[j], vector_types[k], vector_types[l]);
      }
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    for (const auto& v : all_vector_types) {
      add("student_t_rng", rng_return_type<double_type>(t, u, v), t, u, v);
    }
  }
 }
add("sub_col", bare_expr_type(vector_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("sub_row", bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("subtract", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("subtract", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("subtract", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("subtract", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(double_type()));
add("subtract", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(double_type()));
add("subtract", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()));
add("subtract", bare_expr_type(vector_type()), bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("subtract", bare_expr_type(row_vector_type()), bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("subtract", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("sum", bare_expr_type(int_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("sum", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("sum", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("sum", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("sum", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("tail", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()));
add("tail", bare_expr_type(vector_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()));
for (size_t i = 0; i < bare_types.size(); ++i) {
  add("tail", bare_expr_type(bare_array_type(bare_types[i], 1)),
      bare_expr_type(bare_array_type(bare_types[i], 1)), bare_expr_type(int_type()));
  add("tail", bare_expr_type(bare_array_type(bare_types[i], 2)),
      bare_expr_type(bare_array_type(bare_types[i], 2)), bare_expr_type(int_type()));
  add("tail", bare_expr_type(bare_array_type(bare_types[i], 3)),
      bare_expr_type(bare_array_type(bare_types[i], 3)), bare_expr_type(int_type()));
 }
add_unary_vectorized("tan");
add_unary_vectorized("tanh");
add_nullary("target");  // converted to "get_lp" in term_grammar semantics
add("tcrossprod", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("tgamma");
add("to_array_1d", bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(matrix_type()));
add("to_array_1d", bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(vector_type()));
add("to_array_1d", bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(row_vector_type()));
for (size_t i=1; i < 10; i++) {
  add("to_array_1d", bare_expr_type(bare_array_type(double_type(), 1)),
      bare_expr_type(bare_array_type(bare_array_type(double_type(), i))));
  add("to_array_1d", bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(bare_array_type(bare_array_type(int_type(), i))));
 }
add("to_array_2d", bare_expr_type(bare_array_type(double_type(), 2)), bare_expr_type(matrix_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(row_vector_type()), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(double_type(), 1)), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(int_type(), 1)), bare_expr_type(int_type()), bare_expr_type(int_type()), bare_expr_type(int_type()));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(double_type(), 2)));
add("to_matrix", bare_expr_type(matrix_type()), bare_expr_type(bare_array_type(int_type(), 2)));
add("to_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(matrix_type()));
add("to_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("to_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(row_vector_type()));
add("to_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("to_row_vector", bare_expr_type(row_vector_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("to_vector", bare_expr_type(vector_type()), bare_expr_type(matrix_type()));
add("to_vector", bare_expr_type(vector_type()), bare_expr_type(vector_type()));
add("to_vector", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("to_vector", bare_expr_type(vector_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("to_vector", bare_expr_type(vector_type()), bare_expr_type(bare_array_type(int_type(), 1)));
add("trace", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("trace_gen_quad_form", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("trace_quad_form", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(vector_type()));
add("trace_quad_form", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add("transpose", bare_expr_type(row_vector_type()), bare_expr_type(vector_type()));
add("transpose", bare_expr_type(vector_type()), bare_expr_type(row_vector_type()));
add("transpose", bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));
add_unary_vectorized("trunc");
add_unary_vectorized("trigamma");
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("uniform_ccdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_cdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_cdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_lccdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_lcdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("uniform_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("uniform_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
add("variance", bare_expr_type(double_type()), bare_expr_type(bare_array_type(double_type(), 1)));
add("variance", bare_expr_type(double_type()), bare_expr_type(vector_type()));
add("variance", bare_expr_type(double_type()), bare_expr_type(row_vector_type()));
add("variance", bare_expr_type(double_type()), bare_expr_type(matrix_type()));
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("von_mises_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("von_mises_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
}
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("von_mises_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      add("weibull_ccdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_cdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_cdf_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_log", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_lccdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_lcdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
      add("weibull_lpdf", bare_expr_type(double_type()),
          vector_types[i], vector_types[j], vector_types[k]);
    }
  }
 }
for (const auto& t : all_vector_types) {
  for (const auto& u : all_vector_types) {
    add("weibull_rng", rng_return_type<double_type>(t, u), t, u);
  }
 }
for (size_t i = 0; i < vector_types.size(); ++i) {
  for (size_t j = 0; j < vector_types.size(); ++j) {
    for (size_t k = 0; k < vector_types.size(); ++k) {
      for (size_t l = 0; l < vector_types.size(); ++l) {
        for (size_t m = 0; m < vector_types.size(); ++m) {
          add("wiener_log", bare_expr_type(double_type()), vector_types[i],
              vector_types[j],vector_types[k], vector_types[l],
              vector_types[m]);
          add("wiener_lpdf", bare_expr_type(double_type()), vector_types[i],
              vector_types[j],vector_types[k], vector_types[l],
              vector_types[m]);
        }
      }
    }
  }
 }
add("wishart_log", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("wishart_lpdf", bare_expr_type(double_type()), bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
add("wishart_rng", bare_expr_type(matrix_type()), bare_expr_type(double_type()), bare_expr_type(matrix_type()));
